package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// ====== ДОМЕН ======

type SwapEvent struct {
	ID        string    `json:"id"`
	Token     string    `json:"token"`
	Amount    float64   `json:"amount"`
	USD       float64   `json:"usd"`
	Side      string    `json:"side"`
	EventTime time.Time `json:"eventTime"`
}

type WindowStats struct {
	VolumeToken float64 `json:"volume_token"`
	VolumeUSD   float64 `json:"volume_usd"`
	Count       int64   `json:"count"`
	BuyCount    int64   `json:"buy_count"`
	SellCount   int64   `json:"sell_count"`
}

type StatsResponse struct {
	Token   string      `json:"token"`
	Ts      time.Time   `json:"ts"`
	Win5m   WindowStats `json:"win_5m"`
	Win1h   WindowStats `json:"win_1h"`
	Win24h  WindowStats `json:"win_24h"`
	Watermk time.Time   `json:"watermark"`
}

// ====== СТОР (ИНТЕРФЕЙС) ======

type StateSnapshot struct {
	Tokens     map[string]map[int64]Counter `json:"tokens"`
	Watermarks map[string]int64             `json:"watermarks_unix"`
	Dedup      map[string]int64             `json:"dedup_ids_unix"`
	SavedAt    int64                        `json:"saved_at"`
}

type StateStore interface {
	Load(ctx context.Context) (*StateSnapshot, error)
	Save(ctx context.Context, snap *StateSnapshot) error
}

type FileStateStore struct {
	Path string
	mu   sync.Mutex
}

func (f *FileStateStore) Load(ctx context.Context) (*StateSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, err := os.ReadFile(f.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &StateSnapshot{
				Tokens:     make(map[string]map[int64]Counter),
				Watermarks: make(map[string]int64),
				Dedup:      make(map[string]int64),
				SavedAt:    time.Now().Unix(),
			}, nil
		}
		return nil, err
	}
	var snap StateSnapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return nil, err
	}
	if snap.Tokens == nil {
		snap.Tokens = make(map[string]map[int64]Counter)
	}
	if snap.Watermarks == nil {
		snap.Watermarks = make(map[string]int64)
	}
	if snap.Dedup == nil {
		snap.Dedup = make(map[string]int64)
	}
	return &snap, nil
}

// Save с fsync: write temp -> Sync -> close -> rename
func (f *FileStateStore) Save(ctx context.Context, snap *StateSnapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	tmp := f.Path + ".tmp"
	b, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	// Open temp file
	fp, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	// write
	if _, err := fp.Write(b); err != nil {
		_ = fp.Close()
		return err
	}
	// fsync
	if err := fp.Sync(); err != nil {
		_ = fp.Close()
		return err
	}
	if err := fp.Close(); err != nil {
		return err
	}
	// atomic rename
	return os.Rename(tmp, f.Path)
}

// ====== АГРЕГАТОР ======

type Counter struct {
	VolumeToken float64 `json:"volume_token"`
	VolumeUSD   float64 `json:"volume_usd"`
	Count       int64   `json:"count"`
	BuyCount    int64   `json:"buy_count"`
	SellCount   int64   `json:"sell_count"`
}

func (c *Counter) add(ev SwapEvent) {
	c.VolumeToken += ev.Amount
	c.VolumeUSD += ev.USD
	c.Count++
	if strings.EqualFold(ev.Side, "buy") {
		c.BuyCount++
	} else if strings.EqualFold(ev.Side, "sell") {
		c.SellCount++
	}
}

type Aggregator struct {
	mu         sync.RWMutex
	tokens     map[string]map[int64]Counter
	watermarks map[string]time.Time
	dedup      map[string]time.Time
	lateGrace  time.Duration
	retention  time.Duration
	store      StateStore
	hub        *WSHub
	stopCh     chan struct{}
	saveEvery  time.Duration
	dedupTTL   time.Duration
}

func NewAggregator(store StateStore, hub *WSHub) *Aggregator {
	return &Aggregator{
		tokens:     make(map[string]map[int64]Counter),
		watermarks: make(map[string]time.Time),
		dedup:      make(map[string]time.Time),
		lateGrace:  3 * time.Minute,
		retention:  25 * time.Hour,
		store:      store,
		hub:        hub,
		stopCh:     make(chan struct{}),
		saveEvery:  3 * time.Second,
		dedupTTL:   25 * time.Hour,
	}
}

func minuteKey(t time.Time) int64 {
	return t.Truncate(time.Minute).Unix()
}

func (a *Aggregator) Load(ctx context.Context) error {
	snap, err := a.store.Load(ctx)
	if err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	// deep copy tokens
	tokensCopy := make(map[string]map[int64]Counter, len(snap.Tokens))
	for tok, buckets := range snap.Tokens {
		bcopy := make(map[int64]Counter, len(buckets))
		for k, v := range buckets {
			bcopy[k] = v
		}
		tokensCopy[tok] = bcopy
	}
	a.tokens = tokensCopy

	a.watermarks = make(map[string]time.Time, len(snap.Watermarks))
	for k, v := range snap.Watermarks {
		a.watermarks[k] = time.Unix(v, 0).UTC()
	}
	a.dedup = make(map[string]time.Time, len(snap.Dedup))
	now := time.Now().UTC()
	for id, exp := range snap.Dedup {
		if time.Unix(exp, 0).After(now) {
			a.dedup[id] = time.Unix(exp, 0)
		}
	}
	return nil
}

func (a *Aggregator) snapshot() *StateSnapshot {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// deep copy tokens
	toks := make(map[string]map[int64]Counter, len(a.tokens))
	for tok, buckets := range a.tokens {
		bcopy := make(map[int64]Counter, len(buckets))
		for k, v := range buckets {
			bcopy[k] = v
		}
		toks[tok] = bcopy
	}

	ws := make(map[string]int64, len(a.watermarks))
	for k, v := range a.watermarks {
		ws[k] = v.Unix()
	}

	dd := make(map[string]int64, len(a.dedup))
	for id, exp := range a.dedup {
		dd[id] = exp.Unix()
	}

	return &StateSnapshot{
		Tokens:     toks,
		Watermarks: ws,
		Dedup:      dd,
		SavedAt:    time.Now().Unix(),
	}
}

func (a *Aggregator) StartBackground(ctx context.Context) {
	ticker := time.NewTicker(a.saveEvery)
	defer ticker.Stop()
	cleanTicker := time.NewTicker(1 * time.Minute)
	defer cleanTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-a.stopCh:
			return
		case <-ticker.C:
			_ = a.store.Save(ctx, a.snapshot())
		case <-cleanTicker.C:
			a.gc()
		}
	}
}

func (a *Aggregator) Stop() { close(a.stopCh) }

func (a *Aggregator) gc() {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now().UTC()

	// GC дедуп-идов
	for id, exp := range a.dedup {
		if exp.Before(now) {
			delete(a.dedup, id)
		}
	}
	// GC старых бакетов
	cutoff := now.Add(-a.retention)
	for token, buckets := range a.tokens {
		for mk := range buckets {
			if time.Unix(mk, 0).Before(cutoff) {
				delete(buckets, mk)
			}
		}
		if len(buckets) == 0 {
			delete(a.tokens, token)
		}
	}
}

// ProcessEvent — идемпотентно учитывает событие (с допуском out-of-order)
func (a *Aggregator) ProcessEvent(ev SwapEvent) {
	if ev.ID == "" || ev.Token == "" {
		return
	}

	evTime := ev.EventTime.UTC()
	now := time.Now().UTC()
	if evTime.After(now.Add(5 * time.Minute)) {
		// слишком из будущего — защитимся
		evTime = now
	}

	// Захватываем write-lock и выполняем проверку дедуп + update
	a.mu.Lock()
	// check dedup inside lock (avoid TOCTOU)
	if _, ok := a.dedup[ev.ID]; ok {
		a.mu.Unlock()
		return
	}

	// обновим watermark
	wm := a.watermarks[ev.Token]
	if evTime.After(wm) {
		a.watermarks[ev.Token] = evTime
	} else {
		if wm.Sub(evTime) > a.lateGrace {
			// слишком поздно — игнорируем
			a.mu.Unlock()
			return
		}
	}

	// бакет по минуте
	mk := minuteKey(evTime)
	if _, ok := a.tokens[ev.Token]; !ok {
		a.tokens[ev.Token] = make(map[int64]Counter)
	}
	cnt := a.tokens[ev.Token][mk]
	cnt.add(ev)
	a.tokens[ev.Token][mk] = cnt

	// помним дедуп
	a.dedup[ev.ID] = time.Now().UTC().Add(a.dedupTTL)

	// подготовим стейт для отправки в WS без удержания lock во время записи в сеть
	stats := a.computeStatsNoLock(ev.Token, time.Now().UTC())
	a.mu.Unlock()

	// пушим дельту в WS (вне lock)
	if a.hub != nil {
		_ = a.hub.Broadcast(ev.Token, stats)
	}
}

func (a *Aggregator) computeWindowNoLock(token string, now time.Time, dur time.Duration) WindowStats {
	from := now.Add(-dur)
	var res WindowStats
	buckets, ok := a.tokens[token]
	if !ok {
		return res
	}
	startMin := minuteKey(from)
	endMin := minuteKey(now)
	for mk, c := range buckets {
		if mk >= startMin && mk <= endMin {
			res.VolumeToken += c.VolumeToken
			res.VolumeUSD += c.VolumeUSD
			res.Count += c.Count
			res.BuyCount += c.BuyCount
			res.SellCount += c.SellCount
		}
	}
	return res
}

func (a *Aggregator) computeStatsNoLock(token string, now time.Time) *StatsResponse {
	return &StatsResponse{
		Token:   token,
		Ts:      now,
		Win5m:   a.computeWindowNoLock(token, now, 5*time.Minute),
		Win1h:   a.computeWindowNoLock(token, now, time.Hour),
		Win24h:  a.computeWindowNoLock(token, now, 24*time.Hour),
		Watermk: a.watermarks[token],
	}
}

func (a *Aggregator) GetStats(token string) *StatsResponse {
	now := time.Now().UTC()
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.computeStatsNoLock(token, now)
}

// ====== WEBSOCKET HUB (per-conn mutex + cleanup on error) ======

type wsClient struct {
	c  *websocket.Conn
	mu sync.Mutex // protect writes
}

type WSHub struct {
	upgrader websocket.Upgrader
	mu       sync.RWMutex
	// token -> set of *wsClient
	subs map[string]map[*wsClient]struct{}
}

func NewWSHub() *WSHub {
	return &WSHub{
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
		subs: make(map[string]map[*wsClient]struct{}),
	}
}

func (h *WSHub) HandleWS(a *Aggregator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.URL.Query().Get("token")
		if token == "" {
			http.Error(w, "token is required", http.StatusBadRequest)
			return
		}
		c, err := h.upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("ws upgrade:", err)
			return
		}
		wc := &wsClient{c: c}

		h.mu.Lock()
		if _, ok := h.subs[token]; !ok {
			h.subs[token] = make(map[*wsClient]struct{})
		}
		h.subs[token][wc] = struct{}{}
		h.mu.Unlock()

		// отправим текущие статы
		if a != nil {
			if s := a.GetStats(token); s != nil {
				wc.mu.Lock()
				_ = wc.c.WriteJSON(s)
				wc.mu.Unlock()
			}
		}

		// читающая горутина — наблюдает за закрытием
		go func() {
			defer func() {
				// удаляем и закрываем
				h.mu.Lock()
				delete(h.subs[token], wc)
				h.mu.Unlock()
				_ = wc.c.Close()
			}()
			wc.c.SetReadLimit(1024)
			wc.c.SetReadDeadline(time.Now().Add(60 * time.Second))
			wc.c.SetPongHandler(func(string) error {
				wc.c.SetReadDeadline(time.Now().Add(60 * time.Second))
				return nil
			})
			for {
				if _, _, err := wc.c.ReadMessage(); err != nil {
					return
				}
			}
		}()
	}
}

func (h *WSHub) Broadcast(token string, stats *StatsResponse) error {
	h.mu.RLock()
	subs := h.subs[token]
	// create slice to avoid holding lock while writing and to allow cleanup
	clients := make([]*wsClient, 0, len(subs))
	for c := range subs {
		clients = append(clients, c)
	}
	h.mu.RUnlock()

	for _, wc := range clients {
		wc.mu.Lock()
		err := wc.c.WriteJSON(stats)
		wc.mu.Unlock()
		if err != nil {
			// Ошибка записи: удалим клиента
			h.mu.Lock()
			for _, set := range h.subs {
				if _, ok := set[wc]; ok {
					delete(set, wc)
					break
				}
			}
			h.mu.Unlock()
			_ = wc.c.Close()
		}
	}
	return nil
}

// ====== HTTP ======

func statsHandler(a *Aggregator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.URL.Query().Get("token")
		if token == "" {
			http.Error(w, "token is required", http.StatusBadRequest)
			return
		}
		stats := a.GetStats(token)
		if stats == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(stats)
	}
}

// ====== ДЕМО-ГЕНЕРАТОР СОБЫТИЙ ======

func startDemoProducer(ctx context.Context, out chan<- SwapEvent, tokens []string, ratePerSec int) {
	if ratePerSec <= 0 {
		ratePerSec = 1
	}
	interval := time.Second / time.Duration(max(1, ratePerSec))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	rand.Seed(time.Now().UnixNano())
	var seq uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			seq++
			tok := tokens[rand.Intn(len(tokens))]
			side := "buy"
			if rand.Intn(2) == 0 {
				side = "sell"
			}
			amt := (rand.Float64()*9 + 1) / 10.0
			usd := amt * (1000 + rand.Float64()*1000)
			ts := time.Now().UTC()
			if rand.Intn(20) == 0 {
				ts = ts.Add(-time.Duration(rand.Intn(150)) * time.Second)
			}
			ev := SwapEvent{
				ID:        tok + "|" + strconv.FormatUint(seq, 10),
				Token:     tok,
				Amount:    amt,
				USD:       usd,
				Side:      side,
				EventTime: ts,
			}
			select {
			case out <- ev:
			default:
				// drop if channel full (for demo)
			}
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ====== MAIN ======

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	events := make(chan SwapEvent, 10000)
	hub := NewWSHub()
	statePath := filepath.Join(".", "state.json")
	store := &FileStateStore{Path: statePath}
	agg := NewAggregator(store, hub)
	if err := agg.Load(ctx); err != nil {
		log.Fatal("load state:", err)
	}
	go agg.StartBackground(ctx)

	// Консьюм событий из канала
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-events:
				agg.ProcessEvent(ev)
			}
		}
	}()

	// Демогенератор
	go startDemoProducer(ctx, events, []string{"ETH", "BTC", "SOL"}, 200)

	// HTTP
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200); _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("/stats", statsHandler(agg))
	mux.HandleFunc("/ws", hub.HandleWS(agg))

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           logRequests(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Println("listening on :8080")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")
	shCtx, shCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shCancel()
	_ = srv.Shutdown(shCtx)
	agg.Stop()
	_ = store.Save(context.Background(), agg.snapshot())
	log.Println("bye")
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.String(), time.Since(start))
	})
}
