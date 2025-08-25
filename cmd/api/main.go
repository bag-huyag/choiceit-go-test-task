package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"swaps-rt/internal/store"

	"github.com/gorilla/websocket"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

type wsHub struct {
	mu      sync.RWMutex
	clients map[string]map[*websocket.Conn]struct{}
}

func newHub() *wsHub {
	return &wsHub{clients: make(map[string]map[*websocket.Conn]struct{})}
}

func (h *wsHub) add(token string, c *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clients[token] == nil {
		h.clients[token] = make(map[*websocket.Conn]struct{})
	}
	h.clients[token][c] = struct{}{}
}

func (h *wsHub) remove(token string, c *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if m := h.clients[token]; m != nil {
		delete(m, c)
		if len(m) == 0 {
			delete(h.clients, token)
		}
	}
}

func (h *wsHub) broadcast(token string, payload any) {
	h.mu.RLock()
	list := make([]*websocket.Conn, 0, len(h.clients[token]))
	for c := range h.clients[token] {
		list = append(list, c)
	}
	h.mu.RUnlock()

	b, _ := json.Marshal(payload)
	for _, c := range list {
		_ = c.WriteMessage(websocket.TextMessage, b)
	}
}

func main() {
	ctx := context.Background()
	dsn := env("DB_DSN", "postgresql://swaps:swaps@localhost:5432/swaps?sslmode=disable")
	graceSec := 180
	if v := os.Getenv("GRACE_SECONDS"); v != "" {
		if n, err := time.ParseDuration(v + "s"); err == nil {
			graceSec = int(n / time.Second)
		}
	}
	pg, err := store.NewPG(ctx, dsn, time.Duration(graceSec)*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer pg.Close()

	hub := newHub()

	go func() {
		ch, cancel, err := pg.ListenUpdates(ctx)
		if err != nil {
			log.Printf("listen error: %v", err)
			return
		}
		defer cancel()
		for token := range ch {
			stats, err := pg.QueryStats(context.Background(), token, time.Now().UTC())
			if err != nil {
				log.Printf("stats err: %v", err)
				continue
			}
			hub.broadcast(strings.ToUpper(token), stats)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		token := r.URL.Query().Get("token")
		if token == "" {
			http.Error(w, "missing token", http.StatusBadRequest)
			return
		}
		stats, err := pg.QueryStats(r.Context(), strings.ToUpper(token), time.Now().UTC())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(stats)
	})

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		token := r.URL.Query().Get("token")
		if token == "" {
			http.Error(w, "missing token", http.StatusBadRequest)
			return
		}
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		token = strings.ToUpper(token)
		hub.add(token, c)
		defer func() {
			hub.remove(token, c)
			_ = c.Close()
		}()

		if stats, err := pg.QueryStats(r.Context(), token, time.Now().UTC()); err == nil {
			_ = c.WriteJSON(stats)
		}

		c.SetReadDeadline(time.Now().Add(60 * time.Second))
		c.SetPongHandler(func(string) error {
			c.SetReadDeadline(time.Now().Add(60 * time.Second))
			return nil
		})
		go func() {
			t := time.NewTicker(30 * time.Second)
			defer t.Stop()
			for range t.C {
				_ = c.WriteMessage(websocket.PingMessage, nil)
			}
		}()
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	})

	addr := env("HTTP_ADDR", ":8080")
	log.Printf("api listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
