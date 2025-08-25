package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PG struct {
	pool  *pgxpool.Pool
	grace time.Duration
}

func NewPG(ctx context.Context, dsn string, grace time.Duration) (*PG, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &PG{pool: pool, grace: grace}, nil
}

func (p *PG) Close() { p.pool.Close() }

func (p *PG) UpsertEvent(ctx context.Context, evID, token, side string, amt, usd float64, eventTime time.Time) (bool, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
		INSERT INTO dedup(id, seen_at)
		VALUES ($1, now())
		ON CONFLICT (id) DO NOTHING
	`, evID)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		return false, nil
	}

	var wm time.Time
	err = tx.QueryRow(ctx, `SELECT watermark FROM tokens WHERE token=$1`, token).Scan(&wm)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			wm = time.Time{}
			if _, err2 := tx.Exec(ctx, `INSERT INTO tokens(token, watermark) VALUES ($1, $2) ON CONFLICT (token) DO NOTHING`, token, wm); err2 != nil {
				return false, err2
			}
		} else {
			return false, err
		}
	}
	if !wm.IsZero() && wm.Sub(eventTime) > p.grace {
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		return false, nil
	}

	minute := eventTime.UTC().Truncate(time.Minute)
	var buyInc, sellInc int64
	switch side {
	case "buy":
		buyInc = 1
	case "sell":
		sellInc = 1
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO minute_buckets(token, minute_utc, volume_token, volume_usd, cnt, buy_cnt, sell_cnt)
		VALUES ($1,$2,$3,$4,1,$5,$6)
		ON CONFLICT (token, minute_utc) DO UPDATE SET
		  volume_token = minute_buckets.volume_token + EXCLUDED.volume_token,
		  volume_usd   = minute_buckets.volume_usd   + EXCLUDED.volume_usd,
		  cnt          = minute_buckets.cnt          + 1,
		  buy_cnt      = minute_buckets.buy_cnt      + EXCLUDED.buy_cnt,
		  sell_cnt     = minute_buckets.sell_cnt     + EXCLUDED.sell_cnt
	`, token, minute, amt, usd, buyInc, sellInc); err != nil {
		return false, err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO tokens(token, watermark) VALUES ($1,$2)
		ON CONFLICT (token) DO UPDATE SET watermark = GREATEST(tokens.watermark, EXCLUDED.watermark)
	`, token, eventTime); err != nil {
		return false, err
	}

	if _, err := tx.Exec(ctx, `SELECT pg_notify('minute_update', $1)`, token); err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (p *PG) QueryStats(ctx context.Context, token string, now time.Time) (*Stats, error) {
	var wm time.Time
	_ = p.pool.QueryRow(ctx, `SELECT watermark FROM tokens WHERE token=$1`, token).Scan(&wm)
	from5 := now.Add(-5 * time.Minute).UTC().Truncate(time.Minute)
	from1 := now.Add(-1 * time.Hour).UTC().Truncate(time.Minute)
	from24 := now.Add(-24 * time.Hour).UTC().Truncate(time.Minute)
	to := now.UTC().Truncate(time.Minute)

	s := &Stats{Token: token, Now: now, Watermark: wm}
	agg := func(from time.Time) (Window, error) {
		var w Window
		row := p.pool.QueryRow(ctx, `
			SELECT COALESCE(SUM(volume_token),0), COALESCE(SUM(volume_usd),0),
			       COALESCE(SUM(cnt),0), COALESCE(SUM(buy_cnt),0), COALESCE(SUM(sell_cnt),0)
			FROM minute_buckets WHERE token=$1 AND minute_utc >= $2 AND minute_utc <= $3
		`, token, from, to)
		if err := row.Scan(&w.VolumeToken, &w.VolumeUSD, &w.Cnt, &w.Buy, &w.Sell); err != nil {
			return w, err
		}
		return w, nil
	}
	var err error
	if s.W5, err = agg(from5); err != nil {
		return nil, err
	}
	if s.W1, err = agg(from1); err != nil {
		return nil, err
	}
	if s.W24, err = agg(from24); err != nil {
		return nil, err
	}
	return s, nil
}

func (p *PG) ListenUpdates(ctx context.Context) (<-chan string, func(), error) {
	acq, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	pgxConn := acq.Conn()

	if _, err := pgxConn.Exec(ctx, `LISTEN minute_update`); err != nil {
		acq.Release()
		return nil, nil, err
	}

	out := make(chan string, 1024)
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		defer close(out)
		defer func() {
			_, _ = pgxConn.Exec(context.Background(), `UNLISTEN *`)
			acq.Release()
		}()

		for {
			n, err := pgxConn.WaitForNotification(ctx)
			if err != nil {
				return
			}
			if n != nil {
				out <- n.Payload
			}
		}
	}()

	return out, cancel, nil
}
