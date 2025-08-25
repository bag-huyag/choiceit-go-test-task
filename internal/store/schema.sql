CREATE TABLE IF NOT EXISTS tokens (
    token TEXT PRIMARY KEY,
    watermark TIMESTAMPTZ NOT NULL DEFAULT 'epoch'
);


CREATE TABLE IF NOT EXISTS dedup (
    id TEXT PRIMARY KEY,
    seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE TABLE IF NOT EXISTS minute_buckets (
    token TEXT NOT NULL,
    minute_utc TIMESTAMPTZ NOT NULL,
    volume_token DOUBLE PRECISION NOT NULL DEFAULT 0,
    volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    cnt BIGINT NOT NULL DEFAULT 0,
    buy_cnt BIGINT NOT NULL DEFAULT 0,
    sell_cnt BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY(token, minute_utc)
);


CREATE INDEX IF NOT EXISTS idx_buckets_token_time ON minute_buckets(token, minute_utc);

