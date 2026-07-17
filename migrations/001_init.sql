CREATE TABLE IF NOT EXISTS transactions (
    id              UUID PRIMARY KEY,
    merchant_id     TEXT NOT NULL,
    amount_cents    BIGINT NOT NULL,
    currency        TEXT NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    inserted_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
 
CREATE INDEX IF NOT EXISTS idx_transactions_merchant_id ON transactions (merchant_id);
CREATE INDEX IF NOT EXISTS idx_transactions_occurred_at ON transactions (occurred_at);