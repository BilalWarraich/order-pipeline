ALTER TABLE orders ADD COLUMN IF NOT EXISTS idempotency_key TEXT;

-- partial index: most orders won't have a key, and NULLs shouldn't clash anyway
CREATE UNIQUE INDEX IF NOT EXISTS orders_idempotency_key_idx
    ON orders (idempotency_key) WHERE idempotency_key IS NOT NULL;
