-- sweeper looks for old pending orders, keep that cheap
CREATE INDEX IF NOT EXISTS orders_pending_idx ON orders (created_at) WHERE status = 'pending';
