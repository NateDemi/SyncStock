-- Ensure schema exists
CREATE SCHEMA IF NOT EXISTS syncstock;

-- 1) Metadata / watermark
CREATE TABLE IF NOT EXISTS syncstock.meta (
  id                   boolean PRIMARY KEY DEFAULT TRUE,
  last_sales_day_done  date,        -- last fully processed sales day
  run_status           text,
  notes                text,
  updated_at           timestamp NOT NULL DEFAULT now()
);

-- 2) Current stock (point-in-time snapshot)
CREATE TABLE IF NOT EXISTS syncstock.stock (
  inventory_id text PRIMARY KEY REFERENCES store_data.inventory_items(id),
  on_hand      integer   NOT NULL DEFAULT 0,
  updated_at   timestamp NOT NULL DEFAULT now(),
  version      bigint    NOT NULL DEFAULT 0
);

-- 3) Daily ledger (rollup by order_created_date)
CREATE TABLE IF NOT EXISTS syncstock.ledger (
  order_created_date date NOT NULL,   -- calendar date (sales & purchases rolled up)
  inventory_id       text NOT NULL REFERENCES store_data.inventory_items(id),
  purchased_qty      integer NOT NULL DEFAULT 0,
  sold_qty           integer NOT NULL DEFAULT 0,
  on_hand_end        integer NOT NULL,              -- closing balance at end of day
  computed_at        timestamp NOT NULL DEFAULT now(),
  PRIMARY KEY (order_created_date, inventory_id)
);

-- Helpful index for time-range queries
CREATE INDEX IF NOT EXISTS idx_ledger_date ON syncstock.ledger(order_created_date);

-- Ensure the meta row exists
INSERT INTO syncstock.meta (id, run_status, notes)
VALUES (TRUE, 'INITIALIZED', 'Schema created')
ON CONFLICT (id) DO NOTHING;