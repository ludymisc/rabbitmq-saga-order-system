-- Database: Inventory

-- DROP DATABASE IF EXISTS "Inventory";
CREATE SEQUENCE inv_seq START WITH 1 INCREMENT BY 1;

DROP TABLE IF EXISTS inventory;
CREATE TABLE inventory (
    id VARCHAR(255) PRIMARY KEY DEFAULT 'INV' || lpad(nextval('inv_seq')::text, 3, '0'),
    name VARCHAR(100) NOT NULL,
	stock int NOT NULL,
	price int NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE outbox(
id BIGSERIAL PRIMARY KEY,
message_id UUID NOT NULL UNIQUE,
aggregate_id TEXT NOT NULL,
event_type TEXT NOT NULL,
payload JSONB NOT NULL,
created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
published_at TIMESTAMPTZ
);

CREATE INDEX outbox_published ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE processed_message(
message_id UUID PRIMARY KEY,
processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE stock_reservation (
  order_id   VARCHAR(255) PRIMARY KEY,
  item_id    VARCHAR(255) NOT NULL REFERENCES inventory(id),
  quantity   int NOT NULL,
  status     TEXT NOT NULL DEFAULT 'reserved',  -- reserved | released
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
