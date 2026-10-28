-- Database: Order

-- DROP DATABASE IF EXISTS "Order";
CREATE SEQUENCE odr_seq START WITH 1 INCREMENT BY 1;
CREATE TYPE order_status AS ENUM('pending', 'completed', 'failed');

DROP TABLE IF EXISTS order_log;
CREATE TABLE order_log (
    id VARCHAR(255) PRIMARY KEY DEFAULT 'ODR' || lpad(nextval('odr_seq')::text, 3, '0'),
    name VARCHAR(100) NOT NULL,
	quantity int NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	status order_status DEFAULT 'pending'
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