-- Database: Order

-- DROP DATABASE IF EXISTS "Order";
--CREATE SEQUENCE pym_seq START WITH 1 INCREMENT BY 1;

CREATE TYPE order_status AS ENUM('pending', 'completed', 'failed');

DROP TABLE IF EXISTS payment_log;
CREATE TABLE payment_log (
    id VARCHAR(255) PRIMARY KEY DEFAULT 'PYM' || lpad(nextval('pym_seq')::text, 3, '0'),
	order_id VARCHAR(255) NOT NULL UNIQUE, 
    name VARCHAR(100) NOT NULL,
	quantity int NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	status order_status DEFAULT 'pending'
);

