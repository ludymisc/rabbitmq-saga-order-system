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

