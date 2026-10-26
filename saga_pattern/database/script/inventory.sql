-- Database: Order

-- DROP DATABASE IF EXISTS "Order";
CREATE SEQUENCE inv_seq START WITH 1 INCREMENT BY 1;

DROP TABLE IF EXISTS inventory;
CREATE TABLE inventory (
    id VARCHAR(255) PRIMARY KEY DEFAULT 'INV' || lpad(nextval('inv_seq')::text, 3, '0'),
    name VARCHAR(100) NOT NULL,
	stock int NOT NULL,
	price int NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

