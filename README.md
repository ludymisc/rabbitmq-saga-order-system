![Node.js](https://img.shields.io/badge/Node.js-22-green)
![JavaScript](https://img.shields.io/badge/JavaScript-F7DF1E?logo=javascript&logoColor=yellow)
![Microservices](https://img.shields.io/badge/Architecture-Microservices-blueviolet)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-0.9.1-orange?logo=rabbitmq&logoColor=orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=blue)

# RabbitMQ Saga Order System

Event-driven order processing built with the **choreography-based Saga pattern**. Services never call each other directly: they publish and consume events through a RabbitMQ topic exchange, and each service owns its own PostgreSQL database.

If a step fails (item not found, insufficient stock, failed payment), the order is marked as failed and stock that was already reserved is restored through a compensating action.

## Tech Stack

| Category | Tool | Purpose |
| --- | --- | --- |
| Runtime | Node.js (CommonJS, `.cjs`) | Runs all services |
| Message broker | RabbitMQ | Topic exchange `market` for event routing |
| Database | PostgreSQL | One database per service (order, inventory, payment) |
| AMQP client | `amqplib` | Publish / consume messages |
| DB client | `pg` | Connection pools and queries |
| Config | `dotenv` | Loads connection strings from `.env` |
| CLI | `commander` | Command line interface for placing orders |

## Project Structure

```
saga_pattern/
├── order.cjs
├── order_log.cjs
├── inventory_service.cjs
├── payment_service.cjs
├── database/
│   ├── config/
│   │   └── db.cjs
│   └── script/
│       ├── inventory.sql
│       ├── order.sql
│       └── payment.sql
└── .env
```

## Services

| File | Role |
| --- | --- |
| `order.cjs` | CLI that creates an order and publishes it to RabbitMQ as `order.place`. |
| `order_log.cjs` | Receives new orders, records them in the order database, and forwards them to the inventory service as `order.created`. Also listens to `order.completed` and `order.failed`. |
| `inventory_service.cjs` | Checks item availability and stock. If the item doesn't exist or stock is not enough, it marks the order as failed and reports the error back to `order_log`. If the item is available, it reserves the stock and sends a `payment.request` to the payment service. Restores stock when a payment fails. |
| `payment_service.cjs` | Receives `payment.request` from the inventory service and simulates a payment with a randomized success/failure result. It records the payment, updates the order status, and finishes the order (`order.completed` on success, failure event on failure). |

## Event Flow

```
order.cjs
   │  order.place
   ▼
order_log.cjs ──────────────────────────────┐
   │  order.created                         │ order.failed / order.completed
   ▼                                        │
inventory_service.cjs ──── order.failed ────┤
   │  payment.request                       │
   ▼                                        │
payment_service.cjs ──── order.completed ───┘
        │
        └── failure ──► inventory_service.cjs (restores reserved stock)
```

## Database

All database files are in `saga_pattern/database/`:

- **`script/`**: DDL scripts for every database used in this project (order, inventory, and payment). Run them before starting the services.
- **`config/`**: `db.cjs`, which creates and exports the PostgreSQL connection pools (`pool_order`, `pool_inventory`, `pool_payment`).

## Getting Started

### Prerequisites

- Node.js
- RabbitMQ running on `localhost:5672`
- PostgreSQL with three databases created (order, inventory, payment)

### Installation

```bash
npm install amqplib pg dotenv commander
```

### Configuration

Create a `.env` file with one connection string per database:

```env
DB_ORDER_URL=postgresql://user:password@localhost:5432/order_db
DB_INVENTORY_URL=postgresql://user:password@localhost:5432/inventory_db
DB_PAYMENT_URL=postgresql://user:password@localhost:5432/payment_db
```

Then run the DDL scripts from `database/script/` against their matching databases.

### Running

Start each service in its own terminal:

```bash
node order_log.cjs
node inventory_service.cjs
node payment_service.cjs
```

Place an order:

```bash
node order.cjs --item dompet --qty 2
```

| Option | Description |
| --- | --- |
| `-i, --item <name>` | Name of the item to order (stored in lowercase) |
| `-q, --qty <number>` | Quantity to order (default: `1`) |

## Notes

- Item names are matched case-sensitively in the inventory table, so store them in lowercase.
- Keep `.env` out of version control.
