const amqp = require('amqplib');
const { pool_payment } = require('./database/config/db.cjs');
const { writeOutbox, startRelay, consumeIdempotent } = require('./outbox.cjs');

const SUCCESS_RATE = 0.7;

const main = async () => {
    const connection = await amqp.connect('amqp://localhost');
    connection.on('close', () => {
        console.error('[Payment] RabbitMQ connection closed, exiting');
        process.exit(1);
    });

    const channel = await connection.createConfirmChannel();
    const exchange = 'market';
    const queue = 'payment';

    await channel.assertExchange(exchange, 'topic', { durable: true });
    await channel.assertQueue(queue, { durable: true });
    await channel.bindQueue(queue, exchange, 'payment.request');
    channel.prefetch(10);

    startRelay({ pool: pool_payment, channel, exchange });

    consumeIdempotent({
        channel,
        queue,
        pool: pool_payment,
        handler: async (client, data) => {
            const orderId = data.order_id;
            const { item_name: itemName, quantity } = data.items[0];

            console.log(`[Payment] processing order ${orderId} (${quantity} x ${itemName})`);

            // Decided inside the transaction: if anything below fails, the whole
            // thing rolls back and a retry simply rolls the dice again.
            const isSuccess = Math.random() < SUCCESS_RATE;
            const status = isSuccess ? 'completed' : 'failed';

            // Business-key guard in addition to message-level dedupe.
            const inserted = await client.query(
                `INSERT INTO payment_log(order_id, status)
                 VALUES($1, $2)
                 ON CONFLICT (order_id) DO NOTHING
                 RETURNING order_id`,
                [orderId, status]
            );
            if (inserted.rowCount === 0) {
                console.log(`[Payment] order ${orderId} already has a payment, skipping`);
                return;
            }
 
            if (isSuccess) {
                console.log(`[Payment] ${orderId} success`);
                await writeOutbox(client, orderId, 'order.completed', {
                    order_id: orderId,
                    items: data.items,
                });
            } else {
                console.log(`[Payment] ${orderId} failed`);
                // Tell the order service it failed...
                await writeOutbox(client, orderId, 'order.failed', {
                    order_id: orderId,
                    code: 'PAYMENT_FAILED',
                    reason: 'Pembayaran ditolak',
                });
                // ...and tell inventory to undo the reservation.
                await writeOutbox(client, orderId, 'stock.release', {
                    order_id: orderId,
                    items: data.items,
                });
            }
        },
    });
};

main();
