const amqp = require('amqplib');
const { pool_inventory } = require('./database/config/db.cjs');
const { writeOutbox, startRelay, consumeIdempotent } = require('./outbox.cjs');

const main = async () => {
    const connection = await amqp.connect('amqp://localhost');
    connection.on('close', () => {
        console.error('[Inventory] RabbitMQ connection closed, exiting');
        process.exit(1);
    });

    const channel = await connection.createConfirmChannel();
    const exchange = 'market';
    const queue = 'inventory';

    await channel.assertExchange(exchange, 'topic', { durable: true });
    await channel.assertQueue(queue, { durable: true });

    // order.created -> try to reserve stock
    // stock.release -> compensation after a failed payment
    for (const key of ['order.created', 'stock.release']) {
        await channel.bindQueue(queue, exchange, key);
        console.log(`[Setup] Queue '${queue}' di-bind ke key: '${key}'`);
    }
    channel.prefetch(10);

    startRelay({ pool: pool_inventory, channel, exchange });

    consumeIdempotent({
        channel,
        queue,
        pool: pool_inventory,
        handler: async (client, data, routingKey) => {
            const orderId = data.order_id;
            const { item_name: itemName, quantity: qty } = data.items[0];

            // ---- Compensation: payment failed, give the stock back ----
            if (routingKey === 'stock.release') {
                await client.query(
                    `UPDATE inventory SET stock = stock + $1, updated_at = now() WHERE name = $2`,
                    [qty, itemName]
                );
                console.log(`[Inventory] ${orderId} stock restored (${qty} x ${itemName})`);
                return;
            }

            // ---- Reserve: one atomic statement, no check-then-update race ----
            const reserved = await client.query(
                `UPDATE inventory
                 SET stock = stock - $1, updated_at = now()
                 WHERE name = $2 AND stock >= $1
                 RETURNING id`,
                [qty, itemName]
            );

            if (reserved.rowCount === 1) {
                console.log(`[Inventory] ${orderId} stock reserved (${qty} x ${itemName})`);
                await writeOutbox(client, orderId, 'payment.request', data);
                return;
            }

            // Reservation failed: work out why, then tell the order service.
            const exists = await client.query(
                `SELECT 1 FROM inventory WHERE name = $1`,
                [itemName]
            );
            const failure =
                exists.rowCount === 0
                    ? { code: 'ITEM_UNAVAILABLE', reason: `Item (${itemName}) tidak ditemukan` }
                    : { code: 'OUT_OF_STOCK', reason: `Stock item (${itemName}) tidak cukup untuk ${qty} pcs` };

            console.log(`[Inventory] ${orderId} ${failure.code}`);
            await writeOutbox(client, orderId, 'order.failed', {
                order_id: orderId,
                ...failure,
            });
        },
    });
};
 
main();
