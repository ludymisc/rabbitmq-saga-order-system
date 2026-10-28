const amqp = require('amqplib');
const { pool_order } = require('./database/config/db.cjs');
const { startRelay, consumeIdempotent } = require('./outbox.cjs');
 
const main = async () => {
    const connection = await amqp.connect('amqp://localhost');
    connection.on('close', () => {
        console.error('[Order] RabbitMQ connection closed, exiting');
        process.exit(1);
    });

    // Confirm channel: needed so the relay can wait for broker acks.
    const channel = await connection.createConfirmChannel();
    const exchange = 'market';
    const queue = 'order';

    await channel.assertExchange(exchange, 'topic', { durable: true });
    await channel.assertQueue(queue, { durable: true });

    // Order service only listens for the final outcome of a saga.
    for (const key of ['order.completed', 'order.failed']) {
        await channel.bindQueue(queue, exchange, key);
        console.log(`[Setup] Queue '${queue}' di-bind ke key: '${key}'`);
    }
    channel.prefetch(10);
 
    // Publishes order.created rows written by order.cjs.
    startRelay({ pool: pool_order, channel, exchange });

    consumeIdempotent({
        channel,
        queue,
        pool: pool_order,
        handler: async (client, data, routingKey) => {
            const status = routingKey === 'order.completed' ? 'completed' : 'failed';

            // Only move pending -> final; never overwrite a final state.
            const res = await client.query(
                `UPDATE order_log SET status = $1 WHERE id = $2 AND status = 'pending'`,
                [status, data.order_id]
            );

            console.log(
                `[Order] ${data.order_id} -> ${status}` +
                (res.rowCount === 0 ? ' (already final, ignored)' : '') +
                (data.reason ? ` | ${data.code}: ${data.reason}` : '')
            );
        },
    });
};

main();
