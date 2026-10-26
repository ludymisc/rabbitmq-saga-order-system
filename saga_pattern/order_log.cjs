const amqp = require('amqplib');
const {pool_order} = require('./database/config/db.cjs');

const main = async() => {
    const connection = await amqp.connect("amqp://localhost");
    const channel = await connection.createChannel();
    const exchange = "market";
    const queue = "order";

    await channel.assertExchange(exchange, 'topic', {
        durable: true,
    });

    const q = await channel.assertQueue(queue, {
        exclusive: false,
    });

    const routingKeys = ['order.place', 'order.completed', 'order.failed'];

    for (const key of routingKeys) {
        await channel.bindQueue(q.queue, exchange, key);
        console.log(`[Setup] Queue '${q.queue}' berhasil di-bind ke key: '${key}'`);
    };

    channel.consume(q.queue, async (msg) => {
        if(msg === null) return;
        const routingKey = msg.fields.routingKey;
        const rawMessage = msg.content.toString();
        console.log("[Order] [*] %s: %s", routingKey, rawMessage);

        if (routingKey === 'order.place') {
            const orderData = JSON.parse(rawMessage);
            const targetItem = orderData.items[0].item_name;
            const requestQty = orderData.items[0].quantity;
            
            const query = `
            INSERT INTO order_log(name, quantity) 
            VALUES($1, $2) 
            ON CONFLICT (id) DO NOTHING 
            RETURNING id`;
            const result = await pool_order.query(query, [targetItem, requestQty]);
            const orderId = result.rows[0].id;

            if (result.rowCount === 0) {
                console.log(`[Order] Order dengan ID ${orderId} sudah pernah diproses. Pesan duplikat diabaikan.`);
                
                channel.ack(msg);
                return;
            }

            const enriched = { ...orderData, order_id: orderId};
            channel.publish(exchange, 'order.created', Buffer.from(JSON.stringify(enriched)));
        }
    channel.ack(msg);
    }, {
        noAck: false,
    });
}

main();