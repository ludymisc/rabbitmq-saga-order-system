const amqp = require('amqplib');
const {
    pool_order, 
    pool_inventory, 
    pool_payment
} = require('./database/config/db.cjs');

const main = async() => {
    const connection = await amqp.connect("amqp://localhost");
    const channel = await connection.createChannel();
    const exchange = "market";
    const queue = "payment";

    await channel.assertExchange(exchange, 'topic', {
        durable: true,
    });

    const q = await channel.assertQueue(queue, {
        exclusive: false,
    });

    await channel.bindQueue(q.queue, exchange, 'payment.request');
    
    channel.consume(q.queue, async (msg) => {
        if (msg !== null) {
            console.log(" [x] %s: %s", msg.fields.routingKey, msg.content.toString());
            try{
                const rawMessage = msg.content.toString();
                const orderData = JSON.parse(rawMessage);

                const targetItem = orderData.items[0].item_name;
                const requestQty = orderData.items[0].quantity;
                const orderId = orderData.order_id;

                console.log(`[Payment] waiting payment for order : {item: ${targetItem}, qty: ${requestQty}}`);

                const isSuccess = Math.random() < 0.7;
                const status = isSuccess ? 'success' : 'failed';

                console.log(`[Payment] proses payment ${status}, meneruskan pesan ke order_log terminal`);

                const insertData = await pool_payment.query(`INSERT INTO payment_log(order_id, status) 
                    VALUES($1, $2) ON CONFLICT (order_id) DO NOTHING RETURNING order_id`,[orderId, status]);   
                if(insertData.rowCount === 0) {
                    console.log(`[Payment] order ${orderId} already processed, skipping`);
                    return;
                }

                const orderStatus = isSuccess? 'completed' : 'failed';
                await pool_order.query(`
                    UPDATE order_log SET status = $1 WHERE id = $2`, 
                    [orderStatus, orderId]);

                const key = isSuccess ? 'order.completed' : 'order.failed'
                channel.publish(exchange, key, 
                    Buffer.from(JSON.stringify({ order_id: orderId, items: orderData.items })));
            }catch(err) {
                console.error(err.message);
            }
        }
        channel.ack(msg);
    }, {
        noAck: false,
    });
}

main();