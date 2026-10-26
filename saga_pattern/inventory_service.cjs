const amqp = require('amqplib');
const {pool_inventory, pool_order, pool_payment} = require('./database/config/db.cjs')

const main = async() => {
    const connection = await amqp.connect("amqp://localhost");
    const channel = await connection.createChannel();
    const exchange = "market";
    const queue = "inventory"

    await channel.assertExchange(exchange, 'topic', {
        durable: true,
    });

    const q = await channel.assertQueue(queue, {
        exclusive: false,
    });

    const routingKeys = ['order.created', 'order.failed'];

    
    channel.consume(q.queue, async(msg) => {
        if(msg !== null) {
            try{
                const routingKey = msg.fields.routingKey
                const rawMessage = msg.content.toString();
                const orderData = JSON.parse(rawMessage);

                const orderId = orderData.order_id;
                const itemName = orderData.items[0].item_name;
                const itemQty = orderData.items[0].quantity;
                console.log(orderData)

                console.log(`[Inventory] received from order : {item: ${itemName}, qty: ${itemQty}}`);

                if (routingKey === 'order.failed') {
                    console.log(`[Inventory] ${orderId} payment faied, restoring ${itemQty} of ${itemName}`);
                    await pool_inventory.query(
                        `UPDATE inventory SET stock = stock + $1 WHERE name = $2`,
                        [itemQty, itemName]
                    );

                    console.log(`[Inventory] ${orderId} stock restored`);
                    channel.ack(msg);
                    return;
                }

                const query = `SELECT * FROM inventory WHERE name = $1`;
                const result = await pool_inventory.query(query, [itemName]);

                const no_item_message = JSON.stringify({
                    status: "error",
                    code: "OUT_OF_STOCK",
                    message: `[Inventory] The stock of item (${itemName}) is not enough for your order`
                });
                const no_stock_message = JSON.stringify({
                    status: "error",
                    code: "ITEM_UNAVAILABLE",
                    message: `[Inventory] The stock of item you are looking for (${itemName}) is not enough for your order`
                });

                if(result.rows.length === 0) {
                    await pool_order.query(`UPDATE order_log SET status = $1 WHERE id = $2`, ['failed', orderId]);
                    channel.publish(exchange, 'order.failed', Buffer.from(no_item_message.toString()),
                    {persistent: true});
                } else {
                    const currentStock = result.rows[0].stock;
                    console.log("Barang ditemukan, memeriksa stock...");

                    if(currentStock >= itemQty) {
                        console.log(`[Inventory] ${orderId}stock mencukupi, reservasi akan dilakukan`);
                        const query = `UPDATE inventory SET stock = stock - $1 WHERE name = $2`;
                        await pool_inventory.query(query, [itemQty, itemName]);
                        channel.publish(exchange, 'payment.request', Buffer.from(rawMessage),
                        {persistent: true});
                    } else {
                        console.log(no_stock_message);
                        await pool_order.query(`UPDATE order_log SET status = $1 WHERE id = $2`, ['failed', orderId]);
                        channel.publish(exchange, 'order.failed', Buffer.from(no_stock_message.toString()),
                        {persistent: true});
                    }
                }
                channel.ack(msg);
            } catch (err) {
                console.error(err.message);
                channel.nack(msg, false, false);
            }
        }
    })
}
main();