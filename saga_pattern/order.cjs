const amqp = require('amqplib');
const pool_order = require('./database/config/db.cjs');
const { Command } = require('commander');

const program = new Command();

program
    .name('order-cli')
    .description('CLI interaktif untuk mengirim pesanan ke RabbitMQ')
    .version('1.0.0')
    .requiredOption('-i, --item <name>', 'Nama barang yang dibeli (misal: SEPATU)')
    .requiredOption('-q, --qty <number>', 'Jumlah kuantitas', '1');

program.parse(process.argv);
const options = program.opts();

const main = async() => {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();

    const exchange = 'market';
    const key = 'order.place';

    await channel.assertExchange(exchange, 'topic', {
        durable: true,
    });

    const orderData = {
        items:[
            {
                item_name: options.item.toLowerCase(),
                quantity: parseInt(options.qty)
            }
        ]
    };
    const messageBuffer = Buffer.from(JSON.stringify(orderData));
    channel.publish(exchange, key, messageBuffer);

    console.log(`Berhasil mengirim pesanan!`);
    console.log(`Barang: ${options.item.toUpperCase()}`);
    console.log(`Jumlah: ${options.qty}\n`);

    setTimeout(() => {
        connection.close();
        process.exit(0);
    }, 500);
}

main();