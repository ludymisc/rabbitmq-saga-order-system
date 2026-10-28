const { Command } = require('commander');
const { pool_order } = require('./database/config/db.cjs');
const { withTransaction, writeOutbox } = require('./outbox.cjs');

const program = new Command();

program
    .name('order-cli')
    .description('CLI interaktif untuk membuat pesanan')
    .version('1.0.0')
    .requiredOption('-i, --item <name>', 'Nama barang yang dibeli (misal: SEPATU)')
    .requiredOption('-q, --qty <number>', 'Jumlah kuantitas', '1');

program.parse(process.argv);
const options = program.opts();

const main = async () => {
    const itemName = options.item.toLowerCase();
    const quantity = parseInt(options.qty, 10);

    if (!Number.isInteger(quantity) || quantity <= 0) {
        console.error('Quantity harus bilangan bulat positif');
        process.exit(1);
    }

    // The order row and the order.created event are saved atomically.
    // Nothing talks to RabbitMQ here; the relay in order_log.cjs publishes it.
    const orderId = await withTransaction(pool_order, async (client) => {
        const { rows } = await client.query(
            `INSERT INTO order_log(name, quantity) VALUES($1, $2) RETURNING id`,
            [itemName, quantity]
        );
        const id = rows[0].id;

        await writeOutbox(client, id, 'order.created', {
            order_id: id,
            items: [{ item_name: itemName, quantity }],
        });
        return id;
    });
 
    console.log(`Berhasil membuat pesanan ${orderId}!`);
    console.log(`Barang: ${itemName.toUpperCase()}`);
    console.log(`Jumlah: ${quantity}\n`);

    await pool_order.end();
};

main().catch((err) => {
    console.error('Gagal membuat pesanan:', err.message);
    process.exit(1);
});
