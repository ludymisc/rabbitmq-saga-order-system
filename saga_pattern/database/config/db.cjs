const { Pool } = require('pg');
const dotenv = require('dotenv');

dotenv.config({path: '../.env'})

const pool_order = new Pool({
    connectionString: process.env.DB_ORDER_URL
})
const pool_inventory = new Pool({
    connectionString: process.env.DB_INVENTORY_URL
})
const pool_payment = new Pool({
    connectionString: process.env.DB_PAYMENT_URL
})

module.exports = {
    pool_order, pool_inventory, pool_payment
}
