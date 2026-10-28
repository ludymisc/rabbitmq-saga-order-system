// Shared helpers for the outbox pattern.
// Assumes each service DB has:
//   outbox(id, message_id, aggregate_id, event_type, payload, created_at, published_at)
//   processed_messages(message_id PRIMARY KEY, processed_at)
// event_type holds the RabbitMQ routing key (e.g. 'order.created').

const { randomUUID } = require('crypto');

// Run fn(client) inside BEGIN/COMMIT. Rolls back on any error.
async function withTransaction(pool, fn) {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        const result = await fn(client);
        await client.query('COMMIT');
        return result;
    } catch (err) {
        await client.query('ROLLBACK').catch(() => {});
        throw err;
    } finally {
        client.release();
    }
}

// Must be called with the SAME client as the business write.
async function writeOutbox(client, aggregateId, routingKey, payload) {
    const messageId = randomUUID();
    await client.query(
        `INSERT INTO outbox (message_id, aggregate_id, event_type, payload)
         VALUES ($1, $2, $3, $4)`,
        [messageId, aggregateId, routingKey, JSON.stringify(payload)]
    );
    return messageId;
}

// Returns true the first time a message_id is seen, false for duplicates.
async function markProcessed(client, messageId) {
    const res = await client.query(
        `INSERT INTO processed_message (message_id)
         VALUES ($1) ON CONFLICT (message_id) DO NOTHING`,
        [messageId]
    );
    return res.rowCount === 1;
}

// Publish and wait for the broker's confirm. `channel` must be a ConfirmChannel.
function publishConfirmed(channel, exchange, routingKey, content, options) {
    return new Promise((resolve, reject) => {
        channel.publish(exchange, routingKey, content, options, (err) =>
            err ? reject(err) : resolve()
        );
    });
}

// Polls the outbox and publishes unsent rows. Safe to run multiple instances.
function startRelay({ pool, channel, exchange, intervalMs = 500, batchSize = 50 }) {
    let busy = false;

    const tick = async () => {
        if (busy) return;
        busy = true;
        let client;
        try {
            client = await pool.connect();
            await client.query('BEGIN');
            const { rows } = await client.query(
                `SELECT id, message_id, event_type, payload
                 FROM outbox
                 WHERE published_at IS NULL
                 ORDER BY id
                 LIMIT $1
                 FOR UPDATE SKIP LOCKED`,
                [batchSize]
            );

            for (const row of rows) {
                await publishConfirmed(
                    channel,
                    exchange,
                    row.event_type,
                    Buffer.from(JSON.stringify(row.payload)),
                    {
                        persistent: true,
                        messageId: row.message_id,
                        contentType: 'application/json',
                    }
                );
                await client.query(
                    `UPDATE outbox SET published_at = now() WHERE id = $1`,
                    [row.id]
                );
                console.log(`[Relay] published ${row.event_type} (${row.message_id})`);
            }
            await client.query('COMMIT');
        } catch (err) {
            if (client) await client.query('ROLLBACK').catch(() => {});
            console.error('[Relay] error, will retry next tick:', err.message);
        } finally {
            if (client) client.release();
            busy = false;
        }
    };

    const timer = setInterval(tick, intervalMs);
    return () => clearInterval(timer);
}

// Consumes a queue with: dedupe + handler + (any outbox writes) in ONE transaction,
// and ack only after commit.
function consumeIdempotent({ channel, queue, pool, handler }) {
    return channel.consume(
        queue,
        async (msg) => {
            if (msg === null) return;
            try {
                const messageId = msg.properties.messageId;
                if (!messageId) throw new Error('message has no messageId');

                const payload = JSON.parse(msg.content.toString());
                const routingKey = msg.fields.routingKey;

                await withTransaction(pool, async (client) => {
                    const isNew = await markProcessed(client, messageId);
                    if (!isNew) {
                        console.log(`[${queue}] duplicate ${messageId}, skipped`);
                        return;
                    }
                    await handler(client, payload, routingKey);
                });

                channel.ack(msg);
            } catch (err) {
                console.error(`[${queue}] failed:`, err.message);
                // Requeue once; if it fails again on redelivery, drop it
                // (replace with a dead-letter queue later).
                channel.nack(msg, false, !msg.fields.redelivered);
            }
        },
        { noAck: false }
    );
}

module.exports = {
    withTransaction,
    writeOutbox,
    markProcessed,
    startRelay,
    consumeIdempotent,
};
