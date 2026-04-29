// services/notifier/index.ts
import amqp from "amqplib";

async function startNotifier() {
    try {
        const EXCHANGE_NAME = "check_results_exchange";
        const connection = await amqp.connect("amqp://localhost");
        const channel = await connection.createChannel();

        await channel.assertExchange(EXCHANGE_NAME, 'fanout', { durable: true });

        const q = await channel.assertQueue("", { exclusive: true });
        channel.bindQueue(q.queue, EXCHANGE_NAME, '');

        channel.consume(q.queue, (msg) => {
            if (msg !== null) {
                const result = JSON.parse(msg.content.toString());

                if (result.status === 'down') {
                    console.error(`ALERT: ${result.url} is DOWN (Status: ${result.statusCode})`);
                } else {
                    console.log(`INFO: ${result.url} is UP (${result.responseTime}ms)`);
                }

                channel.ack(msg);
            }
        }, { noAck: false });

    } catch (error) {
        console.error("Error:", error);
    }
}

startNotifier();