// services/notifier/index.ts
import amqp from "amqplib";

async function startNotifier() {
    try {
        const connection = await amqp.connect("amqp://localhost");
        const channel = await connection.createChannel();

        const QUEUE_IN = "check_results";

        await channel.assertQueue(QUEUE_IN, { durable: true });

        channel.consume(QUEUE_IN, (msg) => {
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