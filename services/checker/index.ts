// services/checker/index.ts
import amqp from "amqplib";

async function startChecker() {
    try {
        const connection = await amqp.connect("amqp://localhost");
        const channel = await connection.createChannel();

        const EXCHANGE_NAME = "check_results_exchange";
        const QUEUE_IN = "url_checks";

        await channel.assertQueue(QUEUE_IN, { durable: true });
        await channel.assertExchange(EXCHANGE_NAME, 'fanout', { durable: true });


        channel.prefetch(10);

        channel.consume(QUEUE_IN, async (msg) => {
            if (msg !== null) {
                const content = JSON.parse(msg.content.toString());
                console.log(`[Checking] ${content.url}`);

                const start = performance.now();
                let status: 'up' | 'down' = 'up';
                let statusCode = 0;

                try {
                    const response = await fetch(content.url, { signal: AbortSignal.timeout(5000) });
                    statusCode = response.status;
                    status = response.ok ? 'up' : 'down';
                } catch (e) {
                    status = 'down';
                }

                const duration = Math.round(performance.now() - start);


                const result = {
                    ...content,
                    status,
                    statusCode,
                    responseTime: duration
                };

                channel.publish(EXCHANGE_NAME, '', Buffer.from(JSON.stringify(result)));

                console.log(`[Finished] ${content.url} - ${status} (${duration}ms)`);
                channel.ack(msg);
            }
        }, { noAck: false });

    } catch (error) {
        console.error("Error:", error);
    }
}

startChecker();