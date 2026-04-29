import amqp from "amqplib";

const SITES_FILE = "./sites.txt";

async function loadSites(): Promise<string[]> {
    const text = await Bun.file(SITES_FILE).text();
    return text.split(",").map((u) => u.trim()).filter(Boolean);
}

async function startScheduler() {
    try {
        const connection = await amqp.connect("amqp://localhost");
        const channel = await connection.createChannel();

        const QUEUE_NAME = "url_checks";
        await channel.assertQueue(QUEUE_NAME, { durable: true });

        const scheduleChecks = async () => {
            const urls = await loadSites();

            for (const url of urls) {
                const message = JSON.stringify({ url, timestamp: Date.now() });
                channel.sendToQueue(QUEUE_NAME, Buffer.from(message), { persistent: true });
                console.log(`[Sent] → ${url}`);
            }
        };

        setInterval(scheduleChecks, 10000);

    } catch (error) {
        console.error("Error:", error);
    }
}

startScheduler();
