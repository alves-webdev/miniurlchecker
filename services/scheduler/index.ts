import amqp from "amqplib";
import { Database } from "bun:sqlite";

const db = new Database("uptime.sqlite");
db.run("CREATE TABLE IF NOT EXISTS sites (id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT)");
const seed = db.query("SELECT COUNT(*) as count FROM sites").get() as { count: number };
if (seed.count === 0) {
    db.run("INSERT INTO sites (url) VALUES (?), (?)", ["https://google.com", "https://github.com"]);
}

async function startScheduler() {
    try {
        const connection = await amqp.connect("amqp://localhost");
        const channel = await connection.createChannel();

        const QUEUE_NAME = "url_checks";
        await channel.assertQueue(QUEUE_NAME, { durable: true });

        const scheduleChecks = () => {
            const sites = db.query("SELECT * FROM sites").all() as { id: number, url: string }[];

            sites.forEach((site) => {
                const message = JSON.stringify({
                    id: site.id,
                    url: site.url,
                    timestamp: Date.now(),
                });

                channel.sendToQueue(QUEUE_NAME, Buffer.from(message), { persistent: true });
                console.log(`[Sent] → ${site.url}`);
            });
        };

        setInterval(scheduleChecks, 10000);

    } catch (error) {
        console.error("Error:", error);
    }
}

startScheduler();