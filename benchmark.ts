import amqp from "amqplib";

const QUEUE_IN = "url_checks";
const EXCHANGE = "check_results_exchange";
const TIMEOUT_MS = 60_000;

const DEFAULT_URLS = [
  "https://google.com",
  "https://github.com",
  "https://cloudflare.com",
  "https://example.com",
  "https://bun.sh",
  "https://this-does-not-exist-xyzabc123456.com",
];

const args = process.argv.slice(2);
const count = parseInt(args[0] ?? "10");
const customUrls = args[1]?.split(",").map((u) => u.trim());

function buildQueue(n: number): string[] {
  const pool = customUrls ?? DEFAULT_URLS;
  return Array.from({ length: n }, (_, i) => pool[i % pool.length]);
}

type Result = {
  url: string;
  status: "up" | "down";
  responseTime: number;
  e2eLatency: number;
};

function stat(arr: number[]) {
  const sorted = [...arr].sort((a, b) => a - b);
  const sum = sorted.reduce((a, b) => a + b, 0);
  return {
    avg: sum / sorted.length,
    p50: sorted[Math.floor(sorted.length * 0.5)],
    p95: sorted[Math.floor(sorted.length * 0.95)],
    min: sorted[0],
    max: sorted[sorted.length - 1],
  };
}

async function main() {
  const urls = buildQueue(count);
  console.log(`\nStarting benchmark: ${count} URL checks`);
  console.log(`Checker must be running and connected to RabbitMQ.\n`);

  const connection = await amqp.connect("amqp://localhost");
  const ch = await connection.createChannel();

  await ch.assertQueue(QUEUE_IN, { durable: true });
  await ch.assertExchange(EXCHANGE, "fanout", { durable: true });
  const { queue: resultQueue } = await ch.assertQueue("", { exclusive: true });
  ch.bindQueue(resultQueue, EXCHANGE, "");

  const results: Result[] = [];

  let finish!: () => void;
  const done = new Promise<void>((r) => (finish = r));

  ch.consume(
    resultQueue,
    (msg) => {
      if (!msg) return;
      const data = JSON.parse(msg.content.toString());
      results.push({
        url: data.url,
        status: data.status,
        responseTime: data.responseTime,
        e2eLatency: Date.now() - data.timestamp,
      });
      ch.ack(msg);
      process.stdout.write(`\rReceived: ${results.length}/${count}`);
      if (results.length >= count) finish();
    },
    { noAck: false }
  );

  const startTime = Date.now();

  for (const url of urls) {
    ch.sendToQueue(
      QUEUE_IN,
      Buffer.from(JSON.stringify({ url, timestamp: Date.now() })),
      { persistent: true }
    );
  }

  const timer = setTimeout(() => {
    console.warn(
      `\n\nTimeout after ${TIMEOUT_MS / 1000}s — only ${results.length}/${count} results received`
    );
    finish();
  }, TIMEOUT_MS);

  await done;
  clearTimeout(timer);

  const elapsed = (Date.now() - startTime) / 1000;

  await ch.close();
  await connection.close();

  if (results.length === 0) {
    console.error("\nNo results received. Is the checker service running?");
    process.exit(1);
  }

  const e2e = stat(results.map((r) => r.e2eLatency));
  const http = stat(results.map((r) => r.responseTime));
  const up = results.filter((r) => r.status === "up").length;
  const down = results.length - up;

  const line = "─".repeat(46);
  const row = (label: string, value: string) =>
    console.log(`  ${label.padEnd(22)}${value}`);

  console.log(`\n\n${line}`);
  console.log("  BENCHMARK RESULTS");
  console.log(line);
  row("URLs sent:", String(count));
  row("Results received:", String(results.length));
  row("Total time:", `${elapsed.toFixed(2)}s`);
  row("Throughput:", `${(results.length / elapsed).toFixed(2)} URLs/sec`);
  console.log(line);
  console.log("  End-to-end latency  (enqueue → result received)");
  row("  avg:", `${e2e.avg.toFixed(0)}ms`);
  row("  p50:", `${e2e.p50}ms`);
  row("  p95:", `${e2e.p95}ms`);
  row("  min:", `${e2e.min}ms`);
  row("  max:", `${e2e.max}ms`);
  console.log(line);
  console.log("  HTTP response time  (checker fetch only)");
  row("  avg:", `${http.avg.toFixed(0)}ms`);
  row("  p50:", `${http.p50}ms`);
  row("  p95:", `${http.p95}ms`);
  row("  min:", `${http.min}ms`);
  row("  max:", `${http.max}ms`);
  console.log(line);
  row("Up:", `${up}  (${((up / results.length) * 100).toFixed(0)}%)`);
  row("Down:", `${down}  (${((down / results.length) * 100).toFixed(0)}%)`);
  console.log(`${line}\n`);

  process.exit(0);
}

main().catch((err) => {
  console.error("\nBenchmark failed:", err.message);
  process.exit(1);
});
