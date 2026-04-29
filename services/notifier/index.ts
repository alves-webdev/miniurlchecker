import amqp from "amqplib";
import { Database } from "bun:sqlite";

const FAILURE_THRESHOLD = 3;

const db = new Database("./uptime.sqlite");

db.run(`
  CREATE TABLE IF NOT EXISTS check_results (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    url         TEXT    NOT NULL,
    status      TEXT    NOT NULL,
    status_code INTEGER NOT NULL DEFAULT 0,
    response_time INTEGER NOT NULL DEFAULT 0,
    checked_at  INTEGER NOT NULL
  )
`);

db.run(`
  CREATE TABLE IF NOT EXISTS url_state (
    url                  TEXT PRIMARY KEY,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    alerted              INTEGER NOT NULL DEFAULT 0
  )
`);

const insertResult = db.prepare(`
  INSERT INTO check_results (url, status, status_code, response_time, checked_at)
  VALUES (?, ?, ?, ?, ?)
`);

const getState = db.prepare<{ consecutive_failures: number; alerted: number }, [string]>(`
  SELECT consecutive_failures, alerted FROM url_state WHERE url = ?
`);

const upsertState = db.prepare(`
  INSERT OR REPLACE INTO url_state (url, consecutive_failures, alerted) VALUES (?, ?, ?)
`);

function handleResult(result: {
  url: string;
  status: "up" | "down";
  statusCode: number;
  responseTime: number;
  timestamp: number;
}) {
  insertResult.run(result.url, result.status, result.statusCode, result.responseTime, result.timestamp);

  const state = getState.get(result.url) ?? { consecutive_failures: 0, alerted: 0 };

  if (result.status === "up") {
    if (state.alerted) {
      console.log(`RECOVERED: ${result.url} is back UP (${result.responseTime}ms)`);
    } else {
      console.log(`INFO: ${result.url} is UP (${result.responseTime}ms)`);
    }
    upsertState.run(result.url, 0, 0);
    return;
  }

  const failures = state.consecutive_failures + 1;
  const shouldAlert = failures >= FAILURE_THRESHOLD && !state.alerted;
  upsertState.run(result.url, failures, shouldAlert || state.alerted ? 1 : 0);

  if (shouldAlert) {
    console.error(
      `ALERT: ${result.url} is DOWN — ${failures} consecutive failures (HTTP ${result.statusCode})`
    );
  } else if (!state.alerted) {
    console.warn(
      `WARNING: ${result.url} failed (${failures}/${FAILURE_THRESHOLD}, HTTP ${result.statusCode})`
    );
  }
}

async function startNotifier() {
  try {
    const EXCHANGE_NAME = "check_results_exchange";
    const connection = await amqp.connect("amqp://localhost");
    const channel = await connection.createChannel();

    await channel.assertExchange(EXCHANGE_NAME, "fanout", { durable: true });

    const q = await channel.assertQueue("", { exclusive: true });
    channel.bindQueue(q.queue, EXCHANGE_NAME, "");

    channel.consume(
      q.queue,
      (msg) => {
        if (!msg) return;
        const result = JSON.parse(msg.content.toString());
        handleResult(result);
        channel.ack(msg);
      },
      { noAck: false }
    );
  } catch (error) {
    console.error("Error:", error);
  }
}

startNotifier();
