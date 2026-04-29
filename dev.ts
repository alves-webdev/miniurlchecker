const services = [
  { name: "scheduler", path: "./services/scheduler/index.ts", color: "\x1b[36m" },
  { name: "checker",   path: "./services/checker/index.ts",   color: "\x1b[32m" },
  { name: "notifier",  path: "./services/notifier/index.ts",  color: "\x1b[35m" },
];

const reset = "\x1b[0m";

const processes = services.map((service) => {
  const prefix = `${service.color}[${service.name}]${reset} `;

  const proc = Bun.spawn(["bun", "--watch", service.path], {
    stdout: "pipe",
    stderr: "pipe",
  });

  const pipe = async (stream: ReadableStream, isErr = false) => {
    const reader = stream.getReader();
    const decoder = new TextDecoder();
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        const text = decoder.decode(value);
        for (const line of text.split("\n")) {
          if (line.trim()) (isErr ? console.error : console.log)(prefix + line);
        }
      }
    } finally {
      reader.releaseLock();
    }
  };

  pipe(proc.stdout);
  pipe(proc.stderr, true);

  return proc;
});

process.on("SIGINT", () => {
  processes.forEach((p) => p.kill());
  process.exit(0);
});

await Promise.all(processes.map((p) => p.exited));
