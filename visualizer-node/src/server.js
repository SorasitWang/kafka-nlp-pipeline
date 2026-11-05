import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { router as statusRouter, refreshSnapshot } from "./routes/status.js";
import { VISUALIZER_PORT, POLL_INTERVAL_MS } from "./config.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();

app.use("/api", statusRouter);
app.use(express.static(path.join(__dirname, "..", "public")));

await refreshSnapshot();
setInterval(refreshSnapshot, POLL_INTERVAL_MS);

app.listen(VISUALIZER_PORT, () => {
  console.log(`visualizer listening on http://localhost:${VISUALIZER_PORT} (polling every ${POLL_INTERVAL_MS}ms)`);
});
