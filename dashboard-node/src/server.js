import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import { router as resultsRouter } from "./routes/results.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();

app.use("/api", resultsRouter);
app.use(express.static(path.join(__dirname, "..", "public")));

const port = process.env.DASHBOARD_PORT || 3000;
app.listen(port, () => {
  console.log(`dashboard listening on http://localhost:${port}`);
});
