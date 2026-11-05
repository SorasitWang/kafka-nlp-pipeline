import { Router } from "express";
import { buildSnapshot } from "../status.js";

export const router = Router();

let cached = null;
let lastError = null;

export async function refreshSnapshot() {
  try {
    cached = await buildSnapshot();
    lastError = null;
  } catch (err) {
    lastError = err.message;
  }
}

router.get("/status", (req, res) => {
  if (!cached) {
    return res.status(503).json({ error: lastError || "not ready yet" });
  }
  res.json({ ...cached, error: lastError });
});
