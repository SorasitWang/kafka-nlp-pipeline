import pg from "pg";
import "dotenv/config";

const { Pool } = pg;

export const pool = new Pool({
  host: process.env.POSTGRES_HOST || "localhost",
  port: Number(process.env.POSTGRES_PORT || 5432),
  database: process.env.POSTGRES_DB || "textanalysis",
  user: process.env.POSTGRES_USER || "textanalysis",
  password: process.env.POSTGRES_PASSWORD || "textanalysis",
});
