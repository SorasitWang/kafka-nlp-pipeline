import pg from "pg";
import {
  POSTGRES_HOST,
  POSTGRES_PORT,
  POSTGRES_DB,
  POSTGRES_USER,
  POSTGRES_PASSWORD,
} from "./config.js";

const { Pool } = pg;

export const pool = new Pool({
  host: POSTGRES_HOST,
  port: POSTGRES_PORT,
  database: POSTGRES_DB,
  user: POSTGRES_USER,
  password: POSTGRES_PASSWORD,
});

// Recent throughput + last-write time for one service's results table.
export async function fetchTableActivity(table, recentWindowSeconds) {
  const { rows } = await pool.query(
    `SELECT
       count(*) FILTER (WHERE processed_at > now() - $1 * interval '1 second')::int AS recent_count,
       max(processed_at) AS last_processed_at,
       count(*)::int AS total_count
     FROM ${table}`,
    [recentWindowSeconds]
  );
  return rows[0];
}
