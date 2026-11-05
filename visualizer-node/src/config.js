import dotenv from "dotenv";
import { fileURLToPath } from "node:url";

dotenv.config({ path: fileURLToPath(new URL("../../.env", import.meta.url)) });

function required(key) {
  const val = process.env[key];
  if (!val) throw new Error(`${key} is not set; copy .env.example to .env`);
  return val;
}

export const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");
export const TOPIC_NAME = process.env.TOPIC_NAME || "text-stream";

export const POSTGRES_HOST = process.env.POSTGRES_HOST || "localhost";
export const POSTGRES_PORT = Number(process.env.POSTGRES_PORT || 5432);
export const POSTGRES_DB = required("POSTGRES_DB");
export const POSTGRES_USER = required("POSTGRES_USER");
export const POSTGRES_PASSWORD = required("POSTGRES_PASSWORD");

export const VISUALIZER_PORT = Number(process.env.VISUALIZER_PORT || 3001);
export const POLL_INTERVAL_MS = Number(process.env.VISUALIZER_POLL_MS || 2000);
export const HISTORY_LENGTH = 40;
export const RECENT_WINDOW_SECONDS = 15;

// One entry per consumer service: Kafka consumer group id <-> Postgres results table.
export const SERVICES = [
  { id: "sentiment", label: "Sentiment", groupId: "sentiment-service", resultsTable: "sentiment_results" },
  { id: "topic", label: "Topic", groupId: "topic-service", resultsTable: "topic_results" },
  { id: "toxicity", label: "Toxicity", groupId: "toxicity-service", resultsTable: "toxicity_results" },
  { id: "ner", label: "NER", groupId: "ner-service", resultsTable: "ner_results" },
];
