import "dotenv/config";

export const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");
export const TOPIC_NAME = process.env.TOPIC_NAME || "text-stream";

export const POSTGRES_HOST = process.env.POSTGRES_HOST || "localhost";
export const POSTGRES_PORT = Number(process.env.POSTGRES_PORT || 5432);
export const POSTGRES_DB = process.env.POSTGRES_DB || "textanalysis";
export const POSTGRES_USER = process.env.POSTGRES_USER || "textanalysis";
export const POSTGRES_PASSWORD = process.env.POSTGRES_PASSWORD || "textanalysis";

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
