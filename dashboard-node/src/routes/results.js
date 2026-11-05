import { Router } from "express";
import { pool } from "../db.js";

export const router = Router();

// Joined per-message view: latest sentiment/topic/toxicity result side by side.
router.get("/messages", async (req, res) => {
  const limit = Math.min(Number(req.query.limit) || 50, 200);
  const { rows } = await pool.query(
    `SELECT m.id, m.text, m.produced_at,
            s.label AS sentiment_label, s.score AS sentiment_score,
            t.top_label AS topic_label, t.top_score AS topic_score,
            x.is_toxic, x.toxicity_score,
            n.entities AS ner_entities, n.entity_count AS ner_entity_count
     FROM messages m
     LEFT JOIN sentiment_results s ON s.message_id = m.id
     LEFT JOIN topic_results t ON t.message_id = m.id
     LEFT JOIN toxicity_results x ON x.message_id = m.id
     LEFT JOIN ner_results n ON n.message_id = m.id
     ORDER BY m.inserted_at DESC
     LIMIT $1`,
    [limit]
  );
  res.json(rows);
});

// Aggregate: sentiment label distribution.
router.get("/stats/sentiment", async (req, res) => {
  const { rows } = await pool.query(
    `SELECT label, COUNT(*)::int AS count FROM sentiment_results GROUP BY label ORDER BY count DESC`
  );
  res.json(rows);
});

// Aggregate: topic label distribution.
router.get("/stats/topics", async (req, res) => {
  const { rows } = await pool.query(
    `SELECT top_label AS label, COUNT(*)::int AS count FROM topic_results GROUP BY top_label ORDER BY count DESC`
  );
  res.json(rows);
});

// Aggregate: toxicity ratio.
router.get("/stats/toxicity", async (req, res) => {
  const { rows } = await pool.query(
    `SELECT is_toxic, COUNT(*)::int AS count FROM toxicity_results GROUP BY is_toxic`
  );
  res.json(rows);
});

// Aggregate: entity-type occurrence distribution (one message can contribute
// multiple entities, so this counts entity mentions, not messages).
router.get("/stats/ner", async (req, res) => {
  const { rows } = await pool.query(
    `SELECT e->>'label' AS label, COUNT(*)::int AS count
     FROM ner_results, jsonb_array_elements(entities) AS e
     GROUP BY e->>'label'
     ORDER BY count DESC`
  );
  res.json(rows);
});
