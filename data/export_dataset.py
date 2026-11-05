"""One-time export: sample HuggingFace `amazon_polarity` reviews to JSONL for the Kafka producer.

Labels are intentionally dropped -- this project uses pretrained/off-the-shelf
NLP models downstream, not a model trained on this dataset's polarity labels.
"""

import json
import random
from datetime import datetime, timezone
from pathlib import Path

from datasets import load_dataset

SOURCE_DATASET = "amazon_polarity"
SAMPLE_SIZE = 3000
SEED = 42
OUTPUT_PATH = Path(__file__).parent / "dataset.jsonl"


def main():
    ds = load_dataset(SOURCE_DATASET, split="train")
    rng = random.Random(SEED)
    indices = rng.sample(range(len(ds)), SAMPLE_SIZE)

    now = datetime.now(timezone.utc).isoformat()
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        for i, idx in enumerate(indices):
            row = ds[idx]
            text = f"{row['title'].strip()}. {row['content'].strip()}".strip()
            record = {
                "id": f"{SOURCE_DATASET}-{i:06d}",
                "text": text,
                "source_dataset": SOURCE_DATASET,
                "exported_at": now,
            }
            f.write(json.dumps(record) + "\n")

    print(f"wrote {SAMPLE_SIZE} records to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
