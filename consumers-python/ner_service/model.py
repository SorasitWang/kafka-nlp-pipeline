from pathlib import Path

from transformers import pipeline

MODEL_PATH = Path(__file__).resolve().parent / "artifacts" / "ner-bert"
MODEL_NAME = "ner-conll03-distilbert-cased-v1"

_ner_pipeline = None


def _get_ner_pipeline():
    global _ner_pipeline
    if _ner_pipeline is None:
        if not MODEL_PATH.exists():
            raise RuntimeError(
                f"NER model artifact not found at {MODEL_PATH}; "
                "run `poetry run python -m ner_service.training.train_ner` first"
            )
        _ner_pipeline = pipeline(
            "token-classification",
            model=str(MODEL_PATH),
            tokenizer=str(MODEL_PATH),
            aggregation_strategy="simple",
        )
    return _ner_pipeline


def extract_entities(text: str):
    # TokenClassificationPipeline (unlike sentiment/toxicity's pipelines)
    # doesn't take a truncation= call kwarg; it auto-truncates to the
    # tokenizer's model_max_length instead.
    results = _get_ner_pipeline()(text)
    entities = []
    for r in results:
        start, end = r["start"], r["end"]
        entities.append(
            {
                "text": text[start:end] if start is not None and end is not None else r["word"],
                "label": r["entity_group"],
                "score": float(r["score"]),
                "start": int(start) if start is not None else None,
                "end": int(end) if end is not None else None,
            }
        )
    return entities
