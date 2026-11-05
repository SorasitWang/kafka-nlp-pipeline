from transformers import pipeline

MODEL_NAME = "valhalla/distilbart-mnli-12-1"

CANDIDATE_LABELS = [
    "electronics",
    "books & media",
    "home & kitchen",
    "other",
]

_classifier = None


def _get_classifier():
    global _classifier
    if _classifier is None:
        _classifier = pipeline("zero-shot-classification", model=MODEL_NAME)
    return _classifier


def classify_topic(text: str):
    result = _get_classifier()(text, CANDIDATE_LABELS, truncation=True)
    all_labels = [
        {"label": label, "score": float(score)}
        for label, score in zip(result["labels"], result["scores"])
    ]
    return result["labels"][0], float(result["scores"][0]), all_labels
