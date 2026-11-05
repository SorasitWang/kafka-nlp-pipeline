from transformers import pipeline

MODEL_NAME = "unitary/toxic-bert"
TOXIC_THRESHOLD = 0.5

_classifier = None


def _get_classifier():
    global _classifier
    if _classifier is None:
        _classifier = pipeline(
            "text-classification",
            model=MODEL_NAME,
            top_k=None,
            function_to_apply="sigmoid",
        )
    return _classifier


def classify_toxicity(text: str):
    results = _get_classifier()(text, truncation=True)[0]
    top = max(results, key=lambda r: r["score"])
    is_toxic = top["score"] > TOXIC_THRESHOLD
    return top["label"], float(top["score"]), is_toxic
