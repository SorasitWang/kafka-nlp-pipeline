from transformers import pipeline

MODEL_NAME = "distilbert-base-uncased-finetuned-sst-2-english"

_classifier = None


def _get_classifier():
    global _classifier
    if _classifier is None:
        _classifier = pipeline("sentiment-analysis", model=MODEL_NAME)
    return _classifier


def classify_sentiment(text: str):
    result = _get_classifier()(text, truncation=True)[0]
    return result["label"], float(result["score"])
