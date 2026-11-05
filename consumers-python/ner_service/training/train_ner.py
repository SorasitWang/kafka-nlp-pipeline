"""One-time fine-tuning of a token-classification NER model on CoNLL-2003.

Run via: poetry run python -m ner_service.training.train_ner

Saves the resulting model + tokenizer to ner_service/artifacts/ner-bert/,
which ner_service/model.py loads for inference (no Hub access at runtime).
"""

import logging

import numpy as np
from datasets import load_dataset
from seqeval.metrics import classification_report, f1_score, precision_score, recall_score
from transformers import (
    AutoModelForTokenClassification,
    AutoTokenizer,
    DataCollatorForTokenClassification,
    Trainer,
    TrainingArguments,
)

from ..model import MODEL_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("train_ner")

# Backbone: DistilBERT is ~2x faster to fine-tune on CPU than bert-base-cased,
# at a small (~2-4 F1 point) accuracy cost on CoNLL-2003. Swap to
# "bert-base-cased" for higher accuracy if training time/RAM isn't a
# constraint (consider dropping per_device_train_batch_size to 8 if so).
BACKBONE_MODEL_NAME = "distilbert-base-cased"

RUNS_DIR = MODEL_PATH.parent.parent / "training" / "runs"


def load_conll03():
    # The canonical CoNLL-2003 dataset on the HF Hub is script-based, so it
    # requires trust_remote_code=True to run its loader script.
    return load_dataset("conll2003", trust_remote_code=True)


def main():
    dataset = load_conll03()
    label_names = dataset["train"].features["ner_tags"].feature.names
    id2label = dict(enumerate(label_names))
    label2id = {v: k for k, v in id2label.items()}

    tokenizer = AutoTokenizer.from_pretrained(BACKBONE_MODEL_NAME)

    def tokenize_and_align_labels(batch):
        tokenized = tokenizer(batch["tokens"], truncation=True, is_split_into_words=True)
        all_labels = []
        for i, tags in enumerate(batch["ner_tags"]):
            word_ids = tokenized.word_ids(batch_index=i)
            previous_word_id = None
            labels = []
            for word_id in word_ids:
                if word_id is None:
                    labels.append(-100)
                elif word_id != previous_word_id:
                    labels.append(tags[word_id])
                else:
                    labels.append(-100)
                previous_word_id = word_id
            all_labels.append(labels)
        tokenized["labels"] = all_labels
        return tokenized

    tokenized_dataset = dataset.map(tokenize_and_align_labels, batched=True, remove_columns=dataset["train"].column_names)

    model = AutoModelForTokenClassification.from_pretrained(
        BACKBONE_MODEL_NAME,
        num_labels=len(label_names),
        id2label=id2label,
        label2id=label2id,
    )
    data_collator = DataCollatorForTokenClassification(tokenizer=tokenizer)

    def compute_metrics(eval_pred):
        predictions, labels = eval_pred
        predictions = np.argmax(predictions, axis=2)

        true_labels = [
            [id2label[l] for l in label_row if l != -100]
            for label_row in labels
        ]
        true_predictions = [
            [id2label[p] for p, l in zip(pred_row, label_row) if l != -100]
            for pred_row, label_row in zip(predictions, labels)
        ]
        return {
            "precision": precision_score(true_labels, true_predictions),
            "recall": recall_score(true_labels, true_predictions),
            "f1": f1_score(true_labels, true_predictions),
        }

    training_args = TrainingArguments(
        output_dir=str(RUNS_DIR),
        num_train_epochs=3,
        per_device_train_batch_size=16,
        per_device_eval_batch_size=32,
        learning_rate=2e-5,
        weight_decay=0.01,
        eval_strategy="epoch",
        save_strategy="epoch",
        load_best_model_at_end=True,
        metric_for_best_model="f1",
        report_to="none",
        logging_steps=50,
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=tokenized_dataset["train"],
        eval_dataset=tokenized_dataset["validation"],
        data_collator=data_collator,
        tokenizer=tokenizer,
        compute_metrics=compute_metrics,
    )

    trainer.train()

    logger.info("evaluating on test split...")
    test_predictions, test_labels, _ = trainer.predict(tokenized_dataset["test"])
    test_predictions = np.argmax(test_predictions, axis=2)
    true_labels = [[id2label[l] for l in row if l != -100] for row in test_labels]
    true_predictions = [
        [id2label[p] for p, l in zip(pred_row, label_row) if l != -100]
        for pred_row, label_row in zip(test_predictions, test_labels)
    ]
    logger.info("test F1: %.4f", f1_score(true_labels, true_predictions))
    print(classification_report(true_labels, true_predictions))

    MODEL_PATH.mkdir(parents=True, exist_ok=True)
    trainer.save_model(str(MODEL_PATH))
    tokenizer.save_pretrained(str(MODEL_PATH))
    logger.info("saved fine-tuned model to %s", MODEL_PATH)


if __name__ == "__main__":
    main()
