import argparse
import os
import pandas as pd
import torch
from sklearn.model_selection import train_test_split
from torch.utils.data import Dataset
from transformers import Seq2SeqTrainingArguments
from transformers import DataCollatorForSeq2Seq
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from transformers import Seq2SeqTrainer
import logging
import mlflow


parser = argparse.ArgumentParser()
parser.add_argument("data_path",
                    help="path to data in s3 to be used in training")
parser.add_argument("model_path", help="path to the model name to be trained")


if __name__ == "__main__":
    args = parser.parse_args()

    experiment_name = f'{args.model_path.split("/")[1]}_training'
    # os.environ["DISABLE_MLFLOW_INTEGRATION"] = "TRUE"
    os.environ["MLFLOW_TRACKING_URI"] = "http://62.84.125.63:8000"
    os.environ["MLFLOW_EXPERIMENT_NAME"] = experiment_name
    os.environ["MLFLOW_FLATTEN_PARAMS"] = "True"
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
    os.environ["HF_MLFLOW_LOG_ARTIFACTS"] = "True"
    os.environ["AWS_DEFAULT_REGION"] = "ru-central1"
    os.environ["AWS_ACCESS_KEY_ID"] = os.environ['s3Key']
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ['s3secret']

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)-15s %(message)s")
    logger = logging.getLogger()

    # read files in s3
    logger.info("Reading s3 files...")

    storage_options = {
        "client_kwargs": {"endpoint_url": "https://storage.yandexcloud.net"},
        "s3": {"anon": True},
    }
    df = pd.read_parquet(
        args.data_path, engine="pyarrow", storage_options=storage_options
    )

    train_df, val_df = train_test_split(
        df[["zodiac", "horo"]], test_size=0.3, random_state=42
    )
    train_df = train_df.reset_index(drop=True)
    val_df = val_df.reset_index(drop=True)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    train_df = train_df.rename(columns={"zodiac": "source_text",
                                        "horo": "target_text"})
    val_df = val_df.rename(columns={"zodiac": "source_text",
                                    "horo": "target_text"})

    class AdditionDataset(Dataset):
        """ """

        def __init__(self, sentences, targets, tokenizer, max_len):
            self.tokenizer = tokenizer
            self.max_len = max_len
            self.sentences = sentences
            self.targets = targets

        def __len__(self):
            return len(self.sentences)

        def __getitem__(self, index):
            sentence = str(self.sentences[index])
            sentence = " ".join(sentence.split())
            target = self.targets[index]

            return {
                "input_ids": self.tokenizer(sentence)["input_ids"],
                "labels": self.tokenizer(target)["input_ids"],
            }

    tokenizer = AutoTokenizer.from_pretrained(args.model_path)
    model = AutoModelForSeq2SeqLM.from_pretrained(args.model_path)
    train_dataset = AdditionDataset(
        train_df["source_text"].tolist(),
        train_df["target_text"].tolist(),
        tokenizer,
        500,
    )
    test_dataset = AdditionDataset(
        val_df["source_text"].tolist(),
        val_df["target_text"].tolist(),
        tokenizer,
        500
    )

    # training
    # Hugging Face repository id
    repository_id = f"{args.model_path.split('/')[1]}_horoscopes-generator_v2"

    from huggingface_hub import HfFolder

    HfFolder.save_token(os.environ["hugging_face_login"])

    args = Seq2SeqTrainingArguments(
        output_dir=repository_id,
        evaluation_strategy="epoch",
        learning_rate=2e-4,
        per_device_train_batch_size=16,
        per_device_eval_batch_size=16,
        weight_decay=0.01,
        num_train_epochs=1,
        predict_with_generate=True,
        push_to_hub=False,
        save_total_limit=1,
        logging_steps=500,
        save_strategy="epoch",
    )

    data_collator = DataCollatorForSeq2Seq(tokenizer, model=model)

    trainer = Seq2SeqTrainer(
        model,
        args,
        train_dataset=train_dataset,
        eval_dataset=test_dataset,
        data_collator=data_collator,
        tokenizer=tokenizer,
    )
    logger.info("Starting training...")
    trainer.train()
    mlflow.end_run()
    trainer.create_model_card()
    logger.info("Pushing model to hub...")
    trainer.push_to_hub()
    logger.info("All done")
