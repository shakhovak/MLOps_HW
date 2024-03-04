import argparse
import os
import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from datetime import datetime
import mlflow
import logging
from sentence_transformers import SentenceTransformer
from scipy import sparse
from sklearn.metrics.pairwise import cosine_similarity
import os
import re
import time
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from transformers import pipeline

import warnings

warnings.filterwarnings("ignore")

parser = argparse.ArgumentParser()
parser.add_argument('model_name', help="model name to be trained")
parser.add_argument("model_path", help="path to the model name to be evaluated")


if __name__ == "__main__":
    args = parser.parse_args()

    # will use sampling strategy to generate every time new texts
    # To use sampling generation strategy: in transformers, we set do_sample=True
    #       and deactivate Top-K sampling (more on this later) via top_k=0.

    def generate_horo(model, tokenizer, zodiac, top_p, temperature):
        input_ids = tokenizer.encode(zodiac, return_tensors="pt")
        sample_output = model.generate(
            input_ids.to(device),
            do_sample=True,
            max_length=400,
            top_p=top_p,
            temperature=temperature,
            top_k=0,
            no_repeat_ngram_size=2,
            early_stopping=True,
        )

        if model_name == "T5":
            return tokenizer.decode(sample_output[0], skip_special_tokens=True)
        else:
            out = tokenizer.decode(sample_output[0][1:], skip_special_tokens=True)
            if "</s>" in out:
                out = out[: out.find("</s>")].strip()
            return out

    # Loading models for evaluation

    grammar_tokenizer = AutoTokenizer.from_pretrained(
        "RussianNLP/ruRoBERTa-large-rucola"
    )
    grammar_model = AutoModelForSequenceClassification.from_pretrained(
        "RussianNLP/ruRoBERTa-large-rucola"
    )
    grammer_pipe = pipeline(
        "text-classification", model=grammar_model, tokenizer=grammar_tokenizer
    )
    model_comparison = SentenceTransformer("sentence-transformers/LaBSE")

    sent_pipe = pipeline(
        "text-classification", model="seara/rubert-base-cased-russian-sentiment"
    )

    # function for evaluation
    def review_gen(model, tokenizer, lst, top_p, temperature):
        gener_horos = pd.DataFrame()

        for zodiac in lst:

            start = time.time()
            gen_text = re.sub(
                re.compile("[^а-яА-ЯЁё !.,:?;«»]"),
                "",
                generate_horo(
                    model=model,
                    tokenizer=tokenizer,
                    zodiac=zodiac,
                    top_p=top_p,
                    temperature=temperature,
                ),
            )
            end = time.time()
            gen_time = round(end - start, 4)

            symbols = [
                "..",
                ",.",
                "?.",
                " , ",
                " . ",
                " : ",
                ".:",
                ":.",
                "«. »",
                "??",
                "?!",
                ".?",
                "? .",
                "? .",
            ]
            for symb in symbols:
                gen_text = gen_text.replace(symb, "")
            for symb in symbols:
                gen_text = gen_text.replace(symb, "")

            gram_accept = grammer_pipe(gen_text)[0]
            gram_label = gram_accept["label"]

            if gram_label == "LABEL_1":
                label = 1
            else:
                label = 0

            sent_review = sent_pipe(gen_text)[0]
            sent_label = sent_review["label"]

            if sent_label == "negative":
                s_label = 0
            else:
                s_label = 1

            new_row = {
                "zodiac": zodiac,
                "gen_text": gen_text,
                "grm_label": label,
                "sentiment": s_label,
                "gen_time": gen_time,
            }

            gener_horos = pd.concat([gener_horos, pd.DataFrame([new_row])])

        sentences = gener_horos["gen_text"].tolist()
        embeddings = model_comparison.encode(sentences, normalize_embeddings=True)
        emb = sparse.csr_matrix(embeddings)
        sent_div = []
        for item in cosine_similarity(emb):
            sent_div.append(1 - item.mean())
        gener_horos["diver"] = sent_div
        generation_result = {
            "model_name": model_name,
            "params": f"temp={temperature},top_p={top_p}",
            "gramm_corr_%": round(
                len(gener_horos[gener_horos["grm_label"] == 1])
                / len(gener_horos)
                * 100,
                2,
            ),
            "diversity": round(gener_horos["diver"].mean() * 100, 2),
            "positiv_%": round(
                len(gener_horos[gener_horos["sentiment"] == 1])
                / len(gener_horos)
                * 100,
                2,
            ),
            "avg_gen_time": round(gener_horos["gen_time"].mean(), 4),
        }
        return gener_horos, generation_result

    # evaluating prompts
    lst = [
        "Лев",
        "Рыбы",
        "Козерог",
        "Телец",
        "Скорпион",
        "Близнецы",
        "Овен",
        "Весы",
        "Стрелец",
        "Рак",
        "Водолей",
        "Дева",
    ]
    # load pretrained model
    model_name = args.model_name
    model_path = args.model_path
    tokenizer = AutoTokenizer.from_pretrained(model_path)
    model = AutoModelForSeq2SeqLM.from_pretrained(model_path)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)

    # mlflow initiation
    logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
    logger = logging.getLogger()

    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
    os.environ["AWS_DEFAULT_REGION"] = "ru-central1"
    os.environ["AWS_ACCESS_KEY_ID"] = os.environ['s3Key']
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ['s3secret']

    mlflow.set_tracking_uri("http://62.84.125.63:8000")
    logger.info("tracking URI: %s", {mlflow.get_tracking_uri()})

    experiment_name = f"{model_name}_evaluate"
    try:
        experiment = mlflow.get_experiment_by_name(experiment_name)
        experiment_id = experiment.experiment_id
    except AttributeError:
        experiment_id = mlflow.create_experiment(
            experiment_name
        )
    mlflow.set_experiment(experiment_name)
    param_list = [(0.8, 0.7), (0.5, 0.5), (0.1, 0.2)]
    run_name = f"Run_{model_name}" + " " + str(datetime.now())
    logger.info("Scoring the model ...")
    for item in param_list:
        print_top_p = 'top_p='+str(item[0])
        print_temp = "temp=" + str(item[1])
        with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
            gener_horos, generation_result = review_gen(
                model, tokenizer, lst, top_p=item[0], temperature=item[1]
            )
            mlflow.log_param('top_p', item[0])
            mlflow.log_param("temperature", item[1])

            mlflow.log_metric("gramm_corr", generation_result["gramm_corr_%"])
            mlflow.log_metric("diversity", generation_result["diversity"])
            mlflow.log_metric("positivn", generation_result["positiv_%"])
            mlflow.log_metric("avg_gen_time", generation_result["avg_gen_time"])
            gener_horos.to_csv(f"{model_name}.csv")
            dataset = mlflow.data.from_pandas(
                gener_horos, source=f"{model_name}.csv"
            )
            mlflow.log_input(dataset, context="evaluation")
            mlflow.log_artifact(f"{model_name}.csv")

            mlflow.set_tags(
                tags={
                    "project": "horo_generation",
                    "model_family": f"{model_name}",
                    }
            )

    logger.info("Done.")
