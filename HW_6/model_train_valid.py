import pyspark
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

import os
import csv
import mlflow
import logging


from scipy.stats import shapiro
from scipy.stats import ttest_ind


def compare_distributions(scores_init, scores_tuned, alpha=0.01):
    pvalue = ttest_ind(scores_init, scores_tuned).pvalue
    if pvalue < alpha:
        result = "dist_DIFFERENT"
    else:
        result = "dist_IDENTICAL"
    return pvalue, result


def validate_model(
    predictions,
    evaluator,
    N_BOOTSTREPS=100,
    SAMPLE_SIZE=0.01,
):
    scores = []
    for i in range(N_BOOTSTREPS):
        sample = predictions.sample(SAMPLE_SIZE)
        areaUnderROC = evaluator.evaluate(sample)
        scores.append(areaUnderROC)
    stat, p = shapiro(scores)
    return scores, stat, p


def main():
    # spark session params
    app_name = "TrainValid"
    spark_ui_port = 4040  # Порт для spark ui

    spark = (
        pyspark.sql.SparkSession.builder.appName(app_name)
        .config("spark.executor.cores", "4")
        .config("spark.executor.memory", "4g")
        .config("spark.executor.instances", "6")
        .config("spark.default.parallelism", "48")
        .config("spark.driver.memory", "4g")  # Main процесс
        .config("spark.ui.port", spark_ui_port)
        .getOrCreate()
    )
    spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    # read data that already preprocessed
    path = "s3a://mlopsshakhova/airflow/processed/*.parquet"
    df = spark.read.parquet(path)

    # as dataset is inbalanced calculate weight of each class
    y_collect = df.select("tx_fraud").groupBy("tx_fraud").count().collect()
    bin_counts = {y["tx_fraud"]: y["count"] for y in y_collect}
    total = sum(bin_counts.values())
    n_labels = len(bin_counts)
    weights = {bin_: total / (n_labels * count) for bin_, count in bin_counts.items()}
    df = df.withColumn(
        "weight", F.when(F.col("tx_fraud") == 1.0, weights[1]).otherwise(weights[0])
    )

    # start trainin and logging to MLFlow
    logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
    logger = logging.getLogger()

    os.environ["AWS_ACCESS_KEY_ID"] = ""
    os.environ["AWS_SECRET_ACCESS_KEY"] = ""
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
    os.environ["AWS_DEFAULT_REGION"] = "ru-central1"
    mlflow.set_tracking_uri(
        f"http://10.128.0.30:8000"
    )  # change in case new VM is set up
    logger.info("tracking URI: %s", {mlflow.get_tracking_uri()})

    # set up eperiment in ML Flow
    experiment_name = "model_train_val_fin"
    try:
        experiment = mlflow.get_experiment_by_name(experiment_name)
        experiment_id = experiment.experiment_id
    except AttributeError:
        experiment_id = mlflow.create_experiment(
            experiment_name, artifact_location="s3://shakhmlflow/artifacts/"
        )
    mlflow.set_experiment(experiment_name)

    run_name = "Fraud detection model" + " " + str(datetime.now())

    # run ML flow
    with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
        # ===========================================================================
        # create feature vector
        logger.info("Feature building ...")
        train, test = df.randomSplit([0.7, 0.3])
        columns_to_scale = [
            "customer_id",
            "terminal_id",
            "tx_amount",
            "tx_time_seconds",
            "tx_time_days",
        ]

        featureArr = [(col + "_scaled") for col in columns_to_scale]
        assembler_1 = [
            VectorAssembler(inputCols=[col], outputCol=col + "_vec")
            for col in columns_to_scale
        ]
        scaler = [
            StandardScaler(
                inputCol=col + "_vec",
                outputCol=col + "_scaled",
                withMean=True,
                withStd=True,
            )
            for col in columns_to_scale
        ]
        assembler_2 = VectorAssembler(inputCols=featureArr, outputCol="features")
        lr = LogisticRegression(
            labelCol="tx_fraud",
            featuresCol="features",
            maxIter=10,
        )

        pipeline = Pipeline(stages=assembler_1 + scaler + [assembler_2] + [lr])
        evaluator = BinaryClassificationEvaluator(
            labelCol="tx_fraud", metricName="areaUnderROC", weightCol="weight"
        )
        # ===========================================================================
        # train out of box model and collect its statistics
        logger.info("Fitting and validating initial model ...")

        model = pipeline.fit(train)
        predictions = model.transform(test)

        scores_init, stat_init, p_init = validate_model(predictions, evaluator)

        with open("scores_init.csv", "w") as f:
            wr = csv.writer(f, quoting=csv.QUOTE_ALL)
            wr.writerow(scores_init)
        mlflow.log_metric("p_shapiro_init", round(p_init, 4))
        mlflow.log_metric("stat_shapiro_init", round(stat_init, 4))
        mlflow.log_artifact("scores_init.csv")

        # ===========================================================================
        # train model with hyperparametrs tuning and collect its statistics
        logger.info("Fitting and validating tuned model ...")

        paramGrid = (
            ParamGridBuilder()
            .addGrid(lr.regParam, [0.1, 0.001])
            .addGrid(lr.elasticNetParam, [0.6, 0.8])
            .build()
        )

        crossval = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=paramGrid,
            evaluator=evaluator,
            numFolds=2,
        )
        cvmodel = crossval.fit(train)
        bestModel = cvmodel.bestModel
        predictions_hp = bestModel.transform(test)

        scores_tuned, stat_tuned, p_tuned = validate_model(predictions_hp, evaluator)

        with open("scores_tuned.csv", "w") as f:
            wr = csv.writer(f, quoting=csv.QUOTE_ALL)
            wr.writerow(scores_tuned)
        mlflow.log_metric("p_shapiro_tuned", round(p_tuned, 4))
        mlflow.log_metric("stat_shapiro_tuned", round(stat_tuned, 4))
        mlflow.log_artifact("scores_tuned.csv")

        mlflow.log_param(
            "regParam", round(bestModel.stages[-1]._java_obj.getRegParam(), 4)
        )
        mlflow.log_param(
            "elasticNetParam",
            round(bestModel.stages[-1]._java_obj.getElasticNetParam(), 4),
        )
        # ===========================================================================
        # compare scores distrubution of initial and tuned models
        pvalue, result = compare_distributions(scores_init, scores_tuned)
        mlflow.log_metric("pvalue_ttest", round(pvalue, 4))
        mlflow.log_metric("result", result)

        if result == "dist_IDENTICAL":
            mlflow.spark.log_model(model, "main_model")
        else:
            mlflow.spark.log_model(bestModel, "main_model")

        logger.info("Done")
        spark.stop()


if __name__ == "__main__":
    main()
