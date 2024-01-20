import pyspark
from datetime import datetime
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    FloatType,
    TimestampType,
)


def main():
    app_name = "DataPreprocess"
    spark_ui_port = 4040  # Порт для spark ui

    spark = (
        pyspark.sql.SparkSession.builder.appName(
            app_name
        )  # имя приложения ? Нужно для отслеживания таски выполнения
        .config("spark.executor.cores", "4")
        .config(
            "spark.executor.memory", "4g"
        )  # Executor просее. Ориенир для потребления помаяти.
        .config("spark.executor.instances", "6")
        .config("spark.default.parallelism", "48")
        .config("spark.driver.memory", "4g")  # Main процесс
        .config("spark.ui.port", spark_ui_port)
        .getOrCreate()
    )
    spark.conf.set(
        "spark.sql.repl.eagerEval.enabled", True
    )  # to pretty print pyspark.DataFrame in jupyter

    path = "s3a://mlopsshakhova/airflow/"

    schema = StructType(
        [
            StructField("tranaction_id", IntegerType(), True),
            StructField("tx_datetime", TimestampType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("terminal_id", IntegerType(), True),
            StructField("tx_amount", FloatType(), True),
            StructField("tx_time_seconds", IntegerType(), True),
            StructField("tx_time_days", IntegerType(), True),
            StructField("tx_fraud", IntegerType(), True),
            StructField("tx_fraud_scenario", IntegerType(), True),
        ]
    )

    df = spark.read.csv(path, header=False, schema=schema, comment="#")

    df_upd = df.na.drop("any")
    df_upd = df_upd.filter(df_upd["tx_amount"] > 0.0)
    df_upd = df_upd.dropDuplicates(["tranaction_id"])
    df_upd = df_upd.filter(df_upd["customer_id"] >= 0)
    df_upd = df_upd.filter(df_upd["terminal_id"] <= 999)

    Current_Date = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

    df_upd.write.parquet(
        "s3a://mlopsshakhova/airflow/processed/"
        + str(Current_Date)
        + "_"
        + "processed.parquet"
    )


if __name__ == "__main__":
    main()
