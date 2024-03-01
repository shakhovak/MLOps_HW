import pyspark
from pyspark.sql.types import StructType, StructField, TimestampType, StringType
from pyspark.sql.functions import regexp_replace, split, col, length


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
    spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

    path = "s3a://mlopsshakhova/project/raw_data/"

    schema = StructType(
        [
            StructField("date", TimestampType(), True),
            StructField("zodiac", StringType(), True),
            StructField("horo", StringType(), True),
            StructField("source", StringType(), True),
        ]
    )

    df = spark.read.csv(path, header=True, schema=schema)

    df_upd = df.na.drop("any")
    df_upd = df_upd.filter(df_upd["zodiac"] != "Гороскоп на сегодня")
    df_upd = df_upd.filter(df_upd["zodiac"] != "Для всех знаков зодиака.")
    df_upd = df_upd.withColumn("horo", split(df_upd["horo"], "\xa0").getItem(0))
    df_upd = df_upd.withColumn("horo", regexp_replace("horo", "\n", ""))
    df_upd = df_upd.withColumn("horo", regexp_replace("horo", "\xa0", ""))
    df_upd = df_upd.withColumn(
        "horo", regexp_replace("horo", "[^а-яА-ЯЁё !.,:?;«»-]+", "")
    )
    df_upd = df_upd.dropDuplicates()

    df_upd = df_upd.filter(length(col("horo")) < 500)
    df_upd = df_upd.filter(length(col("horo")) > 300)

    df_upd.write.mode("overwrite").parquet(
        "s3a://mlopsshakhova/project/processed/fin_horo.parquet"
    )


if __name__ == "__main__":
    main()
