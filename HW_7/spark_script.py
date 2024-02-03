import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'

checkpoint_path = "./checkpoint/ex_1_2"
kafka_servers = "rc1a-q861d21g835psjg9.mdb.yandexcloud.net:9091 "
topic = "test-topic"

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (SparkSession
        .builder
        .appName("consumer_structured_streaming_ex_1_2")
        .getOrCreate())

schema = StructType([ \
    StructField("ts", IntegerType(), True), \
    StructField("user_id", IntegerType(), True), \
    StructField("geo", StringType(), True), \
    StructField("object_id", StringType(), True), \
    StructField("domain", StringType(), True), \
    StructField("url", StringType(), True), \
    StructField("url_from", StringType(), True), \
  ])

kafkaDF = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("kafka.sasl.mechanism", 'SCRAM-SHA-512') \
            .option("kafka.security.protocol", 'SASL_SSL') \
            .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.scram.ScramLoginModule required username="kate" password="Privet1981";') \
            .option("kafka.ssl.ca.location", '/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt') \
            .option("subscribe",topic) \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING)") 


reworked = kafkaDF.select(from_json(col("value"), schema).alias("t")) \
            .select("t.ts", "t.user_id", "t.object_id", "t.domain", "t.url") \
            .filter("t.geo = 'ru'")\

query = reworked \
            .selectExpr("CAST(user_id AS STRING) AS key", "to_json(struct(*)) AS value") \
            .writeStream.format("kafka") \
            .option("checkpointLocation", checkpoint_path) \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("kafka.sasl.mechanism", 'SCRAM-SHA-512') \
            .option("kafka.security.protocol", 'SASL_SSL') \
            .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.scram.ScramLoginModule required username="kate" password="Privet1981";') \
            .option("kafka.ssl.ca.location", '/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt') \
            .option("topic","reworked") \
            .outputMode("append") \
            .trigger(processingTime='10 seconds') \
            .start() \
            .awaitTermination()