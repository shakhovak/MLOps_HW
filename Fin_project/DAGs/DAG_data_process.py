import uuid
from datetime import date, datetime
from airflow import DAG, settings
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)


# Common settings for your environment
YC_DP_FOLDER_ID = "b1gsqum16i8mi1516107"
YC_DP_SUBNET_ID = "e9bk4319fjbmq6rudpiq"
YC_DP_SA_ID = "ajemukl2tghg2likgshr"
YC_DP_AZ = "ru-central1-a"
YC_DP_SSH_PUBLIC_KEY = Variable.get("SSH_PUBLIC")
YC_DP_GROUP_ID = "enpv65jq6ep903597nmr"


# Settings for S3 buckets
YC_INPUT_DATA_BUCKET = "mlopsshakhova/airflow/"  # YC S3 bucket for input data
YC_SOURCE_BUCKET = "mlopsshakhova"  # YC S3 bucket for pyspark source files
YC_DP_LOGS_BUCKET = "mlopsshakhova/airflow/"  # YC S3 bucket for Data Proc cluster logs


# Создание подключения для Object Storage
session = settings.Session()
ycS3_connection = Connection(
    conn_id="yc-s3",
    conn_type="s3",
    host="https://storage.yandexcloud.net/",
    extra={
        "aws_access_key_id": Variable.get("S3_KEY_ID"),
        "aws_secret_access_key": Variable.get("S3_SECRET_KEY"),
        "host": "https://storage.yandexcloud.net/",
    },
)


if (
    not session.query(Connection)
    .filter(Connection.conn_id == ycS3_connection.conn_id)
    .first()
):
    session.add(ycS3_connection)
    session.commit()

ycSA_connection = Connection(
    conn_id="ubuntu",
    conn_type="yandexcloud",
    extra={
        "extra__yandexcloud__public_ssh_key": Variable.get("DP_PUBLIC_SSH_KEY"),
        "extra__yandexcloud__service_account_json_path": Variable.get("DP_SA_PATH"),
    },
)


if (
    not session.query(Connection)
    .filter(Connection.conn_id == ycSA_connection.conn_id)
    .first()
):
    session.add(ycSA_connection)
    session.commit()


# Настройки DAG
with DAG(
    "DATA_PREPROCESS",
    tags=["data-proc-data-process"],
    schedule_interval="@daily",
    start_date=datetime(year=2024, month=3, day=2, hour=15),
    catchup=False,
) as ingest_dag:

    create_spark_cluster = DataprocCreateClusterOperator(
        task_id="dp-cluster-create-task",
        folder_id=YC_DP_FOLDER_ID,
        cluster_name=f"tmp-dp-{uuid.uuid4()}",
        cluster_description="Temporary cluster for Spark processing under Airflow orchestration",
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_DP_LOGS_BUCKET,
        service_account_id=YC_DP_SA_ID,
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        zone=YC_DP_AZ,
        cluster_image_version="2.0.43",
        masternode_resource_preset="s3-c2-m8",
        masternode_disk_type="network-ssd",
        masternode_disk_size=20,
        datanode_resource_preset="s3-c4-m16",
        datanode_disk_type="network-ssd",
        datanode_disk_size=60,
        datanode_count=1,
        services=["YARN", "SPARK", "HDFS", "MAPREDUCE"],
        computenode_count=0,
        connection_id=ycSA_connection.conn_id,
        dag=ingest_dag,
    )

    # 2 этап: запуск задания PySpark
    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id="dp-cluster-pyspark-task",
        main_python_file_uri=f"s3a://{YC_SOURCE_BUCKET}/scripts/data_process_pyspark.py",
        connection_id=ycSA_connection.conn_id,
        dag=ingest_dag,
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id="dp-cluster-delete-task",
        trigger_rule=TriggerRule.ALL_DONE,
        dag=ingest_dag,
    )
    # Формирование DAG из указанных выше этапов
    create_spark_cluster >> poke_spark_processing >> delete_spark_cluster
