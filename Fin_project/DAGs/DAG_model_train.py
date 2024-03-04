from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime
from airflow.models import DAG

with DAG(
    dag_id="MODEL_TRAIN",
    tags=["ssh-datasphere-model-train"],
    schedule_interval="@daily",
    start_date=datetime(year=2024, month=3, day=3, hour=18),
    catchup=False,
) as dag:

    train_model = SSHOperator(
        task_id="train_model",
        ssh_conn_id="ssh_yandex_cloud",
        conn_timeout=7200,
        cmd_timeout=7200,
        command="source dsjobs/bin/activate&&export PATH=/home/admin/yandex-cloud/bin:$PATH&&datasphere project job execute -p bt1jndpctue1eupfhqn4 -c config_train.yaml",
    )
train_model
