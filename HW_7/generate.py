import time
import random
import socket
from datetime import datetime
import json

from kafka import KafkaProducer


def error_callback(err):
    print("Something went wrong: {}".format(err))


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered " + str(msg))



p = KafkaProducer(bootstrap_servers='rc1a-q861d21g835psjg9.mdb.yandexcloud.net:9091',
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username='producer',
    sasl_plain_password='Privet1981',
    ssl_cafile="/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt")


for i in range(100):
    object_type = random.choice(["banner", "article", "link"])
    object_id = str(random.randint(1, 30))
    data = {
        "ts": int(time.time()),
        "user_id": random.randint(1, 30),
        "geo": random.choice(["ru", "ua", "kz"]),
        "object_id": f"{object_type}{object_id}",
        "domain": "example.com",
        "url": f"https://example.com/{object_type}/{object_id}",
        "url_from": "https://example.com/",
    }
    p.send(
        "test-topic",
        json.dumps(data).encode("utf-8"),
        key=str(data["user_id"]).encode("ascii"),
    )
p.flush(10)
p.close()
