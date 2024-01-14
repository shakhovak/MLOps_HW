# ДЗ2
**1. Создать бакет в Yandex Cloud Object Storage и скопировать в него файлы из хранилища s3://mlops-data/fraud-data/**

## Решение:
Бакет с публичным доступом создан из UI Yandex Cloud Object Storage s3://mlopsshakhova .
Данные скопированы через cmd с помощью команды ```aws s3 --endpoint-url=https://storage.yandexcloud.net  cp s3://mlops-data/fraud-data/ s3://mlopsshakhova --copy-props none –recursive```

Я не стала использовать s3cmd, так как получила отклик с отказом в доступе. Как я поняла, есть проблемы с копированием публичных файлов, поэтому воспользовалась аналогоа aws.

Дополнительно с помощью команды ```aws s3 --endpoint-url=https://storage.yandexcloud.net ls  --recursive s3://mlops-data/fraud-data/ --summarize > bucket-contents-source.txt```

и команды ```aws s3 --endpoint-url=https://storage.yandexcloud.net ls  --recursive s3://mlopsshakhova/ --summarize > bucket-contents-target.txt```

сформировала списки файлов в начальной и финальной директориях для проверки полноты копирования. Файлы приложены [здесь](https://github.com/shakhovak/MLOps_HW/blob/master/HW_2/bucket-contents-source.txt) и [здесь](https://github.com/shakhovak/MLOps_HW/blob/master/HW_2/bucket-contents-target.txt).

**2. Создать Spark-кластер в Data Proc с двумя подкластерами со следующими характеристиками:**
    - Мастер-подкластер: класс хоста s3-c2-m8, размер хранилища 40 ГБ.
    - Data-подкластер: класс хоста s3-c4-m16, 3 хоста, размер хранилища 128 ГБ.
    
## Решение:
Кластер создан, но в процессе копирования у него появились проблемы с доступностью, но я решила не перезапускать кластер.

Кластер после получения принт-скрина удалила.

![Alt text](image.png)

**3. Подключиться к созданному кластеру по SSH и скопировать файлы из s3 хранилища в файловую систему кластера.**

## Решение:
Подключение по ssh через cmd c помощью команды ```ssh ubuntu@51.250.82.79```.
Копирование файлов с помощью команды ```hadoop distcp  s3a://mlopsshakhova/ hdfs://rc1a-dataproc-m-lk9fm0ppyc4nd9si.mdb.yandexcloud.net/user/root/froms3```

К сожалению, процесс превался на 87%, так как у хоста появились проблемы с доступностью.

В итоге из необходимых 40 файлов скопировались только 36.

![Alt text](image-1.png)

Я решила не создавать новый кластер и не перезапускать процесс, так как большую часть файлов получилось скопировать.

Если говорить о расходах, то содеражние кластера обходится намного дороже, чем хранилища s3. Ниде расходы за 1 день хранения s3 и несколько часов работы кластера.

![image](https://github.com/shakhovak/MLOps_HW/assets/89096305/8e4476f9-f500-4ed0-8256-986da04be453)




