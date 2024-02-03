# ДЗ № 7
**Цель работы**:
- [x] Напишите скрипт, который генерирует данные и записывает их в Kafka.
- [x] Создайте Spark job, который на потоке применяет модель к данным из kafka и записывает результаты в другой topic.
- [x] Протестируйте полученное решение.
<hr>

# Общий подход
Для решения этой задачи я выполню следующие шаги:

1. С помощью сревиса Managed Service for Kafka создам кластер Kafka для получения и хранения данных из файла generate.py, который будет имитировать генерацию данных пользователями. Файл можно посмотреть вот здесь.
Для экономии выберу только 1 хост, как показано на принт-скрине ниже:

![image](https://github.com/shakhovak/MLOps_HW/assets/89096305/dd36415a-352c-49bf-8188-ae60d74d436c)

3. В кластере создам 2 топика: test-topic для внесения начальных данных через продюсера, reworked - для занесения уже измененных данных через spark job
   
![image](https://github.com/shakhovak/MLOps_HW/assets/89096305/2b12224f-8170-4802-b9e4-2f07b7f37a29)

4. Добавлю пользователей: consumer + producer - с отдельными ролями, пользователь kate - права и consumer, и producer, чтобы записывать данные из скрипта в новый топик.
   
![image](https://github.com/shakhovak/MLOps_HW/assets/89096305/d465d0c7-18ac-494b-bdaf-c0bde581402d)

6. Создам ВМ в облаке с Ubuntu, на которую установлю:
```python
sudo apt-get install kafkacat
pip install pyspark
sudo apt-get install default-jre
sudo apt update && sudo apt install -y python3 python3-pip libsnappy-dev && \
pip3 install kafka-python lz4 python-snappy crc32c
```
7. Подключу ВМ по ssh в vscode
8. Так как Kafka кластер у меня публичный, то сгенерирую SSL сертификат и помещу его в папку ```/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt```
9. На этой же ВМ размещу файл generate.py и запущу его командой ```python3 generate.py``` и проверю, что записалось в топик test-topic через команду
```python
kafkacat -C \
         -b rc1a-q861d21g835psjg9.mdb.yandexcloud.net:9091 \
         -t test-topic\
         -X security.protocol=SASL_SSL \
         -X sasl.mechanism=SCRAM-SHA-512 \
         -X sasl.username="kate" \
         -X sasl.password="Privet1981" \
         -X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt -Z -K:
```
Как видно из принт-скрина ниже, данные записались. Обращаю внимание, что в скрипте по генерации у меня стоит ограничение, поэтому данные генерируется не постоянно

![image](https://github.com/shakhovak/MLOps_HW/assets/89096305/9107363f-4478-4f4a-a5e9-11fe4cd9c76f)

10. Подготовлю spark-script (его можно посмотреть вот здесь), который будет считаывать данные из топика test-topic с помощью Spark Structured Streaming, фильтровать данные по пользователям и отбирать только пользователей из России. Отфильтрованные данные будует записывать в топик reworked.
11. После запуска скрипта (я не делала spark-submit, а вспользовалась командой ```python3 spark_script.py```. Spark-submit вызвался автоматчески при запуске скрпита. Аргументы для spark-submit я собрала в переменной среды ```os.environ['PYSPARK_SUBMIT_ARGS']```) я проверяю, как данные записались в новый топик. Для этого использую kafkacat:

```python
kafkacat -C \
         -b rc1a-q861d21g835psjg9.mdb.yandexcloud.net:9091 \
         -t reworked\
         -X security.protocol=SASL_SSL \
         -X sasl.mechanism=SCRAM-SHA-512 \
         -X sasl.username="kate" \
         -X sasl.password="Privet1981" \
         -X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt -Z -K:
```
Как видно из скрина, данные уже в обработанном виде записались в новый топик. User_id стал новым ключем:

![image](https://github.com/shakhovak/MLOps_HW/assets/89096305/30b25630-8ad0-4e08-8f49-b3a0f1b623cb)

После этого удаляю Kafka кластер и ВМ.



