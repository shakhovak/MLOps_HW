# Создание MLOps структуры для обучения генеративной языковой модели

![image](https://github.com/shakhovak/MLOps_HW/assets/89096305/f11424c0-fa73-4e8e-9ca5-d6a8889fca1a)

## Цель проекта
Основной целью проекта является разработка инфраструктуры для сбора, обработки данных в целях обучения языковой генеративной модели для использования в приложении. Для учебного примера модель должна генерировать по запросу пользователя ежедневный гороскоп по знаку Зодиака. Данные обновляются ежедневно и модель переобучается на систематической основе. Переобученная модель будет использоваться в приложении.

## Pipeline
Схема обработки данных представлена ниже:

![image](https://github.com/shakhovak/MLOps_HW/assets/89096305/afba4ee5-7302-4e80-8dad-d20c5667b8e7)

**Итого:**
- :triumph: Создано 4 ВМ облаке: 

![image](https://github.com/shakhovak/MLOps_HW/assets/89096305/bd506cd2-33da-47e9-8266-23317c7fc205)


- :triumph: Airflow - 4 DAGs:

![image](https://github.com/shakhovak/MLOps_HW/assets/89096305/77e1db6e-59fc-4c5b-b51f-47651d9bbd56)


### Сбор данных
Сбор данных осуществляется с нескольких сайтов один раз в день. Так как структура данных разная, то при сборе данные приводятся к одной схеме. Сбор данных оркеструется Airflow, который размещен на ВМ в облаке. DAG для сбора данных можно посмотреть вот [здесь](https://github.com/shakhovak/MLOps_HW/blob/master/Fin_project/DAGs/DAG_data_collect.py) .

Используется готовая конфигурация Airflow от Yandex с уже установленными провайдерами. После сбора и приведения к единой структуре, данные добавляются в папку raw data в object storage в формате csv. 

<hr>
<details>
  <summary> Details - click to open:</summary>

В основе ВМ с установленной конфинурацией Apache Airflow 2.2.3. Основной используемый оператор ```PythonOperator```. Этот оператор уже есть в установленной конфигурации, поэтому дополнительных настроек не требуется.

Дполнительно нужно установить следующие библиотеки на ВМ:
```
  sudo python3 -m pip install pandas
  sudo python3 -m pip install BeautifulSoup4
  sudo python3 -m pip install lxml
  sudo python3 -m pip install s3fs
```
Для работы с бакетом s3 добавляем в переменные ключ и секрет от соответствующего бакета, где должны храниться собранные данные. Ключ и секрет были сгенерированы в UI для сервисного аккаунта, который используется при создании ВМ с Airflow.

</details>
<hr>

### Обработка данных для языковой модели
Основная цель обработки - подготовить данные для обучения языковой модели. Этапы обработки:

- удаление пустых строк и дубликатов
- удаление всех знаков кроме пунктуации, текста
- отбор по длине текста (отбираем тексты не меньше 300 знаков и не более 500, чтобы не нагружать читателя :))

Скрипт для обработки в виде задания (можно посмотреть [здесь](https://github.com/shakhovak/MLOps_HW/blob/master/Fin_project/Scripts/data_process_pyspark.py)) будет запускаться на временном кластере Spark. Для создания временного кластера, запуска на нем задания и последующего удаления кластера Airflow запускает соответствующий DAG, который можно посмотреть [здесь](https://github.com/shakhovak/MLOps_HW/blob/master/Fin_project/DAGs/DAG_data_process.py). 

После обработки данные сохраняются в object storage в папку processed в формате parquet.

<hr>
<details>
  <summary>Технические детали - click to open:</summary>

В основе ВМ с установленной конфигурацией Apache Airflow 2.2.3. Скрипт для обработки данных находится в object storage в папке scripts. Для работы DAG требуется провайдер ```yandex```, который уже есть в предустановленной конфигурации, поэтому дополнительных настроек не требуется.

В переменные airflow нужно добавить:
- ключ и секрет к бакету, где храниться скрипт, данные
- публичный ssh-ключ для создания кластера pyspark
- авторизованный ssh ключ для сервисного аккаунта (при этом у аккаунта должны быть права на создания кластера ```mdb.dataproc.agent```, ```dataproc.agent```, ```dataproc.agent``` и на пользование облаком ```vpc.user```). Ключ создается в UI и сохраняется на ВМ, где развернут Airflow. Путь к ключу указывается в переменных Airflow.

В DAG используется операторы ```DataprocCreateClusterOperator```, ```DataprocCreatePysparkJobOperator```, ```DataprocDeleteClusterOperator```.

</details>
<hr>

### Обучение модели
Для обучения модели я буду использовать библиотеку ```datasphere``` с возможностью запуска задания на разных конфигурациях GPU с локальной или облачной ВМ с использованием ресурсов только на время вычисления (детальная [инструкция](https://cloud.yandex.ru/ru/docs/datasphere/operations/projects/work-with-jobs) от Yandex). Так как это новая разработка, то у Яндекса нет специального оператора для запуска заданий из Airflow, поэтому я воспользуюсь SSHOperator.
Скрипт для обучения модели можно посмотреть [здесь](https://github.com/shakhovak/MLOps_HW/blob/master/Fin_project/Scripts/datasphere_scripts/main_train.py). Скрипт используемый Airflow для запуска задания [здесь](https://github.com/shakhovak/MLOps_HW/blob/master/Fin_project/DAGs/DAG_model_train.py). Для хранения среды и скриптов для задания создается отдельная ВМ на Ubuntu.

В процессе обучения параметры обучения записываются в ML Flow, который настроен на отдельной ВМ. Для обучения я использовала библиотеку ```hugging face```, у нее уже есть встроенный callback с ML Flow, поэтому в скрипте по обучению нужно только указать несколько переменных среды. Детальная инструкция по callback вот [здесь](https://huggingface.co/docs/transformers/v4.38.2/en/main_classes/callback#transformers.integrations.MLflowCallback), ролик с примером использования на [youtube](https://www.youtube.com/watch?v=vmfaDZjeB4M&t=1s)

Обученная модель сохраняется в ML Flow, также я добавила еще сохранение на hugging face (репозиторий с готовой моделью https://huggingface.co/Shakhovak/ruT5-base_horoscopes-generator)

![image](https://github.com/shakhovak/MLOps_HW/assets/89096305/099b532a-ec43-449d-84a6-ad8610c7e8a9)


<hr>
<details>
  <summary>Технические детали - click to open:</summary>
  
В основе ВМ с установленной конфигурацией Ubuntu. После запуска нужно сделать апдейт и установить виртуальное окружение:
```
  sudo apt update
  python3 –version
  sudo apt-get install python3-pip
  sudo apt install python3.10-venv
  python3 -m venv <venv_name>
  source <venv_name>/bin/activate
```
Далее необхожима устновка yandex CLI + авторизация машины для доступа к проекту с конфигурациями. Детально про создание CLI можно посмотреть [здесь](https://cloud.yandex.ru/ru/docs/cli/operations/install-cli#linux_1), а [здесь](https://cloud.yandex.ru/ru/docs/cli/operations/install-cli#linux_1) иструкция для настройки доступа ВМ к вычислительным ресурсам.

После авторизации нужно установить библиотеку datasphere, а также библиотеки для обучения модели. Все библиотеки собраны в соответсвующем файле requirements.txt (файл можно посмотреть вот [здесь](https://github.com/shakhovak/MLOps_HW/blob/master/Fin_project/Scripts/datasphere_scripts/requirements.txt)). Отмечу, что у с datasphere по каким-то причинам не работает последняя версия Ml Flow, поэтому пришлось взять одну из предыдущих. Соответсвенно, пришлось взять и предыдущую версию трансформеров.
```
python3 -m pip install datasphere
pip install -r requirements.txt
```

Также на ВМ необходимо разместить (все файлы можно посмотреть в этой [папке](https://github.com/shakhovak/MLOps_HW/tree/master/Fin_project/Scripts/datasphere_scripts):
1. main_train.py - скрипт для обучения модели и загрузки ее в репозиторий
2. config_train.yaml - инструкция для запуска задания
3. requirements.txt - список библиотек для работы скрипта. Этот список будет использоваться datasphere для настройки окружения.

Для запуска задания с ВМ нужно добавить в Airflow SSH connection, где в качестве host указывается публичный ip ВМ, с которого будет запускаться задание.

![image](https://github.com/shakhovak/MLOps_HW/assets/89096305/67732f45-2452-471b-8975-bc2ab70be73e)

Так как я использовала уже развернутый на ВМ Airflow, то требовалось добавить список провайдеров. Для этого использовала команду ```pip install apache-airflow-providers-ssh``` для установки небходимых библиотек. После установки понадобилось перезагрузить базы данных командой ```airflow db reset```. Также потребовалось создать нового пользователя с правами admin.

</details>
<hr>

### Оценка модели
Так как генеративная модель требует оценки пользователем, вынесу ее процесс в отдельный скрипт, который можно посмотреть вот [здесь](https://github.com/shakhovak/MLOps_HW/blob/master/Fin_project/Scripts/datasphere_scripts/main_evaluate.py). Основные параметры для оценки модели:

- :triangular_flag_on_post: Языковая приемлемость (language acceptibilty) - корректность сформированных моделью предложений с точки зрения орфографии, синтаксиса и т.д. Воспользуемся предобученной моделью https://huggingface.co/RussianNLP/ruRoBERTa-large-rucola, которая выдает лейбл = 1 для приемлемых с точки зрения языковых норм предложений. Будем считать % таких предложений в общем корпусе сгенерированных тестовых кейсов.
- :triangular_flag_on_post: Разнообразие текстов (text diversity) - воспользуемся косинусной близостью, чтобы посмотреть похожесть текстов и возьмем разницу 1 - между средним коэффициентом для текста, что и будет характеристикой разнообразия. Для этих целей еще раз векторизуем тексты с помощью модели https://huggingface.co/sentence-transformers/LaBSE. Будем считать среднее разнообразие для выбранной стратегии.
- :triangular_flag_on_post: Эмоциональная окрашенность текстов (text sentiment) - положительны или отрицательны тексты по своему содержанию. Для этого также воспользуемся готовой моделью, обученной для русского языка https://huggingface.co/seara/rubert-base-cased-russian-sentiment, которая выдает 3 лейбла - neutral: нейтральный, positive: позитивный, negative: негативный. При оценке будем присваивать 0 только отрицательному лейблу, позитивный и нейтральный получат 1. Далее будем считать % НЕотрицательных текстов в сгенерированном корпусе.

Рассчитаю эти метрики для разных стратегий генерации, варьируя гиперпараметры, ответственные за креативность текста ```top_p``` и ```temperature```.

Сгенерированные данные, их метрики, а также усредненные данные по тестовой генерации будут записываться в ML Flow.

Оценка будет запускаться DAG в Airflow после обучения модели. Посмотреть DAG можно вот [здесь](https://github.com/shakhovak/MLOps_HW/blob/master/Fin_project/DAGs/DAG_model_evaluate.py). Скрипт по обучению также использует библиотеку ```datashpere``` и размещен на той же ВМ, что и скрипт по обучению.
Также на ВМ необходимо разместить (все файлы можно посмотреть в этой [папке](https://github.com/shakhovak/MLOps_HW/tree/master/Fin_project/Scripts/datasphere_scripts):
1. main_evaluate.py - скрипт для обучения модели и загрузки ее в репозиторий
2. config_evaluate.yaml - инструкция для запуска задания
3. requirements.txt - список библиотек для работы скрипта. Этот список будет использоваться datasphere для настройки окружения.
4. 
Для фиксации результатов оценки используется ML Flow, развернутый на отдельной ВМ с хранилищем артефактов в бакете s3 и базой Postgres для хранения метрик.

![image](https://github.com/shakhovak/MLOps_HW/assets/89096305/e0af5c2d-e4df-47bd-97b5-94c593ddd8ae)


### Использование модели
Обученная модель из репозитория будет использоваться в телеграм чат-боте, который размещается на отдельной ВМ с Ubuntu. В основе чат-бота библиотека ```aiogram```. Скрипт с ботом можно посмотреть вот [здесь](https://github.com/shakhovak/MLOps_HW/blob/master/Fin_project/Scripts/chat_bot.py). На ВМ чат-бот запускается с помощью Linux Service.

> [!IMPORTANT]
> **Пообщаться с ботом и посмотреть свой гороскоп можно по ссылке https://t.me/Terra_NLP_bot .**

<hr>
<details>
  <summary>Технические детали - click to open:</summary>
После запуска ВМ необходимо создать и активировать виртуальное окружение и установить необходимые библиотеки (torch, transformers, aiogram):
  ```
    sudo apt update
    python3 –version
    sudo apt-get install python3-pip
    sudo apt install python3.10-venv
    python3 -m venv <venv_name>
    source <venv_name>/bin/activate
  ```
Файл с чат-ботом и config.json (с токеном) сохраняем также на ВМ. 

Дальше нужно создать файл с daemon-process, для запуска и поддержания работы чат-бота:
  ```python
    [Unit]
    Description=My TG bot
    After=network.target
    
    [Service]
    User=admin
    Environment="TOKEN=<tg=token>"
    ExecStart=/home/admin/venv/bin/python chat_bot.py
    WorkingDirectory=/home/admin
    Restart=always
    
    [Install]
    WantedBy=multi-user.target
  ```
Этот файл должен быть сохранен в директории с daemon-процессами ```/etc/systemd/system ```. Описание файла:
- user - пользователь, под которым создана ВМ
- environment - API-токен в телеграм
- ExecStart - путь к виртуальному окружению и к файлу с ботом
- WorkingDirectory - директория размещения бота


После сохранения файла нужно выполнить следующие команды:
- ```sudo systemctl daemon-reload``` - перезагрузить службу, чтобы она увидела новый файл
- ```sudo systemctl start chatbot.service``` - включаем службу
- ```sudo systemctl enable chatbot.service``` - добавляем службу в автозагрузку
- ```sudo systemctl status chatbot.service``` - проверяем статус работы службы
- ```sudo journalctl -u chatbot.service -b``` - смотрим логи, если есть проблемы

</details>
<hr>


## Дальнейшее развитие проекта

- Продумать более тщательно формат хранения данных. Использование единого файла csv (до обработки) и parquet (после обработки) не масштабируемо. Можно рассмотреть варианты использования базы данных.
- Разобраться с ошибкой при запуске job на сервере datasphere из ВМ, так как при запуске из локальной CLI такой ошибки нет.
- Продумать варианты возможных репозиториев (например, ML FLow - модель уже загружается туда). Использование публичного репозитория на huggin face может быть критично с точки зрения данных.
- Подготовить для деплоймента модели docker, чтобы разворачивать его на ВМ уже с использованием асинхронных технологий (gunicorn + ngnix).
