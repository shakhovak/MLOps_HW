from datetime import date, datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import lxml
import requests
from bs4 import BeautifulSoup
import s3fs
from csv import writer
import warnings

warnings.filterwarnings("ignore")

S3_KEY = Variable.get("S3_KEY_ID")
S3_SECRET = Variable.get("S3_SECRET_KEY")
RAW_DATA_FILE_PATH = Variable.get("S3_FILE_PATH")


def get_horo_1():
    fs = s3fs.S3FileSystem(
        key=S3_KEY,
        secret=S3_SECRET,
        endpoint_url="https://storage.yandexcloud.net",
    )

    url = "https://74.ru/horoscope/daily/"
    response = requests.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"
        },
    )
    soup = BeautifulSoup(response.content, "lxml")
    data = soup.find_all(attrs={"class": "IGRa5"})
    for item in data:
        lst = []
        try:
            zodiac = item.find(attrs={"class": "_4K6U+ _9dcVo"}).text
        except:
            zodiac = ""
        try:
            horoscope = item.find(attrs={"class": "BDPZt KUbeq"}).text
        except:
            horoscope = ""

        day = date.today()
        lst.append(day)
        lst.append(zodiac)
        lst.append(horoscope)
        lst.append("https://74.ru/horoscope/daily/")

        try:
            with fs.open(
                RAW_DATA_FILE_PATH,
                "a",
                encoding="utf-8",
            ) as f_object:
                writer_object = writer(f_object)
                writer_object.writerow(lst)
                f_object.close()
        except:
            print("file not found")


def get_horo_2():
    # ua = fake_useragent.UserAgent()
    fs = s3fs.S3FileSystem(
        key=S3_KEY,
        secret=S3_SECRET,
        endpoint_url="https://storage.yandexcloud.net",
    )
    url = "https://retrofm.ru/goroskop"
    response = requests.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"
        },
    )
    soup = BeautifulSoup(response.content, "lxml")
    data = soup.find_all(class_="text_box")
    for item in data:
        lst = []
        try:
            zodiac = item.find("h6").text
        except:
            zodiac = ""
        try:
            horo = item.text
            horo = horo.replace("\n", "")
            horo = horo.replace("\t\t\t\t\t\t\t", "li")
            horo = horo.split("li")[1]
            horo = horo.replace("\t\t\t\t\t\t", "")
        except:
            horo = ""

        day = date.today()
        lst.append(day)
        lst.append(zodiac)
        lst.append(horo)
        lst.append("https://retrofm.ru/goroskop")

        try:
            with fs.open(
                RAW_DATA_FILE_PATH,
                "a",
                encoding="utf-8",
            ) as f_object:
                writer_object = writer(f_object)
                writer_object.writerow(lst)
                f_object.close()
        except:
            print("file not found")

def get_horo_3():
    fs = s3fs.S3FileSystem(
        key=S3_KEY,
        secret=S3_SECRET,
        endpoint_url="https://storage.yandexcloud.net",
    )
    url_list = [
        "https://www.newsler.ru/horoscope",
        "https://www.newsler.ru/horoscope/erotic",
        "https://www.newsler.ru/horoscope/anti",
        "https://www.newsler.ru/horoscope/business",
        "https://www.newsler.ru/horoscope/health",
        "https://www.newsler.ru/horoscope/cook",
        "https://www.newsler.ru/horoscope/love",
        "https://www.newsler.ru/horoscope/mobile",
    ]
    # ua = fake_useragent.UserAgent()
    for url in url_list:
        response = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"
            },
        )
        soup = BeautifulSoup(response.content, "lxml")
        data = soup.find_all(class_={"col-xs-12 col-md-6"})
        for item in data:
            lst = []
            try:
                zodiac = item.find(class_={"t"}).text
            except:
                zodiac = ""
            try:
                horoscope = item.find(class_={"text"}).text
            except:
                horoscope = ""

            day = date.today()
            lst.append(day)
            lst.append(zodiac)
            lst.append(horoscope)
            lst.append("https://www.newsler.ru/horoscope")

            try:
                with fs.open(
                    RAW_DATA_FILE_PATH,
                    "a",
                    encoding="utf-8",
                ) as f_object:
                    writer_object = writer(f_object)
                    writer_object.writerow(lst)
                    f_object.close()
            except:
                print("file not found")


def get_url_1():
    url = "https://astroscope.ru/horoskop/ejednevniy_goroskop/"
    response = requests.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"
        },
    )
    soup = BeautifulSoup(response.content, "lxml")
    data = soup.find_all(class_={"col-6 col-md-4 d-block text-center p-2"})
    for item in data:
        url = "https:" + item["href"]
        yield url


def get_horo_4():
    fs = s3fs.S3FileSystem(
        key=S3_KEY,
        secret=S3_SECRET,
        endpoint_url="https://storage.yandexcloud.net",
    )
    for url in get_url_1():
        response = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"
            },
        )
        soup = BeautifulSoup(response.content, "lxml")
        data = soup.find_all(class_="text_box")

        lst = []
        try:
            zodiac = soup.find("h1").text.split(" ")[-1]
        except:
            zodiac = ""
        try:
            horo = soup.find(class_={"p-3"}).text
        except:
            horo = ""

        day = date.today()
        lst.append(day)
        lst.append(zodiac)
        lst.append(horo)
        lst.append("https://astroscope.ru/horoskop/ejednevniy_goroskop/")

        try:
            with fs.open(
                RAW_DATA_FILE_PATH,
                "a",
                encoding="utf-8",
            ) as f_object:
                writer_object = writer(f_object)
                writer_object.writerow(lst)
                f_object.close()
        except:
            print("file not found")


with DAG(
    dag_id="parser_mlops",
    schedule_interval="@daily",
    start_date=datetime(year=2024, month=3, day=2),
    catchup=False,
) as dag:
    # 1. Collect data from website and add to csv
    task_collect_data1 = PythonOperator(
        task_id="collect_horo1", python_callable=get_horo_1
    )
    task_collect_data2 = PythonOperator(
        task_id="collect_horo2", python_callable=get_horo_2
    )
    task_collect_data3 = PythonOperator(
        task_id="collect_horo3", python_callable=get_horo_3
    )
    task_collect_data4 = PythonOperator(
        task_id="collect_horo4", python_callable=get_horo_4
    )

(
    task_collect_data1
    >> task_collect_data2
    >> task_collect_data3
    >> task_collect_data4
  
)
