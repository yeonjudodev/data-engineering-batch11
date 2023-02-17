from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2
import json

'''연결함수 정의'''
def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id = 'redshift_dev_db')
    return hook.get_conn().cursor()

'''etl 함수 정의'''
def extract(**context):
    lat = context["params"]["lat"]
    lon = context["params"]["lon"]
    api_key = Variable.get("open_weather_api_key")
    link = f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={api_key}&units=metric&exclude=current,minutely,hourly,alerts"

    task_instance = context['task_instance']
    #execution_date = context['execution_date']
    #logging.info(execution_date)

    f = requests.get(link)
    f_json = f.json()
    return f_json

def transform(**context):
    data = context["task_instance"].xcom_pull(key = "return_value", task_ids = "extract")

    ret = []
    for d in data["daily"]:
        day = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
        ret.append("('{}',{},{},{})".format(day, d["temp"]["day"], d["temp"]["min"], d["temp"]["max"]))
    return ret

def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]

    cur = get_Redshift_connection()
    ret = context["task_instance"].xcom_pull(key = "return_value", task_ids = "transform")

    insert_sql = "BEGIN;DELETE FROM yeonjudodev.weather_forecast;INSERT INTO yeonjudodev.wether_forecast VALUES" +",".join(ret)
    logging.info(insert_sql)
    try:
        cur.execute(insert_sql)
        cur.execute("Commit;")
    except Exception as e:
        cur.execute("Rollback;")
        raise

"""
CREATE TABLE yeonjudodev.weather_forecast (
    date date,
    temp float,
    min_temp float,
    max_temp float,
    created_date timestamp default GETDATE()
);
"""

dag_open_weather = DAG(
        dag_id = 'dag_open_weahter_v3',
        start_date = datetime(2023,2,15),
        schedule_interval = '0 2 * * *',
        max_active_runs = 1,
        catchup = False,
        default_args = {
            'retries': 1,
            'retry_delay': timedelta(minutes = 3),
            }
        )

extract = PythonOperator(
        task_id = 'extract',
        python_callable = extract,
        params = {
            "lat": 37.5665,
            "lon": 126.9780,
        },
        provide_context = True,
        dag = dag_open_weather
        )

transform = PythonOperator(
        task_id = 'transform',
        python_callable = transform,
        params = {
        },
        provide_context = True,
        dag = dag_open_weather
        )

load = PythonOperator(
        task_id = 'load',
        python_callable = load,
        params = {
            "schema": "yeonjudodev",
            "table": "weather_forecast"
        },
        provide_context = True,
        dag = dag_open_weather
        )

extract >> transform >> load
