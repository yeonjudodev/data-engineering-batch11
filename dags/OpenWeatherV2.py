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

#연결함수 정의 
def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id = 'redshift_dev_db')
    return hook.get_conn().cursor()

'''
def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "yeonjudodev"
    redshift_pass = "Yeonjudodev!1"
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect(f"dbname={dbname} user={redshift_user} host={host} password={redshift_pass} port={port}")
    conn.set_session(autocommit=True)
    return conn.cursor()
'''
#etl 함수 정의

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

    #incremental update 
    #임시 테이블 만들고, 기존 테이블의 데이터 적재 
    create_sql = f"""DROP TABLE IF EXISTS {schema}.temp_{table};
    CREATE TABLE {schema}.temp_{table} (LIKE {schema}.{table} INCLUDING DEFAULTS);INSERT INTO {schema}.temp_{table} SELECT * FROM {schema}.{table};"""
    logging.info(create_sql)
    try:
        cur.execute(create_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    #임시 테이블에 ret[] 데이터 적재
    insert_sql = f"INSERT INTO {schema}.temp_{table} VALUES " + ",".join(ret)
    logging.info(insert_sql)
    try:
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    #기존 테이블 대체 
    alter_sql = f"""DELETE FROM {schema}.{table};
      INSERT INTO {schema}.{table}
      SELECT date, temp, min_temp, max_temp FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY updated_date DESC) seq
        FROM {schema}.temp_{table}
      )
      WHERE seq = 1;"""
    logging.info(alter_sql)
    try:
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise
"""
CREATE TABLE yeonjudodev.weather_forecast (
    date date,
    temp float,
    min_temp float,
    max_temp float,
    updated_date timestamp default GETDATE()
);
"""

dag_open_weather = DAG(
        dag_id = 'dag_open_weahter_v2',
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
