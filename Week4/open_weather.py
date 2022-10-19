from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2

def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def etl(**context):
    api_key = Variable.get('weather_api_key')
    lat = 37.5665
    lon = 126.9780

    url = f'https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={api_key}&units=metric&exclude=current,minutely,hourly,alerts'
    response = requests.get(url)
    res_json = response.json()

    temp_data = []
    for data in res_json["daily"]:
        date = datetime.fromtimestamp(data["dt"]).strftime('%Y-%m-%d')
        temp_data.append('("{}", {}, {}, {})'.format(date, data["temp"]["day"], data["temp"]["min"], data["temp"]["max"]))

    cur = get_Redshift_connection()
    query = "DELETE FROM jiyoon0043.weather_forecast; INSERT INTO jiyoon0043.weather_forecast VALUES {}".format(','.join(temp_data))
    try:
        cur.execute(query)
        cur.execute("COMMIT;")
    except:
        cur.execute("ROLLBACK;")
        raise

"""
CREATE TABLE jiyoon0043.weather_forecast (
    date date,
    temp float,
    min_temp float,
    max_temp float,
    updated_date timestamp default GETDATE()
);
"""

open_weather_dag = DAG(
    dag_id = 'open_weather_assignment',
    start_date = datetime(2022, 10, 6),
    schedule_interval = '0 2 * * *',
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

etl = PythonOperator(
    task_id = 'etl',
    python_callable = etl,
    dag = open_weather_dag
)