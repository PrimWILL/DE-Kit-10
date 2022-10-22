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

def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def extract(**context):
    api_key = context["params"]["api_key"]
    lat = context["params"]["lat"]
    lon = context["params"]["lon"]
    link = f'https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={api_key}&units=metric&exclude=current,minutely,hourly,alerts'

    task_instance = context['task_instance']
    execution_date = context['execution_date']

    logging.info(execution_date)
    response = requests.get(link)
    return response.text

def transform(**context):
    response = str(context["task_instance"].xcom_pull(key="return_value", task_ids="extract"))
    logging.info(response)
    res_json = json.loads(response)

    temp_data = []
    for data in res_json["daily"]:
        date = datetime.fromtimestamp(data["dt"]).strftime('%Y-%m-%d')
        temp_data.append("('{}', {}, {}, {})".format(date, data["temp"]["day"], data["temp"]["min"], data["temp"]["max"]))
    return temp_data

def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]

    temp_data = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")

    cur = get_Redshift_connection()
    query = """DELETE FROM jiyoon0043.weather_forecast; INSERT INTO {schema_name}.{table_name} VALUES {values}""".format(schema_name = schema, table_name = table, values = ','.join(temp_data))
    logging.info(query)
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
    schedule_interval = '0 11 * * *',
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    params = {
        'api_key': Variable.get('weather_api_key'),
        'lat': 37.5665,
        'lon': 126.9780
    },
    dag = open_weather_dag
)

transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    dag = open_weather_dag
)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {
        'schema': 'jiyoon0043',
        'table': 'weather_forecast'
    },
    dag = open_weather_dag
)