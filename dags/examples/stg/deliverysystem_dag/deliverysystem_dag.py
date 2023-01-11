import json
import logging
import time
from datetime import datetime, date, timedelta
import requests
from airflow import DAG
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder, IncrementCounter, json2str
from psycopg import Connection
from typing import Any


task_logger = logging.getLogger("airflow.task")

def save_object(conn: Connection, table: str, id: str, val: Any):
    str_val = json2str(val)
    with conn.cursor() as cur:
        query = f"""
                INSERT INTO {table} (object_id, object_value)
                VALUES (%(id)s, %(val)s)
                ON CONFLICT (object_id) DO UPDATE
                SET
                    object_value = EXCLUDED.object_value;
            """
        cur.execute(
            query,
            {
                "id": id,
                "val": str_val
            }
        )


def load_couriers(n, url, counter, conn, headers):
    for i in range(0,n):
        response = requests.get(f"{url}/couriers?sort=id&sort_direction=asc&limit=50&offset={counter._value}", headers=headers)
        response.raise_for_status()
        increment = json.loads(response.content)
        if increment:
            task_logger.info(f"Получен инкремент: {increment}")
            for row in increment:
                save_object(conn, str('stg.deliverysystem_couriers'), str(row["_id"]), row)
            conn.commit()
            task_logger.info(f"Сохранено записей: {counter.new_value()}")
            load_couriers(n-1, url, counter, conn, headers)


def load_deliveries(n, url, counter, conn, headers):
    for i in range(0,n):
        response = requests.get(f"{url}/deliveries?sort=id&sort_direction=asc&limit=50&offset={counter._value}&from={(date.today()-timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')}&to={(date.today()-timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')}",
                                headers=headers)
        response.raise_for_status()
        increment = json.loads(response.content)
        if increment:
            task_logger.info(f"Получен инкремент: {increment}")
            for row in increment:
                save_object(conn, str('stg.deliverysystem_deliveries'), str(row["delivery_id"]), row)
            conn.commit()
            task_logger.info(f"Сохранено записей: {counter.new_value()}")
            load_deliveries(n-1, url, counter, conn, headers)

@dag(
    schedule_interval='0 0 * * 1',
    start_date=pendulum.datetime(2023, 1, 9, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'deliverysystem'],
    is_paused_upon_creation=True
)
def deliverysystem_stg_dag():
    postgres_hook = PostgresHook("PG_WAREHOUSE_CONNECTION")
    conn = postgres_hook.get_conn()

    base_url = Variable.get("api_url")
    nickname = Variable.get("nickname")
    cohort = Variable.get("cohort")
    api_key = Variable.get("api_key")

    headers = {
        "X-Nickname": nickname,
        "X-Cohort": cohort,
        "X-API-KEY": api_key
    }

    courier_counter = IncrementCounter()
    delivery_counter = IncrementCounter()

    rec_depth=10

    @task()
    def load_stg_couriers():
        load_couriers(rec_depth, base_url, courier_counter, conn, headers)

    @task()
    def load_stg_deliveries():
        load_deliveries(rec_depth, base_url, delivery_counter, conn, headers)

    stg_couriers_loader=load_stg_couriers()
    stg_deliveries_loader=load_stg_deliveries()

    stg_couriers_loader >> stg_deliveries_loader

sprint5_deliverysystem_stg_dag = deliverysystem_stg_dag()





