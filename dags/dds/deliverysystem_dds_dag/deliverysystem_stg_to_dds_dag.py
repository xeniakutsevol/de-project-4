import logging

import pendulum
from airflow.decorators import dag, task
from dds.deliverysystem_dds_dag.couriers_loader import CouriersLoader
from dds.deliverysystem_dds_dag.dm_deliveries_loader import DmDeliveriesLoader
from dds.deliverysystem_dds_dag.fct_deliveries_loader import FctDeliveriesLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2023, 1, 11, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'dds', 'deliverysystem'],
    is_paused_upon_creation=True
)
def sprint5_dds_deliverysystem_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="couriers_load")
    def load_couriers():
        loader = CouriersLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_objects()

    @task(task_id="dm_deliveries_load")
    def load_dm_deliveries():
        loader = DmDeliveriesLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_objects()

    @task(task_id="fct_deliveries_load")
    def load_fct_deliveries():
        loader = FctDeliveriesLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_objects()

    couriers = load_couriers()
    dm_deliveries = load_dm_deliveries()
    fct_deliveries = load_fct_deliveries()

    couriers >> dm_deliveries >> fct_deliveries


stg_to_dds_deliverysystem_dag = sprint5_dds_deliverysystem_dag()
