import logging

import pendulum
from airflow.decorators import dag, task
from cdm.settlement_report_loader import ReportLoader
from cdm.courier_ledger_loader import CourierLedgerLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2023, 1, 4, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'cdm'],
    is_paused_upon_creation=True
)
def sprint5_cdm_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="report_load")
    def load_report():
        loader = ReportLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_objects()

    @task(task_id="courier_ledger_load")
    def load_courier_ledger():
        loader = CourierLedgerLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_objects()


    settlement_report = load_report()
    courier_ledger = load_courier_ledger()

    settlement_report >> courier_ledger


cdm_dag = sprint5_cdm_dag()
