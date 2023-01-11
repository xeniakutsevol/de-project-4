from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import date



class Obj(BaseModel):
    courier_id: int
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float


class OriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, objects_threshold: int, limit: int) -> List[Obj]:
        with self._db.client().cursor(row_factory=class_row(Obj)) as cur:
            cur.execute(
                """
                    select courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum,
                    rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum,
                    courier_order_sum+courier_tips_sum*0.95 as courier_reward_sum from
                    (select
                    c.id as courier_id,
                    c.courier_name,
                    t.year as settlement_year,
                    t.month as settlement_month,
                    count(f.order_id) as orders_count,
                    sum(f.sum) as orders_total_sum,
                    avg(f.rate) as rate_avg,
                    sum(f.sum)*0.25 as order_processing_fee,
                    case when avg(f.rate)<4 then sum(greatest(f.sum*0.05, 100))
                    when avg(f.rate)<=4 or avg(f.rate)<4.5 then sum(greatest(f.sum*0.07, 150))
                    when avg(f.rate)<=4.5 or avg(f.rate)<4.9 then sum(greatest(f.sum*0.08, 175))
                    when avg(f.rate)>=4.9 then sum(greatest(f.sum*0.1, 200)) end as courier_order_sum,
                    sum(f.tip_sum) as courier_tips_sum
                    --courier_order_sum+courier_tips_sum*0.95 as courier_reward_sum
                    from dds.fct_deliveries f
                    join dds.dm_couriers c
                    on f.courier_id=c.id
                    join dds.dm_timestamps t
                    on f.timestamp_id=t.id
                    group by c.id, c.courier_name, t.year, t.month) q
                    --WHERE id > %(threshold)s
                    --ORDER BY id ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": objects_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DestRepository:

    def insert_object(self, conn: Connection, object: Obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger(courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum,
                    rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
                    VALUES (%(courier_id)s, %(courier_name)s, %(settlement_year)s, %(settlement_month)s, %(orders_count)s, %(orders_total_sum)s,
                    %(rate_avg)s, %(order_processing_fee)s, %(courier_order_sum)s, %(courier_tips_sum)s, %(courier_reward_sum)s)
                    ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
                    SET
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        rate_avg = EXCLUDED.rate_avg,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        courier_order_sum = EXCLUDED.courier_order_sum,
                        courier_tips_sum = EXCLUDED.courier_tips_sum,
                        courier_reward_sum = EXCLUDED.courier_reward_sum;
                """,
                {
                    "courier_id": object.courier_id,
                    "courier_name": object.courier_name,
                    "settlement_year": object.settlement_year,
                    "settlement_month": object.settlement_month,
                    "orders_count": object.orders_count,
                    "orders_total_sum": object.orders_total_sum,
                    "rate_avg": object.rate_avg,
                    "order_processing_fee": object.order_processing_fee,
                    "courier_order_sum": object.courier_order_sum,
                    "courier_tips_sum": object.courier_tips_sum,
                    "courier_reward_sum": object.courier_reward_sum
                },
            )


class CourierLedgerLoader:
    WF_KEY = "cdm_courier_ledger_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OriginRepository(pg_origin)
        self.stg = DestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_objects(self):
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_objects(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} objects to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for obj in load_queue:
                self.stg.insert_object(conn, obj)

            # wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            # wf_setting_json = json2str(wf_setting.workflow_settings)
            # self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")