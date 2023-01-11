from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel



class Obj(BaseModel):
    id: int
    delivery_id: int
    rate: int
    sum: float
    tip_sum: float
    order_id: int
    courier_id: int
    timestamp_id: int


class OriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, objects_threshold: int, limit: int) -> List[Obj]:
        with self._db.client().cursor(row_factory=class_row(Obj)) as cur:
            cur.execute(
                """
                    select
                    d.id,
                    dd.id as delivery_id,
                    (d.object_value::json->>'rate')::integer as rate,
                    (d.object_value::json->>'sum')::decimal(14,2) as sum,
                    (d.object_value::json->>'tip_sum')::decimal(14,2) as tip_sum,
                    o.id as order_id,
                    c.id as courier_id,
                    t.id as timestamp_id
                    from stg.deliverysystem_deliveries d
                    join dds.dm_deliveries dd
                    on d.object_id=dd.delivery_id
                    join dds.dm_couriers c
                    on (d.object_value::json->>'courier_id')=c.courier_id
                    join dds.dm_orders o
                    on (d.object_value::json->>'order_id')=o.order_key
                    join dds.dm_timestamps t
                    on date_trunc('seconds', (d.object_value::json->>'order_ts')::timestamp)=t.ts
                    WHERE d.id > %(threshold)s
                    ORDER BY d.id ASC
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
                    INSERT INTO dds.fct_deliveries(id, delivery_id, rate, sum, tip_sum, order_id, courier_id, timestamp_id)
                    VALUES (%(id)s, %(delivery_id)s, %(rate)s, %(sum)s, %(tip_sum)s, %(order_id)s, %(courier_id)s, %(timestamp_id)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        delivery_id = EXCLUDED.delivery_id,
                        rate = EXCLUDED.rate,
                        sum = EXCLUDED.sum,
                        tip_sum = EXCLUDED.tip_sum,
                        order_id = EXCLUDED.order_id,
                        courier_id = EXCLUDED.courier_id,
                        timestamp_id = EXCLUDED.timestamp_id;
                """,
                {
                    "id": object.id,
                    "delivery_id": object.delivery_id,
                    "rate": object.rate,
                    "sum": object.sum,
                    "tip_sum": object.tip_sum,
                    "order_id": object.order_id,
                    "courier_id": object.courier_id,
                    "timestamp_id": object.timestamp_id
                },
            )


class FctDeliveriesLoader:
    WF_KEY = "fct_deliveries_stg_to_dds_workflow"
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

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")