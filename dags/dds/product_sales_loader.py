from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime



class Obj(BaseModel):
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class OriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, objects_threshold: int, limit: int) -> List[Obj]:
        with self._db.client().cursor(row_factory=class_row(Obj)) as cur:
            cur.execute(
                """
                    with t as (
                    select
                    ddso.id as order_id,
                    o.object_id as id,
                    json_array_elements(o.object_value::JSON->'order_items')::json->>'id' as product_id,
                    (json_array_elements(o.object_value::JSON->'order_items')::json->>'quantity')::numeric as count,
                    (json_array_elements(o.object_value::JSON->'order_items')::json->>'price')::numeric as price,
                    (json_array_elements(o.object_value::JSON->'order_items')::json->>'quantity')::numeric *
                    (json_array_elements(o.object_value::JSON->'order_items')::json->>'price')::numeric as total_sum
                    from stg.ordersystem_orders o
                    join dds.dm_orders ddso
                    on o.id=ddso.id
                    where (o.object_value::JSON->>'final_status')::varchar!='CANCELLED'
                    ),
                    tt as (
                    select
                    (b.event_value::json->>'order_id')::varchar as order_id,
                    (json_array_elements(b.event_value::JSON->'product_payments')::json->>'product_id')::varchar as product_id,
                    (json_array_elements(b.event_value::JSON->'product_payments')::json->>'bonus_payment')::numeric as bonus_payment,
                    (json_array_elements(b.event_value::JSON->'product_payments')::json->>'bonus_grant')::numeric as bonus_grant
                    from stg.bonussystem_events b
                    where event_type='bonus_transaction'
                    )
                    select t.order_id, p.id as product_id, t.count, t.price, t.total_sum, tt.bonus_payment, tt.bonus_grant
                    from t
                    join dds.dm_products p
                    on t.product_id=p.product_id
                    join tt
                    on t.id=tt.order_id and t.product_id=tt.product_id
                    --WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    --ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
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
                    INSERT INTO dds.fct_product_sales (order_id, product_id, "count", price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(order_id)s, %(product_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                    ON CONFLICT (order_id, product_id) DO UPDATE
                    SET
                        count = EXCLUDED.count,
                        price = EXCLUDED.price,
                        total_sum = EXCLUDED.total_sum,
                        bonus_payment = EXCLUDED.bonus_payment,
                        bonus_grant = EXCLUDED.bonus_grant;
                """,
                {
                    "order_id": object.order_id,
                    "product_id": object.product_id,
                    "count": object.count,
                    "price": object.price,
                    "total_sum": object.total_sum,
                    "bonus_payment": object.bonus_payment,
                    "bonus_grant": object.bonus_grant
                },
            )


class ProductSalesLoader:
    WF_KEY = "product_sales_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OriginRepository(pg_origin)
        self.stg = DestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_objects(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_objects(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} objects to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for obj in load_queue:
                self.stg.insert_object(conn, obj)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            # wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            # wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            # self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")