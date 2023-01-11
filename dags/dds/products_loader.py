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
    restaurant_id: int
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime


class OriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, objects_threshold: int, limit: int) -> List[Obj]:
        with self._db.client().cursor(row_factory=class_row(Obj)) as cur:
            cur.execute(
                """
                    select product_id, product_name, product_price, min(active_from) as active_from, max(active_to) as active_to, restaurant_id from
                    (select
                    json_array_elements(o.object_value::JSON->'order_items')::json->>'id' as product_id,
                    json_array_elements(o.object_value::JSON->'order_items')::json->>'name' as product_name,
                    (json_array_elements(o.object_value::JSON->'order_items')::json->>'price')::numeric as product_price,
                    o.update_ts as active_from,
                    '2099-12-31 00:00:00.000'::timestamp as active_to,
                    d.id as restaurant_id
                    from stg.ordersystem_orders o
                    join dds.dm_restaurants d
                    on d.restaurant_id=(o.object_value::JSON->'restaurant')::json->>'id') q
                    --WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    group by product_id, product_name, product_price, restaurant_id
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
                    INSERT INTO dds.dm_products (product_id, product_name, product_price, active_from, active_to, restaurant_id)
                    VALUES (%(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s, %(restaurant_id)s)
                    ON CONFLICT (product_id) DO UPDATE
                    SET
                        product_name = EXCLUDED.product_name,
                        product_price = EXCLUDED.product_price,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to,
                        restaurant_id = EXCLUDED.restaurant_id;
                """,
                {
                    "product_id": object.product_id,
                    "product_name": object.product_name,
                    "product_price": object.product_price,
                    "active_from": object.active_from,
                    "active_to": object.active_to,
                    "restaurant_id": object.restaurant_id
                },
            )


class ProductsLoader:
    WF_KEY = "products_stg_to_dds_workflow"
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