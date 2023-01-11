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
    id: int
    user_id: int
    restaurant_id: int
    timestamp_id: int
    order_key: str
    order_status: str


class OriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, objects_threshold: int, limit: int) -> List[Obj]:
        with self._db.client().cursor(row_factory=class_row(Obj)) as cur:
            cur.execute(
                """
                    select
                    o.id,
                    o.object_id as order_key,
                    (o.object_value::JSON->>'final_status') as order_status,
                    d.id as restaurant_id,
                    t.id as timestamp_id,
                    u.id as user_id
                    from stg.ordersystem_orders o
                    join dds.dm_restaurants d
                    on d.restaurant_id=(o.object_value::JSON->'restaurant')::json->>'id'
                    join dds.dm_timestamps t
                    on o.id=t.id
                    join dds.dm_users u
                    on u.user_id=(o.object_value::JSON->'user')::json->>'id'
                    WHERE o.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY o.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
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
                    INSERT INTO dds.dm_orders (id, order_key, order_status, restaurant_id, timestamp_id, user_id)
                    VALUES (%(id)s, %(order_key)s, %(order_status)s, %(restaurant_id)s, %(timestamp_id)s, %(user_id)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        order_key = EXCLUDED.order_key,
                        order_status = EXCLUDED.order_status,
                        restaurant_id = EXCLUDED.restaurant_id,
                        timestamp_id = EXCLUDED.timestamp_id,
                        user_id = EXCLUDED.user_id;
                """,
                {
                    "id": object.id,
                    "order_key": object.order_key,
                    "order_status": object.order_status,
                    "restaurant_id": object.restaurant_id,
                    "timestamp_id": object.timestamp_id,
                    "user_id": object.user_id
                },
            )


class OrdersLoader:
    WF_KEY = "orders_stg_to_dds_workflow"
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
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")