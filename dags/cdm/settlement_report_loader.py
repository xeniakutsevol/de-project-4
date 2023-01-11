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
    restaurant_id: str
    restaurant_name: str
    settlement_date: date
    orders_count: int
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float


class OriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, objects_threshold: int, limit: int) -> List[Obj]:
        with self._db.client().cursor(row_factory=class_row(Obj)) as cur:
            cur.execute(
                """
                    with prep as (
                    select
                    o.id as order_id,
                    o.restaurant_id,
                    r.restaurant_name,
                    t.date as settlement_date,
                    sum(f.total_sum) as total_sum,
                    sum(f.bonus_payment) as bonus_payment,
                    sum(f.bonus_grant) as bonus_grant
                    from dds.dm_orders o
                    join dds.dm_restaurants r
                    on o.restaurant_id=r.id
                    join dds.dm_timestamps t
                    on o.timestamp_id=t.id
                    join dds.fct_product_sales f
                    on o.id=f.order_id
                    where o.order_status='CLOSED'
                    group by o.id, o.restaurant_id, r.restaurant_name, t.date)
                    select
                    restaurant_id,
                    restaurant_name,
                    settlement_date,
                    count(order_id) as orders_count,
                    sum(total_sum) as orders_total_sum,
                    sum(bonus_payment) as orders_bonus_payment_sum,
                    sum(bonus_grant) as orders_bonus_granted_sum,
                    sum(total_sum)*0.25 as order_processing_fee,
                    (sum(total_sum)*0.75 - sum(bonus_payment)) as restaurant_reward_sum
                    from prep
                    group by restaurant_id,
                    restaurant_name,
                    settlement_date
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
                    INSERT INTO cdm.dm_settlement_report(restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(settlement_date)s, %(orders_count)s, %(orders_total_sum)s, %(orders_bonus_payment_sum)s, %(orders_bonus_granted_sum)s, %(order_processing_fee)s, %(restaurant_reward_sum)s)
                    ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
                    SET
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                        orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                """,
                {
                    "restaurant_id": object.restaurant_id,
                    "restaurant_name": object.restaurant_name,
                    "settlement_date": object.settlement_date,
                    "orders_count": object.orders_count,
                    "orders_total_sum": object.orders_total_sum,
                    "orders_bonus_payment_sum": object.orders_bonus_payment_sum,
                    "orders_bonus_granted_sum": object.orders_bonus_granted_sum,
                    "order_processing_fee": object.order_processing_fee,
                    "restaurant_reward_sum": object.restaurant_reward_sum
                },
            )


class ReportLoader:
    WF_KEY = "settlement_cdm_workflow"
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