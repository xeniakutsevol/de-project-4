from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, time, date



class TimestampDdsObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    time: time
    date: date


class TimestampOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamps(self, timestamps_threshold: int, limit: int) -> List[TimestampDdsObj]:
        with self._db.client().cursor(row_factory=class_row(TimestampDdsObj)) as cur:
            cur.execute(
                """
                    select
                    id,
                    max(((object_value::JSON->'date')::varchar)::timestamp) ts,
                    extract(year from max(((object_value::JSON->'date')::varchar)::timestamp)) as year,
                    extract(month from max(((object_value::JSON->'date')::varchar)::timestamp)) as month,
                    extract(day from max(((object_value::JSON->'date')::varchar)::timestamp)) as day,
                    max(((object_value::JSON->'date')::varchar)::time) as time,
                    max(((object_value::JSON->'date')::varchar)::date) as date
                    from stg.ordersystem_orders
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    group by id
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": timestamps_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class TimestampDestRepository:

    def insert_timestamp(self, conn: Connection, timestamp: TimestampDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(id, ts, year, month, day, time, date)
                    VALUES (%(id)s, %(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        ts = EXCLUDED.ts,
                        year = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        time = EXCLUDED.time,
                        date = EXCLUDED.date;
                """,
                {
                    "id": timestamp.id,
                    "ts": timestamp.ts,
                    "year": timestamp.year,
                    "month": timestamp.month,
                    "day": timestamp.day,
                    "time": timestamp.time,
                    "date": timestamp.date
                },
            )


class TimestampLoader:
    WF_KEY = "timestamps_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = TimestampOriginRepository(pg_origin)
        self.stg = TimestampDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_timestamps(self):
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
            load_queue = self.origin.list_timestamps(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} objects to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for timestamp in load_queue:
                self.stg.insert_timestamp(conn, timestamp)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")