from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class UserObj(BaseModel):
    id: int
    user_id: str
    user_name: str
    user_login: str


class UserOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_users(self, users_threshold: int, limit: int) -> List[UserObj]:
        with self._db.client().cursor(row_factory=class_row(UserObj)) as cur:
            cur.execute(
                """
                    select id,
                    object_value::json ->> '_id' as user_id,
                    object_value::json ->> 'name' as user_name,
                    object_value::json ->> 'login' as user_login
                    from stg.ordersystem_users
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": users_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class UserDestRepository:

    def insert_user(self, conn: Connection, user: UserObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_users(id, user_id, user_name, user_login)
                    VALUES (%(id)s, %(user_id)s, %(user_name)s, %(user_login)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        user_id = EXCLUDED.user_id,
                        user_name = EXCLUDED.user_name,
                        user_login = EXCLUDED.user_login;
                """,
                {
                    "id": user.id,
                    "user_id": user.user_id,
                    "user_name": user.user_name,
                    "user_login": user.user_login
                },
            )


class UserLoader:
    WF_KEY = "users_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY_USERS = "last_loaded_id"
    BATCH_LIMIT = 100000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = UserOriginRepository(pg_origin)
        self.stg = UserDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_users(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY_USERS: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY_USERS]
            load_queue = self.origin.list_users(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for user in load_queue:
                self.stg.insert_user(conn, user)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY_USERS] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY_USERS]}")