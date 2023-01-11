import logging

import pendulum
from airflow.decorators import dag, task
from dds.users_loader import UserLoader
from dds.restaurants_loader import RestaurantLoader
from dds.timestamp_loader import TimestampLoader
from dds.products_loader import ProductsLoader
from dds.orders_loader import OrdersLoader
from dds.product_sales_loader import ProductSalesLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2023, 1, 4, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_dds_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    #origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="users_load")
    def load_users():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = UserLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()  # Вызываем функцию, которая перельет данные.

    @task(task_id="restaurants_load")
    def load_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = RestaurantLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_restaurants()  # Вызываем функцию, которая перельет данные.

    @task(task_id="timestamps_load")
    def load_timestamps():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = TimestampLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_timestamps()  # Вызываем функцию, которая перельет данные.

    @task(task_id="products_load")
    def load_products():
        loader = ProductsLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_objects()

    @task(task_id="orders_load")
    def load_orders():
        loader = OrdersLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_objects()

    @task(task_id="product_sales_load")
    def load_product_sales():
        loader = ProductSalesLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_objects()

    # Инициализируем объявленные таски.
    users_dict = load_users()
    restaurants_dict = load_restaurants()
    timestamps_dict = load_timestamps()
    products = load_products()
    orders = load_orders()
    product_sales = load_product_sales()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    users_dict >> restaurants_dict >> timestamps_dict >> products >> orders >> product_sales # type: ignore


stg_to_dds_dag = sprint5_dds_dag()
