# DWH для нескольких источников

### Описание
В рамках проекта построено многослойное хранилище данных **компании-агрегатора ресторанов** для предоставления аналитических данных по детализации заказов, общим суммам задолженности перед ресторанами-партнерами, расчетов с курьерами. Хранилище состоит из: STG-слоя со сбором данных из различных источников (**система оплаты баллами/PostgreSQL**, **система обработки заказов/MongoDB**, **система курьерской доставки/API**), DDS-слоя с моделью «снежинка» и нормализацией до 3НФ, CDM-слоя с целевыми витринами.
Наполнение хранилилища автоматизировано с помощью DAGs Airflow. Реализована инкрементальная загрузка данных.

### Структура репозитория
- `/dags/` содержит DAGs Airflow и вспомогательные программные модули для наполнения хранилища данных.
- `api-entities.md` Проектирование слоев DDS, CDM системы курьерской доставки.
- `project_ddl.sql` DDL-скрипты системы курьерской доставки.
- `docker-compose.yml` docker-compose файл для развертывания Airflow.

### Как запустить контейнер
Запустите локально команду:

```
docker run \
-d \
-p 3000:3000 \
-p 3002:3002 \
-p 15432:5432 \
--mount src=airflow_sp5,target=/opt/airflow \
--mount src=lesson_sp5,target=/lessons \
--mount src=db_sp5,target=/var/lib/postgresql/data \
--name=de-sprint-5-server-local \
sindb/de-pg-cr-af:latest
```

После того как запустится контейнер, вам будут доступны:
- Airflow
	- `localhost:3000/airflow`
- БД
	- `jovyan:jovyan@localhost:15432/de`
