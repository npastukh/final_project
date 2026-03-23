# Final Project

Команда 6
- Ракутундрамбула Даниелла Александра
- Пастух Наталья Юрьевна

Итоговый проект по курсу: мы взяли исходные parquet-файлы с данными по доставкам, нормализовали их, загрузили в PostgreSQL и собрали итоговую витрину через Airflow и PySpark.

## Что лежит в проекте

- `docker-compose.yml` — запуск всего стенда
- `airflow/` — DAG-и, ETL-скрипты, Dockerfile и окружение для Airflow
- `sql/` — DDL и проверочные запросы
- `data/raw/` — исходные parquet-файлы
- `schema_report.pdf` — PDF с описанием нормализации, схемой и DDL
- `pgadmin/servers.json` — преднастроенное подключение для pgAdmin

## Что поднимается в Docker

- `PostgreSQL` — основная база данных
- `pgAdmin` — чтобы смотреть таблицы и писать проверочные запросы
- `Apache Airflow` — оркестрация пайплайна

## Быстрый запуск

```bash
docker compose up --build
```

После запуска будут доступны:

- Airflow: `http://localhost:8080` (`admin` / `admin`)
- pgAdmin: `http://localhost:5050` (`admin@example.com` / `admin`)
- PostgreSQL: `localhost:5432` (`project_user` / `project_password`)

## Что получилось в базе

Используются три схемы:

- `stage` — сырые данные после загрузки parquet
- `core` — нормализованная реляционная модель
- `mart` — итоговая витрина

Основные таблицы нормализованной модели:

- `core.dim_city`
- `core.dim_category`
- `core.dim_payment_type`
- `core.dim_cancellation_reason`
- `core.dim_user`
- `core.dim_driver`
- `core.dim_delivery_address`
- `core.dim_store`
- `core.dim_product`
- `core.fct_order`
- `core.fct_order_driver`
- `core.fct_order_line`

Итоговая витрина:

- `mart.order_metrics_daily` собирается по дате, городу и магазину и содержит:

- количество созданных, доставленных и отмененных заказов
- оборот, выручку, расходы и прибыль
- количество покупателей
- средний чек
- заказы на покупателя
- выручку на покупателя
- количество заказов со сменой курьера
- количество активных курьеров
