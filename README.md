# Final Delivery Project

Итоговый проект по нормализации данных доставок и построению аналитической витрины в PostgreSQL с оркестрацией через Airflow и расчетом витрины в PySpark.

## Что внутри

- `docker-compose.yml` для PostgreSQL, pgAdmin и Apache Airflow.
- `airflow/Dockerfile` с Java, PySpark и библиотеками для ETL.
- `airflow/dags` с отдельными идемпотентными DAG-ами и DAG-оркестратором.
- `airflow/scripts` со скриптами загрузки и построения витрины.
- `sql/10_ddl.sql` с DDL для `stage`, `core` и `mart`.
- `data/raw` с parquet-файлами, уже включенными в репозиторий.
- `docs/schema_notes.md` и `docs/schema_report.pdf` с пояснением схемы, нормализации и допущений.

## Архитектура

Пайплайн разделен на три отдельных процесса:

1. `bootstrap_schema` создает схемы и таблицы в PostgreSQL.
2. `load_core` читает parquet, загружает staging и раскладывает данные в нормализованную модель `core`.
3. `build_marts` читает `core` через JDBC в PySpark и пишет итоговую витрину в `mart`.

Для автоматического воспроизведения есть DAG `orchestrate_pipeline`, который запускается один раз и последовательно триггерит остальные DAG-и.
Он настроен как `@once`, поэтому после первого `docker compose up --build` пайплайн стартует автоматически.

## Быстрый старт

```bash
docker compose up --build
```

После старта будут доступны:

- Airflow: `http://localhost:8080`
- pgAdmin: `http://localhost:5050`
- PostgreSQL: `localhost:5432`

Учетные данные:

- Airflow: `admin` / `admin`
- pgAdmin: `admin@example.com` / `admin`
- PostgreSQL: `project_user` / `project_password`

## Что создается в БД

Схемы:

- `stage` для сырой загрузки parquet без потери полей.
- `core` для нормализованной модели.
- `mart` для аналитической витрины.

Основные таблицы `core`:

- `dim_city`
- `dim_category`
- `dim_payment_type`
- `dim_cancellation_reason`
- `dim_user`
- `dim_driver`
- `dim_delivery_address`
- `dim_store`
- `dim_product`
- `fct_order`
- `fct_order_driver`
- `fct_order_line`

Витрина:

- `mart.order_metrics_daily`

## Логика расчета метрик

Гранулярность витрины: дата создания заказа `created_at::date`.

Формулы:

- `turnover_amount` = сумма `ordered_quantity * unit_price * (1 - item_discount_pct/100) * (1 - order_discount_pct/100)`
- `revenue_amount` = сумма `net_quantity * unit_price * (1 - item_discount_pct/100) * (1 - order_discount_pct/100)` только для заказов с ненулевым `paid_at`
- `net_quantity` = `ordered_quantity - canceled_quantity`
- `expense_amount` = `delivery_cost` только для заказов, где доставка уже стартовала
- `profit_amount` = `revenue_amount - expense_amount`

## Почему схема именно такая

Использована 3NF с прагматичным исключением только для итоговой витрины:

- справочные сущности вынесены отдельно, потому что `user_id`, `driver_id`, `store_id`, `item_id` стабильно определяют свои атрибуты
- `city` вынесен в отдельный справочник, потому что повторяется и в адресе доставки, и в адресе магазина
- строка заказа хранится отдельно как `fct_order_line`, потому что в одном заказе один и тот же `item_id` встречается несколько раз
- факт назначения курьера хранится отдельно как `fct_order_driver`, потому что есть заказы со сменой курьера

Подробные комментарии к декомпозиции и допущениям описаны в [docs/schema_notes.md](docs/schema_notes.md).

## Полезные запросы

Примеры лежат в `sql/20_validation_queries.sql`.

## Повторный пересчет

Если нужно заново пересчитать все вручную через Airflow:

1. Открыть Airflow UI.
2. Запустить `orchestrate_pipeline`.
3. Дождаться успешного выполнения трех DAG-ов.

Либо можно пересоздать все контейнеры:

```bash
docker compose down -v
docker compose up --build
```
