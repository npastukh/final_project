# Нормализация и схема

## 1. Что было в исходных данных

Исходный parquet содержит денормализованный набор строк по доставкам. В одной строке одновременно лежат:

- атрибуты заказа
- атрибуты покупателя
- атрибуты курьера
- атрибуты магазина
- атрибуты товара
- признаки отмен и замен

Это создает стандартные аномалии:

- обновления: телефон пользователя, курьера или адрес магазина дублируются в миллионах строк
- вставки: нельзя корректно завести сущность без строки заказа
- удаления: удаление последней строки заказа может удалить описание товара или магазина

## 2. Почему выбрана 3NF

Выбрана 3NF, потому что она:

- устраняет повторяющиеся описательные атрибуты
- сохраняет понятную и практичную модель для ETL
- не требует искусственной денормализации в ядре
- хорошо подходит под последующее построение аналитической витрины

BCNF здесь не дает заметного практического выигрыша относительно 3NF, потому что основные функциональные зависимости и так закрываются отдельными сущностями, а сложность будет выше только в части адресов и замен.

## 3. Выявленные функциональные зависимости

По данным были проверены и подтверждены зависимости:

- `user_id -> user_phone`
- `driver_id -> driver_phone`
- `store_id -> store_address`
- `item_id -> item_title, item_category`

Также было выявлено:

- один заказ может иметь нескольких курьеров
- в одном заказе один и тот же `item_id` встречается несколько раз
- `item_replaced_id` всегда ссылается на другой товар из того же заказа, но исходник не дает надежного идентификатора пары "исходный товар -> товар-замена"

## 4. Декомпозиция

### Справочники

- `dim_city`
- `dim_category`
- `dim_payment_type`
- `dim_cancellation_reason`
- `dim_user`
- `dim_driver`
- `dim_delivery_address`
- `dim_store`
- `dim_product`

### Факты

- `fct_order` хранит заказ как бизнес-событие
- `fct_order_driver` хранит назначения курьеров на заказ
- `fct_order_line` хранит строку заказа

## 5. Почему строка заказа вынесена отдельно

Ключ `(order_id, item_id)` не подходит, потому что один и тот же товар внутри заказа встречается несколько раз. Поэтому строка заказа хранится как отдельный факт с суррогатным ключом, а натуральная уникальность обеспечивается через `(source_file_name, source_row_number)`.

Это сохраняет исходную гранулярность и не теряет строки с разными скидками и количествами.

## 6. Почему курьеры вынесены в отдельный факт

В данных есть смены курьера. Если хранить `driver_id` внутри заказа, теряется история назначений и становится невозможно посчитать:

- количество заказов со сменой курьера
- количество активных курьеров

Поэтому сделана таблица `fct_order_driver`.

## 7. Адреса и города

Адрес доставки и адрес магазина не были полноценно нормализованы до улицы/дома, потому что исходник содержит свободный текст и не гарантирует стабильный формат на уровне улиц. При этом город выделяется надежно, поэтому:

- город вынесен в `dim_city`
- полный адрес доставки хранится в `dim_delivery_address`
- магазин хранится в `dim_store` с выделением `store_name`, `city_id` и `store_address_line`

Это компромисс между чистотой модели и надежностью разбора.

## 8. Допущения по заменам

Поле `item_replaced_id` сохранено в `fct_order_line` как аудит-атрибут.

Допущение для витрины:

- заказанное количество берется из `ordered_quantity`
- отмененное количество берется из `canceled_quantity`
- оплачиваемое количество определяется как `net_quantity = ordered_quantity - canceled_quantity`

Причина такого решения: в исходнике нет надежного line-level идентификатора, который позволил бы безошибочно склеить исходный товар и товар-замену в одну бизнес-операцию.

## 9. Допущения по метрикам

- дата витрины = дата создания заказа
- `turnover` считается по заказанному количеству
- `revenue` считается по оплачиваемому количеству только для заказов с `paid_at`
- `expense` интерпретируется как `delivery_cost` для заказов, где доставка уже стартовала
- `profit = revenue - expense`
- активные курьеры считаются как distinct `driver_id`, которые участвовали в заказах соответствующего среза

## 10. Схема связей

```text
dim_city 1---N dim_delivery_address
dim_city 1---N dim_store
dim_category 1---N dim_product
dim_payment_type 1---N fct_order
dim_cancellation_reason 1---N fct_order
dim_user 1---N fct_order
dim_store 1---N fct_order
dim_delivery_address 1---N fct_order
fct_order 1---N fct_order_driver
dim_driver 1---N fct_order_driver
fct_order 1---N fct_order_line
dim_product 1---N fct_order_line
```

## 11. Что попадает в витрину

### `mart.order_metrics_daily`

Срез:

- год
- месяц
- день
- город
- магазин

Метрики:

- оборот
- выручка
- расходы
- прибыль
- количество созданных, доставленных и отмененных заказов
- количество отмен после доставки
- количество отмен по сервисным ошибкам
- количество покупателей
- средний чек
- заказы на покупателя
- выручка на покупателя
- количество заказов со сменой курьера
- количество активных курьеров

## 12. DDL-скрипты

Ниже приведены DDL-скрипты, которые используются в проекте.

### `sql/00_create_databases.sql`

```sql
CREATE DATABASE airflow OWNER project_user;
CREATE DATABASE dwh OWNER project_user;
```

### `sql/10_ddl.sql`

```sql
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS stage.raw_delivery_lines (
    source_file_name TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    order_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    user_phone TEXT NOT NULL,
    address_text TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    paid_at TIMESTAMP NULL,
    delivery_started_at TIMESTAMP NULL,
    delivered_at TIMESTAMP NULL,
    canceled_at TIMESTAMP NULL,
    payment_type TEXT NOT NULL,
    item_id BIGINT NOT NULL,
    item_title TEXT NOT NULL,
    item_category TEXT NOT NULL,
    item_quantity INTEGER NOT NULL,
    item_price NUMERIC(12, 2) NOT NULL,
    item_canceled_quantity INTEGER NOT NULL,
    item_replaced_id BIGINT NULL,
    order_discount NUMERIC(5, 2) NOT NULL,
    item_discount NUMERIC(5, 2) NOT NULL,
    order_cancellation_reason TEXT NULL,
    driver_id BIGINT NOT NULL,
    driver_phone TEXT NOT NULL,
    delivery_cost NUMERIC(12, 2) NOT NULL,
    store_id BIGINT NOT NULL,
    store_address TEXT NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_file_name, source_row_number)
);

CREATE TABLE IF NOT EXISTS core.dim_city (
    city_id SMALLSERIAL PRIMARY KEY,
    city_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS core.dim_category (
    category_id SMALLSERIAL PRIMARY KEY,
    category_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS core.dim_payment_type (
    payment_type_id SMALLSERIAL PRIMARY KEY,
    payment_type_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS core.dim_cancellation_reason (
    cancellation_reason_id SMALLSERIAL PRIMARY KEY,
    cancellation_reason_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS core.dim_user (
    user_id BIGINT PRIMARY KEY,
    user_phone TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS core.dim_driver (
    driver_id BIGINT PRIMARY KEY,
    driver_phone TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS core.dim_delivery_address (
    address_id BIGSERIAL PRIMARY KEY,
    city_id SMALLINT NOT NULL REFERENCES core.dim_city(city_id),
    raw_address_text TEXT NOT NULL UNIQUE,
    address_line TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS core.dim_store (
    store_id BIGINT PRIMARY KEY,
    store_name TEXT NOT NULL,
    city_id SMALLINT NOT NULL REFERENCES core.dim_city(city_id),
    raw_store_address TEXT NOT NULL UNIQUE,
    store_address_line TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS core.dim_product (
    product_id BIGINT PRIMARY KEY,
    product_name TEXT NOT NULL,
    category_id SMALLINT NOT NULL REFERENCES core.dim_category(category_id),
    UNIQUE (product_name, category_id)
);

CREATE TABLE IF NOT EXISTS core.fct_order (
    order_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES core.dim_user(user_id),
    delivery_address_id BIGINT NOT NULL REFERENCES core.dim_delivery_address(address_id),
    store_id BIGINT NOT NULL REFERENCES core.dim_store(store_id),
    payment_type_id SMALLINT NOT NULL REFERENCES core.dim_payment_type(payment_type_id),
    cancellation_reason_id SMALLINT NULL REFERENCES core.dim_cancellation_reason(cancellation_reason_id),
    created_at TIMESTAMP NOT NULL,
    paid_at TIMESTAMP NULL,
    delivery_started_at TIMESTAMP NULL,
    delivered_at TIMESTAMP NULL,
    canceled_at TIMESTAMP NULL,
    order_discount_pct NUMERIC(5, 2) NOT NULL,
    delivery_cost NUMERIC(12, 2) NOT NULL,
    has_driver_change BOOLEAN NOT NULL DEFAULT FALSE,
    source_row_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS core.fct_order_driver (
    order_id BIGINT NOT NULL REFERENCES core.fct_order(order_id),
    assignment_seq SMALLINT NOT NULL,
    driver_id BIGINT NOT NULL REFERENCES core.dim_driver(driver_id),
    is_final_driver BOOLEAN NOT NULL,
    delivery_started_at TIMESTAMP NULL,
    delivered_at TIMESTAMP NULL,
    canceled_at TIMESTAMP NULL,
    PRIMARY KEY (order_id, assignment_seq),
    UNIQUE (order_id, driver_id)
);

CREATE TABLE IF NOT EXISTS core.fct_order_line (
    order_line_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES core.fct_order(order_id),
    product_id BIGINT NOT NULL REFERENCES core.dim_product(product_id),
    replaced_product_id BIGINT NULL REFERENCES core.dim_product(product_id),
    source_file_name TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    ordered_quantity INTEGER NOT NULL CHECK (ordered_quantity >= 0),
    canceled_quantity INTEGER NOT NULL CHECK (canceled_quantity >= 0 AND canceled_quantity <= ordered_quantity),
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    item_discount_pct NUMERIC(5, 2) NOT NULL CHECK (item_discount_pct >= 0),
    UNIQUE (source_file_name, source_row_number)
);

CREATE INDEX IF NOT EXISTS idx_stage_raw_order_id
    ON stage.raw_delivery_lines(order_id);

CREATE INDEX IF NOT EXISTS idx_core_order_created_at
    ON core.fct_order(created_at);

CREATE INDEX IF NOT EXISTS idx_core_order_store_id
    ON core.fct_order(store_id);

CREATE INDEX IF NOT EXISTS idx_core_order_line_order_id
    ON core.fct_order_line(order_id);

CREATE INDEX IF NOT EXISTS idx_core_order_line_product_id
    ON core.fct_order_line(product_id);

CREATE TABLE IF NOT EXISTS mart.order_metrics_daily (
    metric_date DATE NOT NULL,
    metric_year INTEGER NOT NULL,
    metric_month INTEGER NOT NULL,
    metric_day INTEGER NOT NULL,
    city_id SMALLINT NOT NULL,
    city_name TEXT NOT NULL,
    store_id BIGINT NOT NULL,
    store_name TEXT NOT NULL,
    turnover_amount NUMERIC(14, 2) NOT NULL,
    revenue_amount NUMERIC(14, 2) NOT NULL,
    expense_amount NUMERIC(14, 2) NOT NULL,
    profit_amount NUMERIC(14, 2) NOT NULL,
    created_orders_count INTEGER NOT NULL,
    delivered_orders_count INTEGER NOT NULL,
    canceled_orders_count INTEGER NOT NULL,
    canceled_after_delivery_count INTEGER NOT NULL,
    service_error_cancellations_count INTEGER NOT NULL,
    buyers_count INTEGER NOT NULL,
    average_check NUMERIC(14, 2) NOT NULL,
    orders_per_buyer NUMERIC(14, 4) NOT NULL,
    revenue_per_buyer NUMERIC(14, 2) NOT NULL,
    orders_with_driver_change_count INTEGER NOT NULL,
    active_couriers_count INTEGER NOT NULL,
    refreshed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (metric_date, store_id)
);
```
