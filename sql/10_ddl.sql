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
