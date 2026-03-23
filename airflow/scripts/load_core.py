from common import get_dwh_connection


CORE_REFRESH_SQL = """
TRUNCATE TABLE
    core.fct_order_driver,
    core.fct_order_line,
    core.fct_order,
    core.dim_delivery_address,
    core.dim_store,
    core.dim_product,
    core.dim_driver,
    core.dim_user,
    core.dim_cancellation_reason,
    core.dim_payment_type,
    core.dim_category,
    core.dim_city
RESTART IDENTITY CASCADE;

INSERT INTO core.dim_city (city_name)
SELECT DISTINCT city_name
FROM (
    SELECT BTRIM(SPLIT_PART(address_text, ',', 1)) AS city_name
    FROM stage.raw_delivery_lines
    UNION
    SELECT BTRIM(SPLIT_PART(store_address, ',', 2)) AS city_name
    FROM stage.raw_delivery_lines
) cities
ORDER BY city_name;

INSERT INTO core.dim_category (category_name)
SELECT DISTINCT item_category
FROM stage.raw_delivery_lines
ORDER BY item_category;

INSERT INTO core.dim_payment_type (payment_type_name)
SELECT DISTINCT payment_type
FROM stage.raw_delivery_lines
ORDER BY payment_type;

INSERT INTO core.dim_cancellation_reason (cancellation_reason_name)
SELECT DISTINCT order_cancellation_reason
FROM stage.raw_delivery_lines
WHERE order_cancellation_reason IS NOT NULL
ORDER BY order_cancellation_reason;

INSERT INTO core.dim_user (user_id, user_phone)
SELECT DISTINCT user_id, user_phone
FROM stage.raw_delivery_lines
ORDER BY user_id;

INSERT INTO core.dim_driver (driver_id, driver_phone)
SELECT DISTINCT driver_id, driver_phone
FROM stage.raw_delivery_lines
ORDER BY driver_id;

INSERT INTO core.dim_delivery_address (city_id, raw_address_text, address_line)
SELECT
    city.city_id,
    address.address_text,
    BTRIM(REGEXP_REPLACE(address.address_text, '^[^,]+,', ''))
FROM (
    SELECT DISTINCT address_text
    FROM stage.raw_delivery_lines
) address
JOIN core.dim_city city
    ON city.city_name = BTRIM(SPLIT_PART(address.address_text, ',', 1))
ORDER BY address.address_text;

INSERT INTO core.dim_store (store_id, store_name, city_id, raw_store_address, store_address_line)
SELECT DISTINCT
    raw.store_id,
    BTRIM(SPLIT_PART(raw.store_address, ',', 1)) AS store_name,
    city.city_id,
    raw.store_address,
    BTRIM(REGEXP_REPLACE(raw.store_address, '^[^,]+,[^,]+,', '')) AS store_address_line
FROM stage.raw_delivery_lines raw
JOIN core.dim_city city
    ON city.city_name = BTRIM(SPLIT_PART(raw.store_address, ',', 2))
ORDER BY raw.store_id;

INSERT INTO core.dim_product (product_id, product_name, category_id)
SELECT DISTINCT
    raw.item_id AS product_id,
    raw.item_title AS product_name,
    category.category_id
FROM stage.raw_delivery_lines raw
JOIN core.dim_category category
    ON category.category_name = raw.item_category
ORDER BY raw.item_id;

WITH order_source AS (
    SELECT
        order_id,
        MIN(user_id) AS user_id,
        MIN(address_text) AS address_text,
        MIN(store_id) AS store_id,
        MIN(payment_type) AS payment_type,
        MIN(order_cancellation_reason) AS order_cancellation_reason,
        MIN(created_at) AS created_at,
        MIN(paid_at) AS paid_at,
        MIN(delivery_started_at) AS delivery_started_at,
        MAX(delivered_at) AS delivered_at,
        MAX(canceled_at) AS canceled_at,
        MIN(order_discount) AS order_discount,
        MIN(delivery_cost) AS delivery_cost,
        COUNT(*) AS source_row_count,
        COUNT(DISTINCT driver_id) > 1 AS has_driver_change
    FROM stage.raw_delivery_lines
    GROUP BY order_id
)
INSERT INTO core.fct_order (
    order_id,
    user_id,
    delivery_address_id,
    store_id,
    payment_type_id,
    cancellation_reason_id,
    created_at,
    paid_at,
    delivery_started_at,
    delivered_at,
    canceled_at,
    order_discount_pct,
    delivery_cost,
    has_driver_change,
    source_row_count
)
SELECT
    src.order_id,
    src.user_id,
    address.address_id,
    src.store_id,
    payment.payment_type_id,
    reason.cancellation_reason_id,
    src.created_at,
    src.paid_at,
    src.delivery_started_at,
    src.delivered_at,
    src.canceled_at,
    src.order_discount,
    src.delivery_cost,
    src.has_driver_change,
    src.source_row_count
FROM order_source src
JOIN core.dim_delivery_address address
    ON address.raw_address_text = src.address_text
JOIN core.dim_payment_type payment
    ON payment.payment_type_name = src.payment_type
LEFT JOIN core.dim_cancellation_reason reason
    ON reason.cancellation_reason_name = src.order_cancellation_reason
ORDER BY src.order_id;

WITH driver_source AS (
    SELECT DISTINCT
        order_id,
        driver_id,
        delivery_started_at,
        delivered_at,
        canceled_at
    FROM stage.raw_delivery_lines
),
ranked_drivers AS (
    SELECT
        order_id,
        driver_id,
        delivery_started_at,
        delivered_at,
        canceled_at,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY
                (delivered_at IS NOT NULL) DESC,
                delivered_at DESC NULLS LAST,
                canceled_at DESC NULLS LAST,
                driver_id
        ) AS assignment_seq,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY
                (delivered_at IS NOT NULL) DESC,
                delivered_at DESC NULLS LAST,
                canceled_at DESC NULLS LAST,
                driver_id DESC
        ) = 1 AS is_final_driver
    FROM driver_source
)
INSERT INTO core.fct_order_driver (
    order_id,
    assignment_seq,
    driver_id,
    is_final_driver,
    delivery_started_at,
    delivered_at,
    canceled_at
)
SELECT
    order_id,
    assignment_seq,
    driver_id,
    is_final_driver,
    delivery_started_at,
    delivered_at,
    canceled_at
FROM ranked_drivers
ORDER BY order_id, assignment_seq;

INSERT INTO core.fct_order_line (
    order_id,
    product_id,
    replaced_product_id,
    source_file_name,
    source_row_number,
    ordered_quantity,
    canceled_quantity,
    unit_price,
    item_discount_pct
)
SELECT
    raw.order_id,
    raw.item_id,
    raw.item_replaced_id,
    raw.source_file_name,
    raw.source_row_number,
    raw.item_quantity,
    raw.item_canceled_quantity,
    raw.item_price,
    raw.item_discount
FROM stage.raw_delivery_lines raw
ORDER BY raw.order_id, raw.source_file_name, raw.source_row_number;
"""


def load_stage_to_core() -> None:
    with get_dwh_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(CORE_REFRESH_SQL)
        connection.commit()


if __name__ == "__main__":
    load_stage_to_core()
    print("Loaded stage data into core schema.")
