-- Проверка количества заказов и строк после загрузки
SELECT COUNT(*) AS orders_cnt FROM core.fct_order;
SELECT COUNT(*) AS order_lines_cnt FROM core.fct_order_line;

-- Проверка заказов со сменой курьера
SELECT COUNT(*) AS orders_with_driver_change
FROM core.fct_order
WHERE has_driver_change;

-- Базовая проверка order-витрины
SELECT
    metric_date,
    city_name,
    store_name,
    created_orders_count,
    delivered_orders_count,
    canceled_orders_count,
    turnover_amount,
    revenue_amount,
    profit_amount
FROM mart.order_metrics_daily
ORDER BY metric_date, store_name
LIMIT 50;
