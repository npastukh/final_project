from __future__ import annotations

import csv
import io
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import get_dwh_connection, get_jdbc_properties, get_jdbc_url


def _build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.master("local[1]")
        .appName("delivery-order-mart")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.memoryOverhead", "512m")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraClassPath", "/usr/share/java/postgresql.jar")
        .getOrCreate()
    )


def _serialize_copy_value(value: object) -> str:
    if value is None:
        return r"\N"
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def _load_dataframe_to_table(dataframe, load_table_name: str, target_table_name: str, columns: list[str]) -> None:
    buffer = io.StringIO()
    writer = csv.writer(buffer)

    for row in dataframe.select(*columns).collect():
        writer.writerow([_serialize_copy_value(row[column]) for column in columns])

    buffer.seek(0)

    with get_dwh_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {load_table_name} (
                    LIKE {target_table_name} INCLUDING DEFAULTS
                );
                TRUNCATE TABLE {load_table_name};
                """
            )
            cursor.copy_expert(
                sql=(
                    f"COPY {load_table_name} ({', '.join(columns)}) "
                    r"FROM STDIN WITH (FORMAT CSV, NULL '\N')"
                ),
                file=buffer,
            )
        connection.commit()


def _swap_order_mart(order_columns: list[str]) -> None:
    with get_dwh_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                TRUNCATE TABLE mart.order_metrics_daily;

                INSERT INTO mart.order_metrics_daily ({', '.join(order_columns)})
                SELECT {', '.join(order_columns)}
                FROM mart.order_metrics_daily_load;
                """
            )
        connection.commit()


def build_marts() -> None:
    spark = _build_spark_session()

    try:
        jdbc_url = get_jdbc_url()
        jdbc_properties = get_jdbc_properties()

        orders = (
            spark.read.jdbc(jdbc_url, "core.fct_order", properties=jdbc_properties)
            .select(
                "order_id",
                "user_id",
                "store_id",
                "cancellation_reason_id",
                "created_at",
                "paid_at",
                "delivery_started_at",
                "delivered_at",
                "canceled_at",
                "order_discount_pct",
                "delivery_cost",
            )
            .withColumn("metric_date", F.to_date("created_at"))
            .withColumn("metric_year", F.year("created_at"))
            .withColumn("metric_month", F.month("created_at"))
            .withColumn("metric_day", F.dayofmonth("created_at"))
        )

        order_lines = spark.read.jdbc(jdbc_url, "core.fct_order_line", properties=jdbc_properties).select(
            "order_id",
            "ordered_quantity",
            "canceled_quantity",
            "unit_price",
            "item_discount_pct",
        )
        order_drivers = spark.read.jdbc(jdbc_url, "core.fct_order_driver", properties=jdbc_properties).select(
            "order_id",
            "driver_id",
        )
        stores = F.broadcast(
            spark.read.jdbc(jdbc_url, "core.dim_store", properties=jdbc_properties).select(
                "store_id",
                "store_name",
                "city_id",
            )
        )
        cities = F.broadcast(
            spark.read.jdbc(jdbc_url, "core.dim_city", properties=jdbc_properties).select(
                "city_id",
                "city_name",
            )
        )
        reasons = F.broadcast(
            spark.read.jdbc(jdbc_url, "core.dim_cancellation_reason", properties=jdbc_properties).select(
                "cancellation_reason_id",
                "cancellation_reason_name",
            )
        )

        line_metrics = (
            order_lines.join(
                orders.select("order_id", "order_discount_pct", "paid_at"),
                on="order_id",
                how="inner",
            )
            .withColumn(
                "line_discount_factor",
                (F.lit(1.0) - F.col("item_discount_pct") / F.lit(100.0))
                * (F.lit(1.0) - F.col("order_discount_pct") / F.lit(100.0)),
            )
            .withColumn(
                "ordered_amount",
                F.round(
                    F.col("ordered_quantity") * F.col("unit_price") * F.col("line_discount_factor"),
                    2,
                ),
            )
            .withColumn("net_quantity", F.col("ordered_quantity") - F.col("canceled_quantity"))
            .withColumn(
                "net_amount",
                F.round(F.col("net_quantity") * F.col("unit_price") * F.col("line_discount_factor"), 2),
            )
            .withColumn(
                "revenue_amount",
                F.when(F.col("paid_at").isNotNull(), F.col("net_amount")).otherwise(F.lit(0.0)),
            )
        )

        order_financials = line_metrics.groupBy("order_id").agg(
            F.round(F.sum("ordered_amount"), 2).alias("turnover_amount"),
            F.round(F.sum("revenue_amount"), 2).alias("revenue_amount"),
        )

        driver_counts = order_drivers.groupBy("order_id").agg(
            F.countDistinct("driver_id").alias("driver_count")
        )

        orders_enriched = (
            orders.join(order_financials, on="order_id", how="left")
            .join(driver_counts, on="order_id", how="left")
            .join(stores, on="store_id", how="left")
            .join(cities, on="city_id", how="left")
            .join(reasons, on="cancellation_reason_id", how="left")
            .fillna({"turnover_amount": 0.0, "revenue_amount": 0.0, "driver_count": 0})
            .withColumn(
                "expense_amount",
                F.when(F.col("delivery_started_at").isNotNull(), F.col("delivery_cost")).otherwise(
                    F.lit(0.0)
                ),
            )
            .withColumn("profit_amount", F.round(F.col("revenue_amount") - F.col("expense_amount"), 2))
            .withColumn("created_order_flag", F.lit(1))
            .withColumn("delivered_order_flag", F.when(F.col("delivered_at").isNotNull(), 1).otherwise(0))
            .withColumn(
                "canceled_order_flag",
                F.when(F.col("canceled_at").isNotNull() & F.col("delivered_at").isNull(), 1).otherwise(0),
            )
            .withColumn(
                "canceled_after_delivery_flag",
                F.when(F.col("canceled_at").isNotNull() & F.col("delivered_at").isNotNull(), 1).otherwise(0),
            )
            .withColumn(
                "service_error_flag",
                F.when(
                    F.col("cancellation_reason_name").isin("Ошибка приложения", "Проблемы с оплатой"),
                    1,
                ).otherwise(0),
            )
            .withColumn("driver_change_flag", F.when(F.col("driver_count") > 1, 1).otherwise(0))
        )

        group_keys = [
            "metric_date",
            "metric_year",
            "metric_month",
            "metric_day",
            "city_id",
            "city_name",
            "store_id",
            "store_name",
        ]

        order_metrics = orders_enriched.groupBy(*group_keys).agg(
            F.round(F.sum("turnover_amount"), 2).alias("turnover_amount"),
            F.round(F.sum("revenue_amount"), 2).alias("revenue_amount"),
            F.round(F.sum("expense_amount"), 2).alias("expense_amount"),
            F.round(F.sum("profit_amount"), 2).alias("profit_amount"),
            F.sum("created_order_flag").alias("created_orders_count"),
            F.sum("delivered_order_flag").alias("delivered_orders_count"),
            F.sum("canceled_order_flag").alias("canceled_orders_count"),
            F.sum("canceled_after_delivery_flag").alias("canceled_after_delivery_count"),
            F.sum("service_error_flag").alias("service_error_cancellations_count"),
            F.countDistinct("user_id").alias("buyers_count"),
            F.sum("driver_change_flag").alias("orders_with_driver_change_count"),
        )

        active_couriers = (
            order_drivers.join(
                orders.select(
                    "order_id",
                    "metric_date",
                    "metric_year",
                    "metric_month",
                    "metric_day",
                    "store_id",
                ),
                on="order_id",
                how="inner",
            )
            .join(stores, on="store_id", how="left")
            .join(cities, on="city_id", how="left")
            .groupBy(*group_keys)
            .agg(F.countDistinct("driver_id").alias("active_couriers_count"))
        )

        order_metrics = (
            order_metrics.join(active_couriers, on=group_keys, how="left")
            .fillna({"active_couriers_count": 0})
            .withColumn(
                "average_check",
                F.round(
                    F.when(
                        F.col("created_orders_count") > 0,
                        F.col("revenue_amount") / F.col("created_orders_count"),
                    ).otherwise(F.lit(0.0)),
                    2,
                ),
            )
            .withColumn(
                "orders_per_buyer",
                F.round(
                    F.when(
                        F.col("buyers_count") > 0,
                        F.col("created_orders_count") / F.col("buyers_count"),
                    ).otherwise(F.lit(0.0)),
                    4,
                ),
            )
            .withColumn(
                "revenue_per_buyer",
                F.round(
                    F.when(F.col("buyers_count") > 0, F.col("revenue_amount") / F.col("buyers_count"))
                    .otherwise(F.lit(0.0)),
                    2,
                ),
            )
            .withColumn("refreshed_at", F.current_timestamp())
        )

        order_columns = order_metrics.columns
        _load_dataframe_to_table(
            order_metrics,
            "mart.order_metrics_daily_load",
            "mart.order_metrics_daily",
            order_columns,
        )
        _swap_order_mart(order_columns)
    finally:
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    build_marts()
    print("Built mart.order_metrics_daily with PySpark.")
