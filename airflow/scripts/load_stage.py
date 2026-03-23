from __future__ import annotations

from io import StringIO
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from common import get_dwh_connection, list_parquet_files


COPY_COLUMNS = [
    "source_file_name",
    "source_row_number",
    "order_id",
    "user_id",
    "user_phone",
    "address_text",
    "created_at",
    "paid_at",
    "delivery_started_at",
    "delivered_at",
    "canceled_at",
    "payment_type",
    "item_id",
    "item_title",
    "item_category",
    "item_quantity",
    "item_price",
    "item_canceled_quantity",
    "item_replaced_id",
    "order_discount",
    "item_discount",
    "order_cancellation_reason",
    "driver_id",
    "driver_phone",
    "delivery_cost",
    "store_id",
    "store_address",
]


def _prepare_dataframe(file_path: Path) -> pd.DataFrame:
    dataframe = pq.read_table(file_path).to_pandas()
    dataframe.insert(0, "source_row_number", range(1, len(dataframe) + 1))
    dataframe.insert(0, "source_file_name", file_path.name)
    dataframe["item_replaced_id"] = dataframe["item_replaced_id"].astype("Int64")
    integer_columns = [
        "order_id",
        "user_id",
        "item_id",
        "item_quantity",
        "item_canceled_quantity",
        "driver_id",
        "store_id",
    ]
    for column in integer_columns:
        dataframe[column] = dataframe[column].astype("Int64")
    numeric_columns = ["item_price", "order_discount", "item_discount", "delivery_cost"]
    for column in numeric_columns:
        dataframe[column] = dataframe[column].astype(float)
    return dataframe[COPY_COLUMNS]


def _copy_dataframe(dataframe: pd.DataFrame) -> int:
    buffer = StringIO()
    dataframe.to_csv(
        buffer,
        index=False,
        header=False,
        sep="\t",
        na_rep="\\N",
        date_format="%Y-%m-%d %H:%M:%S.%f",
    )
    buffer.seek(0)

    copy_sql = f"""
        COPY stage.raw_delivery_lines ({", ".join(COPY_COLUMNS)})
        FROM STDIN
        WITH (
            FORMAT csv,
            DELIMITER E'\\t',
            QUOTE '\"',
            ESCAPE '\"',
            NULL '\\N'
        )
    """
    with get_dwh_connection() as connection:
        with connection.cursor() as cursor:
            cursor.copy_expert(copy_sql, buffer)
        connection.commit()
    return len(dataframe)


def load_raw_parquets_to_stage() -> int:
    parquet_files = list_parquet_files()
    if not parquet_files:
        raise FileNotFoundError("No parquet files were found in data/raw.")

    with get_dwh_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE stage.raw_delivery_lines;")
        connection.commit()

    inserted_rows = 0
    for parquet_file in parquet_files:
        dataframe = _prepare_dataframe(parquet_file)
        inserted_rows += _copy_dataframe(dataframe)
    return inserted_rows


if __name__ == "__main__":
    total_rows = load_raw_parquets_to_stage()
    print(f"Inserted {total_rows} rows into stage.raw_delivery_lines")
