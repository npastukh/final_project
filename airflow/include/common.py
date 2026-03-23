import os
from pathlib import Path

import psycopg2


def get_dwh_connection():
    return psycopg2.connect(
        host=os.environ.get("DWH_HOST", "postgres"),
        port=int(os.environ.get("DWH_PORT", "5432")),
        dbname=os.environ.get("DWH_DB", "dwh"),
        user=os.environ.get("DWH_USER", "project_user"),
        password=os.environ.get("DWH_PASSWORD", "project_password"),
    )


def get_jdbc_url() -> str:
    return (
        f"jdbc:postgresql://{os.environ.get('DWH_HOST', 'postgres')}:"
        f"{os.environ.get('DWH_PORT', '5432')}/{os.environ.get('DWH_DB', 'dwh')}"
    )


def get_jdbc_properties() -> dict:
    return {
        "user": os.environ.get("DWH_USER", "project_user"),
        "password": os.environ.get("DWH_PASSWORD", "project_password"),
        "driver": "org.postgresql.Driver",
    }


def get_sql_dir() -> Path:
    return Path(os.environ.get("PROJECT_SQL_DIR", "/opt/project/sql"))


def get_data_dir() -> Path:
    return Path(os.environ.get("PROJECT_DATA_DIR", "/opt/project/data/raw"))


def list_parquet_files() -> list[Path]:
    return sorted(get_data_dir().glob("*.parquet"))


def execute_sql_file(sql_path: str | Path) -> None:
    sql_text = Path(sql_path).read_text(encoding="utf-8")
    with get_dwh_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_text)
        connection.commit()
