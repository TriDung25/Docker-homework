import os
from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
    SQLCheckOperator,
)

POSTGRES_CONN_ID = "postgres_default"  # đổi thành conn id bạn dùng trong DAG migrate
SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")

with DAG(
    dag_id="star_schema_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # không tự chạy theo lịch, chỉ được DAG migrate kích hoạt
    catchup=False,
    template_searchpath=[SQL_DIR],
    tags=["dwh", "star-schema"],
) as dag:

    build_star_schema = SQLExecuteQueryOperator(
        task_id="build_star_schema",
        conn_id=POSTGRES_CONN_ID,
        sql="star_schema.sql",
    )

    # Kiểm tra: số dòng fact = số dòng rental gốc
    check_fact_rows = SQLCheckOperator(
        task_id="check_fact_rows",
        conn_id=POSTGRES_CONN_ID,
        sql="""
            SELECT (SELECT count(*) FROM star.fact_rental)
                 = (SELECT count(*) FROM public.rental)
        """,
    )

    # Kiểm tra: không có khoá ngoại bị NULL ở các dim chính
    check_no_orphans = SQLCheckOperator(
        task_id="check_no_orphans",
        conn_id=POSTGRES_CONN_ID,
        sql="""
            SELECT count(*) = 0
            FROM star.fact_rental
            WHERE customer_key IS NULL OR film_key IS NULL
               OR store_key IS NULL OR staff_key IS NULL
        """,
    )

    build_star_schema >> [check_fact_rows, check_no_orphans]