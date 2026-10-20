import os
from datetime import datetime

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


def extract_table_list():
    conn = psycopg2.connect(
        host="localhost", port=5432, dbname="dwh", user="postgres", password="postgres",
        connect_timeout=10,
    )
    cur = conn.cursor()
    cur.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
    )
    rows = cur.fetchall()
    conn.close()

    os.makedirs("/opt/airflow/output", exist_ok=True)
    with open("/opt/airflow/output/table_list.txt", "w") as f:
        f.write("table_schema | table_name\n")
        f.write("-------------+-----------------\n")
        for schema, name in rows:
            f.write(f"{schema} | {name}\n")
        f.write(f"({len(rows)} rows)\n")


with DAG(
    dag_id="extract_dwh_table_list",
    description="Extract table list from PostgreSQL DWH",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    PythonOperator(task_id="extract_table_list", python_callable=extract_table_list)