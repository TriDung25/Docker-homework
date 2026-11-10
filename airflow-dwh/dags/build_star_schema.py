from datetime import datetime

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

DST = dict(host="localhost", port=5433, dbname="target_db",
           user="postgres", password="postgres", connect_timeout=10)
SQL_FILE = "/opt/airflow/dags/sql/star_schema.sql"
TABLES = ["dim_date", "dim_customer", "dim_film", "dim_store", "dim_staff", "fact_rental"]


def build_star_schema():
    with open(SQL_FILE, encoding="utf-8") as f:
        script = f.read()

    conn = psycopg2.connect(**DST)
    try:
        cur = conn.cursor()
        cur.execute(script)
        conn.commit()
        for table in TABLES:
            cur.execute(f"SELECT count(*) FROM star.{table}")
            print(f"star.{table}: {cur.fetchone()[0]} rows")
    finally:
        conn.close()


with DAG(
    dag_id="build_star_schema",
    description="Transform Pagila (3NF) into a star schema in the target Postgres",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    PythonOperator(task_id="build_star_schema", python_callable=build_star_schema)