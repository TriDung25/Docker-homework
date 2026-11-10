import tempfile
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import psycopg2
from psycopg2 import sql
from airflow import DAG
from airflow.operators.python import PythonOperator

SRC = dict(host="localhost", port=5432, dbname="dwh",
           user="postgres", password="postgres", connect_timeout=10)
DST = dict(host="localhost", port=5433, dbname="target_db",
           user="postgres", password="postgres", connect_timeout=10)

LIST_TABLES = """
SELECT c.relname
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relkind IN ('r', 'p')
  AND NOT c.relispartition
ORDER BY c.relname
"""

LIST_COLUMNS = """
SELECT a.attname,
       CASE t.typtype
            WHEN 'e' THEN 'text'
            WHEN 'd' THEN pg_catalog.format_type(t.typbasetype, t.typtypmod)
            ELSE pg_catalog.format_type(a.atttypid, a.atttypmod)
       END AS col_type
FROM pg_attribute a
JOIN pg_class c     ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_type t      ON t.oid = a.atttypid
WHERE n.nspname = 'public'
  AND c.relname = %s
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum
"""


def migrate_pagila_to_target():
    src = psycopg2.connect(**SRC)
    dst = psycopg2.connect(**DST)
    try:
        scur = src.cursor()
        dcur = dst.cursor()

        dcur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        dst.commit()

        scur.execute(LIST_TABLES)
        tables = [row[0] for row in scur.fetchall()]
        print(f"Found {len(tables)} tables to copy")

        for table in tables:
            scur.execute(LIST_COLUMNS, (table,))
            columns = scur.fetchall()
            col_defs = sql.SQL(", ").join(
                sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(col_type))
                for name, col_type in columns
            )

            dcur.execute(
                sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table))
            )
            dcur.execute(
                sql.SQL("CREATE TABLE {} ({})").format(sql.Identifier(table), col_defs)
            )

            copy_out = sql.SQL("COPY (SELECT * FROM public.{}) TO STDOUT").format(
                sql.Identifier(table)
            ).as_string(src)
            copy_in = sql.SQL("COPY {} FROM STDIN").format(
                sql.Identifier(table)
            ).as_string(dst)

            with tempfile.TemporaryFile(mode="w+") as buffer:
                scur.copy_expert(copy_out, buffer)
                buffer.seek(0)
                dcur.copy_expert(copy_in, buffer)

            dst.commit()
            print(f"{table}: copied {dcur.rowcount} rows")
    finally:
        src.close()
        dst.close()


with DAG(
    dag_id="migrate_pagila_to_target",
    description="Copy all tables from source Postgres (Pagila) to target Postgres",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    migrate = PythonOperator(
        task_id="migrate_pagila_to_target",
        python_callable=migrate_pagila_to_target,
    )
    trigger_star_schema = TriggerDagRunOperator(
        task_id="trigger_star_schema",
        trigger_dag_id="build_star_schema",
    )
    migrate >> trigger_star_schema