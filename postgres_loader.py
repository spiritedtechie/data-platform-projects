import psycopg2
import slugify
import argparse
import os

from dotenv import load_dotenv
from pathlib import Path

load_dotenv(".env")

pg_host = os.getenv("POSTGRES_HOST")
pg_port = os.getenv("POSTGRES_PORT")
pg_db = os.getenv("POSTGRES_DB")
pg_user = os.getenv("POSTGRES_USER")
pg_password = os.getenv("POSTGRES_PASSWORD")


def generate_create_table_statement(file, delimeter=","):
    table_name = Path(file).stem

    with open(file, "r") as f:
        header = f.readline().split(delimeter)

        column_names = []
        for cell in header:
            col_name = slugify.slugify(cell, separator="_")
            column_count = column_names.count(col_name)
            if column_count > 0:
                col_name = f"{col_name}_{column_count + 1}"
            column_names.append(col_name)

        column_defs = ", ".join(f"{col} text" for col in column_names)

        sql = f"""
            drop table if exists {table_name}; 
            create table {table_name} (\n {column_defs} \n);
        """

        return table_name, sql


parser = argparse.ArgumentParser(
    prog="python postgres_loader.py",
    description="Loads a CSV dataset to Postgres database configured in .env",
)
parser.add_argument("filepath", help="The path to the CSV to load")
args = parser.parse_args()

file_path = args.filepath

table_name, sql = generate_create_table_statement(file_path)

conn = psycopg2.connect(
    host=pg_host, port=pg_port, dbname=pg_db, user=pg_user, password=pg_password
)

try:
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql)

            with open(file_path, "r") as f:
                cur.copy_expert(
                    f"COPY {table_name} FROM stdin WITH CSV HEADER DELIMITER ',' ENCODING 'utf-8'",
                    f,
                )
finally:
    conn.close()
