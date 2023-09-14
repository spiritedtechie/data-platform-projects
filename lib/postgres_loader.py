import argparse
import os
from pathlib import Path

import psycopg2
import slugify
from dotenv import load_dotenv
from dataclasses import dataclass


@dataclass
class PostgresDBProperties:
    host: str
    port: str
    db: str
    user: str
    password: str


def generate_create_table_statement(file, schema="public", delimeter=","):
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
            drop table if exists {schema}.{table_name} cascade; 
            create table {schema}.{table_name} (\n {column_defs} \n);
        """

        return table_name, sql


def load_file_to_database(
    file_path: str, db: PostgresDBProperties, schema: str = "public"
):
    table_name, sql = generate_create_table_statement(file_path, schema=schema)

    conn = psycopg2.connect(
        host=db.host,
        port=db.port,
        dbname=db.db,
        user=db.user,
        password=db.password,
    )

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)

                with open(file_path, "r") as f:
                    cur.copy_expert(
                        f"COPY {schema}.{table_name} FROM stdin WITH CSV HEADER DELIMITER ',' ENCODING 'utf-8'",
                        f,
                    )
    finally:
        conn.close()


def main():
    load_dotenv(".env")

    db = PostgresDBProperties(
        os.getenv("POSTGRES_HOST"),
        os.getenv("POSTGRES_PORT"),
        os.getenv("POSTGRES_DB"),
        os.getenv("POSTGRES_USER"),
        os.getenv("POSTGRES_PASSWORD"),
    )

    parser = argparse.ArgumentParser(
        prog="python postgres_loader.py",
        description="Loads a CSV dataset to Postgres database configured in .env",
    )
    parser.add_argument("filepath", help="The path to the CSV to load")
    args = parser.parse_args()

    file_path = args.filepath

    load_file_to_database(file_path, db)


if __name__ == "__main__":
    main()
