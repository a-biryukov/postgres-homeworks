"""Скрипт для заполнения данными таблиц в БД Postgres."""

import psycopg2
import csv
import os


def main():
    conn = psycopg2.connect(host="localhost", database="north", user="postgres", password="12345")

    file_list = ["customers_data.csv", "employees_data.csv", "orders_data.csv"]

    try:
        with conn:
            with conn.cursor() as cur:
                for file_name in file_list:
                    file_path = os.path.join("north_data", file_name)

                    table_name = file_name.split("_")[0]

                    with open(file_path, encoding="utf-8") as file:
                        next(file)
                        file_reader = csv.reader(file, delimiter=",")

                        for row in file_reader:
                            values = ("%s " * len(row)).strip().split()
                            cur.execute(f"INSERT INTO {table_name} VALUES ({", ".join(values)})", tuple(row))

                    print(f"Таблица {table_name} заполнена")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
