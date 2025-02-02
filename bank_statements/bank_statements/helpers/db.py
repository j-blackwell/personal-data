
import os
import duckdb
import polars as pl

from bank_statements.helpers.settings import SETTINGS

def get_table(schema_table: str) -> pl.DataFrame:
    with duckdb.connect(SETTINGS.duckdb) as db:
        return db.execute(f"SELECT * FROM {schema_table}").pl()

def drop_table(schema_table: str):
    with duckdb.connect(SETTINGS.duckdb) as db:
        db.execute(f"DROP TABLE {schema_table}").pl()
