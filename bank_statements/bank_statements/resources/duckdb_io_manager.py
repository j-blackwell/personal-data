from typing import Sequence
from dagster._core.storage.db_io_manager import DbTypeHandler
from dagster_duckdb import DuckDBIOManager
from dagster_duckdb_pandas import DuckDBPandasTypeHandler
from dagster_duckdb_polars import DuckDBPolarsIOManager, DuckDBPolarsTypeHandler

class MyDuckDBIOManager(DuckDBIOManager):
    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        return [DuckDBPandasTypeHandler(), DuckDBPolarsTypeHandler()]

