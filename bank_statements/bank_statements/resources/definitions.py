import dagster as dg
from dagster_db import build_custom_duckdb_io_manager

from bank_statements.helpers.settings import SETTINGS
from bank_statements.resources.pdf_parser import PdfStatementParser

resources = {
    "statement_parser": PdfStatementParser(
        dir_prefix=f"{SETTINGS.bank_statement_dir}/sainsburys"
    ),
    "io_manager": build_custom_duckdb_io_manager().configured(
        {"database": SETTINGS.duckdb}
    ),
}
