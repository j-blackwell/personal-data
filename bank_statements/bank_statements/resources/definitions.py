import dagster as dg
from bank_statements.helpers.settings import SETTINGS
from bank_statements.resources.duckdb_io_manager import MyDuckDBIOManager
from bank_statements.resources.pdf_parser import PdfStatementParser

resources = {
    "statement_parser": PdfStatementParser(
        dir_prefix=f"{SETTINGS.bank_statement_dir}/sainsburys"
    ),
    "io_manager": MyDuckDBIOManager(database=SETTINGS.duckdb)
}
