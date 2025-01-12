import dagster as dg
from bank_statements.resources.duckdb_io_manager import MyDuckDBIOManager
from bank_statements.resources.pdf_parser import PdfStatementParser

resources = {
    "statement_parser": PdfStatementParser(
        dir_prefix="/home/jamesr/Documents/bank-statements/sainsburys"
    ),
    "io_manager": MyDuckDBIOManager(database=dg.EnvVar("DUCKDB"))
}
