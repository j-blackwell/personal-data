from pathlib import Path
import dagster as dg

from bank_statements import assets
from bank_statements.resources import PdfStatementParser  # noqa: TID252

all_assets = dg.load_assets_from_modules([assets])

defs = dg.Definitions(
    assets=all_assets,
    resources={
        "statement_parser": PdfStatementParser(dir_prefix="/home/jamesr/Documents/bank-statements/sainsburys"),
    },
)
