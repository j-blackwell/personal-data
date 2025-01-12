from pathlib import Path

import dagster as dg
import pandas as pd
import polars as pl

from bank_statements.resources.pdf_parser import PdfStatementParser

MONTHLY_PARTITION = dg.MonthlyPartitionsDefinition(start_date="2023-12-01")


@dg.asset(
    partitions_def=MONTHLY_PARTITION,
    metadata={"partition_expr": "STATEMENT_MONTH"},
)
def statement_raw(
    context: dg.AssetExecutionContext,
    statement_parser: PdfStatementParser,
) -> pl.DataFrame:
    partition_key = context.partition_key
    file = Path(f"{partition_key[:7]}.pdf")
    statement_raw = statement_parser.get_statement_raw(file)
    statement_raw["STATEMENT_MONTH"] = pd.to_datetime(partition_key)
    return pl.from_pandas(statement_raw)
