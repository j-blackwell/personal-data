from pathlib import Path

import dagster as dg
import pandas as pd

from bank_statements.resources import PdfStatementParser

MONTHLY_PARTITION = dg.MonthlyPartitionsDefinition(start_date="2023-12-01")


@dg.asset(
    partitions_def=MONTHLY_PARTITION,
)
def statement_raw(
    context: dg.AssetExecutionContext,
    statement_parser: PdfStatementParser,
) -> pd.DataFrame:
    partition_key = context.partition_key
    file = Path(f"{partition_key[:7]}.pdf")
    return statement_parser.get_statement_raw(file)
