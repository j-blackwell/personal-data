from pathlib import Path
from typing import Optional

import dagster as dg
import pandas as pd
import polars as pl

from bank_statements.helpers.pandas import glimpse
from bank_statements.resources.pdf_parser import PdfStatementParser
from bank_statements.transformations import transform_raw

MONTHLY_PARTITION = dg.MonthlyPartitionsDefinition(start_date="2023-12-01")


@dg.asset(
    partitions_def=MONTHLY_PARTITION,
    metadata={"partition_expr": "STATEMENT_MONTH"},
)
def sainsburys_pdf_raw(
    context: dg.AssetExecutionContext,
    statement_parser: PdfStatementParser,
) -> Optional[pl.DataFrame]:
    partition_key = context.partition_key
    if partition_key == "2024-10-01":
        return pl.DataFrame(schema=["0", "1", "2", "PARTITION_MONTH"])
    file = Path(f"{partition_key[:7]}.pdf")
    statement_raw = statement_parser.get_statement_raw(file)
    statement = transform_raw(statement_raw, partition_key)
    statement = pl.from_pandas(statement)
    return statement


@dg.asset(
    partitions_def=MONTHLY_PARTITION,
    metadata={"partition_expr": "STATEMENT_MONTH"},
)
def sainsburys_manual_raw(context: dg.AssetExecutionContext) -> pl.DataFrame:
    partition_key = context.partition_key
    file = Path(
        f"/home/jamesr/Documents/bank-statements/sainsburys/manual/{partition_key[:7]}.csv"
    )
    statement = pl.read_csv(file).with_columns(
        pl.lit(partition_key).str.to_datetime("%Y-%m-%d").alias("STATEMENT_MONTH"),
    )
    return statement


@dg.asset()
def monzo_raw(context: dg.AssetExecutionContext) -> pl.DataFrame:
    return pl.read_csv("/home/jamesr/Documents/bank-statements/monzo/monzo.csv")


@dg.asset()
def triodos_raw(context: dg.AssetExecutionContext) -> pl.DataFrame:
    df_raw = pl.read_csv(
        "/home/jamesr/Documents/bank-statements/triodos/triodos.csv",
        has_header=False,
    )
    df_raw.columns = [
        "DATE",
        "SORT_CODE",
        "ACCOUNT_NUMBER",
        "AMOUNT_GBP",
        "TYPE",
        "DESCRIPTION",
        "UNKNOWN",
        "BALANCE",
    ]

    return df_raw
