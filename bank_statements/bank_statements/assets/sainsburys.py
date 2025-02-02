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


@dg.asset
def sainsburys_curated(
    context: dg.AssetExecutionContext,
    sainsburys_pdf_raw: pl.DataFrame,
    sainsburys_manual_raw: pl.DataFrame,
) -> pl.DataFrame:
    sainsburys_combined = pl.concat(
        [
            sainsburys_pdf_raw.rename(
                {"0": "DATE", "1": "DESCRIPTION", "2": "AMOUNT_GBP"}
            ).with_columns(
                pl.format("{} {}", pl.col("DATE"), pl.col("STATEMENT_MONTH").dt.year())
            ),
            sainsburys_manual_raw,
        ],
        how="vertical_relaxed",
    )

    return sainsburys_combined.with_columns(
        pl.coalesce(
            pl.col("DATE").str.to_date("%d %b %Y", strict=False),
            pl.col("DATE").str.to_date("%d/%m/%y", strict=False),
            pl.col("STATEMENT_MONTH"),
        ),
        pl.when(pl.col("AMOUNT_GBP").str.contains(" CR"))
        .then(pl.format("-{}", pl.col("AMOUNT_GBP").str.replace(" CR", "")))
        .otherwise(pl.col("AMOUNT_GBP"))
        .str.replace(",", "")
        .replace("", None)
        .cast(pl.Decimal)
        .alias("AMOUNT_GBP"),
    )
