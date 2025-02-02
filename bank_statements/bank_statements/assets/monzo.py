import dagster as dg
import polars as pl

from bank_statements.helpers.settings import SETTINGS


@dg.asset()
def monzo_raw(context: dg.AssetExecutionContext) -> pl.DataFrame:
    return pl.read_csv(f"{SETTINGS.bank_statement_dir}/monzo/monzo.csv")


@dg.asset()
def monzo_curated(
    context: dg.AssetExecutionContext,
    monzo_raw: pl.DataFrame,
) -> pl.DataFrame:
    return monzo_raw.select(
        pl.col("Date").str.to_date("%d/%m/%Y").alias("DATE"),
        pl.col("Amount").cast(pl.Decimal(6, 2)).alias("AMOUNT_GBP"),
        pl.coalesce(pl.col("Description"), pl.col("Name")).alias("DESCRIPTION"),
    )
