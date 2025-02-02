import dagster as dg
import polars as pl

from bank_statements.helpers.settings import SETTINGS


@dg.asset()
def triodos_raw(context: dg.AssetExecutionContext) -> pl.DataFrame:
    df_raw = pl.read_csv(
        f"{SETTINGS.bank_statement_dir}/triodos/triodos.csv",
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


@dg.asset
def triodos_curated(
    context: dg.AssetExecutionContext,
    triodos_raw: pl.DataFrame,
) -> pl.DataFrame:

    return triodos_raw.select(
        pl.col("DATE").str.to_datetime("%d/%m/%Y"),
        (pl.col("AMOUNT_GBP").str.replace(",", "").cast(pl.Decimal) * -1).alias(
            "AMOUNT_GBP"
        ),
        pl.col("DESCRIPTION"),
    )
