import dagster as dg
import polars as pl

from bank_statements.assets.monzo import monzo_curated
from bank_statements.assets.sainsburys import sainsburys_curated
from bank_statements.assets.triodos import triodos_curated


@dg.asset(
    ins={
        "sainsburys_curated": dg.AssetIn(sainsburys_curated.key),
        "monzo_curated": dg.AssetIn(monzo_curated.key),
        "triodos_curated": dg.AssetIn(triodos_curated.key),
    }
)
def all_curated(
    context: dg.AssetExecutionContext,
    sainsburys_curated: pl.DataFrame,
    monzo_curated: pl.DataFrame,
    triodos_curated: pl.DataFrame,
) -> pl.DataFrame:
    triodos = triodos_curated.with_columns(
        (
            pl.col("DESCRIPTION").str.to_lowercase().str.contains("monzo spending")
            | pl.col("DESCRIPTION").str.to_lowercase().str.contains("joint spending")
        ).alias("IGNORE_SPENDING"),
        pl.col("DESCRIPTION")
        .str.to_lowercase()
        .str.contains("sainsburys bank")
        .alias("IGNORE_CREDIT_CARD"),
        pl.col("DESCRIPTION").str.contains("J Robinson Stocks").alias("IGNORE_STOCKS"),
    )

    all_curated = (
        pl.concat(
            [
                triodos.with_columns(pl.lit("triodos").alias("SOURCE")),
                sainsburys_curated.drop("STATEMENT_MONTH").with_columns(
                    pl.lit("sainsburys").alias("SOURCE")
                ),
                monzo_curated.with_columns(pl.lit("monzo").alias("SOURCE")),
            ],
            how="diagonal_relaxed",
        )
        .sort("DATE")
        .with_columns(
            pl.col("IGNORE_SPENDING").fill_null(False),
            pl.col("IGNORE_CREDIT_CARD").fill_null(False),
            pl.col("IGNORE_STOCKS").fill_null(False),
        )
    )

    return all_curated
