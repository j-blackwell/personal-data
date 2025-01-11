from typing import Optional

import pandas as pd
import polars as pl
from polars.exceptions import InvalidOperationError


def parse_amount(x: pl.Expr) -> pl.Expr:
    return (
        pl.when(x.str.contains("CR"))
        .then(pl.concat_str(pl.lit("-"), x.str.replace(" CR", "")))
        .otherwise(x)
        .str.replace(",", "")
        .cast(pl.Float64)
    )

def transform_statement(
    statement_raw: pd.DataFrame,
    year: str,
    raise_error: bool = False,
) -> Optional[pl.DataFrame]:
    statement_df = statement_raw.copy()
    col_names = ["DATETIME", "DESCRIPTION", "AMOUNT_GBP"]
    statement_df.columns = col_names + [
        f"AMOUNT_GBP{x-2}" for x in range(3, len(statement_df.columns))
    ]
    statement_pl = pl.DataFrame(statement_df)

    amount_cols = [x for x in statement_pl.columns if "AMOUNT" in x]
    for amount_col in amount_cols:
        try:
            statement_pl = statement_pl.with_columns(
                parse_amount(pl.col(amount_col)).alias(amount_col)
            )
        except InvalidOperationError:
            statement_pl = statement_pl.drop(amount_col)
        else:
            statement_pl = statement_pl.select(
                "DATETIME",
                "DESCRIPTION",
                pl.col(amount_col).alias("AMOUNT_GBP"),
            )
            break

    if "AMOUNT_GBP" not in statement_pl.columns:
        if raise_error:
            raise ValueError("Cannot parse dataframe.")
        else:
            return None

    statement_pl = statement_pl.with_columns(
        pl.format("{} {}", pl.col("DATETIME"), pl.lit(year))
        .str.to_datetime("%d %B %Y")
        .alias("DATETIME")
    )

    return statement_pl
