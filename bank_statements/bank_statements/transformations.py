import datetime as dt
from typing import Optional

import dagster as dg
import pandas as pd
import polars as pl
from polars.exceptions import InvalidOperationError

from bank_statements.helpers.pandas import glimpse


def parse_amount(x: pl.Expr) -> pl.Expr:
    return (
        pl.when(x.str.contains("CR"))
        .then(pl.concat_str(pl.lit("-"), x.str.replace(" CR", "")))
        .otherwise(x)
        .str.replace(",", "")
        .cast(pl.Float64)
    )


def transform_raw(statement_raw: pd.DataFrame, partition_key: str):
    statement_raw.columns = [str(x) for x in range(len(statement_raw.columns))]
    if partition_key == "2024-04-01":
        statement_raw = statement_raw.drop(["3"], axis=1)
    elif partition_key == "2024-06-01":
        first_row = statement_raw.iloc[0]
        first_row_fixed = pd.DataFrame(
            [
                {"0": first_row["0"][:6], "1": first_row["1"], "3": first_row["3"][:9]},
                {
                    "0": first_row["0"][7:],
                    "1": first_row["2"],
                    "3": first_row["3"][10:],
                },
            ]
        )
        statement_raw = statement_raw.drop([0], axis=0)
        statement_raw = statement_raw.drop(["2"], axis=1)
        statement_raw = pd.concat([statement_raw, first_row_fixed])
    elif partition_key == "2024-07-01":
        statement_raw = statement_raw.drop(["3"], axis=1)
    elif partition_key == "2024-08-01":
        statement_raw = statement_raw.drop(["3", "4"], axis=1)
    elif partition_key == "2024-09-01":
        first_row = statement_raw.iloc[0]
        second_row = statement_raw.iloc[1]
        first_row_fixed = pd.DataFrame(
            [
                {
                    "0": first_row["0"][:6],
                    "1": first_row["1"],
                    "3": first_row["3"][:9],
                },
                {
                    "0": first_row["0"][7:],
                    "1": second_row["1"],
                    "3": first_row["3"][10:],
                },
                {
                    "0": second_row["0"],
                    "1": second_row["2"],
                    "3": second_row["3"],
                },
            ]
        )
        statement_raw = statement_raw.drop([0,1], axis=0)
        statement_raw = statement_raw.drop(["2", "4"], axis=1)
        statement_raw = pd.concat([statement_raw, first_row_fixed])
    elif partition_key == "2024-11-01":
        statement_raw = statement_raw.drop(["3"], axis=1)
    elif partition_key == "2024-12-01":
        statement_raw = statement_raw.drop(["2"], axis=1)
    statement_raw["STATEMENT_MONTH"] = pd.to_datetime(partition_key)
    return statement_raw


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
