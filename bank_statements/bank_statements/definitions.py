import dagster as dg

from bank_statements import assets  # noqa: TID252

all_assets = dg.load_assets_from_modules([assets])

defs = dg.Definitions(
    assets=all_assets,
)
