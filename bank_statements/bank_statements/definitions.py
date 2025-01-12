import dagster as dg

from bank_statements import assets
from bank_statements.resources.definitions import resources

all_assets = dg.load_assets_from_modules([assets])

defs = dg.Definitions(
    assets=all_assets,
    resources=resources,
)
