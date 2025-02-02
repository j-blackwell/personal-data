import dagster as dg

from bank_statements.assets import monzo, sainsburys, triodos, curated
from bank_statements.resources.definitions import resources

all_assets = dg.load_assets_from_modules(
    [
        sainsburys,
        monzo,
        triodos,
    ]
)

defs = dg.Definitions(
    assets=all_assets,
    resources=resources,
)
