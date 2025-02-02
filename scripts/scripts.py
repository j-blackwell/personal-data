from subprocess import run

from bank_statements.bank_statements.helpers.settings import SETTINGS


def local():
    run(
        [
            "dagster",
            "dev",
        ]
    )


def validate():
    run(
        [
            "dagster",
            "definitions",
            "validate",
        ]
    )


def db():
    run(
        [
            "duckdb",
            SETTINGS.duckdb,
        ]
    )
