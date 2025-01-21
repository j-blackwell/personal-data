from subprocess import run


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
    from dotenv import dotenv_values

    env = dotenv_values()
    duckdb = env["DUCKDB"]
    assert duckdb is not None
    run(
        [
            "duckdb",
            duckdb,
        ]
    )
