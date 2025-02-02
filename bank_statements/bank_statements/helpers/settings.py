from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    local_dir: str = "~/code-personal/personal-data/.tmp"
    duckdb: str = "~/code-personal/personal-data/.tmp/storage_assets/database.duckdb"
    bank_statement_dir: str = "/home/jamesr/Documents/bank-statements"


SETTINGS = Settings()
