from contextlib import suppress
import pandas as pd

def glimpse(df):
    print(f"Rows: {df.shape[0]}")
    print(f"Columns: {df.shape[1]}")
    for col in df.columns:
        with suppress(AttributeError):
            print(f"$ {col} <{df[col].dtype}> {df[col].head().values}")
