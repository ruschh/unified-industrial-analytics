"""Exploratory data analysis helpers."""
import pandas as pd

def describe_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.describe(include='all')
