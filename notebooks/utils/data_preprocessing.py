"""Data cleaning and feature engineering helpers."""
import pandas as pd

def load_raw_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def basic_clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    return df.fillna(0)
