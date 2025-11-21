import pandas as pd

REQUIRED = ["ref_month","site_code","account_code","amount_br"]

def transform_costs(bronze_path: str, cfg: dict) -> pd.DataFrame:
    df = pd.read_csv(bronze_path) if bronze_path.endswith(".csv") else pd.read_parquet(bronze_path)
    df = df[[c for c in REQUIRED if c in df.columns] + [c for c in df.columns if c not in REQUIRED]].copy()
    df["ref_month"] = pd.to_datetime(df["ref_month"], errors="coerce")
    df["dt"] = df["ref_month"].dt.to_period("M").dt.to_timestamp()
    df["amount_br"] = pd.to_numeric(df["amount_br"], errors="coerce").fillna(0.0)
    if "fx_rate" in df.columns:
        df["fx_rate"] = pd.to_numeric(df["fx_rate"], errors="coerce").fillna(0.0)
    return df