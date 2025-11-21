import pandas as pd

REQUIRED = ["date","site_code","line_code","product_code","units_ok"]

def transform_manuf(bronze_path: str, cfg: dict) -> pd.DataFrame:
    df = pd.read_csv(bronze_path) if bronze_path.endswith(".csv") else pd.read_parquet(bronze_path)
    req = [c for c in REQUIRED if c in df.columns]
    df = df[req + [c for c in df.columns if c not in req]].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["dt"] = df["date"].dt.date.astype("datetime64[ns]")
    for col in ["units_ok","units_rework","scrap_units"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    if "oee" in df.columns:
        df["oee"] = pd.to_numeric(df["oee"], errors="coerce").clip(0,1)
    return df
