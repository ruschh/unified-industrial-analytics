import glob
import pandas as pd

def load_csv_glob(pattern: str) -> pd.DataFrame:
    files = glob.glob(pattern)
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_csv(f) for f in files), ignore_index=True)