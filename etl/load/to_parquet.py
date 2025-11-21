import os
import pandas as pd

def write_parquet_partitions(df: pd.DataFrame, base_dir: str, filename: str = "data.parquet") -> str:
    os.makedirs(base_dir, exist_ok=True)
    out_path = os.path.join(base_dir, filename)
    df.to_parquet(out_path, index=False)   # requer pyarrow ou fastparquet
    return out_path
