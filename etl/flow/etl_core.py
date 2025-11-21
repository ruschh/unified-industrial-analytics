import pandas as pd
from pathlib import Path
from prefect import flow, task, get_run_logger
from etl.extract.csv_loader import load_csv_glob
from etl.transform.energy import transform_energy
from etl.transform.manufacturing import transform_manuf
from etl.transform.costs import transform_costs
from etl.load.to_parquet import write_parquet_partitions
from etl.load.to_duckdb import upsert_duckdb
from etl.quality.gx_checks import run_gx_suite
from etl.utils.io import load_yaml

@task(retries=2, retry_delay_seconds=30)
def stage_bronze(domain: str, cfg: dict):
    pattern = cfg["sources"][domain]["path"]
    df = load_csv_glob(pattern)
    bronze_dir = str(Path(cfg["bronze"]) / domain)      # <- isolado por domínio
    return write_parquet_partitions(df, base_dir=bronze_dir, filename=f"{domain}.parquet")

@task
def stage_silver(domain: str, bronze_parquet: str, cfg: dict):
    if domain == "energy":
        df = transform_energy(bronze_parquet, cfg)
    elif domain == "manufacturing":
        df = transform_manuf(bronze_parquet, cfg)
    else:
        df = transform_costs(bronze_parquet, cfg)
    run_gx_suite(domain, df)        
    silver_dir = str(Path(cfg["silver"]) / domain)      # <- isolado por domínio
    return write_parquet_partitions(df, base_dir=silver_dir, filename=f"{domain}.parquet")

@task
def stage_gold(domain: str, silver_parquet: str, cfg: dict):
    # 1) mantém upsert no DuckDB
    msg = upsert_duckdb(domain, silver_parquet, cfg)

    # 2) salva também como Parquet em data/gold/<domínio>/<domínio>.parquet
    gold_dir = str(Path(cfg["gold"]) / domain)
    df = pd.read_parquet(silver_parquet)
    _ = write_parquet_partitions(df, base_dir=gold_dir, filename=f"{domain}.parquet")

    return msg

@flow(name="etl_whirlpool_core")
def etl_core(config_path: str = "configs/config.yaml"):
    cfg = load_yaml(config_path)
    logger = get_run_logger()
    for domain in ["energy", "manufacturing", "costs"]:
        b = stage_bronze.submit(domain, cfg)
        logger.info(f"Bronze path for {domain}: {b.result()}")
        s = stage_silver.submit(domain, b.result(), cfg)
        logger.info(f"Silver path for {domain}: {s.result()}")
        g = stage_gold.submit(domain, s.result(), cfg)
        logger.info(f"Gold updated for {domain}: {g.result()}")

if __name__ == "__main__":
    etl_core()
