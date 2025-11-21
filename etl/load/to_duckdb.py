import duckdb

def upsert_duckdb(domain: str, silver_path: str, cfg: dict):
    con = duckdb.connect(cfg.get("duckdb_path", "warehouse.duckdb"))
    table = {
        "energy": "fact_energy",
        "manufacturing": "fact_production",
        "costs": "fact_costs"
    }[domain]
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM read_parquet('{silver_path}') LIMIT 0;")
    con.execute(f"INSERT INTO {table} SELECT * FROM read_parquet('{silver_path}');")
    return f"Inserted into {table}"