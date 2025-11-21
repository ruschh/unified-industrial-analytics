import os, pathlib, duckdb, pandas as pd
WAREHOUSE = os.environ.get('WAREHOUSE_PATH', 'data/warehouse/whirlpool.duckdb')
SILVER    = os.environ.get('SILVER_BASE',   'data/silver')
GOLD_DIR  = os.environ.get('GOLD_DIR',      'data/gold')
REPORTS   = os.environ.get('REPORTS_DIR',   'reports/qa')

def bootstrap(con, silver_base):
    p_costs  = f"{silver_base}/costs/costs.parquet"
    p_manu   = f"{silver_base}/manufacturing/manufacturing.parquet"
    p_energy = f"{silver_base}/energy/energy.parquet"

    con.execute(
        """
        CREATE OR REPLACE VIEW fact_costs AS
        WITH base AS (
          SELECT CAST(ref_month AS VARCHAR) AS ref_txt, site_code, cost_center, account_code, account_name,
                 amount_br, amount_fx, fx_rate
          FROM read_parquet('""" + p_costs + """')
          WHERE ref_month IS NOT NULL
        )
        SELECT
          CAST(substr(ref_txt,1,4) AS INTEGER)*10000 + CAST(substr(ref_txt,6,2) AS INTEGER)*100 + 1 AS date_key,
          site_code, cost_center, account_code, account_name,
          amount_br, amount_fx, fx_rate
        FROM base;
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW fact_manufacturing AS
        WITH base AS (
          SELECT CAST(date AS VARCHAR) AS date_txt, site_code, line_code, product_code,
                 units_ok, units_rework, scrap_units, takt_time_s, oee
          FROM read_parquet('""" + p_manu + """')
        ), parts AS (
          SELECT CAST(substr(date_txt,1,4) AS INTEGER) AS y,
                 CAST(substr(date_txt,6,2) AS INTEGER) AS m,
                 CAST(substr(date_txt,9,2) AS INTEGER) AS d,
                 site_code, line_code, product_code,
                 units_ok, units_rework, scrap_units, takt_time_s, oee
          FROM base
        )
        SELECT (y*10000 + m*100 + d) AS date_key,
               site_code, line_code, product_code,
               units_ok, units_rework, scrap_units, takt_time_s, oee
        FROM parts;
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW fact_energy AS
        WITH base AS (
          SELECT CAST(timestamp AS VARCHAR) AS ts_txt, site_code, line_code, equip_code,
                 kwh, kw_demand, kvarh
          FROM read_parquet('""" + p_energy + """')
        ), dparts AS (
          SELECT substr(ts_txt,1,10) AS day_txt, site_code, line_code, equip_code, kwh, kw_demand, kvarh FROM base
        ), parts AS (
          SELECT CAST(substr(day_txt,1,4) AS INTEGER) AS y,
                 CAST(substr(day_txt,6,2) AS INTEGER) AS m,
                 CAST(substr(day_txt,9,2) AS INTEGER) AS d,
                 site_code, line_code, equip_code, kwh, kw_demand, kvarh
          FROM dparts
        )
        SELECT (y*10000 + m*100 + d) AS date_key,
               site_code, line_code, equip_code,
               SUM(kwh) AS kwh_day, MAX(kw_demand) AS kw_demand_peak_day, SUM(kvarh) AS kvarh_day
        FROM parts
        GROUP BY 1,2,3,4;
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW dim_site AS
        SELECT DISTINCT site_code FROM fact_costs
        UNION SELECT DISTINCT site_code FROM fact_manufacturing
        UNION SELECT DISTINCT site_code FROM fact_energy;
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW dim_line AS
        SELECT DISTINCT site_code, line_code FROM fact_manufacturing WHERE line_code IS NOT NULL
        UNION
        SELECT DISTINCT site_code, line_code FROM fact_energy WHERE line_code IS NOT NULL;
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW dim_product AS
        SELECT DISTINCT product_code FROM fact_manufacturing WHERE product_code IS NOT NULL;
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW dim_date AS
        WITH keys AS (
          SELECT DISTINCT date_key FROM fact_costs
          UNION SELECT DISTINCT date_key FROM fact_manufacturing
          UNION SELECT DISTINCT date_key FROM fact_energy
        ), parts AS (
          SELECT date_key,
                 CAST(date_key/10000 AS INTEGER) AS y,
                 CAST((date_key/100)%100 AS INTEGER) AS m,
                 CAST(date_key%100 AS INTEGER) AS d
          FROM keys
        )
        SELECT date_key, make_date(y,m,d) AS date, y,m,d FROM parts;
        """
    )

def ensure_qa(con):
    con.execute("""
    CREATE SCHEMA IF NOT EXISTS qa;

    -- Regra com PRIMARY KEY 
    CREATE OR REPLACE TABLE qa.rules (
      rule_id     TEXT PRIMARY KEY,
      description TEXT,
      severity    TEXT,
      expectation TEXT
    );

    CREATE OR REPLACE TABLE qa.results (
      rule_id  TEXT,
      ok       BOOLEAN,
      message  TEXT,
      meta     JSON,
      run_ts   TIMESTAMP
    );

    DROP MACRO IF EXISTS qa_assert;
    CREATE OR REPLACE MACRO qa_assert(rule_id, cond, msg, meta) AS TABLE
    SELECT CAST(rule_id AS TEXT)      AS rule_id,
           CAST(cond    AS BOOLEAN)   AS ok,
           CAST(msg     AS TEXT)      AS message,
           COALESCE(meta, json('[]')) AS meta,
           NOW()                      AS run_ts;
    """
    )

def run_sql(con, path):
    with open(path, 'r', encoding='utf-8') as f:
        sql = f.read()
    con.execute(sql)

def export_qa(con, out_dir):
    df = con.execute(
        """
        SELECT r.rule_id, r.description, r.severity, q.ok, q.message, q.meta, q.run_ts
        FROM qa.rules r LEFT JOIN qa.results q USING(rule_id)
        ORDER BY q.run_ts DESC
        """
    ).fetch_df()
    os.makedirs(out_dir, exist_ok=True)
    df.to_csv(os.path.join(out_dir, 'qa_report.csv'), index=False)
    df.to_parquet(os.path.join(out_dir, 'qa_report.parquet'), index=False)

def main():
    pathlib.Path(WAREHOUSE).parent.mkdir(parents=True, exist_ok=True)
    os.makedirs(GOLD_DIR, exist_ok=True)
    os.makedirs(REPORTS, exist_ok=True)

    con = duckdb.connect(WAREHOUSE)
    bootstrap(con, SILVER)
    ensure_qa(con)

    run_sql(con, 'sql/03_structure_freshness.sql')
    run_sql(con, 'sql/04_quality.sql')
    run_sql(con, 'sql/05_integrity_fk.sql')
    run_sql(con, 'sql/06_reconciliation_kpis.sql')

    con.execute('CREATE SCHEMA IF NOT EXISTS analytics;')
    con.execute('CREATE OR REPLACE TABLE analytics.kpi_cost_per_unit   AS SELECT * FROM kpi_cost_per_unit;')
    con.execute('CREATE OR REPLACE TABLE analytics.kpi_energy_per_unit AS SELECT * FROM kpi_energy_per_unit;')
    con.execute('CREATE OR REPLACE TABLE analytics.kpi_fx_effect       AS SELECT * FROM kpi_fx_effect;')

    con.execute(f"COPY analytics.kpi_cost_per_unit   TO '{GOLD_DIR}/kpi_cost_per_unit.parquet' (FORMAT PARQUET);")
    con.execute(f"COPY analytics.kpi_energy_per_unit TO '{GOLD_DIR}/kpi_energy_per_unit.parquet' (FORMAT PARQUET);")
    con.execute(f"COPY analytics.kpi_fx_effect       TO '{GOLD_DIR}/kpi_fx_effect.parquet' (FORMAT PARQUET);")

    export_qa(con, GOLD_DIR)
    print('[VALIDATION] OK — QA e KPIs atualizados.')

if __name__ == '__main__':
    main()
