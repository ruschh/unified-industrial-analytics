INSERT OR REPLACE INTO qa.rules VALUES
('R3_DIM_KEYS_NOT_NULL','Chaves de dimensões não nulas','ERROR','sem nulos'),
('R4_DIM_DUPLICATES','Duplicatas nas dimensões','ERROR','chaves exclusivas'),
('R5_DOMAIN_COSTS','Domínio custos/fx_rate','ERROR','valores válidos'),
('R6_DOMAIN_MANU','Domínio manufatura','ERROR','valores válidos'),
('R7_DOMAIN_ENERGY','Domínio energia','ERROR','valores válidos');

INSERT INTO qa.results
SELECT * FROM qa_assert(
  'R3_DIM_KEYS_NOT_NULL',
  (
    SELECT
      (SELECT COUNT(*) FROM dim_site    WHERE site_code IS NULL)=0 AND
      (SELECT COUNT(*) FROM dim_line    WHERE site_code IS NULL OR line_code IS NULL)=0 AND
      (SELECT COUNT(*) FROM dim_product WHERE product_code IS NULL)=0
  ),
  (
    SELECT
      'nulls(dim_site.site_code)='       || (SELECT COUNT(*) FROM dim_site    WHERE site_code IS NULL) || '; ' ||
      'nulls(dim_line.site_code)='       || (SELECT COUNT(*) FROM dim_line    WHERE site_code IS NULL) || '; ' ||
      'nulls(dim_line.line_code)='       || (SELECT COUNT(*) FROM dim_line    WHERE line_code IS NULL) || '; ' ||
      'nulls(dim_product.product_code)=' || (SELECT COUNT(*) FROM dim_product WHERE product_code IS NULL)
  ),
  (SELECT json('[]'))
);

WITH
dup_site AS (
  SELECT site_code, COUNT(*) c FROM dim_site GROUP BY 1 HAVING c>1
),
dup_line AS (
  SELECT site_code, line_code, COUNT(*) c FROM dim_line GROUP BY 1,2 HAVING c>1
),
dup_prod AS (
  SELECT product_code, COUNT(*) c FROM dim_product GROUP BY 1 HAVING c>1
)
INSERT INTO qa.results
SELECT * FROM qa_assert(
  'R4_DIM_DUPLICATES',
  (
    SELECT
      (SELECT COUNT(*)=0 FROM dup_site) AND
      (SELECT COUNT(*)=0 FROM dup_line) AND
      (SELECT COUNT(*)=0 FROM dup_prod)
  ),
  (
    SELECT
      'dup_site=' || (SELECT COUNT(*) FROM dup_site) || '; ' ||
      'dup_line=' || (SELECT COUNT(*) FROM dup_line) || '; ' ||
      'dup_prod=' || (SELECT COUNT(*) FROM dup_prod)
  ),
  (
    SELECT json_object(
      'dup_site', (SELECT to_json(list(site_code)) FROM dup_site),
      'dup_line', (SELECT to_json(list(site_code || '|' || line_code)) FROM dup_line),
      'dup_prod', (SELECT to_json(list(product_code)) FROM dup_prod)
    )
  )
);

WITH bad AS (
  SELECT COUNT(*) AS c
  FROM fact_costs
  WHERE COALESCE(amount_br,0) < 0
     OR COALESCE(amount_fx,0) < 0
     OR COALESCE(fx_rate,0) <= 0
)
INSERT INTO qa.results
SELECT * FROM qa_assert(
  'R5_DOMAIN_COSTS',
  (SELECT c=0 FROM bad),
  (SELECT 'rows_invalid=' || c FROM bad),
  (SELECT json('[]'))
);

WITH bad AS (
  SELECT COUNT(*) AS c
  FROM fact_manufacturing
  WHERE COALESCE(units_ok,0) < 0
     OR COALESCE(units_rework,0) < 0
     OR COALESCE(scrap_units,0) < 0
     OR COALESCE(takt_time_s,0) < 0
     OR oee < 0 OR oee > 1
)
INSERT INTO qa.results
SELECT * FROM qa_assert(
  'R6_DOMAIN_MANU',
  (SELECT c=0 FROM bad),
  (SELECT 'rows_invalid=' || c FROM bad),
  (SELECT json('[]'))
);

WITH bad AS (
  SELECT COUNT(*) AS c
  FROM fact_energy
  WHERE COALESCE(kwh_day,0) < 0
     OR COALESCE(kw_demand_peak_day,0) < 0
     OR COALESCE(kvarh_day,0) < 0
)
INSERT INTO qa.results
SELECT * FROM qa_assert(
  'R7_DOMAIN_ENERGY',
  (SELECT c=0 FROM bad),
  (SELECT 'rows_invalid=' || c FROM bad),
  (SELECT json('[]'))
);
