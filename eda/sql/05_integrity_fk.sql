INSERT OR REPLACE INTO qa.rules VALUES
('R8_FK_COSTS','FK fact_costs → dim_date, dim_site','ERROR','date_key, site_code válidos'),
('R9_FK_MANU','FK fact_manufacturing → dims','ERROR','todas FKs válidas'),
('R10_FK_ENERGY','FK fact_energy → dims','ERROR','todas FKs válidas');

WITH
bad_date AS (
  SELECT f.date_key FROM fact_costs f
  LEFT JOIN dim_date d USING(date_key) WHERE d.date_key IS NULL
),
bad_site AS (
  SELECT f.site_code FROM fact_costs f
  LEFT JOIN dim_site s USING(site_code) WHERE s.site_code IS NULL
),
summary AS (
  SELECT
    (SELECT COUNT(*) FROM bad_date) AS missing_date,
    (SELECT COUNT(*) FROM bad_site) AS missing_site
)
INSERT INTO qa.results
SELECT * FROM qa_assert(
  'R8_FK_COSTS',
  (SELECT missing_date=0 AND missing_site=0 FROM summary),
  (SELECT 'missing_date='||missing_date||'; missing_site='||missing_site FROM summary),
  (SELECT json_object(
    'bad_date_keys',(SELECT to_json(list(date_key)) FROM bad_date),
    'bad_sites',(SELECT to_json(list(site_code)) FROM bad_site)
  ))
);

WITH
bad_date AS (
  SELECT f.date_key FROM fact_manufacturing f
  LEFT JOIN dim_date d USING(date_key) WHERE d.date_key IS NULL
),
bad_site AS (
  SELECT f.site_code FROM fact_manufacturing f
  LEFT JOIN dim_site s USING(site_code) WHERE s.site_code IS NULL
),
bad_line AS (
  SELECT f.site_code, f.line_code FROM fact_manufacturing f
  LEFT JOIN dim_line l USING(site_code, line_code) WHERE l.line_code IS NULL
),
bad_prod AS (
  SELECT f.product_code FROM fact_manufacturing f
  LEFT JOIN dim_product p USING(product_code) WHERE p.product_code IS NULL
),
summary AS (
  SELECT
    (SELECT COUNT(*) FROM bad_date) AS missing_date,
    (SELECT COUNT(*) FROM bad_site) AS missing_site,
    (SELECT COUNT(*) FROM bad_line) AS missing_line,
    (SELECT COUNT(*) FROM bad_prod) AS missing_prod
)
INSERT INTO qa.results
SELECT * FROM qa_assert(
  'R9_FK_MANU',
  (SELECT missing_date=0 AND missing_site=0 AND missing_line=0 AND missing_prod=0 FROM summary),
  (SELECT 'missing_date='||missing_date||'; missing_site='||missing_site||'; missing_line='||missing_line||'; missing_prod='||missing_prod FROM summary),
  (SELECT json_object(
    'bad_date_keys',(SELECT to_json(list(date_key)) FROM bad_date),
    'bad_sites',(SELECT to_json(list(site_code)) FROM bad_site),
    'bad_lines',(SELECT to_json(list(site_code||'|'||line_code)) FROM bad_line),
    'bad_products',(SELECT to_json(list(product_code)) FROM bad_prod)
  ))
);

WITH
bad_date AS (
  SELECT f.date_key FROM fact_energy f
  LEFT JOIN dim_date d USING(date_key) WHERE d.date_key IS NULL
),
bad_site AS (
  SELECT f.site_code FROM fact_energy f
  LEFT JOIN dim_site s USING(site_code) WHERE s.site_code IS NULL
),
bad_line AS (
  SELECT f.site_code, f.line_code FROM fact_energy f
  LEFT JOIN dim_line l USING(site_code, line_code) WHERE l.line_code IS NULL
),
summary AS (
  SELECT
    (SELECT COUNT(*) FROM bad_date) AS missing_date,
    (SELECT COUNT(*) FROM bad_site) AS missing_site,
    (SELECT COUNT(*) FROM bad_line) AS missing_line
)
INSERT INTO qa.results
SELECT * FROM qa_assert(
  'R10_FK_ENERGY',
  (SELECT missing_date=0 AND missing_site=0 AND missing_line=0 FROM summary),
  (SELECT 'missing_date='||missing_date||'; missing_site='||missing_site||'; missing_line='||missing_line FROM summary),
  (SELECT json_object(
    'bad_date_keys',(SELECT to_json(list(date_key)) FROM bad_date),
    'bad_sites',(SELECT to_json(list(site_code)) FROM bad_site),
    'bad_lines',(SELECT to_json(list(site_code||'|'||line_code)) FROM bad_line)
  ))
);
