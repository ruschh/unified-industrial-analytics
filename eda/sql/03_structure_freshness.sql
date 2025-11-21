INSERT OR REPLACE INTO qa.rules VALUES
('R1_COUNTS','Tabelas possuem linhas (>0)','ERROR','count > 0'),
('R2A_FRESHNESS_COSTS','Custos atualizados (mensal)','WARN','lag<=1 mês'),
('R2B_FRESHNESS_MANU','Manufatura atualizada (diário)','WARN','lag<=3 dias'),
('R2C_FRESHNESS_ENERGY','Energia atualizada (diário)','WARN','lag<=3 dias');

INSERT INTO qa.results
SELECT * FROM qa_assert(
  'R1_COUNTS',
  (SELECT (SELECT COUNT(*)>0 FROM dim_date)
       AND (SELECT COUNT(*)>0 FROM dim_site)
       AND (SELECT COUNT(*)>0 FROM dim_line)
       AND (SELECT COUNT(*)>0 FROM dim_product)
       AND (SELECT COUNT(*)>0 FROM fact_costs)
       AND (SELECT COUNT(*)>0 FROM fact_manufacturing)
       AND (SELECT COUNT(*)>0 FROM fact_energy)),
  (
    SELECT
      'dim_date='           || (SELECT COUNT(*) FROM dim_date)           || '; ' ||
      'dim_site='           || (SELECT COUNT(*) FROM dim_site)           || '; ' ||
      'dim_line='           || (SELECT COUNT(*) FROM dim_line)           || '; ' ||
      'dim_product='        || (SELECT COUNT(*) FROM dim_product)        || '; ' ||
      'fact_costs='         || (SELECT COUNT(*) FROM fact_costs)         || '; ' ||
      'fact_manufacturing=' || (SELECT COUNT(*) FROM fact_manufacturing) || '; ' ||
      'fact_energy='        || (SELECT COUNT(*) FROM fact_energy)
  ),
  (SELECT json('[]'))
);

WITH mx AS (
  SELECT MAX(d.date) AS max_date
  FROM fact_costs f
  JOIN dim_date d USING(date_key)
),
lag AS (
  SELECT date_diff('month', max_date, current_date) AS lag_months FROM mx
)
INSERT INTO qa.results
SELECT * FROM qa_assert(
  'R2A_FRESHNESS_COSTS',
  (SELECT lag_months <= 1 FROM lag),
  (SELECT CAST(lag_months AS VARCHAR) || ' month(s) lag' FROM lag),
  (SELECT json_object('max_date', (SELECT max_date FROM mx)))
);

WITH mx AS (
  SELECT MAX(d.date) AS max_date
  FROM fact_manufacturing f
  JOIN dim_date d USING(date_key)
),
lag AS (
  SELECT date_diff('day', max_date, current_date) AS lag_days FROM mx
)
INSERT INTO qa.results
SELECT * FROM qa_assert(
  'R2B_FRESHNESS_MANU',
  (SELECT lag_days <= 3 FROM lag),
  (SELECT CAST(lag_days AS VARCHAR) || ' day(s) lag' FROM lag),
  (SELECT json_object('max_date', (SELECT max_date FROM mx)))
);

WITH mx AS (
  SELECT MAX(d.date) AS max_date
  FROM fact_energy f
  JOIN dim_date d USING(date_key)
),
lag AS (
  SELECT date_diff('day', max_date, current_date) AS lag_days FROM mx
)
INSERT INTO qa.results
SELECT * FROM qa_assert(
  'R2C_FRESHNESS_ENERGY',
  (SELECT lag_days <= 3 FROM lag),
  (SELECT CAST(lag_days AS VARCHAR) || ' day(s) lag' FROM lag),
  (SELECT json_object('max_date', (SELECT max_date FROM mx)))
);
