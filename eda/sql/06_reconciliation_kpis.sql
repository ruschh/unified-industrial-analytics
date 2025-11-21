INSERT OR REPLACE INTO qa.rules VALUES
('R11_RECON_COST_VS_PROD','Custo total proporcional à produção','WARN','|Δ|/média <= 5%'),
('R12_RECON_COST_VS_ENERGY','Custo total coerente com consumo energético','WARN','|Δ|/média <= 5%'),
('R13_KPI_VALID_RANGE','KPIs em faixas válidas','ERROR','sem negativos/absurdos');

WITH agg AS (
  SELECT d.y, d.m, c.site_code,
         SUM(c.amount_br) AS total_cost_brl,
         SUM(m.units_ok)  AS total_units
  FROM fact_costs c
  JOIN dim_date d USING(date_key)
  LEFT JOIN fact_manufacturing m
    ON c.site_code = m.site_code
   AND c.date_key  = m.date_key
  GROUP BY 1,2,3
  HAVING SUM(m.units_ok) > 0
), dev AS (
  SELECT *, ABS(total_cost_brl - (AVG(total_cost_brl) OVER()) * (total_units / NULLIF(AVG(total_units) OVER(),0)))
             / NULLIF(AVG(total_cost_brl) OVER(),0) AS rel_diff
  FROM agg
)
INSERT INTO qa.results
SELECT * FROM qa_assert(
  'R11_RECON_COST_VS_PROD',
  (SELECT MAX(rel_diff) <= 0.05 FROM dev),
  (SELECT 'Máx. desvio relativo = ' || ROUND(MAX(rel_diff)*100,2) || '%' FROM dev),
  (SELECT json_group_array(json_object('site_code', site_code, 'rel_diff', rel_diff)) FROM dev WHERE rel_diff > 0.05)
);

WITH agg AS (
  SELECT d.y, d.m, c.site_code,
         SUM(c.amount_br) AS total_cost_brl,
         SUM(e.kwh_day)   AS total_kwh
  FROM fact_costs c
  JOIN dim_date d USING(date_key)
  LEFT JOIN fact_energy e
    ON c.site_code = e.site_code
   AND c.date_key  = e.date_key
  GROUP BY 1,2,3
  HAVING SUM(e.kwh_day) > 0
), dev AS (
  SELECT *, ABS(total_cost_brl - (AVG(total_cost_brl) OVER()) * (total_kwh / NULLIF(AVG(total_kwh) OVER(),0)))
             / NULLIF(AVG(total_cost_brl) OVER(),0) AS rel_diff
  FROM agg
)
INSERT INTO qa.results
SELECT * FROM qa_assert(
  'R12_RECON_COST_VS_ENERGY',
  (SELECT MAX(rel_diff) <= 0.05 FROM dev),
  (SELECT 'Máx. desvio relativo = ' || ROUND(MAX(rel_diff)*100,2) || '%' FROM dev),
  (SELECT json_group_array(json_object('site_code', site_code, 'rel_diff', rel_diff)) FROM dev WHERE rel_diff > 0.05)
);

CREATE OR REPLACE VIEW kpi_cost_per_unit AS
SELECT f.date_key, d.y, d.m, f.site_code,
       SUM(f.amount_br) / NULLIF(SUM(m.units_ok), 0) AS cost_per_unit
FROM fact_costs f
JOIN dim_date d USING(date_key)
LEFT JOIN fact_manufacturing m
  ON f.site_code = m.site_code
 AND f.date_key  = m.date_key
GROUP BY 1,2,3,4;

CREATE OR REPLACE VIEW kpi_energy_per_unit AS
SELECT e.date_key, d.y, d.m, e.site_code,
       SUM(e.kwh_day) / NULLIF(SUM(m.units_ok), 0) AS kwh_per_unit
FROM fact_energy e
JOIN dim_date d USING(date_key)
LEFT JOIN fact_manufacturing m
  ON e.site_code = m.site_code
 AND e.date_key  = m.date_key
GROUP BY 1,2,3,4;

CREATE OR REPLACE VIEW kpi_fx_effect AS
SELECT date_key, site_code,
       SUM(amount_br - amount_fx * fx_rate) / NULLIF(SUM(amount_br), 0) AS fx_effect_ratio
FROM fact_costs
GROUP BY 1,2;

WITH bad AS (
  SELECT COUNT(*) AS c FROM (
    SELECT cost_per_unit FROM kpi_cost_per_unit WHERE cost_per_unit < 0 OR cost_per_unit > 1e7
    UNION ALL
    SELECT kwh_per_unit FROM kpi_energy_per_unit WHERE kwh_per_unit < 0 OR kwh_per_unit > 1e7
    UNION ALL
    SELECT fx_effect_ratio FROM kpi_fx_effect WHERE ABS(fx_effect_ratio) > 1
  )
)
INSERT INTO qa.results
SELECT * FROM qa_assert(
  'R13_KPI_VALID_RANGE',
  (SELECT c = 0 FROM bad),
  (SELECT 'KPIs fora de faixa = ' || c FROM bad),
  (SELECT json('[]'))
);
