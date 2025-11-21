
# README de Validação

## Módulo de Validação Analítica — Projeto EmpresaX

Este diretório concentra **validações pós-ETL**, em complemento às regras automáticas de qualidade de dados (Great Expectations).

Enquanto o módulo `etl/quality/` foca em **regras automatizadas**, aqui o foco é:

- Validação analítica. 

- Checks de negócio. 

- Comparações entre fontes. 

- Consultas exploratórias em DuckDB


## Objetivos da validação

- Verificar se as métricas agregadas fazem sentido (ex.: totais mensais, médias por site).

- Comparar valores entre:
  - dados brutos vs. dados tratados;

  - relatórios externos vs. saída do pipeline;

- Garantir que o pipeline não introduziu inconsistências.


## Organização do diretório

```text
validacao/
  ├── data/
  │     ├── gold/
  │     │     ├── qa_report.csv
  │     │     └── qa_report.parquet
  │     └── warehouse/
  │           └── whirlpool.duckdb
  └── README.md
```