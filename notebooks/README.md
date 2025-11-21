# README dos Notebooks

## Notebooks — Projeto EmpresaX

Este diretório contém os **notebooks Jupyter** usados para exploração, experimentos e documentação viva do projeto.

Obs: Os notebooks **não são código de produção**.  
Eles servem para:

- Investigar hipóteses.

- Testar transformações.

- Explorar modelos.

- Documentar análises passo a passo.

## Estrutura do diretório

```text
notebooks/
	├── etl/           # Experimentos e debug do pipeline ETL
	├── deploy/        # Protótipos de dashboards / APIs
	├── eda/           # Análises exploratórias por domínio
	├── modelling/     # Experimentos de modelagem e ML
	├── validation/    # Checks pós-ETL, consultas em DuckDB, sanity checks
	└── utils/         # Scripts auxiliares usados pelos notebooks (.py)
```

## Scripts auxiliares (`src/`)

O diretório notebooks/utils/ guarda scripts Python dos notebooks, como:

 - 02_eda.py

 - 03_validation_duckdb.py

 - 04_Preprocessing_and_Modeling.py

 - 05_Prediction_and_Deploy.py

 - 06_app_empresa_ml_dashboard

Isso facilita o reuso de funções entre notebooks sem duplicar código.