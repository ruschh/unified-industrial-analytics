# Projeto EmpresaX — Pipeline de Dados para Custos, Energia e Manufatura

[![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/github/license/ruschh/unified-industrial-analytics)](./LICENSE)
![Repo size](https://img.shields.io/github/repo-size/ruschh/unified-industrial-analytics)
![Last commit](https://img.shields.io/github/last-commit/ruschh/unified-industrial-analytics)
[![CI](https://github.com/ruschh/unified-industrial-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/ruschh/unified-industrial-analytics/actions/workflows/ci.yml)

Este repositório reúne um **projeto completo de ciência de dados**, cobrindo:

- **ETL automatizado** (Prefect + DuckDB + Parquet)

- **Análise exploratória (EDA)** por domínio

- **Modelagem e experimentos de Machine Learning**

- **Validação analítica pós-ETL**

- **Protótipos de deploy (ex.: Streamlit)**

O objetivo é construir uma base robusta para **otimização analítica de custos e planejamento financeiro**, usando dados estruturados de **energia, manufatura e custos**.

## Visão Geral

O projeto é organizado em camadas e módulos:

- Camadas de dados: **Raw → Bronze → Silver → Gold**
- Módulos principais:
  - `etl/` – pipeline automatizado de ingestão, transformação e carga

  - `eda/` – análises exploratórias em código “de produção”

  - `notebooks/` – experimentos, documentação viva e testes

  - `validacao/` – validação analítica, consultas de consistência

  - `deploy/` – protótipos de dashboards e aplicações para consumo dos dados

O data warehouse é mantido localmente em **DuckDB** (dentro de `artifacts/`).


## Arquitetura Geral

```text
[Fontes brutas: CSV / planilhas / dumps]
                  ↓
         data/raw/ (landing)
                  ↓
        BRONZE  → data/bronze/
                  ↓
        SILVER  → data/silver/
                  ↓
        GOLD    → data/gold/
                  ↓
      artifacts/warehouse.duckdb
                  ↓
   EDA • Modelagem • Dashboards
```

## Estrutura de diretórios (visão macro)

```text
project-root/
    ├── artifacts/
    ├── configs/
    ├── data/
    ├── deploy/
    ├── eda/
    ├── etl/
    ├── notebooks/
    ├── reports/
    ├── validacao/
    ├── LICENSE
    ├── README.md
    ├── requirements.txt
    └── environment.yml
```

---

---

---

---


## Criando ambiente virtual

```bash
conda create -n env_empresaX python=3.12
conda activate env_empresaX
pip install -r requirements.txt
python -m etl.flow.etl_core
```

---

## Tecnologias Principais

- Python, Pandas, PyArrow

- DuckDB  

- Prefect  

- Great Expectations  

- Parquet  

- Streamlit  

- Plotly

---

## Componentes do Projeto

- **ETL (`etl/`)** – Pipeline modular Raw → Gold  

- **EDA (`eda/` + `notebooks/eda/`)** – Análises descritivas  

- **Modelagem (`notebooks/modelling/`)** – ML, SHAP  

- **Validação (`validacao/` + `notebooks/validation/`)** – Checks pós-ETL  

- **Deploy (`deploy/` + `notebooks/deploy/`)** – Protótipos Streamlit  

---

## Autor

**Flavio Rusch**  

Cientista de Dados | Ph.D. em Física Estatística | Pós-doutorado em neurociência computacional - USP

Foco em modelagem computacional, ETL moderno, análise de dados industriais e redes complexas.
