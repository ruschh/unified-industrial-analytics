# README do ETL 

Este diretório contém o **pipeline ETL automatizado** responsável por:

- Ingerir dados de custos, energia e manufatura

- Padronizar, limpar e transformar as tabelas

- Escrever as camadas **Bronze, Silver e Gold** em Parquet

- Popular o data warehouse local em **DuckDB**


## Visão Geral

O ETL é orquestrado pelo **Prefect**, com foco em:

- Modularidade (extract, transform, load, quality)

- Logs detalhados e retentativas

- Fácil extensão para novos domínios e fontes

### Fluxo macro:


extract → transform → quality → load → DuckDB


## Estrutura Interna

```text
etl/
 ├── extract/          # Funções de ingestão de dados (CSV, etc.)
 ├── flow/             # Flows do Prefect (entrypoints do ETL)
 │     └── etl_core.py   # Flow principal
 ├── load/             # Escrita em Parquet e DuckDB
 ├── quality/          # Checks de qualidade (Great Expectations / lógicas customizadas)
 ├── transform/        # Lógica de transformação por domínio
 ├── utils/            # Funções auxiliares (paths, logs, etc.)
 ├── Fluxo_ETL.png     # Diagrama visual do fluxo
 ├── __init__.py
 └── __pycache__/
```

## Execução

```bash
conda activate env_empresaX
python -m etl.flow.etl_core
```

Durante a execução:

 - Os dados brutos são lidos de data/raw/

 - As camadas bronze/, silver/ e gold/ são preenchidas em data/

 - O arquivo artifacts/warehouse.duckdb é atualizado


## Observação

 - Toda a lógica de negócios e regras específicas de transformação por domínio (energia, manufatura, custos) são centralizada em transform/.

 - Regras de qualidade automatizadas ficam em quality/, integradas (ou integráveis) ao Great Expectations.

