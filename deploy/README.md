# README do Deploy — `deploy/README.md`

## Módulo de Deploy — Projeto Whirlpool

Este diretório concentra **scripts e protótipos de deploy** dos resultados do projeto, como:

- Dashboards interativos (ex.: **Streamlit**)

- APIs simples para servir previsões ou métricas

- Outras interfaces de consumo de dados (Dados de teste do modelo)


## Objetivo

Transformar os artefatos gerados pelo ETL, EDA e modelagem em:

- Ferramentas de apoio à decisão

- Visualizações acessíveis para área de negócios

- POCs de produtos de dados


## Organização

```text
deploy/
  ├── app_empresa_ml_dashboard.py          # Dashboard principal (exemplo)
  └── README.md
```

## Exemplo de execução (Streamlit)
```bash
streamlit run deploy/app_streamlit.py
```
O app deve se conectar a:

 - artifacts/warehouse.duckdb (dados consolidados)

 - e/ou modelos treinados (ex.: arquivos .joblib)