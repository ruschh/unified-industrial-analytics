## README da EDA — `eda/README.md`

# Módulo de EDA — Projeto EmpresaX

Este diretório reúne **scripts de análise exploratória de dados (EDA)** que utilizam as tabelas tratadas pelo ETL.

Enquanto os notebooks em `notebooks/eda/` são mais experimentais e narrativos, os scripts aqui têm foco em:

- Repetibilidade
- Geração de gráficos padronizados
- Cálculo de métricas descritivas principais

---

## Objetivos da EDA

- Entender distribuições de custo, consumo de energia e produção
- Investigar relações entre variáveis (ex.: custo vs energia, custo por unidade, etc.)
- Verificar efeitos de imputação, normalização e padronização
- Gerar insumos para modelagem (ex.: seleção de features)

---

## Organização sugerida

```text
eda/
├── data				# Análises focadas em dados tratados pela ETL
├── Modelos			# Modelos treinados - arquivos .joblib
├── notebooks			# Arquivo .ipynb contendo a EDA
├── reports			# Relatório Quality Assurance (qa)
├── sql				# Queries SQL
└── scr 				# Scripts em Python
```

## Como usar

1. Certifique-se de que o ETL foi executado e as tabelas Gold estão disponíveis.

2. Execute os scripts de EDA apontando para:

	- data/gold/

	- ou artifacts/warehouse.duckdb


	Exemplo genérico:
	```bash
	python eda/eda.py
	```

Os gráficos e resultados podem ser salvos em: `reports/`, ou subpastas específicas dentro de eda/
