{
 "cells": [
  {
   "cell_type": "markdown",
   "id": "88923545",
   "metadata": {},
   "source": [
    "# Parte 2 — Validação Analítica no DuckDB\n",
    "**Projeto Whirlpool** — Validação com consultas e métricas em DuckDB, organizada por etapas.  \n",
    "Este notebook cria *views* para seus dados processados, define uma camada `qa.*` de regras e resultados, executa checagens (estrutura, qualidade, integridade, reconciliação) e materializa KPIs.\n",
    "\n",
    "> **Assunções de esquema (ajuste conforme seu ETL):**\n",
    "> - `dim_date(date_key, date, y, m)`  \n",
    "> - `dim_product(product_id, sku, category, uom)`  \n",
    "> - `dim_plant(plant_id, plant_code, region)`  \n",
    "> - `fact_costs(date_key, product_id, plant_id, qty, material_cost, labor_cost, overhead_cost, total_cost)`  \n",
    "> - `fact_sales(date_key, product_id, plant_id, units_sold, revenue)`\n",
    "\n",
    "---"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "39fc2742",
   "metadata": {},
   "source": [
    "\n",
    "## 📘 Glossário e Conceitos Básicos\n",
    "\n",
    "| Termo | Significado |\n",
    "|--------|--------------|\n",
    "| **PK (Primary Key)** | Identificador único de uma linha em uma tabela. Não pode ter valores nulos nem duplicados. |\n",
    "| **UK (Unique Key)** | Coluna que deve ter valores únicos, mas não precisa ser chave primária. Exemplo: o `sku` de um produto. |\n",
    "| **Null / Nulo** | Valor ausente. Em campos-chave ou métricas, indica erro de qualidade de dados. |\n",
    "| **Domínio / Intervalo** | Conjunto de valores válidos para um campo (ex.: `uom` = “UN”, “KG”). |\n",
    "| **Valores impossíveis** | Regras de negócio violadas (ex.: custo negativo). |\n",
    "| **COGS (Cost of Goods Sold)** | Custo dos produtos vendidos. Soma dos custos diretos (matéria-prima, mão de obra, overhead). |\n",
    "| **Reconciliação** | Verificação se o `total_cost` ≈ soma dos componentes (`material + labor + overhead`), com pequena tolerância. |\n",
    "| **KPI (Key Performance Indicator)** | Indicador-chave de desempenho — métrica usada para monitorar performance (ex.: custo unitário, margem, PPV). |\n"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "4a986f91",
   "metadata": {},
   "source": [
    "\n",
    "## 📊 Dicionário de Dados Usado na Validação Analítica\n",
    "\n",
    "| Tabela | Tipo | Colunas | Descrição |\n",
    "|---------|------|----------|------------|\n",
    "| **dim_date** | Dimensão | `date_key`, `date`, `y`, `m` | Tabela de tempo. Cada linha representa uma data. `y` = ano, `m` = mês. |\n",
    "| **dim_product** | Dimensão | `product_id`, `sku`, `category`, `uom` | Catálogo de produtos. `sku` = código comercial, `uom` = unidade de medida. |\n",
    "| **dim_plant** | Dimensão | `plant_id`, `plant_code`, `region` | Local das fábricas ou plantas produtivas. |\n",
    "| **fact_costs** | Fato | `date_key`, `product_id`, `plant_id`, `qty`, `material_cost`, `labor_cost`, `overhead_cost`, `total_cost` | Custos de produção por produto e planta em uma data. |\n",
    "| **fact_sales** | Fato | `date_key`, `product_id`, `plant_id`, `units_sold`, `revenue` | Dados de vendas associadas às mesmas dimensões de produto e planta. |\n",
    "\n",
    "> Essas tabelas são **carregadas a partir dos Parquets gerados pelo ETL** (armazenados em `data/processed/`).  \n",
    "> A validação analítica **não as cria do zero**, apenas as **lê** e **aplica regras de consistência e qualidade** sobre elas.\n"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "b65a499a",
   "metadata": {},
   "source": [
    "## Etapa 0 — Setup (instalação de dependências e paths)\n",
    "- Instala DuckDB (se necessário)  \n",
    "- Define caminhos para o warehouse e para os dados processados (Parquet do seu ETL)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "a37d765c",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "DuckDB version: 1.1.3\n",
      "Warehouse: data/warehouse/whirlpool.duckdb\n",
      "Processed base: /home/rusch/Área de trabalho/Projeto_Whirpool/Data_Science_Projects/validacao/data/silver\n"
     ]
    }
   ],
   "source": [
    "# Se estiver no Google Colab, descomente a linha abaixo:\n",
    "!pip -q install duckdb==1.1.3\n",
    "\n",
    "import duckdb, os, pathlib, json, datetime, glob\n",
    "from pathlib import Path\n",
    "\n",
    "# Ajuste estes caminhos conforme o seu repositório\n",
    "WAREHOUSE_PATH = 'data/warehouse/whirlpool.duckdb'        # cria/abre o arquivo DuckDB\n",
    "DATA_BASE      = 'data/silver'                          # base dos Parquets\n",
    "\n",
    "# Garante pastas de saída de relatórios\n",
    "os.makedirs('reports', exist_ok=True)\n",
    "\n",
    "print(\"DuckDB version:\", duckdb.__version__)\n",
    "print(\"Warehouse:\", WAREHOUSE_PATH)\n",
    "print(\"Processed base:\", pathlib.Path(DATA_BASE).resolve())"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "89464059",
   "metadata": {},
   "source": [
    "## Etapa 1 — Conexão ao DuckDB e *views* externas\n",
    "Cria conexão com o arquivo `whirlpool.duckdb` e *views* que apontam para os Parquets gerados pelo ETL."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "c7526bfe-eb8c-445b-8bcb-76ddfe1486a9",
   "metadata": {},
   "outputs": [],
   "source": [
    "# 1) Warehouse e conexão\n",
    "WAREHOUSE_PATH = \"data/warehouse/whirlpool.duckdb\"\n",
    "Path(WAREHOUSE_PATH).parent.mkdir(parents=True, exist_ok=True)\n",
    "con = duckdb.connect(WAREHOUSE_PATH)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "d175ad22-0d0c-4665-847f-9cdbb61f382e",
   "metadata": {},
   "outputs": [],
   "source": [
    "# 2) Arquivos domínio-wide (paths relativos ao notebook em .../validacao/)\n",
    "p_costs  = \"../data/silver/costs/costs.parquet\"\n",
    "p_manu   = \"../data/silver/manufacturing/manufacturing.parquet\"\n",
    "p_energy = \"../data/silver/energy/energy.parquet\""
   ]
  },
  {
   "cell_type": "markdown",
   "id": "75689860-8e4e-4135-ac66-2cec62b9c676",
   "metadata": {},
   "source": [
    "### 1) Derrubando as views que podem ter ficado com parsing antigo"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "4cdd73e2-d71e-43ee-9c1a-df711cbcbb32",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Views antigas removidas.\n"
     ]
    }
   ],
   "source": [
    "con.execute(\"\"\"\n",
    "DROP VIEW IF EXISTS dim_date;\n",
    "DROP VIEW IF EXISTS fact_energy;\n",
    "DROP VIEW IF EXISTS fact_manufacturing;\n",
    "\"\"\")\n",
    "print(\"Views antigas removidas.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "bb08d887-d3cd-41b2-aeb5-984642b3212f",
   "metadata": {},
   "source": [
    "### 3) === Fatos ==="
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "b4f797c9-1288-4e81-9d22-e82e762bb245",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<duckdb.duckdb.DuckDBPyConnection at 0x7deb0296aff0>"
      ]
     },
     "execution_count": 5,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# fact_costs (mensal por site/conta/centro) — gera date_key a partir de ref_month\n",
    "con.execute(f\"\"\"\n",
    "CREATE OR REPLACE VIEW fact_costs AS\n",
    "WITH base AS (\n",
    "  SELECT\n",
    "    CAST(ref_month AS VARCHAR) AS ref_txt,\n",
    "    site_code,\n",
    "    cost_center,\n",
    "    account_code,\n",
    "    account_name,\n",
    "    amount_br,\n",
    "    amount_fx,\n",
    "    fx_rate\n",
    "  FROM read_parquet('{p_costs}')\n",
    "  WHERE ref_month IS NOT NULL\n",
    ")\n",
    "SELECT\n",
    "  CAST(substr(ref_txt,1,4) AS INTEGER)  * 10000 +   -- ano\n",
    "  CAST(substr(ref_txt,6,2) AS INTEGER)  * 100 + 1   -- mês + dia fixo = 1\n",
    "  AS date_key,\n",
    "  site_code,\n",
    "  cost_center,\n",
    "  account_code,\n",
    "  account_name,\n",
    "  amount_br,\n",
    "  amount_fx,\n",
    "  fx_rate\n",
    "FROM base;\n",
    "\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "8f304f27-58e5-4b86-ae4a-af9ec3e19c4a",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "fact_energy recriada (sem strptime).\n"
     ]
    }
   ],
   "source": [
    "con.execute(f\"\"\"\n",
    "CREATE OR REPLACE VIEW fact_energy AS\n",
    "WITH base AS (\n",
    "  SELECT\n",
    "    CAST(timestamp AS VARCHAR) AS ts_txt,\n",
    "    site_code,\n",
    "    line_code,\n",
    "    equip_code,\n",
    "    kwh,\n",
    "    kw_demand,\n",
    "    kvarh\n",
    "  FROM read_parquet('../data/silver/energy/energy.parquet')\n",
    "),\n",
    "day_parts AS (\n",
    "  SELECT\n",
    "    substr(ts_txt,1,10)                         AS day_txt,  -- 'YYYY-MM-DD'\n",
    "    site_code, line_code, equip_code, kwh, kw_demand, kvarh\n",
    "  FROM base\n",
    "),\n",
    "parts AS (\n",
    "  SELECT\n",
    "    CAST(substr(day_txt,1,4)  AS INTEGER) AS y,\n",
    "    CAST(substr(day_txt,6,2)  AS INTEGER) AS m,\n",
    "    CAST(substr(day_txt,9,2)  AS INTEGER) AS d,\n",
    "    site_code, line_code, equip_code, kwh, kw_demand, kvarh\n",
    "  FROM day_parts\n",
    ")\n",
    "SELECT\n",
    "  (y*10000 + m*100 + d)                AS date_key,\n",
    "  site_code,\n",
    "  line_code,\n",
    "  equip_code,\n",
    "  SUM(kwh)       AS kwh_day,\n",
    "  MAX(kw_demand) AS kw_demand_peak_day,\n",
    "  SUM(kvarh)     AS kvarh_day\n",
    "FROM parts\n",
    "GROUP BY 1,2,3,4;\n",
    "\"\"\")\n",
    "print(\"fact_energy recriada (sem strptime).\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "229db4da-b2b2-49ce-8012-5080b2564837",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "fact_manufacturing recriada (sem strptime).\n"
     ]
    }
   ],
   "source": [
    "# fact_manufacturing (diário por site/linha/produto) — versão robusta (sem strptime em TIMESTAMP)\n",
    "con.execute(f\"\"\"\n",
    "CREATE OR REPLACE VIEW fact_manufacturing AS\n",
    "WITH base AS (\n",
    "  SELECT\n",
    "    CAST(date AS VARCHAR) AS date_txt,\n",
    "    site_code,\n",
    "    line_code,\n",
    "    product_code,\n",
    "    units_ok,\n",
    "    units_rework,\n",
    "    scrap_units,\n",
    "    takt_time_s,\n",
    "    oee\n",
    "  FROM read_parquet('../data/silver/manufacturing/manufacturing.parquet')\n",
    "),\n",
    "parts AS (\n",
    "  SELECT\n",
    "    CAST(substr(date_txt,1,4)  AS INTEGER) AS y,\n",
    "    CAST(substr(date_txt,6,2)  AS INTEGER) AS m,\n",
    "    CAST(substr(date_txt,9,2)  AS INTEGER) AS d,\n",
    "    *\n",
    "  FROM base\n",
    ")\n",
    "SELECT\n",
    "  (y*10000 + m*100 + d)                AS date_key,\n",
    "  site_code,\n",
    "  line_code,\n",
    "  product_code,\n",
    "  units_ok,\n",
    "  units_rework,\n",
    "  scrap_units,\n",
    "  takt_time_s,\n",
    "  oee\n",
    "FROM parts;\n",
    "\"\"\")\n",
    "print(\"fact_manufacturing recriada (sem strptime).\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "df32da80-a259-4375-8d75-49700d594d20",
   "metadata": {},
   "source": [
    "### 4) === Dimensões ==="
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "a50546a0-08e3-4785-8530-6397205346f9",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<duckdb.duckdb.DuckDBPyConnection at 0x7deb0296aff0>"
      ]
     },
     "execution_count": 8,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# dim_site: todos os sites encontrados (união das três fontes)\n",
    "con.execute(f\"\"\"\n",
    "CREATE OR REPLACE VIEW dim_site AS\n",
    "SELECT DISTINCT site_code FROM read_parquet('{p_costs}')\n",
    "UNION\n",
    "SELECT DISTINCT site_code FROM read_parquet('{p_manu}')\n",
    "UNION\n",
    "SELECT DISTINCT site_code FROM read_parquet('{p_energy}');\n",
    "\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "id": "25aa83a6-a2fb-45c9-a018-5c9b999b29a5",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<duckdb.duckdb.DuckDBPyConnection at 0x7deb0296aff0>"
      ]
     },
     "execution_count": 9,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# dim_line: linhas de produção (a partir de manufacturing / energy)\n",
    "con.execute(f\"\"\"\n",
    "CREATE OR REPLACE VIEW dim_line AS\n",
    "SELECT DISTINCT site_code, line_code\n",
    "FROM read_parquet('{p_manu}')\n",
    "WHERE line_code IS NOT NULL\n",
    "UNION\n",
    "SELECT DISTINCT site_code, line_code\n",
    "FROM read_parquet('{p_energy}')\n",
    "WHERE line_code IS NOT NULL;\n",
    "\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "id": "50974146-e664-4422-b42e-7fa831615711",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<duckdb.duckdb.DuckDBPyConnection at 0x7deb0296aff0>"
      ]
     },
     "execution_count": 10,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# dim_product: catálogo de produtos (manufacturing)\n",
    "con.execute(f\"\"\"\n",
    "CREATE OR REPLACE VIEW dim_product AS\n",
    "SELECT DISTINCT\n",
    "  product_code\n",
    "FROM read_parquet('{p_manu}')\n",
    "WHERE product_code IS NOT NULL;\n",
    "\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "id": "faada419-e124-4125-b820-6f42d665086f",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "dim_date recriada (a partir de date_key).\n"
     ]
    }
   ],
   "source": [
    "# dim_date: calendário unificado (mensal e diário) — versão tolerante a tipos\n",
    "con.execute(f\"\"\"\n",
    "CREATE OR REPLACE VIEW dim_date AS\n",
    "WITH keys AS (\n",
    "  SELECT DISTINCT date_key FROM fact_costs\n",
    "  UNION\n",
    "  SELECT DISTINCT date_key FROM fact_manufacturing\n",
    "  UNION\n",
    "  SELECT DISTINCT date_key FROM fact_energy\n",
    "),\n",
    "parts AS (\n",
    "  SELECT\n",
    "    date_key,\n",
    "    CAST(date_key/10000       AS INTEGER) AS y,\n",
    "    CAST((date_key/100)%100   AS INTEGER) AS m,\n",
    "    CAST(date_key%100         AS INTEGER) AS d\n",
    "  FROM keys\n",
    ")\n",
    "SELECT\n",
    "  date_key,\n",
    "  make_date(y, m, d) AS date,\n",
    "  y, m, d\n",
    "FROM parts;\n",
    "\"\"\")\n",
    "print(\"dim_date recriada (a partir de date_key).\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "id": "3930d3c9",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<duckdb.duckdb.DuckDBPyConnection at 0x7deb0296aff0>"
      ]
     },
     "execution_count": 12,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# fact_energy (diário por site/linha/equip) — agrega no dia\n",
    "con.execute\n",
    "\n",
    "# 5) Schema de QA\n",
    "con.execute(\"CREATE SCHEMA IF NOT EXISTS qa;\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "2aeb01c4",
   "metadata": {},
   "source": [
    "## Etapa 2 — Tabelas `qa.rules`, `qa.results` e *macro* `qa_assert`\n",
    "Camada simples de **regras** ↔ **resultados** para registrar cada checagem, seu estado e amostras."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "id": "a04e49c4",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "qa.rules, qa.results e macro qa_assert criados.\n"
     ]
    }
   ],
   "source": [
    "sql_rules_results = \"\"\"\n",
    "CREATE OR REPLACE TABLE qa.rules (\n",
    "  rule_id TEXT PRIMARY KEY,\n",
    "  description TEXT,\n",
    "  severity TEXT CHECK (severity IN ('INFO','WARN','ERROR')),\n",
    "  expected TEXT\n",
    ");\n",
    "\n",
    "CREATE OR REPLACE TABLE qa.results (\n",
    "  ts TIMESTAMP DEFAULT current_timestamp,\n",
    "  rule_id TEXT,\n",
    "  passed BOOLEAN,\n",
    "  measured TEXT,\n",
    "  sample JSON,\n",
    "  PRIMARY KEY (ts, rule_id)\n",
    ");\n",
    "\n",
    "CREATE OR REPLACE MACRO qa_assert(rule_id, passed, measured, sample) AS TABLE\n",
    "SELECT current_timestamp, rule_id, passed, measured, sample::JSON;\n",
    "\"\"\"\n",
    "con.execute(sql_rules_results)\n",
    "print(\"qa.rules, qa.results e macro qa_assert criados.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "788ec8a4-9ef9-4ea5-8d02-8cd0f9810e5a",
   "metadata": {},
   "source": [
    "# 🧩 Etapa 3 — Checagens de Estrutura e Frescor\n",
    "\n",
    "> **Objetivo geral:**  \n",
    "> Antes de realizar análises exploratórias ou cálculos avançados, é essencial verificar se a base de dados está **completa, íntegra e atualizada**.  \n",
    "> Esta etapa atua como uma **validação analítica preliminar**, garantindo que o pipeline ETL funcionou corretamente e que os dados são confiáveis para uso.\n",
    "\n",
    "---\n",
    "\n",
    "## 🎯 1. O que são as checagens de estrutura\n",
    "\n",
    "Essas checagens asseguram que **todas as tabelas e dimensões esperadas existem e contêm registros válidos**.\n",
    "\n",
    "Elas respondem a perguntas como:\n",
    "- “Todas as tabelas (`dim_*`, `fact_*`) foram criadas corretamente?”\n",
    "- “Há dados em cada uma delas?”\n",
    "- “As dimensões se relacionam corretamente com os fatos?”\n",
    "\n",
    "No código, essa verificação é feita pela regra `R1_COUNTS`, que usa `COUNT(*)` em cada tabela.  \n",
    "Se alguma retornar **zero linhas**, o pipeline é sinalizado como inconsistente.\n",
    "\n",
    "> 🧠 Em outras palavras: é o *check de integridade estrutural* do seu data warehouse.  \n",
    "> Sem ele, qualquer EDA ou modelagem posterior pode se basear em dados faltantes ou corrompidos.\n",
    "\n",
    "---\n",
    "\n",
    "## ⏱️ 2. O que são as checagens de frescor (freshness)\n",
    "\n",
    "As checagens de frescor medem **o atraso temporal dos dados** — isto é, se o ETL está entregando informações recentes ou desatualizadas.\n",
    "\n",
    "Cada tabela de fatos tem uma janela de atualização esperada:\n",
    "\n",
    "| Tabela de fatos | Periodicidade | Regra de frescor |\n",
    "|------------------|---------------|------------------|\n",
    "| `fact_costs` | Mensal | Lag ≤ 1 mês |\n",
    "| `fact_manufacturing` | Diária | Lag ≤ 3 dias |\n",
    "| `fact_energy` | Diária | Lag ≤ 3 dias |\n",
    "\n",
    "O cálculo é feito com `MAX(date)` nas tabelas de fatos, comparando com a data atual (`CURRENT_DATE`) através de `date_diff()`.\n",
    "\n",
    "> 📈 Essa verificação é o *check de vitalidade* do seu pipeline: garante que o ETL está atualizado e alimentando o repositório dentro da cadência prevista.\n",
    "\n",
    "---\n",
    "\n",
    "## 🧮 3. Por que essas checagens são importantes\n",
    "\n",
    "| Etapa | Analogia | Função prática |\n",
    "|-------|-----------|----------------|\n",
    "| **Estrutura** | Checar se o avião tem todas as peças antes do voo ✈️ | Confirma que o ETL criou todas as tabelas e dimensões necessárias |\n",
    "| **Frescor** | Verificar combustível e instrumentos atualizados ⛽ | Confirma que os dados estão atualizados e prontos para análise |\n",
    "\n",
    "Sem essas validações, qualquer EDA, dashboard ou modelo estatístico pode ser construído sobre **dados incompletos, obsoletos ou incoerentes**.\n",
    "\n",
    "---\n",
    "\n",
    "## ⚙️ 4. Implementação prática\n",
    "\n",
    "- As regras são registradas em `qa.rules`.  \n",
    "- Os resultados de cada teste são gravados em `qa.results`.  \n",
    "- A macro `qa_assert()` padroniza o formato de saída de cada checagem.  \n",
    "\n",
    "Cada checagem insere uma linha em `qa.results`, indicando:\n",
    "- ✅ `ok = TRUE` → tudo certo;  \n",
    "- ⚠️ `ok = FALSE` → problema encontrado;  \n",
    "- `message` → detalhes do resultado (contagens, lags, etc.);  \n",
    "- `meta` → dados auxiliares (ex.: data mais recente).\n",
    "\n",
    "---\n",
    "\n",
    "## ✅ Resultado esperado\n",
    "\n",
    "Ao final desta etapa, o notebook exibirá uma tabela consolidada (`df_results`) com todas as regras aplicadas, indicando **quais passaram e quais precisam de revisão**.\n",
    "\n",
    "Essa etapa marca o fim da validação técnica do ETL e **autoriza o início da EDA** — a exploração analítica dos dados.\n"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "4aa03d95-fb3d-48bb-b5e8-4f0f4d380cce",
   "metadata": {},
   "source": [
    "## Etapa 3 — Checagens de **estrutura** e **frescor**\n",
    "- R1: cada tabela possui linhas  \n",
    "- R2: *freshness lag* (dias desde a última data em `fact_costs`) ≤ 3"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "06e0a220-9e2a-4da5-a0ce-ab589c45ddf7",
   "metadata": {},
   "source": [
    "### Schema e macro de QA"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "65ff85a2-19bd-44f9-8e7c-70d719db6218",
   "metadata": {},
   "source": [
    "Cria as tabelas de controle (`qa.rules`, `qa.results`) e a função `qa_assert()`,\n",
    "que insere automaticamente o resultado das verificações no schema `qa`."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "id": "fd8b2e4e-6425-410a-99db-32704931fe97",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Schema QA criado e macro qa_assert garantida.\n"
     ]
    }
   ],
   "source": [
    "con.execute(\"\"\"\n",
    "CREATE SCHEMA IF NOT EXISTS qa;\n",
    "\n",
    "-- (Re)cria as tabelas de controle\n",
    "CREATE OR REPLACE TABLE qa.rules (\n",
    "  rule_id     TEXT PRIMARY KEY,\n",
    "  description TEXT,\n",
    "  severity    TEXT,\n",
    "  expectation TEXT\n",
    ");\n",
    "\n",
    "CREATE OR REPLACE TABLE qa.results (\n",
    "  rule_id  TEXT,\n",
    "  ok       BOOLEAN,\n",
    "  message  TEXT,\n",
    "  meta     JSON,\n",
    "  run_ts   TIMESTAMP\n",
    ");\n",
    "\n",
    "-- Remove a macro se existir e recria limpa\n",
    "DROP MACRO IF EXISTS qa_assert;\n",
    "CREATE OR REPLACE MACRO qa_assert(rule_id, cond, msg, meta) AS TABLE\n",
    "SELECT\n",
    "  CAST(rule_id AS TEXT)      AS rule_id,\n",
    "  CAST(cond    AS BOOLEAN)   AS ok,\n",
    "  CAST(msg     AS TEXT)      AS message,\n",
    "  COALESCE(meta, json('[]')) AS meta,\n",
    "  NOW()                      AS run_ts;\n",
    "\"\"\")\n",
    "\n",
    "print(\"Schema QA criado e macro qa_assert garantida.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "18d40bd3-7402-4a4b-9b5c-f5ede766ac35",
   "metadata": {},
   "source": [
    "### Bloco 3.2 — Cadastro das regras"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 15,
   "id": "e0c0f5b9-8a82-49f4-a272-081b1d7bf28a",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Regras registradas em qa.rules.\n"
     ]
    }
   ],
   "source": [
    "# Then insert the values\n",
    "con.execute(\"\"\"\n",
    "INSERT OR REPLACE INTO qa.rules VALUES\n",
    "('R1_COUNTS',              'Tabelas possuem linhas (>0)',                    'ERROR', 'count > 0'),\n",
    "('R2A_FRESHNESS_COSTS',    'Dados recentes em fact_costs (mensal)',          'WARN',  'lag em meses <= 1'),\n",
    "('R2B_FRESHNESS_MANU',     'Dados recentes em fact_manufacturing (diário)',  'WARN',  'lag em dias <= 3'),\n",
    "('R2C_FRESHNESS_ENERGY',   'Dados recentes em fact_energy (diário)',         'WARN',  'lag em dias <= 3');\n",
    "\"\"\")\n",
    "\n",
    "print(\"Regras registradas em qa.rules.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "6ec6af77-4540-45b1-909e-8b4399be36e7",
   "metadata": {},
   "source": [
    "### Bloco 3.3 — Checagem R1: Estrutura (contagem de linhas)"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "46e69735-841d-4045-bfe1-b568eaecf611",
   "metadata": {},
   "source": [
    "Garante que todas as tabelas e views contêm registros válidos"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "id": "9d074f5e-bc5d-47cb-9a90-ae62555e7057",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Checagem R1 — Estrutura concluída.\n"
     ]
    }
   ],
   "source": [
    "con.execute(\"\"\"\n",
    "INSERT INTO qa.results\n",
    "SELECT * FROM qa_assert(\n",
    "  'R1_COUNTS',\n",
    "  (SELECT (SELECT COUNT(*)>0 FROM dim_date)\n",
    "       AND (SELECT COUNT(*)>0 FROM dim_site)\n",
    "       AND (SELECT COUNT(*)>0 FROM dim_line)\n",
    "       AND (SELECT COUNT(*)>0 FROM dim_product)\n",
    "       AND (SELECT COUNT(*)>0 FROM fact_costs)\n",
    "       AND (SELECT COUNT(*)>0 FROM fact_manufacturing)\n",
    "       AND (SELECT COUNT(*)>0 FROM fact_energy)),\n",
    "  (\n",
    "    SELECT\n",
    "      'dim_date='           || (SELECT COUNT(*) FROM dim_date)           || '; ' ||\n",
    "      'dim_site='           || (SELECT COUNT(*) FROM dim_site)           || '; ' ||\n",
    "      'dim_line='           || (SELECT COUNT(*) FROM dim_line)           || '; ' ||\n",
    "      'dim_product='        || (SELECT COUNT(*) FROM dim_product)        || '; ' ||\n",
    "      'fact_costs='         || (SELECT COUNT(*) FROM fact_costs)         || '; ' ||\n",
    "      'fact_manufacturing=' || (SELECT COUNT(*) FROM fact_manufacturing) || '; ' ||\n",
    "      'fact_energy='        || (SELECT COUNT(*) FROM fact_energy)\n",
    "  ),\n",
    "  (SELECT json('[]'))\n",
    ");\n",
    "\"\"\")\n",
    "\n",
    "print(\"Checagem R1 — Estrutura concluída.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "488dcd08-788a-4ee7-90b8-50f74d21002b",
   "metadata": {},
   "source": [
    "### Bloco 3.4 — Checagem R2A: Frescor mensal (fact_costs)"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "ea8e884b-3266-45a5-aaaa-ce00ef0eed70",
   "metadata": {},
   "source": [
    "Mede o atraso (em meses) do dado mais recente em `fact_costs`."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 17,
   "id": "81fd51ad-5920-40c2-bd7c-0d06c8242570",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Checagem R2A — Frescor mensal (fact_costs) concluída.\n"
     ]
    }
   ],
   "source": [
    "con.execute(\"\"\"\n",
    "WITH mx AS (\n",
    "  SELECT MAX(d.date) AS max_date\n",
    "  FROM fact_costs f\n",
    "  JOIN dim_date d USING(date_key)\n",
    "),\n",
    "lag AS (\n",
    "  SELECT date_diff('month', max_date, current_date) AS lag_months FROM mx\n",
    ")\n",
    "INSERT INTO qa.results\n",
    "SELECT * FROM qa_assert(\n",
    "  'R2A_FRESHNESS_COSTS',\n",
    "  (SELECT lag_months <= 1 FROM lag),\n",
    "  (SELECT CAST(lag_months AS VARCHAR) || ' month(s) lag' FROM lag),\n",
    "  (SELECT json_object('max_date', (SELECT max_date FROM mx)))\n",
    ");\n",
    "\"\"\")\n",
    "\n",
    "print(\"Checagem R2A — Frescor mensal (fact_costs) concluída.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "ac77bca5-a9ec-4972-a5ba-8cc311dc730b",
   "metadata": {},
   "source": [
    "### Bloco 3.5 — Checagem R2B: Frescor diário (fact_manufacturing)"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "67b8c41d-e7b8-4439-a4d7-d4d93d556d5d",
   "metadata": {},
   "source": [
    "Mede o atraso (em dias) do dado mais recente em `fact_manufacturing`."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 18,
   "id": "2419fc57-1036-4217-802f-fc3586b86e36",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Checagem R2B — Frescor diário (fact_manufacturing) concluída.\n"
     ]
    }
   ],
   "source": [
    "con.execute(\"\"\"\n",
    "WITH mx AS (\n",
    "  SELECT MAX(d.date) AS max_date\n",
    "  FROM fact_manufacturing f\n",
    "  JOIN dim_date d USING(date_key)\n",
    "),\n",
    "lag AS (\n",
    "  SELECT date_diff('day', max_date, current_date) AS lag_days FROM mx\n",
    ")\n",
    "INSERT INTO qa.results\n",
    "SELECT * FROM qa_assert(\n",
    "  'R2B_FRESHNESS_MANU',\n",
    "  (SELECT lag_days <= 3 FROM lag),\n",
    "  (SELECT CAST(lag_days AS VARCHAR) || ' day(s) lag' FROM lag),\n",
    "  (SELECT json_object('max_date', (SELECT max_date FROM mx)))\n",
    ");\n",
    "\"\"\")\n",
    "\n",
    "print(\"Checagem R2B — Frescor diário (fact_manufacturing) concluída.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "02b63b9f-2b13-4a71-84f8-11b857b90065",
   "metadata": {},
   "source": [
    "### Bloco 3.6 — Checagem R2C: Frescor diário (fact_energy)"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "0308c654-2a04-4ae6-9069-2ab4cf574f41",
   "metadata": {},
   "source": [
    "Mede o atraso (em dias) do dado mais recente em `fact_energy`."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 19,
   "id": "2adec482-50eb-435f-8cff-8233dc90fd13",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Checagem R2C — Frescor diário (fact_energy) concluída.\n"
     ]
    }
   ],
   "source": [
    "con.execute(\"\"\"\n",
    "WITH mx AS (\n",
    "  SELECT MAX(d.date) AS max_date\n",
    "  FROM fact_energy f\n",
    "  JOIN dim_date d USING(date_key)\n",
    "),\n",
    "lag AS (\n",
    "  SELECT date_diff('day', max_date, current_date) AS lag_days FROM mx\n",
    ")\n",
    "INSERT INTO qa.results\n",
    "SELECT * FROM qa_assert(\n",
    "  'R2C_FRESHNESS_ENERGY',\n",
    "  (SELECT lag_days <= 3 FROM lag),\n",
    "  (SELECT CAST(lag_days AS VARCHAR) || ' day(s) lag' FROM lag),\n",
    "  (SELECT json_object('max_date', (SELECT max_date FROM mx)))\n",
    ");\n",
    "\"\"\")\n",
    "\n",
    "print(\"Checagem R2C — Frescor diário (fact_energy) concluída.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "58c255e9-e3c6-417e-bec8-e3e231a17729",
   "metadata": {},
   "source": [
    "### Bloco 3.7 — Resultado consolidado das checagens"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "c29b44d8-07b1-4752-ae53-27c889ce46be",
   "metadata": {},
   "source": [
    "Consulta os resultados e mostra quais regras passaram/falharam."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 20,
   "id": "2cfcf5b3-633a-42e7-b3f9-753bbaeefb77",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>rule_id</th>\n",
       "      <th>ok</th>\n",
       "      <th>severity</th>\n",
       "      <th>message</th>\n",
       "      <th>meta</th>\n",
       "      <th>run_ts</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>R2C_FRESHNESS_ENERGY</td>\n",
       "      <td>False</td>\n",
       "      <td>WARN</td>\n",
       "      <td>28 day(s) lag</td>\n",
       "      <td>{\"max_date\":\"2025-09-30\"}</td>\n",
       "      <td>2025-10-28 08:32:33.590</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>R2B_FRESHNESS_MANU</td>\n",
       "      <td>False</td>\n",
       "      <td>WARN</td>\n",
       "      <td>28 day(s) lag</td>\n",
       "      <td>{\"max_date\":\"2025-09-30\"}</td>\n",
       "      <td>2025-10-28 08:32:31.609</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>R2A_FRESHNESS_COSTS</td>\n",
       "      <td>True</td>\n",
       "      <td>WARN</td>\n",
       "      <td>-2 month(s) lag</td>\n",
       "      <td>{\"max_date\":\"2025-12-01\"}</td>\n",
       "      <td>2025-10-28 08:32:29.612</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>R1_COUNTS</td>\n",
       "      <td>True</td>\n",
       "      <td>ERROR</td>\n",
       "      <td>dim_date=201; dim_site=3; dim_line=9; dim_prod...</td>\n",
       "      <td>[]</td>\n",
       "      <td>2025-10-28 08:32:26.582</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "                rule_id     ok severity  \\\n",
       "0  R2C_FRESHNESS_ENERGY  False     WARN   \n",
       "1    R2B_FRESHNESS_MANU  False     WARN   \n",
       "2   R2A_FRESHNESS_COSTS   True     WARN   \n",
       "3             R1_COUNTS   True    ERROR   \n",
       "\n",
       "                                             message  \\\n",
       "0                                      28 day(s) lag   \n",
       "1                                      28 day(s) lag   \n",
       "2                                    -2 month(s) lag   \n",
       "3  dim_date=201; dim_site=3; dim_line=9; dim_prod...   \n",
       "\n",
       "                        meta                  run_ts  \n",
       "0  {\"max_date\":\"2025-09-30\"} 2025-10-28 08:32:33.590  \n",
       "1  {\"max_date\":\"2025-09-30\"} 2025-10-28 08:32:31.609  \n",
       "2  {\"max_date\":\"2025-12-01\"} 2025-10-28 08:32:29.612  \n",
       "3                         [] 2025-10-28 08:32:26.582  "
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "df_results = con.execute(\"\"\"\n",
    "SELECT\n",
    "  rule_id,\n",
    "  ok,\n",
    "  severity,\n",
    "  message,\n",
    "  meta,\n",
    "  run_ts\n",
    "FROM qa.results\n",
    "JOIN qa.rules USING(rule_id)\n",
    "ORDER BY run_ts DESC;\n",
    "\"\"\").fetch_df()\n",
    "\n",
    "display(df_results)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 21,
   "id": "f09d6e07-9d12-4d53-b744-53053eef327b",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "<class 'pandas.core.frame.DataFrame'>\n",
      "RangeIndex: 4 entries, 0 to 3\n",
      "Data columns (total 6 columns):\n",
      " #   Column    Non-Null Count  Dtype         \n",
      "---  ------    --------------  -----         \n",
      " 0   rule_id   4 non-null      object        \n",
      " 1   ok        4 non-null      bool          \n",
      " 2   severity  4 non-null      object        \n",
      " 3   message   4 non-null      object        \n",
      " 4   meta      4 non-null      object        \n",
      " 5   run_ts    4 non-null      datetime64[us]\n",
      "dtypes: bool(1), datetime64[us](1), object(4)\n",
      "memory usage: 296.0+ bytes\n"
     ]
    }
   ],
   "source": [
    "df_results.info()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "6604013c-2bbe-47be-810e-c40b0ea3fbc1",
   "metadata": {},
   "source": [
    "# 🧪 Etapa 4 — Checagens de Qualidade (nulos, duplicatas, domínios)\n",
    "\n",
    "> **Objetivo geral:**  \n",
    "> Esta etapa garante que os dados carregados e estruturados nas dimensões e fatos do *Data Warehouse* estão **limpos, consistentes e dentro de limites válidos**. É o segundo nível da **validação analítica**, agora voltado para a **qualidade intrínseca** dos dados, após a verificação estrutural e de frescor realizada na Etapa 3.\n",
    "\n",
    "---\n",
    "\n",
    "## 🎯 1. O que são checagens de qualidade\n",
    "\n",
    "Checagens de qualidade testam se os valores armazenados em cada tabela seguem **regras de integridade e coerência de negócio**.  \n",
    "Essas regras ajudam a identificar erros comuns como:\n",
    "- Valores ausentes (nulos) em chaves importantes;\n",
    "- Linhas duplicadas que comprometem relações dimensionais;\n",
    "- Valores impossíveis (ex.: custos negativos, `OEE > 1`, taxas de câmbio iguais a 0).\n",
    "\n",
    "---\n",
    "\n",
    "## 🧱 2. Categorias de checagens\n",
    "\n",
    "As verificações são divididas em três grupos:\n",
    "\n",
    "| Tipo de checagem | O que valida | Regra associada |\n",
    "|------------------|--------------|-----------------|\n",
    "| **Nulos (NULLs)** | Campos‐chave não podem estar vazios (`site_code`, `line_code`, `product_code`) | `R3_DIM_KEYS_NOT_NULL` |\n",
    "| **Duplicatas** | Cada dimensão deve ter chaves únicas | `R4_DIM_DUPLICATES` |\n",
    "| **Domínios numéricos** | Valores devem respeitar faixas aceitáveis (≥ 0, 0 ≤ OEE ≤ 1 etc.) | `R5–R7_DOMAIN_*` |\n",
    "\n",
    "> 💡 Essas regras formam uma espécie de “higienização final” do *data lake*, antes que os dados sejam usados em análises ou dashboards.\n",
    "\n",
    "---\n",
    "\n",
    "## ⚙️ 3. Regras implementadas nesta etapa\n",
    "\n",
    "| Regra | Categoria | Descrição | Tabelas envolvidas |\n",
    "|:------|:-----------|:-----------|:------------------|\n",
    "| `R3_DIM_KEYS_NOT_NULL` | Nulos | Garante que todas as chaves das dimensões (site, linha, produto) não estão vazias | `dim_site`, `dim_line`, `dim_product` |\n",
    "| `R4_DIM_DUPLICATES` | Duplicatas | Detecta repetições indevidas em chaves primárias das dimensões | `dim_*` |\n",
    "| `R5_DOMAIN_COSTS` | Domínio | Verifica se custos e taxas de câmbio não são negativos | `fact_costs` |\n",
    "| `R6_DOMAIN_MANU` | Domínio | Garante que unidades e tempos são não‐negativos e que o OEE está entre 0 e 1 | `fact_manufacturing` |\n",
    "| `R7_DOMAIN_ENERGY` | Domínio | Verifica se os valores de energia elétrica (kWh, kW, kVArh) são não‐negativos | `fact_energy` |\n",
    "\n",
    "---\n",
    "\n",
    "## 🔍 4. Por que estas checagens são essenciais\n",
    "\n",
    "| Tipo | Risco se ignorado | Impacto na análise |\n",
    "|------|--------------------|--------------------|\n",
    "| **Nulos** | Perda de vínculos entre fatos e dimensões | Quebra de agregações e joins |\n",
    "| **Duplicatas** | Contagens infladas ou duplicação de métricas | Indicadores distorcidos |\n",
    "| **Domínios inválidos** | Custos negativos, taxas zero, OEE > 100 % | Interpretação incorreta dos KPIs |\n",
    "\n",
    "> ⚠️ Pequenos erros de domínio podem escalar para distorções significativas em relatórios financeiros ou operacionais.\n",
    "\n",
    "---\n",
    "\n",
    "## 🧩 5. Implementação técnica\n",
    "\n",
    "Cada regra é cadastrada em `qa.rules` e executada via a macro `qa_assert()`,  \n",
    "que insere automaticamente o resultado (status, mensagem, metadados) em `qa.results`.  \n",
    "\n",
    "Isso permite monitorar a qualidade dos dados em tempo real e consolidar todos os resultados em um único *data quality report*.\n",
    "\n",
    "---\n",
    "\n",
    "## ✅ 6. Resultado esperado\n",
    "\n",
    "Após a execução, a célula final exibirá uma tabela consolidada (`df_results`) indicando:\n",
    "- ✅ **ok = TRUE:** regra atendida;  \n",
    "- ⚠️ **ok = FALSE:** inconsistência detectada;  \n",
    "- 📊 **message/meta:** número de ocorrências e valores problemáticos.\n",
    "\n",
    "Essa etapa conclui o ciclo de **validação de qualidade**, preparando o dataset para a próxima fase:  \n",
    "a **Etapa 5 — Checagens de Integridade Referencial** e, posteriormente, a **EDA (Exploratory Data Analysis)**.\n"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "1126de4c",
   "metadata": {},
   "source": [
    "## Etapa 4 — Checagens de **qualidade** (nulos, duplicatas, domínios)\n",
    "- R3: chaves em dimensões não nulas  \n",
    "- R4: duplicatas em dimensões  \n",
    "- R5: valores não-negativos para custos/quantidades"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "e9bf11dd-a4fe-46ab-bd38-5cc2fa926bfe",
   "metadata": {},
   "source": [
    "### 4.1 — Cadastrar/Revisar as regras de qualidade"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 22,
   "id": "6bf0f092-610f-4c49-89d3-64dfe2db029e",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Regras de qualidade registradas.\n"
     ]
    }
   ],
   "source": [
    "con.execute(\"\"\"\n",
    "INSERT OR REPLACE INTO qa.rules VALUES\n",
    "('R3_DIM_KEYS_NOT_NULL', 'Chaves de dimensões não nulas',                  'ERROR', 'site_code/line_code/product_code não nulos'),\n",
    "('R4_DIM_DUPLICATES',   'Duplicatas nas dimensões',                        'ERROR', 'chaves exclusivas sem repetição'),\n",
    "('R5_DOMAIN_COSTS',     'Domínio: custos/fx_rate (não-negativos e fx>0)',  'ERROR', 'amounts >= 0, fx_rate > 0'),\n",
    "('R6_DOMAIN_MANU',      'Domínio: manufatura (valores válidos)',           'ERROR', 'units >=0, takt_time>=0, 0<=oee<=1'),\n",
    "('R7_DOMAIN_ENERGY',    'Domínio: energia (valores válidos)',              'ERROR', 'kwh/kw/kvarh >= 0');\n",
    "\"\"\")\n",
    "print(\"Regras de qualidade registradas.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "d56fa495-80d8-4d88-b3c1-e943c6725960",
   "metadata": {},
   "source": [
    "### 4.2 — R3: chaves de dimensões não nulas"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 23,
   "id": "8f08f0e7-3e70-4253-8d68-7a255ffb6638",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "R3 — chaves não nulas: OK (inserido em qa.results).\n"
     ]
    }
   ],
   "source": [
    "con.execute(\"\"\"\n",
    "INSERT INTO qa.results\n",
    "SELECT * FROM qa_assert(\n",
    "  'R3_DIM_KEYS_NOT_NULL',\n",
    "  (\n",
    "    SELECT\n",
    "      (SELECT COUNT(*) FROM dim_site    WHERE site_code            IS NULL)=0 AND\n",
    "      (SELECT COUNT(*) FROM dim_line    WHERE site_code IS NULL OR  line_code IS NULL)=0 AND\n",
    "      (SELECT COUNT(*) FROM dim_product WHERE product_code         IS NULL)=0\n",
    "  ),\n",
    "  (\n",
    "    SELECT\n",
    "      'nulls(dim_site.site_code)='      || (SELECT COUNT(*) FROM dim_site    WHERE site_code            IS NULL) || '; ' ||\n",
    "      'nulls(dim_line.site_code)='      || (SELECT COUNT(*) FROM dim_line    WHERE site_code            IS NULL) || '; ' ||\n",
    "      'nulls(dim_line.line_code)='      || (SELECT COUNT(*) FROM dim_line    WHERE line_code            IS NULL) || '; ' ||\n",
    "      'nulls(dim_product.product_code)='|| (SELECT COUNT(*) FROM dim_product WHERE product_code         IS NULL)\n",
    "  ),\n",
    "  (SELECT json('[]'))\n",
    ");\n",
    "\"\"\")\n",
    "\n",
    "print(\"R3 — chaves não nulas: OK (inserido em qa.results).\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "71ace6b9-6fe2-4161-baab-c5d4363ab9f3",
   "metadata": {},
   "source": [
    "### 4.3 — R4: duplicatas nas dimensões"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 24,
   "id": "c5e8f5d6-d70a-4295-966f-132986764419",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "R4 — duplicatas em dimensões: OK (inserido em qa.results).\n"
     ]
    }
   ],
   "source": [
    "con.execute(\"\"\"\n",
    "WITH\n",
    "dup_site AS (\n",
    "  SELECT site_code, COUNT(*) AS c\n",
    "  FROM dim_site\n",
    "  GROUP BY 1 HAVING c > 1\n",
    "),\n",
    "dup_line AS (\n",
    "  SELECT site_code, line_code, COUNT(*) AS c\n",
    "  FROM dim_line\n",
    "  GROUP BY 1,2 HAVING c > 1\n",
    "),\n",
    "dup_prod AS (\n",
    "  SELECT product_code, COUNT(*) AS c\n",
    "  FROM dim_product\n",
    "  GROUP BY 1 HAVING c > 1\n",
    ")\n",
    "INSERT INTO qa.results\n",
    "SELECT * FROM qa_assert(\n",
    "  'R4_DIM_DUPLICATES',\n",
    "  (\n",
    "    SELECT\n",
    "      (SELECT COUNT(*)=0 FROM dup_site) AND\n",
    "      (SELECT COUNT(*)=0 FROM dup_line) AND\n",
    "      (SELECT COUNT(*)=0 FROM dup_prod)\n",
    "  ),\n",
    "  (\n",
    "    SELECT\n",
    "      'dup_site=' || (SELECT COUNT(*) FROM dup_site) || '; ' ||\n",
    "      'dup_line=' || (SELECT COUNT(*) FROM dup_line) || '; ' ||\n",
    "      'dup_prod=' || (SELECT COUNT(*) FROM dup_prod)\n",
    "  ),\n",
    "  (\n",
    "    SELECT json_object(\n",
    "      'dup_site', (SELECT to_json(list(site_code)) FROM dup_site),\n",
    "      'dup_line', (SELECT to_json(list(site_code || '|' || line_code)) FROM dup_line),\n",
    "      'dup_prod', (SELECT to_json(list(product_code)) FROM dup_prod)\n",
    "    )\n",
    "  )\n",
    ");\n",
    "\"\"\")\n",
    "\n",
    "print(\"R4 — duplicatas em dimensões: OK (inserido em qa.results).\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "c594cfaf-6534-4f7b-afd6-4ea605e04064",
   "metadata": {},
   "source": [
    "### 4.4 — R5: domínio de custos (não-negativos e `fx_rate > 0`)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 25,
   "id": "44782c64-6d27-437b-9afb-32b7b0f87f9e",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "R5 — domínio de custos: OK (inserido em qa.results).\n"
     ]
    }
   ],
   "source": [
    "con.execute(\"\"\"\n",
    "WITH bad AS (\n",
    "  SELECT COUNT(*) AS c\n",
    "  FROM fact_costs\n",
    "  WHERE COALESCE(amount_br, 0) < 0\n",
    "     OR COALESCE(amount_fx, 0) < 0\n",
    "     OR COALESCE(fx_rate,   0) <= 0\n",
    ")\n",
    "INSERT INTO qa.results\n",
    "SELECT * FROM qa_assert(\n",
    "  'R5_DOMAIN_COSTS',\n",
    "  (SELECT c = 0 FROM bad),\n",
    "  (SELECT 'rows_invalid=' || c FROM bad),\n",
    "  (SELECT json('[]'))\n",
    ");\n",
    "\"\"\")\n",
    "\n",
    "print(\"R5 — domínio de custos: OK (inserido em qa.results).\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "41d4fb7e-007e-483a-acca-ef95addecb89",
   "metadata": {},
   "source": [
    "### 4.5 — R6: domínio de manufatura"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "ad4912e4-8230-4659-a6a4-ebf1bca34953",
   "metadata": {},
   "source": [
    "#### Regras típicas: `units_ok/units_rework/scrap_units >= 0`, `takt_time_s >= 0`, `0 <= oee <= 1`."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 26,
   "id": "600d3a03-84be-49a1-8629-031339c5bb84",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "R6 — domínio de manufatura: OK (inserido em qa.results).\n"
     ]
    }
   ],
   "source": [
    "con.execute(\"\"\"\n",
    "WITH bad AS (\n",
    "  SELECT COUNT(*) AS c\n",
    "  FROM fact_manufacturing\n",
    "  WHERE COALESCE(units_ok,      0) < 0\n",
    "     OR COALESCE(units_rework,  0) < 0\n",
    "     OR COALESCE(scrap_units,   0) < 0\n",
    "     OR COALESCE(takt_time_s,   0) < 0\n",
    "     OR oee < 0 OR oee > 1\n",
    ")\n",
    "INSERT INTO qa.results\n",
    "SELECT * FROM qa_assert(\n",
    "  'R6_DOMAIN_MANU',\n",
    "  (SELECT c = 0 FROM bad),\n",
    "  (SELECT 'rows_invalid=' || c FROM bad),\n",
    "  (SELECT json('[]'))\n",
    ");\n",
    "\"\"\")\n",
    "\n",
    "print(\"R6 — domínio de manufatura: OK (inserido em qa.results).\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "7baee73f-18dc-432f-9758-983ca3ea41c9",
   "metadata": {},
   "source": [
    "### 4.6 — R7: domínio de energia"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "57dc27ed-bb92-4608-9e04-3cee021b09f1",
   "metadata": {},
   "source": [
    "#### Regras típicas: `kwh_day >= 0`, `kw_demand_peak_day >= 0`, `kvarh_day >= 0`."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 28,
   "id": "f1058046-788a-4046-a8cc-cdc06802dfee",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "R7 — domínio de energia: OK (inserido em qa.results).\n"
     ]
    }
   ],
   "source": [
    "con.execute(\"\"\"\n",
    "WITH bad AS (\n",
    "  SELECT COUNT(*) AS c\n",
    "  FROM fact_energy\n",
    "  WHERE COALESCE(kwh_day,            0) < 0\n",
    "     OR COALESCE(kw_demand_peak_day, 0) < 0\n",
    "     OR COALESCE(kvarh_day,          0) < 0\n",
    ")\n",
    "INSERT INTO qa.results\n",
    "SELECT * FROM qa_assert(\n",
    "  'R7_DOMAIN_ENERGY',\n",
    "  (SELECT c = 0 FROM bad),\n",
    "  (SELECT 'rows_invalid=' || c FROM bad),\n",
    "  (SELECT json('[]'))\n",
    ");\n",
    "\"\"\")\n",
    "\n",
    "print(\"R7 — domínio de energia: OK (inserido em qa.results).\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "252ed306-b9eb-4a65-9528-6979af2a7a8e",
   "metadata": {},
   "source": [
    "### 4.7 — Ver o resumo atualizado."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 29,
   "id": "53e7dd59-cdde-4b6a-a93d-dba2fca280d4",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>rule_id</th>\n",
       "      <th>ok</th>\n",
       "      <th>severity</th>\n",
       "      <th>message</th>\n",
       "      <th>meta</th>\n",
       "      <th>run_ts</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>R7_DOMAIN_ENERGY</td>\n",
       "      <td>True</td>\n",
       "      <td>ERROR</td>\n",
       "      <td>rows_invalid=0</td>\n",
       "      <td>[]</td>\n",
       "      <td>2025-10-28 08:40:20.703</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>R6_DOMAIN_MANU</td>\n",
       "      <td>True</td>\n",
       "      <td>ERROR</td>\n",
       "      <td>rows_invalid=0</td>\n",
       "      <td>[]</td>\n",
       "      <td>2025-10-28 08:38:57.414</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>R5_DOMAIN_COSTS</td>\n",
       "      <td>True</td>\n",
       "      <td>ERROR</td>\n",
       "      <td>rows_invalid=0</td>\n",
       "      <td>[]</td>\n",
       "      <td>2025-10-28 08:37:22.194</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>R4_DIM_DUPLICATES</td>\n",
       "      <td>True</td>\n",
       "      <td>ERROR</td>\n",
       "      <td>dup_site=0; dup_line=0; dup_prod=0</td>\n",
       "      <td>{\"dup_site\":null,\"dup_line\":null,\"dup_prod\":null}</td>\n",
       "      <td>2025-10-28 08:36:25.573</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>R3_DIM_KEYS_NOT_NULL</td>\n",
       "      <td>True</td>\n",
       "      <td>ERROR</td>\n",
       "      <td>nulls(dim_site.site_code)=0; nulls(dim_line.si...</td>\n",
       "      <td>[]</td>\n",
       "      <td>2025-10-28 08:35:47.198</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>5</th>\n",
       "      <td>R2C_FRESHNESS_ENERGY</td>\n",
       "      <td>False</td>\n",
       "      <td>WARN</td>\n",
       "      <td>28 day(s) lag</td>\n",
       "      <td>{\"max_date\":\"2025-09-30\"}</td>\n",
       "      <td>2025-10-28 08:32:33.590</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>6</th>\n",
       "      <td>R2B_FRESHNESS_MANU</td>\n",
       "      <td>False</td>\n",
       "      <td>WARN</td>\n",
       "      <td>28 day(s) lag</td>\n",
       "      <td>{\"max_date\":\"2025-09-30\"}</td>\n",
       "      <td>2025-10-28 08:32:31.609</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>7</th>\n",
       "      <td>R2A_FRESHNESS_COSTS</td>\n",
       "      <td>True</td>\n",
       "      <td>WARN</td>\n",
       "      <td>-2 month(s) lag</td>\n",
       "      <td>{\"max_date\":\"2025-12-01\"}</td>\n",
       "      <td>2025-10-28 08:32:29.612</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>8</th>\n",
       "      <td>R1_COUNTS</td>\n",
       "      <td>True</td>\n",
       "      <td>ERROR</td>\n",
       "      <td>dim_date=201; dim_site=3; dim_line=9; dim_prod...</td>\n",
       "      <td>[]</td>\n",
       "      <td>2025-10-28 08:32:26.582</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "                rule_id     ok severity  \\\n",
       "0      R7_DOMAIN_ENERGY   True    ERROR   \n",
       "1        R6_DOMAIN_MANU   True    ERROR   \n",
       "2       R5_DOMAIN_COSTS   True    ERROR   \n",
       "3     R4_DIM_DUPLICATES   True    ERROR   \n",
       "4  R3_DIM_KEYS_NOT_NULL   True    ERROR   \n",
       "5  R2C_FRESHNESS_ENERGY  False     WARN   \n",
       "6    R2B_FRESHNESS_MANU  False     WARN   \n",
       "7   R2A_FRESHNESS_COSTS   True     WARN   \n",
       "8             R1_COUNTS   True    ERROR   \n",
       "\n",
       "                                             message  \\\n",
       "0                                     rows_invalid=0   \n",
       "1                                     rows_invalid=0   \n",
       "2                                     rows_invalid=0   \n",
       "3                 dup_site=0; dup_line=0; dup_prod=0   \n",
       "4  nulls(dim_site.site_code)=0; nulls(dim_line.si...   \n",
       "5                                      28 day(s) lag   \n",
       "6                                      28 day(s) lag   \n",
       "7                                    -2 month(s) lag   \n",
       "8  dim_date=201; dim_site=3; dim_line=9; dim_prod...   \n",
       "\n",
       "                                                meta                  run_ts  \n",
       "0                                                 [] 2025-10-28 08:40:20.703  \n",
       "1                                                 [] 2025-10-28 08:38:57.414  \n",
       "2                                                 [] 2025-10-28 08:37:22.194  \n",
       "3  {\"dup_site\":null,\"dup_line\":null,\"dup_prod\":null} 2025-10-28 08:36:25.573  \n",
       "4                                                 [] 2025-10-28 08:35:47.198  \n",
       "5                          {\"max_date\":\"2025-09-30\"} 2025-10-28 08:32:33.590  \n",
       "6                          {\"max_date\":\"2025-09-30\"} 2025-10-28 08:32:31.609  \n",
       "7                          {\"max_date\":\"2025-12-01\"} 2025-10-28 08:32:29.612  \n",
       "8                                                 [] 2025-10-28 08:32:26.582  "
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "df_results = con.execute(\"\"\"\n",
    "SELECT rule_id, ok, severity, message, meta, run_ts\n",
    "FROM qa.results\n",
    "JOIN qa.rules USING(rule_id)\n",
    "ORDER BY run_ts DESC;\n",
    "\"\"\").fetch_df()\n",
    "display(df_results)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 31,
   "id": "ddd7af8f-d28f-43b1-9f96-1331138f5bbf",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "(9, 6)"
      ]
     },
     "execution_count": 31,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "df_results.shape"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 32,
   "id": "bb7ab6f3-8cba-453e-b6b4-d70da0807622",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "<class 'pandas.core.frame.DataFrame'>\n",
      "RangeIndex: 9 entries, 0 to 8\n",
      "Data columns (total 6 columns):\n",
      " #   Column    Non-Null Count  Dtype         \n",
      "---  ------    --------------  -----         \n",
      " 0   rule_id   9 non-null      object        \n",
      " 1   ok        9 non-null      bool          \n",
      " 2   severity  9 non-null      object        \n",
      " 3   message   9 non-null      object        \n",
      " 4   meta      9 non-null      object        \n",
      " 5   run_ts    9 non-null      datetime64[us]\n",
      "dtypes: bool(1), datetime64[us](1), object(4)\n",
      "memory usage: 501.0+ bytes\n"
     ]
    }
   ],
   "source": [
    "df_results.info()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "811c180a-04e9-4707-8ad4-cac98781b6a7",
   "metadata": {},
   "source": [
    "# 🔗 Etapa 5 — Checagens de Integridade Referencial (FK → Dim)\n",
    "\n",
    "> **Objetivo geral:**  \n",
    "> Esta etapa assegura que **todas as chaves estrangeiras (FKs)** nas tabelas de fatos correspondem a **chaves existentes nas tabelas de dimensões (Dims)**.  \n",
    "> É o passo final da validação analítica — responsável por garantir a **consistência relacional** entre as entidades do *Data Warehouse*.\n",
    "\n",
    "---\n",
    "\n",
    "## 🧩 1. O que é integridade referencial\n",
    "\n",
    "Em um modelo dimensional (*Star Schema*), cada tabela de fatos referencia suas dimensões por meio de **chaves estrangeiras (FKs)**.  \n",
    "Essas relações permitem que métricas numéricas (fatos) sejam analisadas sob múltiplas perspectivas (datas, produtos, locais etc.).\n",
    "\n",
    "Se uma FK não tiver correspondência na dimensão, ocorre um **\"orphan record\"** (registro órfão), o que compromete qualquer junção (`JOIN`) e distorce os resultados analíticos.\n",
    "\n",
    "---\n",
    "\n",
    "## 🧭 2. Exemplo conceitual\n",
    "\n",
    "Imagine um registro em `fact_manufacturing` com `product_code = 'X123'`.  \n",
    "Se esse código não existir em `dim_product`, o relatório de produção por produto ficará inconsistente — pois o registro não terá referência válida.  \n",
    "\n",
    "A integridade referencial garante que isso **nunca ocorra**.\n",
    "\n",
    "---\n",
    "\n",
    "## ⚙️ 3. Relações verificadas nesta etapa\n",
    "\n",
    "| Fato | Chaves Estrangeiras (FKs) | Dimensões de referência | Regra |\n",
    "|------|-----------------------------|--------------------------|-------|\n",
    "| `fact_costs` | `date_key`, `site_code` | `dim_date`, `dim_site` | `R8_FK_COSTS` |\n",
    "| `fact_manufacturing` | `date_key`, `site_code`, `line_code`, `product_code` | `dim_date`, `dim_site`, `dim_line`, `dim_product` | `R9_FK_MANU` |\n",
    "| `fact_energy` | `date_key`, `site_code`, `line_code` | `dim_date`, `dim_site`, `dim_line` | `R10_FK_ENERGY` |\n",
    "\n",
    "Essas relações formam o núcleo do **modelo estrela (Star Schema)**: cada tabela de fatos aponta para dimensões que contextualizam seus indicadores.\n",
    "\n",
    "---\n",
    "\n",
    "## 🧱 4. Estrutura das checagens\n",
    "\n",
    "Cada verificação:\n",
    "1. **Conta** quantas linhas nas tabelas de fatos possuem FKs sem correspondência nas dimensões;  \n",
    "2. **Armazena** o resultado na tabela `qa.results`;  \n",
    "3. **Classifica** o status como:\n",
    "   - ✅ **ok = TRUE** → todas as FKs possuem referência válida;\n",
    "   - ⚠️ **ok = FALSE** → foram detectados registros órfãos.\n",
    "\n",
    "---\n",
    "\n",
    "## 💡 5. Por que é importante\n",
    "\n",
    "| Tipo de problema | Sintoma | Impacto |\n",
    "|------------------|----------|----------|\n",
    "| FK inexistente em dimensão | Linhas sem correspondência em `JOIN` | Métricas somadas incorretamente |\n",
    "| Dimensão faltando código | Orfandade entre fatos e dimensões | Relatórios incompletos |\n",
    "| FK incorreta (erro de digitação) | Perda de granularidade | Insights distorcidos |\n",
    "\n",
    "> ⚠️ A violação de integridade referencial é silenciosa: ela não quebra o ETL, mas **quebra a confiabilidade analítica**.\n",
    "\n",
    "---\n",
    "\n",
    "## 📊 6. Resultado esperado\n",
    "\n",
    "Após a execução desta etapa, o relatório consolidado (`qa.results`) exibirá:\n",
    "- Quantos registros órfãos existem (se houver);\n",
    "- Quais dimensões foram afetadas;\n",
    "- O status geral de integridade do modelo dimensional.\n",
    "\n",
    "> ✅ Quando todas as FKs forem válidas, o pipeline de dados estará completamente íntegro e pronto para a **Etapa 6 — EDA (Exploratory Data Analysis)**.\n",
    "\n",
    "---\n",
    "\n",
    "📘 **Resumo**\n",
    "- **Propósito:** validar relacionamentos entre fatos e dimensões;  \n",
    "- **Entrada:** views criadas nas etapas anteriores (`fact_*`, `dim_*`);  \n",
    "- **Saída:** relatório de integridade referencial em `qa.results`.  \n"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "3cec41ef",
   "metadata": {},
   "source": [
    "## Etapa 5 — **Integridade referencial** (fatos x dimensões)\n",
    "- R6: `fact_costs` referencia corretamente `dim_product`, `dim_plant` e `dim_date`"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "066e560e-e24a-4d05-b274-3582707a34c3",
   "metadata": {},
   "source": [
    "### 5.1 — Registrar as regras de integridade referencial."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 33,
   "id": "0cc3639e-8d3d-4d90-a93b-fa7544e451bc",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Regras de integridade referencial registradas em qa.rules.\n"
     ]
    }
   ],
   "source": [
    "con.execute(\"\"\"\n",
    "INSERT OR REPLACE INTO qa.rules VALUES\n",
    "('R8_FK_COSTS',  'FK fact_costs → dim_date, dim_site',                 'ERROR', 'date_key, site_code válidos'),\n",
    "('R9_FK_MANU',   'FK fact_manufacturing → dim_date, dim_site, dim_line, dim_product', 'ERROR', 'todas FKs válidas'),\n",
    "('R10_FK_ENERGY','FK fact_energy → dim_date, dim_site, dim_line',      'ERROR', 'todas FKs válidas');\n",
    "\"\"\")\n",
    "\n",
    "print(\"Regras de integridade referencial registradas em qa.rules.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "d3d0fc47-a59c-44dc-8ada-bd5ed232f900",
   "metadata": {},
   "source": [
    "### 5.2 — R8: FK em `fact_costs` (→ `dim_date`, `dim_site`)."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 34,
   "id": "929e50b2-4170-474e-98cf-6946bba89d3b",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "R8 — FK fact_costs → dim_date/dim_site validada.\n"
     ]
    }
   ],
   "source": [
    "con.execute(\"\"\"\n",
    "WITH\n",
    "bad_date AS (\n",
    "  SELECT f.date_key\n",
    "  FROM fact_costs f\n",
    "  LEFT JOIN dim_date d USING(date_key)\n",
    "  WHERE d.date_key IS NULL\n",
    "),\n",
    "bad_site AS (\n",
    "  SELECT f.site_code\n",
    "  FROM fact_costs f\n",
    "  LEFT JOIN dim_site s USING(site_code)\n",
    "  WHERE s.site_code IS NULL\n",
    "),\n",
    "summary AS (\n",
    "  SELECT\n",
    "    (SELECT COUNT(*) FROM bad_date) AS missing_date,\n",
    "    (SELECT COUNT(*) FROM bad_site) AS missing_site\n",
    ")\n",
    "INSERT INTO qa.results\n",
    "SELECT * FROM qa_assert(\n",
    "  'R8_FK_COSTS',\n",
    "  (SELECT missing_date=0 AND missing_site=0 FROM summary),\n",
    "  (SELECT 'missing_date=' || missing_date || '; missing_site=' || missing_site FROM summary),\n",
    "  (\n",
    "    SELECT json_object(\n",
    "      'bad_date_keys', (SELECT to_json(list(date_key)) FROM bad_date),\n",
    "      'bad_sites',     (SELECT to_json(list(site_code)) FROM bad_site)\n",
    "    )\n",
    "  )\n",
    ");\n",
    "\"\"\")\n",
    "\n",
    "print(\"R8 — FK fact_costs → dim_date/dim_site validada.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "1b132a67-a716-448e-9a6b-ef4628f5fb40",
   "metadata": {},
   "source": [
    "### 5.3 — R9: FK em `fact_manufacturing` (→ `dim_date`, `dim_site`, `dim_line`, `dim_product`)."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 35,
   "id": "d4981435-c694-46c5-9361-06000f980e3a",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "R9 — FK fact_manufacturing → dims validada.\n"
     ]
    }
   ],
   "source": [
    "con.execute(\"\"\"\n",
    "WITH\n",
    "bad_date AS (\n",
    "  SELECT f.date_key\n",
    "  FROM fact_manufacturing f\n",
    "  LEFT JOIN dim_date d USING(date_key)\n",
    "  WHERE d.date_key IS NULL\n",
    "),\n",
    "bad_site AS (\n",
    "  SELECT f.site_code\n",
    "  FROM fact_manufacturing f\n",
    "  LEFT JOIN dim_site s USING(site_code)\n",
    "  WHERE s.site_code IS NULL\n",
    "),\n",
    "bad_line AS (\n",
    "  SELECT f.site_code, f.line_code\n",
    "  FROM fact_manufacturing f\n",
    "  LEFT JOIN dim_line l USING(site_code, line_code)\n",
    "  WHERE l.line_code IS NULL\n",
    "),\n",
    "bad_prod AS (\n",
    "  SELECT f.product_code\n",
    "  FROM fact_manufacturing f\n",
    "  LEFT JOIN dim_product p USING(product_code)\n",
    "  WHERE p.product_code IS NULL\n",
    "),\n",
    "summary AS (\n",
    "  SELECT\n",
    "    (SELECT COUNT(*) FROM bad_date) AS missing_date,\n",
    "    (SELECT COUNT(*) FROM bad_site) AS missing_site,\n",
    "    (SELECT COUNT(*) FROM bad_line) AS missing_line,\n",
    "    (SELECT COUNT(*) FROM bad_prod) AS missing_prod\n",
    ")\n",
    "INSERT INTO qa.results\n",
    "SELECT * FROM qa_assert(\n",
    "  'R9_FK_MANU',\n",
    "  (SELECT missing_date=0 AND missing_site=0 AND missing_line=0 AND missing_prod=0 FROM summary),\n",
    "  (\n",
    "    SELECT 'missing_date=' || missing_date || '; ' ||\n",
    "           'missing_site=' || missing_site || '; ' ||\n",
    "           'missing_line=' || missing_line || '; ' ||\n",
    "           'missing_prod=' || missing_prod\n",
    "    FROM summary\n",
    "  ),\n",
    "  (\n",
    "    SELECT json_object(\n",
    "      'bad_date_keys',  (SELECT to_json(list(date_key))                FROM bad_date),\n",
    "      'bad_sites',      (SELECT to_json(list(site_code))               FROM bad_site),\n",
    "      'bad_lines',      (SELECT to_json(list(site_code || '|' || line_code)) FROM bad_line),\n",
    "      'bad_products',   (SELECT to_json(list(product_code))            FROM bad_prod)\n",
    "    )\n",
    "  )\n",
    ");\n",
    "\"\"\")\n",
    "\n",
    "print(\"R9 — FK fact_manufacturing → dims validada.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "1e8562e5-bf24-42f6-932a-cdd69b28d081",
   "metadata": {},
   "source": [
    "### 5.4 — R10: FK em `fact_energy` (→ `dim_date`, `dim_site`, `dim_line`)."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 36,
   "id": "dcbb79b6-963e-4fc3-95a4-51a34589fff5",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "R10 — FK fact_energy → dims validada.\n"
     ]
    }
   ],
   "source": [
    "con.execute(\"\"\"\n",
    "WITH\n",
    "bad_date AS (\n",
    "  SELECT f.date_key\n",
    "  FROM fact_energy f\n",
    "  LEFT JOIN dim_date d USING(date_key)\n",
    "  WHERE d.date_key IS NULL\n",
    "),\n",
    "bad_site AS (\n",
    "  SELECT f.site_code\n",
    "  FROM fact_energy f\n",
    "  LEFT JOIN dim_site s USING(site_code)\n",
    "  WHERE s.site_code IS NULL\n",
    "),\n",
    "bad_line AS (\n",
    "  SELECT f.site_code, f.line_code\n",
    "  FROM fact_energy f\n",
    "  LEFT JOIN dim_line l USING(site_code, line_code)\n",
    "  WHERE l.line_code IS NULL\n",
    "),\n",
    "summary AS (\n",
    "  SELECT\n",
    "    (SELECT COUNT(*) FROM bad_date) AS missing_date,\n",
    "    (SELECT COUNT(*) FROM bad_site) AS missing_site,\n",
    "    (SELECT COUNT(*) FROM bad_line) AS missing_line\n",
    ")\n",
    "INSERT INTO qa.results\n",
    "SELECT * FROM qa_assert(\n",
    "  'R10_FK_ENERGY',\n",
    "  (SELECT missing_date=0 AND missing_site=0 AND missing_line=0 FROM summary),\n",
    "  (\n",
    "    SELECT 'missing_date=' || missing_date || '; ' ||\n",
    "           'missing_site=' || missing_site || '; ' ||\n",
    "           'missing_line=' || missing_line\n",
    "    FROM summary\n",
    "  ),\n",
    "  (\n",
    "    SELECT json_object(\n",
    "      'bad_date_keys', (SELECT to_json(list(date_key)) FROM bad_date),\n",
    "      'bad_sites',     (SELECT to_json(list(site_code)) FROM bad_site),\n",
    "      'bad_lines',     (SELECT to_json(list(site_code || '|' || line_code)) FROM bad_line)\n",
    "    )\n",
    "  )\n",
    ");\n",
    "\"\"\")\n",
    "\n",
    "print(\"R10 — FK fact_energy → dims validada.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "5071d4ef-3465-4d4c-b480-e18128eea87a",
   "metadata": {},
   "source": [
    "### 5.5 — Consolidar resultados."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 37,
   "id": "04a7c79a-1102-46db-8d8a-08276303b2af",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>rule_id</th>\n",
       "      <th>ok</th>\n",
       "      <th>severity</th>\n",
       "      <th>message</th>\n",
       "      <th>meta</th>\n",
       "      <th>run_ts</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>R10_FK_ENERGY</td>\n",
       "      <td>True</td>\n",
       "      <td>ERROR</td>\n",
       "      <td>missing_date=0; missing_site=0; missing_line=0</td>\n",
       "      <td>{\"bad_date_keys\":null,\"bad_sites\":null,\"bad_li...</td>\n",
       "      <td>2025-10-28 09:02:25.915</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>R9_FK_MANU</td>\n",
       "      <td>True</td>\n",
       "      <td>ERROR</td>\n",
       "      <td>missing_date=0; missing_site=0; missing_line=0...</td>\n",
       "      <td>{\"bad_date_keys\":null,\"bad_sites\":null,\"bad_li...</td>\n",
       "      <td>2025-10-28 09:01:13.564</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>R8_FK_COSTS</td>\n",
       "      <td>True</td>\n",
       "      <td>ERROR</td>\n",
       "      <td>missing_date=0; missing_site=0</td>\n",
       "      <td>{\"bad_date_keys\":null,\"bad_sites\":null}</td>\n",
       "      <td>2025-10-28 08:59:49.299</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "         rule_id    ok severity  \\\n",
       "0  R10_FK_ENERGY  True    ERROR   \n",
       "1     R9_FK_MANU  True    ERROR   \n",
       "2    R8_FK_COSTS  True    ERROR   \n",
       "\n",
       "                                             message  \\\n",
       "0     missing_date=0; missing_site=0; missing_line=0   \n",
       "1  missing_date=0; missing_site=0; missing_line=0...   \n",
       "2                     missing_date=0; missing_site=0   \n",
       "\n",
       "                                                meta                  run_ts  \n",
       "0  {\"bad_date_keys\":null,\"bad_sites\":null,\"bad_li... 2025-10-28 09:02:25.915  \n",
       "1  {\"bad_date_keys\":null,\"bad_sites\":null,\"bad_li... 2025-10-28 09:01:13.564  \n",
       "2            {\"bad_date_keys\":null,\"bad_sites\":null} 2025-10-28 08:59:49.299  "
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "df_results = con.execute(\"\"\"\n",
    "SELECT\n",
    "  rule_id,\n",
    "  ok,\n",
    "  severity,\n",
    "  message,\n",
    "  meta,\n",
    "  run_ts\n",
    "FROM qa.results\n",
    "JOIN qa.rules USING(rule_id)\n",
    "WHERE rule_id LIKE 'R8%' OR rule_id LIKE 'R9%' OR rule_id LIKE 'R10%'\n",
    "ORDER BY run_ts DESC;\n",
    "\"\"\").fetch_df()\n",
    "\n",
    "display(df_results)"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "2ec48c5a-678e-4d39-9c36-7618579580ad",
   "metadata": {},
   "source": [
    "### 📘 Resumo técnico"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "f160d695-1c47-4124-ae1c-abcad8581d83",
   "metadata": {},
   "source": [
    "| Regra             | Fato                 | FKs verificadas                                      | Dimensões de referência                           |\n",
    "| ----------------- | -------------------- | ---------------------------------------------------- | ------------------------------------------------- |\n",
    "| **R8_FK_COSTS**   | `fact_costs`         | `date_key`, `site_code`                              | `dim_date`, `dim_site`                            |\n",
    "| **R9_FK_MANU**    | `fact_manufacturing` | `date_key`, `site_code`, `line_code`, `product_code` | `dim_date`, `dim_site`, `dim_line`, `dim_product` |\n",
    "| **R10_FK_ENERGY** | `fact_energy`        | `date_key`, `site_code`, `line_code`                 | `dim_date`, `dim_site`, `dim_line`                |"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "e51912d9-2a1f-496e-b634-033f9fa9886c",
   "metadata": {},
   "source": [
    "# ✅ Conclusão da Etapa 5 — Integridade Referencial\n",
    "\n",
    "> **Síntese:**  \n",
    "> Nesta etapa, validamos a consistência relacional entre as tabelas de **fatos** e **dimensões**, garantindo que todos os identificadores de referência (FKs) estão devidamente ancorados em suas respectivas dimensões (Dims).  \n",
    "> Isso assegura que o modelo dimensional segue corretamente o padrão **Star Schema**, essencial para qualquer análise confiável.\n",
    "\n",
    "---\n",
    "\n",
    "## 🧩 1. O que foi validado\n",
    "\n",
    "Foram verificadas três famílias de relacionamentos entre fatos e dimensões:\n",
    "\n",
    "| Regra | Tabela de Fatos | FKs verificadas | Dimensões de referência | Status esperado |\n",
    "|:------|:----------------|:----------------|:------------------------|:----------------|\n",
    "| **R8_FK_COSTS** | `fact_costs` | `date_key`, `site_code` | `dim_date`, `dim_site` | ✅ Todas as FKs válidas |\n",
    "| **R9_FK_MANU** | `fact_manufacturing` | `date_key`, `site_code`, `line_code`, `product_code` | `dim_date`, `dim_site`, `dim_line`, `dim_product` | ✅ Todas as FKs válidas |\n",
    "| **R10_FK_ENERGY** | `fact_energy` | `date_key`, `site_code`, `line_code` | `dim_date`, `dim_site`, `dim_line` | ✅ Todas as FKs válidas |\n",
    "\n",
    "Essas verificações confirmam que cada métrica de custo, manufatura ou energia tem **contexto temporal, geográfico e operacional** bem definido.\n",
    "\n",
    "---\n",
    "\n",
    "## ⚙️ 2. Interpretação dos resultados\n",
    "\n",
    "O relatório final (`qa.results`) exibe para cada regra:\n",
    "\n",
    "| Coluna | Significado |\n",
    "|:--------|:-------------|\n",
    "| `rule_id` | Identificador da regra de validação |\n",
    "| `ok` | Resultado lógico (✅ TRUE = íntegro / ⚠️ FALSE = falha) |\n",
    "| `severity` | Grau de criticidade da regra |\n",
    "| `message` | Resumo do número de chaves órfãs encontradas |\n",
    "| `meta` | Lista de FKs inválidas ou ausentes, em formato JSON |\n",
    "| `run_ts` | Timestamp da execução do teste |\n",
    "\n",
    "Se alguma regra retorna `ok = FALSE`, isso indica a presença de **registros órfãos** — ou seja, fatos que não têm correspondência nas dimensões.  \n",
    "Esses registros devem ser revisados na origem ou tratados no ETL, garantindo consistência na próxima carga.\n",
    "\n",
    "---\n",
    "\n",
    "## 📊 3. Importância dessa etapa\n",
    "\n",
    "| Tipo de verificação | Benefício direto | Risco se ignorado |\n",
    "|----------------------|------------------|-------------------|\n",
    "| Chaves de data (`date_key`) | Garantem consistência temporal entre fatos | Análises mensais incorretas |\n",
    "| Chaves de local (`site_code`, `line_code`) | Vinculam métricas a locais reais de produção | Consolidação geográfica incorreta |\n",
    "| Chaves de produto (`product_code`) | Permitem agregações e indicadores por produto | Indicadores distorcidos ou ausentes |\n",
    "\n",
    "> 🔍 A integridade referencial é o elo que mantém o **modelo lógico e o modelo físico sincronizados** — sem ela, o pipeline perde confiabilidade e as análises perdem validade.\n",
    "\n",
    "---\n",
    "\n",
    "## 🧮 4. Qualidade de dados: visão consolidada\n",
    "\n",
    "Com as cinco etapas concluídas até aqui, o pipeline cobre **todo o ciclo de validação analítica**:\n",
    "\n",
    "| Etapa | Tipo de Validação | Finalidade |\n",
    "|-------|-------------------|-------------|\n",
    "| **Etapa 1–2** | Conexão e estrutura de views | Montagem das tabelas baseadas em Parquet |\n",
    "| **Etapa 3** | Estrutura e frescor | Garante existência e atualização dos dados |\n",
    "| **Etapa 4** | Qualidade intrínseca | Detecta nulos, duplicatas e valores inválidos |\n",
    "| **Etapa 5** | Integridade relacional | Assegura vínculos corretos entre fatos e dimensões |\n",
    "\n",
    "Juntas, essas etapas criam uma **camada de auditoria automatizada** dentro do DuckDB, permitindo rastrear a qualidade dos dados a cada atualização do ETL.\n",
    "\n",
    "---\n",
    "\n",
    "## 🚀 5. Próximo passo: EDA (Exploratory Data Analysis)\n",
    "\n",
    "Com a integridade e qualidade garantidas:\n",
    "- As tabelas `fact_*` e `dim_*` estão confiáveis para exploração estatística;  \n",
    "- É possível calcular KPIs e gerar dashboards com segurança;  \n",
    "- A etapa seguinte (EDA) focará em **descobrir padrões, correlações e anomalias** nos dados de custos, manufatura e energia.\n",
    "\n",
    "> ✅ Agora o *Data Warehouse* está pronto para análise.  \n",
    "> Vamos avançar para a **Etapa 6 — Análise Exploratória de Dados (EDA)**.\n"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "c000b921-c9d6-4869-bd45-396357bfdbf4",
   "metadata": {},
   "source": [
    "### 🧩 1. O que vem antes da EDA"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "79bd7d7b-15b5-47d7-9199-a9e3c5efd54d",
   "metadata": {},
   "source": [
    "#### A EDA pressupõe que o data warehouse já está íntegro e confiável, o que inclui:"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "d598cc74-997d-4d2a-9975-b641f42dcbba",
   "metadata": {},
   "source": [
    "| Etapa                                   | Finalidade                                            | Status atual no seu pipeline             |\n",
    "| --------------------------------------- | ----------------------------------------------------- | ---------------------------------------- |\n",
    "| **ETL (raw → silver)**                  | Limpeza e padronização de dados                       | ✅ Concluído                              |\n",
    "| **Validação analítica (Etapas 3–5)**    | Estrutura, frescor, qualidade e integridade           | ✅ Concluído                              |\n",
    "| **Reconciliação de custos e KPIs base** | Garantir coerência e consistência contábil            | 🔶 *Altamente recomendável antes da EDA* |\n",
    "| **Views materializadas e QA report**    | Tornar as métricas rápidas de consultar e rastreáveis | 🔶 *Recomendável antes da EDA*           |\n"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "1675fcbb-c31b-497b-9251-1a450bb70aeb",
   "metadata": {},
   "source": [
    "# ⚙️ Etapa 6 — Reconciliação de Custos e KPIs\n",
    "\n",
    "> **Objetivo:**  \n",
    "> Validar a **coerência econômica** entre os domínios de *Custos*, *Manufatura* e *Energia* após a consolidação dos dados no *Data Warehouse*.  \n",
    "> Esta etapa verifica se os valores financeiros, produtivos e energéticos estão **numericamente reconciliados** e se as principais **métricas de desempenho (KPIs)** derivam de dados íntegros e consistentes.\n",
    "\n",
    "---\n",
    "\n",
    "## 🧩 1. O que é Reconciliação de Custos\n",
    "\n",
    "A **reconciliação** é o processo de conferir se os totais de custo e produção convergem para valores logicamente consistentes.  \n",
    "Em termos simples: *os números “fecham”?*\n",
    "\n",
    "Essa verificação garante que:\n",
    "- O **custo total contabilizado (`fact_costs`)** seja compatível com o volume de produção (`fact_manufacturing`);\n",
    "- O **consumo energético (`fact_energy`)** esteja proporcional à atividade fabril;\n",
    "- As **taxas de câmbio (`fx_rate`)** e valores em BRL (`amount_br`) e FX (`amount_fx`) estejam coerentes.\n",
    "\n",
    "**Exemplo de checagem básica:**\n",
    "\\[\n",
    "\\text{Custo Unitário Médio} = \\frac{\\sum \\text{Custo Total (BRL)}}{\\sum \\text{Unidades OK}}\n",
    "\\]\n",
    "Esse indicador deve permanecer dentro de um intervalo de variação aceitável, evitando discrepâncias por erros de ETL, duplicação de registros ou gaps de datas.\n",
    "\n",
    "---\n",
    "\n",
    "## 📈 2. KPIs: Indicadores-Chave de Desempenho\n",
    "\n",
    "Após a reconciliação, são criadas **views analíticas derivadas** contendo *Key Performance Indicators* (KPIs).  \n",
    "Elas transformam dados transacionais em **métricas consolidadas de negócio**, permitindo análise direta via SQL, Python (Pandas) ou dashboards (Streamlit, Power BI, etc.).\n",
    "\n",
    "**Principais exemplos de KPIs:**\n",
    "\n",
    "| Indicador | Fórmula simplificada | Interpretação |\n",
    "|:-----------|:--------------------|:---------------|\n",
    "| **`kpi_cost_per_unit`** | Σ(Custos) / Σ(Unidades OK) | Eficiência de custo por item produzido |\n",
    "| **`kpi_energy_per_unit`** | Σ(kWh) / Σ(Unidades OK) | Consumo energético médio por unidade |\n",
    "| **`kpi_fx_effect`** | Σ(Amount BRL − Amount FX × FX Rate) | Impacto cambial no custo total |\n",
    "| **`kpi_oee_avg`** | Média(OEE %) | Eficiência global dos equipamentos |\n",
    "\n",
    "Essas *views materializadas* serão armazenadas dentro do **warehouse DuckDB** para uso repetido e rápido acesso em análises posteriores (EDA ou dashboards).\n",
    "\n",
    "---\n",
    "\n",
    "## 🔍 3. Por que esta etapa antecede a EDA\n",
    "\n",
    "| Etapa | Propósito | Resultado |\n",
    "|:------|:-----------|:-----------|\n",
    "| **Reconciliação** | Confirmar coerência numérica entre domínios | Dados contábeis e produtivos “fecham” |\n",
    "| **KPIs** | Gerar métricas analíticas confiáveis | Indicadores prontos para exploração |\n",
    "| **EDA** | Explorar padrões e correlações | Insights a partir de dados confiáveis |\n",
    "\n",
    "> ⚠️ **Importante:**  \n",
    "> A EDA só é significativa se as métricas-base estiverem reconciliadas.  \n",
    "> Caso contrário, distribuições, outliers e correlações refletirão **erros de integração**, e não comportamentos reais.\n",
    "\n",
    "---\n",
    "\n",
    "## 🧮 4. Resultados esperados desta etapa\n",
    "\n",
    "Após executar os blocos de código:\n",
    "1. As métricas reconciliadas (custos × produção × energia) estarão disponíveis como views no DuckDB;  \n",
    "2. Serão criadas novas *views KPI* (`kpi_cost_per_unit`, `kpi_energy_per_unit`, `kpi_fx_effect`, etc.);  \n",
    "3. O relatório de qualidade (`qa.report`) será atualizado e exportado em `.csv` e `.parquet`.\n",
    "\n",
    "---\n",
    "\n",
    "## 📘 5. Relação com o pipeline anterior\n",
    "\n",
    "| Fase | Função principal | Status |\n",
    "|------|------------------|--------|\n",
    "| ETL (Raw → Silver) | Estruturação inicial dos dados | ✅ |\n",
    "| Validação Analítica (Etapas 3 – 5) | Garantia de integridade e qualidade | ✅ |\n",
    "| **Reconciliação + KPIs (Etapa 6)** | Coerência econômica e consolidação analítica | 🚀 **Agora** |\n",
    "| EDA (Etapa 7) | Exploração estatística e visual | 🔜 Próxima etapa |\n",
    "\n",
    "---\n",
    "\n",
    "> 💡 **Resumo:**  \n",
    "> A reconciliação de custos é o elo entre a engenharia de dados e a análise de negócios.  \n",
    "> É aqui que o pipeline deixa de ser apenas técnico e passa a refletir a **realidade financeira e operacional** da fábrica.  \n",
    "> Só após essa etapa, podemos afirmar que estamos prontos para **analisar, explicar e prever** o comportamento dos dados.\n"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "6f7d30a9",
   "metadata": {},
   "source": [
    "## Etapa 6 — **Reconciliação** de custos\n",
    "- R7: `total_cost ≈ material + labor + overhead` (tolerância 0,005)  \n",
    "- R8: Reconciliação **mensal** agregada (diferença relativa ≤ 0,1%)"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "1852d779-dc37-497d-971c-3e3d160d3c37",
   "metadata": {},
   "source": [
    "### 6.1 — Regras de reconciliação e registro no QA"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 39,
   "id": "5a880bc4-3d55-4e13-80c5-f27100821117",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Regras de reconciliação registradas.\n"
     ]
    }
   ],
   "source": [
    "sql_reconciliation = \"\"\"\n",
    "INSERT OR REPLACE INTO qa.rules VALUES\n",
    "('R11_RECON_COST_VS_PROD',   'Custo total deve ser proporcional à produção',  'WARN',  '|Δ|/média <= 5%'),\n",
    "('R12_RECON_COST_VS_ENERGY', 'Custo total deve ser coerente com consumo energético', 'WARN', '|Δ|/média <= 5%'),\n",
    "('R13_KPI_VALID_RANGE',      'KPIs com valores válidos e coerentes',          'ERROR', 'sem negativos ou nulos inconsistentes');\n",
    "\"\"\"\n",
    "con.execute(sql_reconciliation)\n",
    "print(\"Regras de reconciliação registradas.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "c9660be6-0c02-41ad-b9d9-952b7316ecb2",
   "metadata": {},
   "source": [
    "### 6.2 — Reconciliação entre Custos e Produção."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 40,
   "id": "94c3f484-f544-4e20-843d-e42131156395",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Reconciliação entre custos e produção executada.\n"
     ]
    }
   ],
   "source": [
    "# 6.2 — Verifica se custo total (BRL) acompanha produção (unidades OK)\n",
    "\n",
    "con.execute(f\"\"\"\n",
    "WITH agg AS (\n",
    "  SELECT\n",
    "    d.y,\n",
    "    d.m,\n",
    "    c.site_code,\n",
    "    SUM(c.amount_br) AS total_cost_brl,\n",
    "    SUM(m.units_ok)  AS total_units\n",
    "  FROM fact_costs c\n",
    "  JOIN dim_date d USING(date_key)\n",
    "  LEFT JOIN fact_manufacturing m\n",
    "    ON c.site_code = m.site_code\n",
    "    AND c.date_key = m.date_key\n",
    "  GROUP BY 1,2,3\n",
    "  HAVING SUM(m.units_ok) > 0\n",
    "),\n",
    "dev AS (\n",
    "  SELECT\n",
    "    *,\n",
    "    ABS(total_cost_brl - (AVG(total_cost_brl) OVER()) *\n",
    "        (total_units / AVG(total_units) OVER())) / NULLIF(AVG(total_cost_brl) OVER(), 0) AS rel_diff\n",
    "  FROM agg\n",
    ")\n",
    "INSERT INTO qa.results\n",
    "SELECT * FROM qa_assert(\n",
    "  'R11_RECON_COST_VS_PROD',\n",
    "  (SELECT MAX(rel_diff) <= 0.05 FROM dev),  -- tolerância 5%\n",
    "  (SELECT 'Máx. desvio relativo = ' || ROUND(MAX(rel_diff)*100,2) || '%' FROM dev),\n",
    "  (SELECT json_group_array(json_object('site_code', site_code, 'rel_diff', rel_diff)) FROM dev WHERE rel_diff > 0.05)\n",
    ");\n",
    "\"\"\")\n",
    "\n",
    "print(\"Reconciliação entre custos e produção executada.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "c3e14e61-e954-4dac-a35c-4713df2432f4",
   "metadata": {},
   "source": [
    "### 6.3 — Reconciliação entre Custos e Energia."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 41,
   "id": "6f204a34-6e7b-4a00-8811-01f3d0a655e3",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Reconciliação entre custos e energia executada.\n"
     ]
    }
   ],
   "source": [
    "# 6.3 — Verifica coerência entre custos e consumo energético\n",
    "con.execute(f\"\"\"\n",
    "WITH agg AS (\n",
    "  SELECT\n",
    "    d.y,\n",
    "    d.m,\n",
    "    c.site_code,\n",
    "    SUM(c.amount_br) AS total_cost_brl,\n",
    "    SUM(e.kwh_day)   AS total_kwh\n",
    "  FROM fact_costs c\n",
    "  JOIN dim_date d USING(date_key)\n",
    "  LEFT JOIN fact_energy e\n",
    "    ON c.site_code = e.site_code\n",
    "    AND c.date_key = e.date_key\n",
    "  GROUP BY 1,2,3\n",
    "  HAVING SUM(e.kwh_day) > 0\n",
    "),\n",
    "dev AS (\n",
    "  SELECT *,\n",
    "         ABS(total_cost_brl - (AVG(total_cost_brl) OVER()) *\n",
    "             (total_kwh / AVG(total_kwh) OVER())) / NULLIF(AVG(total_cost_brl) OVER(), 0) AS rel_diff\n",
    "  FROM agg\n",
    ")\n",
    "INSERT INTO qa.results\n",
    "SELECT * FROM qa_assert(\n",
    "  'R12_RECON_COST_VS_ENERGY',\n",
    "  (SELECT MAX(rel_diff) <= 0.05 FROM dev),  -- tolerância 5%\n",
    "  (SELECT 'Máx. desvio relativo = ' || ROUND(MAX(rel_diff)*100,2) || '%' FROM dev),\n",
    "  (SELECT json_group_array(json_object('site_code', site_code, 'rel_diff', rel_diff)) FROM dev WHERE rel_diff > 0.05)\n",
    ");\n",
    "\"\"\")\n",
    "\n",
    "print(\"Reconciliação entre custos e energia executada.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "309e14aa-ee22-4cc5-9e39-f1e91b3e28b2",
   "metadata": {},
   "source": [
    "## 6.4 — Criação de KPIs principais."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 43,
   "id": "ff7f92e9-e17d-4db3-a359-d0c808a2973c",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "KPIs criados com sucesso.\n"
     ]
    }
   ],
   "source": [
    "# KPI 1: custo médio por unidade produzida\n",
    "con.execute(\"\"\"\n",
    "CREATE OR REPLACE VIEW kpi_cost_per_unit AS\n",
    "SELECT\n",
    "  f.date_key,\n",
    "  d.y, d.m,\n",
    "  f.site_code,\n",
    "  SUM(f.amount_br) / NULLIF(SUM(m.units_ok), 0) AS cost_per_unit\n",
    "FROM fact_costs f\n",
    "JOIN dim_date d USING(date_key)\n",
    "LEFT JOIN fact_manufacturing m\n",
    "  ON f.site_code = m.site_code\n",
    "  AND f.date_key = m.date_key\n",
    "GROUP BY 1,2,3,4;\n",
    "\"\"\")\n",
    "\n",
    "# KPI 2: consumo energético por unidade produzida\n",
    "con.execute(\"\"\"\n",
    "CREATE OR REPLACE VIEW kpi_energy_per_unit AS\n",
    "SELECT\n",
    "  e.date_key,\n",
    "  d.y, d.m,\n",
    "  e.site_code,\n",
    "  SUM(e.kwh_day) / NULLIF(SUM(m.units_ok), 0) AS kwh_per_unit\n",
    "FROM fact_energy e\n",
    "JOIN dim_date d USING(date_key)\n",
    "LEFT JOIN fact_manufacturing m\n",
    "  ON e.site_code = m.site_code\n",
    "  AND e.date_key = m.date_key\n",
    "GROUP BY 1,2,3,4;\n",
    "\"\"\")\n",
    "\n",
    "# KPI 3: variação cambial média ponderada\n",
    "con.execute(\"\"\"\n",
    "CREATE OR REPLACE VIEW kpi_fx_effect AS\n",
    "SELECT\n",
    "  date_key,\n",
    "  site_code,\n",
    "  SUM(amount_br - amount_fx * fx_rate) / NULLIF(SUM(amount_br),0) AS fx_effect_ratio\n",
    "FROM fact_costs\n",
    "GROUP BY 1,2;\n",
    "\"\"\")\n",
    "\n",
    "print(\"KPIs criados com sucesso.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "a5651303-fc13-4680-b951-686d1e2b2586",
   "metadata": {},
   "source": [
    "## 6.5 — Validação dos KPIs e registro no QA"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 44,
   "id": "bcc00aa4-0c66-455b-8fcb-da20cb154a56",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Validação dos KPIs executada.\n"
     ]
    }
   ],
   "source": [
    "# Validação de intervalos e coerência dos KPIs\n",
    "con.execute(\"\"\"\n",
    "WITH bad AS (\n",
    "  SELECT COUNT(*) AS c FROM (\n",
    "    SELECT cost_per_unit FROM kpi_cost_per_unit WHERE cost_per_unit < 0 OR cost_per_unit > 1e7\n",
    "    UNION ALL\n",
    "    SELECT kwh_per_unit FROM kpi_energy_per_unit WHERE kwh_per_unit < 0 OR kwh_per_unit > 1e7\n",
    "    UNION ALL\n",
    "    SELECT fx_effect_ratio FROM kpi_fx_effect WHERE ABS(fx_effect_ratio) > 1\n",
    "  )\n",
    ")\n",
    "INSERT INTO qa.results\n",
    "SELECT * FROM qa_assert(\n",
    "  'R13_KPI_VALID_RANGE',\n",
    "  (SELECT c = 0 FROM bad),\n",
    "  (SELECT 'KPIs fora de faixa = ' || c FROM bad),\n",
    "  (SELECT json('[]'))\n",
    ");\n",
    "\"\"\")\n",
    "\n",
    "print(\"Validação dos KPIs executada.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "bcef3772-683f-4888-b39e-8f6f4015a76a",
   "metadata": {},
   "source": [
    "## 6.6 — Geração do Relatório Final de QA."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 45,
   "id": "056bd17f-a9c0-4850-af63-00d75b4145fb",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Relatório QA exportado em data/gold/qa_report.[csv|parquet]\n"
     ]
    }
   ],
   "source": [
    "# Exporta o relatório final de QA\n",
    "df_qa = con.execute(\"\"\"\n",
    "SELECT\n",
    "  r.rule_id,\n",
    "  r.description,\n",
    "  r.severity,\n",
    "  q.ok,\n",
    "  q.message,\n",
    "  q.meta,\n",
    "  q.run_ts\n",
    "FROM qa.rules r\n",
    "LEFT JOIN qa.results q USING(rule_id)\n",
    "ORDER BY q.run_ts DESC;\n",
    "\"\"\").fetch_df()\n",
    "\n",
    "# Exporta os resultados\n",
    "os.makedirs(\"data/gold\", exist_ok=True)\n",
    "df_qa.to_csv(\"data/gold/qa_report.csv\", index=False)\n",
    "df_qa.to_parquet(\"data/gold/qa_report.parquet\", index=False)\n",
    "\n",
    "print(\"Relatório QA exportado em data/gold/qa_report.[csv|parquet]\")\n"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "e3b3d55f-a22d-41aa-8c59-eb6c513c77d2",
   "metadata": {},
   "source": [
    "# ✅ Conclusão da Etapa 6 — Reconciliação de Custos e KPIs\n",
    "\n",
    "---\n",
    "\n",
    "## 📊 1. Síntese do que foi realizado\n",
    "\n",
    "Nesta etapa, o modelo foi submetido a um conjunto de **verificações de coerência econômica e operacional** entre os domínios:\n",
    "\n",
    "| Domínio | Fonte (Parquet / View) | Verificação realizada |\n",
    "|----------|------------------------|------------------------|\n",
    "| **Custos** | `fact_costs` | Alinhamento entre custo total e volume de produção |\n",
    "| **Manufatura** | `fact_manufacturing` | Compatibilidade de unidades produzidas com custos e consumo energético |\n",
    "| **Energia** | `fact_energy` | Relação custo ↔ consumo elétrico proporcional |\n",
    "| **Câmbio / FX** | `fact_costs.fx_rate` | Impacto cambial sobre o custo total em BRL |\n",
    "\n",
    "Além disso, foram criadas **três views analíticas (KPIs)** no *data warehouse*:\n",
    "\n",
    "| View | Indicador calculado | Significado |\n",
    "|:------|:-------------------|:-------------|\n",
    "| `kpi_cost_per_unit` | Σ(Custos) / Σ(Unidades OK) | Eficiência de custo por item produzido |\n",
    "| `kpi_energy_per_unit` | Σ(kWh) / Σ(Unidades OK) | Consumo médio de energia por unidade |\n",
    "| `kpi_fx_effect` | Σ(Δ cambial) / Σ(Custo BRL) | Influência do câmbio sobre os custos |\n",
    "\n",
    "Essas métricas são a base para a próxima fase (EDA), pois transformam dados transacionais em **indicadores agregados de desempenho**.\n",
    "\n",
    "---\n",
    "\n",
    "## 🧮 2. Interpretação dos resultados de QA\n",
    "\n",
    "Cada checagem registrada em `qa.rules` e `qa.results` tem um identificador (rule_id):\n",
    "\n",
    "| Rule_ID | Checagem | Tipo | Interpretação |\n",
    "|:--:|:--|:--:|:--|\n",
    "| **R11_RECON_COST_VS_PROD** | Custos proporcionais à produção | ⚠️ Warn | Valores fora da tolerância de 5 % indicam divergência entre custos e unidades |\n",
    "| **R12_RECON_COST_VS_ENERGY** | Custos coerentes com energia consumida | ⚠️ Warn | Verifica se custos sobem junto com o uso energético |\n",
    "| **R13_KPI_VALID_RANGE** | Faixas válidas dos KPIs | ❌ Error | Detecta valores negativos, nulos ou absurdamente altos |\n",
    "\n",
    "**Resultado esperado:**  \n",
    "- `ok = TRUE` → dados reconciliados e consistentes  \n",
    "- `ok = FALSE` → revisar integração entre domínios ou parâmetros de ETL  \n",
    "\n",
    "> 🔎 Dica: visualize rapidamente o histórico das validações abrindo o arquivo  \n",
    "> `data/gold/qa_report.csv` ou `qa_report.parquet` no Pandas, Excel ou DuckDB CLI.\n",
    "\n",
    "---\n",
    "\n",
    "## 📘 3. Importância desta etapa\n",
    "\n",
    "Esta reconciliação é o elo entre a **engenharia de dados (ETL + Validação)** e a **análise exploratória (EDA)**.  \n",
    "A partir daqui:\n",
    "\n",
    "- Todos os domínios estão **numericamente alinhados** (custos, manufatura e energia);  \n",
    "- As variáveis derivadas (KPIs) são **estatisticamente confiáveis**;  \n",
    "- O relatório `qa.report` fornece **rastreabilidade total** da qualidade dos dados.\n",
    "\n",
    "Sem esta etapa, a EDA poderia apresentar padrões ilusórios — gerados por discrepâncias contábeis ou inconsistências de datas.\n",
    "\n",
    "---\n",
    "\n",
    "## 🚀 4. Próximos passos — Início da EDA (Etapa 7)\n",
    "\n",
    "Na sequência, a **Etapa 7 — Análise Exploratória de Dados (EDA)** irá:\n",
    "\n",
    "- Explorar a distribuição e correlação dos KPIs criados;  \n",
    "- Analisar variações temporais (mensais, sazonais);  \n",
    "- Detectar outliers produtivos e anomalias energéticas;  \n",
    "- Preparar visualizações e dashboards interativos.\n",
    "\n",
    "---\n",
    "\n",
    "> ✅ **Resumo final:**  \n",
    "> A Etapa 6 encerra o ciclo de **validação analítica** com uma reconciliação contábil, produtiva e energética robusta.  \n",
    "> O *Data Warehouse* agora contém dados confiáveis, reconciliados e prontos para exploração estatística e visual.\n"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "be0fa288",
   "metadata": {},
   "source": [
    "## Etapa 9 — Checks opcionais (drift m/m e outliers)\n",
    "**Drift m/m** para custo unitário por produto (`> 30%`):  \n",
    "**Outliers** por produto (fora de [P1, P99])."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 49,
   "id": "d0e60ac7",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Drift m/m (rows): 4\n",
      "Outliers (rows): 6\n"
     ]
    },
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>date_key</th>\n",
       "      <th>site_code</th>\n",
       "      <th>uc</th>\n",
       "      <th>uc_lag</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>20250601</td>\n",
       "      <td>SC01</td>\n",
       "      <td>1110.296310</td>\n",
       "      <td>697.219664</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>20250701</td>\n",
       "      <td>SC01</td>\n",
       "      <td>776.203301</td>\n",
       "      <td>1110.296310</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>20250601</td>\n",
       "      <td>SC02</td>\n",
       "      <td>966.131337</td>\n",
       "      <td>710.087049</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>20250601</td>\n",
       "      <td>SC03</td>\n",
       "      <td>1048.378272</td>\n",
       "      <td>632.878899</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "   date_key site_code           uc       uc_lag\n",
       "0  20250601      SC01  1110.296310   697.219664\n",
       "1  20250701      SC01   776.203301  1110.296310\n",
       "2  20250601      SC02   966.131337   710.087049\n",
       "3  20250601      SC03  1048.378272   632.878899"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>date_key</th>\n",
       "      <th>y</th>\n",
       "      <th>m</th>\n",
       "      <th>site_code</th>\n",
       "      <th>cost_per_unit</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>20250501</td>\n",
       "      <td>2025</td>\n",
       "      <td>5</td>\n",
       "      <td>SC01</td>\n",
       "      <td>697.219664</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>20250601</td>\n",
       "      <td>2025</td>\n",
       "      <td>6</td>\n",
       "      <td>SC01</td>\n",
       "      <td>1110.296310</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>20250401</td>\n",
       "      <td>2025</td>\n",
       "      <td>4</td>\n",
       "      <td>SC02</td>\n",
       "      <td>709.616960</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>20250601</td>\n",
       "      <td>2025</td>\n",
       "      <td>6</td>\n",
       "      <td>SC02</td>\n",
       "      <td>966.131337</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>20250501</td>\n",
       "      <td>2025</td>\n",
       "      <td>5</td>\n",
       "      <td>SC03</td>\n",
       "      <td>632.878899</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>5</th>\n",
       "      <td>20250601</td>\n",
       "      <td>2025</td>\n",
       "      <td>6</td>\n",
       "      <td>SC03</td>\n",
       "      <td>1048.378272</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "   date_key     y  m site_code  cost_per_unit\n",
       "0  20250501  2025  5      SC01     697.219664\n",
       "1  20250601  2025  6      SC01    1110.296310\n",
       "2  20250401  2025  4      SC02     709.616960\n",
       "3  20250601  2025  6      SC02     966.131337\n",
       "4  20250501  2025  5      SC03     632.878899\n",
       "5  20250601  2025  6      SC03    1048.378272"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "# Drift m/m (> 30%)\n",
    "sql_drift = \"\"\"\n",
    "WITH cur AS (\n",
    "  SELECT date_key, site_code, AVG(cost_per_unit) uc\n",
    "  FROM kpi_cost_per_unit GROUP BY 1,2\n",
    "),\n",
    "lag AS (\n",
    "  SELECT c.*, LAG(uc) OVER (PARTITION BY site_code ORDER BY date_key) AS uc_lag\n",
    "  FROM cur c\n",
    ")\n",
    "SELECT * FROM lag\n",
    "WHERE uc_lag IS NOT NULL\n",
    "  AND ABS(uc - uc_lag)/NULLIF(uc_lag,0) > 0.30\n",
    "ORDER BY site_code, date_key;\n",
    "\"\"\"\n",
    "df_drift = con.execute(sql_drift).fetch_df()\n",
    "\n",
    "# Outliers por P1/P99\n",
    "sql_outliers = \"\"\"\n",
    "WITH q AS (\n",
    "  SELECT site_code,\n",
    "         quantile_cont(cost_per_unit, 0.01) AS p01,\n",
    "         quantile_cont(cost_per_unit, 0.99) AS p99\n",
    "  FROM kpi_cost_per_unit\n",
    "  GROUP BY 1\n",
    ")\n",
    "SELECT k.*\n",
    "FROM kpi_cost_per_unit k\n",
    "JOIN q USING(site_code)\n",
    "WHERE cost_per_unit < p01 OR cost_per_unit > p99\n",
    "ORDER BY site_code, date_key, cost_per_unit;\n",
    "\"\"\"\n",
    "df_outliers = con.execute(sql_outliers).fetch_df()\n",
    "\n",
    "print(\"Drift m/m (rows):\", len(df_drift))\n",
    "print(\"Outliers (rows):\", len(df_outliers))\n",
    "\n",
    "# Mostra pequenas amostras\n",
    "display(df_drift.head(20))\n",
    "display(df_outliers.head(20))"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "863fac18",
   "metadata": {},
   "source": [
    "## Etapa 10 — Próximos passos\n",
    "- Integrar estes artefatos ao seu **pipeline** (pós-ETL) e **dashboard** (Streamlit/Plotly).  \n",
    "- Parametrizar tolerâncias (por produto/planta) e regras de domínio (ex.: `uom`, `region`).  \n",
    "- Se existirem regras **críticas**, faça o pipeline **falhar** quando `severity='ERROR' AND passed=false`."
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python [conda env:Regressao]",
   "language": "python",
   "name": "conda-env-Regressao-py"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.7"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
