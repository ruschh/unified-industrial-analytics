import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(page_title='EmpresaX – Custos & QA', layout='wide')
WAREHOUSE = 'data/warehouse/whirlpool.duckdb'

@st.cache_data(ttl=120)
def q(sql: str, params: dict | None = None) -> pd.DataFrame:
    con = duckdb.connect(WAREHOUSE, read_only=True)
    df = con.execute(sql, params or {}).fetch_df()
    con.close()
    # Só cria a coluna de data quando houver date_key
    if 'date_key' in df.columns and 'date' not in df.columns:
        df['date'] = pd.to_datetime(df['date_key'].astype(str), format='%Y%m%d', errors='coerce')
    return df

st.title('📊 EmpresaX — Custos, QA & KPIs')

# ---------------- QA ----------------
st.subheader('✔️ Quality Assurance (QA)')
qa = q("""
SELECT rule_id, description, severity, ok, message, run_ts
FROM qa.results JOIN qa.rules USING(rule_id)
ORDER BY run_ts DESC, severity DESC
""")
c1, c2, c3 = st.columns(3)
with c1: st.metric('Regras executadas', len(qa))
with c2: st.metric('Regras OK', int((qa['ok'] == True).sum()))
with c3: st.metric('Alertas/Erros', int((qa['ok'] == False).sum()))
st.dataframe(qa, use_container_width=True)

st.divider()

# ---------------- KPIs ----------------
st.subheader('📈 KPIs (materializados)')

kpi_cost   = q("SELECT * FROM analytics.kpi_cost_per_unit   ORDER BY date_key, site_code")
kpi_energy = q("SELECT * FROM analytics.kpi_energy_per_unit ORDER BY date_key, site_code")
kpi_fx     = q("SELECT * FROM analytics.kpi_fx_effect       ORDER BY date_key, site_code")

all_sites = sorted(set(kpi_cost['site_code']).union(kpi_energy['site_code']).union(kpi_fx['site_code']))
site = st.selectbox('Filtrar por site', all_sites)

tab1, tab2, tab3 = st.tabs(['Cost per Unit', 'kWh per Unit', 'FX Effect'])

with tab1:
    df = kpi_cost[kpi_cost['site_code'] == site].copy()
    fig = px.line(df, x='date', y='cost_per_unit', title=f'Cost per Unit — {site}', markers=True)
    fig.update_layout(xaxis_title='Data', yaxis_title='cost_per_unit', template='plotly_dark')
    fig.update_xaxes(dtick="M1", tickformat="%b %Y")
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df[['date_key','site_code','cost_per_unit']].tail(20), use_container_width=True)

with tab2:
    df = kpi_energy[kpi_energy['site_code'] == site].copy()
    fig = px.line(df, x='date', y='kwh_per_unit', title=f'kWh per Unit — {site}', markers=True)
    fig.update_layout(xaxis_title='Data', yaxis_title='kwh_per_unit', template='plotly_dark')
    fig.update_xaxes(dtick="M1", tickformat="%b %Y")
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df[['date_key','site_code','kwh_per_unit']].tail(20), use_container_width=True)

with tab3:
    df = kpi_fx[kpi_fx['site_code'] == site].copy()
    fig = px.line(df, x='date', y='fx_effect_ratio', title=f'FX Effect — {site}', markers=True)
    fig.update_layout(xaxis_title='Data', yaxis_title='fx_effect_ratio', template='plotly_dark')
    fig.update_xaxes(dtick="M1", tickformat="%b %Y")
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df[['date_key','site_code','fx_effect_ratio']].tail(20), use_container_width=True)

st.caption("Fonte: DuckDB warehouse & QA tables • Atualize os parquets em data/silver e reexecute o 'make validate'.")
