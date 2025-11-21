import streamlit as st
import pandas as pd
import numpy as np
import joblib
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt

from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# SHAP é opcional – tentamos importar
try:
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False

# ======= CONFIG GERAL =======
st.set_page_config(
    page_title="EmpresaX — Predição de Custos",
    page_icon="📈",
    layout="wide"
)

st.title("📈 EmpresaX — Dashboard de Predição de Custos (MVP+)")


# ======================================================================
# =========================== PARÂMETROS ================================
# ======================================================================

DEFAULT_MODEL_NAME = "model_RF_cost_per_unit_imp_20251113_122506.joblib"
DEFAULT_MODEL_DIR  = Path("/home/rusch/Área de trabalho/Projeto_Whirpool/Data_Science_Projects/eda/models")
DEFAULT_MODEL_PATH = (DEFAULT_MODEL_DIR / DEFAULT_MODEL_NAME).as_posix()

TARGET_DEFAULT = "cost_per_unit_imp"  # alvo padrão


# ======================================================================
# ===================== FUNÇÕES AUXILIARES =============================
# ======================================================================

def robust_scale(series: pd.Series) -> pd.Series:
    """Robust Z-score: (x - mediana) / (1.4826 * MAD)."""
    med = series.median()
    mad = np.median(np.abs(series - med))
    if mad == 0:
        mad = series.std() if series.std() > 0 else 1.0
    return (series - med) / (1.4826 * mad)


def add_robust_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gera colunas *_robust de forma estatisticamente robusta,
    se as colunas base existirem.
    """
    df = df.copy()
    if "kwh_per_unit_imp" in df.columns:
        df["kwh_per_unit_imp_robust"] = robust_scale(df["kwh_per_unit_imp"])
    if "cost_per_unit_imp" in df.columns:
        df["cost_per_unit_imp_robust"] = robust_scale(df["cost_per_unit_imp"])
    if "fx_effect_ratio_imp" in df.columns:
        df["fx_effect_ratio_imp_robust"] = robust_scale(df["fx_effect_ratio_imp"])
    return df


def add_robust_if_missing_simple(df: pd.DataFrame) -> pd.DataFrame:
    """
    Versão simples: se *_robust não existir, cria como cópia da coluna base.
    Útil como fallback para compatibilidade com o modelo.
    """
    df = df.copy()
    mapping = {
        "kwh_per_unit_imp_robust": "kwh_per_unit_imp",
        "cost_per_unit_imp_robust": "cost_per_unit_imp",
        "fx_effect_ratio_imp_robust": "fx_effect_ratio_imp",
    }
    for new_col, base_col in mapping.items():
        if new_col not in df.columns and base_col in df.columns:
            df[new_col] = df[base_col]
    return df


def align_with_model_features(df: pd.DataFrame, model, target: str | None):
    """
    - Garante colunas robustas
    - Separa y_true (se alvo existir)
    - Converte tipos básicos
    - Reordena colunas segundo feature_names_in_ do modelo
    """
    df = add_robust_features(df)          # robusto "bonito"
    df = add_robust_if_missing_simple(df) # garante presença, se faltar

    y_true = None
    if target and target in df.columns:
        y_true = df[target].copy()
        X = df.drop(columns=[target])
    else:
        X = df.copy()

    # Conversões de tipos comuns
    if "site_code" in X.columns:
        X["site_code"] = X["site_code"].astype(str)
    if "m" in X.columns:
        X["m"] = pd.to_numeric(X["m"], errors="coerce").astype("Int64")
    if "y" in X.columns:
        X["y"] = pd.to_numeric(X["y"], errors="coerce").astype("Int64")

    # Usa feature_names_in_ como "verdade"
    feat_in = getattr(model, "feature_names_in_", None)
    extra = []
    if isinstance(feat_in, (list, tuple, np.ndarray, pd.Index)):
        feat_in = list(feat_in)

        missing = [c for c in feat_in if c not in X.columns]
        extra = [c for c in X.columns if c not in feat_in]

        # Tratamento especial: se o modelo espera 'date_key' e ela não está no CSV,
        # criamos uma coluna constante (0) só para compatibilizar o deploy.
        if "date_key" in missing:
            # você pode escolher outra convenção aqui, por ex. 0 ou um índice sequencial
            X["date_key"] = 0
            missing = [c for c in missing if c != "date_key"]

        # Se ainda restarem colunas faltando, aí sim damos erro
        if missing:
            raise ValueError(f"Faltam colunas esperadas pelo modelo: {missing}")

        # Reordena X exatamente na ordem de treino
        X = X[feat_in]

    return X, y_true, extra


def compute_regression_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    mae = mean_absolute_error(y_true, y_pred)
    mse = mean_squared_error(y_true, y_pred)
    rmse = np.sqrt(mse)
    mape = np.mean(np.abs((y_true - y_pred) / y_true)) * 100 if np.all(y_true != 0) else np.nan
    r2 = r2_score(y_true, y_pred)
    return {"MAE": mae, "MSE": mse, "RMSE": rmse, "MAPE": mape, "R2": r2}


def get_final_estimator(model):
    """
    Retorna o último estimador do Pipeline (se for um Pipeline),
    senão retorna o próprio modelo.
    """
    # Pipeline com named_steps
    if hasattr(model, "named_steps"):
        return list(model.named_steps.values())[-1]
    # Pipeline com steps (lista de (nome, obj))
    if hasattr(model, "steps"):
        return model.steps[-1][1]
    # Modelo "simples"
    return model


def get_feature_names_from_pipeline(pipeline):
    """
    Extrai nomes das features após o ColumnTransformer/OneHotEncoder/etc.
    """
    if not hasattr(pipeline, "named_steps"):
        return getattr(pipeline, "feature_names_in_", None)

    # procura um ColumnTransformer dentro do pipeline
    ct = None
    for _, step in pipeline.named_steps.items():
        if hasattr(step, "transformers_"):
            ct = step
            break

    if ct is None:
        return getattr(pipeline, "feature_names_in_", None)

    output_features = []
    for name, transformer, cols in ct.transformers_:
        if name == "remainder" and transformer == "drop":
            continue

        if hasattr(transformer, "get_feature_names_out"):
            new_feats = transformer.get_feature_names_out(cols)
            output_features.extend(new_feats)
        else:
            output_features.extend(cols)

    return output_features

def get_preprocess_from_pipeline(pipeline):
    """
    Retorna o ColumnTransformer (ou etapa de preprocessamento) de dentro do Pipeline,
    se existir. Caso contrário, retorna None.
    """
    if not hasattr(pipeline, "named_steps"):
        return None

    for _, step in pipeline.named_steps.items():
        # ColumnTransformer tem atributo transformers_
        if hasattr(step, "transformers_"):
            return step

    return None


@st.cache_resource
def load_model(path: str):
    return joblib.load(path)

# ======================================================================
# =========================== SIDEBAR ==================================
# ======================================================================

st.sidebar.header("Configurações gerais")

model_path = st.sidebar.text_input("Caminho do modelo (.joblib)", value=DEFAULT_MODEL_PATH)
target_name = st.sidebar.text_input("Nome da coluna alvo (TARGET)", value=TARGET_DEFAULT)

uploaded = st.sidebar.file_uploader("Suba um CSV com as features", type=["csv"])
show_preview = st.sidebar.checkbox("Mostrar preview do CSV", value=True)


# ======================================================================
# ===================== CARREGAR MODELO ================================
# ======================================================================

try:
    model = load_model(model_path)
    st.sidebar.success(f"Modelo carregado: {type(model).__name__}")
except Exception as e:
    st.sidebar.error(f"Falha ao carregar modelo: {e}")
    st.stop()


# ======================================================================
# ===================== ABAS PRINCIPAIS ================================
# ======================================================================

tab_upload, tab_predict, tab_graphs, tab_features, tab_config = st.tabs(
    ["📁 Upload & Dados", "📊 Predição & Métricas", "📈 Análise Gráfica", "🧬 Importância das Features", "⚙️ Configuração/Log"]
)

# Container compartilhado entre abas
df_raw = None
X = None
y_true = None
y_pred = None
pred_df = None
extra_cols = []

# ======================================================================
# ========================== ABA 1 — UPLOAD =============================
# ======================================================================

with tab_upload:
    st.subheader("📁 Upload & Exploração de Dados")

    if uploaded is None:
        st.info("Envie um arquivo CSV pela barra lateral para começar.")
    else:
        try:
            uploaded.seek(0)  # <- rebobina o arquivo para o início
            df_raw = pd.read_csv(uploaded)
            st.success(f"Arquivo carregado com sucesso. Formato: {df_raw.shape[0]} linhas × {df_raw.shape[1]} colunas.")

            if show_preview:
                st.markdown("#### Preview das primeiras linhas")
                st.dataframe(df_raw.head())

            st.markdown("#### Informações gerais do DataFrame")
            st.write(df_raw.describe(include="all").transpose())

        except Exception as e:
            st.error(f"Erro ao ler o CSV: {e}")


# ======================================================================
# ===================== PROCESSAMENTO COMUM ============================
# ======================================================================

if uploaded is not None:
    try:
        uploaded.seek(0)  # <- rebobina o arquivo para o início
        df_raw = pd.read_csv(uploaded)

        X, y_true, extra_cols = align_with_model_features(
            df_raw, model, target_name if target_name else None
        )
        y_pred = model.predict(X)
        pred_df = X.copy()
        pred_df["prediction"] = y_pred
        if y_true is not None:
            pred_df["y_true"] = y_true.values

    except Exception as e:
        with tab_predict:
            st.error("Falha ao preparar dados para predição.")
            st.write("Mensagem completa do erro:")
            st.exception(e)

# ======================================================================
# ===================== ABA 2 — PREDIÇÃO & MÉTRICAS ====================
# ======================================================================

with tab_predict:
    st.subheader("📊 Predição & Métricas")

    st.write("DEBUG — uploaded is None?", uploaded is None)
    st.write("DEBUG — pred_df is None?", pred_df is None)

    if uploaded is None or pred_df is None:
        st.info("Suba um CSV e garanta que o alinhamento de features esteja OK na aba anterior.")
    else:
        # Filtros simples
        if "site_code" in pred_df.columns:
            sites = ["(Todos)"] + sorted(pred_df["site_code"].astype(str).unique().tolist())
            site_sel = st.selectbox("Filtrar por site_code", sites)
            df_view = pred_df.copy()
            if site_sel != "(Todos)":
                df_view = df_view[df_view["site_code"].astype(str) == site_sel]
        else:
            df_view = pred_df.copy()

        st.markdown("#### Amostra das predições")
        st.dataframe(df_view.head(50))

        # Métricas se houver y_true
        if "y_true" in df_view.columns:
            metrics = compute_regression_metrics(df_view["y_true"], df_view["prediction"])
            c1, c2, c3, c4, c5 = st.columns(5)

            # MAE
            with c1:
                st.markdown(
                    """
                    <span title="MAE (Mean Absolute Error) é o erro absoluto médio entre o valor real (y_true) e o previsto (prediction), na mesma unidade do alvo. Quanto menor, melhor.">
                    MAE ⓘ
                    </span>
                    """,
                    unsafe_allow_html=True,
                )
                st.metric(label="", value=f"{metrics['MAE']:.3f}")

            # RMSE
            with c2:
                st.markdown(
                    """
                    <span title="RMSE (Root Mean Squared Error) é a raiz quadrada do erro quadrático médio. Penaliza mais fortemente erros grandes e também está na mesma unidade da variável alvo.">
                        RMSE ⓘ
                    </span>
                    """,
                    unsafe_allow_html=True,
                )
                st.metric(label="", value=f"{metrics['RMSE']:.3f}")

            # MAPE
            with c3:
                mape_str = f"{metrics['MAPE']:.2f}" if not np.isnan(metrics["MAPE"]) else "N/A"
                st.markdown(
                    """
                    <span title="MAPE (Mean Absolute Percentage Error) é o erro percentual médio. Indica, em média, quanto % o valor previsto se afasta do valor real. Valores baixos indicam boa performance.">
                        MAPE (%) ⓘ
                    </span>
                    """,
                    unsafe_allow_html=True,
                )
                st.metric(label="", value=mape_str)

            # R²
            with c4:
                st.markdown(
                    """
                    <span title="R² (coeficiente de determinação) mede a proporção da variabilidade dos dados explicada pelo modelo. Valores próximos de 1 indicam excelente capacidade explicativa.">
                        R² ⓘ
                    </span>
                    """,
                    unsafe_allow_html=True,
                )
                st.metric(label="", value=f"{metrics['R2']:.3f}")

            # MSE
            with c5:
                st.markdown(
                    """
                    <span title="MSE (Mean Squared Error) é o erro quadrático médio. Fica em unidades ao quadrado e por isso os valores numéricos tendem a ser grandes. Deve ser interpretado em relação à escala da variável alvo.">
                        MSE ⓘ
                    </span>
                    """,
                    unsafe_allow_html=True,
                )
                st.metric(label="", value=f"{metrics['MSE']:.3f}")

            st.markdown(
                """
                **Interpretação das colunas e métricas**

                - A coluna **`prediction`** representa o valor **previsto** da variável-alvo 
                  (`cost_per_unit_imp` - custo unitário do produto) pelo modelo de Machine Learning, para cada linha do CSV.
                - A coluna **`y_true`** (quando existe no arquivo) é o valor **real observado** no histórico de dados (o `cost_per_unit_imp`
                  calculado por meio da ETL e da EDA).
                - O **resíduo** de cada observação é definido como:

                  > resíduo = y_true - prediction

                - **MAE (Mean Absolute Error)**: erro absoluto médio em **unidades monetárias**.
                  Quanto menor, melhor.
                - **RMSE (Root Mean Squared Error)**: raiz do erro quadrático médio.
                  Penaliza mais fortemente erros grandes. Também está em unidades monetárias.
                - **MSE (Mean Squared Error)**: erro quadrático médio. Fica em unidades ao quadrado
                  e, por isso, costuma ter valor numérico grande — ele deve ser interpretado em relação
                  à escala da variável alvo.
                - **MAPE (%)**: erro percentual médio. Indica, em média, qual o desvio percentual
                  entre o valor real e o previsto.
                - **R² (coeficiente de determinação)**: mede quanta da variabilidade dos dados
                  é explicada pelo modelo. Valores próximos de 1 indicam excelente capacidade
                  explicativa.

                > Observação: como os custos unitários típicos estão na faixa de centenas (por exemplo,
                > 700–1100), um RMSE da ordem de dezenas (ex.: ~20) corresponde a um erro relativo
                > baixo (cerca de 1–3%), o que é considerado um desempenho muito bom.
                """
            )

        else:
            st.info("Nenhuma coluna alvo detectada, métricas numéricas indisponíveis.")

        # Download das predições
        @st.cache_data
        def convert_df_to_csv(df):
            return df.to_csv(index=False).encode("utf-8")

        csv_bytes = convert_df_to_csv(pred_df)
        st.download_button(
            "💾 Baixar predições (CSV)",
            data=csv_bytes,
            file_name="predictions_whirlpool.csv",
            mime="text/csv"
        )


# ======================================================================
# ===================== ABA 3 — ANÁLISE GRÁFICA =======================
# ======================================================================

with tab_graphs:
    st.subheader("📈 Análise Gráfica")

    if uploaded is None or pred_df is None or "y_true" not in pred_df.columns:
        st.info("É necessário ter valores reais (coluna alvo) no CSV para análise gráfica completa.")
    else:
        # ===================== Filtro por site_code =====================
        df_plot = pred_df.copy()

        if "site_code" in df_plot.columns:
            sites = ["(Todos)"] + sorted(df_plot["site_code"].astype(str).unique().tolist())
            site_sel_graphs = st.selectbox(
                "Filtrar gráficos por site_code",
                sites,
                key="site_filter_graphs",
            )
            if site_sel_graphs != "(Todos)":
                df_plot = df_plot[df_plot["site_code"].astype(str) == site_sel_graphs]

        if df_plot.empty:
            st.warning("Nenhuma linha disponível para o site selecionado.")
            st.stop()

        # ===================== Real vs Previsto =====================
        st.markdown(
            """
            <span title="Cada ponto representa uma observação do dataset (ou do site filtrado). 
            Se o modelo estiver bem calibrado, os pontos devem ficar próximos da linha de 45° (linha y = x). 
            Grandes desvios dessa linha indicam previsões com erro maior.">
                <big>Figura 1: Real vs. Previsto ⓘ</big>
            </span>
            """,
            unsafe_allow_html=True,
        )

        y_true = df_plot["y_true"]
        y_pred = df_plot["prediction"]

        min_val = float(min(y_true.min(), y_pred.min()))
        max_val = float(max(y_true.max(), y_pred.max()))

        fig_real_pred = go.Figure()

        fig_real_pred.add_trace(
            go.Scatter(
                x=y_true,
                y=y_pred,
                mode="markers",
                name="Observações",
                opacity=0.5,
                marker=dict(color='cyan', size=6),
                hovertemplate="Real: %{x:.2f}<br>Previsto: %{y:.2f}<extra></extra>",
            )
        )

        fig_real_pred.add_trace(
            go.Scatter(
                x=[min_val, max_val],
                y=[min_val, max_val],
                mode="lines",
                name="Linha y = x",
                line=dict(color='red', dash="dash", width=2),
                hoverinfo="skip",
            )
        )

        fig_real_pred.update_layout(
            xaxis_title="Real",
            yaxis_title="Previsto",
            showlegend=True,
            margin=dict(l=60, r=20, t=20, b=40),
        )

        st.plotly_chart(fig_real_pred, use_container_width=True)

        # ===================== Resíduos =====================
        residuals = y_true - y_pred

        col1, col2 = st.columns(2)

        with col1:
            st.markdown(
                """
                <span title="Mostra a distribuição dos valores de (y_true - prediction) para o conjunto filtrado. 
                Idealmente, os resíduos devem estar centrados em zero, sem assimetria forte e sem caudas muito pesadas.">
                    <big>Figura 2: Histograma dos resíduos ⓘ</big>
                </span>
                """,
                unsafe_allow_html=True,
            )

            fig_hist = px.histogram(
                x=residuals,
                nbins=30,
                labels={"x": "Resíduo (y_true - prediction)", "y": "Frequência"},
            )
            fig_hist.update_layout(
                margin=dict(l=60, r=20, t=20, b=40),
            )
            st.plotly_chart(fig_hist, use_container_width=True)

        with col2:
            st.markdown(
                """
                <span title="Cada ponto mostra o erro (y_true - prediction) em função do valor previsto, 
                para o conjunto filtrado. É desejável que os pontos estejam distribuídos aleatoriamente 
                em torno de zero.">
                    <big>Figura 3: Resíduo vs. Previsto ⓘ</big>
                </span>
                """,
                unsafe_allow_html=True,
            )

            fig_res_vs_pred = px.scatter(
                x=y_pred,
                y=residuals,
                labels={"x": "Previsto", "y": "Resíduo (y_true - prediction)"},
                opacity=0.5,
            )
            fig_res_vs_pred.add_hline(
                y=0, 
                line=dict(color="red", dash="dash", width=2), 
                annotation_text="resíduo = 0"
            )

            fig_res_vs_pred.update_layout(
                margin=dict(l=60, r=20, t=20, b=40),
            )
            st.plotly_chart(fig_res_vs_pred, use_container_width=True)

        # (mantém o texto interpretativo igual — já vale "condicionado ao filtro")


# ======================================================================
# ===================== ABA 4 — FEATURE IMPORTANCE =====================
# ======================================================================

with tab_features:
    st.subheader("🧬 Importância das Features")

    if uploaded is None or pred_df is None:
        st.info("Carregue dados e gere predições para visualizar a importância das features.")
    else:
        # ===================== Filtro por site_code =====================
        df_feat = pred_df.copy()

        if "site_code" in df_feat.columns:
            sites = ["(Todos)"] + sorted(df_feat["site_code"].astype(str).unique().tolist())
            site_sel_feat = st.selectbox(
                "Filtrar explicabilidade por site_code",
                sites,
                key="site_filter_features",
            )
            if site_sel_feat != "(Todos)":
                df_feat = df_feat[df_feat["site_code"].astype(str) == site_sel_feat]

        if df_feat.empty:
            st.warning("Nenhuma linha disponível para o site selecionado.")
            st.stop()

        final_model = get_final_estimator(model)


        # ---------------- Feature importances ----------------
        if hasattr(final_model, "feature_importances_"):
            importances = np.asarray(final_model.feature_importances_)

            feats = get_feature_names_from_pipeline(model)
            if feats is None:
                feats = df_feat.columns.tolist()
            feats = list(feats)

            if len(feats) != len(importances):
                st.error(
                    f"Número de features ({len(feats)}) não corresponde ao número de "
                    f"importâncias ({len(importances)})."
                )
            else:
                imp_df = pd.DataFrame({"feature": feats, "importance": importances})
                imp_df["importance_norm"] = imp_df["importance"] / imp_df["importance"].sum()

                top10 = imp_df.sort_values("importance", ascending=False).head(10)
                top10 = top10.sort_values("importance_norm", ascending=True)

                st.markdown(
                    """
                    #### Importância das Features (RandomForest – Global) ⓘ
                    <span title="Importâncias estimadas pela Random Forest a partir de todo o conjunto de treinamento. 
                    Não dependem do filtro de site_code; representam o efeito médio global das variáveis.">
                        <small>Passe o mouse sobre o título ou sobre as barras para ver os detalhes.</small>
                    </span>
                    """,
                    unsafe_allow_html=True,
                )

                fig_imp = px.bar(
                    top10,
                    x="importance_norm",
                    y="feature",
                    orientation="h",
                    labels={
                        "importance_norm": "Importância normalizada",
                        "feature": "Feature",
                    },
                    hover_data={
                        "importance_norm": ":.2%",
                        "importance": ":.4f",
                    },
                )
                fig_imp.update_layout(
                    margin=dict(l=140, r=40, t=40, b=40),
                    yaxis_title=None,
                )

                st.plotly_chart(fig_imp, use_container_width=True)

        # ---------------- SHAP (explicabilidade) ----------------
        st.markdown("#### SHAP values (explicabilidade local/global por site)")

        if not SHAP_AVAILABLE:
            st.warning("Pacote `shap` não está instalado. Instale com `pip install shap` para ativar.")
        else:
            try:
                # Seleciona as linhas de X que correspondem ao filtro aplicado em df_feat
                idxs = df_feat.index
                X_shap = X.loc[idxs].copy()

                MAX_SHAP_SAMPLES = 500  # pode ajustar esse limite
                if len(X_shap) > MAX_SHAP_SAMPLES:
                    X_shap = X_shap.sample(MAX_SHAP_SAMPLES, random_state=42)

                # 1) Aplica o mesmo preprocessamento do Pipeline (ColumnTransformer)
                preprocess = get_preprocess_from_pipeline(model)
                if preprocess is not None:
                    X_proc = preprocess.transform(X_shap)
                else:
                    X_proc = X_shap.values

                if hasattr(X_proc, "toarray"):
                    X_proc = X_proc.toarray()

                # 2) Nomes das features após preprocess
                feat_names = get_feature_names_from_pipeline(model)
                if feat_names is None:
                    feat_names = [f"f_{i}" for i in range(X_proc.shape[1])]

                # 3) TreeExplainer no estimador final (RandomForest)
                explainer = shap.TreeExplainer(final_model)
                shap_values = explainer.shap_values(X_proc)

                if isinstance(shap_values, list):
                    shap_arr = np.array(shap_values[0])
                else:
                    shap_arr = np.array(shap_values)

                # ================== SHAP GLOBAL (Plotly) ==================
                mean_abs_shap = np.mean(np.abs(shap_arr), axis=0)
                shap_global_df = pd.DataFrame({
                    "feature": feat_names,
                    "mean_abs_shap": mean_abs_shap
                })
                shap_global_df["mean_abs_shap_norm"] = shap_global_df["mean_abs_shap"] / shap_global_df["mean_abs_shap"].sum()

                topN = shap_global_df.sort_values("mean_abs_shap", ascending=False).head(10)
                topN = topN.sort_values("mean_abs_shap_norm", ascending=True)

                st.markdown(
                    """
                    ##### Importância global SHAP (Top 10 – condicionado ao filtro de site) ⓘ
                    <span title="Mostra as 10 variáveis com maior impacto médio absoluto nas predições, segundo os valores SHAP, considerando apenas as observações do subconjunto filtrado (site_code selecionado).">
                        <small>Passe o mouse sobre as barras para detalhes.</small>
                    </span>
                    """,
                    unsafe_allow_html=True,
                )

                fig_shap_global = px.bar(
                    topN,
                    x="mean_abs_shap_norm",
                    y="feature",
                    orientation="h",
                    labels={
                        "mean_abs_shap_norm": "Importância SHAP normalizada",
                        "feature": "Feature",
                    },
                    hover_data={
                        "mean_abs_shap_norm": ":.2%",
                        "mean_abs_shap": ":.4f",
                    },
                )
                fig_shap_global.update_layout(
                    margin=dict(l=140, r=40, t=40, b=40),
                    yaxis_title=None,
                )

                st.plotly_chart(fig_shap_global, use_container_width=True)

                # ================== SHAP LOCAL (uma observação) ==================
                st.markdown("##### Explicabilidade local (uma observação do subconjunto filtrado)")

                n_rows = X_proc.shape[0]
                idx_local = st.slider(
                    "Selecione o índice da observação (após filtro) para análise local",
                    min_value=0,
                    max_value=n_rows - 1,
                    value=0,
                )

                shap_local = shap_arr[idx_local]
                x_local = X_proc[idx_local]

                local_df = pd.DataFrame({
                    "feature": feat_names,
                    "shap_value": shap_local,
                    "abs_shap": np.abs(shap_local),
                    "feature_value": x_local,
                }).sort_values("abs_shap", ascending=True).tail(10)

                fig_shap_local = px.bar(
                    local_df,
                    x="shap_value",
                    y="feature",
                    orientation="h",
                    labels={
                        "shap_value": "Contribuição SHAP (impacto na predição)",
                        "feature": "Feature",
                    },
                    hover_data={
                        "feature_value": True,
                        "abs_shap": ":.4f",
                    },
                )
                fig_shap_local.update_layout(
                    margin=dict(l=180, r=40, t=40, b=40),
                    yaxis_title=None,
                )

                st.plotly_chart(fig_shap_local, use_container_width=True)

                st.markdown(
                    """
                    **Leitura dos gráficos SHAP condicionados ao site**

                    - O gráfico **global SHAP** considera apenas as observações do subconjunto filtrado
                      (por exemplo, um `site_code` específico) e mostra quais variáveis mais
                      contribuem para o comportamento daquele site.
                    - O gráfico **local SHAP** mostra, para uma linha específica desse subconjunto,
                      quais features empurram a predição para cima (valor SHAP positivo) ou para baixo
                      (valor SHAP negativo), e em que magnitude.
                    """
                )

            except Exception as e:
                st.error(f"Não foi possível calcular/plotar SHAP: {e}")



# ======================================================================
# ===================== ABA 5 — CONFIG / LOG ===========================
# ======================================================================

with tab_config:
    st.subheader("⚙️ Configuração e Log")

    st.markdown("#### Caminhos e parâmetros atuais")
    st.write(f"**Modelo:** `{model_path}`")
    st.write(f"**TARGET:** `{target_name}`")
    st.write(f"**DEFAULT_MODEL_PATH:** `{DEFAULT_MODEL_PATH}`")

    if uploaded is not None:
        st.markdown("#### Colunas do CSV enviado")
        st.write(list(df_raw.columns))

        if extra_cols:
            st.markdown("#### Colunas extras ignoradas pelo modelo")
            st.write(extra_cols)
        else:
            st.markdown("#### Nenhuma coluna extra detectada (todas usadas ou esperadas pelo modelo).")
    else:
        st.info("Nenhum CSV carregado ainda.")