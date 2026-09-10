"""
Dashboard - Indicadores Econômicos (Selic, Câmbio, IPCA)
----------------------------------------------------------
Consome a camada Gold (e Silver, para o dado mais recente) do pipeline
medalhão via Amazon Athena, usando awswrangler.

Pré-requisitos:
    - Credenciais AWS configuradas localmente (aws configure), de um
      usuário IAM com a política em streamlit-dashboard-user-policy.json
    - pip install -r requirements.txt

Rodar com: streamlit run dashboard_app.py
"""

import boto3
import awswrangler as wr
import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------------------------------------------------------- config
AWS_REGION = "sa-east-1"
GLUE_DATABASE = "medalhao_bcb_db"
ATHENA_S3_STAGING = "s3://joao-medalhao-bcb-dados/athena-query-results/"

SERIES_INFO = {
    "11": {"nome": "Selic diária", "unidade": "% a.a."},
    "1": {"nome": "Câmbio USD/BRL", "unidade": "R$"},
    "433": {"nome": "IPCA mensal", "unidade": "% no mês"},
}

st.set_page_config(page_title="Indicadores Econômicos - BCB", layout="wide")

boto3.setup_default_session(region_name=AWS_REGION)


# ---------------------------------------------------------------- queries (cacheadas)
@st.cache_data(ttl=3600, show_spinner="Consultando o Athena...")
def carregar_gold() -> pd.DataFrame:
    sql = """
        SELECT codigo_serie, ano_mes, valor_medio, valor_minimo,
               valor_maximo, qtd_observacoes, variacao_percentual_mom
        FROM gold
        ORDER BY codigo_serie, ano_mes
    """
    return wr.athena.read_sql_query(
        sql, database=GLUE_DATABASE, s3_output=ATHENA_S3_STAGING,
        ctas_approach=False,
    )


@st.cache_data(ttl=3600, show_spinner="Buscando últimos valores...")
def carregar_ultimo_valor() -> pd.DataFrame:
    sql = """
        WITH ultimo_dia AS (
            SELECT codigo_serie, MAX(data_referencia) AS data_mais_recente
            FROM silver
            GROUP BY codigo_serie
        )
        SELECT s.codigo_serie, s.data_referencia, s.valor
        FROM silver s
        JOIN ultimo_dia u
          ON s.codigo_serie = u.codigo_serie
         AND s.data_referencia = u.data_mais_recente
    """
    return wr.athena.read_sql_query(
        sql, database=GLUE_DATABASE, s3_output=ATHENA_S3_STAGING,
        ctas_approach=False,
    )


@st.cache_data(ttl=3600, show_spinner=False)
def maior_variacao_por_serie(df_gold: pd.DataFrame) -> pd.DataFrame:
    df_valid = df_gold.dropna(subset=["variacao_percentual_mom"]).copy()
    df_valid["variacao_abs"] = df_valid["variacao_percentual_mom"].abs()
    idx = df_valid.groupby("codigo_serie")["variacao_abs"].idxmax()
    return df_valid.loc[idx, ["codigo_serie", "ano_mes", "variacao_percentual_mom"]]


# ---------------------------------------------------------------- carregar dados
st.title("📊 Indicadores Econômicos — Pipeline Medalhão BCB")
st.caption(
    "Dados públicos do Banco Central (SGS), processados via AWS "
    "Lambda + Glue e consultados em tempo real via Athena."
)

try:
    df_gold = carregar_gold()
    df_ultimo = carregar_ultimo_valor()
except Exception as exc:
    st.error(
        "Não foi possível consultar o Athena. Verifique suas credenciais "
        "AWS locais e se as tabelas 'gold' e 'silver' existem no "
        f"database '{GLUE_DATABASE}'.\n\nErro original: {exc}"
    )
    st.stop()

df_gold["codigo_serie"] = df_gold["codigo_serie"].astype(str)
df_ultimo["codigo_serie"] = df_ultimo["codigo_serie"].astype(str)
df_destaques = maior_variacao_por_serie(df_gold)

# ---------------------------------------------------------------- KPIs
st.subheader("Últimos valores conhecidos")
cols = st.columns(len(SERIES_INFO))
for col, (codigo, info) in zip(cols, SERIES_INFO.items()):
    linha = df_ultimo[df_ultimo["codigo_serie"] == codigo]
    if linha.empty:
        col.metric(info["nome"], "sem dado")
        continue
    valor = linha.iloc[0]["valor"]
    data_ref = linha.iloc[0]["data_referencia"]
    destaque = df_destaques[df_destaques["codigo_serie"] == codigo]
    delta_txt = None
    if not destaque.empty:
        var = destaque.iloc[0]["variacao_percentual_mom"]
        mes = destaque.iloc[0]["ano_mes"]
        delta_txt = f"Maior variação: {var:+.2f}% em {mes}"
    col.metric(
        label=f"{info['nome']} ({data_ref})",
        value=f"{valor:.4f} {info['unidade']}",
        delta=delta_txt,
        delta_color="off",
    )

st.divider()

# ---------------------------------------------------------------- seleção de série
st.subheader("Tendência histórica")
codigo_selecionado = st.selectbox(
    "Escolha a série para explorar:",
    options=list(SERIES_INFO.keys()),
    format_func=lambda c: SERIES_INFO[c]["nome"],
)

df_serie = df_gold[df_gold["codigo_serie"] == codigo_selecionado].sort_values("ano_mes")
info_serie = SERIES_INFO[codigo_selecionado]

col_a, col_b = st.columns([2, 1])

with col_a:
    fig = px.line(
        df_serie,
        x="ano_mes",
        y="valor_medio",
        markers=True,
        title=f"Média mensal — {info_serie['nome']}",
        labels={"ano_mes": "Mês", "valor_medio": f"Valor médio ({info_serie['unidade']})"},
    )
    fig.update_traces(line_color="#0b3d5c")
    st.plotly_chart(fig, use_container_width=True)

with col_b:
    fig_var = px.bar(
        df_serie,
        x="ano_mes",
        y="variacao_percentual_mom",
        title="Variação % mês a mês",
        labels={"ano_mes": "Mês", "variacao_percentual_mom": "Variação (%)"},
        color="variacao_percentual_mom",
        color_continuous_scale=["#c0392b", "#dddddd", "#0b6b3a"],
        color_continuous_midpoint=0,
    )
    fig_var.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig_var, use_container_width=True)

st.subheader("Dados mensais detalhados")
st.dataframe(
    df_serie[
        ["ano_mes", "valor_medio", "valor_minimo", "valor_maximo",
         "qtd_observacoes", "variacao_percentual_mom"]
    ].rename(columns={
        "ano_mes": "Mês",
        "valor_medio": "Valor médio",
        "valor_minimo": "Mínimo",
        "valor_maximo": "Máximo",
        "qtd_observacoes": "Nº observações",
        "variacao_percentual_mom": "Variação MoM (%)",
    }),
    use_container_width=True,
    hide_index=True,
)

st.caption(
    "Fonte: API SGS do Banco Central do Brasil. Pipeline: Lambda → S3 "
    "(bronze) → Glue → S3 (silver/gold) → Athena → Streamlit."
)