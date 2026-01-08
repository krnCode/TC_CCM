"""
Página de visualização dos dados brutos (camada raw) do historico de venda de peças.
"""

import os
import streamlit as st
import polars as pl

from pathlib import Path


# region ----- Página Config -----
st.set_page_config(
    page_title="Historico de Venda de Peças",
    layout="wide",
)

# endregion


# region ----- Caminho Arquivos -----
CAMADA_RAW_PATH: Path = (
    Path(__file__).parent.parent.parent.parent
    / "src"
    / "etl"
    / "data"
    / "0-raw"
    / "historico-venda-pecas"
)

parquet_file = os.listdir(CAMADA_RAW_PATH)[0]


def read_parquet() -> pl.DataFrame:
    return pl.read_parquet(source=CAMADA_RAW_PATH / parquet_file)


# endregion


# region ----- Introdução -----
st.title("Historico de Venda de Peças")
st.write(
    """
        Nesta página são apresentados os dados brutos do historico de venda de peças.
        
        Os dados são um espelho do dataset bruto, apenas sendo transformados em
        parquet para que a leitura seja mais eficiente e o acesso seja mais
        rápido.

        ---
    """
)

# endregion


# region ----- Tabela -----
st.subheader("Tabela")
df: pl.DataFrame = read_parquet()
st.dataframe(df)
st.divider()
# endregion


# region ----- Observações -----
st.subheader("Observações")

col1, col2, col3, col4 = st.columns(spec=4, gap="small")
with col1:
    st.metric("Total de linhas", df.height, border=True)

    duplicadas = df.is_duplicated().sum()
    st.metric("Linhas duplicadas", duplicadas, border=True)

with col2:
    st.metric("Valores únicos por linha", df.n_unique(), border=True)

with col3:
    st.metric("Total de colunas", df.width, border=True)

with col4:
    st.metric(
        "Tamanho do arquivo (em Parquet)",
        str(round(os.path.getsize(CAMADA_RAW_PATH / parquet_file) / 1024, 2)) + " KB",
        border=True,
    )

st.divider()
# endregion


# region ----- Visualizações -----
st.subheader("Visualizações")

st.write(" #### Cardinalidade por Coluna")
st.write(
    """
        A cardinalidade é a quantidade de valores únicos que uma coluna tem.
        A ideia é que quanto mais a cardinalidade seja maior, mais valores únicos a coluna tem.
        Desta forma, podemos avaliar a existência de chaves primárias. 
    """
)

card = df.select([pl.col(c).n_unique().alias(c) for c in df.columns]).transpose(
    include_header=True, header_name="Coluna", column_names=["Cardinalidade"]
)

st.bar_chart(data=card, x="Coluna", y="Cardinalidade")


st.write(" #### Percentual de Nulos / Vazios por Coluna")
st.write(
    """
        Evidencia o percentual de valores nulos ou vazios em cada coluna. Assim podemos
        avaliar a existência de valores faltantes em dados críticos.
    """
)

nulls = df.select(
    [((pl.col(c).is_null() | (pl.col(c) == ""))).mean().alias(c) for c in df.columns]
).transpose(include_header=True, header_name="Coluna", column_names=["Percentual"])

st.bar_chart(nulls, x="Coluna", y="Percentual")


st.write(" #### Top 10 Valores por Coluna")
st.write(
    """
        Este gráfico apresenta os 10 valores mais comuns em cada coluna, e assim
        conseguimos avaliar a distribuição de dados.
    """
)

coluna_escolhida = st.selectbox("Selecione uma coluna", df.columns)

top10 = df[coluna_escolhida].value_counts().sort("count", descending=True).head(10)

st.bar_chart(top10.to_pandas().set_index(coluna_escolhida))

st.divider()

# endregion
