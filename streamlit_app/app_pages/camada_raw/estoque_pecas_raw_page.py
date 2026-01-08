"""
Página de visualização dos dados brutos (camada raw) de estoque de peças.
"""

import os
import streamlit as st
import polars as pl

from pathlib import Path


# region ----- Página Config -----
st.set_page_config(
    page_title="Estoque de Peças",
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
    / "estoque-pecas"
)

parquet_file = os.listdir(CAMADA_RAW_PATH)[0]


@st.cache_data
def read_parquet() -> pl.DataFrame:
    return pl.read_parquet(source=CAMADA_RAW_PATH / parquet_file)


# endregion


# region ----- Introdução -----
st.title("Estoque de Peças")
st.write(
    """
        Nesta página são apresentados os dados brutos de estoque de peças.
        
        Os dados são um espelho do dataset bruto, apenas sendo transformados em
        parquet para que a leitura seja mais eficiente e o acesso seja mais
        rápido.

        ---
    """
)

# endregion


# region ----- Tabela -----
st.subheader("Tabela - Estoque de Peças")
df: pl.DataFrame = read_parquet()
st.dataframe(df)
st.divider()
# endregion


# region ----- Observações -----
st.subheader("Observações - Estoque de Peças")

col1, col2, col3, col4 = st.columns(spec=4, gap="small")
with col1:
    st.metric("Total de linhas", df.height, border=True)

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
