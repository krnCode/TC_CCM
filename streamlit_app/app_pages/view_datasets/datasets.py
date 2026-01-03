"""
Script para ler os datasets iniciais e mostrar no app Streamlit.
"""

import streamlit as st
import polars as pl
from pathlib import Path


# region ----- Página Config -----
st.set_page_config(
    page_title="Datasets Iniciais",
    layout="wide",
)
# endregion


# region ----- Datasets Path -----
DATASETS_PATH: Path = Path(__file__).parent.parent.parent.parent / "src" / "datasets"
# endregion


# region ----- Arquivos dos Datasets -----

estoque_pecas: pl.DataFrame = pl.read_csv(
    source=DATASETS_PATH / "estoque-atual-de-pecas.csv"
)

estoque_veiculos: pl.DataFrame = pl.read_csv(
    source=DATASETS_PATH / "estoque-atual-de-veiculos.csv",
    separator=";",
)

historico_servicos: pl.DataFrame = pl.read_csv(
    source=DATASETS_PATH / "historico-de-servicos-realizados.csv"
)

historico_vendas_pecas: pl.DataFrame = pl.read_csv(
    source=DATASETS_PATH / "historico-de-vendas-de-pecas.csv"
)

historico_vendas_veiculos: pl.DataFrame = pl.read_csv(
    source=DATASETS_PATH / "historico-de-vendas-de-veiculos.csv",
    separator=";",
    ignore_errors=True,
    truncate_ragged_lines=True,
)
# endregion


# region ----- App -----
st.title("Datasets Iniciais")
st.divider()

st.subheader("Estoque de Peças")
st.dataframe(estoque_pecas)

st.subheader("Estoque de Veículos")
st.dataframe(estoque_veiculos)

st.subheader("Histórico de Serviços")
st.dataframe(historico_servicos)

st.subheader("Histórico de Vendas de Peças")
st.dataframe(historico_vendas_pecas)

st.subheader("Histórico de Vendas de Veículos")
st.dataframe(historico_vendas_veiculos)

# endregion
