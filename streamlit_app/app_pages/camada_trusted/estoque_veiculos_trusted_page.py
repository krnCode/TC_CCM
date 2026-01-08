"""
Página de visualização dos dados tratados (camada trusted) de estoque de veículos.
"""

import os
import streamlit as st
import polars as pl

from pathlib import Path
from datetime import date


# region ----- Página Config -----
st.set_page_config(
    page_title="Estoque de Veículos",
    layout="wide",
)

# endregion


# region ----- Caminho Arquivos -----
CAMADA_TRUSTED_PATH: Path = (
    Path(__file__).parent.parent.parent.parent
    / "src"
    / "etl"
    / "data"
    / "1-trusted"
    / "estoque-veiculos"
)

parquet_file = os.listdir(CAMADA_TRUSTED_PATH)[0]


def read_parquet() -> pl.DataFrame:
    return pl.read_parquet(source=CAMADA_TRUSTED_PATH / parquet_file)


# endregion


# region ----- Introdução -----
st.title("Estoque de Veículos")
st.write(
    """
        Nesta página são apresentados os dados tratados de estoque de veiculos.
        
        Aqui os dados já passaram por uma série de transformações e podem já
        ser apresentados de forma mais clara e intuitiva.

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

# from datetime import datetime


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
        str(round(os.path.getsize(CAMADA_TRUSTED_PATH / parquet_file) / 1024, 2))
        + " KB",
        border=True,
    )

st.divider()
# endregion


# region ----- Visualizações -----
st.subheader("Quantidade de Veículos por Unidade")
st.write(
    """
        A quantidade de veículos por unidade é uma medida importante para
        entender o volume de entrada de veículos em um determinado período
        em cada unidade.
    """
)

df_unidade_quantidade_veiculos = (
    df.unique()
    .group_by("nome_da_filial")
    .agg(
        pl.col("marca_do_veiculo")
        .count()
        .cast(pl.Int64)
        .alias("quantidade_total_veiculos")
    )
    .sort("quantidade_total_veiculos")
)

st.bar_chart(
    df_unidade_quantidade_veiculos,
    x="nome_da_filial",
    x_label="",
    y="quantidade_total_veiculos",
    y_label="",
)

st.subheader("Quantidade de Veículos por Marca")
st.write(
    """
        A quantidade de veículos por marca nos mostra a concentração
        do estoque por marca de veículo.
    """
)

df_marca_quantidade_veiculos = (
    df.unique()
    .group_by("marca_do_veiculo")
    .agg(
        pl.col("marca_do_veiculo")
        .count()
        .cast(pl.Int64)
        .alias("quantidade_total_veiculos")
    )
    .sort("quantidade_total_veiculos")
)

st.bar_chart(
    df_marca_quantidade_veiculos,
    x="marca_do_veiculo",
    x_label="",
    y="quantidade_total_veiculos",
    y_label="",
    horizontal=True,
)

st.subheader("Tempo no Estoque")
st.write(
    """
        Neste gráfico a idéia é apresentar a quantidade de veículos no estoque
        pela sua classificação e marca, assim é possível entender quais marcas são
        mais difíceis de dar saída.
    """
)

if "marca_selecionada" not in st.session_state:
    st.session_state.marca_selecionada = "Todas as Marcas"

marca_unica = ["Todas as Marcas"] + sorted(df["marca_do_veiculo"].unique().to_list())

if st.session_state.marca_selecionada not in marca_unica:
    st.session_state.marca_selecionada = "Todas as Marcas"

marca_idx = marca_unica.index(st.session_state.marca_selecionada)
marca_selecionada = st.selectbox(
    "Selecione uma Marca para Filtrar",
    options=marca_unica,
    index=(
        marca_unica.index(st.session_state.marca_selecionada)
        if st.session_state.marca_selecionada in marca_unica
        else 0
    ),
)

st.session_state.marca_selecionada = marca_selecionada

if marca_selecionada != "Todas as Marcas":
    df_filtrado = df.filter(pl.col("marca_do_veiculo") == marca_selecionada)
else:
    df_filtrado = df

df_tempo_veiculos = (
    df_filtrado.unique()
    .group_by("classificacao_tempo_no_estoque")
    .agg(
        pl.col("marca_do_veiculo")
        .count()
        .cast(pl.Int64)
        .alias("quantidade_total_veiculos")
    )
    .sort("classificacao_tempo_no_estoque")
)

st.bar_chart(
    df_tempo_veiculos,
    x="classificacao_tempo_no_estoque",
    x_label="",
    y="quantidade_total_veiculos",
    y_label="",
    horizontal=True,
)

st.divider()
