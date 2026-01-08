"""
Página de visualização dos dados tratados (camada trusted) de estoque de peças.
"""

import os
import streamlit as st
import polars as pl

from pathlib import Path
from datetime import date


# region ----- Página Config -----
st.set_page_config(
    page_title="Estoque de Peças",
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
    / "estoque-pecas"
)

parquet_file = os.listdir(CAMADA_TRUSTED_PATH)[0]


def read_parquet() -> pl.DataFrame:
    return pl.read_parquet(source=CAMADA_TRUSTED_PATH / parquet_file)


# endregion


# region ----- Introdução -----
st.title("Estoque de Peças")
st.write(
    """
        Nesta página são apresentados os dados tratados de estoque de peças.
        
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
st.subheader("Quantidade de Peças por Unidade")
st.write(
    """
        A quantidade de peças por unidade é uma medida importante para
        entender o volume de entrada de peças em um determinado período
        em cada unidade.
    """
)

df_unidade_quantidade_veiculos = (
    df.unique()
    .group_by("nome_da_filial")
    .agg(
        pl.col("categoria_da_peca")
        .count()
        .cast(pl.Int64)
        .alias("quantidade_total_pecas")
    )
    .sort("quantidade_total_pecas")
)

st.bar_chart(
    df_unidade_quantidade_veiculos,
    x="nome_da_filial",
    x_label="",
    y="quantidade_total_pecas",
    y_label="",
)

st.subheader("Peças por Categoria")
st.write(
    """
        Neste quadro demonstramos o volume financeiro em estoque
        de peças por categoria.
    """
)

df_marca_quantidade_veiculos = (
    df.unique()
    .group_by("categoria_da_peca_padronizada")
    .agg(pl.col("valor_da_peca_em_estoque").sum().cast(pl.Float64).alias("valor"))
    .sort("valor")
)

st.bar_chart(
    df_marca_quantidade_veiculos,
    x="categoria_da_peca_padronizada",
    x_label="",
    y="valor",
    y_label="",
    horizontal=True,
)

st.subheader("Obsolescência no Estoque")
st.write(
    """
        Neste gráfico a idéia é apresentar o valor em cada categoria de
        obsolescência no estoque, assim é possível entender quais categorias
        são mais difíceis de dar saída.
    """
)

df_categoria_obsolescencia = (
    df.unique()
    .group_by("classificacao_obsolescencia")
    .agg(pl.col("valor_da_peca_em_estoque").sum().cast(pl.Float64).alias("valor"))
    .sort("valor")
)

st.bar_chart(
    df_categoria_obsolescencia,
    x="classificacao_obsolescencia",
    x_label="",
    y="valor",
    y_label="",
    horizontal=True,
)


st.divider()
