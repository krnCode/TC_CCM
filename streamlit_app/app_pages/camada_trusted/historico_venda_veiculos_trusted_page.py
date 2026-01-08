"""
Página de visualização dos dados tratados (camada trusted) do histórico de venda de veículos.
"""

import os
import streamlit as st
import polars as pl

from pathlib import Path
from datetime import date


# region ----- Página Config -----
st.set_page_config(
    page_title="Histórico de Venda de Veículos",
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
    / "historico-venda-veiculos"
)

parquet_file = os.listdir(CAMADA_TRUSTED_PATH)[0]


def read_parquet() -> pl.DataFrame:
    return pl.read_parquet(source=CAMADA_TRUSTED_PATH / parquet_file)


# endregion


# region ----- Introdução -----
st.title("Histórico de Venda de Veículos")
st.write(
    """
        Nesta página são apresentados os dados tratados do histórico de venda de veículos.
        
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
st.subheader("Visualizações")

st.write(" #### Total de Registros Confiáveis e Não Confiáveis")
st.write(
    """
        Para este dataset, identificamos diversos itens que não são confiáveis 
        para serem considerados em KPIs ou métricas de negócio. O principal motivo
        de registros não confiáveis foi por conta de informações duplicadas, porém
        por com algumas diferenças que geram dúvidas se são realmente duplicidade 
        de valores, erro de inputs, lançamentos de dados incorretos, etc.

        Neste gráfico, apresentamos a comparação destes registros de forma visual.
    """
)

df_total_registros = df.group_by("confiabilidade_do_registro").agg(
    pl.col("marca_do_veiculo").count().alias("total_registros_confiaveis")
)

st.bar_chart(
    df_total_registros,
    x="confiabilidade_do_registro",
    x_label="",
    y="total_registros_confiaveis",
    y_label="",
    horizontal=True,
)

st.subheader("Média de Tempo em Estoque por Marca")
st.write(
    """
        Nesta visualização, apresentamos o tempo médio de cada marca em estoque.
        Estamos considerando o período em estoque ajustado, mas ressaltamos que
        devido a grande quantidade de registros não confiáveis, os períodos
        podem ter grande divergência do período em estoque real. 
    """
)

df_vendas_normais = df.filter(pl.col("tipo_de_venda_do_veiculo") == "NORMAL")

# Agrupa por marca e classificação de tempo no estoque
df_estoque_marcas = df_vendas_normais.group_by("marca_do_veiculo").agg(
    pl.mean("dias_que_o_carro_ficou_no_estoque_ajustado").alias(
        "media_de_dias_em_estoque"
    )
)

st.bar_chart(
    df_estoque_marcas,
    x="marca_do_veiculo",
    x_label="",
    y="media_de_dias_em_estoque",
    y_label="",
    horizontal=True,
)
