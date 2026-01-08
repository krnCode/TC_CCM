"""
Página de visualização dos dados tratados (camada trusted) do historico de veículos..
"""

import os
import streamlit as st
import polars as pl

from pathlib import Path
from datetime import date


# region ----- Página Config -----
st.set_page_config(
    page_title="Historico de Serviços",
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
    / "historico-servicos"
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
st.subheader("Visualizações")

st.write(" #### Valor de Serviços Realizados por Unidade")
st.write(
    """
        Neste gráfico conseguimos avaliar qual unidade possui maior valor
        de serviços realizados. Para os cálculos, desconsideramos o valor
        informado em revisões gratuitas.
    """
)

df_unidade_valor_servicos = (
    df.unique()
    .group_by("nome_da_filial")
    .agg(
        pl.col("valor_do_servico_ajustado_com_revisao_gratuita")
        .sum()
        .alias("valor_total_servicos")
    )
    .sort("valor_total_servicos")
)


st.bar_chart(
    df_unidade_valor_servicos,
    x="nome_da_filial",
    x_label="N",
    y="valor_total_servicos",
    y_label="",
)


st.write(" #### Valor de Serviços por Tipo de Serviço")
st.write(
    """
        Nesta visualização, segregamos o valor de serviços pelo tipo de
        serviço, de forma a identificar os serviços que mais tem entrada financeira.
    """
)

df_tipo_servico_valor_servicos = (
    df.unique()
    .group_by("categoria_do_servico_padronizada")
    .agg(
        pl.col("valor_do_servico_ajustado_com_revisao_gratuita")
        .sum()
        .alias("valor_total_servicos")
    )
    .sort("valor_total_servicos")
)

st.bar_chart(
    df_tipo_servico_valor_servicos,
    x="categoria_do_servico_padronizada",
    x_label="",
    y="valor_total_servicos",
    y_label="",
    horizontal=True,
)


st.write(" #### Quantidade de Serviços por Categoria")
st.write(
    """
        Aqui consideramos a quantidade de OS geradas por categoria de serviço,
        assim conseguimos avaliar quais são os serviços mais solicitados independente
        do valor.
    """
)

# considerar a coluna Numero_Da_OS_De_Servico para saber o total de serviços
df_categoria_servico_quantidade_servicos = (
    df.group_by("categoria_do_servico_padronizada")
    .agg(pl.col("numero_da_os_de_servico").count().alias("quantidade_total_servicos"))
    .sort("quantidade_total_servicos")
)

st.bar_chart(
    df_categoria_servico_quantidade_servicos,
    x="categoria_do_servico_padronizada",
    x_label="",
    y="quantidade_total_servicos",
    y_label="",
    horizontal=True,
)


# endregion
