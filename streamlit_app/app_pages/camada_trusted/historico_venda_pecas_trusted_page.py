"""
Página de visualização dos dados tratados (camada trusted) do histórico de venda de peças.
"""

import os
import streamlit as st
import polars as pl

from pathlib import Path
from datetime import date


# region ----- Página Config -----
st.set_page_config(
    page_title="Histórico de Venda de Peças",
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
    / "historico-venda-pecas"
)

parquet_file = os.listdir(CAMADA_TRUSTED_PATH)[0]


def read_parquet() -> pl.DataFrame:
    return pl.read_parquet(source=CAMADA_TRUSTED_PATH / parquet_file)


# endregion


# region ----- Introdução -----
st.title("Histórico de Venda de Peças")
st.write(
    """
        Nesta página são apresentados os dados tratados do histórico de venda de peças.
        
        Aqui os dados já passaram por uma série de transformações e podem já
        ser apresentados de forma mais clara e intuitiva.

        *Obs: Como este dataset possui mais de 150 mil linhas, o Streamlit
        desabilita a classificação dos valores das colunas.*

        ---
    """
)
# endregion

# region ----- Tabela -----
st.subheader("Tabela")
df: pl.DataFrame = read_parquet()
df = df.drop("lucro_da_venda_recalculado")
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
        para serem considerados em KPIs ou métricas de negócio pois existem 
        diversas inconsistências que podem afetar o resultado final, como por 
        exemplo, a data de registro não estar correta, o valor de venda estar 
        incorreto, linhas totalmente duplicadas, linhas parcialmente duplicadas em
        que seria necessário confirmar se estão corretas, itens sem valor de custo,
        etc.

        Neste gráfico, apresentamos a comparação destes registros de forma visual.
    """
)

df_total_registros = df.group_by("confiabilidade_do_registro").agg(
    pl.col("tipo_de_venda_da_peca").count().alias("total_registros_confiaveis")
)

st.bar_chart(
    df_total_registros,
    x="confiabilidade_do_registro",
    x_label="",
    y="total_registros_confiaveis",
    y_label="",
    horizontal=True,
)


st.subheader("Registros com Custo da Venda Desproporcional")
st.write(
    """
        Há diversos registros em que o custo da venda é desproporcional 
        (acima de 400.000 reais). Para estes casos, é necessário confirmar
        o valor real do custo para poder ter o valor curreto de lucro ou perda
        na venda. Nesta visualização, apresentamos a quantidade de registros
        com custo da venda desproporcional por departamento para identificar
        os principais ofensores..
    """
)

df_registros_com_custo_da_venda_desproporcional = df.filter(
    pl.col("custo_da_peca") > 400000
)

df_registros_com_custo_da_venda_desproporcional = (
    df_registros_com_custo_da_venda_desproporcional.group_by(
        "departamento_da_venda",
    ).agg(pl.count().alias("quantidade_de_registros"))
)


st.bar_chart(
    df_registros_com_custo_da_venda_desproporcional,
    x="departamento_da_venda",
    x_label="",
    y="quantidade_de_registros",
    y_label="",
    horizontal=True,
)


st.subheader("Custo da Venda Sem Valor")
st.write(
    """
        Há diversos registros em que o custo da venda é igual a zero. Para estes
        casos, é necessário confirmar o valor real do custo para poder ter o
        valor curreto de lucro ou perda na venda. Nesta visualização, apresentamos
        a quantidade de registros com custo da venda sem valor por departamento
        para identificar os principais ofensores.
    """
)


df_registros_com_custo_da_venda_sem_valor = df.filter(pl.col("custo_da_peca") == 0)

df_registros_com_custo_da_venda_sem_valor = (
    df_registros_com_custo_da_venda_sem_valor.group_by(
        "departamento_da_venda",
    ).agg(pl.count().alias("quantidade_de_registros"))
)

st.bar_chart(
    df_registros_com_custo_da_venda_sem_valor,
    x="departamento_da_venda",
    x_label="",
    y="quantidade_de_registros",
    y_label="",
    horizontal=True,
)
