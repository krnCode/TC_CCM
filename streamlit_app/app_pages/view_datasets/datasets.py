"""
Script para ler os datasets brutos, fazer análises iniciais e mostrar no app Streamlit.
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
    source=DATASETS_PATH / "estoque-atual-de-pecas.csv",
    infer_schema=False,
)

estoque_veiculos: pl.DataFrame = pl.read_csv(
    source=DATASETS_PATH / "estoque-atual-de-veiculos.csv",
    separator=";",
    infer_schema=False,
)

historico_servicos: pl.DataFrame = pl.read_csv(
    source=DATASETS_PATH / "historico-de-servicos-realizados.csv",
    infer_schema=False,
)

historico_vendas_pecas: pl.DataFrame = pl.read_csv(
    source=DATASETS_PATH / "historico-de-vendas-de-pecas.csv",
    infer_schema=False,
)

historico_vendas_veiculos: pl.DataFrame = pl.read_csv(
    source=DATASETS_PATH / "historico-de-vendas-de-veiculos.csv",
    separator=";",
    infer_schema=False,
    ignore_errors=True,
    truncate_ragged_lines=True,
)
# endregion


# region ----- App -----
st.title("Datasets Iniciais")
st.write(
    """
        Nesta página os datasets são apresentados conforme os arquivos originais.
        O objetivo é ter uma visão geral dos dados para entender o que apresentam, 
        como estão estruturados e quais são as principais informações.
        
        Nesta análise inicial, também será possível identificar possíveis problemas de 
        formatação, dados faltantes, dados nulos, informações em colunas erradas, etc.
    """
)
st.divider()

# region ----- Estoque de Peças -----
with st.expander(
    "Estoque de Peças",
    expanded=False,
):
    col1, col2 = st.columns(spec=[4, 2], gap="small")
    with col1:
        st.subheader("Estoque de Peças")
        st.dataframe(estoque_pecas)

    with col2:
        st.subheader("Avaliação inicial")
        with st.expander("SEVERIDADE: CRÍTICO", expanded=False):
            st.write(
                """
                Impactam diretamente em KPIs e decisões de negócio.

                * Peças em estoque com valor de R$ 0,01;
                * Peças com valor mas que estão zeradas no estoque;
                * Existência de entrada de estoque de peças que tiveram a última venda a 
                mais de 10 anos atrás - 2000-01-01 (data errada ou compras de peças que 
                não tem movimentação?);
                * Obsolescência elevada em estoque (avaliar impacto financeiro);
            """
            )

        with st.expander("SEVERIDADE: MÉDIA", expanded=False):
            st.write(
                """
                Impacta em análises específicas.

                * Categorias não estão padronizadas;
            """
            )
# endregion


# region ----- Estoque de Veículos -----
with st.expander(
    "Estoque de Veículos",
    expanded=False,
):
    col1, col2 = st.columns(spec=[4, 2], gap="small")
    with col1:
        st.subheader("Estoque de Veículos")
        st.dataframe(estoque_veiculos)

    with col2:
        st.subheader("Avaliação inicial")
        with st.expander("SEVERIDADE: CRÍTICO", expanded=False):
            st.write(
                """
                Impactam diretamente em KPIs e decisões de negócio.

                * Colunas de data de entrada no estoque triplicadas com datas diferentes 
                , e alguns registros tem divergência no mês de entrada, e isso impacta  
                em reconhecimento por competência - necessário ajustar;
                """
            )

        with st.expander("SEVERIDADE: MÉDIA", expanded=False):
            st.write(
                """
                Impacta em análises específicas.

                * Registros sem informação se o veículo é novo ou semi-novo (dado 
                importante, responsável precisa atualizar);
                * Categoria de tipo do combustível como "NAO ESPECIFICADO" - ideal 
                alterar para o tipo de combustível específico;
                * Ano de fabricação e modelo do veículo com datas erradas (0, 1700) -
                ideal alterar para o ano correto;
                """
            )

        with st.expander("SEVERIDADE: BAIXA", expanded=False):
            st.write(
                """
                Não tem impacto relevante, mas demonstram pequenos problemas de controle 
                interno.

                * Kilometragem de veículo desproporcional (4.000.000.000) - necessário 
                ajustar;
                * Existência de veículos semi-novos que não tem kilometragem (está 
                correto ou precisa ajustar?);
                """
            )
# endregion


# region ----- Histórico de Serviços -----
with st.expander(
    "Histórico de Serviços",
    expanded=False,
):
    col1, col2 = st.columns(spec=[4, 2], gap="small")
    with col1:
        st.subheader("Histórico de Serviços")
        st.dataframe(historico_servicos)

    with col2:
        st.subheader("Avaliação inicial")
        with st.expander("SEVERIDADE: CRÍTICO", expanded=False):
            st.write(
                """
            Impactam diretamente em KPIs e decisões de negócio.

            * Filial CCM AUTOS 1 possui dois códigos diferentes (0-1-2 e 1-1-3);
            * Serviços com valor realizado menor que R$ 1,00;
            * Lucro de serviço realizado com valor negativo (-R$ 6.298,88);
            * Diversas descrições de serviços não são compatíveis com a categoria do 
            serviço realizado;
            * Revisões gratuitas mas que possuem valor de serviço e lucro;
            """
            )

        with st.expander("SEVERIDADE: MÉDIA", expanded=False):
            st.write(
                """
                Impacta em análises específicas.
            
                * Diversos registros não tem a informação de qual a seção o serviço foi 
                realizado (registrado como "UNKNOWN");
                * Diversos registros não possuem informação de tempo utilizado para 
                realizar o serviço;
                * Registro com tempo utilizado para realizar o serviço em horas 
                negativas;
                """
            )

        with st.expander("SEVERIDADE: BAIXA", expanded=False):
            st.write(
                """
                Não tem impacto relevante, mas demonstram pequenos problemas de controle
                interno.

                * Descrição de serviço não segue um padrão, possui diversas 
                inconsistências:
                    * Números que não tem significado claro;
                    * Digitação de códigos ao invés de descrições;
                    * Itens mais detalhados que outros;
                    * Descrições completas e abreviadas para o mesmo serviço;
                * Várias categorias para o mesmo serviço com nomes diferentes;
                * Informado situação da OS como "9" em todas as OS, mas não há a 
                descrição desta situação;
                """
            )
# endregion


# region ----- Histórico de Vendas de Peças -----
with st.expander(
    "Histórico de Vendas de Peças",
    expanded=False,
):
    col1, col2 = st.columns(spec=[4, 2], gap="small")
    with col1:
        st.subheader("Histórico de Vendas de Peças")
        st.dataframe(historico_vendas_pecas)

    with col2:
        st.subheader("Avaliação inicial")
        with st.expander("SEVERIDADE: CRÍTICO", expanded=False):
            st.write(
                """
                Impactam diretamente em KPIs e decisões de negócio.
                
                * Filial CCM AUTOS 1 possui dois códigos diferentes (0-1-2 e 1-1-3);
                * Lançamentos de venda mas com quantidade vendida igual a zero;
                * Lançamentos de venda com valor de custo igual a zero;
                * Peças com custo zero;
                * Muitas vendas com perdas (avaliar impacto financeiro);
                """
            )

        with st.expander("SEVERIDADE: BAIXA", expanded=False):
            st.write(
                """
                * Descrição das peças não segue um padrão;
                * Categorias das peças possui diversas classificações para o mesmo tipo;
                * Departamento possui 4 itens para peças;
                """
            )
# endregion


# region ----- Histórico de Vendas de Veículos -----
with st.expander(
    "Histórico de Vendas de Veículos",
    expanded=False,
):
    col1, col2 = st.columns(spec=[4, 2], gap="small")
    with col1:
        st.subheader("Histórico de Vendas de Veículos")
        st.dataframe(historico_vendas_veiculos)

    with col2:
        st.subheader("Avaliação inicial")
        with st.expander("SEVERIDADE: CRÍTICO", expanded=False):
            st.write(
                """
                Impactam diretamente em KPIs e decisões de negócio.

                * Códigos de filial como datas;
                * Devoluções em que o valor do lucro é positivo;
                * Devoluções sem informação específica de marca, modelo, familia, 
                categoria, etc;
                * Lançamentos de devoluções duplicados;
                * Lançamentos repetidos da mesma venda, e sem informação do tipo de 
                venda;
                * Inconsistências na coluna de dias em estoque:
                    * Lançado código ao invés de dias;
                    * Dias negativos em estoque;
                """
            )

        with st.expander("SEVERIDADE: MÉDIA", expanded=False):
            st.write(
                """
                Impacta em análises específicas.

                * Lançamentos com tipo de combustível como "NAO ESPECIFICADO";
                * Lançamentos sem o ano modelo do veículo;
                * Lançamentos sem o ano de fabricação do veículo;
                * Inconsistências na coluna de dias em estoque:
                    * Lançado código ao invés de dias;
                    * Dias negativos em estoque;
                """
            )

        with st.expander("SEVERIDADE: BAIXA", expanded=False):
            st.write(
                """
                Não tem impacto relevante, mas demonstram pequenos problemas de controle
                interno.

                * Informação de cor do veículo como "1";
                """
            )
# endregion


# endregion
