"""
Script principal para iniciar o app Streamlit.
"""

import streamlit as st

pgs: dict = {
    "HOME": [
        st.Page(title="Sobre", page="./app_pages/home/about.py"),
    ],
    "DATASETS INICIAIS": [
        st.Page(title="Datasets", page="./app_pages/view_datasets/datasets.py"),
    ],
    "CAMADA RAW": [
        st.Page(
            title="Estoque de Peças",
            page="./app_pages/camada_raw/estoque_pecas_raw_page.py",
        ),
        st.Page(
            title="Estoque de Veículos",
            page="./app_pages/camada_raw/estoque_veiculos_raw_page.py",
        ),
        st.Page(
            title="Historico de Serviços",
            page="./app_pages/camada_raw/historico_servicos_raw_page.py",
        ),
        st.Page(
            title="Historico de Vendas de Peças",
            page="./app_pages/camada_raw/historico_venda_pecas_raw_page.py",
        ),
        st.Page(
            title="Historico de Vendas de Veículos",
            page="./app_pages/camada_raw/historico_venda_veiculos_raw_page.py",
        ),
    ],
    "CAMADA TRUSTED": [
        st.Page(
            title="Estoque de Peças",
            page="./app_pages/camada_trusted/estoque_pecas_trusted_page.py",
        ),
        st.Page(
            title="Estoque de Veiculos",
            page="./app_pages/camada_trusted/estoque_veiculos_trusted_page.py",
        ),
        st.Page(
            title="Historico de Serviços",
            page="./app_pages/camada_trusted/historico_servicos_trusted_page.py",
        ),
        st.Page(
            title="Historico de Vendas de Peças",
            page="./app_pages/camada_trusted/historico_venda_pecas_trusted_page.py",
        ),
        st.Page(
            title="Historico de Vendas de Veículos",
            page="./app_pages/camada_trusted/historico_venda_veiculos_trusted_page.py",
        ),
    ],
    "CONSIDERAÇÕES FINAIS": [
        st.Page(
            title="Closing",
            page="./app_pages/consideracoes_finais/closing.py",
        ),
    ],
}

pages = st.navigation(pgs)

pages.run()
