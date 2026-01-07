"""
Página de entrada do app, com informações sobre o projeto.
"""

import streamlit as st


# region ----- Página Config -----
st.set_page_config(
    page_title="Sobre",
    layout="centered",
)
# endregion


# region ----- Sobre o projeto -----
st.title("Sobre")
st.divider()

st.write(
    """
        Olá, tudo bem?

        Esta página do Streamlit serve para apresentar os dados deste projeto, 
        desde os dados brutos até a camada Trusted.

        O objetivo é ter um painel interativo com que foi possível analisar dos 
        dados e fornecer visualizações e informações do que foi identificado 
        e tratado.

        Para seguir, basta selecionar as páginas do menu à esquerda.
    """
)
