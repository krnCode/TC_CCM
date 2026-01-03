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
}

pages = st.navigation(pgs)

pages.run()
