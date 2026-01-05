"""
Script para manter funções de suporte separadas e reutilizáveis, mantendo o código
modular.
"""

import polars as pl


def salvar_parquet(df: pl.DataFrame, file_name: str, path: str) -> None:
    """
    Salva um DataFrame em um arquivo Parquet.

    Args:
        df (pl.DataFrame): DataFrame a ser salvo.
        file_name (str): Nome do arquivo a ser salvo.
        path (str): Caminho onde o arquivo irá ser salvo.
    """
    df.write_parquet(path + file_name)
