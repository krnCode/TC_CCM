"""
Script para manter funções de suporte separadas e reutilizáveis, mantendo o código
modular.
"""

import polars as pl
from pathlib import Path


def salvar_parquet(df: pl.DataFrame, file_name: str, path: Path) -> None:
    """
    Salva um DataFrame em um arquivo Parquet.

    Args:
        df (pl.DataFrame): DataFrame a ser salvo.
        file_name (str): Nome do arquivo a ser salvo.
        path (str): Caminho onde o arquivo irá ser salvo.
    """
    path_with_file_name: Path = path / file_name
    df.write_parquet(file=path_with_file_name)
