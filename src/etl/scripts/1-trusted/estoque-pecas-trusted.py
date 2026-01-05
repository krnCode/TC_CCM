"""
Script de tratamento dos dados da camada Trusted de estoque de peças.

Esta camada tem como objetivo a limpeza, padronização e tratamento dos dados brutos
"""

import os
import polars as pl

from pathlib import Path
from src.etl.schemas.estoque-pecas-schema import ESTOQUE_PECAS_SCHEMA

# region ----- Caminho Arquivo Raw -----
RAW_FOLDER_PATH: Path = (
    Path(__file__).parent.parent.parent.parent
    / "etl"
    / "data"
    / "0-raw"
    / "estoque-pecas"
)

RAW_DATA_FILE = os.listdir(RAW_FOLDER_PATH)[0]

RAW_FILE_PATH = RAW_FOLDER_PATH / RAW_DATA_FILE
# endregion


# region ----- Criar DF -----
df: pl.DataFrame = pl.read_parquet(RAW_FILE_PATH)
print(df.columns)
# endregion
