"""
Script de tratamento dos dados da camada Trusted de estoque de peças.

Esta camada tem como objetivo a limpeza, padronização e tratamento dos dados brutos
"""

import os
import polars as pl

from pathlib import Path
from datetime import datetime
from src.etl.schemas.estoque_pecas_schema import ESTOQUE_PECAS_SCHEMA

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


# region ----- Suporte -----
# Padronização categorias
def padronizar_categorias() -> dict:
    categorias_padrao: dict = {}
    return categorias_padrao


# region ----- Tratar Arquivo Raw -----
# Ler o parquet raw
df_raw: pl.DataFrame = pl.read_parquet(source=RAW_FILE_PATH)

# Transformar dtypes das colunas
df_raw = df_raw.with_columns([pl.col("Valor_da_Peca_em_Estoque").cast(pl.Float64)])
df_raw = df_raw.with_columns([pl.col("Quantidade_da_Peca_em_Estoque").cast(pl.Int64)])
df_raw = df_raw.with_columns(
    [
        pl.col("Data_de_Ultima_Venda_da_Peca")
        .str.strptime(pl.Date, "%Y-%m-%d")
        .cast(pl.Date)
    ]
)
df_raw = df_raw.with_columns(
    [
        pl.col("Data_da_Ultima_Entrada_no_Estoque_da_Peca")
        .str.strptime(pl.Date, "%Y-%m-%d")
        .cast(pl.Date)
    ]
)

# Polars não consegue transformar os dados de strings diretamente para boolean, desta
# forma precisamos transformar os dados na coluna antes de transformar
df_raw = df_raw.with_columns(
    [
        pl.col("Peca_Esta_Obsoleta")
        .map_elements(lambda x: True if x == "True" else False)
        .cast(pl.Boolean)
    ]
)

# Para peças em que a quantidade é igual a 0, o valor em estoque também deve ser igual
# a 0
df_raw = df_raw.with_columns(
    [
        pl.when(pl.col("Quantidade_da_Peca_em_Estoque") == 0)
        .then(0.0)
        .otherwise(pl.col("Valor_da_Peca_em_Estoque"))
        .alias("Valor_da_Peca_em_Estoque_Revisado")
    ]
)


# endregion

df_raw.write_excel(workbook="estoque-pecas-trusted.xlsx")

# region ----- Salvar Arquivo Trusted -----
# Salvar em um novo dataframe com os dados finais
# df_trusted: pl.DataFrame = pl.DataFrame(data=df_raw, schema=ESTOQUE_PECAS_SCHEMA)
# endregion
