"""
Script de tratamento dos dados da camada Trusted de estoque de veículos.

Esta camada tem como objetivo a limpeza, padronização e tratamento dos dados brutos
"""

import os
import polars as pl

from pathlib import Path
from datetime import datetime
from src.etl.schemas.estoque_veiculos_schema import ESTOQUE_VEICULOS_SCHEMA
from src.ferramentas.funcoes_suporte import salvar_parquet


# region ----- Caminho Arquivo Raw -----
RAW_FOLDER_PATH: Path = (
    Path(__file__).parent.parent.parent.parent
    / "etl"
    / "data"
    / "0-raw"
    / "estoque-veiculos"
)

RAW_DATA_FILE = os.listdir(RAW_FOLDER_PATH)[0]

RAW_FILE_PATH = RAW_FOLDER_PATH / RAW_DATA_FILE
# endregion


# region ----- Suporte -----
# Classificação tempo no estoque
def class_tempo_no_estoque() -> dict:
    class_tempo_no_estoque: dict = {
        "MENOS DE 1 MES": "1 - MENOS DE 1 MES",
        "1 A 3 MESES": "2 - 1 A 3 MESES",
        "3 A 6 MESES": "3 - 3 A 6 MESES",
        "6 A 9 MESES": "4 - 6 A 9 MESES",
        "9 A 12 MESES": "5 - 9 A 12 MESES",
        "1 A 2 ANOS": "6 - 1 A 2 ANOS",
        "2 A 3 ANOS": "7 - 2 A 3 ANOS",
        "MAIS DE 3 ANOS": "8 - MAIS DE 3 ANOS",
    }

    return class_tempo_no_estoque


# endregion


# region ----- Caminho Arquivo Trusted -----
TRUSTED_FOLDER_PATH: Path = (
    Path(__file__).parent.parent.parent.parent
    / "etl"
    / "data"
    / "1-trusted"
    / "estoque-veiculos"
)

TRUSTED_FOLDER_PATH.mkdir(parents=True, exist_ok=True)
# endregion


# region ----- Tratar Arquivo Raw -----
# Ler o parquet raw
df_raw: pl.DataFrame = pl.read_parquet(source=RAW_FILE_PATH)


# Transformar dtypes das colunas
df_raw = df_raw.with_columns([pl.col("Custo_do_Veiculo").cast(pl.Float64)])

df_raw = df_raw.with_columns(
    pl.col("Ano_Modelo_do_Veiculo").replace({"0": None}).cast(pl.String)
)
df_raw = df_raw.with_columns(
    pl.col("Ano_Modelo_do_Veiculo")
    .str.strptime(pl.Date, "%Y")
    .cast(pl.Date, strict=True)
)

df_raw = df_raw.with_columns(
    pl.col("Ano_Fabricacao_do_Veiculo").replace({"0": None}).cast(pl.String)
)
df_raw = df_raw.with_columns(
    pl.col("Ano_Fabricacao_do_Veiculo")
    .str.strptime(pl.Date, "%Y")
    .cast(pl.Date, strict=True)
)

df_raw = df_raw.with_columns(
    pl.col("Data_de_Entrada_do_Veiculo_no_Estoque")
    .str.strptime(pl.Date, "%d/%m/%Y")
    .cast(pl.Date, strict=True)
)

df_raw = df_raw.with_columns(
    pl.col("Data_de_Entrada_do_Veiculo_no_Estoque_duplicated_0")
    .str.strptime(pl.Date, "%d/%m/%Y")
    .cast(pl.Date, strict=True)
)

df_raw = df_raw.with_columns(
    pl.col("Data_de_Entrada_do_Veiculo_no_Estoque_duplicated_1")
    .str.strptime(pl.Date, "%d/%m/%Y")
    .cast(pl.Date, strict=True)
)

df_raw = df_raw.with_columns([pl.col("Kilometragem_Atual_do_Veiculo").cast(pl.Float64)])


# Classificação tempo no estoque
df_raw = df_raw.with_columns(
    pl.col("Tempo_Total_no_Estoque")
    .replace(class_tempo_no_estoque())
    .alias("Classificacao_Tempo_no_Estoque")
)


# Avaliar as colunas duplicadas de data, e considerar a data mais antiga
# De forma prudente, o custo deverá ser reconhecido no período mais antigo
df_raw = df_raw.with_columns(
    pl.min_horizontal(
        "Data_de_Entrada_do_Veiculo_no_Estoque",
        "Data_de_Entrada_do_Veiculo_no_Estoque_duplicated_0",
        "Data_de_Entrada_do_Veiculo_no_Estoque_duplicated_1",
    ).alias("Data_de_Entrada_do_Veiculo_no_Estoque_Atualizada")
)

# Renomear colunas para estarem em minúsculo
df_raw = df_raw.rename(str.lower)

# endregion


# df_raw.write_excel(workbook="estoque-veiculos-trusted.xlsx")


# region ----- Salvar Arquivo Trusted -----
# Salvar em um novo dataframe com os dados finais
df_trusted: pl.DataFrame = pl.DataFrame(
    data=df_raw,
    schema=ESTOQUE_VEICULOS_SCHEMA,
)

time_now: datetime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
file_name_trusted: str = f"estoque-veiculos-trusted-{time_now}.parquet"


salvar_parquet(
    df=df_trusted,
    path=TRUSTED_FOLDER_PATH,
    file_name=file_name_trusted,
)


# endregion
