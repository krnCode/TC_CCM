"""
Script de tratamento dos dados da camada Trusted de estoque de peças.

Esta camada tem como objetivo a limpeza, padronização e tratamento dos dados brutos
"""

import os
import polars as pl

from pathlib import Path
from datetime import datetime
from src.etl.schemas.estoque_pecas_schema import ESTOQUE_PECAS_SCHEMA
from src.ferramentas.funcoes_suporte import salvar_parquet

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


# region ---- Caminho Arquivo Trusted ----
TRUSTED_FOLDER_PATH: Path = (
    Path(__file__).parent.parent.parent.parent
    / "etl"
    / "data"
    / "1-trusted"
    / "estoque-pecas"
)

TRUSTED_FOLDER_PATH.mkdir(parents=True, exist_ok=True)
# endregion


# region ----- Suporte -----
# Padronização categorias
def categorias_padrao() -> dict:
    categorias_padrao: dict = {
        "ACESS.NAO ORIGINAIS": "ACESSORIOS NÃO ORIGINAIS",
        "ACESSORIOS NAO ORIGI": "ACESSORIOS NÃO ORIGINAIS",
        "ACESSORIOS NAO ORIG": "ACESSORIOS NÃO ORIGINAIS",
        "ACESSORIOS ORIGINAIS": "ACESSORIOS ORIGINAIS",
        "COMB./LUB.": "LUBRIFICANTES/COMBUSTÍVEL",
        "LUBRIFICANTES": "LUBRIFICANTES/COMBUSTÍVEL",
        "LUBRIFICANTES/COMB.": "LUBRIFICANTES/COMBUSTÍVEL",
        "VOLKS-COM. LUBRIF.": "LUBRIFICANTES/COMBUSTÍVEL",
        "PECAS NAO ORIGINAIS": "PEÇAS NÃO ORIGINAIS",
        "VOLKS-PEC. NAO ORIG.": "PEÇAS NÃO ORIGINAIS",
        "PEÇAS N.ORIGINAIS": "PEÇAS NÃO ORIGINAIS",
        "NISSAN-PECAS ORIG.": "PEÇAS ORIGINAIS",
        "PECAS ORIGINAIS VW": "PEÇAS ORIGINAIS",
        "PECAS ORIGINAIS": "PEÇAS ORIGINAIS",
        "PEÇAS ORIGINAIS": "PEÇAS ORIGINAIS",
        "PNEUS": "PNEUS",
        "VOLKS-PNEUS": "PNEUS",
        "NISSAN-PNEUS": "PNEUS",
        "OUTRAS MERCADORIAS": "OUTRAS MERCADORIAS",
        "VOLKS-OUTRAS MERCAD.": "OUTRAS MERCADORIAS",
    }

    return categorias_padrao


# Classificação obsolescencia
def classificar_obsolescencia() -> dict:
    classificacao_obsolescencia: dict = {
        "0": "1 - DE 0 A 6 MESES",
        "6 MESES A 1 ANO": "2 - DE 6 MESES A 1 ANO",
        "1 ANO A 2 ANOS": "3 - DE 1 ANO A 2 ANOS",
        "2 ANOS A 3 ANOS": "4 - DE 2 ANOS A 3 ANOS",
        "MAIS DE 3 ANOS": "5 - MAIS DE 3 ANOS",
    }

    return classificacao_obsolescencia


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

# Padronizar as categorias
df_raw = df_raw.with_columns(
    pl.col("Categoria_da_Peca")
    .replace(categorias_padrao())
    .alias("Categoria_da_Peca_Padronizada")
)

# Incluir a classificação de obsolescencia
df_raw = df_raw.with_columns(
    pl.col("Quanto_Tempo_a_Peca_Esta_Obsoleta")
    .replace(classificar_obsolescencia())
    .alias("Classificacao_Obsolescencia")
)


# Renomear colunas para estarem em minúsculo
df_raw = df_raw.rename(str.lower)

# endregion

# df_raw.write_excel(workbook="estoque-pecas-trusted.xlsx")

# region ----- Salvar Arquivo Trusted -----
# Salvar em um novo dataframe com os dados finais
df_trusted: pl.DataFrame = pl.DataFrame(
    data=df_raw,
    schema=ESTOQUE_PECAS_SCHEMA,
)

# Salvar o dataframe
time_now: datetime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
file_name: str = f"estoque-pecas-raw-{time_now}.parquet"
file_name_trusted: str = f"estoque-pecas-trusted-{time_now}.parquet"


# Salvar arquivo parquet na camada trusted
salvar_parquet(
    df=df_trusted,
    path=TRUSTED_FOLDER_PATH,
    file_name=file_name_trusted,
)

# endregion
