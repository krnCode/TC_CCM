"""
Script de tratamento dos dados da camada Trusted do historico de serviços.

Esta camada tem como objetivo a limpeza, padronização e tratamento dos dados brutos
"""

import os
import polars as pl

from pathlib import Path
from datetime import datetime
from src.etl.schemas.historico_servicos_schema import HISTORICO_SERVICOS_SCHEMA
from src.ferramentas.funcoes_suporte import salvar_parquet


# region ----- Caminho Arquivo Raw -----
RAW_FOLDER_PATH: Path = (
    Path(__file__).parent.parent.parent.parent
    / "etl"
    / "data"
    / "0-raw"
    / "historico-servicos"
)

RAW_DATA_FILE = os.listdir(RAW_FOLDER_PATH)[0]

RAW_FILE_PATH = RAW_FOLDER_PATH / RAW_DATA_FILE
# endregion


# region ----- Caminho Arquivo Trusted -----
TRUSTED_FOLDER_PATH: Path = (
    Path(__file__).parent.parent.parent.parent
    / "etl"
    / "data"
    / "1-trusted"
    / "historico-servicos"
)

TRUSTED_FOLDER_PATH.mkdir(parents=True, exist_ok=True)
# endregion


# region ----- Suporte -----
# Padronizar categoria do serviço
def padronizar_categoria_serviço() -> dict:
    padronizar_categoria_serviço: dict = {
        "ACESSORIOS": "ACESSORIOS",
        "ALINHAMENTO/BALANCEAMENTO": "ALINHAMENTO E BALANCEAMENTO",
        "ALINHAMENTO E BALANCEAMEN": "ALINHAMENTO E BALANCEAMENTO",
        "LUBRIFICACAO": "LUBRIFICAÇÃO",
        "LUBRIFICAÇÃO": "LUBRIFICAÇÃO",
        "MECANICA GERAL": "MECANICA",
        "MECANICA E ELETRICA": "MECANICA",
        "MECANICA": "MECANICA",
        "PDI-REVISÃO DE ENTREGA": "REVISÃO",
        "REVISAO PROGRAMADA": "REVISÃO",
        "REVISAO GRATUITA": "REVISÃO GRATUITA",
        "REVISAO": "REVISÃO",
        "SERV TERCEIROS": "SERVIÇOS DE TERCEIRO",
        "SERVICO TERCEIRO": "SERVIÇOS DE TERCEIRO",
        "SERVIÇOS DE TERCEIROS": "SERVIÇOS DE TERCEIRO",
        "ELETRIC": "ELETRICA",
        "FUNILARI": "FUNILARIA",
        "MONTAGEM": "MONTAGEM",
        "LAVAGE": "LAVAGEM",
        "PINTURA": "PINTURA",
        "DEMAIS CATEGORIA": "OUTROS",
    }

    return padronizar_categoria_serviço


# endregion


# region ----- Tratar Arquivo Raw -----
# Ler o parquet raw
df_raw: pl.DataFrame = pl.read_parquet(source=RAW_FILE_PATH)


# Transformar dtypes das colunas
df_raw = df_raw.with_columns(
    [pl.col("Data_De_Realizacao_Do_Servico").str.strptime(pl.Date, "%Y-%m-%d")]
)

df_raw = df_raw.with_columns(
    pl.col("Quantidade_De_Servicos_Realizados").cast(pl.Float64)
)

df_raw = df_raw.with_columns(
    [pl.col("Valor_Total_Do_Servico_Realizado").cast(pl.Float64)]
)

df_raw = df_raw.with_columns([pl.col("Lucro_Do_Servico").cast(pl.Float64)])
df_raw = df_raw.with_columns(
    [pl.col("Tempo_Que_O_Servico_Levou_Para_Ser_Realizado_em_Horas").cast(pl.Float64)]
)


# Padronizar categoria do serviço
df_raw = df_raw.with_columns(
    pl.col("Categoria_Do_Servico")
    .replace(padronizar_categoria_serviço())
    .alias("Categoria_Do_Servico_Padronizada")
)


# Totalizar o valor das OS e retornar no dataframe como Total_OS
df_totalizar_os: pl.DataFrame = df_raw.group_by("Numero_Da_OS_De_Servico").agg(
    pl.col("Valor_Total_Do_Servico_Realizado").sum().alias("Total_OS")
)
df_raw = df_raw.join(df_totalizar_os, on="Numero_Da_OS_De_Servico", how="left")


# Ajustar o código da filial incorreto (1-1-3)
df_raw = df_raw.with_columns(
    pl.when(pl.col("Cod_Filial") == "1-1-3")
    .then(pl.lit("0-1-1"))
    .otherwise(pl.col("Cod_Filial"))
    .alias("Cod_Filial_Ajustado")
)


# Ajustar valor de revisões gratuitas que possuem valor acima de 0 para 0 (REVISÃO GRATUITA)
df_raw = df_raw.with_columns(
    pl.when(
        (pl.col("Categoria_Do_Servico_Padronizada") == "REVISÃO GRATUITA")
        & (pl.col("Valor_Total_Do_Servico_Realizado") > 0)
    )
    .then(0)
    .otherwise(pl.col("Valor_Total_Do_Servico_Realizado"))
    .alias("Valor_Do_Servico_Ajustado_Com_Revisao_Gratuita")
)


# Ajustar valor negativo de tempo de serviço para positivo
df_raw = df_raw.with_columns(
    pl.when(pl.col("Tempo_Que_O_Servico_Levou_Para_Ser_Realizado_em_Horas") < 0)
    .then(pl.col("Tempo_Que_O_Servico_Levou_Para_Ser_Realizado_em_Horas") * -1)
    .otherwise(pl.col("Tempo_Que_O_Servico_Levou_Para_Ser_Realizado_em_Horas"))
    .alias("Tempo_Do_Servico_Horas_Ajustado")
)

# Ajustar nome das colunas para estarem em minúsculo
df_raw = df_raw.rename(str.lower)

# endregion

# df_raw.write_excel(workbook="historico-servicos-raw.xlsx")

# region ----- Salvar Arquivo Trusted -----
# Salvar em um novo dataframe com os dados finais
df_trusted: pl.DataFrame = pl.DataFrame(
    data=df_raw,
    schema=HISTORICO_SERVICOS_SCHEMA,
)

time_now: datetime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
file_name_trusted: str = f"historico-servicos-trusted-{time_now}.parquet"


salvar_parquet(
    df=df_trusted,
    path=TRUSTED_FOLDER_PATH,
    file_name=file_name_trusted,
)
# endregion
