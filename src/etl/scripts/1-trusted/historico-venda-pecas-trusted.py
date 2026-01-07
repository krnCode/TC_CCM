"""
Script de tratamento dos dados da camada Trusted do historico de venda de peças.

Esta camada tem como objetivo a limpeza, padronização e tratamento dos dados brutos
"""

import os
import polars as pl

from pathlib import Path
from datetime import datetime


# region ----- Caminho Arquivo Raw -----
RAW_FOLDER_PATH: Path = (
    Path(__file__).parent.parent.parent.parent
    / "etl"
    / "data"
    / "0-raw"
    / "historico-venda-pecas"
)

RAW_DATA_FILE = os.listdir(RAW_FOLDER_PATH)[0]

RAW_FILE_PATH = RAW_FOLDER_PATH / RAW_DATA_FILE
# endregion


# region ----- Suporte -----
# Padronização categorias
def categorias_padrao() -> dict:
    categorias_padrao: dict = {
        "ACESS.NAO ORIGINAIS": "ACESSORIOS NÃO ORIGINAIS",
        "ACESSORIOS NAO ORIGI": "ACESSORIOS NÃO ORIGINAIS",
        "ACESSORIOS NAO ORIG": "ACESSORIOS NÃO ORIGINAIS",
        "ACESSORIOS ORIGINAIS": "ACESSORIOS ORIGINAIS",
        "NISSAN-ACES. ORIG.": "ACESSORIOS ORIGINAIS",
        "NISSAN-ACES.NAO ORIG": "ACESSORIOS NÃO ORIGINAIS",
        "NISSAN-COM LUBRIF.": "LUBRIFICANTES/COMBUSTÍVEL",
        "COMB./LUB.": "LUBRIFICANTES/COMBUSTÍVEL",
        "LUBRIFICANTES": "LUBRIFICANTES/COMBUSTÍVEL",
        "LUBRIFICANTES/COMB.": "LUBRIFICANTES/COMBUSTÍVEL",
        "NISSAN-COM. LUBRIF.": "LUBRIFICANTES/COMBUSTÍVEL",
        "VOLKS-COM. LUBRIF.": "LUBRIFICANTES/COMBUSTÍVEL",
        "PECAS NAO ORIGINAIS": "PEÇAS NÃO ORIGINAIS",
        "VOLKS-PEC. NAO ORIG.": "PEÇAS NÃO ORIGINAIS",
        "PEÇAS N.ORIGINAIS": "PEÇAS NÃO ORIGINAIS",
        "FORD-PECAS NAO ORIG.": "PEÇAS NÃO ORIGINAIS",
        "NISSAN-PEC NAO ORIG.": "PEÇAS NÃO ORIGINAIS",
        "GM-PECAS ORIG. ": "PEÇAS ORIGINAIS",
        "NISSAN-PECAS ORIG.": "PEÇAS ORIGINAIS",
        "PECAS ORIGINAIS VW": "PEÇAS ORIGINAIS",
        "PECAS ORIGINAIS": "PEÇAS ORIGINAIS",
        "PEÇAS ORIGINAIS": "PEÇAS ORIGINAIS",
        "VOLKS-PECAS ORIG.": "PEÇAS ORIGINAIS",
        "PNEUS": "PNEUS",
        "VOLKS-PNEUS": "PNEUS",
        "NISSAN-PNEUS": "PNEUS",
        "OUTRAS MERCADORIAS": "OUTRAS MERCADORIAS",
        "VOLKS-OUTRAS MERCAD.": "OUTRAS MERCADORIAS",
    }

    return categorias_padrao


# endregion


# region ----- Tratar Arquivo Raw -----
# Ler o parquet raw
df_raw: pl.DataFrame = pl.read_parquet(source=RAW_FILE_PATH)


# Transformar dtypes das colunas
df_raw = df_raw.with_columns(
    [pl.col("Data_da_Venda").str.strptime(pl.Date, "%Y-%m-%d")]
)

df_raw = df_raw.with_columns(pl.col("Quantidade_Vendida").cast(pl.Int64))
df_raw = df_raw.with_columns(pl.col("Valor_da_Venda").cast(pl.Float64))
df_raw = df_raw.with_columns(pl.col("Custo_da_Peca").cast(pl.Float64))
df_raw = df_raw.with_columns(pl.col("Lucro_da_Venda").cast(pl.Float64))
df_raw = df_raw.with_columns(pl.col("Margem_da_Venda").cast(pl.Float64))


# Remover linhas duplicadas
colunas_subset: list = [
    "Cod_Concessionaria",
    "Cod_Filial",
    "Nome_da_Concessionaria",
    "Nome_da_Filial",
    "Marca_da_Filial",
    "Data_da_Venda",
    "Quantidade_Vendida",
    "Valor_da_Venda",
    "Custo_da_Peca",
    "Lucro_da_Venda",
    "Margem_da_Venda",
    "Descricao_da_Peca",
    "Categoria_da_Peca",
    "Departamento_da_Venda",
    "Tipo_de_Venda_da_Peca",
    "Cidade_da_Venda",
    "Estado_Brasileiro_da_Venda",
    "Macroregiao_Geografica_da_Venda",
]
df_raw = df_raw.unique(subset=colunas_subset)


# Padronizar as categorias
df_raw = df_raw.with_columns(
    pl.col("Categoria_da_Peca")
    .replace(categorias_padrao())
    .alias("Categoria_da_Peca_Padronizada")
)


# Ajustar o código da filial incorreto (1-1-3)
df_raw = df_raw.with_columns(
    pl.when(pl.col("Cod_Filial") == "1-1-3")
    .then(pl.lit("0-1-1"))
    .otherwise(pl.col("Cod_Filial"))
    .alias("Cod_Filial_Ajustado")
)


# Ajustar valores quando a quantidade vendida é igual a zero
zerar_valores: list = [
    "Valor_da_Venda",
    "Custo_da_Peca",
    "Lucro_da_Venda",
    "Margem_da_Venda",
]

df_raw = df_raw.with_columns(
    [
        pl.when(pl.col("Quantidade_Vendida") == 0)
        .then(0)
        .otherwise(pl.col(col))
        .alias(f"{col}_Ajustado")
        for col in zerar_valores
    ]
)

# Ajustar valor de custo
# endregion

print(df_raw.columns)
df_raw.write_excel(workbook="historico-venda-pecas-trusted.xlsx")
