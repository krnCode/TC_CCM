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
    / "historico-venda-veiculos"
)

RAW_DATA_FILE = os.listdir(RAW_FOLDER_PATH)[0]

RAW_FILE_PATH = RAW_FOLDER_PATH / RAW_DATA_FILE
# endregion


# region ----- Tratar Arquivo Raw -----
# Ler o parquet raw
df_raw: pl.DataFrame = pl.read_parquet(source=RAW_FILE_PATH)


# Ajustar o código da filial incorreto (1-1-3 / 01/01/2002 / 01/01/2003)
df_raw = df_raw.with_columns(
    pl.when(
        (pl.col("Cod_Filial") == "01/01/2002") | (pl.col("Cod_Filial") == "01/01/2003")
    )
    .then(pl.lit("0-1-1"))
    .otherwise(pl.col("Cod_Filial"))
    .alias("Cod_Filial_Ajustado")
)


# Classificar linhas duplicadas, parcialmente duplicadas
colunas_subset: list = [
    "Cod_Concessionaria",
    "Cod_Filial",
    "Nome_da_Concessionaria",
    "Nome_da_Filial",
    "Marca_da_Filial",
    "Data_da_Venda",
    "Quantidade_Vendida",
    "Valor_da_Venda",
    "Custo_do_Veiculo",
    "Lucro_da_Venda",
    "Margem_da_Venda",
    "Marca_do_Veiculo",
    "Modelo_do_Veiculo",
    "Familia_do_Veiculo",
    "Categoria_do_Veiculo",
    "Cor_do_Veiculo",
    "Veiculo_Novo_ou_Semi_Novo",
    "Tipo_do_Combustivel",
    "Ano_Modelo_do_Veiculo",
    "Ano_Fabricacao_do_Veiculo",
    "Dias_que_o_Carro_Ficou_no_Estoque",
    "Tipo_de_Venda_do_Veiculo",
    "Cidade_da_Venda",
    "Estado_Brasileiro_da_Venda",
    "Macroregiao_Geografica_da_Venda",
    "",
    "Cod_Filial_Ajustado",
]

df_raw = df_raw.with_columns(
    pl.struct(pl.all()).is_duplicated().alias("Linha_Duplicada")
)

df_raw = df_raw.with_columns(
    pl.struct(colunas_subset).is_duplicated().alias("Linha_Duplicada_Parcial")
)

df_raw = df_raw.with_columns(
    pl.when(pl.col("Linha_Duplicada"))
    .then(pl.lit("DUPLICADO"))
    .when(pl.col("Linha_Duplicada_Parcial"))
    .then(pl.lit("DUPLICADO PARCIALMENTE"))
    .otherwise(pl.lit("OK"))
    .alias("Status_Duplicidade")
)


# Classificar itens com colunas deslocadas e repetidas
df_raw = df_raw.with_columns(
    pl.when(
        pl.col("Dias_que_o_Carro_Ficou_no_Estoque").str.contains("9BWAG45U4PT01905")
    )
    .then(pl.lit("CONTÉM COLUNAS DESLOCADAS E REPETIDAS"))
    .otherwise(pl.lit("OK"))
    .alias("Colunas_Deslocadas_e_Repetidas")
)


# Renomear coluna sem nome
df_raw = df_raw.rename({"": "coluna_extra"})


# Ajustar as colunas deslocadas para a direita, por coluna
# O que será feito é criar novas colunas com os valores da coluna correta
colunas_deslocadas = [
    "Tipo_de_Venda_do_Veiculo",
    "Nome_do_Vendedor_que_Realizou_a_Venda",
    "Nome_do_Comprador_do_Veiculo",
    "Cidade_da_Venda",
    "Estado_Brasileiro_da_Venda",
    "Macroregiao_Geografica_da_Venda",
    "coluna_extra",
]

df_raw = df_raw.with_columns(
    # Para cada coluna deslocada, pegar o valor da próxima coluna
    # Utilizado list comprehension para iterar sobre as colunas
    [
        pl.when(pl.col("coluna_extra").is_not_null())
        .then(pl.col(colunas_deslocadas[i + 1]))
        .otherwise(pl.col(colunas_deslocadas[i]))
        .alias(colunas_deslocadas[i] + "_Ajustado")
        for i in range(len(colunas_deslocadas) - 1)
    ]
    + [
        # A última coluna ajustada vira null (não existe valor correto)
        pl.when(pl.col("coluna_extra").is_not_null())
        .then(pl.lit(None))
        .otherwise(pl.col("coluna_extra"))
        .alias("coluna_extra_Ajustado")
    ]
)

df_raw = df_raw.with_columns(
    pl.when(
        pl.col("Dias_que_o_Carro_Ficou_no_Estoque").str.contains("9BWAG45U4PT01905")
    )
    .then(0)
    .otherwise(pl.col("Dias_que_o_Carro_Ficou_no_Estoque"))
    .alias("Dias_que_o_Carro_Ficou_no_Estoque_Ajustado")
)


# Transformar dtypes das colunas
df_raw = df_raw.with_columns(pl.col("Data_da_Venda").str.strptime(pl.Date, "%d/%m/%Y"))

df_raw = df_raw.with_columns(
    pl.col("Ano_Modelo_do_Veiculo")
    .cast(pl.Float64)
    .cast(pl.Int64)
    .cast(pl.String)
    .str.strptime(pl.Date, "%Y")
)
df_raw = df_raw.with_columns(
    pl.col("Ano_Fabricacao_do_Veiculo")
    .cast(pl.Float64)
    .cast(pl.Int64)
    .cast(pl.String)
    .str.strptime(pl.Date, "%Y")
)


df_raw = df_raw.with_columns(pl.col("Quantidade_Vendida").cast(pl.String))

df_raw = df_raw.with_columns(pl.col("Valor_da_Venda").cast(pl.Float64))
df_raw = df_raw.with_columns(pl.col("Custo_do_Veiculo").cast(pl.Float64))
df_raw = df_raw.with_columns(pl.col("Lucro_da_Venda").cast(pl.Float64))
df_raw = df_raw.with_columns(pl.col("Margem_da_Venda").cast(pl.Float64))

df_raw = df_raw.with_columns(
    pl.col("Dias_que_o_Carro_Ficou_no_Estoque_Ajustado").cast(pl.Int64)
)


# Deletar coluna "coluna_extra_Ajustado"
df_raw = df_raw.drop("coluna_extra_Ajustado")


# Converter dias no estoque para valores positivos
df_raw = df_raw.with_columns(
    pl.when(pl.col("Dias_que_o_Carro_Ficou_no_Estoque_Ajustado") < 0)
    .then(pl.col("Dias_que_o_Carro_Ficou_no_Estoque_Ajustado") * -1)
    .otherwise(pl.col("Dias_que_o_Carro_Ficou_no_Estoque_Ajustado"))
    .alias("Dias_que_o_Carro_Ficou_no_Estoque_Ajustado")
)


# Recalcular lucro e margem
df_raw = df_raw.with_columns(
    (pl.col("Valor_da_Venda") - pl.col("Custo_do_Veiculo")).alias(
        "Lucro_da_Venda_Recalculado"
    )
)

df_raw = df_raw.with_columns(
    (pl.col("Lucro_da_Venda_Recalculado") / pl.col("Valor_da_Venda")).alias(
        "Margem_da_Venda_Recalculado"
    )
)


# Classificar linhas como CONFIÁVEIS ou NÃO CONFIÁVEIS conforme a qualidade do registro
df_raw = df_raw.with_columns(
    pl.when(
        (pl.col("Status_Duplicidade") == "OK")
        & (pl.col("Colunas_Deslocadas_e_Repetidas") == "OK")
    )
    .then(pl.lit("CONFIÁVEL"))
    .otherwise(pl.lit("NÃO CONFIÁVEL"))
    .alias("Confiabilidade_do_Registro")
)

print(df_raw)

# endregion

df_raw.write_excel(workbook="historico-venda-veiculos-trusted.xlsx")
