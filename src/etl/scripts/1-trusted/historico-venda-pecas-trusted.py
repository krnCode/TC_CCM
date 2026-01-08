"""
Script de tratamento dos dados da camada Trusted do historico de venda de peças.

Esta camada tem como objetivo a limpeza, padronização e tratamento dos dados brutos
"""

import os
import polars as pl

from pathlib import Path
from datetime import datetime
from src.etl.schemas.historico_venda_pecas_schema import HISTORICO_VENDA_PECAS_SCHEMA
from src.ferramentas.funcoes_suporte import salvar_parquet


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


# region ----- Caminho Arquivo Trusted -----
TRUSTED_FOLDER_PATH: Path = (
    Path(__file__).parent.parent.parent.parent
    / "etl"
    / "data"
    / "1-trusted"
    / "historico-venda-pecas"
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


# Padronizar departamentos
def departamentos_padronizados() -> dict:
    departamentos_padronizados: dict = {
        "ACESSORIOS": "ACESSÓRIOS",
        "ADMINISTRACAO": "ADMINISTRAÇÃO",
        "ASSISTENCIA TECNICA": "ASSISTÊNCIA TÉCNICA",
        "ASSISTENCIA TECNICA ": "ASSISTÊNCIA TÉCNICA",
        "FUNILARIA E PINTURA": "FUNILARIA E PINTURA",
        "OFICINA": "OFICINA",
        "PECAS": "PEÇAS",
        "PECAS ATACADO": "PEÇAS",
        "PECAS BALCAO": "PEÇAS",
        "PECAS VAREJO": "PEÇAS",
        "VEICULOS NOVOS": "VEÍCULOS NOVOS",
    }

    return departamentos_padronizados


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


# Destacar linhas duplicadas, parcialmente duplicadas
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


# Padronizar as categorias
df_raw = df_raw.with_columns(
    pl.col("Categoria_da_Peca")
    .replace(categorias_padrao())
    .alias("Categoria_da_Peca_Padronizada")
)


# Padronizar departamentos
df_raw = df_raw.with_columns(
    pl.col("Departamento_da_Venda")
    .replace(departamentos_padronizados())
    .alias("Departamento_da_Venda_Padronizada")
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


# Recalcular lucro da venda
df_raw = df_raw.with_columns(
    (pl.col("Valor_da_Venda_Ajustado") - pl.col("Custo_da_Peca_Ajustado")).alias(
        "Lucro_da_Venda_Recalculado"
    )
)

# Recalcular margem da venda
df_raw = df_raw.with_columns(
    (pl.col("Lucro_da_Venda_Recalculado") / pl.col("Valor_da_Venda_Ajustado")).alias(
        "Margem_da_Venda_Recalculado"
    )
)

# Ajustar margem quando o lucro for 0
df_raw = df_raw.with_columns(
    pl.when(pl.col("Lucro_da_Venda_Ajustado") == 0)
    .then(0)
    .otherwise(pl.col("Margem_da_Venda_Recalculado"))
    .alias("Margem_da_Venda_Recalculado")
)

# Classificar valor de custo desproporcionais
df_raw = df_raw.with_columns(
    pl.when(pl.col("Custo_da_Peca") > 400000)
    .then(pl.lit("VALOR DE CUSTO DESPROPORCIONAL"))
    .otherwise(pl.lit("OK"))
    .alias("Custo_da_Peca_Classificado")
)


# Classificar devoluções com lucro
df_raw = df_raw.with_columns(
    pl.when(
        (pl.col("Lucro_da_Venda") > 0)
        & (pl.col("Tipo_de_Venda_da_Peca") == "DEVOLUCAO")
    )
    .then(pl.lit("DEVOLUCAO COM LUCRO"))
    .otherwise(pl.lit("OK"))
    .alias("Lucro_da_Venda_Classificado")
)

# Classificar registros com valor de custo igual a 0
df_raw = df_raw.with_columns(
    pl.when(pl.col("Custo_da_Peca") == 0)
    .then(pl.lit("VALOR DE CUSTO IGUAL A 0"))
    .otherwise(pl.lit("OK"))
    .alias("Custo_da_Peca_Classificado")
)


# Classificar linhas como CONFIÁVEIS ou NÃO CONFIÁVEIS conforme a qualidade do registro
df_raw = df_raw.with_columns(
    pl.when(
        (pl.col("Status_Duplicidade") == "OK")
        & (pl.col("Custo_da_Peca_Classificado") == "OK")
        & (pl.col("Lucro_da_Venda_Classificado") == "OK")
    )
    .then(pl.lit("CONFIÁVEL"))
    .otherwise(pl.lit("NÃO CONFIÁVEL"))
    .alias("Confiabilidade_do_Registro")
)


# Renomear colunas para minusculas
df_raw = df_raw.rename(str.lower)

# endregion


# df_raw.write_excel(workbook="historico-venda-pecas-trusted.xlsx")


# region ----- Salvar Arquivo Trusted -----
# Salvar em um novo dataframe com os dados finais
df_trusted: pl.DataFrame = pl.DataFrame(
    data=df_raw,
    schema=HISTORICO_VENDA_PECAS_SCHEMA,
)

time_now: datetime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
file_name_trusted: str = f"historico-venda-pecas-trusted-{time_now}.parquet"


# salvar_parquet(
#     df=df_trusted,
#     path=TRUSTED_FOLDER_PATH,
#     file_name=file_name_trusted,
# )

file = os.listdir(TRUSTED_FOLDER_PATH)[0]
test: pl.DataFrame = pl.read_parquet(source=TRUSTED_FOLDER_PATH / file)

print("usando arquivo: " + file)
print(test)
print(
    test[
        "custo_da_peca",
        "custo_da_peca_ajustado",
        "custo_da_peca_classificado",
        "lucro_da_venda_recalculado",
        "lucro_da_venda_ajustado",
        "lucro_da_venda_classificado",
    ].sort(by="custo_da_peca", descending=True)
)
# endregion
