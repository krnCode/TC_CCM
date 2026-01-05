"""
Script para definir o esquema dos dados da camada Trusted de estoque de peças.
"""

import polars as pl

ESTOQUE_PECAS_SCHEMA: pl.DataFrame = pl.DataFrame(
    {
        "Cod_Concessionaria": pl.String,
        "Cod_Filial": pl.String,
        "Nome_da_Concessionaria": pl.Utf8,
        "Nome_da_Filial": pl.Utf8,
        "Marca_da_Filial": pl.Utf8,
        "Valor_da_Peca_em_Estoque": pl.Float64,
        "Quantidade_da_Peca_em_Estoque": pl.Int64,
        "Descricao_da_Peca": pl.Utf8,
        "Categoria_da_Peca": pl.Utf8,
        "Data_de_Ultima_Venda_da_Peca": pl.Date,
        "Data_da_Ultima_Entrada_no_Estoque_da_Peca": pl.Date,
        "Peca_Esta_Obsoleta": pl.Boolean,
        "Quanto_Tempo_a_Peca_Esta_Obsoleta": pl.Utf8,
        "Nome_da_Marca_da_Peca": pl.Utf8,
        "Codigo_da_Peca_no_Estoque": pl.String,
    }
)
