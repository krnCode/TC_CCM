"""
Script para definir o esquema dos dados da camada Trusted de estoque de peças.
"""

import polars as pl

ESTOQUE_PECAS_SCHEMA: dict = {
    "cod_concessionaria": pl.String,
    "cod_filial": pl.String,
    "nome_da_concessionaria": pl.Utf8,
    "nome_da_filial": pl.Utf8,
    "marca_da_filial": pl.Utf8,
    "valor_da_peca_em_estoque": pl.Float64,
    "quantidade_da_peca_em_estoque": pl.Int64,
    "descricao_da_peca": pl.Utf8,
    "categoria_da_peca": pl.Utf8,
    "data_de_ultima_venda_da_peca": pl.Date,
    "data_da_ultima_entrada_no_estoque_da_peca": pl.Date,
    "peca_esta_obsoleta": pl.Boolean,
    "quanto_tempo_a_peca_esta_obsoleta": pl.Utf8,
    "nome_da_marca_da_peca": pl.Utf8,
    "codigo_da_peca_no_estoque": pl.String,
    "valor_da_peca_em_estoque_revisado": pl.Float64,
    "categoria_da_peca_padronizada": pl.Utf8,
    "classificacao_obsolescencia": pl.Utf8,
}
