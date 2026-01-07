"""
Script para definir o esquema dos dados da camada Trusted do historico de venda de peças.
"""

import polars as pl

HISTORICO_VENDA_PECAS_SCHEMA: dict = {
    "cod_concessionaria": pl.String,
    "cod_filial": pl.String,
    "nome_da_concessionaria": pl.Utf8,
    "nome_da_filial": pl.Utf8,
    "marca_da_filial": pl.Utf8,
    "data_da_venda": pl.Date,
    "quantidade_vendida": pl.Float64,
    "valor_da_venda": pl.Float64,
    "custo_da_peca": pl.Float64,
    "lucro_da_venda": pl.Float64,
    "margem_da_venda": pl.Float64,
    "descricao_da_peca": pl.Utf8,
    "categoria_da_peca": pl.Utf8,
    "departamento_da_venda": pl.Utf8,
    "tipo_de_venda_da_peca": pl.Utf8,
    "nome_do_vendedor_que_realizou_a_venda": pl.Utf8,
    "nome_do_comprador_da_peca": pl.Utf8,
    "cidade_da_venda": pl.Utf8,
    "estado_brasileiro_da_venda": pl.Utf8,
    "macroregiao_geografica_da_venda": pl.Utf8,
    "linha_duplicada": pl.Utf8,
    "linha_duplicada_parcial": pl.Utf8,
    "status_duplicidade": pl.Utf8,
    "categoria_da_peca_padronizada": pl.Utf8,
    "departamento_da_venda_padronizada": pl.Utf8,
    "cod_filial_ajustado": pl.String,
    "valor_da_venda_ajustado": pl.Float64,
    "custo_da_peca_ajustado": pl.Float64,
    "lucro_da_venda_ajustado": pl.Float64,
    "margem_da_venda_ajustado": pl.Float64,
    "lucro_da_venda_recalculado": pl.Float64,
    "margem_da_venda_recalculado": pl.Float64,
    "custo_da_peca_classificado": pl.Utf8,
    "lucro_da_venda_classificado": pl.Utf8,
    "confiabilidade_do_registro": pl.Utf8,
}
