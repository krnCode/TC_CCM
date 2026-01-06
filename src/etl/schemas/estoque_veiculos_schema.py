"""
Script para definir o esquema dos dados da camada Trusted de estoque de veiculos.
"""

import polars as pl

ESTOQUE_VEICULOS_SCHEMA: dict = {
    "nome_da_concessionaria": pl.Utf8,
    "nome_da_filial": pl.Utf8,
    "custo_do_veiculo": pl.Float64,
    "marca_da_filial": pl.Utf8,
    "marca_do_veiculo": pl.Utf8,
    "modelo_do_veiculo": pl.Utf8,
    "cor_do_veiculo": pl.Utf8,
    "veiculo_novo_ou_semi_novo": pl.Utf8,
    "tipo_do_combustivel": pl.Utf8,
    "ano_modelo_do_veiculo": pl.Date,
    "ano_fabricacao_do_veiculo": pl.Date,
    "tempo_total_no_estoque": pl.Utf8,
    "kilometragem_atual_do_veiculo": pl.Float64,
    "data_de_entrada_do_veiculo_no_estoque": pl.Date,
    "data_de_entrada_do_veiculo_no_estoque_duplicated_0": pl.Date,
    "data_de_entrada_do_veiculo_no_estoque_duplicated_1": pl.Date,
    "classificacao_tempo_no_estoque": pl.Utf8,
    "data_de_entrada_do_veiculo_no_estoque_atualizada": pl.Date,
}
