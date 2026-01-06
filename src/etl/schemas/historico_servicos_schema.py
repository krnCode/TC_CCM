"""
Script para definir o esquema dos dados da camada Trusted do historico de serviços.
"""

import polars as pl

HISTORICO_SERVICOS_SCHEMA: dict = {
    "cod_concessionaria": pl.Utf8,
    "cod_filial": pl.String,
    "nome_da_concessionaria": pl.Utf8,
    "nome_da_filial": pl.Utf8,
    "data_de_realizacao_do_servico": pl.Date,
    "quantidade_de_servicos_realizados": pl.Float64,
    "valor_total_do_servico_realizado": pl.Float64,
    "lucro_do_servico": pl.Float64,
    "descricao_do_servico_feito": pl.Utf8,
    "secao_que_o_servico_foi_feito": pl.Utf8,
    "departamento_que_realizou_o_servico": pl.Utf8,
    "categoria_do_servico": pl.Utf8,
    "tipo_de_servico_realizado": pl.Utf8,
    "nome_do_vendedor_que_vendeu_o_servico": pl.Utf8,
    "nome_do_mecanico_que_fez_o_servico": pl.Utf8,
    "nome_do_cliente_que_fez_o_servico": pl.Utf8,
    "cidade_do_servico": pl.Utf8,
    "estado_brasileiro_do_servico": pl.Utf8,
    "macroregiao_geografica_do_servico": pl.Utf8,
    "tempo_que_o_servico_levou_para_ser_realizado_em_horas": pl.Float64,
    "numero_da_os_de_servico": pl.String,
    "situacao_da_os": pl.String,
    "categoria_do_servico_padronizada": pl.Utf8,
    "total_os": pl.Float64,
    "cod_filial_ajustado": pl.String,
    "valor_do_servico_ajustado_com_revisao_gratuita": pl.Float64,
    "tempo_do_servico_horas_ajustado": pl.Float64,
}
