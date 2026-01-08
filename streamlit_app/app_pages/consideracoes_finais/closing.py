"""
Página para as considerações finais do projeto.
"""

import streamlit as st


st.title("Considerações Finais do Projeto")

st.markdown(
    """
Nesta seção, apresentamos os principais itens identificados na análise dos dados, as limitações do projeto e os próximos passos recomendados.
"""
)

st.header("1. Principais Itens Identificados nos Dados")
st.markdown(
    """
A análise dos datasets enviados revelou as seguintes características e pontos fortes:

*   **Gestão Abrangente de Estoque:** Os dados de estoque de peças (`estoque_pecas_schema.py`) 
    demonstram um registros detalhados de itens, incluindo quantidade, valor, descrição, categoria,
    datas de última venda e entrada, status de obsolescência e informações de marca. Isso permite uma
    visão aprofundada da saúde do estoque e a identificação de itens obsoletos.
*   **Rastreamento de Desempenho de Vendas:** O histórico de vendas de peças (`historico_venda_pecas_schema.py`)
    fornece dados ricos sobre as transações, abrangendo quantidade vendida, valor da venda, custo, lucro, margem,
    descrição e categoria da peça, departamento, tipo de venda, vendedor, comprador e informações geográficas
    (apesar de terem sido apresentados apenas dados de uma única cidade, este registro possibilita acumular dados
    de outras regiões de vendas quando estas ocorrerem).
    Esses dados são cruciais para análises de desempenho de vendas, insights de rentabilidade e identificação de 
    padrões geográficos de vendas.
*   **Qualidade e Padronização de Dados:** A criação de campos como `categoria_da_peca_padronizada`, 
    `departamento_da_venda_padronizada`, `status_duplicidade` e `confiabilidade_do_registro`
    nos esquemas de dados demonstra esforço significativo na qualidade e padronização dos dados durante o processo
    de ETL. Isso garante que os dados sejam mais confiáveis e consistentes para análise.
*   **Processo ETL Estruturado:** A arquitetura do projeto com camadas `0-raw` (dados brutos) e `1-trusted` 
    (dados tratados), juntamente com a definição de esquemas (`src/etl/schemas`) e scripts ETL (`src/etl/scripts`),
    confirma uma abordagem estruturada e robusta para o processamento de dados. Isso transforma dados brutos em um 
    formato mais confiável e utilizável.
*   **Aplicação Streamlit para Visualização de Dados:** O aplicativo web criado em `streamlit_app`, com diversas 
    páginas, tem como objetivo principal providendiar a visualização e apresentação desses dados de maneira acessível
    e interativa, tornando os insights disponíveis para os usuários finais.
"""
)

st.header("2. Limitações")
st.markdown(
    """
Apesar dos pontos positivos, algumas limitações foram identificadas:

*   **Prazo:** Devido ao prazo de entrega do projeto, foram realizadas análises mais voltadas para a qualidade e confiabilidade
    dos dados, em vez de tendências e previsões. Em um cenário real, a qualidade dos dados pode ser mais importante do que a
    previsibilidade de tendências, e com mais tempo disponível, estas análises podem ser realizadas e com dados mais precisos, 
    além de mais criticidade em termos de padronização e apresentação dos dados..
*   **Integração de Dados Externos:** Visto que os dados foram providenciados diretamente por arquivos CSV, a ingestão de dados
    destes dados foi feita de forma mais rasa, apenas lendo os arquivos CSV,e os transformando em parquet. Em um cenário real,
    a ingestão de dados externos pode ser mais eficiente, por exemplo, através de APIs de integração de dados.
*   **Ausência de Objetivo Específico:** O projeto não tem como objetivo principal específico, desta forma, as ações realizadas
    foram mais voltadas em análises exploratórias e de dados brutos, também incluindo passos de limpeza e padronização,
    além do processo inicial de ETL, criação de Data Lake, armazenamento e apresentação dos dados.
"""
)

st.header("3. Próximos Passos")
st.markdown(
    """
Com base na análise, as seguintes recomendações são propostas para o avanço do projeto:

*   **Análise Aprofundada dos Dados:** Realizar análises específicas em cada conjunto de dados, com objetivos específicos,
    para extrair insights acionáveis. Isso inclui identificar as peças mais vendidas, o estoque de movimentação lenta,
    as regiões de vendas mais lucrativas e as tendências de serviços.
*   **Aprimoramento do Dashboard:** Desenvolver dashboards mais interativos e perspicazes dentro da aplicação Streamlit.
    Estes dashboards devem visualizar KPIs chave, tendências e anomalias identificadas durante a fase de análise de dados,
    oferecendo uma experiência de usuário mais rica. Além disso, providenciar informações que possam responder de forma
    direta a perguntas de negócio.
*   **Integração de Fontes de Dados Externas:** Considerar a integração de fontes de dados externas ou diretamente de APIs,
    desta forma facilitando a ingestão para peças, dados de vendas, etc, para assim enriquecer a análise e proporcionar uma
    perspectiva mais ampla.
*   **Otimização de Desempenho do ETL:** À medida que o volume de dados aumenta, revisar e otimizar os scripts ETL para
    garantir um desempenho e escalabilidade eficientes no processamento dos dados.
*   **Feedback do Usuário e Iteração:** Coletar feedback contínuo das partes interessadas sobre a aplicação Streamlit.
    Isso ajudará a identificar áreas para melhoria, novas funcionalidades necessárias ou diferentes visões analíticas que 
    agregariam mais valor.
"""
)
