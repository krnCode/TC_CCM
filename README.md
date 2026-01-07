## Introdução

Este projeto é um exemplo de como um ciclo de vida de dados pode ser estruturado e executado de forma eficiente. O objetivo é fornecer uma visão geral do início de um processo ETL, com a ingestão, transformação, limpeza e padronização dos dados brutos, bem como a criação de uma camada Trusted para armazenar os dados processados e a apresentação em uma interface interativa utilizando o Streamlit.

O projeto é composto por dois componentes principais: o ETL e o Streamlit. O ETL é responsável por transformar os dados brutos em um formato legível pelo usuário, enquanto o Streamlit é usado para apresentar os resultados em uma interface interativa. Ambos os componentes são integrados em uma aplicação web, que permite a visualização de dados em tempo real e a interação com o usuário.

## Estrutura do Projeto

A estrutura do projeto reflete uma abordagem modular e organizada, facilitando a manutenção e a expansão:

- `src/datasets/`: Contém os conjuntos de dados brutos iniciais, em formato CSV.

- `src/etl/`: Abriga o coração do pipeline ETL, subdividido em:

  - `0-raw/`: Scripts para ingestão inicial e armazenamento dos dados brutos, preservando sua forma original.
  - `1-trusted/`: Scripts dedicados à limpeza, padronização, tratamento de anomalias (duplicidades, inconsistências), ajuste de tipos de dados e enriquecimento. É nesta camada que indicadores de qualidade e confiabilidade são inseridos.
  - `schemas/`: Definições de esquemas (com `Polars`) para cada conjunto de dados nas camadas Trusted, garantindo a validação da estrutura e dos tipos de dados.

- `src/ferramentas/`: Contém funções de suporte e utilitários que auxiliam no processo ETL. Este script evolui assim como o próprio ETL também avança, tornando-se uma ferramenta de suporte para o todo processo..

- `streamlit_app/`: Aplicação interativa desenvolvida com Streamlit para visualização, exploração e apresentação dos dados processados.

## Pipeline ETL em Detalhes

O pipeline ETL é orquestrado para converter dados brutos em informações de alto valor para análise:

- __Extração (Raw):__ Os dados são lidos de suas fontes originais (arquivos CSV) e armazenados como arquivos Parquet na camada `0-raw`, garantindo performance e compressão.

- __Transformação (Trusted):__ Na camada `1-trusted`, o `Polars` é empregado para realizar transformações essenciais. Isso inclui:

  - Ajuste de tipos de dados (e.g., datas, numéricos).
  - Identificação e tratamento de registros duplicados e parcialmente duplicados.
  - Padronização de campos categóricos (`Categoria_da_Peca`, `Departamento_da_Venda`).
  - Correção de dados incorretos ou inconsistentes (`Cod_Filial`).
  - Recálculo de métricas financeiras (lucro, margem) com base em valores ajustados.
  - Classificação de anomalias (valores de custo desproporcionais, devoluções com lucro) e atribuição de um status de `Confiabilidade_do_Registro`.

- __Carga (Trusted):__ Os dados transformados são salvos como arquivos Parquet na camada `1-trusted`, aderindo  aos schemas definidos para assegurar a conformidade.

## Modelagem e Qualidade de Dados

A definição explícita de schemas utilizando `Polars` é um pilar fundamental deste projeto. Ela permite a validação automática da estrutura e dos tipos de dados em cada etapa do pipeline, prevenindo erros e garantindo a consistência. A camada Trusted foca na elevação da qualidade dos dados, categorizando e corrigindo informações para que as análises subsequentes sejam construídas sobre uma base sólida e confiável.

## Visualização Interativa com Streamlit

Para facilitar o acesso aos dados e permitir uma exploração intuitiva, o projeto inclui uma aplicação desenvolvida em `Streamlit`. Esta ferramenta permite que analistas e usuários de negócio interajam com os datasets processados e visualizem os dados de forma dinâmica e acessível.

## Considerações Finais

Este projeto apresenta uma abordagem inicial para o ciclo de vida dos dados, començando da ingestão até a da primeira parte dos tratamentos e limpeza dos dados. A ênfase na qualidade dos dados na camada Trusted é muito importante para qualquer avaliação analítica.

Apesar de não estar neste projeto, a criação de uma camada `Refined` seria o próximo passo comum a ser seguido. A idéia seria gerar transformações adicionais, agregação e modelagem dos dados para consumo analítico e de negócio, de forma a deixar os acessos mais fáceis, intuitivos e com respostas a possíveis perguntas de negócio já demonstradas.