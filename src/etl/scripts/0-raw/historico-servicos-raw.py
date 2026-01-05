"""
Script para tratar os dados brutos de historico de serviços.

Os tratamentos são mínimos, deixando o dado o mais próximo do original o possível.
"""

import polars as pl

from datetime import datetime
from pathlib import Path
from src.ferramentas.funcoes_suporte import salvar_parquet

# region ----- Caminho do dataset -----
DATASETS_PATH: Path = Path(__file__).parent.parent.parent.parent / "datasets"
# endregion

# region ----- Ler dataset bruto -----
servicos_realizados_raw: pl.DataFrame = pl.read_csv(
    source=DATASETS_PATH / "historico-de-servicos-realizados.csv",
    infer_schema=False,
)
# endregion


# region ----- Salvar dataset bruto em Parquet -----
RAW_DATA_PATH: Path = (
    Path(__file__).parent.parent.parent.parent
    / "etl"
    / "data"
    / "0-raw"
    / "historico-servicos"
)
RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)

time_now: datetime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
file_name: str = f"historico-servicos-raw-{time_now}.parquet"


salvar_parquet(
    df=servicos_realizados_raw,
    file_name=file_name,
    path=RAW_DATA_PATH,
)
# endregion
