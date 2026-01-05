"""
Script para tratar os dados brutos de estoque de veículos.

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
estoque_veiculos_raw: pl.DataFrame = pl.read_csv(
    source=DATASETS_PATH / "estoque-atual-de-veiculos.csv",
    infer_schema=False,
)
# endregion


# region ----- Salvar dataset bruto em Parquet -----
RAW_DATA_PATH: Path = (
    Path(__file__).parent.parent.parent.parent
    / "etl"
    / "data"
    / "0-raw"
    / "estoque-veiculos"
)
RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)

time_now: datetime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
file_name: str = f"estoque-veiculos-raw-{time_now}.parquet"


salvar_parquet(
    df=estoque_veiculos_raw,
    file_name=file_name,
    path=RAW_DATA_PATH,
)
# endregion
