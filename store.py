from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence, Optional

import pandas as pd


class ParquetRecordStore:
    """
    Generic store for writing arbitrary records (dicts) into Parquet datasets.

    Directory layout:
      root_dir/
        <dataset>/
          symbol=AAPL.US/part-*.parquet
          symbol=MULT.DE/part-*.parquet

    Minimal expectations:
      - record is a dict-like mapping
      - for time series you usually include: symbol, asof_date
    """

    def __init__(self, root_dir: str = "data"):
        self.root = Path(root_dir)
        self.root.mkdir(parents=True, exist_ok=True)

    def append(
        self,
        dataset: str,
        record: Mapping[str, Any],
        partition_cols: Optional[Sequence[str]] = ("symbol",),
    ) -> None:
        if not dataset or not isinstance(dataset, str):
                raise ValueError("dataset must be a non-empty string")
        
        if not isinstance(record, Mapping) or len(record) == 0:
            raise ValueError("record must be a non-empty dict-like mapping")

        df = pd.DataFrame([dict(record)])

        dataset_path = self.root / dataset
        dataset_path.mkdir(parents=True, exist_ok=True)

        # Build partition path like: symbol=MULT.DE/
        if partition_cols:
            part_path = dataset_path
            for col in partition_cols:
                if col not in df.columns:
                    raise ValueError(f"partition column '{col}' missing from record")
                val = df[col].iloc[0]
                part_path = part_path / f"{col}={val}"
            part_path.mkdir(parents=True, exist_ok=True)
        else:
            part_path = dataset_path

        # Unique filename per append (safe + simple)
        filename = f"part-{pd.Timestamp.utcnow().value}.parquet"

        df.to_parquet(
            part_path / filename,
            engine="pyarrow",
            index=False,
            compression="snappy",
        )

    def read(self, dataset: str, filters=None) -> pd.DataFrame:
        dataset_path = self.root / dataset
        print('dataset path: ', dataset_path)
        if not dataset_path.exists():
            raise FileNotFoundError(f"Dataset not found: {dataset_path}")
        return pd.read_parquet(dataset_path, filters=filters)