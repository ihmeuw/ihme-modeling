from pathlib import Path
from typing import Any, List, Tuple, Union
import warnings

from covid_shared.shell_tools import mkdir
import pandas as pd
import yaml

from covid_model_seiir_pipeline.lib.io.keys import (
    DatasetKey,
    MetadataKey,
)

POTENTIAL_INDEX_COLUMNS = ['location_id', 'date', 'age_start']


class CSVMarshall:
    """Marshalls DataFrames to/from CSV files."""
    # interface methods
    @classmethod
    def dump(cls, data: pd.DataFrame, key: DatasetKey, strict: bool = True) -> None:
        path, _ = cls._resolve_key(key)

        if strict and path.exists():
            msg = f"Cannot dump data for key {key} - would overwrite"
            raise LookupError(msg)

        write_index = bool(set(data.index.names).intersection(POTENTIAL_INDEX_COLUMNS))
        data.to_csv(path, index=write_index)

    @classmethod
    def load(cls, key: DatasetKey) -> pd.DataFrame:
        path, columns = cls._resolve_key(key)
        data = pd.read_csv(path, usecols=columns)
        # Use list comp to keep ordering consistent.
        index_cols = [c for c in POTENTIAL_INDEX_COLUMNS if c in data.columns]
        if 'date' in index_cols:
            data['date'] = pd.to_datetime(data['date'])
        if index_cols:
            data = data.set_index(index_cols).sort_index()
        return data

    @classmethod
    def touch(cls, *paths: Path) -> None:
        for path in paths:
            mkdir(path, parents=True, exists_ok=True)

    @classmethod
    def exists(cls, key: DatasetKey) -> bool:
        path, _ = cls._resolve_key(key)
        return path.exists()

    @classmethod
    def _resolve_key(cls, key: DatasetKey) -> Tuple[Path, Union[List[str], None]]:
        path = key.root
        if key.prefix:
            path /= key.prefix
        path /= key.data_type
        if key.leaf_name:
            path /= key.leaf_name
        return path.with_suffix(".csv"), key.columns


class ParquetMarshall:
    """Marshalls DataFrames to/from Parquet files."""
    # interface methods
    @classmethod
    def dump(cls, data: pd.DataFrame, key: DatasetKey, strict: bool = True) -> None:
        path, _ = cls._resolve_key(key)

        if strict and path.exists():
            msg = f"Cannot dump data for key {key} - would overwrite"
            raise LookupError(msg)

        data.to_parquet(path, engine='fastparquet', compression='gzip')

    @classmethod
    def load(cls, key: DatasetKey) -> pd.DataFrame:
        path, columns = cls._resolve_key(key)
        with warnings.catch_warnings():
            # Super noisy parquet warning that doesn't matter
            warnings.simplefilter('ignore')
            data = pd.read_parquet(path, columns=columns, engine='fastparquet')
        return data

    @classmethod
    def touch(cls, *paths: Path) -> None:
        for path in paths:
            mkdir(path, parents=True, exists_ok=True)

    @classmethod
    def exists(cls, key: DatasetKey) -> bool:
        path, _ = cls._resolve_key(key)
        return path.exists()

    @classmethod
    def _resolve_key(cls, key: DatasetKey) -> Tuple[Path, Union[List[str], None]]:
        path = key.root
        if key.prefix:
            path /= key.prefix
        path /= key.data_type
        if key.leaf_name:
            path /= key.leaf_name
        return path.with_suffix(".parquet"), key.columns


class YamlMarshall:
    """Marshalls primitive python data structures to and from yaml."""

    @classmethod
    def dump(cls, data: Any, key: MetadataKey, strict: bool = True) -> None:
        path = cls._resolve_key(key)
        if strict and path.exists():
            msg = f"Cannot dump data for key {key} - would overwrite"
            raise LookupError(msg)

        with path.open('w') as file:
            yaml.dump(data, file, sort_keys=False)

    @classmethod
    def load(cls, key: MetadataKey) -> Any:
        path = cls._resolve_key(key)
        with path.open() as file:
            data = yaml.full_load(file)
        return data

    @classmethod
    def exists(cls, key: MetadataKey) -> bool:
        path = cls._resolve_key(key)
        return path.exists()

    @classmethod
    def _resolve_key(cls, key: MetadataKey) -> Path:
        path = (key.root / key.data_type).with_suffix(".yaml")
        return path


DATA_STRATEGIES = {
    'csv': CSVMarshall,
    'parquet': ParquetMarshall,
}
METADATA_STRATEGIES = {
    'yaml': YamlMarshall,
}
STRATEGIES = {**DATA_STRATEGIES, **METADATA_STRATEGIES}
