"""Inteface methods for on disk I/O.

These methods use information from the provided key to select an appropriate
strategy for streaming data to and from disk.

"""
from typing import Any, Union

from covid_model_seiir_pipeline.lib.io.keys import (
    DatasetKey,
    MetadataKey,
)
from covid_model_seiir_pipeline.lib.io.data_roots import DataRoot
from covid_model_seiir_pipeline.lib.io.marshall import STRATEGIES


def load(key: Union[MetadataKey, DatasetKey]) -> Any:
    """Loads the dataset associated with the provided key."""
    if key.disk_format not in STRATEGIES:
        raise
    return STRATEGIES[key.disk_format].load(key)


def dump(dataset: Any, key: Union[MetadataKey, DatasetKey]) -> None:
    """Writes the provided dataset to the location represented by the key."""
    if key.disk_format not in STRATEGIES:
        raise
    STRATEGIES[key.disk_format].dump(dataset, key)


def exists(key: Union[MetadataKey, DatasetKey]) -> bool:
    """Returns whether a dataset is found at key's location."""
    if key.disk_format not in STRATEGIES:
        raise
    return STRATEGIES[key.disk_format].exists(key)


def touch(data_root: DataRoot, **prefix_args):
    """Generates the subdirectory structure associated with the data root."""
    if data_root._data_format not in STRATEGIES:
        raise
    STRATEGIES[data_root._data_format].touch(*data_root.terminal_paths(**prefix_args))

