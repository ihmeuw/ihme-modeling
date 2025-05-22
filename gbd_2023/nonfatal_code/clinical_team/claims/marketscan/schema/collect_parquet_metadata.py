"""
Run this each time the ICD-mart is updated. It will produce 2 files. A compiled metadata
file from every parquet in the ICD-mart and a stats file which is used by the pipeline jobmon
workflow to allocate resources."""

from pathlib import Path
from typing import List, Union

import pandas as pd
import pyarrow.parquet as pq
from pyarrow._parquet import FileMetaData

from marketscan.schema import config


def create_file_lookup(file: Path, row_group: int, num_rows: int) -> pd.DataFrame:
    """Pull metadata from parquet for a single file."""
    parts = [f for f in file.parts if "=" in f]
    file_df = pd.DataFrame({"row_group": row_group, "num_rows": num_rows}, index=[row_group])
    for p in parts:
        partition = p.split("=")
        file_df[partition[0]] = int(partition[1])

    return file_df


def glob_files() -> List[Path]:
    """Get a list of ICD Mart files."""
    settings = config.get_settings()
    icd_mart_base = "FILEPATH"

    return [p for p in Path(icd_mart_base).rglob("*.parquet")]


def collect_metadata(files: List[Path]) -> Union[list, pd.DataFrame]:
    """Collect metadata into a list and a dataframe for a given set of parquet paths."""
    metadata_collector = []

    file_list = []
    row_group = 0
    for file in files:
        parquet_file = pq.ParquetFile(file)
        metadata_collector.append(parquet_file.metadata)
        file_list.append(
            create_file_lookup(
                file=file, row_group=row_group, num_rows=parquet_file.metadata.num_rows
            )
        )
        row_group += 1

    file_df = pd.concat(file_list, sort=False, ignore_index=False)
    return metadata_collector, file_df


def compile_metadata(metadata_collector: List[FileMetaData]) -> FileMetaData:
    """Combine metadata row groups."""
    metadata = metadata_collector[0]
    for _meta in metadata_collector[1:]:
        metadata.append_row_groups(_meta)
    return metadata


if __name__ == "__main__":
    metadata_collector, file_df = collect_metadata(glob_files())

    print("metadata collected and stats df created")
    metadata = compile_metadata(metadata_collector)
    print("metadata compiled")

    if metadata.num_row_groups != len(metadata_collector):
        raise RuntimeError("We expect 1 row group per metadata file")
    if len(file_df) != len(file_df.drop_duplicates()):
        raise RuntimeError("There are unexpected duplicates in the file_df object")

    print("metadata and stats df validated. Writing now")
    settings = config.get_settings()
    metadata.write_metadata_file("FILEPATH")
    file_df.to_parquet("FILEPATH")