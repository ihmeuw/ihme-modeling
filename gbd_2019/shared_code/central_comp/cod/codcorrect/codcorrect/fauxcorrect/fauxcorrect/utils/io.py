from os.path import exists, join
from typing import Dict, List, Optional

import pandas as pd

from draw_sources.draw_sources import DrawSink, DrawSource
from dataframe_io.exceptions import InvalidSpec

from fauxcorrect.utils.constants import (
    Keys, Columns, FilePaths, Measures)


def cache_hdf(
        df: pd.DataFrame,
        filepath: str,
        key: str,
        data_columns: List[str] = None
) -> None:
    """
    Cache DataFrame in h5 file.
    Uses fixed format if data_columns are not present.

    Arguments:
        df: the DataFrame to cache
        filepath: where the h5 should be written
        key: the h5 key to write
        data_columns: optional list of data columns with which
            the h5 should be indexed
    """
    df.to_hdf(
        filepath,
        key,
        mode='w',
        format='table' if data_columns else 'fixed',
        data_columns=data_columns
    )


def read_cached_hdf(
        filepath: str,
        key: str,
        where: List[str] = None,
        columns: List[str] = None
) -> pd.DataFrame:
    """
    Read cached DataFrame from h5 file.

    Arguments:
        filepath: where the h5 should be read from
        key: the h5 key to read
        where: optional list of where clauses to use when reading cached h5.
            E.g. ['sex_id==2']
        columns: columns to return. Default all columns

    Returns:
        Cached DataFrame

    Raises:
        FileNotFoundError: if file containing cached DataFrame does not exist
    """
    if not exists(filepath):
        raise FileNotFoundError(f'Cached file does not exist: {filepath}')
    return pd.read_hdf(filepath, key, where=where, columns=columns)


def read_from_draw_source(
    draw_dir: str,
    file_pattern: str,
    h5_tablename: str = Keys.DRAWS,
    num_workers: Optional[int] = 1,
    filters: Optional[Dict[str, int]] = None
) -> pd.DataFrame:

    if not isinstance(num_workers, int):
        raise ValueError(
            f"num_workers must be an int. "
            f"Received {type(num_workers)}.")
    try:
        draws = DrawSource({
            'draw_dir': draw_dir,
            'file_pattern': file_pattern,
            'h5_tablename': h5_tablename,
            'num_workers': num_workers
        }).content(filters=filters)
    except InvalidSpec:
        raise FileNotFoundError(
            f"Draw files were not found for draw_dir {draw_dir}, "
            f"file_pattern {file_pattern}, h5_tablename {h5_tablename} "
            f"and filters {filters}."
        )
    return draws

def read_unaggregated_unscaled_draws(parent_dir: str,
                                     location_id: int,
                                     sex_id: int
) -> pd.DataFrame:
    """Read unaggregated unscaled draws by location and sex."""

    draw_dir = join(
        parent_dir,
        FilePaths.UNAGGREGATED_DIR,
        FilePaths.UNSCALED_DIR,
        FilePaths.DIAGNOSTICS_DIR,
        FilePaths.DEATHS_DIR
    )
    file_pattern = FilePaths.UNAGGREGATED_UNSCALED_FILE_PATTERN.format(
        location_id=location_id, sex_id=sex_id)

    return read_from_draw_source(draw_dir, file_pattern)


def read_unaggregated_rescaled_draws(parent_dir: str,
                                     location_id: int,
                                     sex_id: int
) -> pd.DataFrame:
    """Read unaggregated rescaled draws by location and sex."""

    draw_dir = join(
        parent_dir,
        FilePaths.UNAGGREGATED_DIR,
        FilePaths.RESCALED_DIR,
        FilePaths.DEATHS_DIR
    )
    file_pattern = FilePaths.RESCALED_DRAWS_FILE_PATTERN.format(
        location_id=location_id, sex_id=sex_id, )

    return read_from_draw_source(draw_dir, file_pattern)


def read_unaggregated_shocks_draws(parent_dir: str,
                                   location_id: int,
                                   sex_id: int
) -> pd.DataFrame:
    """Read unaggregated shocks draws by location and sex."""

    draw_dir = join(
        parent_dir,
        FilePaths.UNAGGREGATED_DIR,
        FilePaths.SHOCKS_DIR,
        FilePaths.DEATHS_DIR
    )
    file_pattern = FilePaths.UNAGGREGATED_SHOCKS_FILE_PATTERN
    filters = {Columns.LOCATION_ID: location_id,
               Columns.SEX_ID: sex_id}

    return read_from_draw_source(draw_dir, file_pattern,
                                 filters=filters)


def read_aggregated_rescaled_draws_for_summaries(parent_dir: str,
                                                 location_id: int,
                                                 year_id: int,
                                                 measure_id: int
) -> pd.DataFrame:

    draw_dir = join(
        parent_dir,
        FilePaths.AGGREGATED_DIR,
        FilePaths.RESCALED_DIR)
    file_pattern = FilePaths.SUMMARY_AGGREGATE_READ_PATTERN.format(
        measure_id=measure_id, location_id=location_id, year_id=year_id)
    return read_from_draw_source(draw_dir, file_pattern)


def save_scaled_draws(
        draws: pd.DataFrame,
        parent_dir: str,
        location_id: int,
        sex_id: int
) -> None:
    """
    Save scaled draws for given location and sex while sharding on the year.
    """
    DrawSink({
        'draw_dir': join(
            parent_dir,
            FilePaths.DRAWS_SCALED_DIR,
            FilePaths.DEATHS_DIR
        ),
        'file_pattern': FilePaths.SCALE_DRAWS_FILE_PATTERN.format(
            sex_id=sex_id, location_id=location_id
        ),
        'h5_tablename': Keys.DRAWS
    }).push(draws, append=False)


def save_aggregated_draws(parent_dir: str,
                          draw_type: str,
                          location_id: int,
                          sex_id: int,
                          df: pd.DataFrame
) -> None:
    draw_dir = join(
        parent_dir,
        FilePaths.AGGREGATED_DIR,
        draw_type)
    file_pattern = FilePaths.CAUSE_AGGREGATE_FILE_PATTERN.format(
        measure_id=Measures.Ids.DEATHS,
        sex_id=sex_id,
        location_id=location_id
    )
    sink = DrawSink({
        'draw_dir': draw_dir,
        'file_pattern': file_pattern,
        'h5_tablename': Keys.DRAWS})
    sink.add_transform(add_measure_id_to_sink,
                       measure_id=Measures.Ids.DEATHS)
    sink.push(df, append=False)


def add_measure_id_to_sink(df, measure_id):
    df[Columns.MEASURE_ID] = measure_id
    return df


def sink_draws(
        draw_dir: str,
        file_pattern: str,
        draws: pd.DataFrame
) -> None:
    params = {
        'draw_dir': draw_dir,
        'file_pattern': file_pattern,
        'h5_tablename': Keys.DRAWS
    }
    draw_sink = DrawSink(params)
    draw_sink.push(draws, append=False)


def dataframe_to_csv(df, filepath, save_index=False):
    df.to_csv(filepath, index=save_index)
