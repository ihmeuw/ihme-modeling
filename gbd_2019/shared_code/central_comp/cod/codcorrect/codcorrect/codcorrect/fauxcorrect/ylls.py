from os.path import exists, join
import logging

import pandas as pd

from draw_sources.draw_sources import DrawSink, DrawSource

from fauxcorrect.queries.pred_ex import get_pred_ex
from fauxcorrect.utils.constants import Columns, FilePaths, GBD, Keys, Measures
from fauxcorrect.utils.io import (
    add_measure_id_to_sink,
    cache_hdf,
    read_cached_hdf,
    read_from_draw_source
)


def cache_pred_ex(parent_dir: str, gbd_round_id: int) -> None:
    """
    Cache predicted life expectancy for a machinery run.
    Save in h5 table format queryable by location_id, sex_id, and year_id
    since YLL calculation will be parallelized along those values.

    Arguments:
        parent_dir (str): parent fauxcorrect directory
            e.g. PATH/{version}
        gbd_round_id (int): GBD round ID
    """

    pred_ex_path = _get_pred_ex_path(parent_dir)
    pred_ex = get_pred_ex(gbd_round_id)
    data_cols = [Columns.LOCATION_ID, Columns.SEX_ID, Columns.YEAR_ID]
    cache_hdf(pred_ex, pred_ex_path, Keys.PRED_EX, data_cols)


def calculate_ylls(
        machine_process: str,
        parent_dir: str,
        location_id: int,
        sex_id: int
) -> None:
    """
    Use scaled draws and predicted life expectancy to calculate YLLs.
    Draws are stored broken down by location and sex for parallel execution.

    Arguments:
        machine_process (str): either codcorrect or fauxcorrect
        parent_dir (str): parent directory
            e.g. PATH/{version}
        location_id (int): draws location_id
        sex_id (int): draws sex_id

    Raises:
        ValueError: if draws do not have matching predicted life expectancy
    """
    pred_ex = _read_pred_ex_filtered(parent_dir, location_id, sex_id)
    read_deaths_map = {
        GBD.Process.Name.CODCORRECT: _read_scaled_aggregated_codcorrect_draws,
        GBD.Process.Name.FAUXCORRECT: _read_scaled_fauxcorrect_draws
    }
    read_shocks_map = {
        GBD.Process.Name.CODCORRECT: _read_aggregated_shock_draws,
        GBD.Process.Name.FAUXCORRECT: _read_unaggregated_shock_draws
    }
    draws = read_deaths_map[machine_process](parent_dir, location_id, sex_id)
    shocks = read_shocks_map[machine_process](parent_dir, location_id, sex_id)

    yll_index_cols = [col for col in Columns.INDEX if col != Columns.CAUSE_ID]
    logging.info("Calculating YLLs for non-shocks")
    ylls = _perform_calculation(pred_ex, draws, yll_index_cols)
    logging.info("Calculating YLLs for shocks")
    yll_shocks = _perform_calculation(pred_ex, shocks, yll_index_cols)
    save_map = {
        GBD.Process.Name.CODCORRECT: _save_all_ylls_codcorrect,
        GBD.Process.Name.FAUXCORRECT: _save_all_ylls_fauxcorrect
    }
    save_map[machine_process](ylls, yll_shocks, parent_dir, location_id, sex_id)


def _perform_calculation(pred_ex, draws, yll_index_cols):
    """Multiplies pred_ex by every death draw, indexed by loc/year/age/sex."""
    merged = pd.merge(draws, pred_ex, on=yll_index_cols, how='left')
    nan_draws = merged[merged.isnull().any(axis=1)]
    if not nan_draws.empty:
        raise ValueError(
            f'Found draws without matching predicted life expectancy '
            f'for draws:\n{nan_draws[yll_index_cols]}'
        )

    merged[Columns.DRAWS] = merged[Columns.DRAWS].mul(
        merged[Columns.PRED_EX], axis=0
    )

    merged.drop(Columns.PRED_EX, axis=1, inplace=True)
    return merged


def _get_pred_ex_path(parent_dir: str) -> str:
    """Get path to cached predicted life expectancy."""
    return join(parent_dir, FilePaths.INPUT_FILES_DIR, FilePaths.PRED_EX)


def _read_pred_ex_filtered(
        parent_dir: str,
        location_id: int,
        sex_id: int
) -> pd.DataFrame:
    """
    Read predicted life expectancy filtered by location_id, sex_id, and year_id
    """
    pred_ex_filter = [
        f'{Columns.LOCATION_ID}=={location_id}',
        f'{Columns.SEX_ID}=={sex_id}'
    ]
    pred_ex_columns = Columns.INDEX + [Columns.PRED_EX]
    pred_ex_path = _get_pred_ex_path(parent_dir)
    return read_cached_hdf(
        pred_ex_path, Keys.PRED_EX, pred_ex_filter, pred_ex_columns
    )


def _read_scaled_aggregated_codcorrect_draws(
        parent_dir: str,
        location_id: int,
        sex_id: int
) -> pd.DataFrame:
    """Read scaled, aggregated codcorrect draws for given location and sex"""
    draw_dir = join(
        parent_dir, FilePaths.AGGREGATED_DIR,
        FilePaths.RESCALED_DIR
    )
    file_pattern = FilePaths.CAUSE_AGGREGATE_FILE_PATTERN.format(
        measure_id=Measures.Ids.DEATHS,
        sex_id=sex_id,
        location_id=location_id
    )

    return read_from_draw_source(draw_dir, file_pattern)


def _read_scaled_fauxcorrect_draws(
        parent_dir: str,
        location_id: int,
        sex_id: int
) -> pd.DataFrame:
    """Read scaled fauxcorrect draws for given location, sex, and year"""
    draw_dir = join(
        parent_dir, FilePaths.DRAWS_SCALED_DIR, FilePaths.DEATHS_DIR
    )
    file_pattern = FilePaths.SCALE_DRAWS_FILE_PATTERN.format(
        sex_id=sex_id, location_id=location_id
    )
    return read_from_draw_source(draw_dir, file_pattern)


def _read_aggregated_shock_draws(
        parent_dir: str,
        location_id: int,
        sex_id: int
) -> pd.DataFrame:
    """Read aggregated shock draws for given location and sex"""
    draw_dir = join(
        parent_dir,
        FilePaths.AGGREGATED_DIR,
        FilePaths.SHOCKS_DIR
    )
    file_pattern = FilePaths.CAUSE_AGGREGATE_FILE_PATTERN.format(
        measure_id=Measures.Ids.DEATHS,
        sex_id=sex_id,
        location_id=location_id
    )
    return read_from_draw_source(draw_dir, file_pattern)


def _read_unaggregated_shock_draws(
        parent_dir: str,
        location_id: int,
        sex_id: int
) -> pd.DataFrame:

    draw_dir = os.path.join(
        parent_dir,
        FilePaths.UNAGGREGATED_DIR,
        FilePaths.SHOCKS_DIR,
        FilePaths.DEATHS_DIR
    )
    file_pattern = FilePaths.UNSCALED_DRAWS_FILE_PATTERN
    filters={
            constants.Columns.LOCATION_ID: location_id,
            constants.Columns.SEX_ID: sex_id
    }

    return read_from_draw_source(
        draw_dir,
        file_pattern,
        constants.DAG.Tasks.Cores.YLLS,
        filters
    )


def _save_all_ylls_codcorrect(
        ylls: pd.DataFrame,
        yll_shocks: pd.DataFrame,
        parent_dir: str,
        location_id: int,
        sex_id: int,
        measure_id: int = 4
) -> None:
    """Save YLLs for given location and sex"""
    ylls_sink = DrawSink({
        'draw_dir': join(
            parent_dir,
            FilePaths.AGGREGATED_DIR,
            FilePaths.RESCALED_DIR,
            FilePaths.YLLS_DIR
        ),
        'file_pattern': (
            FilePaths.YLL_DRAWS_FILE_PATTERN.format(
                sex_id=sex_id, location_id=location_id
            )
        ),
        'h5_tablename': Keys.DRAWS
    })
    ylls_sink.add_transform(add_measure_id_to_sink, measure_id=measure_id)
    ylls_sink.push(ylls, append=False)

    shocks_sink = DrawSink({
        'draw_dir': join(
            parent_dir,
            FilePaths.AGGREGATED_DIR,
            FilePaths.SHOCKS_DIR,
            FilePaths.YLLS_DIR
        ),
        'file_pattern': (
            FilePaths.YLL_DRAWS_FILE_PATTERN.format(
                sex_id=sex_id, location_id=location_id
            )
        ),
        'h5_tablename': Keys.DRAWS
    })
    shocks_sink.add_transform(add_measure_id_to_sink, measure_id=measure_id)
    shocks_sink.push(yll_shocks, append=False)


def _save_all_ylls_fauxcorrect(
        ylls: pd.DataFrame,
        yll_shocks: pd.DataFrame,
        parent_dir: str,
        location_id: int,
        sex_id: int,
        measure_id: int = 4
) -> None:
    """Save YLLs for given location and sex"""
    ylls_sink = DrawSink({
        'draw_dir': join(
            parent_dir, FilePaths.DRAWS_SCALED_DIR, FilePaths.YLLS_DIR
        ),
        'file_pattern': FilePaths.YLL_DRAWS_FILE_PATTERN.format(
            sex_id=sex_id, location_id=location_id
        ),
        'h5_tablename': Keys.DRAWS
    })
    ylls_sink.add_transform(add_measure_id_to_sink, measure_id=measure_id)
    ylls_sink.push(ylls, append=False)

    shocks_sink = DrawSink({
        'draw_dir': join(
            parent_dir,
            FilePaths.UNAGGREGATED_DIR,
            FilePaths.SHOCKS_DIR,
            FilePaths.YLLS_DIR
        ),
        'file_pattern': FilePaths.YLL_DRAWS_FILE_PATTERN.format(
            sex_id=sex_id, location_id=location_id
        ),
        'h5_tablename': Keys.DRAWS
    })
    shocks_sink.add_transform(add_measure_id_to_sink, measure_id=measure_id)
    shocks_sink.push(yll_shocks, append=False)
