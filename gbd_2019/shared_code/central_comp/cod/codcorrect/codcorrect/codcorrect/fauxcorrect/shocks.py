import argparse
import logging
import os
import pandas as pd
from typing import List, Optional, Tuple
import sys

from draw_sources.draw_sources import DrawSource, DrawSink
from fauxcorrect.utils.constants import (
    Columns,
    FilePaths,
    GBD,
    LocationAggregation,
    Measures
)
from fauxcorrect.utils import io


def append_shocks(
        parent_dir: str,
        machine_process: str,
        measure_ids: List[int],
        location_id: int,
        most_detailed_location: bool,
        sex_id: int
) -> None:
    """
    Add yll and death shocks (location-aggregated) to re/scaled ylls and
    re/scaled deaths (also location-aggregated).
    Draws are stored broken down by location and sex for parallel execution.

    Arguments:
        parent_dir (str):
        machine_process (str):
        measure_ids (list): the measure_ids included in this run
        location_id (int): draws location_id
        most_detailed_location (bool):
        sex_id (int): draws sex_id

    """
    scaled_dir, shocks_dir = _get_input_filepaths(
        parent_dir,
        machine_process,
        most_detailed_location
    )
    input_file_pattern = FilePaths.APPEND_SHOCKS_FILE_PATTERN.format(
        sex_id=sex_id, location_id=location_id)
    # Deaths
    if Measures.Ids.DEATHS in measure_ids:
        scaled_params = {
            'draw_dir': os.path.join(scaled_dir, FilePaths.DEATHS_DIR),
            'file_pattern': input_file_pattern
        }
        scaled_ds = DrawSource(scaled_params)
        scaled = scaled_ds.content(
            filters={
                Columns.LOCATION_ID: location_id,
                Columns.SEX_ID: sex_id,
                Columns.MEASURE_ID: Measures.Ids.DEATHS
            }
        )
        shock_params = {
            'draw_dir': os.path.join(shocks_dir, FilePaths.DEATHS_DIR),
            'file_pattern': input_file_pattern
        }
        shock_ds = DrawSource(shock_params)
        shocks = shock_ds.content(
            filters={
                Columns.LOCATION_ID: location_id,
                Columns.SEX_ID: sex_id,
                Columns.MEASURE_ID: Measures.Ids.DEATHS
            }
        )
        new_scaled = _append_shocks(scaled, shocks)
    else:
        new_scaled = None

    # YLLS
    if Measures.Ids.YLLS in measure_ids:
        scaled_yll_params = {
            'draw_dir': os.path.join(scaled_dir, FilePaths.YLLS_DIR),
            'file_pattern': input_file_pattern
        }
        scaled_yll_ds = DrawSource(scaled_yll_params)
        scaled_ylls = scaled_yll_ds.content(
            filters={
                Columns.LOCATION_ID: location_id,
                Columns.SEX_ID: sex_id,
                Columns.MEASURE_ID: Measures.Ids.YLLS
            }
        )
        shock_yll_params = {
            'draw_dir': os.path.join(shocks_dir, FilePaths.YLLS_DIR),
            'file_pattern': input_file_pattern
        }
        shock_yll_ds = DrawSource(shock_yll_params)
        shock_ylls = shock_yll_ds.content(
            filters={
                Columns.LOCATION_ID: location_id,
                Columns.SEX_ID: sex_id,
                Columns.MEASURE_ID: Measures.Ids.YLLS
            }
        )
        new_scaled_ylls = _append_shocks(scaled_ylls, shock_ylls)
    else:
        new_scaled_ylls = None

    save_map = {
        GBD.Process.Name.CODCORRECT: _save_all_codcorrect_outputs,
        GBD.Process.Name.FAUXCORRECT: _save_all_fauxcorrect_outputs
    }
    save_map[machine_process](
        parent_dir,
        new_scaled,
        new_scaled_ylls,
        location_id,
        sex_id
    )


def _append_shocks(
        scaled_data: pd.DataFrame,
        shock_data: pd.DataFrame
) -> pd.DataFrame:
    """ Given a deaths/yll dataframe, add shocks to it """
    df = pd.concat([scaled_data, shock_data]).reset_index(drop=True)
    df = (
        df[Columns.INDEX + [Columns.MEASURE_ID] + Columns.DRAWS]
        .groupby(Columns.INDEX + [Columns.MEASURE_ID])
        .sum()
        .reset_index()
    )
    return df


def _get_input_filepaths(
        parent_dir: str,
        machine_process: str,
        most_detailed_location: int
) -> Tuple[str, str]:
    if machine_process == GBD.Process.Name.CODCORRECT:
        scaled_dir = os.path.join(
            parent_dir,
            LocationAggregation.Type.CAUSE_AGGREGATED_RESCALED
        )
        shocks_dir = os.path.join(
            parent_dir,
            LocationAggregation.Type.CAUSE_AGGREGATED_SHOCKS
        )
    elif machine_process == GBD.Process.Name.FAUXCORRECT:
        scaled_dir = os.path.join(
            parent_dir,
            LocationAggregation.Type.SCALED
        )
        shocks_dir = os.path.join(
            parent_dir,
            LocationAggregation.Type.UNAGGREGATED_SHOCKS
        )
        if not most_detailed_location:
            shocks_dir = os.path.join(shocks_dir, FilePaths.LOCATION_AGGREGATES)
    else:
        raise ValueError(
            f'--machine_process argument must be '
            f'"{GBD.Process.Name.CODCORRECT}" or '
            f'"{GBD.Process.Name.FAUXCORRECT}". '
            f'Recieved "{machine_process}".'
        )
    return scaled_dir, shocks_dir


def _save_all_codcorrect_outputs(
        parent_dir: str,
        new_scaled: Optional[pd.DataFrame],
        new_scaled_ylls: Optional[pd.DataFrame],
        location_id: int,
        sex_id: int
) -> None:
    file_pattern = (
        FilePaths.APPEND_SHOCKS_FILE_PATTERN.format(
            sex_id=sex_id, location_id=location_id
        )
    )
    # Deaths
    if new_scaled is not None:
        deaths_dir = os.path.join(
            parent_dir,
            FilePaths.DRAWS_DIR,
            FilePaths.DEATHS_DIR
        )
        io.sink_draws(deaths_dir, file_pattern, new_scaled)
    # YLLs
    if new_scaled_ylls is not None:
        ylls_dir = os.path.join(
            parent_dir,
            FilePaths.DRAWS_DIR,
            FilePaths.YLLS_DIR
        )
        io.sink_draws(ylls_dir, file_pattern, new_scaled_ylls)


def _save_all_fauxcorrect_outputs(
        parent_dir: str,
        new_scaled: Optional[pd.DataFrame],
        new_scaled_ylls: Optional[pd.DataFrame],
        location_id: int,
        sex_id: int
) -> None:
    file_pattern = (
        constants.FilePaths.APPEND_SHOCKS_FILE_PATTERN.format(
            sex_id=sex_id, location_id=location_id
        )
    )
    if new_scaled is not None:
        deaths_dir = os.path.join(
            parent_dir,
            FilePaths.DRAWS_SCALED_DIR,
            FilePaths.DEATHS_DIR
        )
        io.sink_draws(deaths_dir, file_pattern, new_scaled)
    # YLLs
    if new_scaled_ylls is not None:
        ylls_dir = os.path.join(
            parent_dir,
            FilePaths.DRAWS_SCALED_DIR,
            FilePaths.YLLS_DIR
        )
        io.sink_draws(ylls_dir, file_pattern, new_scaled_ylls)
