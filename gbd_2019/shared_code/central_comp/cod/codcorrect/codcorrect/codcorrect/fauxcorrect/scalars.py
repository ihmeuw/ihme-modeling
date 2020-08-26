import logging
from os.path import join
import pandas as pd

from dataframe_io.exceptions import InvalidSpec
from draw_sources.draw_sources import DrawSink, DrawSource

from fauxcorrect.backfill import lookup_location_or_cause
from fauxcorrect.utils import constants, io


def apply_scalars(
        parent_dir: str,
        location_id: int,
        sex_id: int,
        year_ids: int,
        scalar_version_id: int
) -> None:
    """
    Apply scalars to draws.
    Draws are stored broken down by location and sex for parallel execution.

    Arguments:
        parent_dir (str): parent fauxcorrect directory
            e.g. PATH/{version}
        location_id (int): draws location_id
        sex_id (int): draws sex_id
        year_ids (List[int]): year_ids
        scalar_version_id (int): source CoDCorrect version for scalars

    Raises:
        ValueError: if draws do not have matching scalars
    """
    logging.info("Beginning scalar application.")
    scalar_location = lookup_location_or_cause(
        parent_dir=parent_dir, loc_or_cause_id=location_id,
        is_loc_backfill=True
    )
    logging.info("Reading unscaled draws.")
    draws = _read_unscaled_draws(parent_dir, location_id, sex_id)

    if scalar_location != constants.SpecialMappings.NO_SCALE_VALUE:
        logging.info("scalar_location is valid, reading in scalars.")
        scalars = _read_scalars_filtered(
            scalar_location, sex_id, year_ids, scalar_version_id, location_id
        )

        exempt_draws = draws[~draws.is_scaled]
        draws = draws[draws.is_scaled]

        draws = pd.merge(draws, scalars, on=constants.Columns.INDEX, how='left')

        logging.info("Applying 0.0 scalar to missing demographics.")
        draws.loc[
            draws[constants.Columns.SCALAR].isnull(),
            constants.Columns.SCALAR
        ] = 0.0

        nan_draws = draws[draws.isnull().any(axis=1)]
        if not nan_draws.empty:
            raise ValueError(
                'Found draws without matching scalars for draws:\n'
                f'{nan_draws[constants.Columns.INDEX]}'
            )

        logging.info("Applying scalars.")
        draws[constants.Columns.DRAWS] = draws[constants.Columns.DRAWS].mul(
            draws[constants.Columns.SCALAR], axis=0
        )
        draws.drop(constants.Columns.SCALAR, axis=1, inplace=True)

        draws = pd.concat([draws, exempt_draws]).reset_index(drop=True)

    logging.info("Drop is_scaled column before saving.")
    draws.drop(
        labels=constants.Columns.IS_SCALED,
        axis='columns',
        inplace=True
    )
    logging.info("Saving scaled draws to disk.")
    _save_scaled_draws(draws, parent_dir, location_id, sex_id)


def _get_scalars_path(scalar_version_id: int) -> str:
    """
    Get the file path for the summary scalars for a CoDCorrect run

    Files are saved in h5 table format queryable by location_id, sex_id, and
    year_id since scalar application will be parallelized along those values.

    Arguments:
        scalar_version_id (int): source CoDCorrect version for scalars
    """
    return join(
        constants.FilePaths.CODCORRECT_ROOT_DIR.format(
            version_id=scalar_version_id
        ),
        constants.FilePaths.SCALAR_SOURCE_DIR,
        constants.FilePaths.SCALARS
    )


def _read_scalars_filtered(
        scalar_location_id: int,
        sex_id: int,
        year_ids: int,
        scalar_version_id: int,
        draw_location_id: int
) -> pd.DataFrame:
    """Read scalars filtered by location_id, sex_id, and year_id"""
    scalars_filter = [
        f'{constants.Columns.LOCATION_ID}=={scalar_location_id}',
        f'{constants.Columns.SEX_ID}=={sex_id}',
        f'{constants.Columns.YEAR_ID} in {year_ids}'
    ]
    scalars_columns = constants.Columns.INDEX + [constants.Columns.SCALAR]
    scalars_path = _get_scalars_path(scalar_version_id)
    scalar_data = io.read_cached_hdf(
        scalars_path, constants.Keys.SCALARS, scalars_filter, scalars_columns
    )
    scalar_data[constants.Columns.LOCATION_ID] = draw_location_id
    return scalar_data


def _read_unscaled_draws(
        parent_dir: str,
        location_id: int,
        sex_id: int
) -> pd.DataFrame:
    """Read unscaled draws for given location, sex, and year"""
    draw_dir = join(
        parent_dir,
        constants.FilePaths.DRAWS_UNSCALED_DIR,
        constants.FilePaths.DEATHS_DIR
    )
    file_pattern = constants.FilePaths.UNSCALED_DRAWS_FILE_PATTERN

    try:
        draws = DrawSource({
            'draw_dir': draw_dir,
            'file_pattern': file_pattern,
            'h5_tablename': constants.Keys.DRAWS,
            'num_workers': constants.DAG.Tasks.Cores.APPLY_SCALARS
        }).content(filters={
            constants.Columns.LOCATION_ID: location_id,
            constants.Columns.SEX_ID: sex_id
        })
    except InvalidSpec:
        raise FileNotFoundError(
            f"Draw files were not found for location: {location_id} and sex: "
            f"{sex_id}."
        )
    return draws


def _save_scaled_draws(
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
            constants.FilePaths.DRAWS_SCALED_DIR,
            constants.FilePaths.DEATHS_DIR
        ),
        'file_pattern': constants.FilePaths.SCALE_DRAWS_FILE_PATTERN.format(
            sex_id=sex_id, location_id=location_id
        ),
        'h5_tablename': constants.Keys.DRAWS
    }).push(draws, append=False)
