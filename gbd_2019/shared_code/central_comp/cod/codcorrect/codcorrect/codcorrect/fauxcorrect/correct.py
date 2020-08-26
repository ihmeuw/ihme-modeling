import logging
from os.path import join
import pandas as pd

from typing import Dict, List, Tuple

from dataframe_io.exceptions import InvalidSpec
from draw_sources.draw_sources import DrawSink, DrawSource

from fauxcorrect.queries.spacetime_restrictions import (
    get_all_spacetime_restrictions
)
from fauxcorrect.utils import constants, io
from fauxcorrect.parameters.master import MachineParameters


def cache_spacetime_restrictions(parent_dir: str, gbd_round_id: int) -> None:
    """
    Cache regional scalars for a codcorrect run.
    Save in h5 table format queryable by location_id, sex_id, and year_id
    since spacetime restrictions will be parallelized along those values.

    Arguments:
        parent_dir: parent codcorrect directory
            e.g. PATH/{version}
        gbd_round_id: GBD round ID
    """
    restrictions_path = _get_spacetime_restrictions_path(parent_dir)
    restrictions = get_all_spacetime_restrictions(gbd_round_id)
    data_cols = [
        constants.Columns.CAUSE_ID,
        constants.Columns.LOCATION_ID,
        constants.Columns.YEAR_ID
    ]
    io.cache_hdf(
        restrictions,
        restrictions_path,
        constants.Keys.SPACETIME_RESTRICTIONS,
        data_cols
    )


def apply_correction(
        parent_dir: str,
        location_id: int,
        sex_id: int,
        version: MachineParameters
) -> None:
    """
    Apply deaths correction to draws.
    Draws are stored broken down by location and sex for parallel execution.

        (1) Reads in best model CoD draws
        (2) Converts to cause fraction space
        (3) Rescales so cause fractions add up to 1 for a given level-parent_id
            group
        (4) Adjusts cause fractions based on the parent_id
        (5) Multiplies cause fractions by latest version of the envelope to get
            corrected death numbers

    Arguments:
        parent_dir (str): parent codcorrect directory
            e.g. PATH/{version}
        location_id (int): draws location_id
        sex_id (int): draws sex_id
        version (MachineParameters): master parameters for the given
             machine run.

    Raises:
        ValueError: If there are NaNs in draws after removing non-scalable
            draws, spacetime-restricted draws, and draws that are all zeros
    """
    logging.info("Beginning deaths correction.")
    logging.info("Reading helper files")
    (best_models, spacetime_restrictions, envelope_data,
        envelope_summ) = _read_helper_files(parent_dir,
                                            location_id,
                                            sex_id,
                                            version)
    logging.info("Reading unscaled draws")
    data = _read_unscaled_draws(parent_dir, location_id, sex_id)
    logging.info("Add envelope summary column")
    data = _add_envelope_summary(data, envelope_summ)
    data = _keep_draws_that_should_be_scaled(data)
    # Filter and preserve zeros, confirm NaNs are gone
    logging.info("Filtering out zeros")
    data, zeros = _filter_zeros(data)
    # Merge expected data and fill missing demographics with zeros
    expected_data = _filter_and_expand_expected_data(
        version,
        sex_id,
        [location_id]
    )
    data = pd.merge(
        expected_data,
        data,
        how='left',
        on=constants.Columns.INDEX
    ).fillna(0)
    data = _remove_spacetime_restricted_demographics(
        data,
        spacetime_restrictions
    )
    if data.isna().any(axis=None):
        raise ValueError(
            f"There are NaNs in the data\n{data[data.isna().any(axis=1)]}"
    )
    logging.info(
        "Formatting data for rescale, converting to cause fraction space"
    )
    formatted_data = _format_for_rescale(data, version)
    logging.info("Saving formatted, unscaled draws")
    unscaled_data = _convert_to_deaths(formatted_data, envelope_data)
    unscaled_data = _add_zeros_back(unscaled_data, zeros)
    _save_unscaled_draws(unscaled_data, parent_dir, location_id, sex_id)
    # Rescale data
    logging.info("Rescaling data")
    scaled_data = _rescale_data(formatted_data)
    logging.info("Converting rescaled data to deaths space")
    scaled_data = _convert_to_deaths(scaled_data, envelope_data)
    logging.info("Adding zeros back")
    scaled_data = _add_zeros_back(scaled_data, zeros)
    logging.info("Saving data")
    _save_rescaled_draws(scaled_data, parent_dir, location_id, sex_id)
    logging.info("All done!")


def _get_spacetime_restrictions_path(parent_dir: str) -> str:
    """Get path to cached spacetime restrictions"""
    return join(
        parent_dir,
        constants.FilePaths.INPUT_FILES_DIR,
        constants.FilePaths.SPACETIME_RESTRICTIONS
    )


def _read_spacetime_restrictions(parent_dir: str) -> pd.DataFrame:
    """
    Read all spacetime restrictions.
    There aren't many of them, so it's fine to keep all of them in memory.
    """
    return io.read_cached_hdf(
        _get_spacetime_restrictions_path(parent_dir),
        constants.Keys.SPACETIME_RESTRICTIONS,
        columns=[
            constants.Columns.CAUSE_ID,
            constants.Columns.LOCATION_ID,
            constants.Columns.YEAR_ID
        ]
    )


def _get_envelope_draws_path(parent_dir: str) -> str:
    """
    Get path to cached all-cause mortality envelope draws
    """
    return join(
        parent_dir,
        constants.FilePaths.INPUT_FILES_DIR,
        constants.FilePaths.ENVELOPE_DRAWS_FILE
    )


def _get_envelope_summary_path(parent_dir: str) -> str:
    """
    Get path to cached all-cause mortality envelope summary
    """
    return join(
        parent_dir,
        constants.FilePaths.INPUT_FILES_DIR,
        constants.FilePaths.ENVELOPE_SUMMARY_FILE
    )


def _read_envelope_draws(
        parent_dir: str,
        location_id: int,
        sex_id: int
) -> pd.DataFrame:
    """Read in envelope draws filtered by location_id and sex_id."""
    envelope_filter = [
        f'{constants.Columns.LOCATION_ID}=={location_id}',
        f'{constants.Columns.SEX_ID}=={sex_id}'
    ]
    envelope_columns = (
        constants.Columns.DEMOGRAPHIC_INDEX + constants.Columns.DRAWS
    )
    envelope_path = _get_envelope_draws_path(parent_dir)
    envelope_data = io.read_cached_hdf(
        envelope_path,
        constants.Keys.ENVELOPE_DRAWS,
        envelope_filter,
        envelope_columns
    )
    return envelope_data


def _read_envelope_summary(
        parent_dir: str,
        location_id: int,
        sex_id: int
) -> pd.DataFrame:
    """Read in envelope summary filtered by location_id and sex_id."""
    envelope_filter = [
        f'{constants.Columns.LOCATION_ID}=={location_id}',
        f'{constants.Columns.SEX_ID}=={sex_id}'
    ]
    envelope_columns = (
        constants.Columns.DEMOGRAPHIC_INDEX + constants.Columns.ENVELOPE_DRAWS
    )
    envelope_path = _get_envelope_summary_path(parent_dir)
    envelope_summ = io.read_cached_hdf(
        envelope_path,
        constants.Keys.ENVELOPE_SUMMARY,
        envelope_filter,
        envelope_columns
    )
    return envelope_summ


def _read_helper_files(
        parent_dir: str,
        location_id: int,
        sex_id: int,
        version: MachineParameters
) -> tuple:
    """Read in and return helper DataFrames.

    Arguments:
        parent_dir (str): parent codcorrect directory
            e.g. PATH/{version}
        location_id (int): draws location_id
        sex_id (int): draws sex_id
        version (MachineParameters): master parameters for the given
             machine run.

    Returns:
        best_models: DataFrame containing all best model ids
            and relevant cause metadata for a given sex
        spacetime_restrictions: DataFrame containing all spacetime restricted
            cause-location-years
        envelope_data: all-cause mortality envelope draws
        envelope_summ: all-cause mortality envelope summaries
            (mean, upper, lower)
    """
    # List of best models (excluding shocks)
    logging.info('Reading best models')
    best_models = version.best_model_metadata
    best_models = best_models.loc[(best_models['sex_id'] == int(sex_id)) &
                                  (best_models['model_version_type_id']
                                  .isin(list(range(0, 5))))]

    # Space-time restrictions
    logging.info('Reading spacetime restrictions')
    spacetime_restrictions = _read_spacetime_restrictions(parent_dir)

    # Envelope
    logging.info('Reading envelope draws')
    envelope_data = _read_envelope_draws(
        parent_dir,
        location_id,
        sex_id
    )
    logging.info('Reading envelope summary')
    envelope_summ = _read_envelope_summary(
        parent_dir,
        location_id,
        sex_id
    )

    return (
        best_models,
        spacetime_restrictions,
        envelope_data,
        envelope_summ
    )


def _read_unscaled_draws(
        parent_dir: str,
        location_id: int,
        sex_id: int
) -> pd.DataFrame:
    """Read unscaled draws for given location, sex, and year"""
    draw_dir = join(
        parent_dir,
        constants.FilePaths.UNAGGREGATED_DIR,
        constants.FilePaths.UNSCALED_DIR,
        constants.FilePaths.DEATHS_DIR
    )
    file_pattern = constants.FilePaths.UNSCALED_DRAWS_FILE_PATTERN

    try:
        draws = DrawSource({
            'draw_dir': draw_dir,
            'file_pattern': file_pattern,
            'h5_tablename': constants.Keys.DRAWS,
            'num_workers': constants.DAG.Tasks.Cores.APPLY_CORRECTION
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


def _add_envelope_summary(df, env):
    """CODEm draws should already have an envelope column in them. The CODEm
    draws are saved in death space, with the mean envelope and pop in the
    draws. CoDCorrect first converts the draws to cause fraction space
    using the envelope they were modeled on, which should be in the draw
    files. If there is no envelope column in the draws, add one"""
    if constants.Columns.ENVELOPE in df:
        return df
    merged = pd.merge(
        df,
        env,
        on=constants.Columns.DEMOGRAPHIC_INDEX,
        how='left'
    )
    assert merged[constants.Columns.ENVELOPE].notnull().all(), 'missing envelope'
    return merged


def _keep_draws_that_should_be_scaled(df: pd.DataFrame) -> pd.DataFrame:
    """Filters data by is_scaled column. Note that no data
    with is_scaled=False should actually be read into this module.
    Shocks/HIV/IC have is_scaled=False but should not be read in here.
    FauxCorrect will apply is_scaled=False to new causes but CoDCorrect
    should not.
    """
    return df.loc[df.is_scaled == True, :]


def _remove_spacetime_restricted_demographics(
        df: pd.DataFrame,
        spacetime_restrictions: pd.DataFrame
) -> pd.DataFrame:
    """Remove spacetime restricted cause-location-years from data so they do
    not contribute to scaling step"""
    data = pd.merge(
        df,
        spacetime_restrictions,
        on=[
            constants.Columns.CAUSE_ID,
            constants.Columns.LOCATION_ID,
            constants.Columns.YEAR_ID
        ],
        how='left',
        indicator=True
    )
    data = data.loc[data._merge=="left_only"]
    data = data.drop('_merge', axis=1)
    return data


def _filter_and_expand_expected_data(
    version: MachineParameters,
    sex_id: int,
    location_id: List[int]
) -> pd.DataFrame:
    # all metadata expected for full machinery run
    expected_data = version.expected_metadata
    # expected metadata contains HIC/IC/Shocks, which we don't need, so
    # filter to causes represented in the CoDCorrect cause hierarchy only.
    # also filter by the sex_id passed into this script
    hierarchy = (
        version._cause_parameters[constants.CauseSetId.CODCORRECT].hierarchy
    )
    correct_cause_ids = hierarchy.loc[
        hierarchy.level > 0][constants.Columns.CAUSE_ID].tolist()
    filtered_expected_data = expected_data.loc[
        (expected_data.cause_id.isin(correct_cause_ids)) &
        (expected_data.sex_id == sex_id)]
    # expand cause_ages column to age_group_ids
    expanded_expected_data = (
        filtered_expected_data.explode(
            constants.Columns.CAUSE_AGES).rename(
            columns={
                constants.Columns.CAUSE_AGES: constants.Columns.AGE_GROUP_ID
            }
        ).reset_index(drop=True)
    )
    # add location and year ids
    join_key = 1
    expanded_expected_data.loc[:, constants.Columns.JOIN_KEY] = join_key
    loc_and_year_idx = pd.MultiIndex.from_product(
        iterables=[
            location_id,
            version.year_ids,
            [join_key]
        ],
        names=[
            constants.Columns.LOCATION_ID,
            constants.Columns.YEAR_ID,
            constants.Columns.JOIN_KEY
        ]
    )
    loc_and_year = pd.DataFrame(index=loc_and_year_idx).reset_index()
    return (
        pd.merge(
            expanded_expected_data,
            loc_and_year,
            on=constants.Columns.JOIN_KEY,
            how='left'
        ).drop(constants.Columns.JOIN_KEY, axis=1)
    )


def _filter_zeros(df: pd.DataFrame):
    zeros = (df[constants.Columns.DRAWS] == 0).all(axis=1)
    return df[~zeros].copy(), df[zeros].copy()


def _format_for_rescale(
        data: pd.DataFrame,
        version: MachineParameters
) -> pd.DataFrame:
    """Runs all steps to take the raw data and prepare it to be rescaled."""

    # Keep just the variables we need
    keep = (
        constants.Columns.INDEX + [constants.Columns.ENVELOPE] +
        constants.Columns.DRAWS
    )
    data = data.loc[:, keep]
    # Convert to cause fractions
    for col in constants.Columns.DRAWS:
        data[col] = data[col] / data[constants.Columns.ENVELOPE]
        data[col] = data[col].fillna(0)
    # Merge on hierarchy variables
    hierarchy = (
        version._cause_parameters[constants.CauseSetId.CODCORRECT].hierarchy
    )
    data = pd.merge(data,
                    hierarchy,
                    on=constants.Columns.CAUSE_ID, how='left')
    keep = (
        constants.Columns.INDEX + constants.Columns.CAUSE_HIERARCHY +
        constants.Columns.DRAWS
    )
    data = data.loc[:, keep]

    return data


def _rescale_data(data):
    """Rescales data to make it internally consistent within hierarchy.

    First, takes DataFrame and rescale to 1 within each level of the index
    columns.

    Then runs down cause hierarchy and rescales each level according to the
    parent.

    Returns a scaled DataFrame
    """
    # Rescale
    data = _rescale_group(data,
                         constants.Columns.DEMOGRAPHIC_INDEX +
                         constants.Columns.CAUSE_HIERARCHY,
                         constants.Columns.DRAWS)
    # Propagate down levels
    for level in (range(data[constants.Columns.LEVEL].min() +
                        1, data[constants.Columns.LEVEL].max() + 1)):
        data = _rescale_to_parent(data, level)

    return data


def _rescale_group(data, groupby_columns, data_columns):
    """Rescale column to 1

    Takes a set of columns and rescales to 1 in groups defined by the groupby
    columns.

    NOTE: the intermediate total column CANNOT have a value of 0 or else this
          can cause problems aggregating up the hierarchy later. If this
          happens, make the all values within that group equal and resume.
    """
    # Make totals
    temp = data[groupby_columns + data_columns].copy(deep=True)
    temp = temp.groupby(groupby_columns)[data_columns].sum().reset_index()
    rename_columns = {'{}'.format(d): '{}_total'.format(d)
                      for d in data_columns}
    temp = temp.rename(columns=rename_columns)
    # Attempt to rescale
    data = pd.merge(data, temp, on=groupby_columns)
    for data_column in data_columns:
        data[data_column] = (data[data_column] /
                             data['{}_total'.format(data_column)])
    data = data.drop(['{}_total'.format(d) for d in data_columns], axis=1)
    # Fill in problem cells
    data[data_columns] = data[data_columns].fillna(1)
    # Remake totals
    temp = data[groupby_columns + data_columns].copy(deep=True)
    temp = temp.groupby(groupby_columns)[data_columns].sum().reset_index()
    temp = temp.rename(columns=rename_columns)
    # Rescale
    data = pd.merge(data, temp, on=groupby_columns)
    for data_column in data_columns:
        data[data_column] = (data[data_column] /
                             data['{}_total'.format(data_column)])
    data = data.drop(['{}_total'.format(d) for d in data_columns], axis=1)
    return data


def _rescale_to_parent(data, level):
    """Rescales child data to be internally consistent with parent data.

    This function is called once for every level of the hierarchy.

    It subsets out all data for that level, & merges on the parent cause data.
    Then it overwrites the child adjusted values with the product of the parent
    and child adjusted values so that the data is internally consistent.

    Data MUST BE IN CAUSE FRACTION space in order for this to work.

    Returns: Entire DataFrame with adjusted child data.
    """
    parent_keep_columns = constants.Columns.INDEX + constants.Columns.DRAWS
    merge_columns = (
        constants.Columns.DEMOGRAPHIC_INDEX + [constants.Columns.PARENT_ID]
    )
    temp_child = (
        data.loc[data[constants.Columns.LEVEL] == level].copy(deep=True)
    )
    temp_parent = (
        data.loc[data[constants.Columns.CAUSE_ID].isin(
            temp_child[constants.Columns.PARENT_ID]
            .drop_duplicates())].copy(deep=True)
    )
    temp_parent = temp_parent.loc[:, parent_keep_columns]

    parent_rename_columns = {'cause_id': 'parent_id'}
    for col in constants.Columns.DRAWS:
        parent_rename_columns[col] = '{}_parent'.format(col)
    temp_parent = temp_parent.rename(columns=parent_rename_columns)

    temp_child = pd.merge(temp_child, temp_parent, on=merge_columns)

    for col in constants.Columns.DRAWS:
        temp_child[col] = (
            temp_child[col] * temp_child['{}_parent'.format(col)]
        )
        temp_child.drop('{}_parent'.format(col), axis=1, inplace=True)

    return pd.concat(
        [data.loc[data[constants.Columns.LEVEL] != level],
        temp_child],
        sort=False
    )


def _convert_to_deaths(data, envelope):
    """Multiplies death draws by envelope."""
    envelope_data = (
        envelope[constants.Columns.DEMOGRAPHIC_INDEX + constants.Columns.DRAWS]
    )
    # rename envelope draws, ex.'draw_1_env'
    envelope_data = (
        envelope_data.rename(columns={
            d: '{}_env'.format(d) for d in constants.Columns.DRAWS
        })
    )
    data = pd.merge(data, envelope_data,
                    on=constants.Columns.DEMOGRAPHIC_INDEX,
                    how='left',
                    indicator=True)
    if not (data._merge == "both").all():
        raise ValueError(
            "There are scaled draws not matched to all-cause mortality draws")
    data = data.drop('_merge', axis=1)
    # Convert to death space
    for col in constants.Columns.DRAWS:
        data[col] = (data[col] * data['{}_env'.format(col)])
    # Drop old columns
    data = data.drop(
        ['{}_env'.format(d) for d in constants.Columns.DRAWS],
        axis=1
    )

    return data


def _add_zeros_back(data, zeros):
    # Add true zeros (post-restriction) back in
    df = pd.concat([data, zeros], sort=False).reset_index(drop=True)
    df = df[constants.Columns.INDEX + constants.Columns.DRAWS].groupby(
        constants.Columns.INDEX).sum().reset_index()
    return df


def _save_rescaled_draws(
        draws: pd.DataFrame,
        parent_dir: str,
        location_id: int,
        sex_id: int
) -> None:
    """Saves rescaled draws in an h5 file for given location and sex."""
    DrawSink({
        'draw_dir': join(
            parent_dir,
            constants.FilePaths.UNAGGREGATED_DIR,
            constants.FilePaths.RESCALED_DIR,
            constants.FilePaths.DEATHS_DIR
        ),
        'file_pattern': constants.FilePaths.RESCALED_DRAWS_FILE_PATTERN.format(
            location_id=location_id, sex_id=sex_id
        ),
        'h5_tablename': constants.Keys.DRAWS
    }).push(draws, append=False)


def _save_unscaled_draws(
        draws: pd.DataFrame,
        parent_dir: str,
        location_id: int,
        sex_id: int
) -> None:
    """Saves rescaled draws in an h5 file for given location and sex."""
    DrawSink({
        'draw_dir': join(
            parent_dir,
            constants.FilePaths.UNAGGREGATED_DIR,
            constants.FilePaths.UNSCALED_DIR,
            constants.FilePaths.DIAGNOSTICS_DIR,
            constants.FilePaths.DEATHS_DIR
        ),
        'file_pattern': (
            constants.FilePaths.UNAGGREGATED_UNSCALED_FILE_PATTERN.format(
                location_id=location_id, sex_id=sex_id
            )
        ),
        'h5_tablename': constants.Keys.DRAWS
    }).push(draws, append=False)
