import logging
from typing import Dict, List

import numpy as np
import pandas as pd

from codcorrect.lib.workflow import task_templates as tt
from codcorrect.legacy.utils.constants import Columns, Jobmon
from codcorrect.legacy.parameters.machinery import MachineParameters

logger = logging.getLogger(__name__)


def apply_correction(
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
        location_id (int): draws location_id
        sex_id (int): draws sex_id
        version (MachineParameters): machinery parameters for the given
             machine run.

    Raises:
        ValueError: If there are NaNs in draws after removing non-scalable
            draws, spacetime-restricted draws, and draws that are all zeros
    """
    logger.info("Beginning deaths correction.")
    logger.info("Reading helper files")
    spacetime_restrictions, envelope_data, envelope_summ = _read_helper_files(
        location_id, sex_id, version
    )

    logger.info("Reading unscaled draws")
    data = version.file_system.read_unscaled_draws(
        location_id=location_id,
        sex_id=sex_id,
        num_workers=tt.ApplyCorrection.compute_resources[Jobmon.NUM_CORES],
    )

    logger.info("Add envelope summary column")
    data = _add_envelope_summary(data, envelope_summ)
    data = _keep_draws_that_should_be_scaled(data)

    # Filter and preserve zeros, confirm NaNs are gone
    logger.info("Filtering out zeros")
    data, zeros = _filter_zeros(data, version.draw_cols)

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
        on=Columns.INDEX
    ).fillna(0)
    data = _remove_spacetime_restricted_demographics(
        data,
        spacetime_restrictions
    )
    if data.isna().any(axis=None):
        raise ValueError(
            f"There are NaNs in the data\n{data[data.isna().any(axis=1)]}"
        )
    logger.info(
        "Formatting data for rescale, converting to cause fraction space"
    )
    formatted_data = _format_for_rescale(data, version)
    logger.info("Saving formatted, unscaled draws")
    unscaled_data = _convert_to_deaths(
        formatted_data, envelope_data, version.draw_cols)
    unscaled_data = _add_zeros_back(unscaled_data, zeros, version.draw_cols)
    version.file_system.save_pre_scaling_draws(unscaled_data)

    # Rescale data
    logger.info("Rescaling data")
    scaled_data = _rescale_data(formatted_data, version.draw_cols)
    logger.info("Confirming most detailed causes' cause fractions sum to 1")
    _validate_cause_fractions_sum_to_1(
        scaled_data, version.draw_cols, version.most_detailed_cause_ids)
    logger.info("Converting rescaled data to deaths space")
    scaled_data = _convert_to_deaths(
        scaled_data, envelope_data, version.draw_cols)
    logger.info("Adding zeros back")
    scaled_data = _add_zeros_back(scaled_data, zeros, version.draw_cols)
    logger.info("Saving data")
    version.file_system.save_rescaled_draws(scaled_data)
    logger.info("All done!")


def _read_helper_files(
        location_id: int,
        sex_id: int,
        version: MachineParameters
) -> tuple:
    """Read in and return helper DataFrames.

    Arguments:
        location_id (int): draws location_id
        sex_id (int): draws sex_id
        version (MachineParameters): machinery parameters for the given
             machine run.

    Returns:
        spacetime_restrictions: DataFrame containing all spacetime restricted
            cause-location-years
        envelope_data: all-cause mortality envelope draws
        envelope_summ: all-cause mortality envelope summaries
            (mean, upper, lower)
    """
    # Space-time restrictions
    logger.info('Reading spacetime restrictions')
    spacetime_restrictions = version.file_system.read_spacetime_restrictions()

    # Envelope draws
    logger.info('Reading envelope draws')
    envelope_data = version.file_system.read_envelope_draws(
        location_id, sex_id, version.draw_cols
    )

    # Envelope summary
    logger.info('Reading envelope summary')
    envelope_summ = version.file_system.read_envelope_summary(location_id, sex_id=sex_id)

    return (spacetime_restrictions, envelope_data, envelope_summ)


def _add_envelope_summary(df, env):
    """
    CODEm draws should already have an envelope column in them. The CODEm
    draws are saved in death space, with the mean envelope and pop in the
    draws. CoDCorrect first converts the draws to cause fraction space
    using the envelope they were modeled on, which should be in the draw
    files. If there is no envelope column in the draws, add one
    """
    if Columns.ENVELOPE in df:
        if any(np.isnan(df[Columns.ENVELOPE])):
            logger.info("Nans found in envelope column. Dropping.")
            df = df.drop(Columns.ENVELOPE, axis=1)
        else:
            logger.info("Using envelope column from draws file.")
            return df

    merged = pd.merge(
        df,
        env,
        on=Columns.DEMOGRAPHIC_INDEX,
        how='left'
    )
    assert merged[Columns.ENVELOPE].notnull(
    ).all(), 'missing envelope'
    return merged


def _keep_draws_that_should_be_scaled(df: pd.DataFrame) -> pd.DataFrame:
    """Filters data by is_scaled column. Note that no data
    with is_scaled=False should actually be read into this module.
    Shocks/IC have is_scaled=False but should not be read in here.
    CoDCorrect should not apply is_scaled=False.
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
            Columns.CAUSE_ID,
            Columns.LOCATION_ID,
            Columns.YEAR_ID
        ],
        how='left',
        indicator=True
    )
    data = data.loc[data._merge == "left_only"]
    data = data.drop('_merge', axis=1)
    return data


def _filter_and_expand_expected_data(
        version: MachineParameters,
        sex_id: int,
        location_id: List[int]
) -> pd.DataFrame:
    # all metadata expected for full machinery run
    expected_data = version.expected_metadata

    # expected metadata contains IC/Shocks, which we don't need, so
    # filter to only causes that go into the correction.
    # Also filter by the sex_id passed into this script
    correct_cause_ids = version.cause_ids_to_correct
    filtered_expected_data = expected_data.loc[
        (expected_data.cause_id.isin(correct_cause_ids)) &
        (expected_data.sex_id == sex_id)]

    # expand cause_ages column to age_group_ids
    expanded_expected_data = (
        filtered_expected_data.explode(Columns.CAUSE_AGES).rename(
            columns={
                Columns.CAUSE_AGES: Columns.AGE_GROUP_ID
            }
        ).reset_index(drop=True)
    )
    # add location and year ids
    join_key = 1
    expanded_expected_data.loc[:, Columns.JOIN_KEY] = join_key
    loc_and_year_idx = pd.MultiIndex.from_product(
        iterables=[
            location_id,
            version.year_ids,
            [join_key]
        ],
        names=[
            Columns.LOCATION_ID,
            Columns.YEAR_ID,
            Columns.JOIN_KEY
        ]
    )
    loc_and_year = pd.DataFrame(index=loc_and_year_idx).reset_index()
    return (
        pd.merge(
            expanded_expected_data,
            loc_and_year,
            on=Columns.JOIN_KEY,
            how='left'
        ).drop(Columns.JOIN_KEY, axis=1)
    )


def _filter_zeros(df: pd.DataFrame, draw_cols: List[str]):
    zeros = (df[draw_cols] == 0).all(axis=1)
    return df[~zeros].copy(), df[zeros].copy()


def _format_for_rescale(
        data: pd.DataFrame,
        version: MachineParameters
) -> pd.DataFrame:
    """Runs all steps to take the raw data and prepare it to be rescaled."""

    # Keep just the variables we need
    keep = (
        Columns.INDEX + [Columns.ENVELOPE] +
        version.draw_cols
    )
    data = data.loc[:, keep]
    # Convert to cause fractions
    for col in version.draw_cols:
        data[col] = data[col] / data[Columns.ENVELOPE]
        data[col] = data[col].fillna(0)

    # Merge on hierarchy variables
    data = data[data.cause_id.isin(version.correction_hierarchy.cause_id.drop_duplicates())]
    data = pd.merge(data,
                    version.correction_hierarchy,
                    on=Columns.CAUSE_ID, how='left')

    # Create/update 'excluded_from_correction' column
    data = _update_excluded_from_correction_metadata(data, version.correction_exclusion_map)

    # Subset columns to the essentials
    keep = (
        Columns.INDEX +
        Columns.CAUSE_HIERARCHY +
        [Columns.EXCLUDED_FROM_CORRECTION] +
        version.draw_cols
    )
    data = data.loc[:, keep]

    return data


def _update_excluded_from_correction_metadata(
    data: pd.DataFrame,
    correction_exclusion_map: pd.DataFrame,
) -> pd.DataFrame:
    """
    Update (or create) 'excluded_from_correction' column for use within scaling.

    Certain causes for particular or all locations are excluded from correction,
    meaning that they are involved in the scaling process, but are not scaled themselves.
    Other causes are scaled around them.

    Rules:
        * if it doesn't exist in the metadata, add column with 0s everywhere
        * if it does exist, update the value with the cause/location mapping
            such that only locations in the exclusion map are marked to be excluded.
            Causes that are marked with 'excluded_from_correction' but don't exist in the map
            are excluded from scaling in all locations
        * Causes that are not at level 1 cannot be excluded from correction. This is a
            consequence of the implementation
    """
    # If the 'excluded_from_correction' column is missing, tack it on with 0s and continue
    if not Columns.EXCLUDED_FROM_CORRECTION in data:
        data[Columns.EXCLUDED_FROM_CORRECTION] = 0

        return data

    # Separate causes that will potentially be updated from those that will not change
    exclusion_causes_to_update = correction_exclusion_map[Columns.CAUSE_ID].unique().tolist()
    data_to_update = data[data[Columns.CAUSE_ID].isin(exclusion_causes_to_update)]
    unchanged_data = data[~data[Columns.CAUSE_ID].isin(exclusion_causes_to_update)]

    # Merge the exclusion map onto data to be updated, filling in 0s where no cause-loc pair
    # exists in the map. 'excluded_from_correction_x' will be dropped later
    updated_data = pd.merge(
        data_to_update,
        correction_exclusion_map,
        on=[Columns.CAUSE_ID, Columns.LOCATION_ID],
        suffixes=("_x", ""),
        how="left",
    ).fillna(0)

    data = pd.concat([updated_data, unchanged_data], sort=False)

    # Check invariant that all excluded causes are at level 1
    excluded_causes_not_at_level_1 = data[
        (data[Columns.LEVEL] != 1) & (data[Columns.EXCLUDED_FROM_CORRECTION] == 1)
    ]
    if not excluded_causes_not_at_level_1.empty:
        raise RuntimeError(
            f"{len(excluded_causes_not_at_level_1)} non-level 1 row(s) in the correction "
            "hierarchy marked to be excluded from correction, which is currently not allowed "
            ". Excluded causes must be at level 1. Example rows:\n"
            f"{excluded_causes_not_at_level_1.head()}"
        )

    return data


def _rescale_data(data: pd.DataFrame, draw_cols: List[str]) -> pd.DataFrame:
    """Rescales data to make it internally consistent within hierarchy.

    First, takes DataFrame and rescale to 1 within each level of the index
    columns, 'relative' rescaling.

    Then runs down cause hierarchy and rescales each level according to the
    parent, 'absolute' rescaling.

    Note:
        Data MUST BE IN CAUSE FRACTION space.

    Returns:
        a scaled DataFrame
    """
    # Perform 'relative' rescale, where cause fractions for each parent cause sum to 1
    data = _rescale_relative(data, draw_cols)

    # Propagate scaling down levels, 'absolute' scaling
    for level in (range(data[Columns.LEVEL].min() + 1, data[Columns.LEVEL].max() + 1)):
        data = _rescale_absolute(data, level, draw_cols)

    return data


def _rescale_relative(data: pd.DataFrame, draw_cols: List[str]) -> pd.DataFrame:
    """Perform the relative rescale for causes across all levels.

    Takes DataFrame and rescales to 1 within each level of the group by
    columns, 'relative' rescaling. Accounts for 'rock' causes, which are excluded from
    scaling and thus do not change in value.

    Formula:
        (Cause X / sum of included causes with same parent cause/level as cause X) *
        (1 - sum of excluded causes with same parent cause/level as cause X)
    
    Example:
        Causes A, B, and C have same parent cause at any level of the cause hierarchy
        
        Unscaled:

          A  |  B  |  C  |  Sum
        -----|-----|-----|-------
         0.5 | 0.2 | 0.4 |  1.1

        Relatively scaled:

          A  |  B  |  C  |  Sum
        -----|-----|-----|-------
         0.45| 0.18| 0.36|  1.0
    
    Returns:
        Relatively scaled data frame
    """
    # Separate causes to be scaled from those excluded from correction
    to_scale = data[data[Columns.EXCLUDED_FROM_CORRECTION] == 0]
    excluded = data[data[Columns.EXCLUDED_FROM_CORRECTION] == 1]

    # Ensure no data is lost, primarily a check that the exclusion metadata is as expected
    if len(to_scale) + len(excluded) != len(data):
        raise RuntimeError(
            "After splitting data into 'to_scale', 'excluded', rows do not add as expected:"
            f" {len(to_scale)} (scaled) + {len(excluded)} (excluded) != {len(data)} (data)"
        )

    # Make cause fraction sums across groupby_columns for both scaled and non-scaled
    groupby_columns = Columns.DEMOGRAPHIC_INDEX + Columns.CAUSE_HIERARCHY
    to_scale_sums_columns = {f"{draw_col}": f"{draw_col}_total" for draw_col in draw_cols}
    excluded_sums_columns = {f"{draw_col}": f"{draw_col}_excluded" for draw_col in draw_cols}

    # Compute sums
    to_scale_sums = (
        to_scale[groupby_columns + draw_cols]
        .groupby(groupby_columns)[draw_cols]
        .sum()
        .reset_index()
        .rename(columns=to_scale_sums_columns)
    )
    excluded_sums = (
        excluded[groupby_columns + draw_cols]
        .groupby(groupby_columns)[draw_cols]
        .sum()
        .reset_index()
        .rename(columns=excluded_sums_columns)
    )

    _validate_parent_is_0_if_children_sum_to_0(
        to_scale, to_scale_sums, draw_cols, to_scale_sums_columns
    )

    # Set 0 to-be-scaled cause fraction sums to 1; we can't divide by 0
    # 1 here is arbitrary. If the sum is 0, all values in the grouping must be 0 and 0 / X = 0
    for col in to_scale_sums_columns.values():
        to_scale_sums.loc[to_scale_sums[col] == 0, col] = 1

    # Merge all draws together:
    #   * to-scale sums match to-scale indexes (inner join)
    #   * Excluded sums are a subset of the to-scale indexes, including a possibly empty df.
    #     If there's no excluded causes for a demographic + parent we'll have NaNs; set to 0
    all_data = (
        pd.merge(to_scale, to_scale_sums, on=groupby_columns, how="inner")
        .pipe(pd.merge, excluded_sums, on=groupby_columns, how="left")
        .fillna(0)
    )

    # Relative scaling: scale cause fractions to parent cause/level sum,
    # accounting for 'rock' causes excluded from scaling if there are any
    for col in draw_cols:
        all_data[col] = (
            all_data[col] / all_data[f"{col}_total"] * (1 - all_data[f"{col}_excluded"])
        )

    rescaled = pd.concat([all_data, excluded], sort=False)

    return rescaled[Columns.INDEX + Columns.CAUSE_HIERARCHY + draw_cols]


def _rescale_absolute(data: pd.DataFrame, level: int, draw_cols: List[str]) -> pd.DataFrame:
    """Perform absolute rescaling to ensure child data is internally consistent with parent.

    This function is called once for every level of the hierarchy.

    It subsets out all data for that level, & merges on the parent cause data.
    Then it overwrites the child adjusted values with the product of the parent
    and child adjusted values so that the data is internally consistent.

    Example:
        Causes A, B, and C have same parent cause at any level of cause hierarchy >= 1
        
        Relatively scaled:

          A  |  B  |  C  |  Sum  |  Absolutely scaled parent cause
        -----|-----|-----|-------|----------------------------------
         0.45| 0.18| 0.36|  1.0  |            0.4

        Absolutely Scaled:

          A  |  B  |  C  |  Sum  |  Absolutely scaled parent cause
        -----|-----|-----|-------|----------------------------------
         0.18| 0.07| 0.14|  ~0.4 |            0.4
    
    Returns:
        Relatively scaled data frame

    Returns:
        Entire DataFrame with adjusted child data.
    """
    # Create dictionary for renaming column in the parent df and merge columns
    parent_columns = {f"{draw_col}": f"{draw_col}_parent" for draw_col in draw_cols}
    parent_columns[Columns.CAUSE_ID] = Columns.PARENT_ID

    child = data.loc[data[Columns.LEVEL] == level].copy(deep=True)
    parent = (
        data.loc[data[Columns.CAUSE_ID].isin(child[Columns.PARENT_ID].unique())]
        .copy(deep=True)[Columns.INDEX + draw_cols]
        .rename(columns=parent_columns)
    )

    # Merge parent envelope onto child draws
    both = pd.merge(
        child, parent, on=Columns.DEMOGRAPHIC_INDEX + [Columns.PARENT_ID]
    )

    # Absolute scaling: multiply relative child cfs with absolute parent cf envelopes
    for col in draw_cols:
        both[col] = both[col] * both[f"{col}_parent"]

    # Drop draw_X_parent columns
    del parent_columns[Columns.CAUSE_ID]
    both = both.drop(parent_columns.values(), axis=1)

    return pd.concat([data.loc[data[Columns.LEVEL] != level], both], sort=False)


def _validate_parent_is_0_if_children_sum_to_0(
    to_scale: pd.DataFrame,
    to_scale_sums: pd.DataFrame,
    draw_cols: List[str],
    to_scale_sums_columns: Dict[str, str],
) -> None:
    """Validate that the parent cause is 0 if the sum of the child causes is 0.

    Otherwise, the parent cause deaths will be lost during absolute scaling, leading
    to the sum of cause fractions for the most detailed causes being less than 1.
    If this error is hit, the parent cause model(s) must be adjusted so that they
    are also 0 where the child cause models are 0.

    Example, relatively scaled cause fractions:

          Parent  |  Child 1  |  Child 2
        ----------|-----------|-----------
           0.4    |     0     |     0
    
        After absolute scaling, the parent's 0.4 cause fraction will not be spread between
        the two child causes. It is lost.

    The opposite case, where the parent is zero but the children sum to non-zero,
    is allowed. During absolute scaling, the parent's 0 values override the children's
    non-zero values.
    """
    sums_draw_cols = [f"{col}_total" for col in draw_cols]
    to_scale_sums[(to_scale_sums[sums_draw_cols] == 0).any(axis=1)]

    zero_child_sums = (
        to_scale_sums.rename(columns={v: k for k, v in to_scale_sums_columns.items()})
        .melt(
            id_vars=Columns.DEMOGRAPHIC_INDEX + ["parent_id"],
            value_vars=draw_cols,
            var_name="draw",
            value_name="child_cf_sum"
        )
        .query("child_cf_sum == 0")
    )
    parent = to_scale.melt(
        id_vars=Columns.DEMOGRAPHIC_INDEX + ["cause_id"],
        value_vars=draw_cols,
        var_name="draw",
        value_name="parent_cf"
    ).rename(columns={"cause_id": "parent_id"})

    zero_child_sums_and_parent = pd.merge(
        zero_child_sums,
        parent,
        on=Columns.DEMOGRAPHIC_INDEX + ["parent_id", "draw"],
        how="left",
    )

    non_zero_parent = zero_child_sums_and_parent.query("parent_cf > 0")
    if not non_zero_parent.empty:
        raise RuntimeError(
            f"Before relative scaling, found {len(non_zero_parent)} demographics/parent "
            "causes/draws where the cause fraction sum of the child causes is 0 but the "
            "parent's cause fraction is non-zero. During scaling, the parent cause deaths "
            "would be lost, leading to cause fractions not summing to 1 across most "
            f"detailed causes.\n{non_zero_parent}"
        )


def _validate_cause_fractions_sum_to_1(
    data: pd.DataFrame,
    draw_cols: List[str],
    most_detailed_cause_ids: List[int],
) -> None:
    """Validates the cause fractions by demographic sum to 1 across most detailed causes.

    If the sum is less than/greater than 1, then we have artificially deleted/added deaths
    to the non-shock envelope. This should not happen, so we throw an error.
    """
    most_detailed = data[data[Columns.CAUSE_ID].isin(most_detailed_cause_ids)]

    # Compute sums
    sums = (
        most_detailed[Columns.DEMOGRAPHIC_INDEX + draw_cols]
        .groupby(Columns.DEMOGRAPHIC_INDEX)[draw_cols]
        .sum()
        .reset_index()
    )

    invalid_rows = sums[(abs(sums[draw_cols] - 1) > 1e-8).any(axis=1)]
    if not invalid_rows.empty:
        raise RuntimeError(
            f"{len(invalid_rows)} demographic(s) have cause fractions for most detailed "
            "causes that do not sum to 1 for at least 1 draw. Example:\n"
            f"{invalid_rows.head()}"
        )


def _convert_to_deaths(data, envelope, draw_cols):
    """Multiplies death draws by envelope."""
    envelope_data = (
        envelope[Columns.DEMOGRAPHIC_INDEX + draw_cols]
    )
    # rename envelope draws, ex.'draw_1_env'
    envelope_data = (
        envelope_data.rename(columns={
            d: '{}_env'.format(d) for d in draw_cols
        })
    )
    data = pd.merge(data, envelope_data,
                    on=Columns.DEMOGRAPHIC_INDEX,
                    how='left',
                    indicator=True)
    if not (data._merge == "both").all():
        raise ValueError(
            "There are scaled draws not matched to all-cause mortality draws")
    data = data.drop('_merge', axis=1)
    # Convert to death space
    for col in draw_cols:
        data[col] = (data[col] * data['{}_env'.format(col)])
    # Drop old columns
    data = data.drop(
        ['{}_env'.format(d) for d in draw_cols],
        axis=1
    )

    return data


def _add_zeros_back(data, zeros, draw_cols):
    # Add true zeros (post-restriction) back in
    df = pd.concat([data, zeros], sort=False).reset_index(drop=True)
    df = df[Columns.INDEX + draw_cols].groupby(
        Columns.INDEX).sum().reset_index()
    return df
