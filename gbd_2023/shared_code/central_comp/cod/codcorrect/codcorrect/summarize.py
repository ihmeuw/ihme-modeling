import logging
import numpy as np
import pandas as pd
from typing import List

from db_queries import get_age_weights
import gbd.constants as gbd
from hierarchies.dbtrees import agetree
from core_maths.summarize import pct_change

from codcorrect.legacy.utils.constants import Causes, Columns, DataBases, SummaryType
from codcorrect.lib.validations import data_validations
from codcorrect.legacy.parameters.machinery import MachineParameters

logger = logging.getLogger(__name__)


def summarize(
        release_id: int,
        location_id: int,
        measure_id: int,
        year_id: int,
        version: MachineParameters
) -> None:
    # Get draw data
    logger.info("Read in scaled draws from disk.")
    scaled_estimates = version.file_system.read_final_draws(location_id, year_id, measure_id)
    data_validations.check_duplicates(scaled_estimates, subset=Columns.INDEX)
    if Columns.MEASURE_ID in scaled_estimates:
        scaled_estimates = scaled_estimates.drop(Columns.MEASURE_ID, axis=1)

    scaled_estimates = prep_summarize_gbd(
        scaled_estimates,
        release_id,
        location_id,
        year_id,
        version
    )

    logger.info("Summarizing data.")
    summaries = _generate_summaries(scaled_estimates, DataBases.GBD, version.draw_cols)
    # Add in measure_id
    summaries[Columns.MEASURE_ID] = measure_id
    # Division by 0 can create inf, replace all inf with na and replace
    # na with 0
    summaries = summaries.replace([np.inf, -np.inf], np.nan).fillna(0)
    data_validations.check_duplicates(summaries, subset=Columns.INDEX + [Columns.METRIC_ID])
    logger.info("Saving summaries.")
    _save_summaries(
        summaries,
        location_id=location_id,
        year_id=year_id,
        measure_id=measure_id,
        version=version,
    )


def summarize_cod(
        release_id: int,
        location_id: int,
        year_id: int,
        version: MachineParameters
) -> None:
    measure_id = gbd.measures.DEATH
    logger.info("Read in scaled draws with shocks from disk.")
    data_with_shocks = version.file_system.read_final_draws(location_id, year_id, measure_id)
    data_validations.check_duplicates(data_with_shocks, subset=Columns.INDEX)
    if Columns.MEASURE_ID in data_with_shocks:
        data_with_shocks = data_with_shocks.drop(Columns.MEASURE_ID, axis=1)

    logger.info("Read in scaled draws without shocks from disk.")
    data_no_shocks = version.file_system.read_aggregated_rescaled_draws(
        location_id, year_id=year_id
    )
    data_validations.check_duplicates(data_no_shocks, subset=Columns.INDEX)
    if Columns.MEASURE_ID in data_no_shocks:
        data_no_shocks = data_no_shocks.drop(Columns.MEASURE_ID, axis=1)

    logger.info("Read in population.")
    population = version.file_system.read_population(location_id, year_id)
    population = _compute_population_aggregates(
        population, version.aggregate_age_group_ids, release_id
    )

    df_dict = {'data_with_shocks': data_with_shocks,
               'data_no_shocks': data_no_shocks}
    summaries_dict = {}
    for df_name in df_dict:
        df = df_dict[df_name]
        # create sex_id 3
        logger.info(f"Compute sex aggregates for {df_name}.")
        sex_aggregate = _compute_sex_aggregate(df)
        df = pd.concat(
            [df, sex_aggregate],
            sort=True
        ).reset_index(drop=True)
        # create all age
        logger.info(f"Compute age aggregates for {df_name}.")
        age_aggregate = _compute_age_aggregates(
            df, version.aggregate_age_group_ids, release_id
        )
        # merge on population
        logger.info("Merge population on demographic indices.")
        df = _merge_population(df, population)
        # create age standardized
        logger.info(f"Compute age standardized for {df_name}.")
        age_standardized_rates = _compute_age_standardized_rate(
            df,
            release_id=release_id,
            draw_cols=version.draw_cols
        ).drop(Columns.POPULATION, axis=1)
        # Drop population column, add in our age aggregates before
        # calculating cause fractions for DB
        logger.info(f"Add age aggregates to {df_name}.")
        df = df.drop(Columns.POPULATION, axis=1)
        df = pd.concat(
            [df, age_aggregate],
            sort=True
        ).reset_index(drop=True)

        logger.info(f"Compute cause fractions for {df_name}.")
        cause_fractions = _compute_cause_fractions_codcorrect(
            df, draw_cols=version.draw_cols)

        # add age-standardized to our count-space df
        logger.info(f"Add age standardized to {df_name}.")
        df = pd.concat([df, age_standardized_rates], sort=True)
        rename = {Columns.COD_MEAN: Columns.CAUSE_FRACTION_MEAN,
                  Columns.COD_LOWER: Columns.CAUSE_FRACTION_LOWER,
                  Columns.COD_UPPER: Columns.CAUSE_FRACTION_UPPER}
        rename_values = list(rename.values())
        logger.info("Summarizing data.")
        df_summary = _generate_summaries(df, DataBases.COD, version.draw_cols)[
            Columns.INDEX + list(rename.keys())]
        cf_summary = _generate_summaries(
            cause_fractions, DataBases.COD, version.draw_cols).rename(
            columns=rename)[Columns.INDEX + rename_values]
        df_summary = df_summary.merge(
            cf_summary, on=Columns.INDEX, how='left')
        df_summary[rename_values] = df_summary[rename_values].fillna(0)
        summaries_dict[df_name] = df_summary
    summary_val_cols = Columns.COD_SUMMARY + Columns.CAUSE_FRACTION_SUMMARY
    shocks_rename_cols = [col + '_with_shocks' for col in summary_val_cols]
    rename = dict(zip(summary_val_cols, shocks_rename_cols))
    summaries_dict['data_with_shocks'] = (
        summaries_dict['data_with_shocks'].rename(
            columns=rename)
    )
    summary = pd.merge(summaries_dict['data_no_shocks'],
                       summaries_dict['data_with_shocks'],
                       on=Columns.INDEX, how='outer')
    # there will be NaNs for mean/upper/lower_death for any shocks.
    summary[shocks_rename_cols] = (
        summary[shocks_rename_cols].fillna(0))
    if _is_most_detailed_location(location_id, version):
        model_version_ids = _get_model_version_ids(version)
        summary = summary.merge(
            model_version_ids,
            on=[Columns.CAUSE_ID, Columns.SEX_ID, Columns.AGE_GROUP_ID],
            how='left')
        summary[Columns.MODEL_VERSION_ID] = summary[
            Columns.MODEL_VERSION_ID].fillna(0)
    else:
        summary[Columns.MODEL_VERSION_ID] = 0

    # Add in measure_id
    summary[Columns.MEASURE_ID] = measure_id
    # Division by 0 can create inf, replace all inf with na and replace
    # na with 0
    summary = summary.replace([np.inf, -np.inf], np.nan).fillna(0)
    data_validations.check_duplicates(summary, subset=Columns.INDEX)
    logger.info("Saving summaries.")
    _save_cod_summaries(
        summary,
        location_id=location_id,
        year_id=year_id,
        version=version,
    )


def prep_summarize_gbd(
        df: pd.DataFrame,
        release_id: int,
        location_id: int,
        year_id: int,
        version: MachineParameters
) -> pd.DataFrame:
    # Read population from disk
    logger.info("Read in population cache")
    population = version.file_system.read_population(location_id, year_id)
    population = _compute_population_aggregates(
        population, version.aggregate_age_group_ids, release_id
    )

    # Compute sex aggregates
    logger.info("Compute sex aggregates and combine with scaled estimates.")
    sex_aggregate = _compute_sex_aggregate(df)
    df = pd.concat(
        [df, sex_aggregate],
        sort=True
    ).reset_index(drop=True)

    # Compute age aggregates
    logger.info("Compute age aggregates and combine with scaled estimates.")
    age_aggregate = _compute_age_aggregates(df, version.aggregate_age_group_ids, release_id)

    # Compute ASR
    # First add a metric id to the existing scaled estimates,
    # then merge on a population column, then compute ASR
    logger.info("Compute age standardized rate.")
    df[Columns.METRIC_ID] = gbd.metrics.NUMBER
    logger.info("Merge population on demographic indices.")
    df = _merge_population(df, population)
    age_standardized_rates = _compute_age_standardized_rate(
        df,
        release_id=release_id,
        draw_cols=version.draw_cols
    )

    # Compute GBD rates
    # Drop pop column from scaled estimates, add a metric_id to the age
    # aggregates, combine with scaled estimates,
    # and then merge on a new population before computing rates for the DB
    logger.info("Compute GBD rates.")
    df = df.drop(Columns.POPULATION, axis=1)
    age_aggregate[Columns.METRIC_ID] = gbd.metrics.NUMBER
    df = pd.concat(
        [df, age_aggregate],
        sort=True
    ).reset_index(drop=True)
    # merge on a new population column that is not age or sex restricted
    df = _merge_population(df, population)
    # Do not add back into the unscaled data
    rate_estimates = _compute_rates(df, version.draw_cols)

    logger.info("Compute GBD cause fractions.")
    cause_fractions = _compute_cause_fractions_codcorrect(df, version.draw_cols)
    cause_fractions[Columns.METRIC_ID] = gbd.metrics.PERCENT

    logger.info("Bringing newly created demographics together.")
    df = pd.concat(
        [
            df, age_standardized_rates, rate_estimates,
            cause_fractions
        ],
        sort=True
    )
    if Columns.POPULATION in df.columns:
        df = df.drop(columns=Columns.POPULATION)
    return df


def summarize_pct_change(
        release_id: int,
        location_id: int,
        measure_id: int,
        year_start_id: int,
        year_end_id: int,
        version: MachineParameters
):
    """Compute summaries for percent change years."""
    year_start_draws = version.file_system.read_final_draws(
        location_id, year_start_id, measure_id
    )
    year_end_draws = version.file_system.read_final_draws(
        location_id, year_end_id, measure_id
    )

    year_start_draws = prep_summarize_gbd(
        year_start_draws,
        release_id,
        location_id,
        year_start_id,
        version)

    year_end_draws = prep_summarize_gbd(
        year_end_draws,
        release_id,
        location_id,
        year_end_id,
        version)

    df = pd.concat([year_start_draws, year_end_draws]).reset_index(drop=True)
    change_df = pct_change(
        df, year_start_id, year_end_id, Columns.YEAR_ID,
        version.draw_cols, change_type='pct_change')
    change_df = change_df.dropna()
    logger.info("Generating pct change summaries")
    change_summaries = _generate_summaries_pct_change(
        change_df, version.draw_cols)
    change_summaries[Columns.MEASURE_ID] = measure_id
    # Division by 0 can create inf, replace all inf with na and replace
    # na with 0
    change_summaries = (
        change_summaries.replace([np.inf, -np.inf], np.nan).fillna(0)
    )
    data_validations.check_duplicates(
        change_summaries,
        subset=[
            Columns.LOCATION_ID,
            Columns.AGE_GROUP_ID,
            Columns.SEX_ID,
            Columns.CAUSE_ID,
            Columns.METRIC_ID,
        ]
    )
    logger.info("Saving summaries.")
    _save_pct_change_summaries(
        change_summaries,
        location_id=location_id,
        year_start_id=year_start_id,
        year_end_id=year_end_id,
        measure_id=measure_id,
        version=version,
    )


def _compute_sex_aggregate(
        data: pd.DataFrame,
        groupby_cols: List[str] = Columns.INDEX
) -> pd.DataFrame:
    """Combines sex_id 1 and 2 into both sexes estimates and returns."""
    aggregate = data.copy()
    aggregate[Columns.SEX_ID] = gbd.sex.BOTH
    aggregate = aggregate.groupby(groupby_cols).sum().reset_index()
    return aggregate


def _compute_age_aggregates(
        data: pd.DataFrame,
        aggregate_age_group_ids: List[int],
        release_id: int,
        groupby_cols: List[str] = Columns.INDEX
) -> pd.DataFrame:
    """
    Takes a dataframe in count space, calculates all aggregated ages from
    aggregate_age_group_ids, and returns aggregates as a new dataframe.

    Arguments:
        df (pd.DataFrame): dataframe containing indices and draws to create
            age aggregates with.
        aggregate_age_group_ids: Age groups to be aggregated to. Ex:
            gbd.constants.GBD_COMPARE_AGES, all ages
        release_id (int): Release ID

    Returns:
        A new aggregated data set
    """
    # Drop age standardized from age aggregates as that is computed separately
    aggregate_age_group_ids = [
        age_group_id for age_group_id in aggregate_age_group_ids
        if age_group_id != gbd.age.AGE_STANDARDIZED
    ]

    data = data[~data[Columns.AGE_GROUP_ID].isin(aggregate_age_group_ids)]
    # create age trees
    age_trees = []
    for age_group in aggregate_age_group_ids:
        tree = agetree(age_group_id=age_group, release_id=release_id)
        age_trees.append(tree)

    agg_ages = []
    # for each tree, identify the child age groups, groupby sum the children
    # to produce the parent estimates.
    for atree in age_trees:
        child_ids = list(map(lambda x: x.id, atree.root.children))
        child_data = data[data[Columns.AGE_GROUP_ID].isin(child_ids)].copy()
        child_data[Columns.AGE_GROUP_ID] = atree.root.id
        child_data = child_data.groupby(groupby_cols).sum().reset_index()
        agg_ages.append(child_data)
    aggregated_ages: pd.DataFrame = pd.concat(agg_ages).reset_index(drop=True)
    return aggregated_ages


def _compute_population_aggregates(
        data: pd.DataFrame,
        aggregate_age_group_ids: List[int],
        release_id: int,
        groupby_cols: List[str] = Columns.DEMOGRAPHIC_INDEX
) -> pd.DataFrame:
    """Adds age/sex aggregates on to the population dataframe."""
    pop_data = data.copy()
    logger.info("Generating both-sexes on population")
    sex_aggregate = _compute_sex_aggregate(pop_data, groupby_cols)
    pop_data = pd.concat([pop_data, sex_aggregate], ignore_index=True)
    logger.info("Generating aggregated-ages on population")
    age_aggregate = _compute_age_aggregates(
        pop_data, aggregate_age_group_ids, release_id, groupby_cols
    )
    pop_data = pd.concat([pop_data, age_aggregate], ignore_index=True)
    return pop_data


def _compute_age_standardized_rate(
        data: pd.DataFrame,
        release_id: int,
        draw_cols: List[str]
) -> pd.DataFrame:
    """
    Computes the age standardized rate from most-detailed age data and returns
    as a new dataframe.

    Arguments:
        data (pd.DataFrame)
        release_id (int)

    Returns:
        pd.DataFrame
    """
    age_std: pd.DataFrame = data.copy()
    age_weights: pd.DataFrame = get_age_weights(release_id=release_id)
    # Merge age-weight data
    age_std = pd.merge(
        age_std,
        age_weights,
        on=[Columns.AGE_GROUP_ID],
        how='left'
    )
    # calculate age-standardized value:
    # first we divide all draws by the population value for the respective
    # demographic, then we multiply the result by the age weight.
    age_std[draw_cols] = age_std[draw_cols].div(
        age_std[Columns.POPULATION].values, axis='index'
    ).mul(age_std[Columns.AGE_WEIGHT_VALUE].values, axis='index')

    # set all age groups to age-standardized
    age_std[Columns.AGE_GROUP_ID] = gbd.age.AGE_STANDARDIZED
    age_std.drop(Columns.AGE_WEIGHT_VALUE, inplace=True, axis=1)
    # groupby sum to aggregate into final age-standardized rates
    age_std = age_std.groupby(Columns.INDEX).sum().reset_index()
    # mark the metric for these estimates as rate
    age_std[Columns.METRIC_ID] = gbd.metrics.RATE
    return age_std


def _compute_rates(data: pd.DataFrame, draw_cols: List[str]) -> pd.DataFrame:
    """
    Computes the GBD rates by dividing all draws by the population estimates.

    :param data: pd.DataFrame
    :return pd.DataFrame
    """
    rate_data = data.copy()
    rate_data[draw_cols] = rate_data[draw_cols].div(
        rate_data[Columns.POPULATION].values,
        axis='index'
    )
    rate_data[Columns.METRIC_ID] = gbd.metrics.RATE
    return rate_data


def _compute_cause_fractions_codcorrect(data: pd.DataFrame, draw_cols) -> pd.DataFrame:
    all_cause = data.loc[
        data[Columns.CAUSE_ID] == Causes.ALL_CAUSE
    ].set_index(Columns.INDEX_NO_CAUSE)
    cf_df = data.set_index(Columns.INDEX)
    cf_df[draw_cols] = cf_df[draw_cols].div(
        all_cause[draw_cols], axis='index', fill_value=0.0)
    return cf_df.reset_index()


def _generate_summaries(
        df: pd.DataFrame,
        database: str,
        draw_cols: List[str]
) -> pd.DataFrame:
    """
    Generate mean, lower, and upper in the schema required for the db.

    Arguments:
        data (pd.DataFrame):
        database (str) DB name
        draw_cols: list of names of the draw columns
    Returns:
        pd.DataFrame
    """
    mean_col = Columns.VALUE
    lower_col = Columns.LOWER
    upper_col = Columns.UPPER
    keep = Columns.INDEX + [Columns.METRIC_ID]

    data = df.copy()
    data.loc[:, mean_col] = np.mean(data[draw_cols].values, axis=1)
    data.loc[:, lower_col] = np.percentile(
        data[draw_cols].values,
        q=2.5,
        axis=1
    )
    data.loc[:, upper_col] = np.percentile(
        data[draw_cols].values,
        q=97.5,
        axis=1
    )
    data = data[keep + [mean_col, lower_col, upper_col]]
    return data


def _generate_summaries_pct_change(df: pd.DataFrame, draw_cols) -> pd.DataFrame:

    data = df.rename(columns={Columns.PCT_CHANGE_MEANS: Columns.VALUE})
    data.loc[:, Columns.LOWER] = np.percentile(
        data[draw_cols].values,
        q=2.5,
        axis=1
    )
    data.loc[:, Columns.UPPER] = np.percentile(
        data[draw_cols].values,
        q=97.5,
        axis=1
    )
    data = data[Columns.MULTI_SUMMARY_COLS]
    return data


def _merge_population(
        df: pd.DataFrame,
        population: pd.DataFrame
) -> pd.DataFrame:
    """
    Convenience function to merge on a population dataframe and confirm that
    all demographics have matching population information once the merge is
    complete.

    Merges on location_id, year_id, sex_id, and age_group_id
    """
    df = pd.merge(
        df,
        population,
        on=Columns.DEMOGRAPHIC_INDEX,
        how='left',
        indicator=True
    )
    if not (df._merge == "both").all():
        missing = df.loc[df._merge != "both"]
        raise ValueError(
            f"There are demographics missing population information:\n"
            f"{missing}"
        )
    df = df.drop("_merge", axis=1)
    data_validations.check_duplicates(df, subset=Columns.INDEX)
    return df


def _save_summaries(
        data: pd.DataFrame,
        location_id: int,
        year_id: int,
        measure_id: int,
        version: MachineParameters,
) -> None:
    """
    Saves draws in a CSV file in the schema required for the GBD db.

    Returns: None
    """
    keep_cols = [
        Columns.MEASURE_ID, Columns.LOCATION_ID, Columns.SEX_ID,
        Columns.AGE_GROUP_ID, Columns.YEAR_ID, Columns.CAUSE_ID,
        Columns.METRIC_ID, Columns.VALUE, Columns.UPPER, Columns.LOWER
    ]
    data = data[keep_cols]
    data.sort_values(
        by=[
            Columns.MEASURE_ID, Columns.YEAR_ID, Columns.LOCATION_ID,
            Columns.SEX_ID, Columns.AGE_GROUP_ID, Columns.CAUSE_ID,
            Columns.METRIC_ID
        ],
        inplace=True
    )
    version.file_system.save_summaries(
        summaries=data,
        database=DB,
        location_id=location_id,
        year_id=year_id,
        measure_id=measure_id,
        summary_type=SummaryType.SINGLE,
    )


def _save_pct_change_summaries(
        data: pd.DataFrame,
        location_id: int,
        year_start_id: int,
        year_end_id: int,
        measure_id: int,
        version: MachineParameters,
) -> None:
    """
    Saves draws in a CSV file in the schema required for the db.

    Returns: None
    """
    keep_cols = Columns.MULTI_SUMMARY_COLS + [Columns.MEASURE_ID]
    data = data[keep_cols]
    data.sort_values(
        by=[
            Columns.MEASURE_ID, Columns.YEAR_START_ID, Columns.YEAR_END_ID,
            Columns.LOCATION_ID, Columns.SEX_ID, Columns.AGE_GROUP_ID,
            Columns.CAUSE_ID, Columns.METRIC_ID
        ],
        inplace=True
    )
    year_id = '_'.join([str(year_start_id), str(year_end_id)])

    version.file_system.save_summaries(
        summaries=data,
        database=DB,
        location_id=location_id,
        year_id=year_id,
        measure_id=measure_id,
        summary_type=SummaryType.MULTI,
    )


def _save_cod_summaries(
        data: pd.DataFrame,
        location_id: int,
        year_id: int,
        version: MachineParameters,
) -> None:
    """
    Saves draws in a CSV file in the schema required for the db.

    Returns: None
    """
    measure_id = gbd.measures.DEATH
    cod_summary_cols = Columns.COD_SUMMARY + Columns.CAUSE_FRACTION_SUMMARY
    with_shocks_cols = [col + '_with_shocks' for col in cod_summary_cols]
    keep_cols = (Columns.INDEX + [Columns.MODEL_VERSION_ID] +
                 cod_summary_cols + with_shocks_cols)
    data = data[keep_cols]
    data[Columns.OUTPUT_VERSION_ID] = version.cod_output_version_id
    data.sort_values(
        by=[
            Columns.YEAR_ID, Columns.LOCATION_ID,
            Columns.SEX_ID, Columns.AGE_GROUP_ID, Columns.CAUSE_ID
        ],
        inplace=True
    )

    version.file_system.save_summaries(
        summaries=data,
        database=DB,
        location_id=location_id,
        year_id=year_id,
        measure_id=gbd.measures.DEATH,
    )


def _is_most_detailed_location(location_id: int,
                               version: MachineParameters) -> bool:
    most_detailed = []
    for key in version._location_parameters:
        most_detailed = (
            most_detailed + version._location_parameters[key].most_detailed_ids)
    most_detailed = list(set(most_detailed))
    return location_id in most_detailed


def _get_model_version_ids(version: MachineParameters) -> pd.DataFrame:
    model_version_df = version._eligible_metadata.explode(Columns.CAUSE_AGES)
    model_version_df = model_version_df.rename(
        columns={Columns.CAUSE_AGES: Columns.AGE_GROUP_ID})
    model_version_df = model_version_df.sort_values(
        by=Columns.MODEL_VERSION_TYPE_ID)
    model_version_df = model_version_df.drop_duplicates(
        subset=[Columns.CAUSE_ID, Columns.SEX_ID, Columns.AGE_GROUP_ID])
    return model_version_df[[Columns.MODEL_VERSION_ID,
                             Columns.CAUSE_ID,
                             Columns.SEX_ID,
                             Columns.AGE_GROUP_ID]]
