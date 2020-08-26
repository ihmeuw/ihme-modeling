import logging
import numpy as np
import os
import pandas as pd
import subprocess
from typing import Optional, List

from db_queries.get_age_metadata import get_age_weights
from draw_sources.draw_sources import DrawSource
import gbd.constants as gbd
from hierarchies.dbtrees import agetree
from core_maths.summarize import pct_change

from fauxcorrect.utils import io
from fauxcorrect.utils.constants import (
    Ages, Columns, FilePaths, Keys, DataBases, GBD, Decomp,
    Measures)
from fauxcorrect.validations.error_check import check_duplicates
from fauxcorrect.parameters.master import MachineParameters


def summarize(
        tool_name: str,
        parent_dir: str,
        gbd_round_id: int,
        location_id: int,
        measure_id: int,
        year_id: int
) -> None:
    # Get draw data
    logging.info("Read in scaled draws from disk.")
    scaled_estimates = _read_scaled_draws(
        tool_name=tool_name,
        parent_dir=parent_dir,
        location_id=location_id,
        year_id=year_id,
        measure_id=measure_id
    )
    check_duplicates(scaled_estimates, subset=Columns.INDEX)
    if Columns.MEASURE_ID in scaled_estimates:
        scaled_estimates = scaled_estimates.drop(Columns.MEASURE_ID, axis=1)

    scaled_estimates = prep_summarize_gbd(scaled_estimates,
                                          tool_name,
                                          parent_dir,
                                          gbd_round_id,
                                          location_id,
                                          year_id)

    logging.info("Summarizing data.")
    summaries = _generate_summaries(scaled_estimates, DataBases.GBD)
    # Add in measure_id
    summaries[Columns.MEASURE_ID] = measure_id
    # Division by 0 can create inf, replace all inf with na and replace
    # na with 0
    summaries = summaries.replace([np.inf, -np.inf], np.nan).fillna(0)
    logging.info("Saving summaries.")
    _save_summaries(
        summaries,
        parent_dir=parent_dir,
        location_id=location_id,
        year_id=year_id,
        measure_id=measure_id
    )


def summarize_cod(
        parent_dir: str,
        gbd_round_id: int,
        location_id: int,
        year_id: int,
        version: MachineParameters
) -> None:
    measure_id = Measures.Ids.DEATHS
    logging.info("Read in scaled draws with shocks from disk.")
    data_with_shocks = _read_scaled_draws(version.process,
                                          parent_dir,
                                          location_id,
                                          year_id,
                                          measure_id)
    check_duplicates(data_with_shocks, subset=Columns.INDEX)
    if Columns.MEASURE_ID in data_with_shocks:
        data_with_shocks = data_with_shocks.drop(Columns.MEASURE_ID, axis=1)

    logging.info("Read in scaled draws without shocks from disk.")
    data_no_shocks = io.read_aggregated_rescaled_draws_for_summaries(
        parent_dir, location_id, year_id, measure_id=measure_id)
    check_duplicates(data_no_shocks, subset=Columns.INDEX)
    if Columns.MEASURE_ID in data_no_shocks:
        data_no_shocks = data_no_shocks.drop(Columns.MEASURE_ID, axis=1)

    logging.info("Read in population.")
    population: pd.DataFrame = io.read_cached_hdf(
        filepath=os.path.join(
            parent_dir,
            FilePaths.INPUT_FILES_DIR,
            FilePaths.POPULATION_FILE
        ),
        key=Keys.POPULATION,
        where=[f"'location_id'=={location_id} and 'year_id'=={year_id}"]
    )
    population = population[Columns.DEMOGRAPHIC_INDEX + [Columns.POPULATION]]
    population = _compute_population_aggregates(population, gbd_round_id)

    df_dict = {'data_with_shocks': data_with_shocks,
               'data_no_shocks': data_no_shocks}
    summaries_dict = {}
    for df_name in df_dict:
        df = df_dict[df_name]
        # create sex_id 3
        logging.info(f"Compute sex aggregates for {df_name}.")
        sex_aggregate = _compute_sex_aggregate(df)
        df = pd.concat(
            [df, sex_aggregate],
            sort=True
        ).reset_index(drop=True)
        # create all age
        logging.info(f"Compute age aggregates for {df_name}.")
        age_aggregate = _compute_age_aggregates(df, gbd_round_id)
        # merge on population
        logging.info("Merge population on demographic indices.")
        df = _merge_population(df, population)
        # create age standardized
        logging.info(f"Compute age standardized for {df_name}.")
        age_standardized_rates = _compute_age_standardized_rate(
            df,
            gbd_round_id=gbd_round_id
        ).drop(Columns.POPULATION, axis=1)
        # Drop population column, add in our age aggregates before
        # calculating cause fractions for COD database
        logging.info(f"Add age aggregates to {df_name}.")
        df = df.drop(Columns.POPULATION, axis=1)
        df = pd.concat(
            [df, age_aggregate],
            sort=True
        ).reset_index(drop=True)

        # Do not add back into the unscaled data, we need only count space for
        # cause fraction calculation.
        logging.info(f"Compute cause fractions for {df_name}.")
        cause_fractions = _compute_cause_fractions_codcorrect(df)

        # add age-standardized to our count-space df
        logging.info(f"Add age standardized to {df_name}.")
        df = pd.concat([df, age_standardized_rates], sort=True)
        rename = {Columns.COD_MEAN: Columns.CAUSE_FRACTION_MEAN,
                     Columns.COD_LOWER: Columns.CAUSE_FRACTION_LOWER,
                     Columns.COD_UPPER: Columns.CAUSE_FRACTION_UPPER}
        rename_values = list(rename.values())
        logging.info("Summarizing data.")
        df_summary = _generate_summaries(df, DataBases.COD)[
            Columns.INDEX + list(rename.keys())]
        cf_summary = _generate_summaries(cause_fractions, DataBases.COD).rename(
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
    # there will be NaNs for mean/upper/lower_death for any shocks,
    # because of the outer merge on cause_id, when obviously there's no
    # non-shock data for a shock cause
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
    logging.info("Saving summaries.")
    _save_cod_summaries(summary, parent_dir, location_id, year_id,
                        version.version_id)


def prep_summarize_gbd(
        df: pd.DataFrame,
        tool_name: str,
        parent_dir: str,
        gbd_round_id: int,
        location_id: int,
        year_id: int
) -> pd.DataFrame:
    # Read population from disk
    logging.info("Read in population cache")
    population: pd.DataFrame = io.read_cached_hdf(
        filepath=os.path.join(
            parent_dir,
            FilePaths.INPUT_FILES_DIR,
            FilePaths.POPULATION_FILE
        ),
        key=Keys.POPULATION,
        where=[f"location_id=={location_id} and year_id=={year_id}"]
    )
    population = population[Columns.DEMOGRAPHIC_INDEX + [Columns.POPULATION]]
    population = _compute_population_aggregates(population, gbd_round_id)

    # Compute sex aggregates
    logging.info("Compute sex aggregates and combine with scaled estimates.")
    sex_aggregate = _compute_sex_aggregate(df)
    df = pd.concat(
        [df, sex_aggregate],
        sort=True
    ).reset_index(drop=True)

    # Compute age aggregates
    logging.info("Compute age aggregates and combine with scaled estimates.")
    age_aggregate = _compute_age_aggregates(df, gbd_round_id)

    # Compute ASR
    # First add a metric id to the existing scaled estimates,
    # then merge on a population column, then compute ASR
    logging.info("Compute age standardized rate.")
    df[Columns.METRIC_ID] = gbd.metrics.NUMBER
    logging.info("Merge population on demographic indices.")
    df = _merge_population(df, population)
    age_standardized_rates = _compute_age_standardized_rate(
        df,
        gbd_round_id=gbd_round_id
    )

    # Compute GBD rates
    # Drop pop column from scaled estimates, add a metric_id to the age
    # aggregates, combine with scaled estimates,
    # and then merge on a new population before computing rates for GBD
    # database
    logging.info("Compute GBD rates.")
    df =  df.drop(Columns.POPULATION, axis=1)
    age_aggregate[Columns.METRIC_ID] = gbd.metrics.NUMBER
    df = pd.concat(
        [df, age_aggregate],
        sort=True
    ).reset_index(drop=True)
    # merge on a new population column that is not age or sex restricted
    df = _merge_population(df, population)
    # Do not add back into the unscaled data, we need only count space for
    # cause fraction calculation.
    rate_estimates = _compute_rates(df)

    logging.info("Compute GBD cause fractions.")
    if tool_name == GBD.Process.Name.FAUXCORRECT:
        cause_fractions = _compute_cause_fractions(
            df,
            parent_dir=parent_dir,
            location_id=location_id,
            year_id=year_id
    )
    else:
        cause_fractions = _compute_cause_fractions_codcorrect(
            df)
    cause_fractions[Columns.METRIC_ID] = gbd.metrics.PERCENT

    logging.info("Bringing all our newly created demographics together.")
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
        tool_name: str,
        parent_dir: str,
        gbd_round_id: int,
        location_id: int,
        measure_id: int,
        year_start_id: int,
        year_end_id: int
):

    year_start_draws = _read_scaled_draws(tool_name,
                                          parent_dir,
                                          location_id,
                                          year_start_id,
                                          measure_id)

    year_end_draws = _read_scaled_draws(tool_name,
                                        parent_dir,
                                        location_id,
                                        year_end_id,
                                        measure_id)

    year_start_draws = prep_summarize_gbd(year_start_draws,
                                          tool_name,
                                          parent_dir,
                                          gbd_round_id,
                                          location_id,
                                          year_start_id)

    year_end_draws = prep_summarize_gbd(year_end_draws,
                                        tool_name,
                                        parent_dir,
                                        gbd_round_id,
                                        location_id,
                                        year_end_id)

    df = pd.concat([year_start_draws, year_end_draws]).reset_index(drop=True)
    change_df = pct_change(df, year_start_id, year_end_id, Columns.YEAR_ID,
                           Columns.DRAWS, change_type='pct_change')
    change_df = change_df.dropna()
    logging.info("Generating pct change summaries")
    change_summaries = _generate_summaries_pct_change(change_df)
    # One last bit of formatting add in measure_id
    change_summaries[Columns.MEASURE_ID] = measure_id
    # Division by 0 can create inf, replace all inf with na and replace
    # na with 0
    change_summaries = (
        change_summaries.replace([np.inf, -np.inf], np.nan).fillna(0)
    )
    logging.info("Saving summaries.")
    _save_pct_change_summaries(
        change_summaries,
        parent_dir=parent_dir,
        location_id=location_id,
        year_start_id=year_start_id,
        year_end_id=year_end_id,
        measure_id=measure_id,
    )


def _read_scaled_draws(
        tool_name: str,
        parent_dir: str,
        location_id: int,
        year_id: int,
        measure_id: int
) -> pd.DataFrame:
    if tool_name == GBD.Process.Name.FAUXCORRECT:
        draw_dir = os.path.join(
            parent_dir,
            FilePaths.DRAWS_SCALED_DIR,
            str(measure_id)
        )
        file_pattern = FilePaths.SUMMARY_INPUT_FILE_PATTERN.format(
            location_id=location_id, year_id=year_id)
    else:
        draw_dir = os.path.join(
            parent_dir,
            FilePaths.DRAWS_DIR)
        file_pattern = FilePaths.SUMMARY_AGGREGATE_READ_PATTERN.format(
            measure_id=measure_id, location_id=location_id, year_id=year_id)
    return DrawSource({
        'draw_dir': draw_dir,
        'file_pattern': file_pattern,
        'h5_tablename': Keys.DRAWS
    }).content()


def _compute_sex_aggregate(
        data: pd.DataFrame,
        groupby_cols: List[str]=Columns.INDEX
) -> pd.DataFrame:
    """Combines sex_id 1 and 2 into both sexes estimates and returns."""
    aggregate = data.copy()
    aggregate[Columns.SEX_ID] = gbd.sex.BOTH
    aggregate = aggregate.groupby(groupby_cols).sum().reset_index()
    return aggregate


def _compute_age_aggregates(
        data: pd.DataFrame,
        gbd_round_id: int,
        groupby_cols: List[str]=Columns.INDEX
) -> pd.DataFrame:
    """
    Takes a dataframe in count space, calculates all aggregated ages from
    gbd.constants.GBD_COMPARE_AGES + ALL_AGES, and returns aggregates as a new
    dataframe

    Arguments:
        df (pd.DataFrame): dataframe containing indices and draws to create
            age aggregates with.

        gbd_round_id (int):

    Returns:
        A new aggregated data set
    """
    compare_ages = list(
        set(gbd.GBD_COMPARE_AGES).union(set(Ages.END_OF_ROUND_AGE_GROUPS))
    )

    if gbd.age.ALL_AGES not in compare_ages:
        compare_ages.append(gbd.age.ALL_AGES)

    data = data[~data[Columns.AGE_GROUP_ID].isin(compare_ages)]
    # create age trees
    age_trees = []
    for age_group in compare_ages:
        tree = agetree(age_group_id=age_group, gbd_round_id=gbd_round_id)
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
        gbd_round_id: int,
        groupby_cols: List[str]=Columns.DEMOGRAPHIC_INDEX
) -> pd.DataFrame:
    """Adds age/sex aggregates on to the population dataframe."""
    pop_data = data.copy()
    logging.info("Generating both-sexes on population")
    sex_aggregate = _compute_sex_aggregate(pop_data, groupby_cols)
    pop_data = pd.concat([pop_data, sex_aggregate], ignore_index=True)
    logging.info("Generating aggregated-ages on population")
    age_aggregate = _compute_age_aggregates(pop_data, gbd_round_id,
        groupby_cols)
    pop_data = pd.concat([pop_data, age_aggregate], ignore_index=True)
    return pop_data


def _compute_age_standardized_rate(
        data: pd.DataFrame,
        gbd_round_id: int
) -> pd.DataFrame:
    """
    Computes the age standardized rate from most-detailed age data and returns
    as a new dataframe.

    Arguments:
        data (pd.DataFrame)
        gbd_round_id (int)

    Returns:
        pd.DataFrame
    """
    age_std: pd.DataFrame = data.copy()
    age_weights: pd.DataFrame = get_age_weights(gbd_round_id=gbd_round_id)
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
    age_std[Columns.DRAWS] = age_std[Columns.DRAWS].div(
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


def _compute_rates(data: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the GBD rates by dividing all draws by the population estimates.

    :param data: pd.DataFrame
    :return pd.DataFrame
    """
    rate_data = data.copy()
    rate_data[Columns.DRAWS] = rate_data[Columns.DRAWS].div(
        rate_data[Columns.POPULATION].values,
        axis='index'
    )
    rate_data[Columns.METRIC_ID] = gbd.metrics.RATE
    return rate_data


def _compute_cause_fractions(
        data: pd.DataFrame,
        parent_dir: str,
        location_id: int,
        year_id: int
) -> pd.DataFrame:
    """
    Creates the cause fraction estimates for the gbd database and returns as a
    new dataframe.

    :param data: pd.DataFrame
    :return pd.DataFrame
    """
    cause_fractions = data.copy()
    # Envelope may be carried over from CODEm draws, remove it.
    if Columns.ENVELOPE in cause_fractions.columns:
        cause_fractions.drop(labels=[Columns.ENVELOPE], axis=1, inplace=True)
    # Read in the envelope from disk
    envelope: pd.DataFrame = io.read_cached_hdf(
        filepath=os.path.join(
            parent_dir,
            FilePaths.INPUT_FILES_DIR,
            FilePaths.ENVELOPE_SUMMARY_FILE
        ),
        key=Keys.ENVELOPE,
        where=[f"'location_id'=={location_id} and 'year_id'=={year_id}"]
    )
    # merge together
    cause_fractions = pd.merge(
        cause_fractions,
        envelope,
        on=[
            Columns.LOCATION_ID, Columns.YEAR_ID, Columns.SEX_ID,
            Columns.AGE_GROUP_ID
        ],
        how='left',
        indicator=True
    )
    if not (cause_fractions._merge=="both").all():
        missing = cause_fractions.loc[cause_fractions._merge!="both"]
        raise ValueError(
            f"There are demographics missing cause_fraction information:\n"
            f"{missing}"
        )
    cause_fractions = cause_fractions.drop("_merge", axis=1)

    # Compute: draws / envelope_mean
    cause_fractions[Columns.DRAWS] = cause_fractions[Columns.DRAWS].div(
        cause_fractions[Columns.ENVELOPE].values, axis='index'
    ).reset_index(drop=True)
    cause_fractions.drop([Columns.ENVELOPE], axis=1, inplace=True)
    return cause_fractions


def _compute_cause_fractions_codcorrect(data: pd.DataFrame) -> pd.DataFrame:
    all_cause = data.loc[data[
        Columns.CAUSE_ID] == Decomp.Causes.ALL_CAUSE].set_index(
        Columns.INDEX_NO_CAUSE)
    cf_df = data.set_index(Columns.INDEX)
    cf_df[Columns.DRAWS] = cf_df[Columns.DRAWS].div(
        all_cause[Columns.DRAWS], axis='index', fill_value=0.0)
    return cf_df.reset_index()


def _generate_summaries(df: pd.DataFrame, database: str) -> pd.DataFrame:
    """
    Generate mean, lower, and upper in the schema required for the gbd db.

    Arguments:
        data (pd.DataFrame):
        database (str) One of 'cod' or 'gbd'.
    Returns:
        pd.DataFrame
    """
    if database == DataBases.GBD:
        mean_col = Columns.VALUE
        lower_col = Columns.LOWER
        upper_col = Columns.UPPER
        keep = Columns.INDEX + [Columns.METRIC_ID]
    elif database == DataBases.COD:
        mean_col = Columns.COD_MEAN
        lower_col = Columns.COD_LOWER
        upper_col = Columns.COD_UPPER
        keep = Columns.INDEX
    else:
        raise ValueError("Only 'cod' or 'gbd' are supported for database "
                         "argument. Passed: {}".format(database))
    data = df.copy()
    data.loc[:, mean_col] = np.mean(data[Columns.DRAWS].values, axis=1)
    data.loc[:, lower_col] = np.percentile(
        data[Columns.DRAWS].values,
        q=2.5,
        axis=1
    )
    data.loc[:, upper_col] = np.percentile(
        data[Columns.DRAWS].values,
        q=97.5,
        axis=1
    )
    data = data[keep + [mean_col, lower_col, upper_col]]
    return data


def _generate_summaries_pct_change(df: pd.DataFrame) -> pd.DataFrame:

    data = df.rename(columns={Columns.PCT_CHANGE_MEANS: Columns.VALUE})
    data.loc[:, Columns.LOWER] = np.percentile(
        data[Columns.DRAWS].values,
        q=2.5,
        axis=1
    )
    data.loc[:, Columns.UPPER] = np.percentile(
        data[Columns.DRAWS].values,
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
    if not (df._merge=="both").all():
        missing = df.loc[df._merge!="both"]
        raise ValueError(
            f"There are demographics missing population information:\n"
            f"{missing}"
        )
    df = df.drop("_merge", axis=1)
    check_duplicates(df, subset=Columns.INDEX)
    return df


def _save_summaries(
        data: pd.DataFrame,
        parent_dir: str,
        location_id: int,
        year_id: int,
        measure_id: int,
        summary_type: Optional[str] = 'single'
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

    filepath = os.path.join(
        parent_dir,
        FilePaths.SUMMARY_DIR,
        'gbd',
        summary_type,
        str(measure_id),
        FilePaths.SUMMARY_OUTPUT_FILE_PATTERN.format(
            year_id=year_id, location_id=location_id
        )
    )
    data.to_csv(filepath, index=False, encoding="utf8")
    permissions_change = ['chmod', '775', filepath]
    subprocess.check_output(permissions_change)


def _save_pct_change_summaries(
        data: pd.DataFrame,
        parent_dir: str,
        location_id: int,
        year_start_id: int,
        year_end_id: int,
        measure_id: int
) -> None:
    """
    Saves draws in a CSV file in the schema required for the GBD db.

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
    filepath = os.path.join(
        parent_dir,
        FilePaths.SUMMARY_DIR,
        'gbd',
        FilePaths.MULTI_DIR,
        str(measure_id),
        FilePaths.SUMMARY_OUTPUT_FILE_PATTERN.format(
            year_id=year_id, location_id=location_id
        )
    )

    data.to_csv(filepath, index=False, encoding="utf8")
    permissions_change = ['chmod', '775', filepath]
    subprocess.check_output(permissions_change)


def _save_cod_summaries(
        data: pd.DataFrame,
        parent_dir: str,
        location_id: int,
        year_id: int,
        output_version_id: int
) -> None:
    """
    Saves draws in a CSV file in the schema required for the cod db.

    Returns: None
    """
    measure_id = Measures.Ids.DEATHS
    cod_summary_cols = Columns.COD_SUMMARY + Columns.CAUSE_FRACTION_SUMMARY
    with_shocks_cols = [col + '_with_shocks' for col in cod_summary_cols]
    keep_cols = (Columns.INDEX + [Columns.MODEL_VERSION_ID] +
                 cod_summary_cols + with_shocks_cols)
    data = data[keep_cols]
    data[Columns.OUTPUT_VERSION_ID] = output_version_id
    data.sort_values(
        by=[
            Columns.YEAR_ID, Columns.LOCATION_ID,
            Columns.SEX_ID, Columns.AGE_GROUP_ID, Columns.CAUSE_ID
        ],
        inplace=True
    )

    filepath = os.path.join(
        parent_dir,
        FilePaths.SUMMARY_DIR,
        DataBases.COD,
        str(measure_id),
        FilePaths.SUMMARY_OUTPUT_FILE_PATTERN.format(
            year_id=year_id, location_id=location_id
        )
    )
    data.to_csv(filepath, index=False, encoding="utf8")
    permissions_change = ['chmod', '775', filepath]
    subprocess.check_output(permissions_change)


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
