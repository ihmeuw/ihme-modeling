"""
Create shared function wrappers within the auxiliary (supplemental) script. The
reason for shared function wrappers within the auxiliary file is to reduce
the chance for making an error. For instance, creating our own get_draws
wrapper will make it so that we only need to set certain args once (e.g.
gbd_round_id only needs to be set here). Also, creating our own wrapper means
that we need to change our central function calls in one place when we need
to update code each round.
"""

# ----IMPORTS------------------------------------------------------------------

import errno
import logging
import operator
import os

import numpy as np
import pandas as pd

from get_draws.api import get_draws
from db_queries import (get_best_model_versions, get_ids, get_age_metadata,
                        get_location_metadata)

import Config as cfg

logging.basicConfig(
        level=logging.INFO,
        format=' %(asctime)s - %(levelname)s - %(message)s')

# ----FUNCTIONS----------------------------------------------------------------


def get_vision_draws(meid,
                     measure_id,
                     location_id,
                     age_group_id,
                     status,
                     gbd_round_id,
                     decomp_step):
    """
    Pull draws for vision loss models.

    Arguments:
        meid (int): modelable entity id
        measure_id (int): measure
        location_id (int): location

    Returns:
        A dataframe of draws
    """
    draws = get_draws(gbd_id_type='modelable_entity_id',
                      gbd_id=meid,
                      source="epi",
                      measure_id=measure_id,
                      location_id=location_id,
                      age_group_id=age_group_id,
                      year_id=cfg.year_id,
                      status=status,
                      gbd_round_id=gbd_round_id,
                      decomp_step=decomp_step)
    return draws


def get_best_vision_model_versions(meids, gbd_round_id, status, decomp_step):
    """
    Pull either best or latest model versions for specified meids.

    Arguments:
        meids (intlist): List of meids.
        gbd_round_id (int):
        status (str):
        decomp_step (str):

    Returns:
    A list of meids for which there is a current best or latest model
    """
    best_models = get_best_model_versions(entity="modelable_entity",
                                          ids=meids,
                                          gbd_round_id=gbd_round_id,
                                          status=status,
                                          decomp_step=decomp_step)
    best_meids = best_models.modelable_entity_id.values.tolist()
    return best_meids


def zero_out_draws(df, meid, measure_id):
    """
    Args:
        df (pandas DataFrame): a dataframe of draws
        meid (int): modelable entity id
        measure_id (int): measure

    Returns:
        A dataframe with 0 in the draws
    """
    df['modelable_entity_id'] = meid
    df['measure_id'] = measure_id
    df['model_version_id'] = "Filled data. Not a real model"
    df[['draw_{}'.format(i) for i in range(1000)]] = 0
    return df


def impute_birth_estimates(df):
    """
    Args:
        df(pandas DataFrame): a dataframe of draws without estimates for birth

    Returns:
        A dataframe with birth estimates equal to estimates for the
        early neonatal age group.

    """
    birth_prev = pd.DataFrame()
    birth_prev = df.loc[df.age_group_id == 2].copy()
    birth_prev['age_group_id'] = 164
    df = df.append(birth_prev)
    return df


def get_most_detailed_locations(location_set, gbd_round_id, decomp_step):
    """
    Get the most detailed locations that are estimated for GBD.

    Arguments:
        location_set (int): The location_set_id to pull location metadata for.
        gbd_round_id (int): The gbd_round_id to pull location_metadata for.
        decomp_step (str): The decomp_step to pull location_metadata for.

    Returns:
        A dataframe with only the most detailed locations for which we produce
        estimates

    """
    locs = get_location_metadata(location_set_id=location_set,
                                 gbd_round_id=gbd_round_id,
                                 decomp_step=decomp_step)
    locs = locs.query("most_detailed == 1 and is_estimate == 1")
    return locs


def apply_grs(cause_df,
              cap_df,
              cap_col,
              age_restrictions,
              location_id,
              draw_col='draw_',
              subset_by_year=False,
              year_id=None,
              year_col=None,
              draw_count=1000):
    """
    Some causes are restricted geographically in the sense that they are not
    present for some locations. Since dismod will create estimates for all
    locations, estimates for locations where a particular disease does not
    exist must be replaced with zeros.

    Arguments:
        cause_df (dataframe): Pandas dataframe of draws to apply geographic
            restrictions to.
        cap_df (dataframe): Dataframe containing a map of the geographic
            restrictions by location.
        cap_col (str): The name of the binary column used for determining
            whether a specific location has geographic restrictions.
        age_restrictions (intlist): Age_group_ids to zero out.
        location_id (int): The location_id to apply restrictions to.
        draw_col (str): The naming pattern for all input draw columns for
            the cause_df dataframe (e.g. draw_{}).Defaults to "draw_".
        subset_by_year (Bool): Whether to subset cap_df by a specific year
            Defaults to False.
        year_id (int): Year to subset cap_df by should subset_by_year be
            equal to "True". Defaults to None.
        year_col (str): Column containing the year_id to subset by
            should subset_by_year be
            equal to "True". Defaults to None.
        draw_count (int): The number of draws in the dataframe. Defaults to
            1000.

    Returns:
        Dataframe of draws where geographic restrictions have been applied.

    """
    if subset_by_year:
        cap_df = cap_df[cap_df[year_col] == year_id]
    cap_df = cap_df.rename(columns={cap_col: 'prev_0'})

    # Assume that locations with a value of 0 have a prevalence of zero
    logging.info(cap_df.shape)
    cap_prev = cap_df.query(
        "location_id == @location_id").prev_0.values[0]
    for i in range(draw_count):
        cause_df['{}{}'.format(draw_col,
            i)] = cause_df['{}{}'.format(draw_col, i)] * cap_prev

    # Apply age restrictions
    for i in range(draw_count):
        cause_df.loc[cause_df.age_group_id.isin(age_restrictions),
                     '{}{}'.format(draw_col, i)] = 0
    return cause_df


def cap_vita(df, location_id):
    """
    Args:
        df(pandas DataFrame): DataFrame with age_group_id (and a value for each
            age in cfg.age_group_id), location_id, sex_id, year_id
        location_id(int): GBD location_id

    Returns:
        A dataframe where vitamin a is capped to be no greater than the age
        group 6 value within each draw, year, and sex
    """
    for sex in df.sex_id.unique():
        logging.info("sex = {}".format(sex))
        for year in df.year_id.unique():
            logging.info("year = {}".format(year))
            df = df.set_index([
                               "location_id",
                               "sex_id",
                               "year_id",
                               "age_group_id"
                               ]).sort_index()
            one_set_of_ages = df.loc[location_id, sex, year].copy()
            max_vals = one_set_of_ages.loc[6]
            for age in cfg.age_ids_list:
                if age in [7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                           20, 30, 31, 32, 235]:
                    one_set_of_ages.loc[age] = one_set_of_ages.loc[age].clip(
                        upper=max_vals)
                    df.loc[location_id, sex, year, age] = one_set_of_ages.loc[
                       age]
                else:
                    pass
            df.reset_index(inplace=True)
    return df


def get_mod_plus_proportions(prop_path):
    """
    Args:
        None

    Returns:
        A dataframe with draws of proportions of moderate or worse vision loss
    """
    mild_mod_xwalk = pd.read_stata(prop_path)

    # Get year start
    age_group_ids = get_age_metadata(age_group_set_id=cfg.age_group_set_id,
                                     gbd_round_id=cfg.gbd_round_id)

    # Filter age group column to ensure that only useable metadata is present.
    age_group_ids = age_group_ids[['age_group_id', 'age_group_years_start']]
    age_group_ids.rename(columns={'age_group_years_start': 'age_start'},
                         inplace=True)

    # Add birth prevalence (this is not included in get age metadata)
    birth_prev = pd.Series([164, 0])
    birth_prev = pd.DataFrame([birth_prev])
    birth_prev = birth_prev.rename(columns={0: 'age_group_id', 1: 'age_start'})
    age_group_ids = pd.concat([birth_prev, age_group_ids], ignore_index=True)

    # Filter for age_group_ids that come from the config file list.
    age_group_ids = age_group_ids[
        age_group_ids.age_group_id.isin(cfg.age_ids_list)]
    mild_mod_x_walk = pd.merge(
        age_group_ids, mild_mod_xwalk, on='age_start', how='left')

    # Remove all proportions except for the mild or worse ones.
    mild_mod_x_walk = mild_mod_x_walk[['age_group_id',
                                       'mod_p_d_dmild_dmod_dsev_dvb',
                                       'mod_p_d_dmild_dmod_dsev_dvb_se']]
    under5_mean = mild_mod_x_walk.query("age_group_id == 2")[
        'mod_p_d_dmild_dmod_dsev_dvb'].values[0]
    under5_se = mild_mod_x_walk.query("age_group_id == 2")[
        'mod_p_d_dmild_dmod_dsev_dvb_se'].values[0]
    mild_mod_x_walk.loc[
        mild_mod_x_walk.age_group_id.isin(cfg.under_5),
        'mod_p_d_dmild_dmod_dsev_dvb'] = under5_mean
    mild_mod_x_walk.loc[
        mild_mod_x_walk.age_group_id.isin(cfg.under_5),
        'mod_p_d_dmild_dmod_dsev_dvb_se'] = under5_se
    if list(np.sort(mild_mod_x_walk.age_group_id.values)) != list(
            np.sort(cfg.age_ids_list)):
        logging.info(list(np.sort(mild_mod_x_walk.age_group_id.values)))
        logging.info(list(np.sort(cfg.age_ids_list)))
        raise ValueError(
            "all age group ids specified in the config file need to be valid")

    for i in range(1000):
        mild_mod_x_walk['draw_{}'.format(i)] = 0
    means = {}
    st_errs = {}
    for age_group in mild_mod_x_walk.age_group_id.values:
        # Get mean and se
        means[age_group] = mild_mod_x_walk.query(
            "age_group_id == @age_group"
            ).mod_p_d_dmild_dmod_dsev_dvb.values[0]
        st_errs[age_group] = mild_mod_x_walk.query(
            "age_group_id == @age_group"
            ).mod_p_d_dmild_dmod_dsev_dvb_se.values[0]

    # Drop columns that are no longer necessary
    mild_mod_x_walk.drop(['mod_p_d_dmild_dmod_dsev_dvb',
                          'mod_p_d_dmild_dmod_dsev_dvb_se'],
                         axis=1, inplace=True)
    for age_group in mild_mod_x_walk.age_group_id.values:
        draw_values = list(np.random.normal(loc=means[age_group],
                           scale=st_errs[age_group], size=1000))
        # Fill in mild_mod_x_walk row by row with draw values
        mild_mod_x_walk.loc[
            mild_mod_x_walk.age_group_id == age_group] = [
            age_group] + draw_values

    return mild_mod_x_walk


def get_dict_totals(dictionary, sev, exclude_rop, exclude_vita):
    """
    Args:
        dictionary (dict): dictionary of dataframes with cols 'age_group_id',
            'sex_id', 'year_id', 'location_id'
        sev (str): severity level. can take value "LOW_MOD", "LOW_SEV", or
            "BLIND"
        exclude_rop (bool):
            denotes whether or not rop should be excluded from the total
        exclude_vita (bool): denotes whether or not Vitamin A should be
            excluded from the total.

    Returns:
        A dataframe of all of the dataframes summed up for each age_group_id,
        sex_id, year_id, and location_id
    """
    df = pd.DataFrame()
    sev_specific_causes = [
        cause for cause in list(dictionary.keys()) if sev in cause]
    if exclude_rop:
        sev_specific_causes = [
            cause for cause in sev_specific_causes if "ROP" not in cause]
    if exclude_vita:
        sev_specific_causes = [
            cause for cause in sev_specific_causes if "VITA" not in cause]
    for cause in sev_specific_causes:
        df = df.append(dictionary[cause])
    # Sum up all causes
    df = df.groupby(['age_group_id',
                     'sex_id',
                     'year_id',
                     'location_id'],
                    as_index=False)[[c for c in df if 'draw' in c]].sum()
    return df


def sort_totals_and_envelope(df1, df2):
    """
    Args:
        df1 (pandas DataFrame): total of all etiologies summed together within
            a specific severity
        df2 (pandas DataFrame): severity level envelope prevalence
    Returns:
        Two pandas DataFrames that have been sorted and only include 1k draw
        columns
    """
    df1 = df1.set_index(['location_id',
                         'sex_id', 'year_id',
                         'age_group_id']).sort_index().reset_index()
    df1 = df1[[c for c in df1.columns if "draw" in c]]
    df2 = df2.set_index(['location_id',
                         'sex_id',
                         'year_id',
                         'age_group_id']).sort_index().reset_index()
    df2 = df2[[c for c in df2.columns if "draw" in c]]
    # Make sure column ordering is the same
    df1 = df1[['draw_{}'.format(i) for i in range(1000)]]
    df2 = df2[['draw_{}'.format(i) for i in range(1000)]]
    return df1, df2


def add_subtract_envelope(envelopes, causes, sev, cause, operation):
    """
    Args:
        envelopes (dict): dictionary of envelope dataframes
        causes (dict): dictionary of cause dataframes
        sev (str): severity level (e.g. "BLIND")
        cause (str): specific etiology (e.g. "ROP")
        operation (str): accepts either "ADD" or "SUBTRACT"

    Returns:
        A dataframe of envelope level prevalence with the cause level
        prevalence added in or subtracted out, depending on the value of
        operation
    """
    cause = cause.upper()
    sev = sev.upper()
    env_df = envelopes[sev].copy()
    cause_df = causes['{s}_{c}'.format(s=sev, c=cause)].copy()
    logging.info(
       "{o}ING {c} from the {s} envelope".format(o=operation, c=cause, s=sev))
    env_df = pd.merge(env_df,
                      cause_df,
                      on=['location_id',
                          'age_group_id',
                          'year_id',
                          'sex_id'], suffixes=['_ENV', '_{}'.format(cause)])
    for i in range(1000):
        envelope_draw = env_df['draw_{}_ENV'.format(i)]
        cause_draw = env_df['draw_{i}_{c}'.format(i=i, c=cause)]
        if operation == "SUBTRACT":
            env_df['draw_{}'.format(i)] = envelope_draw - cause_draw
        elif operation == "ADD":
            env_df['draw_{}'.format(i)] = envelope_draw + cause_draw
        else:
            raise ValueError("""operation arg for add_subtract_envelope
                                takes either ADD or SUBTRACT
                             """)
    env_df.drop(
        [c for c in env_df.columns if cause in c], axis=1, inplace=True)
    env_df.drop(
        [c for c in env_df.columns if "ENV" in c], axis=1, inplace=True)

    if env_df['draw_{}'.format(i)].isnull().values.any():
        logging.info('Nans in draw_{} for sev {}'.format(i, sev))

    if np.all(env_df['draw_{}'.format(i)].dtype != np.float64):
        raise TypeError("The value needs to be a float64, currently {}".format(
            env_df['draw_{}'.format(i)].dtype))

    if np.all(env_df['draw_{}'.format(i)].values < 0):
        raise ValueError(
           "{} envelope prevalence draws need to be >= 0".format(sev))
    return env_df


def rename_draw_cols(df, append_str):
    """
    Args:
        df(pandas DataFrame): a dataframe of draws
        append_str(str): string to append to "draw_{draw_num}" in the draw
            columns.

    Returns:
        A dataframe with new name for draw columns.
    """
    df = df.copy()
    draws = [x for x in df.columns if 'draw' in x]
    renames = ["draw_{i}_{r}".format(i=i, r=append_str) for i in range(1000)]
    df.rename(columns=dict(list(zip(sorted(draws, key=lambda x: x[5]),
              sorted(renames, key=lambda x: x[5])))), inplace=True)
    return df


def get_best_corrected_blindness(squeezed_blind_re, blindness_envelope,
                                 location_id):
    """
    Calculate best corrected blindness by subtracting blindness due to
    refractive error from the blindness envelope. This modelable entitiy is an
    input into the super squeeze.

    Args:
        squeezed_blind_re (dataframe): Dataframe of estimates for blindness due
            to refractive error.
        blindness_envelope (dataframe): Dataframe of estimates for the
            blindness envelope.
        location_id (int): The location to pull and adjust draws for.

    Returns:
        Dataframe of best corrected blindness estimates.

    """
    logging.info("running the bc blindness code for {}".format(location_id))
    keepcols = ['measure_id', 'metric_id', 'sex_id', 'year_id', 'age_group_id',
                'location_id']
    keepcols.extend(('draw_{i}'.format(i=i) for i in range(1000)))

    logging.info("now getting results for location id {}".format(location_id))
    merged = pd.merge(squeezed_blind_re,
                      blindness_envelope,
                      on=['location_id',
                          'year_id',
                          'sex_id',
                          'age_group_id',
                          'measure_id',
                          'metric_id'],
                      suffixes=['_RE', '_ENV'])
    # Subtract out refractive error
    for i in range(0, 1000):
        merged['draw_{}'.format(i)] = merged[
            'draw_{}_ENV'.format(i)] - merged['draw_{}_RE'.format(i)]

    merged = merged[keepcols]
    merged['modelable_entity_id'] = 9805

    envelope = blindness_envelope.query(
        "sex_id == 2 and year_id == 2019 and age_group_id == 235"
    ).draw_0.values[0]
    re = squeezed_blind_re.query(
        "sex_id == 2 and year_id == 2019 and age_group_id == 235"
    ).draw_0.values[0]
    final = merged.query(
        "sex_id == 2 and year_id == 2019 and age_group_id == 235"
    ).draw_0.values[0]

    if (envelope - re) != final:
        raise ValueError("""
                         The difference of the blindness and the squeezed re
                         are not equal to the final estimates""")
    return merged


def adjust_near_vision_prevalence(location_id):
    """
    Change measure_id from proportion to prevalence. Pull the data, convert
    proportion to prevalence, and save.

    Args:
         location_id (int): The location to pull and adjust draws for.

    Returns:
        Dataframe of near vision estimates with prevalence as the measure.

    """
    logging.info("running the near vision prevalence adjustment for {}".format(
        location_id))

    near_vision = get_vision_draws(
        meid=2424,
        measure_id=18,
        location_id=location_id,
        age_group_id=cfg.age_ids_list,
        status=cfg.current_status,
        gbd_round_id=cfg.gbd_round_id,
        decomp_step=cfg.step)
    near_vision['measure_id'] = 5
    near_vision['modelable_entity_id'] = 20969
    return near_vision


def check_df_for_nulls(df, entity_name):
    """
    Check for nulls across all columns of a dataframe.

    Arguments:
        df (Dataframe): Pandas dataframe of estimates to check for nulls.
        entity_name (str): Entity representative of the estimates being checked
            e.g. cause_id.

    Returns:
        NONE

    Raises:
        ValueError if nulls are present at any point in the dataframe.
    """
    if df.isnull().values.any():
        raise ValueError("ME {} has null values".format(entity_name))


def check_for_nulls(cause_dict):
    """
    Take a dictionary of dataframes and check for nulls across all columns.

    Arguments:
        cause_dict (dictionary): Dictionary of cause specific dataframes.

    Returns:
        NONE
    """
    causes = list(cause_dict.keys())
    logging.info("Checking for null values in {} MEs".format(len(causes)))
    for cause in causes:
        df = cause_dict[cause]
        check_df_for_nulls(df, cause)


def check_df_for_negative_draws(df, entity_name):
    """
    Check for negatives across all columns of a dataframe.

    Arguments:
        df (Dataframe): Pandas dataframe of estimates to check for negatives.
        entity_name (str): Entity representative of the estimates being checked
            e.g. cause_id.

    Returns:
        NONE

    Raises:
        ValueError if nulls are present at any point in the dataframe.

    """
    if np.any((df < 0).any()):
        raise ValueError("Cause {} contains negative draw values".format(
            entity_name))


def check_for_negative_draws(cause_dict):
    """
    Take a dictionary of dataframes and check for negative draws across all
    columns.

    Arguments:
        cause_dict (dictionary): Dictionary of cause specific dataframes.

    Returns:
        NONE
    """
    causes = list(cause_dict.keys())
    for cause in causes:
        logging.info("Checking {} for negative draws".format(cause))
        df = cause_dict[cause]
        check_df_for_negative_draws(df, cause)


def get_draw_col_count(df, draw_col, entity):
    """
    Obtain the number of draw columns for a specified dataframe.

    Arguments:
        df (Dataframe): Pandas dataframe of draws to obtain the column count
            for.
        draw_col (str): The label used for all draw columns present in the
        dataset. (e.g this would be "draw_" for the draw columns "draw_0",
        "draw_501", "draw_999", etc.
        entity (str): Descriptor used to identify the dataframe for
        diagnostic purposes (e.g. the cause name).

    Returns:
        NONE

    """
    expression = '^{}'.format(draw_col)
    draws = df.filter(regex=expression, axis=1)
    col_count = draws.shape[1]
    logging.info(
        "The number of draw columns for {} is {}".format(entity, col_count))


def make_directory(dir_path):
    """
    Check if a directory exists at a specified path and create a new one at
    that path if it doesn't.

    Arguments:
        dir_path (str): The path to either check the existence of or create.

    Returns:
        NONE
    """
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path)
        except OSError as e:
            if e.errno == errno.EEXIST:
                logging.info("{} already exists".format(dir_path))
            else:
                raise e


def transform_values(operator_name, val1, val2):
    """
    Apply a single specified operation to two variables to produce a result.
    This function is written to work with any operators from python's operator
    module (https://docs.python.org/3/library/operator.html).

    Arguments:
        operator_name (str): The name of the operator to use for the given
            transformation (e.g. mul for multiplication, truediv for division
            and sub for subtraction.
            operator module to use for making a specific transformation.
        val1 (int/intlist/series): The first numeric value or values that need
            to be transformed. Please note that order matters here. For
            example if you are dividing values, this variable would represent
            the numerator.
        val2 (int/intlist/series): The second numeric value or values that need
            to be transformed. Please note that order matters here. For
            example if you are dividing values, this variable would represent
            the denominator.

    Returns:
        Either a list integer or series that has been transformed for the
        specified operation.

    """
    return getattr(operator, operator_name)(val1, val2)


def transform_summary(operator_name,
                      df1,
                      df2,
                      df1_col,
                      df2_col,
                      result_col,
                      merge_cols,
                      negative_vals=False):
    """
    Transform two dataframes by a specified operation. These
    transformations will not occure at the draw level. This function is
    written to work with any operators from python's operator module
    (https://docs.python.org/3/library/operator.html).

    Arguments:
        operator_name (str): The name of the operator to use for the given
            transformation (e.g. mul for multiplication, truediv for division
            and sub for subtraction).
        df1 (Dataframe): First pandas dataframe to transform.
        df2 (Dataframe): Second pandas dataframe to transform.
        df1_col (str): The name of the value column for the first dataframe
            (e.g. mean_value).
        df2_col (str): The name of the value column for the second dataframe
            (e.g. mean_value).
        result_col (str): The name of the column that should contain all
            values of the resulting transformation (e.g. mean_value).
        merge_cols (strlist): The column or columns to merge two dataframe of
            on for non-draw level transformations.
        negative_vals ():

    Returns:
        Dataframe of values that have been transformed by the specified
        operation.

    """
    df1 = df1.rename(columns={df1_col: 'mean1'})
    df2 = df2.rename(columns={df2_col: 'mean2'})

    master = df1.merge(df2, how='inner', on=merge_cols)

    input1 = master['mean1']
    input2 = master['mean2']

    if negative_vals:
        input2 = np.minimum(input1 * 0.9, input2)

    master[result_col] = transform_values(operator_name,
                                          input1,
                                          input2)

    master = master.drop(labels=['mean1', 'mean2'], axis=1)

    return master
