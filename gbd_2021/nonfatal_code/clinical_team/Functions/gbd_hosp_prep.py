"""
Clinical informatics functions
must run on the GBD environment or a clone
"""
import pandas as pd
import numpy as np
import platform
import getpass
import datetime
import sys
import warnings
from db_queries import get_cause_metadata, get_population, get_covariate_estimates, get_location_metadata
from db_tools.ezfuncs import query
import os

from clinical_info.Functions import hosp_prep
from clinical_info.Functions import cached_pop_tools

if platform.system() == "Linux":
    root = FILEPATH
else:
    root = FILEPATH


def get_sample_size(df, gbd_round_id, decomp_step, clinical_age_group_set_id,
                    fix_group237=False,
                    use_cached_pop=False,
                    run_id=None):
    """
    This function attaches sample size to hospital data.  It's for sources that
    should have fully covered populations, so sample size is just population.
    Checks if age_group_id is a column that exists and if not, it attaches it.

    Parameters
        df: Pandas DataFrame
            contains the data that you want to add sample_size to.  Will add
            pop to every row.
    """
    # bit of a patch as we move away from year_end with single year span
    if 'year_id' in df.columns:
        df.rename(columns = {'year_id' : 'year_start'}, inplace = True)
        df['year_end'] = df['year_start']

    # process
    ## attach age group id to data
    ## get pop with those age group ids in the data
    ## attach pop by age group id
    if 'age_group_id' not in df.columns:
        # pull age_group to age_start/age_end map
        age_group = hosp_prep.get_hospital_age_groups(clinical_age_group_set_id=clinical_age_group_set_id)

        # merge age group id on
        pre = df.shape[0]
        df = df.merge(age_group, how='left', on=['age_start', 'age_end'])
        assert df.shape[0] == pre, "number of rows changed during merge"
        assert df.age_group_id.notnull().all(), ("age_group_id is missing "
            "for some rows")

    # get population
    if use_cached_pop:
        if run_id is None:
            raise ValueError("run_id cannot be none if you want to pull cached population")
        if gbd_round_id == 7 and decomp_step in ('step1'):
            sum_under1 = True
        else:
            sum_under1 = False
        pop = cached_pop_tools.get_cached_pop(
            run_id=run_id, sum_under1=sum_under1)
        # retain expected col name
        pop.rename(columns={'pop_run_id': 'run_id'}, inplace=True)

    else:
        pop = get_population(age_group_id=list(df.age_group_id.unique()),
                             location_id=list(df.location_id.unique()),
                             sex_id=[1,2],
                             year_id=list(df.year_start.unique()),
                             gbd_round_id=gbd_round_id, decomp_step=decomp_step)

    if fix_group237:

        fix_pop = get_population(age_group_id=[235, 32],
                                 location_id=list(df.location_id.unique()),
                                 sex_id=[1,2],
                                 year_id=list(df.year_start.unique()),
                                 gbd_round_id=gbd_round_id, decomp_step=decomp_step)
        pre = fix_pop.shape[0]
        fix_pop['age_group_id'] = 237
        fix_pop = fix_pop.groupby(fix_pop.columns.drop('population').tolist()).agg({'population': 'sum'}).reset_index()
        assert pre/2 == fix_pop.shape[0]

        pop = pd.concat([pop, fix_pop], sort=False, ignore_index=True)
    else:
        pass

    # rename pop columns to match hospital data columns
    pop.rename(columns={'year_id': 'year_start'}, inplace=True)
    pop['year_end'] = pop['year_start']
    pop.drop("run_id", axis=1, inplace=True)

    demography = ['location_id', 'year_start', 'year_end',
                  'age_group_id', 'sex_id']

    # merge on population
    pre_shape = df.shape[0]
    df = df.merge(pop, how='left', on=demography)  # attach pop info to hosp
    assert pre_shape == df.shape[0], "number of rows don't match after merge"
    assert df.population.notnull().all(),\
        "population is missing for some rows. look at this df! \n {}".\
            format(df.loc[df.population.isnull(), demography].drop_duplicates())

    return(df)

def get_bundle_cause_info(df):
    cause_id_info = query(QUERY)

    acause_info = query(QUERY)
 
    acause_info = acause_info.merge(cause_id_info, how="left", on="cause_id")


    rei_id_info = query(QUERY)

    rei_info = query(QUERY)

    rei_info = rei_info.merge(rei_id_info, how="left", on="rei_id")


    acause_info.rename(columns={'cause_id': 'cause_rei_id',
                                'acause': 'acause_rei'}, inplace=True)

    rei_info.rename(columns={'rei_id': 'cause_rei_id',
                             'rei': 'acause_rei'}, inplace=True)

    folder_info = pd.concat([acause_info, rei_info], sort=False)


    folder_info = folder_info.dropna(subset=['bundle_id'])


    folder_info.drop("cause_rei_id", axis=1, inplace=True)


    folder_info.drop_duplicates(inplace=True)


    folder_info.rename(columns={'acause_rei': 'bundle_acause_rei'},
                       inplace=True)

    pre = df.shape[0]
    df = df.merge(folder_info, how="left", on="bundle_id")
    assert pre == df.shape[0]

    return(df)


def all_group_id_start_end_switcher(df, clinical_age_group_set_id, remove_cols=True, ignore_nulls=False):
    """
    Takes a dataframe with age start/end OR age group ID and switches from one
    to the other

    Args:
        df: (Pandas DataFrame) data to swich age labelling
        remove_cols: (bool)  If True, will drop the column that was switched
            from
        ignore_nulls: (bool)  If True, assertions about missing ages
            will be ignored.  Not a good idea to use in production but is useful
            for when you just need to quickly see what ages you have.
    """
    # Determine if we're going from start/end to group id or vise versa
    if sum([w in ['age_start', 'age_end', 'age_group_id'] for w in df.columns]) == 3:
        assert False,\
        "All age columns are present, unclear which output is desired. "\
        "Simply drop the columns you don't want"


    elif sum([w in ['age_start', 'age_end'] for w in df.columns]) == 2:
        merge_on = ['age_start', 'age_end']
        switch_to = ['age_group_id']

    elif 'age_group_id' in df.columns:
        merge_on = ['age_group_id']
        switch_to = ['age_start', 'age_end']
    else:
        assert False, "Age columns not present or named incorrectly"

    # pull in our hospital age groups
    ages = hosp_prep.get_hospital_age_groups(clinical_age_group_set_id=clinical_age_group_set_id)

    # determine if the data contains only our hosp age groups or not
    age_set = "hospital"
    for m in merge_on:
        ages_unique = ages[merge_on].drop_duplicates()
        df_unique = df[merge_on].drop_duplicates()
        # if their shapes aren't the same it's irregular ages
        if ages_unique.shape[0] != df_unique.shape[0]:
            age_set = 'non_hospital'
        elif (ages_unique[m].sort_values().reset_index(drop=True) != df_unique[m].sort_values().reset_index(drop=True)).all():
            age_set = 'non_hospital'

 
    ages_unique.sort_values(merge_on).reset_index(drop=True)
    df_unique.sort_values(merge_on).reset_index(drop=True)
    if ages_unique.equals(df_unique):
        pass  # do nothing if they're equal
    else:
        age_set = 'non_hospital'  # assign to non_hosp if not

    if age_set == 'non_hospital':

        ages = query(QUERY)
        ages.rename(columns={"age_group_years_start": "age_start",
                                      "age_group_years_end": "age_end"},
                                      inplace=True)

        if 'age_end' in merge_on:
            # terminal age in hosp data is 99, switch to 125 so groups aren't duped
            df.loc[df['age_end'] == 100, 'age_end'] = 125
        # drop duplicates age groups that cause rows added in the merge
        duped_ages = [294, 308, 27, 161, 38, 301, 49]
        ages = ages[~ages.age_group_id.isin(duped_ages)]

    dupes = ages[ages.duplicated(['age_start', 'age_end'], keep=False)].sort_values('age_start')

    # merge on the age group we want
    pre = df.shape[0]
    df = df.merge(ages, how='left', on=merge_on)
    assert pre == df.shape[0],\
        "Rows were duplicated, probably from these ages \n{}".format(dupes)

    # check the merge
    if not ignore_nulls:
        for s in switch_to:
            assert df[s].isnull().sum() == 0, ("{} contains missing values from "
                "the merge. The values with Nulls are {}".format(s, df.loc[df[s].isnull(), merge_on].drop_duplicates().sort_values(by=merge_on)))

    if remove_cols:
        # drop the one we don't
        df.drop(merge_on, axis=1, inplace=True)
    return(df)


def get_current_mvid(covariate_id, gbd_round_id, decomp_step):

    df = get_covariate_estimates(covariate_id=covariate_id,
                                            gbd_round_id=gbd_round_id,
                                            decomp_step=decomp_step,
                                            location_id=102, year_id=2005)
    return df['model_version_id'].iloc[0]


def get_covar_for_clinical(run_id, covariate_name_short, **kwargs):

    # read pickle
    p = pd.read_pickle(FILEPATH)

    # extract cov lookup using short name
    cov_lookup = p.central_lookup[covariate_name_short]

    # pull data
    df = get_covariate_estimates(covariate_id=cov_lookup['covariate_id'],
                                 gbd_round_id=cov_lookup['gbd_round_id'],
                                 decomp_step=cov_lookup['decomp_step'],
                                 model_version_id=cov_lookup['model_version_id'],
                                 **kwargs)

    # validate version id
    assert df['model_version_id'].unique().size == 1, "More than 1 model version id is present!!"
    obs = df['model_version_id'].iloc[0]
    exp = cov_lookup['model_version_id']
    if obs != exp:
        raise ValueError((f"Unexpected model version id was returned from "
                          f"get_covar_estimates. Got {obs}, but expected {exp}"))
    return df
