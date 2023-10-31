"""
Tools related to storing and pulling cached populations
"""

import glob
import os
import warnings
import pandas as pd
import numpy as np
from db_queries import get_population
from clinical_info.Functions import hosp_prep, demographic_utils as du


def _get_pop_filepaths(run_id):
    """Incase we want to change paths just change this and the writer path below"""
    pop_files = glob.glob(FILEPATH)
    pop_files = sorted(pop_files)
    return pop_files


def get_cached_pop(run_id, sum_under1=False, drop_duplicates=True):
    """Returns a df of the full population cache for a given run_id

    run_id (int): determines which cached pop run to pull
    drop_duplicates (bool): Some locations/years are duplicated in inp data, get rid of dupes!
    """

    pop_files =  _get_pop_filepaths(run_id)
    pop = pd.concat([pd.read_csv(f) for f in pop_files],
                    sort=False, ignore_index=True)

    if pop['pop_run_id'].unique().size != 1:
        raise ValueError(f"There are multiple population run_ids "\
                         f"present {pop['pop_run_id'].unique()}")
    if drop_duplicates:
        pop = pop.drop_duplicates()

    if sum_under1:
        groups = pop.drop('population', axis=1).columns.tolist()
        pop = du.sum_under1_data(df=pop,
                                 group_cols=groups,
                                 sum_cols=['population'],
                                 clinical_age_group_set_id=2)
    return pop


def check_pop_files(run_id, exp_sources):
    """
    Compares the CSVs present for a single run against what we'd expect from the database
    Used in master_data to confirm all parallel jobs finished"""

    pop_files =  _get_pop_filepaths(run_id)

    pop_files = [os.path.basename(pf)[:-4] for pf in pop_files]

    diff = set(pop_files).symmetric_difference(exp_sources)
    if diff:
        raise ValueError(f"There are some missing files. Sources: {diff}")

    return


def get_md_files(run_id):
    """get a list of filepaths for a master data run"""

    ppath = FILEPATH
    files = glob.glob(f"{ppath}/*.H5")
    files = sorted(files)
    return files


def _get_demos(file, df):
    """extract age, loc and year from master data source file"""

    demo_cols = ['age_group_id', 'location_id', 'year_start']

    if file is None:
        tmp = df[demo_cols].drop_duplicates().copy()
    else:
        print(f"Reading in master data file for {os.path.basename(file)}")
        tmp = pd.read_hdf(file, columns=demo_cols).drop_duplicates()

    # get the "good" age groups for post age splitting
    good_ages = hosp_prep.get_hospital_age_groups(clinical_age_group_set_id=2).age_group_id.unique().tolist()

    # each set of demo values
    # should be a list so they play well with shared funcs
    ages = tmp.age_group_id.unique().tolist()
    ages = list(set(ages + good_ages))  # combine with good ages that we'll need after age-splitting
    sexes = [1, 2, 3]  # manually pull all 3 values
    locations = tmp.location_id.unique().tolist()
    years = tmp.year_start.unique().tolist()  # alternatively we could just use a range from 1990:this year

    return ages, sexes, locations, years, tmp


def _get_claims_locations(gbd_round_id, decomp_step):
    """
    Some of the claims formatting scripts use cached pop, but cached pop is
    mostly based on inpatient master_data process. This function gets all
    location ids for all non-marketscan claims sources.
    """

    claims_location_names = ["Poland", "Taiwan (Province of China)",
                             "Russian Federation", "Singapore"]

    location_metadata = db_queries.get_location_metadata(
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        location_set_id=35)

    national_location_ids = location_metadata.loc[
        location_metadata.location_name.isin(claims_location_names),
        "location_id"].unique().tolist()

    subnational = location_metadata.loc[
        location_metadata.parent_id.isin(national_location_ids),
        "location_id"].unique().tolist()

    return national_location_ids + subnational


def _get_claims_demos(file, df):
    """
    Some of the claims formatting scripts use cached pop, but cached pop is
    mostly based on inpatient master_data process. This function gets population
    for all claims locations, all possible years, all sexes, and ages that
    existed in master_data
    """

    ages, _, _, _ = _get_demos(file=file, df=df)
    sexes = [1, 2, 3]  # manually pull all 3 values
    locations = _get_claims_locations()
    years = list(range(1990, 2020))

    return ages, sexes, locations, years


def _map_agged_age_groups(pop, age_dict):

    pop['new_age_group_id'] = np.nan

    for new_age, old_ages in age_dict.items():
        for old_age in old_ages:
            pop.loc[pop['age_group_id'] == old_age, 'new_age_group_id'] = new_age
            
    if pop.isnull().sum().sum() != 0:
        raise ValueError("There should not be any null values present")

    pop.drop('age_group_id', axis=1, inplace=True)
    pop.rename(columns={'new_age_group_id': 'age_group_id'}, inplace=True)

    return pop


def test_pop_results(pop, tmp, ages, sexes=[1, 2, 3]):
    """
    pop (pd.DataFrame): Contains GBD population estimates
    tmp (pd.DataFrame): Contains clinical demographic info
    
    The test back merges pop onto tmp (pop was created using tmp)
    to ensure that all demographic values have been correctly returned
    """
    tmp_list  = []
    for s in sexes:
        tmp2 = tmp.copy()
        tmp2['sex_id'] = s
        tmp_list.append(tmp2)
    tmp = pd.concat(tmp_list, sort=False, ignore_index=True)
    tmp.rename(columns={'year_start': 'year_id'}, inplace=True)

    pop_ages = pop['age_group_id'].unique().tolist()
    # all ages returned by pop should be what we requested
    age_diff = set(pop_ages) - set(ages)
    assert not age_diff, f"what happened {age_diff}"

    # don't need pop for non-good age groups
    tmp = du.retain_good_age_groups(df=tmp,
                                    clinical_age_group_set_id=2)

    demo_cols = ['age_group_id', 'sex_id', 'location_id', 'year_id']
    m = tmp.merge(pop, how='left', on=demo_cols)
    if m.isnull().sum().sum() != 0:
        raise ValueError(f"We've got Nulls  {m[m['population'].isnull()]}")
    return


def get_source_pop(gbd_round_id, decomp_step, pop_run_id,
                   aggregate_single_ages=False,
                   file=None, df=None):
    """
    file (str): The master data filepath to read in, if none then you must pass a df

    df (pandas.DataFrmae): The master data file, if none then you must pass a filepath

    gbd_round_id (int): identifies the gbd round to get pop from

    decomp_step (str): identifies which decomp step population to use

    """

    if file is None and df is None:
        raise ValueError("We must have either the filepath or the masterdata itself")
    if isinstance(file, str) and isinstance(df, pd.DataFrame):
        raise ValueError("We must have a filepath or the data, not both")

    ages, sexes, locations, years, tmp = _get_demos(file=file, df=df)

    pop = get_population(gbd_round_id=gbd_round_id, decomp_step=decomp_step,
                         run_id=pop_run_id,
                         age_group_id=ages,
                         sex_id=sexes,
                         location_id=locations,
                         year_id=years)


    pop.rename(columns={'run_id': 'pop_run_id'}, inplace=True)

    # confirm pop outputs match input data
    test_pop_results(pop=pop, tmp=tmp, ages=ages)

    return pop


def test_id_cols(pop, id_cols):
    """Test that the data is square"""
    failed_cols = []
    for year in pop['year_id'].unique():
        year_pop = pop[pop['year_id'] == year]
        for col in id_cols:
            unique_col_values = year_pop[col].value_counts().unique()
            if unique_col_values.size != 1:
                failed_cols.append(col)
    if failed_cols:
        raise ValueError(f"These columns don't have uniform unique values {failed_cols}")
    return


def write_source_pop(pop, run_id, source_name):
    """Write a single pop file to a given run. Used in master_data and
    the write all pops function below

    Params:
        pop (pd.DataFrame): df of GBD population estimates
        run_id (int): clinical run ID
        source_name (str): The source name we use to process data

    Returns:
        Nothing
    """

    id_cols = ['age_group_id', 'location_id', 'year_id', 'sex_id', 'pop_run_id']

    ppath = FILEPATH
    print(f"Writing {source_name} to csv\n")
    write_path = f"{ppath}/{source_name}.csv"
    if os.path.exists(write_path):
        print("There is already a cached pop file present")
        print("Appending on population to existing files")
        existing_df = pd.read_csv(write_path)
        pop = pd.concat([pop, existing_df], sort=False, ignore_index=True)

        pop.drop_duplicates(subset=id_cols, inplace=True)
    else:
        pass

    test_id_cols(pop, id_cols)
    pop.to_csv(write_path, index=False)

    return


def write_all_source_pops(run_id, aggregate_single_ages):
    """If you want to gen a population cache for all sources _after_ master data has
    been run this use this. Otherwise they should be created automatically within master data
    """

    files = get_md_files(run_id)

    for file in files:
        source_name = os.path.basename(file)[:-3]
        pop = get_source_pop(file=file, aggregate_single_ages=aggregate_single_ages)
        write_source_pop(pop, run_id, source_name)

    return
