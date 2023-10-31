
import pandas as pd
import numpy as np
import getpass
import sys
import itertools
from db_queries import get_population
from db_tools.ezfuncs import query
import datetime
import time

from clinical_info.Functions import gbd_hosp_prep, hosp_prep
from clinical_info.Mapping import clinical_mapping, bundle_swaps

def make_square(df):
    """
    takes a dataframe and returns the square of every age/sex/bundle id which
    exists in the given dataframe but only the years available for each
    location id
    """

    def expandgrid(*itrs):
        # create a template df with every possible combination of
        #  age/sex/year/location to merge results onto
        # define a function to expand a template with the cartesian product
        product = list(itertools.product(*itrs))
        return({'Var{}'.format(i+1):[x[i] for x in product] for i in range(len(itrs))})

    agid_map = df[['age_start', 'age_group_id']].drop_duplicates()

    # ages = df.age_group_id.unique()
    ages = df.age_start.unique()
    sexes = df.sex_id.unique()
    bundles = df.bundle_id.unique()
    dat = pd.DataFrame(expandgrid(ages,
                                  sexes, [69],
                                  df.year_id.unique(),
                                  bundles))
    dat.columns = ['age_start', 'sex_id', 'location_id', 'year_id',
                   'bundle_id']
    exp_rows = dat.shape[0]

    test = df.merge(dat, how='outer', on=dat.columns.tolist())
    assert dat.shape[0] - df.shape[0] == test['count'].isnull().sum(), "Unexpected rows created"
    test.loc[test['count'].isnull(), 'count'] = 0
    df = test.copy()

    # get age group onto the expanded rows
    assert agid_map.shape[0] == df.age_start.unique().size, "too many rows in agid map"
    df.drop('age_group_id', axis=1, inplace=True)
    df = df.merge(agid_map, how='left', on='age_start')
    assert df.age_group_id.isnull().sum() == 0, "There should not be null group IDs"
    assert df.shape[0] == exp_rows, "df row count was not equal to expected rows"
    return(df)


def check_dx_types(inc, prev):
    """
    Test that only 'in' and 'out' are the values present in the inc and prev DFs
    """
    exp_dx_types = set(['in', 'out'])
    inc_dx_types = set(inc['diagnosis_type'].unique())
    prev_dx_types = set(prev['diagnosis_type'].unique())
    assert not exp_dx_types.symmetric_difference(prev_dx_types), "unexpected dx values"
    assert not exp_dx_types.symmetric_difference(inc_dx_types), "unexpected dx values"  
    return

def get_data(gbd_round_id, use_otp=False):

    if gbd_round_id == 5:
        assert False, "Too much work to make this whole thing backwards compatible, Lookup the commit history if you need 2017 prep code"
        inc = pd.read_excel(FILEPATH)
        prev = pd.read_excel(FILEPATH)
    elif gbd_round_id == 6 or gbd_round_id == 7:
        inc = pd.read_stata(FILEPATH)
        prev = pd.read_stata(FILEPATH)
        if not use_otp:
            check_dx_types(inc, prev)
            inc = inc.query("diagnosis_type == 'in'").drop('diagnosis_type', axis=1)
            prev = prev.query("diagnosis_type == 'in'").drop('diagnosis_type', axis=1)


    else:
        assert False, "gbd round id isn't an acceptable value."

    return prev, inc

def prep_cols(df):
    # prep to get cols in line with other processes
    df['location_id'] = 69
    df.rename(columns={'year': 'year_id'}, inplace=True)
    # add sex id
    df['sex_id'] = 1
    df.loc[df.sex == "f", 'sex_id'] = 2
    assert df.sex_id.isnull().sum() == 0
    assert (df['sex'].value_counts().reset_index(drop=True) == df['sex_id'].value_counts().reset_index(drop=True)).all()
    return df

def agg_nn_ages(df):
    """
    data is in count space so we can easily group it together to go from neonatal ages to under 1
    """
    pre_count = df['count'].sum()
    df.loc[df['age_start'] < 1, ['age_start', 'age_end']] = [0, 0]
    assert df[['age_start', 'age_end']].drop_duplicates().shape[0] == 21
    
    groups = ['age_start', 'age_end', 'sex_id', 'year_id', 'location_id', 'bundle_id']
    df = df.groupby(groups).agg({'count': 'sum'}).reset_index()
    assert pre_count == df['count'].sum(), "Counts changed"
    return df

def extract_age(df, clinical_age_group_set_id, aggregate_neonatals=True):
    """
    For GBD2019 ages have been stored as strings, including neo-natal groups.
    """
    df['age'] = df['age'].astype(str)
    # get ages as ints
    pre = df.shape[0]
    df = pd.concat([df, df.age.str.split("-", expand=True)], axis=1)
    assert df.shape[0] == pre
    df.rename(columns={0: 'age_start', 1: 'age_end'}, inplace=True)
    df.loc[df.age_start == "95+", ['age_start', 'age_end']] = ["95", "124"]
    df.loc[df.age_start == "64", 'age_start'] = "65"
    
    nnd = {'6 days': [0, 0.01917808],
           '28 days': [0.01917808, 0.07671233],
           '364 days': [0.07671233, 1]}
    for key, value in nnd.items():
        df.loc[df.age_end == key, ['age_start', 'age_end']] = value

    # remove days from nn age groups
    df['age_end'].str.replace(" days", "").unique()
    
    df['age_start'] = pd.to_numeric(df['age_start'])
    df['age_end'] = pd.to_numeric(df['age_end'])
    
    if aggregate_neonatals:
        df = agg_nn_ages(df)

    df['age_end'] = df['age_end'] + 1
    df = gbd_hosp_prep.all_group_id_start_end_switcher(df.copy(), remove_cols=True,
                                                       clinical_age_group_set_id=clinical_age_group_set_id)
    return df


def square_it(df):
    # make data square
    pre = df['count'].sum()
    df = make_square(df)
    assert pre == df['count'].sum()
    return df


def get_sample(df, gbd_round_id, decomp_step):
    # get population for sample size
    ages = df.age_group_id.unique().tolist()
    pop = get_population(year_id = df.year_id.unique().tolist(), location_id=69,
                         age_group_id=ages, sex_id=[1,2],
                         gbd_round_id=gbd_round_id, decomp_step=decomp_step)
    pop.rename(columns={'population': 'sample_size', 'run_id': 'population_run_id'}, inplace=True)

    pop = pop.groupby(pop.columns.drop('sample_size').tolist()).agg({'sample_size': 'sum'}).reset_index()

    # merge on sample size
    df = df.merge(pop, how='left', on=['age_group_id', 'location_id',
                                       'sex_id', 'year_id'])
    assert df.sample_size.isnull().sum() == 0
    return df


def prep_final_formatting(df, nid_dict):

    # create year_end/start and rename counts to cases
    df.rename(columns={'year_id': 'year_start',
                       'count': 'cases'}, inplace=True)
    df['year_end'] = df['year_start']

    # create the rate estimate column
    df['mean'] = df['cases'] / df['sample_size']

    # merge on nid
    for y in df.year_start.unique():
        df.loc[df['year_start'] == y, 'nid'] = nid_dict[y]

    # create upper lower with nulls
    df['upper'] = np.nan
    df['lower'] = np.nan

    df['source_type_id'] = 10
    df['estimate_id'] = 17
    df['representative_id'] = 1
    df['diagnosis_id'] = 1
    return df


def fill_missing_square_data(df):
    og = df[df['count'] != 0].copy()
    df = df[df['count'] == 0].copy()
    pre = df.shape[0]
    df.drop(['age_end'], axis=1, inplace=True)
    df = df.merge(og[['age_start', 'age_end']].drop_duplicates(),
                  how='left', on='age_start')
    assert pre == df.shape[0]
    df = pd.concat([og, df], sort=False, ignore_index=True)
    return df


if __name__ == "__main__":
    run_id = 25
    gbd_round_id = 7
    decomp_step = 'iterative'
    map_version = 30

    nid_dict = {
        1991: 336846,
        1992: 336845,
        1993: 336844,
        1994: 336843,
        1995: 336842,
        1996: 336841,
        1997: 336840,
        1998: 336839,
        1999: 336838,
        2000: 336837,
        2001: 336836,
        2002: 336835,
        2003: 336834,
        2004: 336833,
        2005: 336832,
        2006: 336831,
        2007: 336830,
        2008: 336829,
        2009: 336828,
        2010: 336827,
        2011: 336826,
        2012: 336825,
        2013: 336824,
        2014: 336822,
        2015: 336820,
        2016: 336817,
        2017: 406980
    }
    prev, inc = get_data(gbd_round_id=gbd_round_id)
    df = pd.concat([prev, inc], sort=False, ignore_index=True)
    bak = df.copy()
    initial_bundles = df.bundle_id.unique()

    if gbd_round_id == 7:
        df = bundle_swaps.apply_bundle_swapping(df=df, map_version_older=24,
                                                 map_version_newer=28,
                                                 drop_data=True)
        initial_bundles = df.bundle_id.unique()

    df = prep_cols(df)
    df = extract_age(df, clinical_age_group_set_id=2, aggregate_neonatals=True)

    print(df.shape)
    df = clinical_mapping.apply_restrictions(df, age_set='age_group_id', cause_type='bundle',
                                             map_version=map_version)
    df = hosp_prep.group_id_start_end_switcher(df, remove_cols=False, clinical_age_group_set_id=1)
    df = square_it(df)
    df = fill_missing_square_data(df)
    df.drop(['age_start', 'age_end'], axis=1, inplace=True)

    print(df.shape)

    df = get_sample(df, gbd_round_id=gbd_round_id, decomp_step=decomp_step)

    df = prep_final_formatting(df.copy(), nid_dict)

    print(df.shape)
    df = clinical_mapping.apply_restrictions(df, age_set='age_group_id', cause_type='bundle',
                                             map_version=map_version)
    print(df.shape)

    final_bundles = df.bundle_id.unique()

    diffs = set(initial_bundles).symmetric_difference(set(final_bundles))
    assert not diffs, "No bundle differences allowed. symmetric diff if {}".format(diffs)

    print("Begin writing intermediate data...")
    today = datetime.datetime.now().strftime("%Y-%m-%d")
 
    write_path = FILEPATH.format(run_id)
    df.to_csv(write_path, index=False)
    print("All done!")
