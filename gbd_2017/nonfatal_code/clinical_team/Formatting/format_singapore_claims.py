"""
format singapore claims data
"""
import pandas as pd
import numpy as np
import getpass
import sys
import itertools
from db_queries import get_population
from db_tools.ezfuncs import query
import datetime
import time

user = getpass.getuser()
prep_path = "FILEPATH".format(user)
sys.path.append(prep_path)

import gbd_hosp_prep

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

def get_data():
    maps = pd.read_csv("{FILEPATH}clean_map.csv")


    inc = pd.read_excel("{FILEPATH}/incidence_bundles.xlsx")
    prev = pd.read_excel("{FILEPATH}/prevalence_bundles.xlsx")

    # get bid to bundle name map to add on bundle id
    bundles = query("QUERY", conn_def=DATABASE)
    return prev, inc, bundles, maps


def get_bid(prev, inc, bundles):
    inc = inc.merge(bundles, how='left', on='bundle_name')
    # reviewed the csv I sent to them
    inc.loc[inc.bundle_name == "Epilepsy due to other meningitis", 'bundle_id'] = 44
    print(inc[inc.bundle_id.isnull()].bundle_name.unique())
    assert inc.bundle_id.isnull().sum() == 0
    inc['source_file'] = 'incidence'

    prev = prev.merge(bundles, how='left', on='bundle_name')
    prev.loc[prev.bundle_name == 'unknown', 'bundle_id'] = 2872
    print(prev[prev.bundle_id.isnull()].bundle_name.unique())
    assert prev.bundle_id.isnull().sum() == 0
    prev['source_file'] = 'prevalence'

    # bring em together
    df = pd.concat([prev, inc], ignore_index=True)
    return df


def prep_cols(df):
    # prep to get cols in line with other processes
    df['location_id'] = 69
    df.rename(columns={'year': 'year_id'}, inplace=True)
    # add sex id
    df['sex_id'] = 1
    df.loc[df.sex == "F", 'sex_id'] = 2
    assert df.sex_id.isnull().sum() == 0
    return df

def extract_age(df):
    # get ages as ints
    pre = df.shape[0]
    df = pd.concat([df, df.age.str.split("-", expand=True)], axis=1)
    assert df.shape[0] == pre
    df.rename(columns={0: 'age_start', 1: 'age_end'}, inplace=True)
    df.loc[df.age_start == "85+", ['age_start', 'age_end']] = ["85", "124"]
    df.loc[df.age_start == "64", 'age_start'] = "65"
    df['age_start'] = pd.to_numeric(df['age_start'])
    df['age_end'] = pd.to_numeric(df['age_end'])

    df['age_end'] = df['age_end'] + 1
    df = gbd_hosp_prep.all_group_id_start_end_switcher(df.copy(), remove_cols=False)
    return df


def square_it(df):
    # make data square
    pre = df['count'].sum()
    df = make_square(df)
    assert pre == df['count'].sum()
    # merge on measure from our map
    pre_shape = df.shape[0]
    df = df.merge(maps[['bundle_id', 'bid_measure']].drop_duplicates(), how='left', on='bundle_id')
    assert pre_shape == df.shape[0]
    return df


def get_sample(df):
    # get population for sample size
    ages = df.age_group_id.unique().tolist() + [31, 32, 235]
    pop = get_population(year_id = df.year_id.unique().tolist(), location_id=69,
                         age_group_id=ages, sex_id=[1,2])
    pop.drop('run_id', axis=1, inplace=True)
    pop.rename(columns={'population': 'sample_size'}, inplace=True)
    pop.loc[pop.age_group_id.isin([31, 32, 235]), 'age_group_id'] = 160
    pop = pop.groupby(pop.columns.drop('sample_size').tolist()).agg({'sample_size': 'sum'}).reset_index()

    # merge on sample size
    df = df.merge(pop, how='left', on=['age_group_id', 'location_id',
                                       'sex_id', 'year_id'])
    assert df.sample_size.isnull().sum() == 0
    return df


def get_acause(df):
    # append on "cause_name#bundle_id"
    q = """QUERY""".format(tuple(df.bundle_id.unique()))
    cause_name = query(q, conn_def=DATABASE)
    cause_name.loc[cause_name.acause.isnull(), 'acause'] = cause_name.loc[cause_name.acause.isnull(), 'rei']
    assert cause_name.acause.isnull().sum() == 0
    cause_name = cause_name[['bundle_id', 'acause']]
    cause_name['acause'] = cause_name['acause'] + "#" + cause_name['bundle_id'].astype(str)
    # merge it on
    pre = df.shape[0]
    df = df.merge(cause_name, how='left', on='bundle_id')
    return df


def create_cf_cols(df):
    df['cf_raw'] = df['count'] / df['sample_size']
    df['cf_corr'], df['cf_rd'], df['cf_final'] =\
    df['cf_raw'], df['cf_raw'], df['cf_raw']
    return df


def final_col_prep(df, nid_dict):
    # drop all the og columns
    df.drop(['bundle_name', 'sex', 'age', 'source_file',
             'age_group_id', 'age_end', 'count',
             'bundle_id'], axis=1, inplace=True)
    df.rename(columns={'age_start': 'age', 'sex_id': 'sex',
                      'year_id': 'year'}, inplace=True)

    # add nid
    for y in df.year.unique():
        df.loc[df['year'] == y, 'NID'] = nid_dict[y]
    # df['NID'] = nid

    # match the cols in marketcan
    df['iso3'] = "SGP"
    df['list'] = "ICD9_detail"
    df['national'] = 1
    df['region'] = 100
    # match these to MS for Noise reduction
    df['source'] = "_Marketscan_prevalence"
    df.loc[df.bid_measure == "incidence", 'source'] = "_Marketscan_incidence"
    df.drop('bid_measure', axis=1, inplace=True)
    df['source_label'] = df['source']
    df['source_type'] = "Singapore Claims"
    df['subdiv'] = ""

    pre_cols = df.columns
    df = df[['acause', 'NID', 'location_id', 'iso3', 'list', 'national', 'region',
             'source', 'source_label', 'source_type', 'subdiv', 'year',
            'age', 'sex', 'sample_size', 'cf_raw', 'cf_corr', 'cf_rd', 'cf_final']]

    assert not set(pre_cols).symmetric_difference(set(df.columns)), "columns don't align"
    assert df[df.columns.drop('subdiv')].isnull().sum().sum() == 0
    return df


def prep_final_formatting(df, nid_dict):
    """
    the marketscan process in stata outputs 2 files
    """
    stata_format = pd.read_stata("{FILEPATH}/131.dta")
    # get a series of columns that will be filled with np.nan
    null_cols = pd.DataFrame(stata_format.isnull().sum() / float(stata_format.shape[0])).reset_index()
    null_cols.columns = ['col_name', 'null_prop']
    null_cols = null_cols.loc[null_cols['null_prop'] == 1, 'col_name']

    # fill out the other columns
    for y in df.year_id.unique():
        df.loc[df['year_id'] == y, 'nid'] = nid_dict[y]
    #df['nid'] = nid
    df['source_type'] = "Facility - other/unknown"
    df['sex'] = np.nan
    df.loc[df['sex_id'] == 1, 'sex'] = "Male"
    df.loc[df['sex_id'] == 2, 'sex'] = "Female"
    df['year_start'], df['year_end'] = df['year_id'], df['year_id']
    df['cases'] = df['count']
    df['location_name'] = "Singapore"
    df['bundle_id'] = df['bundle_id'].astype(int)
    df['measure'] = np.nan
    df.loc[df['bid_measure'] == "prev", 'measure'] = "prevalence"
    df.loc[df['bid_measure'] == "inc", 'measure'] = "incidence"
    df['mean'] = df['cases'] / df['sample_size']\
    # std for 5 or fewer cases
    df['standard_error'] = (((5 - df['cases']) / df['sample_size']) + df['cases'] * np.sqrt(5 / (df['sample_size'] * df['sample_size']))) / 5
    # std for over 5
    df.loc[df['cases'] > 5, 'standard_error'] = np.sqrt(df.loc[df['cases'] > 5, 'cases']) / df.loc[df['cases'] > 5, 'sample_size']
    df['egeoloc'] = -99
    df['representative_name'] = "Nationally and subnationally representative"
    df['year_issue'], df['sex_issue'], df['age_issue'] = [0.,0.,0.]
    df['age_demographer'] = 1.
    df['unit_type'] = "Person"
    df['unit_value_as_published'] = 1
    df['measure_issue'] = 0
    df['measure_adjustment'] = 0
    df['extractor'] = 'USERNAME'
    df['uncertainty_type'] = "Sample size"
    df['urbanicity_type'] = "Unknown"
    df['recall_type'] = "Not Set"
    df['is_outlier'] = 0

    # fill all the nulls
    cols = df.columns
    for to_null in null_cols:
        if to_null not in cols:
            df[to_null] = np.nan
    
    df.drop(['count', 'age', 'sex_id', 'age_group_id', 'year_id',
             'bid_measure', 'source_file'], axis=1, inplace=True)
    assert not set(df.columns).symmetric_difference(set(stata_format.columns))

    return df


def apply_bundle_restrictions(df, col_to_restrict, drop_restricted=True):
    """
    Function that applies age (and sex) restrictions.  Data is
    expected to have columns "age_start", "age_end", and "sex_id", and of
    course the column that you want to restrict.

    Parameters:
        df: pandas DataFrame
            data that you want to restrict. Must have at least the columns
            "age_start", "age_end", and "sex_id"
        col_to_restrict: string
            Your main data column, the column of interest, the column you want
            to restrict.  Must be a string.

    Returns:
        Dataframe with the other set of age columns from the one you passed in
    """
    start = time.time()
    assert set(['bundle_id', 'age_start', 'age_end', 'sex_id', col_to_restrict]) <=\
        set(df.columns), "you're missing a column"

    if df[col_to_restrict].isnull().sum() > 0 and drop_restricted:
        warnings.warn("There are {} rows with null values which will be dropped".\
            format(df[col_to_restrict].isnull().sum()))

    # store columns that we started with
    start_cols = df.columns

    restrict = pd.read_csv(r"{FILEPATH}/bundle_restrictions.csv")
    restrict = restrict.reset_index(drop=True)  # excel file has a weird index
    restrict = restrict[restrict.bundle_id.notnull()]
    restrict = restrict.drop_duplicates()

    # replace values below 1 with zero in get_cause, we don't differentiate
    # under one years old.  This is necessary, there is a age_start = 0.1 years
    restrict.loc[restrict['yld_age_start'] < 1, 'yld_age_start'] = 0

    # merge on restrictions
    pre = df.shape[0]
    df = df.merge(restrict, how='left', on='bundle_id')
    assert pre == df.shape[0], ("merge made more rows, there's something wrong"
                                " in the restrictions file")

    # set col_to_restrict to zero where male in cause = 0
    df.loc[(df['male'] == 0) & (df['sex_id'] == 1), col_to_restrict] = np.nan

    # set col_to_restrict to zero where female in cause = 0
    df.loc[(df['female'] == 0) & (df['sex_id'] == 2), col_to_restrict] = np.nan

    # set col_to_restrict to zero where age end is smaller than yld age start
    df.loc[df['age_end'] <= df['yld_age_start'], col_to_restrict] = np.nan

    # set col_to_restrict to zero where age start is larger than yld age end
    df.loc[df['age_start'] > df['yld_age_end'], col_to_restrict] = np.nan

    df.drop(['male', 'female', 'yld_age_start', 'yld_age_end'], axis=1,
            inplace=True)

    if drop_restricted:
        # drop the restricted values
        df = df[df[col_to_restrict].notnull()]

    assert set(start_cols) == set(df.columns)

    return(df)


def fill_missing_square_data(df):
    og = df[df['count'] != 0].copy()
    df = df[df['count'] == 0].copy()
    pre = df.shape[0]
    df.loc[df.sex_id == 2, 'sex'] = 'F'
    df.loc[df.sex_id == 1, 'sex'] = 'M'
    df.drop(['bundle_name', 'age_end'], axis=1, inplace=True)
    df = df.merge(og[['bundle_id', 'bundle_name']].drop_duplicates(),
                 how='left', on='bundle_id')
    df = df.merge(og[['age_start', 'age_end']].drop_duplicates(),
                 how='left', on='age_start')
    assert pre == df.shape[0]
    df = pd.concat([og, df], ignore_index=True)
    return df


if __name__ == "__main__":
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
    }
    prev, inc, bundles, maps = get_data()
    df = get_bid(prev, inc, bundles)
    back = df.copy()

    df = back.copy()
    df = prep_cols(df)
    df = extract_age(df)
    print(df.shape)
    df = apply_bundle_restrictions(df, 'count')
    df = square_it(df)
    df = fill_missing_square_data(df)
    df = apply_bundle_restrictions(df, 'count')
    print(df.shape)

    df = get_sample(df)

    df2 = prep_final_formatting(df.copy(), nid_dict)
    print(df2.shape)
    df2['sex_id'] = 1
    df2.loc[df2.sex == "Female", 'sex_id'] = 2
    assert (df2.sex_id.unique() == [2, 1]).all()
    assert df2.bundle_id.unique().size == df.bundle_id.unique().size
    df2 = apply_bundle_restrictions(df2, 'cases')
    df2.drop('sex_id', axis=1, inplace=True)
    print(df2.bundle_id.unique().size, df.bundle_id.unique().size)
    df2.shape
    # make age_end inclusive
    df2['age_end'] = df2['age_end'] - 1

    df = get_acause(df)
    df = create_cf_cols(df)

    df = df[df.bundle_id.isin(maps.bundle_id.unique())]

    df = final_col_prep(df, nid_dict)

    print("Begin writing intermediate data...")
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    # write this to intermediate
    df.to_stata("{FILEPATH}/{}_all_singapore_bundles.dta".format(today), write_index=False)

    for each in df.acause.unique():
        tmp = df[df.acause == each].copy()
        bundle_id = tmp.acause.str.split("#").iloc[0][1]
        tmp.to_stata("{FILEPATH}/{}.dta".format(bundle_id), write_index=False)
    print("Finished writing intermediate data, beginning on formatted...")
    # write df2 to the other place
    df2.to_stata("{FILEPATH}/{}_all_singapore_bundles.dta".format(today), write_index=False)

    strs = ['bundle_name', 'sex', 'source_type', 'location_name', 'measure', 'representative_name', 'unit_type']
    for s in strs:
        df2[s] = df2[s].astype(str)
    for each in df2.bundle_id.unique():
        tmp = df2[df2.bundle_id == each].copy()
        tmp.to_stata("{FILEPATH}/{}.dta".format(each), write_index=False)
    print("All done!")
