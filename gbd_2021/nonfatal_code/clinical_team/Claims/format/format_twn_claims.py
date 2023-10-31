
import pandas as pd
import numpy as np
import getpass
import sys
import itertools
from db_queries import get_population
from db_tools.ezfuncs import query
import datetime
import time
import ipdb
import warnings

from clinical_info.Functions import gbd_hosp_prep
from clinical_info.Mapping import clinical_mapping as cm, bundle_swaps

def repurpose_db_data(run_id, map_version_newer, map_version_older=24):
    """Read in data from the database, filter columns, perform the bundle swap
    between map version retaining only unchanged ICD level bundles, compare
    the diffs and write to a run_id"""

    exp_cols = ['age_group_id', 'sex_id', 'location_id', 'year_start', 'year_end',
                'merged_nid', 'bundle_id', 'estimate_id', 'representative_id',
                'source_type_id', 'diagnosis_id', 'run_id', 'cases',
                'mean', 'upper', 'lower', 'sample_size']
    # pull in the gbd 2019 results
    df = query((QUERY)
    df = df[exp_cols]

    df = df.rename(columns={"merged_nid": "nid"})

    df = bundle_swaps.apply_bundle_swapping(df=df,
                                            map_version_older=map_version_older,
                                            map_version_newer=map_version_newer,
                                            drop_data=True)
    clin_bun = cm.get_active_bundles(estimate_id=[17,21])
    a = set(clin_bun.bundle_id)
    b = set(df.bundle_id.unique())

    total_diffs = a.symmetric_difference(b)
    in_clin_bun_only = a - b
    in_twn_only = b - a

    df['run_id'] = run_id
    # write df to csv
    base = (FILEPATH)
    df.to_csv(f"{base}/{FILENAME}", index=False)
    return df, in_clin_bun_only

def identify_missing_bundles(in_clin_bun_only):
    bnames = query(QUERY)
    df = pd.DataFrame({'bundle_id': list(in_clin_bun_only)})
    df = df.merge(bnames, how='left', on='bundle_id', validate='1:1')
    return df


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

    ages = df.age_start.unique()
    sexes = df.sex_id.unique()
    years = df.year_id.unique()
    bundles = df.bundle_id.unique()
    locations = df.location_id.unique()

    dat = pd.DataFrame(expandgrid(ages,
                                  sexes, locations,
                                  years,
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

def get_data(inp_only, map_round_dict, gbd_round='current'):

    if gbd_round == 'current':
        round_id = max(map_round_dict.keys())
        print('Using the most current map for GBD round {}'.format(round_id))
    else:
        try: 
            map_round_dict[gbd_round]
        except KeyError:
            print('Update the gbd round and map version dictionary')
        round_id = gbd_round
    
    if round_id == 5:
        maps = pd.read_csv(FILEPATH)
        root = FILEPATH
        if inp_only:
            inc = pd.read_stata(root + FILEPATH)
            prev = pd.read_stata(root + FILEPATH)
        else:
            inc = pd.read_stata(root + FILEPATH)
            prev = pd.read_stata(root + FILEPATH)
    else:
        map_v = map_round_dict.get(round_id)
        maps = clinical_mapping.get_clinical_process_data('icg_bundle',
                                map_version = map_v)

        root = FILEPATH
        otp_visits = pd.read_csv(FILEPATH)
        bid_otp = otp_visits[otp_visits.adj_ms_prev_otp == 0].bundle_id.unique().tolist()
        if inp_only:
            inc = pd.read_excel(root + FILEPATH)
            prev = pd.read_excel(root + FILEPATH)
        else:
            inc_list = []
            prev_list = []
            for i in range(1,3,1):
                inc = pd.read_excel(root + 'oip105{}_i_bid.xlsx'.format(i))
                prev = pd.read_excel(root + 'oip105{}_p_bid.xlsx'.format(i))
                if i == 1:
                    inc_otp = inc[inc.bundle_id.isin(bid_otp)]
                    prev_otp = prev[prev.bundle_id.isin(bid_otp)]
                else:
                    inc_otp = inc[~inc.bundle_id.isin(bid_otp)]
                    prev_otp = prev[~prev.bundle_id.isin(bid_otp)]
                
                inc_list.append(inc_otp)
                prev_list.append(prev_otp)
            inc = pd.concat(inc_list)
            prev = pd.concat(prev_list)
    
    # get bid to bundle name map to add on bundle id
    bundles = query(QUERY)
    return prev, inc, bundles, maps

def get_bid(prev, inc, bundles):

    inc = inc.merge(bundles, how='left', on='bundle_id')

    inc.loc[inc.bundle_name == "Epilepsy due to other meningitis", 'bundle_id'] = 44
    print(inc[inc.bundle_id.isnull()].bundle_name.unique())
    assert inc.bundle_id.isnull().sum() == 0
    inc['source_file'] = 'incidence'

    prev = prev.merge(bundles, how='left', on='bundle_id')
    prev.loc[prev.bundle_name == 'unknown', 'bundle_id'] = 2872
    print(prev[prev.bundle_id.isnull()].bundle_name.unique())
    assert prev.bundle_id.isnull().sum() == 0
    prev['source_file'] = 'prevalence'

    df = pd.concat([prev, inc], ignore_index=True)
    df.loc[df.bundle_id.isin(['79']), 'bundle_id'] = 3419

    df.drop(['indicator'], axis = 1, inplace = True)

    return df


def prep_cols(df):
    # prep to get cols in line with other processes
    df['location_id'] = 8
    
    # all data is from 2016
    df['year_id'] = 2016

    df.rename(columns={'year': 'year_admin',
                        'sex': 'sex_id',
                        'age_ihmec': 'age',
                        'measurement' : 'count',
                        'enrol': 'sample_size'}, inplace=True)

    assert df.sex_id.isnull().sum() == 0
    return df

def extract_age(df, round_id):
    
    if round_id > 5:
        df['age'] = df.age.astype(str)
        nn = df.loc[df.age.str.endswith('d'), 'age'].unique().tolist()
        bids_nn = df[df.age.isin(nn)].bundle_id.unique()

        for s in df.sex_id.unique():
            sample = df.loc[(df.sex_id == s) & (df.age.isin(nn)),
                'sample_size'].unique().sum()
        
            df.loc[(df.age == 0) & (df.sex_id == s),
                'sample_size'] = sample + df.loc[(df.age==0) &
                                            (df.sex_id == s),
                                            'sample_size']
            for b in bids_nn:
                count = df.loc[(df.age.isin(nn)) & (df.bundle_id == b) & 
                        (df.sex_id == s),'count'].unique().sum()
                df.loc[(df.age ==0) & (df.bundle_id == b) & 
                        (df.sex_id == s), 'count'] = count + df.loc[(df.age == 0) & 
                                                                (df.bundle_id == b) & 
                                                                (df.sex_id == s), 'count']
        df = df[~df.age.isin(nn)]
    
    # get ages as ints
    df['age_start'] = np.nan
    df['age_end'] = df['age']
    
    if round_id == 5: 
        df['age_end'] = df.age_end.str.lstrip('0')
        df.loc[df.age_end == '', 'age_end'] = '0'

    df['age_start'] = pd.to_numeric(df['age_start'])
    df['age_end'] = pd.to_numeric(df['age_end'])

    df['age_start'] = df['age_end'] - 4
    df.loc[df.age == '0', ['age_start', 'age_end']] = 0
    df.loc[df.age == '95', ['age_start', 'age_end']] = [95, 124]
    df.loc[df.age_end == 4, 'age_start'] = 1
    df['age_end'] = df['age_end'] + 1

    df = gbd_hosp_prep.all_group_id_start_end_switcher(df.copy(), remove_cols = False)
    return df


def square_it(df):
    # make data square
    pre = df['count'].sum()
    cols = ['sex_id', 'bundle_id', 'age_group_id', 'location_id', 'year_id']

    temp = df.groupby(cols).agg({'count': 'sum'}).reset_index()
    df = df.merge(temp, how = 'left', on = cols, suffixes = ['_old', '_new'])
    df.drop(['count_old', 'year_admin', 'unit'], axis = 1, inplace = True)
    df.drop_duplicates(inplace = True)
    df.rename(columns={'count_new':'count'}, inplace = True)

    df = make_square(df)
    assert pre == df['count'].sum()
    # merge on measure from our map
    pre_shape = df.shape[0]
    bid_m = pd.read_csv(FILEPATH)

    df = df.merge(bid_m.drop_duplicates(), how='left', on='bundle_id')
    df.rename({'measure_id': 'bid_measure'}, axis=1, inplace=True)
    assert pre_shape == df.shape[0]
    return df


def get_sample(df):

    pre = df.shape[0]
    back = df[df.sample_size.notnull()]
    back = back[['age_group_id', 'sex_id', 'sample_size']].drop_duplicates()
    
    df.drop(['sample_size'], axis = 1, inplace = True)
    df = df.merge(back, how = 'left', on = ['age_group_id', 'sex_id']) 
    df.drop_duplicates(inplace = True)
    
    assert df.sample_size.isnull().sum() == 0
    assert df.shape[0] == pre 
    return df


def get_fake_acause(df):
    q = """QUERY""".format(tuple(df.bundle_id.unique()))
    cause_name = query(q)
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
    df.drop(['bundle_name', 'sex', 'age', 'source_file',
             'age_group_id', 'age_end', 'count',
             'bundle_id', 'facility'], 
             axis=1, inplace=True)
    df.rename(columns={'age_start': 'age', 'sex_id': 'sex',
                      'year_id': 'year'}, inplace=True)

    # add nid
    for y in df.year.unique():
        df.loc[df['year'] == y, 'NID'] = nid_dict[y]

    # match the cols in marketcan
    df['iso3'] = "TWN"
    df['list'] = "ICD9_detail"
    df['national'] = 1
    df['region'] = 100
    # match these to MS for Noise reduction
    df['source'] = "_Marketscan_prevalence"
    df.loc[df.bid_measure == "incidence", 'source'] = "_Marketscan_incidence"
    df.drop('bid_measure', axis=1, inplace=True)
    df['source_label'] = df['source']
    df['source_type'] = "Taiwan National Health Insurance Claims Data"
    df['subdiv'] = ""

    pre_cols = df.columns
    df = df[['acause', 'NID', 'location_id', 'iso3', 'list', 'national', 'region',
             'source', 'source_label', 'source_type', 'subdiv', 'year',
            'age', 'sex', 'sample_size', 'cf_raw', 'cf_corr', 'cf_rd', 'cf_final']]

    df.drop_duplicates(inplace=True) 
    assert not set(pre_cols).symmetric_difference(set(df.columns)), "columns don't align"
    assert df[df.columns.drop('subdiv')].isnull().sum().sum() == 0
    return df


def prep_final_formatting(df, nid_dict):

    stata_format = pd.read_stata(FILEPATH)
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
    df['location_name'] = "Taiwan"
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
    df['extractor'] = USERNAME
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
             'bid_measure', 'source_file', 'facility'], 
             axis=1, inplace=True)

    assert not set(df.columns).symmetric_difference(set(stata_format.columns))

    return df


def apply_bundle_restrictions(df, col_to_restrict, round_id, drop_restricted=True):
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
    if df[col_to_restrict].isnull().sum() > 0 and drop_restricted:
        warnings.warn("There are {} rows with null values which will be dropped".\
        format(df[col_to_restrict].isnull().sum()))
    if round_id > 5:
        if col_to_restrict == 'count':
            temp = df.copy()
            temp.drop(['age_start', 'age_end'], axis=1, inplace=True)

            df = clinical_mapping.apply_restrictions(temp, 'age_group_id', cause_type='bundle') 
            
            # append back the age_group id since it is needed elsewhere
            df = gbd_hosp_prep.all_group_id_start_end_switcher(df.copy(), remove_cols = False)
        else:
            df = clinical_mapping.apply_restrictions(df, 'binned', cause_type='bundle') 
    
    else:
        assert set(['bundle_id', 'age_start', 'age_end', 'sex_id', col_to_restrict]) <=\
        set(df.columns), "you're missing a column"

        start_cols = df.columns

        restrict = pd.read_csv(FILEPATH)
        restrict = restrict.reset_index(drop=True)  # excel file has a weird index
        restrict = restrict[restrict.bundle_id.notnull()]
        restrict = restrict.drop_duplicates()

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
    df['sex_id'] = pd.to_numeric(df['sex_id'])
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

    prev_ids = [548, 547, 546]
    df.loc[(df.bundle_id.isin(prev_ids)) & (df.bid_measure.isnull()),
         'bid_measure'] = 'prev'
    
    df.loc[(df.bundle_id == 79) & (df.bid_measure.isnull()),
        'bid_measure'] = 'inc'

    return df

def main(round_id):
    gbd_round = [7]
    msg = 'Update corresponding map version for the given GBD round'
    assert round_id in gbd_round, msg
    map_version = [28]
    map_round_dict = dict(list(zip(gbd_round, map_version)))

    for e in [True, False]:
        sequence(e, map_round_dict, round_id)

def sequence(inp_only, map_round_dict, round_id):
    nid_dict = {
        2016: 336203
    }
    prev, inc, bundles, maps = get_data(inp_only, map_round_dict, gbd_round=round_id)
    df = get_bid(prev, inc, bundles)
    back = df.copy()
    df = prep_cols(df)
    df = extract_age(df, round_id)
    print(df.shape)
    df = apply_bundle_restrictions(df, 'count', round_id)
    df = square_it(df)
    df = fill_missing_square_data(df)
    df = apply_bundle_restrictions(df, 'count', round_id)
    print(df.shape)

    # Sample size is already included in the raw data set
    df = get_sample(df)
    
    df2 = prep_final_formatting(df.copy(), nid_dict)
    print(df2.shape)
    df2['sex_id'] = 1
    df2.loc[df2.sex == "Female", 'sex_id'] = 2

    assert (df2.sex_id.unique() == [1, 2]).all()
    assert df2.bundle_id.unique().size == df.bundle_id.unique().size
    df2 = apply_bundle_restrictions(df2, 'cases', round_id)
    df2.drop('sex_id', axis=1, inplace=True)
    print(df2.bundle_id.unique().size, df.bundle_id.unique().size)
    df2.shape
    # make age_end inclusive
    df2['age_end'] = df2['age_end'] - 1

    df = get_fake_acause(df)
    df = create_cf_cols(df)

    df['bundle_id'] = pd.to_numeric(df.bundle_id)
    df = df[df.bundle_id.isin(maps.bundle_id.unique())]

    df = final_col_prep(df, nid_dict)

    if inp_only:
        file_source = 'inp_only'
    else:
        file_source = 'inp_oup'

    df2.drop('egeoloc', axis = 1, inplace = True)
    df['seq'] = np.nan


    dfs = [df, df2]
    for d in dfs:
        for colname in list(d.select_dtypes(include=['object']).columns):
            try:
                d[colname] = d[colname].astype(str)
            except (ValueError, TypeError) as e:
                pass

    df2['age_start'] = pd.to_numeric(df2['age_start'])
    df2['age_end'] = pd.to_numeric(df2['age_end'])

    print("Begin writing intermediate data...")
    path = FILEPATH.format(file_source)
    today = datetime.datetime.now().strftime("%Y-%m-%d")

    # write this to intermediate
    df.to_stata(path + FILEPATH, 
               write_index=False)

    for each in df.acause.unique():
        tmp = df[df.acause == each].copy()
        bundle_id = tmp.acause.str.split("#").iloc[0][1]
        tmp.to_stata(path + FILEPATH, write_index=False)
    print("Finished writing intermediate data, beginning on formatted...")
    df2.to_stata(path + FILEPATH.format(today), write_index=False)

    strs = ['bundle_name', 'sex', 'source_type', 'location_name', 'measure', 'representative_name', 'unit_type']
    for s in strs:
        df2[s] = df2[s].astype(str)
    for each in df2.bundle_id.unique():
        tmp = df2[df2.bundle_id == each].copy()
        tmp.to_stata(path + "formatted/{}.dta".format(each), write_index=False)
    print("All done!")
