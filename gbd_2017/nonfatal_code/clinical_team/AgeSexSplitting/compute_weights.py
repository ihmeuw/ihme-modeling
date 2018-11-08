import getpass
import sys
import warnings
import numpy as np
import pandas as pd
import re
import datetime
from db_queries import get_population, get_cause_metadata, get_rei_metadata
from db_tools.ezfuncs import query

# load our functions
user = getpass.getuser()
prep_path = r"FILEPATH/Functions"
sys.path.append(prep_path)
repo = r"FILEPATH/hospital"
import hosp_prep
import gbd_hosp_prep


def prep_weights(df, level):
    """
    Function that computes weights for use in age sex splitting.

    Parameters:
        df: pandas DataFrame
            input data to inform weights. Should already have the appropriate
            age groups.  Should have column "product" which is the product
            of cause_fraction and the hospital utilization envelope.
            age_group_id should be present.  df is used only as an input to
            make weights.
        level: string
            Must be "bundle_id" or "nonfatal_cause_name". indicates if we're making
            bundle level weights or cause level weights.
    Returns:
        DataFrame that has weights computed at level of the Parameter 'level'
    """
    print("Getting {} weights...".format(level))

    # remove the all sexes sex id
    df = df[df.sex_id != 3].copy()

    # code is set up to use both age_start/age_end and age_group_id
    df = gbd_hosp_prep.all_group_id_start_end_switcher(df, remove_cols=False)

    for source in df.source.unique():
        age_min = df[df.source==source].age_start.min()
        age_max = df[df.source==source].age_start.max()
        assert age_max == 95,\
            "source {} doesn't have max age_start == 95".format(source)

    # keep relevant columns
    df = df[['age_group_id', 'age_start', 'age_end', 'location_id',
             'sex_id', 'year_end', 'year_start', 'product',
             'nonfatal_cause_name']].copy()

    # make square aka cartesian. We want population to be conributed by all
    # age-countries, regardless if there are any cases for all age-country pairs
    # For every location-year that we already have, we want all age-sex
    # (and bundle/cause) combinations.  This introduces Nulls where there wasn't
    # any data, which are then filled with zeros
    template = hosp_prep.make_square(df)
    df = template.merge(df, how='left',
                        on=['age_group_id', 'sex_id', 'location_id',
                            'year_start',
                            'nonfatal_cause_name',
                            'year_end', 'age_start', 'age_end'])
    # fill zeros
    df.update(df['product'].fillna(0))

    # merge pop on so we can convert to count space, so we can do addition
    # get info for pop
    age_list = list(df.age_group_id.unique())
    loc_list = list(df.location_id.unique())
    year_list = list(df.year_start.unique())

    # get pop
    pop = get_population(QUERY)

    # format pop
    pop.drop("run_id", axis=1, inplace=True)  # and lock it ??
    pop['year_start'] = pop['year_id']
    pop['year_end'] = pop['year_id']
    pop.drop('year_id', axis=1, inplace=True)

    # merge pop
    df = df.merge(pop, how='left',
                  on=['location_id', 'year_start', 'year_end',
                      'age_group_id', 'sex_id'])

    # multiply by pop to get into count space so we can get to
    # age / sex / bundle groups
    df['counts'] = df['product'] * df['population']

    if level == 'bundle_id':
        assert False, "Level can no longer be bundle id"
        # in this section you merge on bundle_id via nonfatal_cause_name, and
        # then drop nonfatal_cause_name
        maps = pd.read_csv(r"FILEPATH/clean_map.csv")
        assert hosp_prep.verify_current_map(maps)
        maps = maps[['nonfatal_cause_name', 'bundle_id', 'level']].copy()
        maps = maps[maps.level == 1].copy()
        maps = maps.drop('level', axis=1)
        maps = maps.dropna(subset=['bundle_id'])
        maps = maps.drop_duplicates()

        df = df.merge(maps, how='left', on='nonfatal_cause_name')

        df.drop("nonfatal_cause_name", axis=1, inplace=True)

    group_cols = ['age_end', 'age_start', 'age_group_id', 'sex_id', level]
    df = df.groupby(by=group_cols).agg({'counts': 'sum',
                                        'population': 'sum'}).reset_index()

    # divide by pop to get back into ratespace ... and we have the start of our
    # weights
    df['weight'] = df['counts'] / df['population']

    return(df)


def compute_weights(df, round_id, fill_gaps=True, overwrite_weights=False):
    """
    Args:
        df: pandas DataFrame that contains the data that you want to use to
            inform weights. Should already have the appropriate
            age groups.  Should have column "product" which is the product
            of cause_fraction and the hospital utilization envelope.
            age_group_id should be present. df is used only as an input to
            the other function, in order to make weights.
        round_id: int
            indicates what round of weights we're on, which affects file names
            for output. Should either be 1 or 2
        fill_gaps: Boolean
            switch. If true, age sex restrictions will be used to find gaps in
            nonfatal_cause_name age sex pattern.  if a gap is found, that entire
            age sex pattern will be replaced with the baby sequelae's level 1
            bundle's age sex pattern.  This is all if a baby sequelae has a
            bundle id
    Returns:
        string "Done"
    """

    assert round_id == 1 or round_id == 2,\
        "round_id isn't 1 or 2, it's {}".format(round_id)

    # get weights
    nonfatal_df = prep_weights(df, 'nonfatal_cause_name')
    # has column nonfatal_cause_name
    print("done getting both kinds of weights")

    restrict = pd.read_csv(r"FILEPATH/all_restrictions.csv")
    restrict = restrict[['Baby Sequelae  (nonfatal_cause_name)', 'male',
                         'female', 'yld_age_start', 'yld_age_end']].copy()
    restrict = restrict.reset_index(drop=True)  # excel file has a weird index
    restrict.rename(columns={'Baby Sequelae  (nonfatal_cause_name)':
                             'nonfatal_cause_name'}, inplace=True)
    restrict['nonfatal_cause_name'] =\
        restrict['nonfatal_cause_name'].str.lower()
    restrict = restrict[restrict.nonfatal_cause_name.notnull()]
    restrict = restrict.drop_duplicates()

    restrict.loc[restrict['yld_age_start'] < 1, 'yld_age_start'] = 0

    # merge age restrictions onto bundle weights by bundle_id
    nonfatal_df = nonfatal_df.merge(restrict, how='left',
                                    on='nonfatal_cause_name')


    if fill_gaps:
        print("Applying restrictions to find gaps in NFC weights")

        nonfatal_df.loc[(nonfatal_df['age_start'] > nonfatal_df['yld_age_start'])&(nonfatal_df['weight']==0)&(nonfatal_df['age_end'] < nonfatal_df['yld_age_end']), 'weight'] = np.nan


    merged = nonfatal_df.copy()

    assert nonfatal_df.shape[0] == merged.shape[0],\
        "I expected that they would have the same number of rows"
    merged.loc[merged['age_start'] > merged['yld_age_end'], 'weight'] = 0
    merged.loc[merged['age_end'] < merged['yld_age_start'], 'weight'] = 0
    merged.loc[(merged['male'] == 0) & (merged['sex_id'] == 1), 'weight'] = 0
    merged.loc[(merged['female'] == 0) & (merged['sex_id'] == 2), 'weight'] = 0

    # drop the columns we used to make restrictions
    merged.drop(['male', 'female', 'yld_age_start', 'yld_age_end'],
                 axis=1, inplace=True)

    if fill_gaps:
        print("filling gaps")
        df_list = []
        print("Replacing gaps in bundle weights with cause weights")
        counter = 0.0
        is_null = 0
        for bundle in merged.bundle_id.unique():
            counter += 1
            for sex in merged.sex_id.unique():
                df_s = merged[(merged.sex_id == sex)&(merged.bundle_id == bundle)].copy()
                if df_s['weight'].isnull().any():
                    
                    is_null += 1
                    df_s['weight'] = df_s['bundle_weight']
                df_list.append(df_s)
        print(r"{}% of the age patterns were replaced with cause weights".format(is_null / counter))
        merged = pd.concat(df_list).reset_index(drop=True)

    # check the weights
    # group by age and sex, combining different nonfatal_cause_names in one group
    groups = merged.groupby(['age_group_id', 'sex_id'])

    bad = []  # list to append to
    # loop over each group
    for group in groups:
        # group[1] contains a dataframe of a specific group
        if (group[1].weight == 0).all():
            # group[0] is a tuple contains the age, sex that ID the group
            bad.append(group[0])  # add it to the list

    warning_message =  """
    There are age-sex groups in the weights where every weight is zero

    Here's some tuples (age_group_id, sex_id) that identify these groups:
    {g}
    """.format(g=", ".join(str(g) for g in bad))

    if len(bad) > 0:
        warnings.warn(warning_message)

    if overwrite_weights:
        merged.drop(['age_start', 'age_end', 'counts', 'population'],
             axis=1).to_csv(r"FILEPATH/"
                            r"weights_nonfatal_cause_name.csv",
                            index=False)

    if round_id == 1:
        round_name = "one"
    if round_id == 2:
        round_name = "two"
    # save for splitting
    merged.drop(['age_start', 'age_end', 'counts', 'population'],
                 axis=1).to_csv(r"FILEPATH/"
                                r"round_{}_weights_nonfatal_cause_name.csv".format(round_name),
                                index=False)
    # write an archive file
    today = re.sub("\W", "_", str(datetime.datetime.now()))[0:10]
    merged.drop(['age_start', 'age_end', 'counts', 'population'],
                 axis=1).to_csv(r"FILEPATH/"
                                r"{}_round_{}_weights_nonfatal_cause_name.csv".format(today, round_name),
                                index=False)
    # result for tableau
    merged['sex_id'].replace([1,2], ["Male", "Female"], inplace=True)
    merged.to_csv(r"FILEPATH/"
                  r"round_{}_weights_for_tableau_nonfatal_cause_name.csv".format(round_name),
                  index=False)
    # write an archive file
    merged.to_csv(r"FILEPATH/"
                  r"{}_round_{}_weights_for_tableau_nonfatal_cause_name.csv".format(today, round_name),
                  index=False)

    return("done")
