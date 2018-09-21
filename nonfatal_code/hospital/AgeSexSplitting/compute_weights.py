import getpass
import sys
import warnings
import numpy as np
import pandas as pd
from db_queries import get_population, get_cause_metadata, get_rei_metadata
from db_tools.ezfuncs import query

# load our functions
if getpass.getuser() == 'USERNAME':
    sys.path.append("FILENAME")
import hosp_prep


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

    # input asserts
    # assert level == "bundle_id" or level == "nonfatal_cause_name",\
    #     "level must either be 'bundle_id' or 'nonfatal_cause_name'"

    # input data should already have appropriate age groups if this is round 2,
    # then you're using data that used to stop at age 80+ or something but you
    # fixed it to go to 95+

    print("Getting {} weights...".format(level))

    for source in df.source.unique():
        age_min = df[df.source==source].age_start.min()
        age_max = df[df.source==source].age_start.max()
        # assert age_min == 0,\
        #     "source {} doesn't have min age_start == 0".format(source)
        assert age_max == 95,\
            "source {} doesn't have max age_start == 95".format(source)

    # keep relevant columns
    # nonfatal_cause_name should be kept regarless if these are
    # nonfatal_cause_name or bundle weights.
    df = df[['age_group_id', 'age_start', 'age_end', 'location_id',
             'sex_id', 'year_end', 'year_start', 'product',
             'nonfatal_cause_name']].copy()

    # make square aka cartesian. We want population to be conributed by all
    # age-countries, regardless if there are any cases for all age-country pairs
    # For every location-year that we already have, we want all age-sex
    # (and bundle/cause) combinations.  This introduces Nulls where there wasn't
    # any data, which are then filled with zeros
    template = hosp_prep.make_square(df)  # took under a min
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
    pop = get_population(age_group_id=age_list, location_id=loc_list,
                         sex_id=[1, 2], year_id=year_list)

    # format pop
    pop.drop("process_version_map_id", axis=1, inplace=True)  # and lock it ??
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
        # in this section you merge on bundle_id via nonfatal_cause_name, and
        # then drop nonfatal_cause_name
        maps = pd.read_csv("FILENAME")
        maps = maps[['nonfatal_cause_name', 'bundle_id', 'level']].copy()
        maps = maps[maps.level == 1].copy()
        maps = maps.drop('level', axis=1)
        maps = maps.dropna(subset=['bundle_id'])
        maps = maps.drop_duplicates()

        df = df.merge(maps, how='left', on='nonfatal_cause_name')

        # this is old stuff from when you did bundle level weights.
        # cause_id_info = query("SQL QUERY")
        # cause_id_info = cause_id_info[(cause_id_info.cause_id.notnull())|(cause_id_info.rei_id.notnull())]  # drops where both are null
        #
        # # put rei and cause into the same column
        # cause_id_info.loc[cause_id_info.cause_id.isnull(), 'cause_id'] =\
        #     cause_id_info.loc[cause_id_info.cause_id.isnull(), 'rei_id']
        #
        # df = df.merge(cause_id_info, how='left', on='bundle_id')

        df.drop("nonfatal_cause_name", axis=1, inplace=True)

    # ok so now we are in count space. don't really need product anymore.
    # time to aggregate want groups of age, sex, level (disease cause),
    # while summing pop and counts
    group_cols = ['age_end', 'age_start', 'age_group_id', 'sex_id', level]
    df = df.groupby(by=group_cols).agg({'counts': 'sum',
                                        'population': 'sum'}).reset_index()

    # divide by pop to get back into ratespace ... and we have the start of our
    # weights
    df['weight'] = df['counts'] / df['population']

    # get bundle name for Tableau
    # NOTE not relevant for
    # if level == 'bundle_id':
    #     df = df.merge(query("SQL QUERY"),
    #                   how='left', on='bundle_id')

    return(df)


def compute_weights(df, round_id, fill_gaps=True):
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

    # bundle_df = prep_weights(df, 'bundle_id')  # has column bundle_id
    print("done getting both kinds of weights")

    # here you merge on bundle_id AND cause_id via nonfatal_cause_name
    # maps = pd.read_csv(r"FILENAME")
    # maps = maps[['nonfatal_cause_name', 'bundle_id', 'level']].copy()
    # maps = maps[maps.level == 1].copy()
    # maps = maps.drop('level', axis=1)
    # maps = maps.dropna(subset=['bundle_id'])
    # maps = maps.drop_duplicates()

    # # get bundle_id onto nonfatal_df
    # pre = nonfatal_df.shape[0]
    # # maps has ['nonfatal_cause_name', 'bundle_id']
    # nonfatal_df = nonfatal_df.merge(maps, how='left', on='nonfatal_cause_name')
    # # now nonfatal_df has columns nonfatal_cause_name, bundle_id
    # assert nonfatal_df.shape[0] == pre, "number of rows changed during merge"

    # NOTE bundle_df already has bundle_id

    # get age restictions so we can identify where the gaps are
    restrict = pd.read_excel("FILENAME", sheetname='NE to ME1')
    restrict = restrict[['Baby Sequelae  (nonfatal_cause_name)', 'male',
                         'female', 'yld_age_start', 'yld_age_end']].copy()
    restrict = restrict.reset_index(drop=True)  # excel file has a weird index
    restrict.rename(columns={'Baby Sequelae  (nonfatal_cause_name)':
                             'nonfatal_cause_name'}, inplace=True)
    restrict['nonfatal_cause_name'] =\
        restrict['nonfatal_cause_name'].str.lower()
    restrict = restrict[restrict.nonfatal_cause_name.notnull()]
    restrict = restrict.drop_duplicates()
    # replace values below 1 with zero in get_cause, we don't differentiate
    # under one years old.
    restrict.loc[restrict['yld_age_start'] < 1, 'yld_age_start'] = 0

    # merge age restrictions onto bundle weights by bundle_id
    nonfatal_df = nonfatal_df.merge(restrict, how='left',
                                    on='nonfatal_cause_name')

    # AGE SEX RESTRICTIONS FOR GAP FILLING: noting zeros inside ranges
    if fill_gaps:
        # change from 0 to nan where we don't want data to be missing
        # NOTE want gaps to be noted just in nonfatal_df
        print("Applying restrictions to find gaps in NFC weights")
        # nonfatal_df.loc[(nonfatal_df['age_start'] > nonfatal_df['yld_age_start'])&(nonfatal_df['weight']==0), 'weight'] = np.nan
        # nonfatal_df.loc[(nonfatal_df['age_end'] < nonfatal_df['yld_age_end'])&(nonfatal_df['weight']==0), 'weight'] = np.nan
        nonfatal_df.loc[(nonfatal_df['age_start'] > nonfatal_df['yld_age_start'])&(nonfatal_df['weight']==0)&(nonfatal_df['age_end'] < nonfatal_df['yld_age_end']), 'weight'] = np.nan


    # specify which weight comes from where
    # bundle_df.rename(columns={'weight': 'bundle_weight'},inplace=True)

    # merge cause weights onto bundle weights
    # print("merging NFC and bundle weights together")
    # merged = nonfatal_df.merge(bundle_df.drop(['counts', 'population'], axis=1),
    #                          how='left',
    #                          on=['age_start', 'age_end', 'age_group_id',
    #                              'sex_id', 'bundle_id'])
    merged = nonfatal_df.copy()

    assert nonfatal_df.shape[0] == merged.shape[0],\
        "I expected that they would have the same number of rows"

    # AGE SEX RESTRICTIONS: forbidding non-zero values outside of ranges
    # NOTE want to do this to both the nonfatal_df and the bundle_df
    # test.loc[test['age_start'] > test['yld_age_end'], ['weight', 'bundle_weight']] = [0, 0]
    merged.loc[merged['age_start'] > merged['yld_age_end'], 'weight'] = 0
    merged.loc[merged['age_end'] < merged['yld_age_start'], 'weight'] = 0
    merged.loc[(merged['male'] == 0) & (merged['sex_id'] == 1), 'weight'] = 0
    merged.loc[(merged['female'] == 0) & (merged['sex_id'] == 2), 'weight'] = 0

    # drop the columns we used to make restrictions
    merged.drop(['male', 'female', 'yld_age_start', 'yld_age_end'],
                 axis=1, inplace=True)

    # replace bundle age/sex pattern with cause age/sex pattern where there are
    # nulls in the bundle weights. NOTE we're replacing the entire pattern, not
    # just filling one gap.
    if fill_gaps:
        print("filling gaps")
        df_list = []
        print("Replacing gaps in bundle weights with cause weights")
        counter = 0.0  # gotta be a float in python 2
        is_null = 0
        for bundle in merged.bundle_id.unique():
            counter += 1
            for sex in merged.sex_id.unique():
                df_s = merged[(merged.sex_id == sex)&(merged.bundle_id == bundle)].copy()
                if df_s['weight'].isnull().any():
                    # print sex, bundle
                    is_null += 1
                    df_s['weight'] = df_s['bundle_weight']
                df_list.append(df_s)
        print(r"{}% of the age patterns were replaced with cause weights".format(is_null / counter))
        merged = pd.concat(df_list).reset_index(drop=True)

    # bundle name for tableau
    # merged = merged.merge(query("SQL QUERY"), how='left', on='bundle_id')
    # save
    if round_id == 1:
        round_name = "one"
    if round_id == 2:
        round_name = "two"
    # save for splitting
    merged.drop(['age_start', 'age_end', 'counts', 'population'],
                 axis=1).to_csv("FILENAME",
                                index=False)
    # result for tableau
    merged['sex_id'].replace([1,2], ["Male", "Female"], inplace=True)
    merged.to_csv("FILENAME",
                  index=False)

    return("done")
