# -*- coding: utf-8 -*-
"""
Map spreadsheet to compiled hospital data by ICD code
Can write a log that holds the following information: All locations in data,
all sources in data, ICD codes that did not match anything in the maps,
the number & percent of data that did not match something in the maps.

Parameters:

df: DataFrame
    dataframe with all the hospital data in it at the ICD level.
maps_path: string
    filepath as raw string that points to the latest clean map
write_log: Boolean
    switch that determines if you want to write a log

"""
import pandas as pd
import platform
import datetime
import re
import warnings

if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"


def icd_mapping(df, maps_path, level_of_analysis='nonfatal_cause_name', write_log=False):
    """
    This won't work if we start getting multiple sources for one location.  This
    code drops duplicated rows of the data after merging on the map.  If there
    happend to be a repeated ICD code, age, sex, year, location combo with the
    same number of cases, we would lose that data.  This is because of multi -
    level bundle_ids
    """

    def use_special_map(data, map_path, sheetname, asource):
        """
        input data that needs to be mapped with a special map.
        Things like aggregated ICD codes, or non ICD codes.  This function
        requires that the map be in an excel spreadsheet

        This function could be improved.  It's less than ideal that the maps
        have to be formatted within this function

        Call this function once per source.

        Parameters:
            data: Pandas DataFrame
                data that needs special mapping
            map_path: string
                path to special map
            sheetname: string
                sheet of map to use.
            asource: name of the source that needs special mapping.
        """
        # read in the special map
        spec_map = pd.read_excel(map_path, sheetname=sheetname)

        # this is really specific to a single special map....
        if "indonesia" in map_path:
        # if asource == "IDN_SIRS":
            print("fixing cause code col name in Indonesia")
            spec_map.rename(columns={'subnational_icd_code': 'cause_code'},
                            inplace=True)
            spec_map['code_system_id'] = 2

        if asource == "VNM_MOH":
            # NOTE this is arbitrary!
            spec_map['code_system_id'] = 2
            spec_map['cause_code'] = spec_map['cause_code'].astype(str)

        # In many cases, but not all, having "code_system_id" doesn't
        # make sense, but consistency is key.
        assert 'nonfatal_cause_name' in spec_map.columns, ("spec_map is"
            " missing the column code_system_id")

        # keep cols of interest
        spec_map = spec_map[['cause_code', 'code_system_id',
                             'nonfatal_cause_name']].copy()


        # remove non alphanumeric chars from icd codes
        spec_map['cause_code'] =\
            spec_map['cause_code'].str.replace("\W", "")

        # convert to string
        # This will break if there are any unicode chars that can't be ascii
        spec_map['cause_code'] = spec_map['cause_code'].astype(str)

        # make baby sequelae lower case
        spec_map['nonfatal_cause_name'] =\
            spec_map['nonfatal_cause_name'].str.lower()

        # check map icd codes against data icd codes
        set(data.cause_code).symmetric_difference(set(spec_map.cause_code))

        # merge the special map onto the subset of data
        pre = data.shape[0]
        merged_subset = data.merge(spec_map, how='left',
                                   on=["cause_code", "code_system_id"])
        assert merged_subset.shape[0] == pre, ("Shape changed during "
            "merge")
        assert merged_subset.nonfatal_cause_name.isnull().sum() == 0, (
            "the specially mapped data has nulls in the column "
            "'nonfatal_cause_name' after merge")

        return(merged_subset)


    assert (level_of_analysis == 'nonfatal_cause_name') |\
            (level_of_analysis == 'bundle_id'), ("Please set level_of_analysis"
                " to either 'nonfatal_cause_name' or 'bundle_id'")
    print("there are {} cases to start".format(round(df.val.sum(), 0)))
    # read in clean map
    maps = pd.read_csv(maps_path, dtype={'cause_code': object})

    # cast to string
    maps['cause_code'] = maps['cause_code'].astype(str)


    # keep relevant columns
    if level_of_analysis == 'nonfatal_cause_name':
        maps = maps[['cause_code', 'nonfatal_cause_name',
                     'code_system_id']].copy()
    # want both columns if we're mapping to bundle
    if level_of_analysis == 'bundle_id':
        maps = maps[['cause_code', 'nonfatal_cause_name', 'bundle_id',
                     'code_system_id']].copy()

    # drop duplicate values
    maps = maps.drop_duplicates()

    # make sure that cause_code is a string. this is a redundancy/contingency
    df['cause_code'] = df['cause_code'].astype(str)

    # match on upper case
    df['cause_code'] = df['cause_code'].str.upper()
    maps['cause_code'] = maps['cause_code'].str.upper()

    if level_of_analysis == 'nonfatal_cause_name':
        # store variables for data check later
        before_values = df['cause_code'].value_counts()  # value counts before

    # SPECIAL MAPPING
    df_sources = df.source.unique()  # save so that we don't need to recompute
    # use special map for Indonesia
    # create subset of idn data and using special map
    if "IDN_SIRS" in df_sources:
        ind_mapped = use_special_map(df[df.source == 'IDN_SIRS'].copy(),
                                     map_path=r"FILEPATH",
                                     sheetname='indonesia_data_icd_codes',
                                     asource='IDN_SIRS')
        # drop Indonesia from master data
        df = df[df['source'] != 'IDN_SIRS']


    # use special map for Vietnam
    if "VNM_MOH" in df_sources:
        vnm_mapped = use_special_map(df[df.source == 'VNM_MOH'].copy(),
                                     map_path=r"FILEPATH",
                                     sheetname='TQ',
                                     asource='VNM_MOH')
        # drop Vietnam from master data
        df = df[df['source'] != 'VNM_MOH']

    # merge the hospital data with excel spreadsheet maps
    df = df.merge(maps, how='left',
                  on=["cause_code", "code_system_id"])

    # bring special mapped data and normal data back together
    if "IDN_SIRS" in df_sources:
        df = pd.concat([df, ind_mapped]).reset_index(drop=True)
    if "VNM_MOH" in df_sources:
        df = pd.concat([df, vnm_mapped]).reset_index(drop=True)

    # won't ever work for bundle_id because many bundle_ids are null
    # this check needs to run after the merge but before we alter the icd
    # codes using the methods below.
    if level_of_analysis == 'nonfatal_cause_name':
        after_values = df['cause_code'].value_counts()  # value counts after
        # assert various things to verify merge
        assert set(before_values.index) == set(after_values.index),\
            "Unique ICD codes are not the same"
            #


    def extract_primary_dx(df):
        """
        creates a new column with just the primary dx when multiple dx have been
        combined together (this happens often in the phil health data
        """
        # split the icd codes on letter
        df['split_code'] = df.cause_code.map(lambda x: re.split("([A-Z])", x))
        # keep primary icd code
        df['split_code'] = df['split_code'].str[0:3]
        df['split_code'] = df['split_code'].map(lambda x: "".join(map(str, x)))
        return(df)


    def map_to_truncated(df):
        """
        takes a dataframe of icd codes which didn't map to baby sequela
        merges clean maps onto every level of truncated icd codes, from 6 digits to 3 digits
        when needed.
        returns only the data that has successfully mapped to a baby sequela
        """
        df_list = []
        for i in [7, 6, 5, 4]:
            df['cause_code'] = df['split_code'].str[0:i]
            good = df.merge(maps, how='left', on=['cause_code', 'code_system_id'])
            # keep only rows that successfully merged
            good = good[good['nonfatal_cause_name'].notnull()]
            # drop the icd codes that matched from df
            df = df[~df.cause_code.isin(good.cause_code)]
            df_list.append(good)

        dat = pd.concat(df_list)
        return(dat)

    # create a sum of cases to make sure we're not losing data
    pre_cases = df.val.sum()
    # we want only rows where nfc is null
    no_map = df[df['nonfatal_cause_name'].isnull()].copy()
    # keep only rows in df where nfc is not null
    df = df[df['nonfatal_cause_name'].notnull()].copy()
    # create a raw cause code column to remove icd codes that were
    # fixed and mapped successfully
    no_map['raw_cause_code'] = no_map['cause_code']

    # split icd 9 and 10 codes that didn't map
    no_map_10 = no_map[no_map.code_system_id == 2].copy()
    no_map_9 = no_map[no_map.code_system_id == 1].copy()

    # create remap_df to retry mapping for icd 10
    remap_10 = no_map_10.copy()
    remap_10.drop('nonfatal_cause_name', axis=1, inplace=True)
    # take the first icd code when they're together
    remap_10 = extract_primary_dx(remap_10)
    # now do the actual remapping, losing all rows that don't map
    remap_10 = map_to_truncated(remap_10)

    # create remap_df to retry mapping for icd 9
    remap_9 = no_map_9.copy()
    remap_9.drop('nonfatal_cause_name', axis=1, inplace=True)
    # take the first icd code when they're together
    remap_9 = extract_primary_dx(remap_9)
    # now do the actual remapping, losing all rows that don't map
    remap_9 = map_to_truncated(remap_9)

    # remove the rows where we were able to re-map to baby sequela
    no_map_10 = no_map_10[~no_map_10.raw_cause_code.isin(remap_10.raw_cause_code)]
    no_map_9 = no_map_9[~no_map_9.raw_cause_code.isin(remap_9.raw_cause_code)]

    # bring our split data frames back together
    df = pd.concat([df, remap_10, remap_9, no_map_10, no_map_9])

    df.drop(['raw_cause_code', 'split_code'], axis=1, inplace=True)

    # warn that we haven't lost more than 50 cases out of billions
    if abs(pre_cases - df.val.sum()) > 50:
        warnings.warn("""

                      More than 50 cases were lost.
                      To be exact, the differnce (before - after) is {}
                      """.format(pre_cases - df.val.sum()))

    # write the data that didn't match for further inspection
    df[df['nonfatal_cause_name'].isnull()].\
        to_csv(r"FILEPATH", index=False)

    # create series of rows without baby sequela
    no_match_count = df[level_of_analysis].isnull().sum()
    no_match_count = float(no_match_count)  # for when this runs on python 2
    no_match_per = round(no_match_count/df.shape[0] * 100, 4)
    print(r"{} rows did not match a {} in the map out of {}.").\
        format(no_match_count, level_of_analysis, df.shape[0])
    print("This is {}% of total rows that did not match".format(no_match_per))

    if level_of_analysis == 'nonfatal_cause_name':
        assert no_match_per < 5, r"5% or more of rows didn't match"

    if write_log:
        print("Writing Log file")

        text = open(root + "FILEPATH" +
                    re.sub("\W", "_", str(datetime.datetime.now())) +
                    "_icd_mapping_output.txt", "w")
        text.write("""
                   Number of unmatched rows: {}
                   Total rows of data: {}
                   Percent of unmatched:  {}
                   """.format(no_match_count, df.shape[0], no_match_per))
        text.close()

    # map missing baby sequela to _none
    if level_of_analysis == 'nonfatal_cause_name':
        df.loc[df['nonfatal_cause_name'].isnull(), 'nonfatal_cause_name'] = "_none"
    if level_of_analysis == 'bundle_id':
        df.drop("nonfatal_cause_name", axis=1, inplace=True)
        df = df.drop_duplicates()

    print("performing groupby and sum")
    df.drop('outcome_id', axis=1, inplace=True)
    groups = ['location_id', 'year_start', 'year_end', 'age_group_unit',
              'age_start', 'age_end', 'sex_id', 'source', 'nid',
              'facility_id', 'representative_id', 'diagnosis_id',
              'metric_id', level_of_analysis]#, 'use_in_maternal_denom']
    df = df.groupby(groups).agg({'val': 'sum'}).reset_index()

    # re form outcome_id column
    df['outcome_id'] = 'case'

    int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
                'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
                'diagnosis_id', 'metric_id']#, 'use_in_maternal_denom']
    float_cols = ['val']
    str_cols = ['source', 'facility_id', 'outcome_id']

    if level_of_analysis == 'nonfatal_cause_name':
        df[level_of_analysis] = df[level_of_analysis].astype(str)
    if level_of_analysis == 'bundle_id':
        df[level_of_analysis] = pd.to_numeric(df[level_of_analysis],
                                              errors='raise',
                                              downcast='integer')

    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors='raise', downcast='float')
    for col in str_cols:
        df[col] = df[col].astype(str)

    print("there are {} cases to end".format(round(df.val.sum(), 0)))

    return(df)
