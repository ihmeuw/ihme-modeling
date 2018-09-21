"""
read data aggregated by bundle
map acause on
loop through bundle ids and write to modelers folders

File path structure is FILEPATH
"""
import pandas as pd
import numpy as np
import datetime
import os
import time
import platform
import re
from db_tools.ezfuncs import query
from db_queries import get_cause_metadata
import warnings


if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

def write_bundles(df, write_location="test", write_fixed_maternal=False, extra_filename=""):

    assert write_location == 'test' or write_location == 'work', ("parameter "
        "write_location needs to be either 'test' or 'write', "
        "you put {}").format(write_location)

    if write_location == 'work':
        warnings.warn("""
                      write_location is set to work.
                      """)
        time.sleep(5)

    # drop bundle 'total_maternal', we don't want to write it
    df = df[df.bundle_id != 1010]

    # get injuries bundle_ids so we can keep injury corrected data later
    pc_injuries = pd.read_csv("FILEPATH")
    inj_bid_list = pc_injuries['Level1-Bundle ID'].unique()

    # CAUSE INFORMATION
    # get cause_id so we can write to an acause
    # have to go through cause_id to get to a relationship between BID &
    # acause
    cause_id_info = query("QUERY")
    # get acause
    acause_info = query("QUERY")
    # merge acause, bid, cause_id info together
    acause_info = acause_info.merge(cause_id_info, how="left", on="cause_id")

    # REI INFORMATION
    # get rei_id so we can write to a rei
    rei_id_info = query("QUERY")
    # get rei
    rei_info = query("QUERY")
    # merge rei, bid, rei_id together into one dataframe
    rei_info = rei_info.merge(rei_id_info, how="left", on="rei_id")

    # COMBINE REI AND ACAUSE
    # rename acause to match
    acause_info.rename(columns={'cause_id': 'cause_rei_id',
                                'acause': 'acause_rei'}, inplace=True)
    # rename rei to match
    rei_info.rename(columns={'rei_id': 'cause_rei_id',
                             'rei': 'acause_rei'}, inplace=True)

    # concat rei and acause together
    folder_info = pd.concat([acause_info, rei_info])

    # drop rows that don't have bundle_ids
    folder_info = folder_info.dropna(subset=['bundle_id'])

    # drop cause_rei_id, because we don't need it for getting data into
    # folders
    folder_info.drop("cause_rei_id", axis=1, inplace=True)

    # drop duplicates, just in case there are any
    folder_info.drop_duplicates(inplace=True)

    # MERGE ACAUSE/REI COMBO COLUMN ONTO DATA BY BUNDLE ID
    # there are NO null acause_rei entries!
    df = df.merge(folder_info, how="left", on="bundle_id")

    if write_fixed_maternal:
        # this is basically just a double check that we're only writing
        # data for maternal causes
        # GET MATERNAL CAUSES
        causes = get_cause_metadata(cause_set_id=9)
        condition = causes.path_to_top_parent.str.contains("366")

        # subset just causes that meet the condition sdf
        maternal_causes = causes[condition]

        # make list of maternal causes
        maternal_list = list(maternal_causes['acause'].unique())

        # keep only maternal causes
        df = df[df['acause_rei'].isin(maternal_list)]
        # drop the denominator bundle
        df = df[df['bundle_id'] != 1010]


    start = time.time()
    bundle_ids = df['bundle_id'].unique()

    # prevalence, indicence should be lower case
    df['measure'] = df['measure'].str.lower()

    readme = pd.read_excel("FILEPATH")

    columns_before = df.columns

    ordered = ['seq', 'input_type',
               'underlying_nid', 'nid', 'source_type',
               'bundle_id', 'bundle_name',
               'location_id', 'location_name',
               'sex',
               'year_start', 'year_end',
               'age_start', 'age_end',
               'measure',
               'mean_0', 'lower_0', 'upper_0',
               'mean_1', 'lower_1', 'upper_1', 'correction_factor_1',
               'mean_2', 'lower_2', 'upper_2', 'correction_factor_2',
               'mean_3', 'lower_3', 'upper_3', 'correction_factor_3',
               'mean_inj', 'lower_inj', 'upper_inj', 'correction_factor_inj',
               'standard_error',
               'cases',
               'effective_sample_size',
               'sample_size',
               'unit_type', 'unit_value_as_published',
               'uncertainty_type',
               'uncertainty_type_value',
               'representative_name',
               'urbanicity_type',
               'recall_type',
               'recall_type_value',
               'sampling_type',
               'response_rate',
               'design_effect',
               'extractor','is_outlier', 'acause_rei']
    df = df[ordered]
    columns_after = df.columns
    assert set(columns_after) == set(columns_before),\
        "the columns {} were added/lost while changing column order"\
        .format(set(columns_after).symmetric_difference(set(columns_before)))

    # adjust min age_end to 0.999 instead of 1
    df.loc[df['age_start'] == 0, 'age_end'] = 0.999

    print("BEGINNING WRITING, THE START TIME IS {}".format(time.strftime('%X %x %Z')))
    failed_bundles = []  # initialize empty list to append to in this for loop
    counter = 0  # initialize counter to report how close we are to done
    length = len(bundle_ids)
    for bundle in bundle_ids:
        counter += 1
        completeness = float(counter) / length * 100
        print r"{}% done".format(completeness)
        # subset bundle data
        df_b = df[df['bundle_id'] == bundle].copy()

        # drop columns based on measure - inc/prev/injury
        # if the measure is prev: keep all 3 correction factors
        # if measure is inc and not an inj: keep 2 correction factors
        # if measure is inc and an inj: keep only injury correction factor
        df_b_measure = df_b.measure.unique()[0]
        if df_b.bundle_id.isin(inj_bid_list).all():
            df_b.drop(['mean_1', 'upper_1', 'lower_1', 'correction_factor_1',
                       'mean_2', 'upper_2', 'lower_2', 'correction_factor_2',
                       'mean_3', 'upper_3', 'lower_3', 'correction_factor_3'],
                       axis=1, inplace=True)
        if ((df_b_measure == 'incidence') and not
            (df_b.bundle_id.isin(inj_bid_list).all())):


            df_b.drop(['mean_inj', 'upper_inj', 'lower_inj',
                       'correction_factor_inj'],
                       axis=1, inplace=True)
        if df_b_measure == 'prevalence':
            df_b.drop(['mean_inj', 'upper_inj', 'lower_inj',
                       'correction_factor_inj'],
                      axis=1, inplace=True)


        acause_rei = str(df_b.acause_rei.unique()[0])
        df_b.drop('acause_rei', axis=1, inplace=True)


        if write_location == 'test':
            writedir = ("FILEPATH")
        elif write_location == 'work':
            writedir = ("FILEPATH")


        if not os.path.isdir(writedir):
            os.makedirs(writedir)  # make the directory if it does not exist

        # write for modelers
        # make path
        vers_id = "v8" # last one was v6, should have been v7
        date = datetime.datetime.today().strftime("%Y_%m_%d")  # YYYY-MM-DD
        if write_fixed_maternal:
            extra_filename = "_adjusted_denominator"
        bundle_path = "{}{}_{}_{}{}.xlsx".\
            format(writedir, int(bundle), vers_id, date, extra_filename)
        print r"Now writing at {}".format(bundle_path)


        # try to write to modelers' folders
        try:
            writer = pd.ExcelWriter(bundle_path, engine='xlsxwriter')
            df_b.to_excel(writer, sheet_name="extraction", index=False)
            readme.to_excel(writer, sheet_name='README', index=False)
            writer.save()
        except:
            failed_bundles.append(bundle)  # if it fails for any reason
            # make note of it
    end = time.time()

    text = open("FILEPATH")
    text.write("function: write_bundles " + "\n"+
               "start time: " + str(start) + "\n" +
               " end time: " + str(end) + "\n" +
               " run time: " + str((end-start)/60.0) + " minutes")
    text.close()

    print("DONE WRITING, THE CURRENT TIME IS {}".format(time.strftime('%X %x %Z')))
    return(failed_bundles)
