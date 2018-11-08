'''
PULL VITAL REGISTRATION DATA
'''
# Imports (general)
import argparse
import getpass
import numpy as np
import os
import pandas as pd
import sys
from os.path import join
from db_queries import (get_demographics,
                        get_cause_metadata as cm)
from db_tools.ezfuncs import query
from cod_prep.claude.claude_io import get_claude_data
from cod_prep.downloaders.causes import get_all_related_causes
from cod_prep.downloaders.nids import get_datasets


def get_related_causes_dict(cause_list, gbd_round_id, cause_set_id):

    cause_hierarchy = cm(gbd_round_id=gbd_round_id, cause_set_id=cause_set_id)

    related_from = list()
    related_to = list()
    for shocks_cause in cause_list:
        related = get_all_related_causes(cause_id=shocks_cause,
                                         cause_meta_df=cause_hierarchy)
        related_from = related_from + related
        related_to = related_to + [shocks_cause]*len(related)

    related_dict = dict(zip(related_from, related_to))
    return related_dict


def subset_causes(df, cause_ids):

    assert type(cause_ids) is list
    return df.loc[df['cause_id'].isin(cause_ids),:]


def pull_vr(cause_ids, start_year, end_year):

    assert type(start_year) is int, "Starting year must be an integer"
    assert type(end_year) is int, "Ending year must be an integer"
    assert end_year>=start_year, "End year must be greater than or equal to starting year"
    year_ids = list(range(start_year, end_year + 1))
    if type(cause_ids) is int:
        cause_ids = [cause_ids]
    assert type(cause_ids) is list, "Cause IDs must be passed as a list of ints"

    vr = get_claude_data("disaggregation",
                         year_id=year_ids,
                         data_type_id=9, 
                         is_active=True,
                         verbose=True,
                         location_set_id=35, 
                         exec_function=subset_causes,
                         exec_function_args=[cause_ids])
    return vr


def pull_vr_old(cause_ids, start_year, end_year):

    assert type(start_year) is int, "Starting year must be an integer"
    assert type(end_year) is int, "Ending year must be an integer"
    assert end_year>=start_year, "End year must be greater than or equal to starting year"
    year_ids = list(range(start_year, end_year + 1))
    if type(cause_ids) is int:
        cause_ids = [cause_ids]
    assert type(cause_ids) is list, "Cause IDs must be passed as a list of ints"

    vr_old = get_claude_data("disaggregation",
                     year_id=year_ids,
                     data_type_id=9, 
                     verbose=True,
                     source="ICD7A",
                     location_set_id=35, 
                     exec_function=subset_causes,
                     exec_function_args=[cause_ids])
    return vr_old



def format_vr(df, related_causes_dict):
    # Subset to necessary columns
    formatted = df.drop(labels=['code_id','extract_type_id','site_id'],
                        axis=1,errors='ignore')
    # GROUPING AND ADDING BY AGGREGATE CAUSE ID AND SOURCE
    # Map to the shock cause IDs that are used for modeling
    formatted['cause_id'] = formatted['cause_id'].map(related_causes_dict)
    formatted = (formatted
                   .groupby(by=[i for i in formatted.columns if i!='deaths'])
                   .sum()
                   .reset_index())
    # Add new columns
    formatted['dataset'] = "VR"
    # Add acauses for all causes
    q = ("""SELECT cause_id, acause
           FROM ADDRESS
           WHERE cause_id IN {}"""
              .format(tuple(df['cause_id'].unique().tolist())))
    acause_df = query(q,conn_def='epi')
    acause_dict = dict(zip(acause_df['cause_id'],
                          acause_df['acause']))
    formatted['dataset_event_type'] = formatted['cause_id'].map(acause_dict)
    formatted = formatted.rename(columns={'year_id':'year'})

    causes_no_btl = [855, 945, 851]
    btl_nids = list(get_datasets(source="ICD9_BTL")['nid'].unique())
    is_btl_and_badcause = formatted['nid'].isin(btl_nids) & formatted['cause_id'].isin(causes_no_btl)
    formatted = formatted[~is_btl_and_badcause]

    return formatted


if __name__=="__main__":
    DEFAULT_MIN_YEAR = 1950
    DEFAULT_MAX_YEAR = max(get_demographics('mort')['year_id'])
    # Read input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-c","--causes",type=str,
                        help="The list of cause_ids to pull from VR, passed as a"
                             " string of comma-separated integers")
    parser.add_argument("-s","--startyear",type=int,
                        default=DEFAULT_MIN_YEAR,
                        help="The first year to pull CoD data for.")
    parser.add_argument("-e","--endyear",type=int,
                        default=DEFAULT_MAX_YEAR,
                        help="The first year to pull CoD data for.")
    parser.add_argument("-o","--outfile",type=str,
                        help="The CSV file where pulled VR data will be saved")
    parser.add_argument("-n","--encoding",type=str,
                        help="Encoding for the final saved file")
    cmd_args = parser.parse_args()
    assert cmd_args.outfile is not None, "Program requires an output filepath"
    # Convert causes into a list of ints
    assert np.all([i in [str(x) for x in range(0,10)]+[','] 
                     for i in cmd_args.causes]), "Invalid characters in 'causes'"
    cause_ids = [int(i) for i in cmd_args.causes.split(",")]

    related_causes_dict = get_related_causes_dict(cause_list=cause_ids,
                                                  gbd_round_id=5,
                                                  cause_set_id=4)

    if cmd_args.endyear >= 1980: 
      vr = pull_vr(cause_ids=related_causes_dict.keys(),
                   start_year=cmd_args.startyear,
                   end_year=cmd_args.endyear)

    elif cmd_args.endyear < 1974:
      vr = pull_vr_old(cause_ids=related_causes_dict.keys(),
                   start_year=cmd_args.startyear,
                   end_year=cmd_args.endyear)


    vr = format_vr(df=vr, related_causes_dict=related_causes_dict)
    vr.to_csv(cmd_args.outfile, encoding=cmd_args.encoding, index=False)
    print("VR data pulled successfully for the following "
          "cause_ids: {}".format(cause_ids))
    