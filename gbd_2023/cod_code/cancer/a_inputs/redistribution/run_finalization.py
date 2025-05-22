# -*- coding: utf-8 -*-
'''
Description: Recombines redistributed data in preparation for compile steps,
    applying recodes and restrictions before saving
Arguments: 06_redistrbuted.dta file
Output: .dta file
Contributors: INDIVIDUAL_NAME INDIVIDUAL_NAME, INDIVIDUAL_NAME INDIVIDUAL_NAME, INDIVIDUAL_NAME INDIVIDUAL_NAME, INDIVIDUAL_NAME INDIVIDUAL_NAME, INDIVIDUAL_NAME INDIVIDUAL_NAME
'''

# Import libraries
import pandas as pd
import os
from os import path
import sys
import numpy as np
from sys import argv
from datetime import date, datetime
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning) 
from multiprocessing import Pool
import multiprocessing as mp
from itertools import product, repeat

# Import cancer_estimation utilities and modules
from cancer_estimation.py_utils import (
    common_utils as utils,
    data_format_tools as dft,
    gbd_cancer_tools
)
from cancer_estimation._database import cdb_utils as cdb
from cancer_estimation.registry_pipeline import cause_mapping as cm
from cancer_estimation.a_inputs.a_mi_registry import (
    mi_dataset as md,
    populations as pop,
    pipeline_tests as pt
)

# Set db_link, used for connecting to SQL database
db_link = cdb.db_api()

def restrict_redistributed(final_data, pre_rdp_data):
    ''' Removes data for any causes that were not present before redistribution
            (prevents creation of cause-data that )
    '''
    uid_cols = md.get_uid_cols(7)
    existing_causes = pre_rdp_data.loc[:, uid_cols].drop_duplicates()
    final_causes = final_data.merge(existing_causes)
    return(final_causes)


def calculate_garbage_contribution(final_data, pre_rdp_data, metric):
    ''' Calculates the proportion of each cause that must have come from garbage
            redistribution
    '''
    uid_cols = md.get_uid_cols(7)
    pre_rdp_no_garbage = pre_rdp_data.loc[pre_rdp_data['acause'] != "_gc",
                                          uid_cols + [metric]]
    pre_rdp_no_garbage.rename(columns={metric: 'pre_rdp'}, inplace=True)
    output = final_data.merge(pre_rdp_no_garbage,how='left',indicator=True)
    output.loc[output['_merge'].eq('left_only'), 'pre_rdp'] = 0
    output['{}_gc'.format(metric)] = (output[metric]-output['pre_rdp'])
    output['{}_pct_gc'.format(metric)] = output['{}_gc'.format(
        metric)]/output[metric]
    del output['pre_rdp']
    del output['_merge']
    return(output)


def load_dataset_nids(dataset_id, data_type_id, nid_cols):
    ''' Returns a dataframe containing the NIDs for the dataset-data_type
    '''
    ds_df = cdb.db_api().get_entry("dataset", "dataset_id", dataset_id)
    qid = ds_df['queue_id'].values[0]
    nid_df = cdb.db_api().get_table("nqry")

    # Try initial match on data_type_id to see if there are matching NIDs
    matching_entries = (nid_df['queue_id'].isin([qid]) &
                        nid_df['data_type_id'].isin([data_type_id]))
    
    # If there are no matches, its possible the NIDs are under "data_type_id = 1" which will never
    # have a match based off of the data_type_id value processed through prep (as of 1/16/24) given
    # that data_type_id = 1 datasets have their data split by incidence and mortality, so the only
    # data_type_ids processed through prep are data_type_id = [2,3] (incidence and mortality).
    # Perform another check under data_type_id = 1 to see if we get any matches.
    if not any(matching_entries):
        matching_entries = (nid_df['queue_id'].isin([qid]) & (nid_df['data_type_id'] == 1))

    nid_df = nid_df.loc[matching_entries, nid_cols]
    return(nid_df)

def format_nids_for_merge(nid_df):
    '''
    '''
    def from_range(row):
        return(int(row['year_start'] != row['year_end']))

    uid_cols = ['registry_index', 'year_start', 'year_end']
    nid_df['from_range'] = nid_df.apply(from_range, axis=1)
    nid_df = gbd_cancer_tools.fill_from_year_range(nid_df)
    nid_df = nid_df.sort_values(uid_cols+['from_range']
                                ).drop_duplicates(uid_cols, keep='first')
    nid_df = gbd_cancer_tools.add_year_id(nid_df)
    nid_df['year'] = ((nid_df['year_start'] +
                       nid_df['year_end']) / 2).astype(float)
    assert not nid_df.duplicated(uid_cols).any(), \
        "Error removing redundant entries; redundant entries still exist"
    return(nid_df)


def match_nid_to_data(data, nids, uid_cols):
    ''' Attaches the NID associated with each datapoint to the dataframe
    '''
    data = gbd_cancer_tools.add_year_id(data)
    df = data.merge(nids, on=['registry_index', 'year'],
                    how='outer', indicator=True)
    matching_nid = ((df['year'] >= df['nid_start'])
                    & (df['year'] <= df['nid_end']))
    df = df.loc[(df['_merge'].isin(['both']) & matching_nid)
                | df['_merge'].isin(['left_only']), :]
    df['nid_range'] = df['nid_end'] - df['nid_start'] + 1
    df = df.sort_values(uid_cols+['nid_range']
                        ).drop_duplicates(uid_cols, keep='first')
    assert len(df) == len(
        data), "Some datapoints lost while finding matching NIDS"
    return(df)


def add_nids(input_df, dataset_id, data_type_id):
    ''' Returns the dataframe with attached NIDs
    '''
    nid_cols = ['registry_index', 'year_start',
                'year_end', 'nid', 'underlying_nid']
    uid_cols = md.get_uid_cols(7)
    output_cols = input_df.columns.tolist() + ['nid', 'underlying_nid']

    # Retrieve dataframe containing NID matches from NQRY SQL table
    nid_df = load_dataset_nids(dataset_id, data_type_id, nid_cols)
    
    # If there are resulting NID matches...
    if len(nid_df) > 0:  
        nid_df = format_nids_for_merge(nid_df)
        nid_df.rename(columns={'year_start': 'nid_start',
                               'year_end': 'nid_end'}, inplace=True)
        output = match_nid_to_data(input_df, nid_df, uid_cols)
        assert not output.duplicated(uid_cols).any(), \
            "Error during merge with NIDs: redundant entries exist"
        if output['nid'].isnull().any():
            print("ALERT! SOME DATAPOINTS ARE MISSING NIDs!!") # There may be only partial NID matching
        assert len(output) == len(input_df), "Some datapoints lost while adding NIDS"
        return(output[output_cols])
    
    # Else if there are no NID matches, set 'nid' & 'underlying_nid' -> 0, this will raise 
    # an exception once returned flagging to the user that we are missing NIDs for this dataset_id & data_type_id.
    else:
        input_df['nid'] = 0
        input_df['underlying_nid'] = 0
        return input_df


def compareSum(subset, parent_cause, exist_codes, metric, uid_cols, uids_noAcause, ds_id):
    ''' 
        Compare the sum of parent and sum of children to ensure any aggregations don't result in a parent < the children
        We do this by coding system ID as recommended by INDIVIDUAL_NAME so we keep IICC3 encodings away from ICD, GBD, and Custom
        if sum of parent >= sum of children and sum of children is not 0
            do nothing
            keep parent
            keep children
        else if sum of children == 0 and sum of parent is not 0
            drop children
            keep parent
        else if sum of parent == 0 and sum of children is not 0
            drop parent
            keep children
        else if sum of parent < sum of children and sum of parent is not 0 and sum of children is not 0
            if all cases/deaths from parent come from _gc
                aggregate parent and subtypes to make new parent
                keep parent
                keep children
            else
                flag/look into dataset
        else # don't think it should reach here...
            drop parent
            aggregate subtypes to create parent
        returns adjusted data
    '''
    subset = dft.collapse(subset, by_cols=uid_cols, func='sum', stub=metric)
    
    # At the moment, if our data is for neo_liver there is a small chance that the ectomies are present. If that's the case
    # we want to be sure that we collapse that into the parent.  The first if/else block ensures that the liver parent encompasses
    # the ectomies (and processes the leukemias separately). The second if/else block ensures that our data is properly
    # collapsed over UIDs so our data is as tight as possible before recalculating at all.

    # handle where hbl could have ectomies at this point
    if parent_cause == 'neo_liver':
        # df with just subtypes & aggregate
        subtypes = subset.loc[subset['acause'] == 'neo_liver_hbl']
        sub_agg_sum = dft.collapse(subtypes, by_cols=uids_noAcause, func='sum', stub=metric)
        sub_indiv_sum = dft.collapse(subtypes, by_cols=uid_cols, func='sum', stub=metric)
        # df with just the parent (and potentially ectomies in this case)
        parent = subset.loc[subset['acause'] != 'neo_liver_hbl']
        parent_sum = dft.collapse(subset, by_cols=uids_noAcause, func='sum', stub=metric)
        parent_sum['acause'] = parent_cause
        # df with all causes
        all_sum = pd.concat([sub_indiv_sum, parent_sum], sort=True)
    else :
        # df with just subtypes & aggregate
        subtypes = subset.loc[(subset['acause'].str.contains(parent_cause, regex = True)) & ~(subset['acause'].eq(parent_cause))]
        sub_agg_sum = dft.collapse(subtypes, by_cols=uids_noAcause, func='sum', stub=metric)
        sub_indiv_sum = dft.collapse(subtypes, by_cols=uid_cols, func='sum', stub=metric)
        # df with just the parent
        parent = subset.loc[subset['acause'].eq(parent_cause)]
        parent_sum = dft.collapse(parent, by_cols=uids_noAcause, func='sum', stub=metric)
        parent_sum['acause'] = parent_cause
        # df with all causes
        all = subset.loc[(subset['acause'].str.contains(parent_cause, regex = True))]
        all_sum = dft.collapse(all, by_cols=uid_cols, func='sum', stub=metric)
   
    if parent_cause == 'neo_liver':
        hbl = sub_indiv_sum.copy()
        # check to make sure hbl isn't empty
        if hbl.empty:
            validated_subset = all_sum.copy()
        # if hbl > parent
        elif (hbl[metric] > parent_sum[metric])[0]:
            saveErrors(ds_id=ds_is, acause=parent_cause, reg_idx=subset.registry_index.unique()[0], ys=subset.year_start.unique()[0], \
                ye=subset.year_end.unique()[0], sex=subset.sex_id.unique()[0], age=subset.age.unique()[0], metric=metric, \
                    message="liver_hbl is larger than the parent - this shouldn't happen. Look into dataset.")
            pause_look = input("liver_hbl is larger than the parent - this shouldn't happen. Look into dataset.")
            validated_subset = all_sum.copy()
        else:
            validated_subset = all_sum.copy()

    else:
        # if the parent or the children aren't present
        if parent_sum.empty | sub_agg_sum.empty:
            validated_subset = all_sum.copy()
            print('      missing leukemia parent or all children')

        # if sum of parent >= sum of the children & sum of children != 0
        elif (parent_sum[metric] >= sub_agg_sum[metric])[0] & (sub_agg_sum[metric] != 0)[0]: #added [0] to avoid 'truth of a series is ambiguous' errors
            validated_subset = all_sum.copy()

        # elif sum of children == 0 and sum of parent != 0
        elif (sub_agg_sum[metric] == 0)[0] & (parent_sum[metric] != 0)[0]:
            print('      children are all 0 {metric}'.format(metric=metric))
            validated_subset = all_sum.copy()

        # elif sum of parent == 0 and sum of children != 0
        elif (parent_sum[metric] == 0)[0] & (sub_agg_sum[metric] != 0)[0]:
            print('      parent is empty')
            validated_subset = all_sum.copy()

        # elif sum of parent < sum of children & sum of parent != 0 & sum of children != 0
        elif (parent_sum[metric] < sub_agg_sum[metric])[0] & (parent_sum[metric] != 0)[0] & (sub_agg_sum[metric] != 0)[0]:
            print('      parent is less than children, will be recalculated')
            validated_subset = all_sum.copy()
        
        else :
            validated_subset = all_sum.copy()
    
    
    # collapse one more time just to be sure
    validated_subset = dft.collapse(validated_subset, by_cols=uid_cols, func='sum', stub=metric)

    return(validated_subset)


def aggregate_subtypes_worker(uid, subset, this_dataset, parent_cause, num_codes_all, dst, csi, tdf):
    '''		
        # have to handle differently based on coding systems
        If cause == neo_leukemia
            if any amount of subtypes in data and parent not in data
                aggregate subtypes to create parent
                append parent to data 

                if 4 or 5 subtypes 
                    append children to parent
                if 3 subtypes and equal to lymphoid, myeloid, other 
                    drop children
                if 3 subtypes and equal to ALL, AML, CML and coding system id = 3
                    append children to parent
                if 2 subtypes and equal to ALL, AML and coding system id = 3
                    append children to parent
                else
                    flag for INDIVIDUAL_NAME
            
            if only parent in data
                do nothing

            if any amount of subtypes in data and parent in data
                if coding_system == ICD10 #do same as above
                    aggregate parent and subtypes to form new parent
                    append parent to data 

                    if 4 or 5 subtypes
                        append children to parent
                    if 3 subtypes and equal to lymphoid, myeloid, other 
                        drop children
                    if 3 subtypes and equal to ALL, AML, CML and coding system id = 3
                        append children to parent
                    if 2 subtypes and equal to ALL, AML and coding system id = 3
                        append children to parent
                    else 
                        flag for INDIVIDUAL_NAME

                if coding_system is in ICD9, ICCC3, GBD, CUSTOM
                    run compareSum function
                    if 5 subtypes
                        do nothing
                    if 4 subtypes
                        aggregate children
                        subtract children sum from parent to get other subtype
                        append parent and children
                    if 3 subtypes and equal to lymphoid, myeloid, other 
                        drop children
                    if 3 subtypes and equal to ALL, AML, CML and coding system id = 3
                        append children to parent
                    if 2 subtypes and equal to ALL, AML and coding system id = 3
                        append children to parent
                    else 
                        flag for INDIVIDUAL_NAME

        If cause == neo_liver
            if coding_system in ICD9, ICD10:
                if any amount of 5 liver ectomies in data
                    aggregate subtypes to create parent
                    append parent to subtypes

                if only parent in data
                    do nothing

                if parent and any amount of 5 liver ectomies present
                    aggregate parent and 5 liver ectomies to form new parent
                
            if liver_hbl present and parent present:
                # a bit trickier for neo_liver_hbl but essentially we want to take the cases/deaths it 
                # received from rdp and add it to liver parent
                
                aggregate cases/deaths from rdp for liver_hbl and add to liver parent

                if liver_hbl > liver_parent
                    flag and look into data
    '''
    # SHOW COMPLETION STATUS
    print("Currently on uid {} for parent cause: {}".format(uid, parent_cause))
    print("Total uids {} for {}".format(len(subset['uid'].unique()), parent_cause))

    # set necessary vars
    subset = subset.loc[subset['uid'].eq(uid)]
    uid_cols = md.get_uid_cols(7)
    uids_noAcause = [uid for uid in uid_cols if 'acause' not in uid]
    metric = this_dataset.metric
    exist_codes = subset['acause'].unique() # parent and/or subtype codes in current group

    # get coding_system id and name
    coding_system_id = dst.loc[dst['dataset_id'].eq(this_dataset.dataset_id), 'coding_system_id'].values[0]
    coding_system_id_defined = (coding_system_id is not None) and (not np.isnan(coding_system_id))
    if coding_system_id_defined:
        coding_system_name = csi.loc[csi['coding_system_id'] == int(coding_system_id), 'coding_system'].values[0]
    else:
        # if coding_system_id is null, pull from age-sex splitting step and manually create ids/names
        # Pull prep_step 4
        # Extract the coding_system_name[s] present
        cs_name = tdf.coding_system.unique()
        if len(cs_name) > 1: # if there are multiple coding_systems listed, manually look this up
            saveErrors(ds_id=this_dataset.dataset_id, acause=parent_cause, reg_idx=subset.registry_index.unique()[0], ys=subset.year_start.unique()[0], \
                ye=subset.year_end.unique()[0], sex=subset.sex_id.unique()[0], age=subset.age.unique()[0], metric=metric, \
                    message="Dataset has multiple coding systems. Check prep step 4 for listed coding_systems and update coding_system_id column in `dataset` table.")
            pause_look = input("Dataset has multiple coding systems. Check prep step 4 for listed coding_systems and update coding_system_id column in `dataset` table.")
        else:
            cs_name = cs_name[0]
        # Match it to a coding_system_id if possible
        if cs_name == 'ICD9_detail':
            cs_id = 2
            cs_name = 'ICD9'
        else:
            cs_id = csi.loc[csi['coding_system'].eq(cs_name), 'coding_system_id'].values[0]
        if cs_id is None: # if coding_system_id is still null, manually look it up
            saveErrors(ds_id=this_dataset.dataset_id, acause=parent_cause, reg_idx=subset.registry_index.unique()[0], ys=subset.year_start.unique()[0], \
                ye=subset.year_end.unique()[0], sex=subset.sex_id.unique()[0], age=subset.age.unique()[0], metric=metric, \
                    message="Dataset has non-matched coding_system. Check prep step 4 for listed coding_system and update codings_system_id in `dataset` table.")
            pause_look = input("Dataset has non-matched coding_system. Check prep step 4 for listed coding_system and update codings_system_id in `dataset` table.")
        else :
            coding_system_id = cs_id
            coding_system_name = cs_name

    # enforce types on coding_system variables
    coding_system_id = int(coding_system_id)
    coding_system_name = str(coding_system_name)

    # compareSum for leukemias + liver
    if parent_cause in ['neo_leukemia', 'neo_liver']:
        subset = compareSum(subset, parent_cause, exist_codes, metric, uid_cols, uids_noAcause, this_dataset.dataset_id)

    # collapse subset just in case it wasn't done already
    subset = dft.collapse(subset, by_cols=uid_cols, func='sum', stub=metric)
    # tags each subtype and aggregates based on that col and uids
    sub_df = subset.loc[(subset['acause'].str.contains(parent_cause, regex = True)) & ~(subset['acause'].eq(parent_cause))]
    # contains any parent and subtypes
    with_sub = subset.loc[(subset['acause'].str.contains(parent_cause, regex = True))]
    parent = subset.loc[subset['acause']==parent_cause]

    # Quick check to see if parent already >= to children
    sub_collapse = dft.collapse(sub_df, by_cols=uids_noAcause, func='sum', stub=metric)
    par_collapse = dft.collapse(parent, by_cols=uids_noAcause, func='sum', stub=metric)
    # if the children don't exist in this UID
    if sub_df.empty:
        # return subset
        return(subset)
    # if the parent doesn't exist in this UID
    elif parent.empty:
        # create the parent from the subcauses
        print('creating parent')
    # if the children sum to 0 in this UID
    elif sub_collapse.empty:
        # if the parent is also 0 (and is present)
        if par_collapse.empty & ~(parent.empty):
            # return subset
            return(subset)
        # if the parent is >= to children
        if par_collapse[metric][0] >= sub_collapse[metric][0]:
            # return subset
            return(subset)
    # if the parent sums to 0 in this UID
    elif par_collapse.empty:
        # if the children sum is > 0
        if ~(sub_collapse.empty):
            # create the parent from the subcauses
            print('creating parent')
        # if the children sum is also 0
        if sub_collapse.empty & ~(sub_df.empty):
            # return subset
            return(subset)
    # if the parent is already >= to the children
    elif par_collapse[metric][0] >= sub_collapse[metric][0]:
        # return subset
        return(subset)
    
    
    ######################
    ##      LIVER        #
    ######################
    # special case until rdp packages are fixed for liver, to aggregate 5 subcauses
    if parent_cause == "neo_liver":
        if coding_system_id in [1, 2]:
            # separate out liver hbl
            hbl_subset = with_sub.loc[with_sub['acause'] == "neo_liver_hbl"]
            with_sub = with_sub.loc[with_sub['acause'] != "neo_liver_hbl"]
            if (parent_cause not in exist_codes) & (len(exist_codes) >= 1):
                with_sub['acause'] = parent_cause
                with_sub = dft.collapse(with_sub, by_cols=uid_cols, func='sum', stub=metric)
            elif set([parent_cause]) == set(exist_codes):
                pass
            elif (parent_cause in exist_codes) & (len(exist_codes) > 1):
                with_sub['acause'] = parent_cause
                with_sub = dft.collapse(with_sub, by_cols=uid_cols, func='sum', stub=metric)
        else:
            pass
        # handle liver hbl separately
        if (len(set(["neo_liver_hbl", parent_cause])) <= len(set(exist_codes))) & (coding_system_id in [1, 2]):
            # a bit trickier for neo_liver_hbl but essentially we want to take 
            # the cases/deaths it received from rdp and add it to liver parent to create new parent
            parent_subset = with_sub.loc[with_sub['acause'].eq(parent_cause)]
            hbl_subset[metric] = hbl_subset["{}_gc".format(metric)] 
            parent_and_hbl = pd.concat([parent_subset, hbl_subset], sort=True)
            parent_and_hbl['acause'] = parent_cause
            parent_and_hbl = dft.collapse(parent_and_hbl, by_cols=uid_cols, # new parent
                                                        func='sum', stub=metric)
            # combine untouched liver hbl and new parent
            with_sub = pd.concat([sub_df[sub_df['acause'].eq("neo_liver_hbl")], parent_and_hbl], sort=True)

            # check to see if hbl sum is greater than parent
            parent_sum = with_sub[with_sub['acause'].eq(parent_cause)].sum()
            hbl_sum = with_sub[with_sub['acause'].eq(parent_cause)].sum()
            assert ((hbl_sum[metric] <= parent_sum[metric]) & (len(set([parent_cause, "neo_liver_hbl"])) <= len(set(exist_codes)))), "hbl sum is greater than parent sum"

            # combine original hbl and new parent
            complete = with_sub.copy()
        elif (len(set(["neo_liver_hbl", parent_cause])) <= len(set(exist_codes))) & (coding_system_id == 3):
            #aggregate parent and neo_liver_hbl to create new parent
		    #append parent to neo_liver_hbl
            with_sub['acause'] = parent_cause
            with_sub = dft.collapse(with_sub, by_cols=uid_cols, func='sum', stub=metric)
            # combine untouched liver hbl and new parent
            complete = pd.concat([sub_df[sub_df['acause'].eq("neo_liver_hbl")], with_sub], sort=True)                                    
        else:
            # don't do anything else to other coding systems, assumes neo_liver_hbl not present
            complete = with_sub.copy()

    ######################
    ## EYE + LYMPHOMA    #
    ######################
    elif parent_cause in ['neo_eye', 'neo_lymphoma']:
        if (parent_cause not in exist_codes) & (len(exist_codes) >= 1): # added >= to catch eye subtypes since they don't overlap in any age groups
            # no parent, only subtypes - collapse sum of cause-agnostic UIDs
            with_sub = dft.collapse(with_sub, by_cols=uids_noAcause, func='sum', stub=metric)
            with_sub['acause'] = parent_cause
            complete = pd.concat([sub_df, with_sub], sort=True)

        elif set([parent_cause]) == set(exist_codes):
            # only parent, do nothing, keep original subset
            complete = subset.copy()

        # dataset contains parent & subtypes
            '''
            As of GBD2023 and now relying on CoD's RDP process, the 'ALL-GC' RDP package will redistribute to potentially the parent cause
            resulting in having the parent & subtypes present after step 6 (RDP). This used to not be the case since we had manual control over
            specific behavior during RDP, but we no longer do. 

            The result is that we have a list of dataset_ids [454, 476, 49, 188, 187, 210, 315, 255] that were expected (unknown evaluation method) to have 
            the parent & subtype present at this step, but with the new RDP process we can expect a majority of our datasets to have a tiny value for
            the parent cause due to RDP, in addition to the subtype causes.

            The code below essentially performs this operation: parent = parent + subtypes

            If the dataset_id isn't in the list of exception datasets above, we will write out where this aggregation occurs and save it to the finalization_errors.csv file
            through the saveErrors function, but we will still perform the above operation, as approved for GBD2023.
            '''
        elif (parent_cause in exist_codes) & (len(exist_codes) > 1):
            if this_dataset.dataset_id in [454, 476, 49, 188, 187, 210, 315, 255]: # These are exception datasets that we've confirmed do/should have the parent&subcauses
                pass
            else:
                saveErrors(ds_id=this_dataset.dataset_id, acause=parent_cause, reg_idx=subset.registry_index.unique()[0], ys=subset.year_start.unique()[0], \
                    ye=subset.year_end.unique()[0], sex=subset.sex_id.unique()[0], age=subset.age.unique()[0], metric=metric, \
                        message="{parent} parent and subcauses present - this dataset may have had the parent cause introduced during RDP - good to validate for accuracy.".format(parent=parent_cause))
                # pause_look = input("-> {parent} parent and subcauses present - this is highly unlikely for lymphoma&eye. Validate this dataset then continue... :".format(parent=parent_cause)) # This pause line caused the code to crash, may work interactively.
            
            # parent and some subtypes
            with_sub = dft.collapse(with_sub, by_cols=uids_noAcause, func='sum', stub=metric)
            with_sub['acause'] = parent_cause
            complete = pd.concat([sub_df, with_sub], sort = True)
        
        else: # subset is empty
            saveErrors(ds_id=this_dataset.dataset_id, acause=parent_cause, reg_idx=subset.registry_index.unique()[0], ys=subset.year_start.unique()[0], \
                ye=subset.year_end.unique()[0], sex=subset.sex_id.unique()[0], age=subset.age.unique()[0], metric=metric, \
                    message="Paused to see why it would reach here, remove debug if testing complete")
            pause_look = input("Paused to see why it would reach here, remove debug if testing complete")

    ######################
    ##      Leukemia     #
    ######################
    elif parent_cause == 'neo_leukemia' :
        # if any number of subtypes but no parent are present
        if (parent_cause not in exist_codes) & (len(exist_codes) >= 1):
            # if 4or5 subtypes, append children to parent
            if len(exist_codes) >= 4:
                with_sub = dft.collapse(with_sub, by_cols=uids_noAcause, func='sum', stub=metric)
                with_sub['acause'] = parent_cause
                complete = pd.concat([sub_df, with_sub], sort=True)
                
            # if coding_system_id = 3, 3 subtypes, and == ALL, AML, CML, append children to parent
            elif (len(exist_codes) == 3) & (coding_system_id == 3) & ('neo_leukemia_ll_acute' in exist_codes) & ('neo_leukemia_ml_acute' in exist_codes) & ('neo_leukemia_ml_chronic' in exist_codes):
                with_sub = dft.collapse(with_sub, by_cols=uids_noAcause, func='sum', stub=metric)
                with_sub['acause'] = parent_cause
                complete = pd.concat([sub_df, with_sub], sort=True)

            # if coding_system_id = 3, 2 subtypes, and == ALL, AML; append children to parent
            elif (len(exist_codes) == 2) & (coding_system_id == 3) & ('neo_leukemia_ll_acute' in exist_codes) & ('neo_leukemia_ml_acute' in exist_codes):
                with_sub = dft.collapse(with_sub, by_cols=uids_noAcause, func='sum', stub=metric)
                with_sub['acause'] = parent_cause
                complete = pd.concat([sub_df, with_sub], sort=True)

            # if the dataset as a whole reports most-if-not-all leukemia subcauses (it's just this UID that's missing some), create the parent
            elif (num_codes_all >= 3) | (this_dataset.dataset_id in [443,499,332,333,439,440,443,525]):
                with_sub = dft.collapse(with_sub, by_cols=uids_noAcause, func='sum', stub=metric)
                with_sub['acause'] = parent_cause
                complete = pd.concat([sub_df, with_sub], sort=True)

            else:
                error_message = "Unexpected leukemia edge-case encountered. Look into this dataset. Missing parent."
                saveErrors(ds_id=this_dataset.dataset_id, acause=parent_cause, reg_idx=subset.registry_index.unique()[0], ys=subset.year_start.unique()[0], \
                        ye=subset.year_end.unique()[0], sex=subset.sex_id.unique()[0], age=subset.age.unique()[0], metric=metric, \
                            message=error_message)
                print(error_message)
            
        # if only parent present, do nothing & keep original subset
        elif set([parent_cause]) == set(exist_codes):
            complete = subset.copy()
        
        # if any number of subtypes + parent are present
        elif (parent_cause in exist_codes) & (len(exist_codes) > 1):
            # if coding_system == ICD10
            if coding_system_id == 1:
                # if 4or5 subtypes, append children to parent
                if len(exist_codes) >= 5:
                    with_sub = dft.collapse(with_sub, by_cols=uids_noAcause, func='sum', stub=metric)
                    with_sub['acause'] = parent_cause
                    complete = pd.concat([sub_df, with_sub], sort=True)
                    
                # if 3 subtypes, and == ALL, AML, CML, append children to parent
                elif (len(exist_codes) == 4) & ('neo_leukemia_ll_acute' in exist_codes) & ('neo_leukemia_ml_acute' in exist_codes) & ('neo_leukemia_ml_chronic' in exist_codes):
                    with_sub = dft.collapse(with_sub, by_cols=uids_noAcause, func='sum', stub=metric)
                    with_sub['acause'] = parent_cause
                    complete = pd.concat([sub_df, with_sub], sort=True)

                # if 2 subtypes, and == ALL, AML; append children to parent
                elif (len(exist_codes) == 3) & ('neo_leukemia_ll_acute' in exist_codes) & ('neo_leukemia_ml_acute' in exist_codes):
                    with_sub = dft.collapse(with_sub, by_cols=uids_noAcause, func='sum', stub=metric)
                    with_sub['acause'] = parent_cause
                    complete = pd.concat([sub_df, with_sub], sort=True)

                # if the dataset as a whole reports most-if-not-all leukemia subcauses (it's just this UID that's missing some), recreate the parent
                elif (num_codes_all >= 4) | (this_dataset.dataset_id in [311,322]):
                    with_sub = dft.collapse(with_sub, by_cols=uids_noAcause, func='sum', stub=metric)
                    with_sub['acause'] = parent_cause
                    complete = pd.concat([sub_df, with_sub], sort=True)

                else:
                    saveErrors(ds_id=this_dataset.dataset_id, acause=parent_cause, reg_idx=subset.registry_index.unique()[0], ys=subset.year_start.unique()[0], \
                        ye=subset.year_end.unique()[0], sex=subset.sex_id.unique()[0], age=subset.age.unique()[0], metric=metric, \
                            message="Unexpected leukemia edge-case encountered. Look into this dataset. ICD10")
                    pause_look = input("Unexpected leukemia edge-case encountered. Look into this dataset. ICD10")

            # if coding_system == ICD9, IICC3, GBD, or Custom
            else:
                # if all 5 subtypes present, resum children & append
                if len(exist_codes) == 6:
                    with_sub = dft.collapse(with_sub, by_cols=uids_noAcause, func='sum', stub=metric)
                    with_sub['acause'] = parent_cause
                    complete = pd.concat([sub_df, with_sub], sort=True)

                # if all subtypes but CLL present (this would only really happen in the younger age groups)
                elif (len(exist_codes) == 5) & ('neo_leukemia_ll_chronic' not in exist_codes):
                    with_sub = dft.collapse(with_sub, by_cols=uids_noAcause, func='sum', stub=metric)
                    with_sub['acause'] = parent_cause
                    complete = pd.concat([sub_df, with_sub], sort=True)

                # if 3 subtypes, and == ALL/AML/CML, and cs_id = 3: append children to parent
                elif (len(exist_codes) == 4) & (coding_system_id == 3) & ('neo_leukemia_ll_acute' in exist_codes) & ('neo_leukemia_ml_acute' in exist_codes) & ('neo_leukemia_ml_chronic' in exist_codes):
                    with_sub = dft.collapse(with_sub, by_cols=uids_noAcause, func='sum', stub=metric)
                    with_sub['acause'] = parent_cause
                    complete = pd.concat([sub_df, with_sub], sort=True)

                # if 2 subtypes, and cs_id=3, and == ALL/AML: append children to parent
                elif (len(exist_codes) == 3) & (coding_system_id == 3) & ('neo_leukemia_ll_acute' in exist_codes) & ('neo_leukemia_ml_acute' in exist_codes):
                    with_sub = dft.collapse(with_sub, by_cols=uids_noAcause, func='sum', stub=metric)
                    with_sub['acause'] = parent_cause
                    complete = pd.concat([sub_df, with_sub], sort=True)

                # if the dataset as a whole reports most-if-not-all leukemia subcauses (it's just this UID that's missing some), recreate the parent
                elif (num_codes_all >= 4) | (this_dataset.dataset_id in [311]):
                    with_sub = dft.collapse(with_sub, by_cols=uids_noAcause, func='sum', stub=metric)
                    with_sub['acause'] = parent_cause
                    complete = pd.concat([sub_df, with_sub], sort=True)

                # else flag
                else:
                    saveErrors(ds_id=this_dataset.dataset_id, acause=parent_cause, reg_idx=subset.registry_index.unique()[0], ys=subset.year_start.unique()[0], \
                        ye=subset.year_end.unique()[0], sex=subset.sex_id.unique()[0], age=subset.age.unique()[0], metric=metric, \
                            message="Unexpected leukemia edge-case encountered. Look into this dataset. Non-ICD10")
                    pause_look = input("Unexpected leukemia edge-case encountered. Look into this dataset. Non-ICD10")

    else :
        complete = subset.copy()
    
    # Sometimes, the above code doesn't work correctly, and 'complete' isn't created. Raise an error.
    try:
        complete
    except:
        print("Unable to finish 'aggregate_subtypes_worker' -- dataframe 'complete' has not been created.")
        print("Given the complexity of the code, it hasn't been feasible to pinpoint the cause.")
        print(f"Important parameters -- uid: {uid}, parent_cause: {parent_cause}, num_codes_all: {num_codes_all}")
        raise NameError("'complete' has not been created.")

    # flag if input causes = the output causes (no parent created)
    if len(exist_codes) == len(complete['acause'].unique()):
REDACTED
        print('      UIDs: reg_idx={reg_idx} start={ys} end={ye} sex={sex} age={age}'.format(reg_idx=subset.registry_index.unique()[0], ys=subset.year_start.unique()[0], \
            ye=subset.year_end.unique()[0], sex=subset.sex_id.unique()[0], age=subset.age.unique()[0]))

    # flag if the input causes < the output causes (we generated new causes - likely a parent cause)
    elif len(exist_codes) < len(complete['acause'].unique()) :
REDACTED
        print('      UIDs: reg_idx={reg_idx} start={ys} end={ye} sex={sex} age={age}'.format(reg_idx=subset.registry_index.unique()[0], ys=subset.year_start.unique()[0], \
            ye=subset.year_end.unique()[0], sex=subset.sex_id.unique()[0], age=subset.age.unique()[0]))
    
    # flag if the input causes > the output causes (we lost causes - likely due to aggregating the liver ectomies into the parent)
    elif len(exist_codes) > len(complete['acause'].unique()) :
REDACTED
        print('      UIDs: reg_idx={reg_idx} start={ys} end={ye} sex={sex} age={age}'.format(reg_idx=subset.registry_index.unique()[0], ys=subset.year_start.unique()[0], \
            ye=subset.year_end.unique()[0], sex=subset.sex_id.unique()[0], age=subset.age.unique()[0]))
    
    # collapse one final time to make sure everything is packed tightly
    complete = dft.collapse(complete, by_cols=uid_cols, func='sum', stub=metric)
    return(complete)


def aggregate_subtypes(input_df, this_dataset, metric, uid_cols):
    ''' Creates neo_eye, neo_lymphoma, neo_leukemia parents by aggregating up the 
        children subtypes
    '''
    parent_causes = ['neo_leukemia', 'neo_lymphoma', 'neo_eye', 'neo_liver']
    metric = this_dataset.metric
    uids_noAcause =[uid for uid in uid_cols if 'acause' not in uid]
    nonagg_df = input_df.loc[(~input_df['acause'].str.contains("|".join(parent_causes), regex = True))]
    agg_df = input_df.loc[(input_df['acause'].str.contains("|".join(parent_causes),regex = True))]

    # check if only parents are present or nothing to aggregate
    if len(agg_df) == 0 or set(list(agg_df['acause'].unique())) <= set(parent_causes): 
        return(input_df)

    # load needed tables
    dst = db_link.get_table("dataset")
    csi = db_link.get_table('coding_system')
    temp = md.MI_Dataset(this_dataset.dataset_id, 4, this_dataset.data_type_id)
    tdf = temp.load_input()

    total_dfs = []
    for parent_cause in parent_causes:
        cur_subset = agg_df[agg_df['acause'].str.contains(parent_cause)].copy()
        if len(cur_subset) > 0:
            # work on each uid group
            print('  Enforcing parent:subcause hierarchy for {parent}'.format(parent=parent_cause))

            # create uid by group for dataset to subset on later
            cur_subset.loc[:, 'uid'] = cur_subset.groupby(uids_noAcause).ngroup()
            uid_list = list(cur_subset['uid'].unique())
            num_codes_all = len(cur_subset.acause.unique())
            # run aggregate_subtype_worker in parallel
            with Pool(processes= mp.cpu_count()) as pool: # or max your hardware can support
                # have your pool map each subset to its respective
                prepped_data = pool.starmap(aggregate_subtypes_worker, 
                                            zip(uid_list,
                                            repeat(cur_subset, times = len(uid_list)), 
                                            repeat(this_dataset, times = len(uid_list)), 
                                            repeat(parent_cause, times = len(uid_list)),
                                            repeat(num_codes_all, times=len(uid_list)),
                                            repeat(dst, times = len(uid_list)),
                                            repeat(csi, times = len(uid_list)),
                                            repeat(tdf, times = len(uid_list))))

            prepped_data = pd.concat(prepped_data, ignore_index=True)
            total_dfs.append(prepped_data)
        else:
            continue

    total_df = pd.concat(total_dfs + [nonagg_df], sort=True)

    # recalculate _gc percent
    if "{}_gc".format(metric) in total_df.columns:
        total_df['{}_pct_gc'.format(metric)] = total_df["{}_gc".format(metric)]/total_df[metric]
    return(total_df)


def saveErrors(ds_id, acause, reg_idx, ys, ye, sex, age, metric, message):
    ''' Function to save individual errors to common CSV file
    '''
    # Compile error data
    errors = pd.DataFrame({'date':[date.today()], 'time':[datetime.now().strftime("%H:%M")], 'dataset_id':[ds_id], 'acause':[acause], 'registry_index':[reg_idx], \
        'year_start':[ys], 'year_end':[ye], 'sex_id':[sex], 'age_group_id':[age], 'metric':[metric], 'error_reason':[message], 'resolution':['']})
    
    # Try to write the error data to the error file
    # First make sure the directory exists
    finalization_dir = utils.get_path(key="finalization",
                                        process = "mi_dataset", 
                                        base_folder="workspace") + '/'
    
    # If the utils.get_path() above results in issues running this step, you can try manually specifying the path below instead, unsure why get_path() has lookup issues
    # finalization_dir = '/share/cancer/<GBD_ROUND>/a_inputs/a_mi_registry/finalization/'
    
    _ = utils.ensure_dir(finalization_dir)
    # Now try to add to the file, or create it if it doesn't exist. Otherwise print some error message
    finalization_file = "{}finalization_errors.csv".format(finalization_dir)
    if os.path.exists(finalization_file):
        try:
            errors.to_csv(finalization_file, index=False,
                            mode='a', header=False)
        except:
            print("Errors didn't save! They should have saved to {}".format(finalization_file))
    else:
        errors.to_csv(finalization_file, index=False,
                            mode = 'w', header=True)
    print(f"Error info saved to {finalization_file}")


def test_output(output_df, input_df):
    ''' Verify that no cause-years were dropped through finalization. Ignore age
            because new cause-ages may be added in recode
    '''
    test_cols = [c for c in md.get_uid_cols(7) if c != 'age']
    test_df = pd.merge(input_df[test_cols].drop_duplicates(),
                       output_df[test_cols].drop_duplicates(),
                       how='left', indicator=True)
    assert not test_df['_merge'].eq("left_only").any(), \
        "Some uids lost during finalization"

    # also checks for any non gbd_causes in df
    #assert not test_df['acause'].str.isalpha().any(), \
    #    "Some causes not matched to any gbd cause"
    return(None)


def main(dataset_id, data_type_id):
    ''' Applies post-rdp restrictions and combines metric data with population
            and metadata (NIDs)

        High level summary:
        -
    '''
    # Load dataset
    this_dataset = md.MI_Dataset(dataset_id, 7, data_type_id)
    input_data = this_dataset.load_input()
    metric = this_dataset.metric

    # Ensure collapse of data
    uid_cols = md.get_uid_cols(7)
    df = input_data[uid_cols + [metric]]
    df = dft.collapse(df, by_cols=uid_cols, func='sum', stub=metric)
    df = md.stdz_col_formats(df)

    # Apply post-rdp adjustments: recode data that are outside of restrictions,
    #   then remove data for causes that did not exist prior to RDP
    df = cm.recode(df, data_type_id)
    pre_final_df = dft.collapse(df, by_cols=uid_cols, func='sum', stub=metric)
    pt.verify_metric_total(pre_final_df, input_data, metric, "after recode in finalization")
    if dataset_id not in range(10, 20):
        pre_rdp_data = md.MI_Dataset(dataset_id, 6, data_type_id).load_input()
        pre_final_df = calculate_garbage_contribution(pre_final_df, pre_rdp_data, metric)
    
    # Aggregate up eye, leukemia, liver, and lymphoma subtypes
    if dataset_id != 10:
        post_agg_df = aggregate_subtypes(pre_final_df, this_dataset, metric, uid_cols)
    else:
        post_agg_df = pre_final_df.copy()

    
    # Merge with population data
    # If dataset is a VR dataset (dataset_ids 10-19, only dataset_id 10 is currently active and used for prep)
    # then we can merge with IHME Population data
    # Attach IHME population data if the dataset is a VR dataset (dataset_id = 10) OR
    # (not yet implemented / tested) if the dataset's registry index's location_id is in the High SDI quintile 
    # based off the most recent location_set_id for SDI regions & the registry_index from the SQL registry table has
    # coverage_of_location_id = 1 indicating that the registry index covers the entire location id.
    '''
    Dataset IDs that (as of 1/19/24):
    -Have a registry index with a location_id in the High SDI quintile
    -Have a registry index with coverage_of_location_id = 1
    [628, 386, 776, 799, 625, 322, 379, 496, 495, 320, 356, 291, 112, 429, 494,
     590, 458, 665, 500, 533, 667, 452, 453, 451, 229, 276, 847, 213, 316, 224, 
     739, 335, 370, 357, 233, 360, 28, 720, 290, 265, 277, 657, 318, 377, 362, 
     726, 245, 292, 561, 407, 358, 547, 311]
    
    This list can be determined by reading in all prep 01 inc/mor files into one dataframe, gathering the
    unique list of registry indexes, filtering on the SQL registry table for these unique
    registry indexes, filtering on coverage_of_location_id = 1, taking that final list of
    registry_indexes and filtering back on the dataframe with all the 01 inc/mor files to return the final
    set of dataset_ids.
    '''
    if dataset_id in range(10, 20):
        df_w_pop = pop.merge_with_population(
            post_agg_df, this_dataset, supplement_missing=True)
    
    # If dataset is NOT a VR dataset, check if we have registry population data available 
    else:
        # GBD2020: only merge pop if pop file has data values
        # removes cause-years that appeared after rdp
        restrict_df = restrict_redistributed(df, pre_rdp_data)

        # If registry population data is available (from the original dataset), then we merge the population data
        if len(this_dataset.load_pop(7)) > 0:
            df_w_pop = pop.merge_with_population(
                post_agg_df, this_dataset, supplement_missing=False)
        
        # If no registry population data is available then assign 'pop' column as np.nan
        else:
            df_w_pop = post_agg_df.copy()

            # Add empty column for pop
            '''
            The rationale here is that even if a dataset does not have registry population data that
            we still want to create the 07.dta output file as the data can still be used to influence MIRs,
            but may be dropped later down our pipeline (unclear on the details of that piece).

            Worth revisiting each GBD round and conversing with modelers / team lead to see if we want to
            retain this behavior.

            Older datasets (lower queue_ids / dataset_ids) may generally not have had population data
            included as a requirement when formatting, but we do have guidelines in place to only
            format datasets that include registry population data, so this is mostly an issue
            for older datasets.
            '''
            df_w_pop['pop'] = np.nan 


    # Merge NIDs to dataframe and ensure there are no missing NIDs
    df = add_nids(df_w_pop, dataset_id, data_type_id) 

    # If there are missing, saves a CSV with the dataset_id & data_type_id with the rows that are missing NID matches
    # To resolve these missing NIDs, one must reference the SQL nqry table and add rows with the correct NID that will need to be found from the GHDx 
    # based on searching for the original dataset's NID using keywords as descriptors. 
    # See https://hub.ihme.washington.edu/pages/viewpage.action?pageId=84610022#NQRY/nqry_staging/SourcesWorkflow-Cancerteamprocedure:
    # for information on the NQRY table and how it's generally updated, recommend consulting with the DSS / a DS / DA on the team for hunting down NIDs.
    if df['nid'].isna().any() or len(df.loc[df['nid'] == 0, ]) != 0:
        if dataset_id == 10: # Skip requirement if finalizing VR dataset
            pass
        else:
            fp_missing_nids = utils.get_path(key="finalization",
                                            process = "mi_dataset", 
                                            base_folder="workspace") + '/missing_nids_' + str(this_dataset.dataset_id) + "_" + str(this_dataset.data_type_id) + ".csv"
            df_nan_nids = df.loc[df['nid'].isna() ,]
            df_zero_nids = df.loc[df['nid'] == 0, ]
            df_missing_nids = pd.concat([df_nan_nids, df_zero_nids])
            print("There are missing NIDs for:")
            print("Dataset name: {}".format(this_dataset._get_data_folder().rsplit("/", 1)[1]))
            print("Dataset_id: {}".format(this_dataset.dataset_id))
            print("\n")
            print("CSV saved with rows of data missing NID matches here: {}".format(fp_missing_nids))
            df_missing_nids.to_csv(fp_missing_nids, index = False)
            raise Exception("Missing NIDs for this dataset, please track down NIDs from the missing_nids CSV file and update the SQL nqry table as necessary then re-run this step.")

    # Test final output
    if dataset_id not in range(10, 20):
        test_output(df, post_agg_df)
    else: 
        test_output(df, post_agg_df)

    # Write to 07.dta file after performing final checks
    md.complete_prep_step(df, input_data, this_dataset)
    print("\nData finalized.")


if __name__ == '__main__':
    dataset_id = int(argv[1])
    data_type_id = int(argv[2])
    main(dataset_id, data_type_id)


'''
Test parameters to play with:

# This dataset does NOT have population data and CANNOT use IHME population data 
dataset_id = 376 # JPN_Q390_I
data_type_id = 2 # Incidence
this_dataset.prep_type_id
md.complete_prep_step(df, input_data, this_dataset)
def complete_prep_step(cleaned_df, input_df, ds_instance, dropped_dict : dict = {}, is_pop=False):
cleaned_df = df
input_df = input_data
ds_instance = this_dataset

# This dataset DOES have population data
dataset_id = 616 # LTU_Q613_M_I
data_type_id = 3 # Mortality

# This dataset DOES have population data
dataset_id = 25 # aus_vic_1982_2007_inc
data_type_id = 2 # Incidence


dataset_id = 309 # RUS_Q326_M_I
data_type_id = 2 # Incidence (dataset also has data_type_id = 3, originally the dataset is set to data_type_id = 1 in the dataset table)


dataset_id = 376 # JPN_Q390_I
data_type_id = 2 # Incidence

dataset_id = 279 # KOR_Q273_I # This dataset seems to fail step 7
data_type_id = 2 # Incidence


dataset_id = 757 # ECU_Q751_P # This dataset seems to fail step 7
data_type_id = 2 # Incidence

dataset_id = 448 # ESP_Q456_M_I
data_type_id = 2 # Incidence
'''