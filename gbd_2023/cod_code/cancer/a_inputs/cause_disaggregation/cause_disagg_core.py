# -*- coding: utf-8 -*-
'''
Description: Set of functions needed to run cause_disaggregation
How to Use: Integrate with run_cause_disaggregation module
Contents:
Constributors: INDIVIDUAL_NAME, INDIVIDUAL_NAME, INDIVIDUAL_NAME
'''
import pandas as pd
import numpy as np

# Import cancer_estimation utilities and modules
from cancer_estimation.py_utils import (
    common_utils as utils,
    data_format_tools as dft
)
from cancer_estimation.registry_pipeline import cause_mapping as cm
from cancer_estimation.py_utils.gbd_cancer_tools import add_year_id
from cancer_estimation.a_inputs.a_mi_registry.cause_disaggregation import (
    get_resources as resc,
    cause_disagg_tests as cd_test
)
from cancer_estimation.a_inputs.a_mi_registry import (
    populations as mp,
    mi_dataset as md,
    pipeline_tests as pt,
    prep_proportions as pp
)


def disaggregate_acause(df, ds_instance):
    ''' Description: Returns a dataframe in which metric values have been
            distributed across all associated acauses
        How it Works: Utilizes the create_metric_weights function to reshape
            the df so that acause is in long form. Adds proportions to
            each acause by observation, then applies those proportions to
            split the input metric value across the attributed acauses.
            Finally, collapses to re-combine data to single datapoints by
            gbd_cause and acause.
REDACTED

    '''
    # Ensure that 'age' is not listed as a uid
    metric = ds_instance.metric
    uids_noAcause = [c for c in md.get_uid_cols(5) if 'acause' not in c]
    acause_cols = [a for a in df.columns if 'acause' in a]
    all_uids = md.get_uid_cols(5)
    needs_split = (df['acause2'].notnull()
                   & ~df['acause2'].isin([""]))
    to_split = df.loc[needs_split, :]
    no_split = df.loc[~needs_split, :]
    # If no split needed, simply return the dataframe with a renamed acause1
    if len(to_split) == 0:
        df.rename(columns={'acause1': 'acause'}, inplace=True)
        acause_cols.remove('acause1')
        df.drop(labels=acause_cols, axis=1, inplace=True)
        return(df)
    print("disaggregating acause...")
    # create weights used for splitting
    weight_df = pp.create_metric_weights(df, all_uids, ds_instance)
    
    # adjust weights for garbage codes
    weight_df.loc[weight_df['gc_count'] == 0, 'gc_count'] = 1
    weight_df['wgt'] = weight_df['wgt']/weight_df['gc_count']
    del weight_df['gc_count']

    # calculate proportions based on the weights
    proportions_df = pp.add_proportions(weight_df, uids_noAcause)
    # adjust by proportions
    is_split = to_split.merge(proportions_df)
    is_split['split_value'] = is_split[metric]
    is_split.loc[:, metric] = is_split['proportion'] * is_split['split_value']
    is_split = md.stdz_col_formats(is_split)
    #
    no_split.rename(columns={'acause1': 'acause'}, inplace=True)
    acause_cols.remove('acause1')
    no_split.drop(labels=acause_cols, axis=1, inplace=True)
    #
    output = no_split.append(is_split)
    pt.verify_metric_total(df, output, metric, "disaggregate acause")
    return(output.loc[:, no_split.columns.tolist()])


def redist_kaposi(df, metric, uid_cols):
    ''' Adjusts Kaposi Sarcoma data to account for HIV-attributed cases.
        ------
        Inputs:
            df : a mortality-incidence input dataset at stage 5
            metric : one of ['pop', 'cases', 'deaths']
            uid_cols : list indicating column-set that uniquely identifies observations
    '''
    # subset data to split. exit if no split necessary
    kaposi_prop = pp.get_kaposi_proportions()
    # fix for datasets with mismatch cause vs acause names
    if 'cause' in kaposi_prop.columns and 'acause' not in kaposi_prop.columns:
        kaposi_prop.rename(columns={'cause': 'acause'},  inplace=True)
    del kaposi_prop['coding_system']
    to_split = df.loc[df['acause'].isin(kaposi_prop['acause'].unique()), :]
    no_split = df.loc[~df['acause'].isin(kaposi_prop['acause'].unique()), :]
    if len(to_split) == 0:
        return(df)
    print("disaggregating kaposi sarcoma data...")
    # remove duplicated values
    kaposi_prop = kaposi_prop.drop_duplicates(subset = ['sex_id', 'acause', 'age', 'target'])
    # merge with weights
    to_split = to_split.merge(kaposi_prop,
                              on=['sex_id', 'acause', 'age'],
                              how='left',
                              indicator=True)
    assert not to_split['_merge'].isin(["left_only"]).any(), \
        "Error: Not all Kaposi data could be merged with proportions"
    # Mark those those data that are both kaposi and have the correct year range.
    to_split = add_year_id(to_split)
    within_range = ((to_split['year'] >= to_split['year_start']) &
                    (to_split['year'] <= to_split['year_end']))
    to_split.loc[within_range, 'match'] = 1
    split_groups = to_split.groupby(uid_cols, as_index=False)['match'].max()
    # Split only marked data
    is_split = to_split.merge(
        split_groups[split_groups['match'].isin([1])], how='inner')
    is_split['split_value'] = is_split[metric]
    is_split.loc[:, metric] = is_split['proportion'] * is_split['split_value']
    is_split.loc[:, 'acause'] = is_split['target']
    # Format kaposi data that did not meet any year range criteria
    cant_split = to_split.merge(
        split_groups[split_groups['match'].isin([0])], how='inner')
    cant_split = cant_split.loc[:, no_split.columns.tolist()].drop_duplicates()
    output = pd.concat([no_split, is_split, cant_split])
    pt.verify_metric_total(df, output, metric, 'kaposi_redist')
    return(output.loc[:, no_split.columns.tolist()])


def redist_nmsc_gc(df, metric):
    ''' Splits non-melanoma skin cancer data proportionately into subcauses
        ------
        Inputs:
            df : a mortality-incidence input dataset at stage 5
            metric : one of ['pop', 'cases', 'deaths']
    '''
    # subset data to split. exit if no split necessary
    nmsc_props = pp.get_nmsc_proportions()
    nmsc_props.rename(columns={'cause': 'acause', 'sex':'sex_id'},  inplace=True)
    del nmsc_props['coding_system']
    to_split = df.loc[df['acause'].isin(nmsc_props['acause']), :]
    no_split = df.loc[~df['acause'].isin(nmsc_props['acause']), :]
    if len(to_split) == 0:
        return(df)
    print("disaggregating nmsc data...")
    # merge with proportions to split causes
    is_split = to_split.merge(nmsc_props,
                              on=['sex_id', 'acause', 'age'],
                              how='left',
                              indicator=True)
    assert not is_split['_merge'].isin(["left_only"]).all(), \
        "Error during merge with NMSC proportions"
    # apply proportions
    is_split.loc[:, 'acause'] = is_split['mapped_cause']
    is_split['split_value'] = is_split[metric]
    is_split.loc[:, metric] = is_split['proportion'] * is_split['split_value']
    output = no_split.append(is_split)
    pt.verify_metric_total(df, output, metric, 'NMSC')
    return(output.loc[:, no_split.columns.tolist()])


def map_remaining_garbage(df, data_type_id):
    ''' map disaggregated causes to gbd_causes, preserving any ICD codes that are
           associated with garbage codes

    '''
    # Set the output to drop the gbd_cause column. Final cause information will be stored in acause
    output_cols = [c for c in df.columns.tolist() if c != 'gbd_cause']
    # update coding_system to account for disaggregated garbage
REDACTED
    df.loc[
        df['coding_system'].str[:1].isin(["C", "D"]), 'coding_system'] = "ICD10"
REDACTED

    cause_map = cm.load_rdp_cause_map(data_type_id)
    cause_map.rename(columns={'acause': 'acause_update',
                              'cause': 'acause'}, inplace=True)
    df = df.merge(cause_map,
                  on=['acause', 'coding_system'],
                  how='left')
    # preserve any ICD codes that are associated with garbage codes
    df.loc[df['acause_update'].notnull(), 'acause'] = df['acause_update']
    # Update transition causes (causes that are in flux or are often inaccurately entered into
    #   custom maps)
    df = remap_coding(df, data_type_id)
    assert not df.loc[df['acause'].isnull() | df['acause'].isin(['']), :].any().any(), \
        "Error mapping remaining garbage"
    return(df.loc[:, output_cols])


def remap_coding(df, data_type_id):
    ''' Updates coding_system where ICD9_detail data are present within the uid, then
        duly replaces ICD10 garbage mapping with ICD9_detail codes where necessary
REDACTED
    '''
    def update_coding_system(group):
        if group.coding_system[group.coding_system == 'ICD9_detail'].any():
            group.coding_system = 'ICD9_detail'
        else:
            group.coding_system = 'ICD10'
        return(group)

    if not df['coding_system'].isin(["ICD9_detail"]).any():
        return(df)
    # Load garbage remap file
    garb_rm = cm.load_garbage_remap(data_type_id)
    garb_rm.rename(columns={'ICD10': 'acause', 'ICD9_detail': 'new_acause'},
                   inplace=True)

    df = df.groupby(by=['registry_index', 'sex_id', 'year_start', 'year_end'])\
        .apply(update_coding_system)
    df = df.merge(garb_rm, on=['acause'], how='left')
    needs_recode = (~df['new_acause'].isnull() &
                    df['coding_system'].isin(["ICD9_detail"]))
    df.loc[needs_recode, 'acause'] = df['new_acause']
    del df['new_acause']
    return(df)
