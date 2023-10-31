'''
Description: contains logic to build custom weights used in age_sex_splitting
    and cause_disaggregation
'''
import pandas as pd
import numpy as np
import re
from cancer_estimation.py_utils import (
    common_utils as utils,
    data_format_tools as dft,
    gbd_cancer_tools as gct
)
from cancer_estimation._database import cdb_utils
from cancer_estimation.a_inputs.a_mi_registry import (
    mi_dataset as md,
    populations as pop,
    pipeline_tests as pt
)
from cancer_estimation.registry_pipeline import cause_mapping as cm
try:
    # Python3
    from functools import lru_cache
except ImportError:
    # fallback to Python2 if running on gbd environment
    from functools32 import lru_cache


def load_weight_causes(data_type_id):
    ''' Returns an ICD map which enables garbage-mapped data to merge with cause rates
    '''
    cause_map = cm.load_cause_map(data_type_id)
    cause_map = cause_map.loc[cause_map.coding_system.str.startswith('ICD'), [
        'cause', 'gbd_cause']]
    cause_map.rename(columns={ "cause": "acause",
                                "gbd_cause": "mapped_cause"}, inplace=True)
    cause_map.loc[:, 'acause'] = cause_map['acause'].str.replace("_benign", "")
    cause_map.loc[:, 'acause'] = cause_map.loc[cause_map['acause'] != "average_cancer",
                                               'acause'].str.replace("_cancer", "")
    return(cause_map)

@lru_cache()
def load_cause_rates(data_type_id):
    ''' imports "gold standard" (data rich) estimates for global cancer rates
    '''
    # cause/sex restrictions
    db_link = cdb_utils.db_api("cancer_db")
    gbd_id = utils.get_gbd_parameter('current_gbd_round')
    d_step = utils.get_gbd_parameter('current_decomp_id')
    refresh_id = utils.get_gbd_parameter('current_refresh_id')
    if data_type_id == 2:
        cause_map = db_link.get_table("prep_incidence_rates")
        cause_map = cause_map[cause_map['is_best'].eq(1) & 
                                cause_map['gbd_round_id'].eq(gbd_id) &
                                cause_map['decomp_step'].eq(d_step) &
                                cause_map['refresh'].eq(refresh_id)]
    elif data_type_id == 3:
        cause_map = db_link.get_table("prep_mortality_rates")
        cause_map = cause_map[cause_map['is_best'].eq(1) & 
                                cause_map['gbd_round_id'].eq(gbd_id)&
                                cause_map['decomp_step'].eq(d_step) &
                                cause_map['refresh'].eq(refresh_id)]

    cause_map = md.stdz_col_formats(cause_map)
    return(cause_map)


def gen_pop_wgts(wgt_type, location_ids):
    ''' Loads a formatted dataframe of IHME population estimates to be used
        as weights when splitting population
    '''
    if wgt_type not in ["age_wgt", "sex_wgt"]:
        raise AssertionError(
            "Error: wrong weight type sent to gen_wgts function")
    wgts = pop.load_formatted_ihme_pop(location_ids)
    wgts = gct.cancer_age_from_age_group_id(wgts)
    wgts = wgts[wgts['age'].ne(1)]
    if wgt_type == "age_wgt":
        wgts.rename(columns={'pop': 'wgt'}, inplace=True)
        output_cols = ['location_id', 'year', 'sex_id', 'age', 'wgt']
    elif wgt_type == "sex_wgt":
        wgts.rename(columns={'pop': 'wgt', 'sex_id': 'new_sex_id'}, inplace=True)
        output_cols = ['location_id', 'year', 'new_sex_id', 'age', 'wgt']
        wgts = wgts.loc[wgts['new_sex_id'].isin([1, 2]), :]
    wgts = md.stdz_col_formats(wgts)
    pt.test_for_missingness(wgts, col_to_check = "wgt")
    return(wgts[output_cols])


def create_metric_weights(df, uid_cols, ds_instance):
    ''' Returns a dataframe of expected values ("weights") used for splitting 
        data in the cancer pipeline.

        Params:
            df : DataFrame
                    input data to create weights off of
            uid_cols : list
                        names of unique columns to later merge the cause rates on
            ds_instance : MI_Dataset obj
        
        Returns: DataFrame, input data with weights
    '''
    rate_df = add_cause_rates(df, uid_cols, ds_instance)
    # merge with population data
    if len(ds_instance.load_pop()) == 0:
        use_ihme_pop = True
    else:
        use_ihme_pop = False
    weight_df = pop.merge_with_population(rate_df, ds_instance, use_ihme_pop)
    # make weights (weights = expected number of events = rate*pop)
    weight_df.loc[:, 'wgt'] = weight_df['rate'] * weight_df['pop']

    if ds_instance.prep_type_id == 5:
        weight_df = md.stdz_col_formats(
            weight_df, additional_float_stubs=['wgt', 'rate', 'gc_count'])
    else:
        weight_df = md.stdz_col_formats(
            weight_df, additional_float_stubs=['wgt', 'rate'])
    return(weight_df)


def add_cause_rates(dataset, uid_cols, ds_instance):
    ''' Returns expected values used for splitting data in the cancer pipeline.
        Uses two merges
        Params:
            dataset : DataFrame
                    pandas dataframe
    '''
    print("      loading cause rates...")
    prep_step = ds_instance.prep_type_id
    has_unknonwn_sex = dataset['sex_id'].isin([9]).any()
    # make acause long and remove blank acause entries
    merge_df = reshape_acause_long(
        df=dataset.copy(), unique_identifiers=uid_cols)    
    merge_df = merge_df.loc[~merge_df['acause'].eq('nan'),] #need to fix mapping entries 
    # save original acause for later reversion
    merge_df.loc[:, 'orig_acause'] = merge_df['acause']
    # convert sex=9 data to sex=3 to enable merge with weights
    if has_unknonwn_sex:
        merge_df.loc[:, 'orig_sex'] = merge_df['sex_id']
        merge_df.loc[merge_df['sex_id'] == 9, 'sex_id'] = 3

    # load maps
    cause_map = load_weight_causes(ds_instance.data_type_id)
    acause_rates = load_cause_rates(ds_instance.data_type_id)
    # adjust cause to enable merge with the most appropriate rates

    cause_df = merge_df.merge(cause_map, on='acause',
                                how='left', indicator=True)
    mapped_to_gbd_cause = (cause_df['_merge'].isin(['both']))
    cause_df.loc[mapped_to_gbd_cause, 'acause'] = cause_df['mapped_cause']
    cause_df.drop(['_merge', 'mapped_cause'], axis=1, inplace=True)
    # merge with rates, if rate is not available, use "average_cancer" rate
    cause_df = md.stdz_col_formats(cause_df)
    acause_rates = md.stdz_col_formats(acause_rates)

    # for age sex splitting, we want to use average cancer rate for
    # grouped causes with multiple acauses
    if prep_step == 4:
        uids_noAcause = [col for col in uid_cols if not('acause' in col)]
        #cause_map = cause_map.append(gbd_acauses)
        acause_rates['acause_count'] = 1
        cause_df['acause_count'] = cause_df.groupby(uids_noAcause)['acause_num'].transform('nunique')
        rate_df = cause_df.merge(acause_rates, on=['age','sex_id','acause', 'acause_count'],
                              how='left', indicator=True)
        del acause_rates['acause_count']
        del rate_df['acause_count']
    else:
        rate_df = cause_df.merge(acause_rates, on=['age','sex_id','acause'], 
                                    how='left', indicator=True)
    
    # save indication of count of garbage codes in dataset
    if prep_step == 5:
        uids_noAcause = [col for col in uid_cols if not('acause' in col)]
        rate_df['gc_count'] = rate_df.groupby(uids_noAcause)['acause'].transform(
                                        lambda x: (~x.str.contains("neo_")).sum()
                                        )
        rate_df.loc[rate_df['acause'].str.contains("neo_"), 'gc_count'] = 0

    not_mapped_to_rate = (rate_df['_merge'].isin(['left_only']))

    if prep_step == 5:
        # mapping leukemia garbage codes to specific leukemia cause by age
        all_gc = ['C91.9', 'C91.7', 'C91.8', 'C91.5', 'C91.4']
        aml_gc = ['C92.7', 'C93.9', 'C92.9', 'C93.5', 'C92.8', 'C93.2', 'C93.7']
        cll_gc = ['C91.1']
        rate_df.loc[(rate_df['orig_acause'].isin(aml_gc)) & 
                (not_mapped_to_rate) & 
                (rate_df['age'] > 9), 'acause'] = "neo_leukemia_other"

        rate_df.loc[(rate_df['orig_acause'].isin(aml_gc)) & 
                (not_mapped_to_rate) & 
                (rate_df['age'] <= 9), 'acause'] = "neo_leukemia_ml_acute"

        rate_df.loc[(rate_df['orig_acause'].isin(all_gc)) & 
                (not_mapped_to_rate) & 
                (rate_df['age'] > 9), 'acause'] = "neo_leukemia_other"

        rate_df.loc[(rate_df['orig_acause'].isin(all_gc)) & 
                (not_mapped_to_rate) & 
                (rate_df['age'] <= 9), 'acause'] = "neo_leukemia_ll_acute"

        rate_df.loc[(rate_df['orig_acause'].isin(cll_gc)) & 
                (not_mapped_to_rate) & 
                (rate_df['age'] > 9), 'acause'] = "neo_leukemia_ll_chronic"

        rate_df.loc[(rate_df['orig_acause'].isin(cll_gc)) & 
                (not_mapped_to_rate) & 
                (rate_df['age'] <= 9), 'acause'] = "neo_leukemia_ll_acute"

        rate_df.loc[(not_mapped_to_rate) & 
                    ~(rate_df['orig_acause'].isin(aml_gc + all_gc + cll_gc)), 'acause'] = "average_cancer"
    else:
        rate_df.loc[(not_mapped_to_rate), 'acause'] = "average_cancer"
    rate_df.drop(['_merge', 'rate', 'sex_id'], axis=1, inplace=True)
    rate_df = rate_df.merge(
        acause_rates, on=['acause','age'], how='left')

    # revert sex if unknown sex was present
    if has_unknonwn_sex:
        rate_df.loc[:, 'sex_id'] = rate_df['orig_sex']
        del rate_df['orig_sex']
    # set rates to 0 for non-prepped age
    rate_df.loc[~rate_df['age'].isin(acause_rates['age'].unique()), 'rate'] = 0
    # temp remove unused age_groups 
    rate_df = rate_df.loc[~rate_df['age'].isin([3,4,5,6,26,91,92,93,94])]

    # reset acause to the input acause
    rate_df.loc[:, 'acause'] = rate_df['orig_acause']
    # validate that values exist for all rate entries
    assert not rate_df['rate'].isnull().any(), \
        "Error in merge. Some observations do not have rates"
    return(rate_df)


def reshape_acause_long(df, unique_identifiers):
    ''' Returns a dataframe that has been reshaped so that acause values are long. From
        this dataframe, missing acause values have been dropped
    '''
    acause_vals = [c for c in df.columns.values if 'acause' in c]
    unique_identifiers = [
        c for c in unique_identifiers if c not in acause_vals]
    df.reset_index(inplace=True, drop=True)
    df.loc[:, 'merge_ix'] = df.index
    reshape_df = df[acause_vals + ['merge_ix']].copy()
    long_df = dft.wide_to_long(reshape_df,
                               stubnames='acause',
                               i='merge_ix',
                               j='acause_num')
    # remove blank values generated in reshape
    has_value = (long_df['acause'].notnull() &
                 ~long_df['acause'].isin(["", "."]))
    long_df = long_df.loc[has_value, :].merge(
        df[unique_identifiers + ['merge_ix']])
    del long_df['merge_ix']
    return(long_df)


def create_sex_weights(sex_wgts, uid_vars, metric):
    ''' Adds weights for additional sexes if sex = 3 data exists

        Params:
        sex_wgts : DataFrame
                    pandas dataframe (weights used for split)
        uid_vars : list
                    cols to group data by to determine unique sex ids
        metric : str
                    name of column name that contains the 
                    pop/cases/deaths/metric counts data
        
        Returns: DataFrame, cause wgts with sex specific wgts added for split
    '''
    if len(sex_wgts) == 0:
        # return empty weights if none can be created.
        return(sex_wgts)

    # substitute sex 9 to create single weights for sex 3 & 9 together
    sex_wgts.loc[sex_wgts['sex_id'] == 9, 'sex_id'] = 3
    # reshape and collapse to create weights by sex
    sex_wgts.loc[:, 'sex_id'] = sex_wgts['sex_id'].astype(int)
    sex_wgts = dft.long_to_wide(sex_wgts, stub='wgt', i=uid_vars, j='sex_id')
    sex_wgts.fillna(value=0, inplace=True)
    # ensure presence of all weight columns
    for num in [1, 2]:
        wgt_col = "wgt{}".format(num)
        if not wgt_col in sex_wgts.columns:
            sex_wgts[wgt_col] = 0
    # reset wgt3 as total of wgt1 and wgt2
    sex_wgts['wgt3'] = sex_wgts['wgt1'] + sex_wgts['wgt2']
    # recalculate weights for sex = 1 and sex = 2 as their proportions of the
    #   total number of deaths (weight for sex = 3)
    sex_wgts['wgt1'] = sex_wgts['wgt1'].divide(sex_wgts['wgt3'])
    sex_wgts['wgt2'] = sex_wgts['wgt2'].divide(sex_wgts['wgt3'])
    noWgt = (sex_wgts.wgt3 == 0)
    sex_wgts.loc[noWgt, 'wgt1'] = .5
    sex_wgts.loc[noWgt, 'wgt2'] = .5
    # drop weights for sex = 3. save the data so they can be joined with
    # sex = 3 data and split it into two sexes
    del sex_wgts['wgt3']
    sex_wgts = dft.wide_to_long(sex_wgts, stubnames='wgt', i=uid_vars, j='sex_id')
    sex_wgts = sex_wgts.rename(index=str, columns={'sex_id': 'new_sex_id'})
    sex_wgts['sex_id'] = 3
    sex_wgts = md.stdz_col_formats(sex_wgts,
                                   additional_float_stubs=['wgt', 'new_sex_id'])
    pt.test_for_missingness(sex_wgts, col_to_check = "wgt")
    return(sex_wgts)


def subset_to_sex_split_data(dataset, uid_cols):
    ''' Marks the dataset with to_split  i.e.:
            -marks as splittable if group has only sex 3, or sex 3 and sex 1 or 2
            -marks data to be dropped, sex 3 when all sexes are present or 
                sex 1 or sex 2 when one and sex 3 are present
        to remove redundancy in datasets with all sex data

        Params: dataset : DataFrame, 
                            input data to be sex split
                uid_cols : list, 
                            cols to group data by to determine unique sex ids

        Returns: input dataset with marked columns for no sex split/split
    '''
    uids_SG = [c for c in uid_cols if not any(
        drop in c for drop in ['sex_id', 'acause', 'obs'])]
    # determine which data need to be split
    sg = dataset.groupby(uids_SG, as_index=False)[
                'sex_id'].nunique().reset_index(drop=True)

    sg = dataset.groupby(by=uids_SG, as_index=False).agg({'sex_id': pd.Series.nunique})
    sg.rename(columns={'sex_id': 'sex_id_count'}, inplace=True)
    df = dataset.merge(sg)

    # subset the data to be split and skip UIDS where male or female data exist.
    # decide to split if unique sex_ids in each group is < 3 and sex_id = 3
    #   also drop 'unknown' data if any other sex specification is present
    can_split_sex = ((df['sex_id_count'] < 3) &
                      (df['sex_id'] == 3))
    # decide to keep individual sex data when unique sex_id in each group = 3 or 1
    keep_no_split = (((df['sex_id_count'] == 1) & (df['sex_id'].isin([1,2])))|
                    ((df['sex_id_count'] == 3) & (df['sex_id'].isin([1,2]))))
    # keep indicator column for if this data has all sexes and one sex
    df['to_split'] = np.where(can_split_sex, 1, 0)
    df['no_split'] = np.where(keep_no_split, 1, 0)
    subset_df = df.loc[:, dataset.columns.tolist() + ['to_split', 'no_split']]
    return(subset_df)


def add_proportions(df, uid_cols):
    ''' Creates a column of scaled proportions across which data should be 
        distributed. 
        proportion = weight/weight_total 
        (where weight total is sum of all weights in that unique group)
        
        When weights are 0, gives a default proportion out the total number of 
        rows in each unique group that needs splitting

        Params: df : DataFrame, 
                        input data with weights
                uid_cols : list, 
                        columns on which to perform merges on with data
        
        Returns: DataFrame, input data with computed proportions
    '''
    wgt_tot = df.groupby(uid_cols, as_index=False)[
        'wgt'].sum().rename(columns={'wgt': 'wgt_tot'})
    df = df.merge(wgt_tot, on=uid_cols)
    df.loc[df.wgt_tot == 0, 'wgt'] = 1 # okay to set wgt to one
    del df['wgt_tot']

    wgt_tot = df.groupby(uid_cols, as_index=False)[
        'wgt'].sum().rename(columns={'wgt': 'wgt_tot'})
    df = df.merge(wgt_tot, on=uid_cols)
    df.loc[:, 'wgt_tot'].fillna(value=0, inplace=True)
    df.loc[:, 'proportion'] = df['wgt'] / df['wgt_tot']
    df.loc[df['proportion'].isin([np.inf, np.NaN]), 'proportion'] = 0.0
    return(df)


def get_kaposi_proportions():
    ''' Retrieves from the database proportions for disaggregating kaposi causes
    '''
    db_link = cdb_utils.db_api("cancer_db")
    kprop = db_link.get_table("prep_kaposi_proportions")
    kprop.rename(columns={'acause': 'mapped_cause',
                          'cause': 'acause'}, inplace=True)
    kprop.loc[:, 'proportion'] = kprop['proportion'].astype(float)
    return(kprop)


def get_nmsc_proportions():
    ''' Retrieves from the database proportions for splitting nmsc data into
        BCC and SCC
    '''
    db_link = cdb_utils.db_api("cancer_db")
    scc_bcc = db_link.get_table("prep_nmsc_proportions")
    scc_bcc.rename(columns={'acause': 'mapped_cause',
                            'cause': 'acause'}, inplace=True)
    scc_bcc.loc[:, 'proportion'] = scc_bcc['proportion'].astype(float)
    return(scc_bcc)
