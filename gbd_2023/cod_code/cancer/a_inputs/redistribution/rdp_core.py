'''
Description: Applies redistribution packages to the input data to redistribute garbage
How To Use:
Contributors: USERNAME
'''
### Load common cancer paths
import os, sys
import pandas as pd
import numpy as np
import json
import time
import cancer_estimation.py_utils.common_utils as utils
from cancer_estimation.a_inputs.a_mi_registry import (
    mi_dataset as md,
    pipeline_tests as pt
)
from cancer_estimation.py_utils.gbd_cancer_tools import add_year_id
from cancer_estimation.a_inputs.a_mi_registry.redistribution import rdp_tests

''' Prepare cause map '''

def evaluate_cause_restrictions(cause_map, proportion_metadata, proportion_ids):
    ''' Returns cause restricted proportion data

        -- Args:
            cause_map : DataFrame, map of age restrictions on causes
            proportion_metadata : DataFrame, break down of 
            proportion_ids : list, 

        Returns: DataFrame,

    '''
    print("Evaluating cause map restrictions...")

    for t in proportion_ids:
        if t != 'region':
            cause_map['restrictions'] = cause_map['restrictions'].map(lambda x: x.replace(t, 'full_cause_map.loc[i,"'+t+'"]'))
    
    # REDACTED
    cause_map.loc[cause_map['restrictions'].eq('(full_cause_map.loc[i,"age"] >= 1.0)'), 'restrictions'] = '(full_cause_map.loc[i,"age"] >= 0.0)'
    cause_map.loc[cause_map['restrictions'].eq('(full_cause_map.loc[i,"age"] >= 2.0)'), 'restrictions'] = '(full_cause_map.loc[i,"age"] >= 0.0)'

    dict_cause_map = cause_map.set_index('cause').to_dict()
    full_cause_map = []
    for pid in proportion_metadata['proportion_id']:
        temp = cause_map.loc[:,['cause','restrictions']].copy(deep=True)
        temp['proportion_id'] = pid
        full_cause_map.append(temp)
    full_cause_map = pd.concat(full_cause_map).reset_index(drop=True)

    # REDACTED
    full_cause_map.loc[full_cause_map['restrictions'].eq('(full_cause_map.loc[i,"age"] >= 1.0)'), 'restrictions'] = '(full_cause_map.loc[i,"age"] >= 0.0)'
    full_cause_map.loc[full_cause_map['restrictions'].eq('(full_cause_map.loc[i,"age"] >= 2.0)'), 'restrictions'] = '(full_cause_map.loc[i,"age"] >= 0.0)'

    full_cause_map = pd.merge(
        full_cause_map, proportion_metadata, on='proportion_id').reset_index(drop=True)
    full_cause_map['id'] = full_cause_map.index
    maps = {'id': [], 'eval': []}
    for i in full_cause_map.index:
        maps['id'].append(i)
        maps['eval'].append(
            eval(dict_cause_map['restrictions'][full_cause_map.loc[i, 'cause']]))
    return(pd.merge(full_cause_map, pd.DataFrame(maps), on='id').loc[:, ['proportion_id', 'cause', 'eval']].reset_index(drop=True))


#######################
# Prepare packages  ###
#######################

def read_json(file_path):
    ''' load json file
    '''
    json_data = open(file_path)
    data = json.load(json_data)
    json_data.close()
    return(data)


def get_package_exceptions(coding_system):
    ''' Gets a dict of specific package exceptions for handling rdp pkgs

        create_target_pkgs: List of packages that we want to create targets for 
                            even if that cause-sex-registry-year-age didn't exist 
                            in our dataset originally
        cond_target_pkgs: List of packages where we will only redistribute the 
                          cases/deaths from garbage code into causes that existed 
                          previously in our dataset
        create_zero_pkgs: List of packages where we will propagate zeros through 
                          from garbage code to redistributed causes
        Args: 
            coding_system - str, 
                    coding system of input datasets

        Returns: dict, 
                    package exceptions
    '''
    if coding_system == "ICD10":
REDACTED
                                            'P-186', 'P-187', 'P-188', 'P-189', 
                                            'P-197', 'P-198', 'REG-20', 'REG-22', 'REG-23', 
                                            'REG-24', 'REG-25', 'REG-26', 'REG-27'],
                        'cond_target_pkgs':['ALL-GC', 'P-178', 'REG-21', 'REG-28', 
                                            'REG-29', 'REG-30', 'REG-31', 'PREV-168'],
                        'create_zero_pkgs':['P-198', 'P-185', 'P-186', 'P-187', 'P-179']}
    elif coding_system == "ICD9_detail":
        package_except = {'create_target_pkgs':['HIV-CORR-kaposi_neo_other', 'REG-10', 'REG-12', 
                                                'REG-13', 'REG-14', 'REG-15', 'REG-16', 'REG-18', 
                                                'P-165', 'P-167', 'P-169', 'P-170', 'P-171', 
                                                'P-182', 'P-183'],
                        'cond_target_pkgs':['P-1', 'REG-11', 'REG-17', 'REG-19', 'REG-20', 'REG-21', 
                                            'REG-22', 'MCOD-205', 'MCOD-213', 'MCOD-219', 'P-142', 
REDACTED
                        'create_zero_pkgs':['P-165', 'P-169', 'P-170', 'P-171','P-183']}
    return(package_except)


def extract_package(package):
    ''' Extract useful package data and formats it in a useable way
    '''
    fmt_package = {}
    fmt_package['package_name'] = package['package_name']
    if 'create_targets' in package:
        fmt_package['create_targets'] = package['create_targets']
    else:
        package['create_targets'] = 0

    fmt_package['garbage_codes'] = package['garbage_codes']
    fmt_package['target_groups'] = {}
    c = 0

    # retrieves target and weights groups in long format
    for tg in package['target_groups']:
        target_codes = []
        except_codes = []
        for tc in tg['target_codes']:
            target_codes.append(tc)
        if 'except_codes' in tg:
            for ec in tg['except_codes']:
                except_codes.append(ec)
        fmt_package['target_groups'][str(c)] = {'target_codes':list((set(target_codes)-set(except_codes))-set(fmt_package['garbage_codes'])),'weights':tg['weights']}
        c += 1
    fmt_package['weight_groups'] = {}
    c = 0
    for wg in package['weight_groups']:
        fmt_package['weight_groups'][str(c)] = wg['weight_group_parameter']
        c += 1

    return(fmt_package)


def get_packages(this_dataset, input_data, package_folder, cause_map):
    ''' Imports packages and re-formats them in table format

        -- Args:
            this_dataset : MI_Dataset object, unique to input dataset
            input_data : DataFrame, the input subset
            package_folder : str, folder path containing rdp packages
            cause_map : DataFrame, map of age restrictions on causes

        Returns: DataFrame, formatted table version of rdp packages
                            unique to split subset
    '''
    print("Importing packages...")
    input_codes = pd.Series(input_data['cause'])
    package_list = read_json(package_folder + '/_package_list.json')
    packages = []

    # import in packages that contains the garbage codes in the input data
    package_info = []
    for rdp in package_list:
        package = extract_package(read_json(package_folder + '/' + rdp + '.json'))
        garbage_present = input_codes.isin(package['garbage_codes']).tolist()
        if not any(garbage_present):
            continue
        package_with_creation = create_target_df(this_dataset, package)
        package_with_creation['package_name'] = package['package_name'] 
        package_with_creation['package_num'] = rdp
        package_with_creation['create_targets'] = package['create_targets']
        package_with_creation['input_codes'] = [package['garbage_codes'] for i in package_with_creation.index]
        package_info.append(package_with_creation)
        print("    - {} {}".format(rdp, package['package_name']))
        packages.append(package)
    try:
        pkgs_total = pd.concat(package_info)
        test_map = pd.read_csv("FILEPATH/rdp_cause_map_{}.csv".format(this_dataset.data_type_id))
        pkgs_total = pkgs_total.merge(test_map[['cause', 'acause', 'coding_system']], how = "left", on = ["cause"])
        pkgs_total.to_csv("{}/redistribution/rdp_package_info_ICD10_{}.csv".format(
                                    utils.get_path("mi_dataset_resources", 
                                    process = "mi_dataset"), this_dataset.data_type_id))
    except:
        pass
    return(packages)



#######################
# Prepare data      ###
#######################

def pull_metadata(data, uid_cols):
    ''' Create metadata groups based on signature variables
    '''
    data = data[uid_cols].drop_duplicates().reset_index(drop=True)
    data['unique_group_id'] = data.index
    return(data)


def prep_data(data, proportion_ids, residual_cause='cc_code'):
    ''' Formats dataset for use
    '''
    output_cols = ['proportion_id', 'cause', 'freq']
    data = add_year_id(data).rename(columns={'year':'year_id'})
    proportion_metadata = pull_metadata(data, proportion_ids).rename(
                                columns={'unique_group_id': 'proportion_id'})
    data = pd.merge(data, proportion_metadata, on=proportion_ids)
    prop_map = data.loc[:,['proportion_id']].drop_duplicates()
    prop_map.loc[:, 'cause'] = residual_cause
    prop_map.loc[:, 'freq'] = 0.0
    output_data = pd.concat([data, prop_map])
    output_data = output_data[output_cols].groupby(
                ['proportion_id', 'cause']).sum().reset_index()
    return(output_data[output_cols], proportion_metadata)


#######################
# Get Proportions   ###
#######################

def find_weight_groups(package, proportion_metadata):
    ''' Find weight groups in rdp packages and attach
        to proportion metadata 
    '''
    meta = proportion_metadata.copy(deep=True)
    weights = []
    for wg in package['weight_groups']:
        resources = package['weight_groups'][wg]
        meta['weight_group'] = str(wg)
        try:
            meta['eval'] = meta.eval(resources)
        except:
            meta['year_id']= meta['year_id'].astype(str)
            meta['eval'] = meta.eval(resources)
        weights.append(meta.loc[meta['eval']==True, ['proportion_id', 'weight_group']])
    return(pd.concat(weights))


def create_target_df(this_dataset, package):
    ''' Creates dataframe that contains all the target groups with the codes that 
        the package will redistribute into, by target group
        limiting to just one icd code by cause

        Args:
            this_dataset - MI_Dataset object
            package - json object, current rdp package

        Returns: DataFrame, 
                    formatted table with target groups laid out and associated
                    gbd causes in each target group
    '''
    targets = []
    cause_map = pd.read_csv("{}/rdp_cause_map_{}.csv".format(this_dataset.temp_folder,
                this_dataset.data_type_id), index_col=0)

    for tg in package['target_groups']:
        temp = pd.DataFrame(
            {'cause': package['target_groups'][tg]['target_codes']})
        temp = temp.merge(cause_map, how = "left", on = "cause")
        temp.loc[temp['acause'].isnull(), 'acause'] = ''
        temp = temp.loc[~temp.duplicated(subset = ['acause'])] # keep by unique acause
        
REDACTED
        # as we don't want to redistribute to both parent and subtypes
        if not set(['neo_liver', 'neo_leukemia']).isdisjoint(set(list(temp['acause'].unique()))):
            for keep_parent in ['neo_liver', 'neo_leukemia']:
                subtype_set = temp.loc[temp['acause'].str.contains(keep_parent),'acause'].unique()
                if keep_parent in temp['acause'].unique() and len(set(subtype_set)) > 1:
                    temp = temp.loc[(temp['acause'].eq(keep_parent))|
                                    (~temp['acause'].str.contains(keep_parent))]
        
        temp['target_group'] = tg
        targets.append(temp)
    targets = pd.concat(targets).reset_index(drop=True)
    return(targets)


def create_proportion_list(data, this_dataset, weight_groups, targets, package, 
                                                            reg_id, prop_metadata):
    '''Creates a proportion list based on two components:
        1) Only create targets if package is in create_target_pkgs list
        2) Only redistribute to targets for a package in cond_target_pkgs
            if that cause was present in our dataset before

        Args:
            data - DataFrame, current subset of data
            this_dataset - MI_Dataset object
            weight_groups - DataFrame, table of weight groups by proportion_id
            targets - DataFrame, table of target groups and associated codes
            package - json object, current package
            reg_id - not used
            prop_metadata - DataFrame, table of current subset specific proportion_id
                                        and uid columns

        Returns: DataFrame, 
                    formatted table with target groups laid out and associated
                    gbd causes in each target group
    '''
    proportion_list = []
    # taking into account specifc package exception
    pkg_exceptions = get_package_exceptions(prop_metadata['coding_system'].unique()[0])
    if package['package_name'] in pkg_exceptions['create_target_pkgs']:
        package['create_targets'] = 1 # override create targets for all
    elif package['package_name'] in pkg_exceptions['cond_target_pkgs']:
        package['create_targets'] = 0
    else:
        package['create_targets'] = 0
        #rdp_tests.save_rdp_errors(this_dataset, prop_metadata, 
        #                "Missing package {} in exception list".format(package['package_name']))

    # adds target code if not present in pre-rdp data's causes and param create_targets is 1
    for pid in weight_groups['proportion_id']:
        temp = targets.copy(deep=True)
        if package['create_targets'] == 1:
            temp['freq'] = 0.0001 # assigns nonzero freq for allowed create_target pkgs
        elif package['create_targets'] == 0: # create non zero freq to only codes present in dataset
            temp['freq'] = 0
            temp.loc[temp['acause'].isin(list(data['cause'].unique())), 'freq'] = 0.0001
        temp['proportion_id'] = pid
        proportion_list.append(temp)

    return(proportion_list)


def create_weight_df(package):
    ''' Create formatted DataFrame of weight information by weight group
        from the rdp package
    '''
    weights = []
    for tg in package['target_groups']:
        wg = 0
        for wgt in package['target_groups'][tg]['weights']:
            weights.append(
                {'target_group': tg, 'weight_group': str(wg), 'weight': wgt})
            wg += 1
    weights = pd.DataFrame(weights)
    return(weights)


def add_custom_rdp_proportions(this_dataset, prop_map, package):
    ''' Adds in custom rdp proportions for redistribution
        Incidence: 
            proportions are by age, sex, acause, super_region for incidence data
            using only gold standard datasets
        Mortality:
            proportions are by age, sex, acause fo mortality data using all mort dts
        Args:
            this_dataset - MI_Dataset object
            prop_map - DataFrame, table of target groups and associated codes

        Returns: DataFrame, 
                    proportion metadata map with rdp proportions merged in
    '''
    if this_dataset.metric == "cases":
        uid_cols = ['age', 'sex', 'super_region', 'acause']
        metric = "inc"
    elif this_dataset.metric == "deaths":
        uid_cols = ['age', 'sex', 'acause']
        metric = "mor"

    # reads in proportions
    new_rdp_prop = pd.read_csv("{}/redistribution/rdp_prop_{}.csv".format(
                                utils.get_path("mi_dataset_resources", 
                                process = "mi_dataset"), metric))
    
    if this_dataset.metric == "cases": # by super region
        new_rdp_prop.rename({'prop':'new_freq',
                            'super_region_name':'super_region',
                            'sex_id':'sex'}, 
                            axis = 1, inplace = True)
    elif this_dataset.metric == "deaths": # global
        new_rdp_prop.rename({'prop':'new_freq',
                            'sex_id':'sex'}, 
                            axis = 1, inplace = True)
        new_rdp_prop['age'] = new_rdp_prop['five_year']
    
    # standardizing column types for better merge
    for col in ['age', 'sex']:
        new_rdp_prop[col] = new_rdp_prop[col].astype(int)
        prop_map[col] = prop_map[col].astype(int)

    prop_map_w_new = prop_map.merge(new_rdp_prop[uid_cols + ['new_freq']],
                                        on = uid_cols,
                                        how = "left")
    
    assert (not prop_map_w_new['new_freq'].isnull().all()), "All proportions didn't merge"

    # only set proportions to merged in proportions if original freq was nonzero
    prop_map_w_new['freq'] = np.where((~prop_map_w_new['freq'].eq(0)) & 
                                (~prop_map_w_new['new_freq'].isnull()),
                                prop_map_w_new['new_freq'], prop_map_w_new['freq'])
    
    prop_missing = float(len(prop_map_w_new[prop_map_w_new['new_freq'].isnull()])/len(prop_map_w_new))
    if prop_missing > 0.50:
        pass
        #rdp_tests.save_rdp_errors(this_dataset, prop_map, 
        #                    "Too many missing rdp prop {} for package {}".format(prop_missing,
         #                                                           package['package_name']))
    del prop_map_w_new['new_freq']
    return(prop_map_w_new)
    

''' Get data proportions '''

def get_proportions(data, this_dataset, proportion_metadata, package, cause_map_evaluated, 
                    residual_cause='cc_code'):
    ''' 
    This function generates proportions based on the frequencies in the data using the following steps:
        1. Get list of targets for each target group
        2. Generate base proportions (if the package is supposed to create causes)
        3. Pull frequencies of each cause in the data
        4. Determine and pull weight group for proportion group
        5. Pull weights from weight group for each target group
        6. Calculate fraction to each cause
    '''
    weight_groups = find_weight_groups(package, proportion_metadata)
    if len(weight_groups) == 0:
        return([])
    targets = create_target_df(this_dataset, package)
    registry_id = proportion_metadata['registry_index'].unique()[0]
    proportion_list = create_proportion_list(data, this_dataset, weight_groups, targets, package, 
                                             registry_id, proportion_metadata)
    prop_df = pd.concat(proportion_list).sort_values(
                                        by=['proportion_id','target_group','cause']
                                        ).reset_index(drop=True)
    prop_map = prop_df.merge(proportion_metadata, on='proportion_id')

    prop_map = add_custom_rdp_proportions(this_dataset, prop_map, package)

    # merge on super region weights
    assert len(prop_map) >= len(prop_df), "ERROR. Lost proportions before eval merge"
    # Merge with cause restrictions
    prop_map = prop_map.merge(cause_map_evaluated, on=['proportion_id','cause'])
    assert len(prop_map) >= len(prop_df), "ERROR. Lost proportions after eval merge"
    # Zero out if the cause is restricted
    prop_map.loc[prop_map['eval']==False,'freq'] = 0
    # Calculate totals for each cause
    proportions = prop_map.loc[:, ['proportion_id', 'target_group', 'cause', 'freq']
                                ].groupby(['proportion_id','target_group','cause']
                                ).sum().reset_index()
    # Calculate totals for each target group & merge back on
    proportions = proportions.set_index(['proportion_id','target_group']
                            ).join(proportions.groupby(['proportion_id','target_group'])
                                   .sum().rename(columns={'freq':'total'}))
    proportions = pd.merge(proportions.reset_index(),weight_groups,on='proportion_id')
    # Merge with weights
    weights = create_weight_df(package)
    proportions = proportions.merge(weights, on=['target_group', 'weight_group'])
    # Calculate final proportions to apply
    for c in ['freq', 'weight', 'total']:
        proportions.loc[:, c] = proportions[c].astype('float64')
    proportions['proportion'] = (proportions.freq / proportions.total) * \
        proportions.weight
    # If the total proportion for a given proportion id
    #  is 0, move to the residual code
    print("      adding residual causes where needed...")
    prop_totals = proportions.loc[proportions.total == 0,
                            ['proportion_id', 'target_group', 'weight', 'total']
                            ].drop_duplicates(
                            ).set_index('total'
                            ).rename(columns={'weight': 'proportion'})
    prop_totals.at[0, 'cause'] = residual_cause
    prop_totals = prop_totals.reset_index().loc[:,
                                             ['proportion_id', 'proportion', 'cause']
                                            ].groupby( ['proportion_id', 'cause']
                                            ).sum().reset_index()
    sum_at_cause = proportions.loc[proportions.total != 0,
                                        ['proportion_id', 'cause', 'proportion']
                                ].groupby(
                                    ['proportion_id', 'cause']
                                ).sum().reset_index()
    proportions = pd.concat([prop_totals, sum_at_cause]).reset_index(drop=True)

    # Again make sure everything sums to 1
    proportions = proportions.set_index(['proportion_id']).join(
        proportions.groupby(
            ['proportion_id']
        ).sum().rename(columns={'proportion': 'total'}))
    proportions.loc[:, 'proportion'] = (proportions.proportion / proportions.total)
    proportions = proportions.reset_index()[
        ['proportion_id', 'cause', 'proportion']]
    assert data.proportion_id.isin(proportions.proportion_id.unique()).all(), \
        "ERROR: not all proportion_ids are present in the proportions dataframe"
    return(proportions)

''' Redistribute garbage '''

def redistribute_garbage(data, this_dataset, proportions, package, prop_metadata):
    ''' Redistribute cases/deaths from our garbage codes using rdp proportions
        into the respective codes
    '''
    diagnostics = []
    # Make sure the package contains only the codes in the dataset
    assert data.proportion_id.isin(proportions.proportion_id.unique()).all(), \
        "ERROR: not all proportion_ids are present in the proportions dataframe"
    # Tag garbage
    data.loc[:, 'garbage'] = 0
    data.loc[(data['cause'].isin(package['garbage_codes'])), 'garbage'] = 1
    diagnostics.append(data.loc[data['garbage']==1])
    # Get total number of garbage codes for each proportion_id
    temp = data.loc[data['garbage']==1,
                   ['proportion_id', 'freq']].groupby(
                       ['proportion_id']).sum().reset_index()
    temp = temp.rename(columns={'freq': 'garbage'})
    # Redistribute garbage onto targets
    these_proportions = proportions.loc[proportions['proportion_id'].isin(
                                                data['proportion_id'].unique()), :]
    additions = pd.merge(these_proportions, temp, on=[ 'proportion_id'], how='outer')
    for c in ['proportion', 'garbage']:
        additions.loc[:, c].fillna(0, inplace=True)
    additions.loc[:, 'freq'] = additions['proportion'] * additions['garbage']
    orig_gbg = len(additions[additions['garbage'].eq(0)])

    pkg_except = get_package_exceptions(prop_metadata['coding_system'].unique()[0])

    # preserve 0s for certain packages
    if package['package_name'] in pkg_except['create_zero_pkgs']:
        additions = additions.loc[(additions['freq'] > 0) | 
                                ((additions['freq'] == 0) & (additions['proportion'] > 0)),
                                ['proportion_id', 'cause', 'freq']]
    else:
        additions = additions.loc[(additions['freq'] > 0),
                                ['proportion_id', 'cause', 'freq']]
    # check for 0s generated
    if len(additions) > 0:
        zero_count = float(len(additions.loc[additions['freq'] == 0]))/float(len(additions))
        if zero_count > 0.50 and orig_gbg > 0:
            pass
    diagnostics.append(additions)
    # Zero out garbage codes
    data.loc[data['garbage'] == 1, 'freq'] = 0
    # Tack on redistributed data
    output_data = pd.concat([data, additions])
    output_data = output_data.loc[:, ['proportion_id', 'cause', 'freq']].reset_index(drop=True)
    # Create diagnostics
    diagnostics = pd.concat(diagnostics)
    diagnostics.loc[:, 'garbage'].fillna(0, inplace=True)
    # Collapse to proportion id
    diagnostics = diagnostics.groupby(['proportion_id', 'garbage', 'cause'])[
        'freq'].sum().reset_index()
    # return
    return(output_data, diagnostics)


''' Run everything '''

def test_package_application(input_df, current_df):
    '''
    '''
    current_df.loc[:,'freq'].fillna(value=0, inplace=True)
    diff = current_df['freq'].sum() - input_df['freq'].sum()
    if abs(diff) > 1:
        message = "Difference from input after package application is too large ({})".format(diff)
        raise AssertionError(message)
    return


def apply_packages(prepped_data, this_dataset, proportion_metadata, evaluated_cause_map,
                    packages, residual_cause='cc_code', diagnostic_output=False):
    ''' Parent function for applying rdp to each package
    '''
    print("Running redistribution...")
    diagnostics_all = []
    seq = 0
    data = prepped_data.copy()
    for package in packages:
        print("    - package {}".format(package['package_name']))
        print("      calculating proportions...")
        proportions = get_proportions(
            data, this_dataset, proportion_metadata, package, evaluated_cause_map, 
            residual_cause=residual_cause)
        if len(proportions) == 0:
            print("No proportions available from this package.")
            continue
        print("      redistributing data...")
        data, diagnostics = redistribute_garbage(data, this_dataset, proportions, package, proportion_metadata)
        if len(diagnostics) == 0:
            continue
        data = data.loc[(data.freq >= 0) | (data.cause == residual_cause)]
        data = data.groupby(['proportion_id', 'cause']).sum().reset_index()
        diagnostics.loc[:, 'seq'] = seq
        diagnostics.loc[:, 'package'] = package['package_name']
        seq += 1
        if diagnostic_output:
            diagnostics_all.append(diagnostics)
        test_package_application(prepped_data, data)
    if diagnostic_output:
        diagnostics = pd.concat(diagnostics_all).reset_index(drop=True)
    return(data.loc[data.freq >= 0], diagnostics)


def run(input_data, this_dataset, PACKAGE_MAP, TEMP_FOLDER):
    '''
    '''
    if int(input_data['freq'].sum()) == 0:
        print("Data sums to zero.")
        return(input_data)
    else:
        pass
    output_cols =  ['registry_index', 'year_start','year_end', 'year_id', 'sex', 'coding_system',
                                'split_group', 'age', 'cause', 'freq']
    proportion_uids = ['dev_status', 'super_region', 'region', 'subnational_level1', 'subnational_level2', 'country',
                    'location_id', 'registry_index', 'year_start', 'year_end', 'year_id', 'sex', 'coding_system', 'split_group', 'age']
    residual_cause = 'neo_other_cancer'
    resources_dir = utils.get_path("mi_dataset_resources", process="mi_dataset")
    
    package_folder = '{}/redistribution/{}'.format(resources_dir, PACKAGE_MAP)
    cause_map_file = package_folder + '/_package_map.csv'
    cause_map = pd.read_csv(cause_map_file)
    packages = get_packages(this_dataset, input_data, package_folder, cause_map)
    if len(packages) == 0:
        print("No packages available with which to redistribute this data.")
        return(input_data)

    prepped_data, proportion_metadata = prep_data(input_data, proportion_uids, residual_cause=residual_cause)
    evaluated_cause_map = evaluate_cause_restrictions(cause_map, proportion_metadata, proportion_uids)
    result, diagnostics = apply_packages(prepped_data, this_dataset, proportion_metadata, evaluated_cause_map,
                                                packages, residual_cause=residual_cause)

    output_data = result.merge(proportion_metadata, on='proportion_id')
    output_data.loc[output_data['cause'] == "neo_other", 'cause'] = "neo_other_cancer"
    output_data = output_data.loc[:, output_cols]
    output_data.loc[:,'freq'].fillna(value=0, inplace=True)
    pt.verify_metric_total(input_data, output_data, "freq", 
                                              "after running redistribution core")
    return(output_data)

