'''
Description: Applies redistribution packages to the input data to redistribute garbage
How To Use:
'''
### Load common cancer paths
import os, sys
import pandas as pd
import json
import time
import cancer_estimation.py_utils.common_utils as utils
import cancer_estimation.a_inputs.a_mi_registry.mi_dataset as md
from cancer_estimation.py_utils.gbd_cancer_tools import add_year_id


''' Prepare cause map '''

def evaluate_cause_restrictions(cause_map, proportion_metadata, proportion_ids):
    '''
    '''
    print("Evaluating cause map restrictions...")
    for t in proportion_ids:
        if t != 'region':
            cause_map['restrictions'] = cause_map['restrictions'].map(lambda x: x.replace(t, 'full_cause_map.ix[i,"'+t+'"]'))
    dict_cause_map = cause_map.set_index('cause').to_dict()
    full_cause_map = []
    for pid in proportion_metadata['proportion_id']:
        temp = cause_map.ix[:,['cause','restrictions']].copy(deep=True)
        temp['proportion_id'] = pid
        full_cause_map.append(temp)
    full_cause_map = pd.concat(full_cause_map).reset_index(drop=True)
    full_cause_map = pd.merge(
        full_cause_map, proportion_metadata, on='proportion_id').reset_index(drop=True)
    full_cause_map['id'] = full_cause_map.index
    maps = {'id': [], 'eval': []}
    for i in full_cause_map.index:
        maps['id'].append(i)
        maps['eval'].append(
            eval(dict_cause_map['restrictions'][full_cause_map.ix[i, 'cause']]))
    return(pd.merge(full_cause_map, pd.DataFrame(maps), on='id').ix[:, ['proportion_id', 'cause', 'eval']].reset_index(drop=True))



''' Prepare packages '''

def read_json(file_path):
    ''' load json
    '''
    json_data = open(file_path)
    data = json.load(json_data)
    json_data.close()
    return(data)


def extract_package(package, cause_map):
    ''' Extract useful package data
    '''
    fmt_package = {}
    fmt_package['package_name'] = package['package_name']
    if 'create_targets' in package:
        fmt_package['create_targets'] = package['create_targets']
    else:
        package['create_targets'] = 0
    # do something
    fmt_package['garbage_codes'] = package['garbage_codes']
    fmt_package['target_groups'] = {}
    c = 0
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


def get_packages(input_data, package_folder, cause_map):
    ''' Import packages
    '''
    print("Importing packages...")
    input_codes = pd.Series(input_data['cause'])
    package_list = read_json(package_folder + '/_package_list.json')
    packages = []
    for rdp in package_list:
        package = extract_package(read_json(package_folder + '/' + rdp + '.json'), cause_map)
        garbage_present = input_codes.isin(list(package['garbage_codes'])).tolist()
        if not any(garbage_present):
                continue
        print("    - {} {}".format(rdp, package['package_name']))
        packages.append(package)
    return(packages)



''' Prepare data '''

def pull_metadata(data, uid_cols):
    ''' Create metadata groups based on signature variables
    '''
    data = data[uid_cols].drop_duplicates().reset_index(drop=True)
    data['unique_group_id'] = data.index
    return(data)


# Import data
def prep_data(data, proportion_ids, residual_cause='cc_code'):
    '''
    '''
    output_cols = ['proportion_id', 'cause', 'freq']
    data = add_year_id(data).rename(columns={'year':'year_id'})
    proportion_metadata = pull_metadata(data, proportion_ids).rename(
                                columns={'unique_group_id': 'proportion_id'})
    data = pd.merge(data, proportion_metadata, on=proportion_ids)
    prop_map = data.ix[:,['proportion_id']].drop_duplicates()
    prop_map.loc[:, 'cause'] = residual_cause
    prop_map.loc[:, 'freq'] = 0.0
    output_data = pd.concat([data, prop_map])
    output_data = output_data[output_cols].groupby(
                ['proportion_id', 'cause']).sum().reset_index()
    return(output_data[output_cols], proportion_metadata)



''' Get proportions '''

def find_weight_groups(package, proportion_metadata):
    ''' Find weight groups
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
        weights.append(meta.ix[meta['eval']==True, ['proportion_id', 'weight_group']])
    return(pd.concat(weights))


def create_target_df(package):
    '''
    '''
    targets = []
    for tg in package['target_groups']:
        temp = pd.DataFrame(
            {'cause': package['target_groups'][tg]['target_codes']})
        temp['target_group'] = tg
        targets.append(temp)
    targets = pd.concat(targets).reset_index(drop=True)
    return(targets)


def create_proportion_list(data, weight_groups, targets, package):
    '''
    '''
    proportion_list = []
    for pid in weight_groups['proportion_id']:
        temp = targets.copy(deep=True)
        if package['create_targets'] == 1:
            temp['freq'] = 0.001
        else:
            temp['freq'] = 0
        temp['proportion_id'] = pid
        proportion_list.append(temp)
    for tg in package['target_groups']:
        temp = data.ix[data['cause'].isin(package['target_groups'][tg]['target_codes']),
                       ['proportion_id', 'cause', 'freq']].copy(deep=True)
        temp['target_group'] = tg
        proportion_list.append(temp)
    return(proportion_list)


def create_weight_df(package):
    '''
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



''' Get data proportions '''

def get_proportions(data, proportion_metadata, package, cause_map_evaluated, residual_cause='cc_code'):
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
    targets = create_target_df(package)
    proportion_list = create_proportion_list(data, weight_groups, targets, package)
    prop_df = pd.concat(proportion_list).sort_values(
                                        by=['proportion_id','target_group','cause']
                                        ).reset_index(drop=True)
    prop_map = prop_df.merge(proportion_metadata, on='proportion_id')
    assert len(prop_map) >= len(prop_df), "ERROR. Lost proportions before eval merge"
    # Merge with cause restrictions
    prop_map = prop_map.merge(cause_map_evaluated, on=['proportion_id','cause'])
    assert len(prop_map) >= len(prop_df), "ERROR. Lost proportions after eval merge"
    # Zero out if the cause is restricted
    prop_map.ix[prop_map['eval']==False,'freq'] = 0
    # Calculate totals for each cause
    proportions = prop_map.ix[:, ['proportion_id', 'target_group', 'cause', 'freq']
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
        proportions.ix[:, c] = proportions[c].astype('float64')
    proportions['proportion'] = (proportions.freq / proportions.total) * \
        proportions.weight
    # If the total proportion for a given proportion id
    #  is 0, move to the residual code
    print("      adding residual causes where needed...")
    prop_totals = proportions.ix[proportions.total == 0,
                            ['proportion_id', 'target_group', 'weight', 'total']
                            ].drop_duplicates(
                            ).set_index('total'
                            ).rename(columns={'weight': 'proportion'})
    prop_totals.at[0, 'cause'] = residual_cause
    prop_totals = prop_totals.reset_index().ix[:,
                                             ['proportion_id', 'proportion', 'cause']
                                            ].groupby( ['proportion_id', 'cause']
                                            ).sum().reset_index()
    sum_at_cause = proportions.ix[proportions.total != 0,
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

def redistribute_garbage(data, proportions, package):
    '''
    '''
    diagnostics = []
    # Make sure the package contains only the codes in the dataset
    assert data.proportion_id.isin(proportions.proportion_id.unique()).all(), \
        "ERROR: not all proportion_ids are present in the proportions dataframe"
    # Tag garbage
    data.loc[:, 'garbage'] = 0
    data.ix[(data['cause'].isin(package['garbage_codes'])), 'garbage'] = 1
    diagnostics.append(data.ix[data['garbage']==1])
    # Get total number of garbage codes for each proportion_id
    temp = data.ix[data['garbage']==1,
                   ['proportion_id', 'freq']].groupby(
                       ['proportion_id']).sum().reset_index()
    temp = temp.rename(columns={'freq': 'garbage'})
    # Redistribute garbage onto targets
    these_proportions = proportions.loc[proportions['proportion_id'].isin(
                                                data['proportion_id'].unique()), :]
    additions = pd.merge(these_proportions, temp, on=[ 'proportion_id'], how='outer')
    for c in ['proportion', 'garbage']:
        additions.loc[:, c].fillna(0, inplace=True)
    additions.ix[:, 'freq'] = additions['proportion'] * additions['garbage']
    additions = additions.ix[additions['freq'] > 0,
                             ['proportion_id', 'cause', 'freq']]
    diagnostics.append(additions)
    # Zero out garbage codes
    data.ix[data['garbage'] == 1, 'freq'] = 0
    # Tack on redistributed data
    output_data = pd.concat([data, additions])
    output_data = output_data.ix[:, ['proportion_id', 'cause', 'freq']].reset_index(drop=True)
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


def apply_packages(prepped_data, proportion_metadata, evaluated_cause_map,
                    packages, residual_cause='cc_code', diagnostic_output=False):
    '''
    '''
    print("Running redistribution...")
    diagnostics_all = []
    seq = 0
    data = prepped_data.copy()
    for package in packages:
        print("    - package {}".format(package['package_name']))
        print("      calculating proportions...")
        proportions = get_proportions(
            data, proportion_metadata, package, evaluated_cause_map, residual_cause=residual_cause)
        if len(proportions) == 0:
            print("No proportions available from this package.")
            continue
        print("      redistributing data...")
        data, diagnostics = redistribute_garbage(data, proportions, package)
        if len(diagnostics) == 0:
            continue
        data = data.ix[(data.freq >= 0) | (data.cause == residual_cause)]
        data = data.groupby(['proportion_id', 'cause']).sum().reset_index()
        diagnostics.loc[:, 'seq'] = seq
        diagnostics.loc[:, 'package'] = package['package_name']
        seq += 1
        if diagnostic_output:
            diagnostics_all.append(diagnostics)
        test_package_application(prepped_data, data)
    if diagnostic_output:
        diagnostics = pd.concat(diagnostics_all).reset_index(drop=True)
    return(data.ix[data.freq >= 0], diagnostics)


def run(input_data, PACKAGE_MAP, TEMP_FOLDER):
    '''
    '''
    if int(input_data['freq'].sum()) == 0:
        print("Data sums to zero.")
        return(input_data)

    else:
        input_data.loc[input_data['cause'] == "neo_other_cancer", 'cause'] = "neo_other"
    
    output_cols =  ['registry_index', 'year_start','year_end', 'year_id', 'sex', 'coding_system',
                                'split_group', 'age', 'cause', 'freq']
    proportion_uids = ['dev_status', 'super_region', 'region', 'subnational_level1', 'subnational_level2', 'country',
                    'location_id', 'registry_index', 'year_start', 'year_end', 'year_id', 'sex', 'coding_system', 'split_group', 'age']
    residual_cause = 'ZZZ'
    resources_dir = utils.get_path("mi_dataset_resources", process="mi_dataset")
    package_folder = '{}/redistribution/{}'.format(resources_dir, PACKAGE_MAP)
    cause_map_file = package_folder + '/_package_map.csv'
    cause_map = pd.read_csv(cause_map_file)
    packages = get_packages(input_data, package_folder, cause_map)
    if len(packages) == 0:
        print("No packages available with which to redistribute this data.")
        return(input_data)

    start_time = time.time()
    prepped_data, proportion_metadata = prep_data(input_data, proportion_uids, residual_cause=residual_cause)
    evaluated_cause_map = evaluate_cause_restrictions(cause_map, proportion_metadata, proportion_uids)
    result, diagnostics = apply_packages(prepped_data, proportion_metadata, evaluated_cause_map,
                                                packages, residual_cause=residual_cause)
    output_data = result.merge(proportion_metadata, on='proportion_id')

    output_data.loc[output_data['cause'] == "neo_other", 'cause'] = "neo_other_cancer"
    
    output_data = output_data.ix[:, output_cols]
    output_data.loc[:,'freq'].fillna(value=0, inplace=True)
    diff = output_data['freq'].sum() - input_data['freq'].sum()
    assert abs(diff) < 1, "Difference from input after rdp is too large"
    return(output_data)
