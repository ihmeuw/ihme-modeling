### Load common cancer paths
import os, sys
sys.path.append(h + '/cancer_estimation/00_common')
import set_common_roots as common
roots = common.roots

## Import additional libraries
import pandas as pd
import json
import time

""" Prepare cause map """
def get_cause_restrictions(cause_map):
    return cause_map.set_index('cause').to_dict()

def evaluate_cause_restrictions(cause_map, proportion_metadata, proportion_ids):
    print "          -adding proportion ids"
    for t in proportion_ids:
        if t != 'region':
            cause_map['restrictions'] = cause_map['restrictions'].map(lambda x: x.replace(t, 'full_cause_map.ix[i,"'+t+'"]'))
    dict_cause_map = get_cause_restrictions(cause_map)
    full_cause_map = []
    print "          -adding proportion metadata"
    for pid in proportion_metadata['proportion_id']:
        temp = cause_map.ix[:,['cause','restrictions']].copy(deep=True)
        temp['proportion_id'] = pid
        full_cause_map.append(temp)
    full_cause_map = pd.concat(full_cause_map).reset_index(drop=True)
    full_cause_map = pd.merge(full_cause_map,proportion_metadata,on='proportion_id').reset_index(drop=True)
    full_cause_map['id'] = full_cause_map.index
    maps = {'id': [], 'eval': []}

    print "         - generating full cause map"
    for i in full_cause_map.index:
        maps['id'].append(i)
        maps['eval'].append(eval(dict_cause_map['restrictions'][full_cause_map.ix[i,'cause']]))
    return pd.merge(full_cause_map,pd.DataFrame(maps),on='id').ix[:,['proportion_id','cause','eval']].reset_index(drop=True)


""" Prepare packages """
# Read in JSON
def read_json(file_path):
    json_data = open(file_path)
    data = json.load(json_data)
    json_data.close()
    return data

# Extract useful package data
def extract_package(package, cause_map):
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

    return fmt_package

# Import packages
def get_packages(input_data, package_folder, cause_map, output_file):
    input_codes = pd.Series(input_data['cause'])
    package_list = read_json(package_folder + '/_package_list.json')
    packages = []
    for rdp in package_list:
        #print "Checking package: " + rdp
        package = extract_package(read_json(package_folder + '/' + rdp + '.json'), cause_map)
        garbage_present = input_codes.isin(list(package['garbage_codes'])).tolist()
        if not any(garbage_present):
                continue
        print "          Importing package: " + rdp + "  " + package['package_name']
        packages.append(package)
    if len(packages) == 0:
        input_data.to_stata(output_file, write_index=False)
        os._exit(0)
    return packages

""" Prepare data """
# Create metadata groups based on signature variables
def pull_metadata(data, unique_ids):
    data = data[unique_ids].drop_duplicates().reset_index(drop=True)
    data['unique_group_id'] = data.index
    return data
# Import data
def prep_data(data, signature_ids, proportion_ids, residual_cause='cc_code'):
    signature_metadata = pull_metadata(data, signature_ids).rename(columns={'unique_group_id': 'signature_id'})
    proportion_metadata = pull_metadata(data, proportion_ids).rename(columns={'unique_group_id': 'proportion_id'})
    data = pd.merge(data, signature_metadata, on=signature_ids)
    data = pd.merge(data, proportion_metadata, on=proportion_ids)
    data = pd.concat([data,data.ix[:, ['proportion_id', 'signature_id']].drop_duplicates().set_value(data.ix[:, ['proportion_id', 'signature_id']].drop_duplicates().index, 'cause', residual_cause).set_value(data.ix[:, ['proportion_id', 'signature_id']].drop_duplicates().index, 'freq', 0)])
    data = data[['proportion_id', 'signature_id', 'cause', 'freq']].groupby(['proportion_id', 'signature_id', 'cause']).sum().reset_index()
    return data[['proportion_id', 'signature_id', 'cause', 'freq']], signature_metadata, proportion_metadata


""" Get proportions """
# Find weight groups
def find_weight_groups(package, proportion_metadata):
    meta = proportion_metadata.copy(deep=True)
    weights = []
    for wg in package['weight_groups']:
        parameters = package['weight_groups'][wg]
        meta['weight_group'] = str(wg)
        meta['eval'] = meta.eval(parameters)
        weights.append(meta.ix[meta['eval']==True, ['proportion_id', 'weight_group']])
    return pd.concat(weights)
# Get weights for cause groups
def get_weights(package, weight_group, target_group):
 
    return package['target_groups'][target_group]['weights'][int(weight_group)]
# Get data proportions
def get_proportions(data, proportion_metadata, package, cause_map_evaluated, residual_cause='cc_code'):
    """
    This function generates proportions based on the frequencies in the data using the following steps:
        1. Get list of targets for each target group
        2. Generate base proportions (if the package is supposed to create causes)
        3. Pull frequencies of each cause in the data
        4. Determine and pull weight group for proportion group
        5. Pull weights from weight group for each target group
        6. Calculate fraction to each cause
    """
    weight_groups = find_weight_groups(package, proportion_metadata)

    print "                -Identifying targets"
    targets = []
    for tg in package['target_groups']:
        temp = pd.DataFrame({'cause': package['target_groups'][tg]['target_codes']})
        temp['target_group'] = tg
        targets.append(temp)
    targets = pd.concat(targets).reset_index(drop=True)

    print "                -Pulling data counts"
    proportions = []
    for pid in weight_groups['proportion_id']:
        temp = targets.copy(deep=True)
        if package['create_targets'] == 1:
            temp['freq'] = 0.001
        else:
            temp['freq'] = 0
        temp['proportion_id'] = pid
        proportions.append(temp)
    for tg in package['target_groups']:
        temp = data.ix[data['cause'].isin(package['target_groups'][tg]['target_codes']),['proportion_id','cause','freq']].copy(deep=True)
        temp['target_group'] = tg
        proportions.append(temp)
    proportions = pd.concat(proportions).sort_values(by=['proportion_id','target_group','cause']).reset_index(drop=True)
    proportions = pd.merge(proportions,proportion_metadata,on='proportion_id')
    print "                -Merging on cause restrictions"
    # Merge on cause restrictions
    proportions = pd.merge(proportions,cause_map_evaluated,on=['proportion_id','cause'])
    # Zero out if the cause is restricted
    proportions.ix[proportions['eval']==False,'freq'] = 0
    # Calculate totals for each cause
    print "                -Calculating totals for each cause"
    proportions = proportions.ix[:,['proportion_id','target_group','cause','freq']].groupby(['proportion_id','target_group','cause']).sum().reset_index()
    # Calculate totals for each target group & merge back on
    print "                -Calculating totals for each target group"
    proportions = proportions.set_index(['proportion_id','target_group']).join(proportions.groupby(['proportion_id','target_group']).sum().rename(columns={'freq':'total'}))
    proportions = pd.merge(proportions.reset_index(),weight_groups,on='proportion_id')
    # Merge on weights
    print "                -Merging on weights"
    proportions['weight'] = proportions.index.map(lambda x: get_weights(package, proportions.ix[x,'weight_group'], proportions.ix[x,'target_group']))
    # Calculate final proportions to apply
    print "                -Reformatting data type"
    for c in ['freq', 'weight', 'total']:
        proportions[c] = proportions[c].astype('float64')
    print "                -Calculating proportions"
    proportions['proportion'] = (proportions.freq / proportions.total) * proportions.weight
    # If the total proportion for a given proportion id is 0, move to the residual code
    print "                -Adding residual causes where needed"
    proportions = pd.concat([proportions.ix[proportions.total==0,['proportion_id','target_group','weight','total']].drop_duplicates().set_index('total').set_value(0,'cause',residual_cause).rename(columns={'weight':'proportion'}).reset_index().ix[:,['proportion_id','proportion','cause']].groupby(['proportion_id','cause']).sum().reset_index(),proportions.ix[proportions.total!=0,['proportion_id','cause','proportion']].groupby(['proportion_id','cause']).sum().reset_index()]).reset_index(drop=True)
    # Again make sure everything sums to 1
    print "                -Make sure everything summs to 1"
    proportions = proportions.set_index(['proportion_id']).join(proportions.groupby(['proportion_id']).sum().rename(columns={'proportion':'total'}))
    proportions['proportion'] = (proportions.proportion / proportions.total)
    return proportions.reset_index().ix[:,['proportion_id','cause','proportion']]

def get_proportions2(data, proportion_metadata, package, cause_map_evaluated, residual_cause='cc_code'):
    """
    This function generates proportions based on the frequencies in the data using the following steps:
        1. Get list of targets for each target group
        2. Generate base proportions (if the package is supposed to create causes)
        3. Pull frequencies of each cause in the data
        4. Determine and pull weight group for proportion group
        5. Pull weights from weight group for each target group
        6. Calculate fraction to each cause
    """
    weight_groups = find_weight_groups(package, proportion_metadata)

    print "                -Identifying targets"
    targets = []
    for tg in package['target_groups']:
        temp = pd.DataFrame({'cause': package['target_groups'][tg]['target_codes']})
        temp['target_group'] = tg
        targets.append(temp)
    targets = pd.concat(targets).reset_index(drop=True)

    print "                -Pulling data counts"
    proportions = []
    for pid in weight_groups['proportion_id']:
        temp = targets.copy(deep=True)
        if package['create_targets'] == 1:
            temp['freq'] = 0.001
        else:
            temp['freq'] = 0
        temp['proportion_id'] = pid
        proportions.append(temp)
    for tg in package['target_groups']:
        temp = data.ix[data['cause'].isin(package['target_groups'][tg]['target_codes']),['proportion_id','cause','freq']].copy(deep=True)
        temp['target_group'] = tg
        proportions.append(temp)
    proportions = pd.concat(proportions).sort_values(by=['proportion_id','target_group','cause']).reset_index(drop=True)
    proportions = pd.merge(proportions,proportion_metadata,on='proportion_id')
    print "                -Merging on cause restrictions"
    # Merge on cause restrictions
    proportions = pd.merge(proportions,cause_map_evaluated,on=['proportion_id','cause'])
    # Zero out if the cause is restricted
    proportions.ix[proportions['eval']==False,'freq'] = 0
    # Calculate totals for each cause
    print "                -Calculating totals for each cause"
    proportions = proportions.ix[:,['proportion_id','target_group','cause','freq']].groupby(['proportion_id','target_group','cause']).sum().reset_index()
    # Calculate totals for each target group & merge back on
    print "                -Calculating totals for each target group"
    proportions = proportions.set_index(['proportion_id','target_group']).join(proportions.groupby(['proportion_id','target_group']).sum().rename(columns={'freq':'total'}))
    proportions = pd.merge(proportions.reset_index(),weight_groups,on='proportion_id')
    # Merge on weights
    print "                -Merging on weights"
    weights = []
    for tg in package['target_groups']:
        wg = 0
        for wgt in package['target_groups'][tg]['weights']:
            weights.append({'target_group': tg, 'weight_group': str(wg), 'weight': wgt})
            wg += 1
    weights = pd.DataFrame(weights)
    proportions = pd.merge(proportions, weights, on=['target_group', 'weight_group'])
    # Calculate final proportions to apply
    print "                -Reformatting data type"
    for c in ['freq', 'weight', 'total']:
        proportions[c] = proportions[c].astype('float64')
    print "                -Calculating proportions"
    proportions['proportion'] = (proportions.freq / proportions.total) * proportions.weight
    # If the total proportion for a given proportion id is 0, move to the residual code
    print "                -Adding residual causes where needed"
    proportions = pd.concat([proportions.ix[proportions.total==0,['proportion_id','target_group','weight','total']].drop_duplicates().set_index('total').set_value(0,'cause',residual_cause).rename(columns={'weight':'proportion'}).reset_index().ix[:,['proportion_id','proportion','cause']].groupby(['proportion_id','cause']).sum().reset_index(),proportions.ix[proportions.total!=0,['proportion_id','cause','proportion']].groupby(['proportion_id','cause']).sum().reset_index()]).reset_index(drop=True)
    # Again make sure everything sums to 1
    print "                -Make sure everything summs to 1"
    proportions = proportions.set_index(['proportion_id']).join(proportions.groupby(['proportion_id']).sum().rename(columns={'proportion':'total'}))
    proportions['proportion'] = (proportions.proportion / proportions.total)
    return proportions.reset_index().ix[:,['proportion_id','cause','proportion']]

""" Redistribute garbage """
def redistribute_garbage(data, proportions, package):
    # Make sure the package contains all the codes in the proportions set
    temp = data[['proportion_id', 'signature_id']].drop_duplicates().reset_index(drop=True).copy(deep=True)
    proportions = pd.merge(temp, proportions, on='proportion_id')
    # Merge on proportions with the full data set
    data = pd.merge(data, proportions, on=['proportion_id', 'signature_id', 'cause'], how='outer').reset_index(drop=True)
    data['freq'] = data['freq'].fillna(0)
    # Tag garbage
    data['garbage'] = 0
    data.ix[(data['cause'].isin(package['garbage_codes']))&(data['proportion'].isnull()), 'garbage'] = 1
    # Make a column with total garbage for each signature_id
    data = data.set_index(['signature_id']).join(data.ix[data['garbage']==1,['signature_id','freq']].set_index('signature_id').sum(level='signature_id').rename(columns={'freq':'garbage_freq'})).reset_index()
    # data.to_csv('/vagrant/engine_room/redistribution_test/intermediate/'+package['package_name']+'_garbage_1.csv')
    # Redistribute!
    data.ix[(data.proportion.notnull())&(data.garbage_freq.notnull())&(data.garbage.notnull())&(data.garbage_freq!=0),'freq'] = data.freq + (data.proportion * data.garbage_freq)
    # Create diagnostic table
    diagnostics = pd.concat([data.ix[data.garbage==1,['signature_id','cause','garbage','freq']].rename(columns={'freq':'unit'}),data.ix[data.proportion.notnull(),['signature_id','cause','garbage','proportion']].rename(columns={'proportion':'unit'})])
    # Zero out garbage codes
    data.ix[data.garbage==1,'freq'] = 0
    # data.to_csv('/vagrant/engine_room/redistribution_test/intermediate/'+package['package_name']+'_garbage_2.csv')
    return data.ix[:,['proportion_id','signature_id','cause','freq']], diagnostics

def redistribute_garbage2(data, proportions, package):
    diagnostics = []
    # Make sure the package contains all the codes in the proportions set
    temp = data[['proportion_id', 'signature_id']].drop_duplicates().reset_index(drop=True).copy(deep=True)
    proportions = pd.merge(temp, proportions, on='proportion_id')
    # Tag garbage
    data['garbage'] = 0
    data.ix[(data['cause'].isin(package['garbage_codes'])), 'garbage'] = 1
    diagnostics.append(data.ix[data['garbage']==1])
    # Get total number of garbage codes for each signature_id
    temp = data.ix[data['garbage']==1, ['proportion_id', 'signature_id', 'freq']].groupby(['proportion_id', 'signature_id']).sum().reset_index()
    temp = temp.rename(columns={'freq': 'garbage'})
    # Redistribute garbage onto targets
    additions = pd.merge(proportions, temp, on=['proportion_id', 'signature_id'], how='outer')
    for c in ['proportion', 'garbage']:
        additions[c] = additions[c].fillna(0)
    additions['freq'] = additions['proportion'] * additions['garbage']
    additions = additions.ix[additions['freq']>0, ['signature_id', 'proportion_id', 'cause', 'freq']]
    diagnostics.append(additions)
    # Zero out garbage codes
    data.ix[data['garbage']==1, 'freq'] = 0
    # Tack on redistributed data
    data = pd.concat([data, additions]).ix[:, ['proportion_id', 'signature_id', 'cause', 'freq']].reset_index(drop=True)
    # Create diagnostics
    diagnostics = pd.concat(diagnostics)
    diagnostics['garbage'] = diagnostics['garbage'].fillna(0)
    # Collapse to proportion id
    diagnostics = diagnostics.groupby(['proportion_id', 'garbage', 'cause'])['freq'].sum().reset_index()
    # Return outputs
    return data, diagnostics


""" Run everything """
def run_redistribution(input_data, signature_ids, proportion_ids, cause_map, package_folder, output_file, residual_cause='cc_code', diagnostic_output=False):
    data, signature_metadata, proportion_metadata = prep_data(input_data, signature_ids, proportion_ids, residual_cause=residual_cause)
    print "Importing packages"
    packages = get_packages(input_data, package_folder, cause_map, output_file)
    print "Evaluating cause map restrictions"
    cause_map_evaluated = evaluate_cause_restrictions(cause_map, proportion_metadata, proportion_ids)
    print "Running redistribution..."
    diagnostics_all = []
    seq = 0
    for package in packages:
        print "    package: "+package['package_name']
        print "        Deaths before = "+str(data.freq.sum())
        print "            ... calculating proportions"
        proportions = get_proportions2(data, proportion_metadata, package, cause_map_evaluated, residual_cause=residual_cause)
        print "            ... redistributing data"
        data, diagnostics = redistribute_garbage2(data, proportions, package)
        data = data.ix[(data.freq>0)|(data.cause==residual_cause)]
        data = data.groupby(['proportion_id','signature_id','cause']).sum().reset_index()
        diagnostics['seq'] = seq
        diagnostics['package'] = package['package_name']
        seq += 1
        if diagnostic_output:
            diagnostics_all.append(diagnostics)
        print "        Deaths after = "+str(data.freq.sum())
    print "Done!"
    data = pd.merge(data, signature_metadata, on='signature_id')
    if diagnostic_output:
        diagnostics = pd.concat(diagnostics_all).reset_index(drop=True)
    return data.ix[data.freq>0], diagnostics, signature_metadata, proportion_metadata


"""
    Assign data location and data type. accept arguments if they are sent.
"""
    TEMP_FOLDER = sys.argv[1]
    MAP_FOLDER = sys.argv[2]
    PACKAGE_MAP = sys.argv[3]
    SPLIT_GROUP = int(sys.argv[4])
    MAGIC_TABLE = int(sys.argv[5])
"""
"""
print('        ...Redistributing {}...    '.format(TEMP_FOLDER))

if MAGIC_TABLE == 1:
    print "MAGIC TABLE ON"

rdp_data_folder = TEMP_FOLDER
input_file = '{}/rdp_script_input.dta'.format(rdp_data_folder)
output_file = '{}/rdp_script_output.dta'.format(rdp_data_folder)
package_folder = '{}/{}'.format(MAP_FOLDER, PACKAGE_MAP)
cause_map = '{}/_package_map.csv'.format(package_folder)
signature_ids = ['dev_status', 'super_region', 'region', 'subnational_level1', 'subnational_level2', 'country', 'location_id', 'registry_id', 'year_start', 'year_end', 'sex', 'coding_system', 'split_group', 'age']
proportion_ids = signature_ids
residual_cause = 'ZZZ'

input_data = pd.read_stata(input_file)
cause_map = pd.read_csv(cause_map)

start_time = time.time()
if MAGIC_TABLE == 1:
    print "Will produce diagnostic table"
    output_data, diagnostics, signature_metadata, proportion_metadata = run_redistribution(input_data, signature_ids, proportion_ids, cause_map, package_folder, output_file, residual_cause=residual_cause, diagnostic_output=True)
    diagnostics.to_csv('{}/magic_table.csv'.format(TEMP_FOLDER))
else:
    output_data, diagnostics, signature_metadata, proportion_metadata = run_redistribution(input_data, signature_ids, proportion_ids, cause_map, package_folder, output_file, residual_cause=residual_cause)
print " --- {} seconds --- ".format(time.time()-start_time)
output_data = output_data.ix[:,['location_id', 'registry_id', 'year_start', 'year_end', 'sex', 'coding_system', 'split_group', 'age', 'cause', 'freq']]
#

## set data_types and export to stata. multiply freq by a factor of 100 to reduce stata numerical formatting errros
print 'Finalizing...'
for c in ['year_start', 'year_end', 'sex', 'split_group']:
    output_data[c] = output_data[c].fillna(-1).astype('int64')
for c in ['freq', 'age']:
    output_data[c] = output_data[c].astype('float64')
for c in ['registry_id', 'coding_system', 'cause']:
    output_data[c] = output_data[c].astype('str').str.encode('latin-1')

## Export
output_data.to_stata(output_file, write_index=False)

pre_rdp_sum = input_data['freq'].sum()
post_rdp_sum = output_data['freq'].sum()
if post_rdp_sum != pre_rdp_sum:
    print "Difference exists in total events before and after redistribution:"
    print "Difference in events = " + str(post_rdp_sum - pre_rdp_sum) + " events"

print "Done!"