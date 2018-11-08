import sqlalchemy as sql
import pandas as pd
import requests
import json
import os

bf = 'FILEPATH'

output = []

# Define useful functions
def write_json(json_dict,file_path):
    je = open(file_path,'w')
    je.write(json.dumps(json_dict))
    je.close()

def make_formatted_output(df,model,pk_field):
    output = []
    for i in df.index:
        v = df.ix[i,pk_field]
        temp = {'model':model, 'pk':int(v), 'fields':{}}
        for c in df.columns:
            if c != pk_field and pd.notnull(df.ix[i,c]):
                v = df.ix[i,c]
                if df[c].dtype in ['int64']:
                    v = int(v)
                elif df[c].dtype in ['float64']:
                    pass
                elif df[c].dtype in ['bool']:
                    v = bool(v)
                else:
                    pass
                temp['fields'][c] = v
        output.append(temp)
    return output

def get_data(url):
    response = requests.get('FILEPATH'+url)
    try:
        return pd.DataFrame(response.json())
    except:
        return pd.DataFrame([response.json()])

""" Computed Packages """
# ComputedPackage
print "Importing ComputedPackage"
computedpackage_df = get_data("computed_package")

run_dates = [""]

# Merge on proportions
regressions_df = computedpackage_df.copy(deep=True)
regressions_df = regressions_df.ix[~(regressions_df['computed_package_id'].isin([50,51])), ['computed_package_id', 'computed_package_name']].drop_duplicates().sort('computed_package_id').reset_index(drop=True)
all_regression_proportions = []
v = 1
for run_date in run_dates:
    for cpid in regressions_df['computed_package_id'].drop_duplicates():
        print run_date, cpid
        cpid = int(cpid)
        temp = pd.read_stata(bf+'FILEPATH'.format(cpid, run_date))
        temp['computed_package_id'] = cpid
        if v == 1:
            temp['computed_package_version_description'] = "Initial proportions from {}".format(run_date)
        else:
            temp['computed_package_version_description'] = "Updated proportions from {}".format(run_date)
        temp['computed_package_version_description'] = "Updated proportions from {}".format(run_date)
        temp['version'] = v
        if run_date == run_dates[-1]:
            temp['is_best'] = True
        else:
            temp['is_best'] = False
        all_regression_proportions.append(temp)
    v += 1

all_regression_proportions = pd.concat(all_regression_proportions)

regressions_df = pd.merge(regressions_df, all_regression_proportions, on='computed_package_id')

# Make combined packages 7 & 8 for REG-15 in ICD9
temp_package_df = regressions_df.copy(deep=True)
temp_package_df = temp_package_df.ix[temp_package_df['computed_package_id'].isin([7,8]), 
                                     ['computed_package_id', 'computed_package_version_description', 
                                      'version', 'is_best', 'country', 'sex', 'age', 'acause', 'wgt']]
temp_package_df['computed_package_id'] = 50
temp_package_df = temp_package_df.groupby(['computed_package_id', 'version', 'is_best', 
                                           'country', 'computed_package_version_description',
                                           'sex', 'age', 'acause']).sum().reset_index()
temp_package_df = pd.merge(temp_package_df, 
                           temp_package_df.groupby(['computed_package_id', 'version', 'is_best',
                                                    'country', 'computed_package_version_description',
                                                    'sex', 'age']).sum().reset_index().rename(columns={'wgt':'total'}), 
                           on=['computed_package_id', 'version', 'is_best', 'computed_package_version_description', 
                               'country', 'sex', 'age'])
temp_package_df['wgt'] = temp_package_df['wgt'] / temp_package_df['total']
temp_package_df['computed_package_name'] = 'Urinary cancer & Endocrine gland'
temp = temp_package_df.ix[:, ['version', 'acause']].drop_duplicates().reset_index(drop=True)
temp['group_cause'] = temp.index + 1
temp_package_df = pd.merge(temp_package_df, temp, on=['version', 'acause'])
regressions_df = pd.concat([regressions_df, temp_package_df]).reset_index(drop=True)

# ComputedPackageVersion
computedpackageversion_df = regressions_df.copy(deep=True)
computedpackageversion_df = computedpackageversion_df.ix[:, ['computed_package_id', 'computed_package_version_description', 'version', 'is_best']].drop_duplicates().sort(['version', 'computed_package_id']).reset_index(drop=True)
computedpackageversion_df['computed_package_version_id'] = computedpackageversion_df.index + 1
regressions_df = pd.merge(regressions_df, 
                          computedpackageversion_df[['computed_package_version_id', 
                                                     'computed_package_id', 
                                                     'version']], 
                          on=['computed_package_id', 'version'])

# ComputedCauseGroup
regressions_df = regressions_df.ix[:, ['computed_package_version_id', 'country', 'sex', 'age', 'group_cause', 'acause', 'wgt']].reset_index(drop=True)
regressions_df = regressions_df.rename(columns={'group_cause':'group'})
computedcausegroup_df = regressions_df.copy(deep=True)
computedcausegroup_df = computedcausegroup_df.rename(columns={'acause':'computed_group_name'})
computedcausegroup_df = computedcausegroup_df.ix[:, ['computed_package_version_id', 'group', 'computed_group_name']].drop_duplicates().reset_index(drop=True)
computedcausegroup_df['computed_group_id'] = computedcausegroup_df.index + 1
regressions_df = pd.merge(regressions_df, computedcausegroup_df, on=['computed_package_version_id', 'group'])

# ComputedTarget
computedtarget_df = computedcausegroup_df.copy(deep=True)
computedtarget_df = computedtarget_df.rename(columns={'computed_group_name':'target_codes'})
computedtarget_df['computed_target_id'] = computedtarget_df['computed_group_id']
computedtarget_df = computedtarget_df.ix[:, ['computed_target_id', 'computed_group_id', 'target_codes']]
# Wgt group
regressions_df = regressions_df.ix[:, ['computed_package_version_id', 'computed_group_id', 'country', 'sex', 'age', 'wgt']]
computedwgtgroup_df = regressions_df.copy(deep=True)
computedwgtgroup_df = computedwgtgroup_df.ix[:, ['computed_package_version_id', 'country', 'sex', 'age']].drop_duplicates().reset_index(drop=True)
computedwgtgroup_df.ix[computedwgtgroup_df['sex']=='Male', 'sex_sort'] = 1
computedwgtgroup_df.ix[computedwgtgroup_df['sex']=='Female', 'sex_sort'] = 2
computedwgtgroup_df['age_sort'] = computedwgtgroup_df['age']
computedwgtgroup_df = computedwgtgroup_df.sort(['computed_package_version_id', 'country', 'sex_sort', 'age_sort']).reset_index(drop=True)
computedwgtgroup_df = computedwgtgroup_df.ix[:, ['computed_package_version_id', 'country', 'sex', 'age']]
computedwgtgroup_df['computed_wgt_group_id'] = computedwgtgroup_df.index + 1
computedwgtgroup_df['wgt_group_name'] = computedwgtgroup_df['country'] + ', ' + computedwgtgroup_df['sex'] + ', ' + computedwgtgroup_df['age'].map(lambda x: str(x))
regressions_df = pd.merge(regressions_df, computedwgtgroup_df, on=['computed_package_version_id', 'country', 'sex', 'age'])
computedwgtgroup_df = computedwgtgroup_df.ix[:, ['computed_package_version_id', 'computed_wgt_group_id', 'wgt_group_name']].drop_duplicates()
# Wgt group logic set
computedwgtgrouplogicset_df = computedwgtgroup_df.copy(deep=True)
computedwgtgrouplogicset_df = computedwgtgrouplogicset_df.ix[:, ['computed_wgt_group_id']]
computedwgtgrouplogicset_df['computed_wgt_group_logic_set_id'] = computedwgtgrouplogicset_df['computed_wgt_group_id']
# Wgt group logic
computedwgtgrouplogic_df = regressions_df.copy(deep=True)
computedwgtgrouplogic_df = computedwgtgrouplogic_df.ix[:, ['computed_wgt_group_id', 'country', 'sex', 'age']].drop_duplicates().sort('computed_wgt_group_id').reset_index(drop=True)
computedwgtgrouplogic_df.ix[computedwgtgrouplogic_df['sex']=='Male', 'sex'] = 1
computedwgtgrouplogic_df.ix[computedwgtgrouplogic_df['sex']=='Female', 'sex'] = 2
computedwgtgrouplogic_df = pd.melt(computedwgtgrouplogic_df, id_vars=['computed_wgt_group_id'])
computedwgtgrouplogic_df['value'] = computedwgtgrouplogic_df['value'].map(lambda x: str(x))
computedwgtgrouplogic_df['operator'] = '=='
computedwgtgrouplogic_df = computedwgtgrouplogic_df.sort(['computed_wgt_group_id', 'variable', 'value']).reset_index(drop=True)
computedwgtgrouplogic_df = computedwgtgrouplogic_df.rename(columns={'computed_wgt_group_id':'computed_wgt_group_logic_set_id'})
computedwgtgrouplogic_df['computed_wgt_group_logic_id'] = computedwgtgrouplogic_df.index + 1
computedwgtgrouplogic_df = computedwgtgrouplogic_df.ix[:, ['computed_wgt_group_logic_id', 'computed_wgt_group_logic_set_id', 'variable', 'operator', 'value']]
# Wgts
computedwgt_df = regressions_df.copy(deep=True)
computedwgt_df = computedwgt_df.ix[:, ['computed_package_version_id', 'computed_wgt_group_id', 'computed_group_id', 'wgt']].reset_index(drop=True)
computedwgt_df['computed_package_wgt_id'] = computedwgt_df.index + 1

# Computed packages
output = []
for i in make_formatted_output(computedpackageversion_df,'rdp.ComputedPackageVersion','computed_package_version_id'):
    output.append(i)
write_json(output, bf+'FILEPATH')

# Computed cause group
for i in make_formatted_output(computedcausegroup_df,'rdp.ComputedCauseGroup','computed_group_id'):
    output.append(i)
write_json(output, bf+'FILEPATH')

# Computed targets
for i in make_formatted_output(computedtarget_df,'rdp.ComputedTarget','computed_target_id'):
    output.append(i)
write_json(output, bf+'FILEPATH')

# Computed wgt groups
computedwgtgroup_df['s'] = computedwgtgroup_df.index.map(lambda x: int((x % 20) + 1))
for s in computedwgtgroup_df['s'].drop_duplicates():
    output = []
    temp = computedwgtgroup_df.ix[computedwgtgroup_df['s']==s].copy(deep=True)
    temp = temp.drop('s', axis=1)
    for i in make_formatted_output(temp,'rdp.ComputedWgtGroup','computed_wgt_group_id'):
        output.append(i)
    write_json(output, bf+'FILEPATH'.format(s))

# Computed weight group logic sets
computedwgtgrouplogicset_df['s'] = computedwgtgrouplogicset_df.index.map(lambda x: int((x % 20) + 1))
for s in computedwgtgrouplogicset_df['s'].drop_duplicates():
    output = []
    temp = computedwgtgrouplogicset_df.ix[computedwgtgrouplogicset_df['s']==s].copy(deep=True)
    temp = temp.drop('s', axis=1)
    for i in make_formatted_output(temp,'rdp.ComputedWgtGroupLogicSet','computed_wgt_group_logic_set_id'):
        output.append(i)
    write_json(output, bf+'FILEPATH'.format(s))

# Computed wgt group logic
computedwgtgrouplogic_df['s'] = computedwgtgrouplogic_df.index.map(lambda x: int((x % 20) + 1))
for s in computedwgtgrouplogic_df['s'].drop_duplicates():
    output = []
    temp = computedwgtgrouplogic_df.ix[computedwgtgrouplogic_df['s']==s].copy(deep=True)
    temp = temp.drop('s', axis=1)
    for i in make_formatted_output(temp,'rdp.ComputedWgtGroupLogic','computed_wgt_group_logic_id'):
        output.append(i)
    write_json(output, bf+'FILEPATH'.format(s))

# Computed wgts
computedwgt_df = computedwgt_df.ix[:, ['computed_package_wgt_id', 'computed_wgt_group_id', 'computed_group_id', 'wgt']]
computedwgt_df['wgt'] = computedwgt_df['wgt'].astype('float64')
computedwgt_df['wgt'] = computedwgt_df['wgt'].map(lambda x: round(x,18))
computedwgt_df['s'] = computedwgt_df.index.map(lambda x: int((x % 41) + 1))
for s in computedwgt_df['s'].drop_duplicates():
    output = []
    temp = computedwgt_df.ix[computedwgt_df['s']==s].copy(deep=True)
    temp = temp.drop('s', axis=1)
    for i in make_formatted_output(temp,'rdp.ComputedWgt','computed_package_wgt_id'):
        output.append(i)
    write_json(output, bf+'FILEPATH'.format(s))

print "Done!"