** ********************************************************************************************************
** Purpose: Load maps from the database and save
** ********************************************************************************************************

import requests
import json
import sys
import pandas as pd

cause_set_version_id = 16
map_type_id = 1

causes_filepath = "/home/j/WORK/00_dimensions/03_causes/gbd2015_causes_all.dta"
output_folder = "/home/j/WORK/00_dimensions/03_causes/temp"
url = "strUrl"

map_df = []

code_system = sys.argv[1]


def multi_try_stata_write(df, path, tries=0, max_tries=3):
    """Try multiple times to write to stata."""
    try:
        df.to_stata(path, write_index=False)
    except IOError:
        if tries < max_tries:
            tries = tries + 1
            multi_try_stata_write(df, path, tries=tries)
        else:
            raise

# Pull causes
causes_df = pd.read_stata(causes_filepath)
causes_df = causes_df[['cause_id', 'acause', 'cause_name']]

# Get list of all cscsmt_ids
response = requests.get(url+"/cscsmt/")
cscsmt_df = pd.DataFrame(response.json())
cscsmt_df['cscsmt_id'] = cscsmt_df['cscsmt_id'].map(lambda x: int(x))

# Get map types
response = requests.get(url+"/map_types")
maptypes_df = pd.DataFrame(response.json())

# Get code systems
response = requests.get(url+"/code_system")
cs_df = pd.DataFrame(response.json())
cs_df['code_system_id'] = cs_df['code_system_id'].map(lambda x: int(x))

for cs in cs_df.ix[cs_df['name']==code_system, 'code_system_id'].tolist():
    # Pull code list
    response = requests.get(url+"/code_system/{}/codes/".format(cs))
    codes_df = pd.DataFrame(response.json())

    # Bring in maps for each cscsmt_id
    temp = []
    for cscsmt_id in cscsmt_df.ix[(cscsmt_df['code_system_id']==cs)&(cscsmt_df['cause_set_version_id']==cause_set_version_id), 'cscsmt_id'].drop_duplicates():
        # Get maps for source
        response = requests.get(url+"/cscsmt/{}/maps".format(cscsmt_id))
        temp.append(pd.DataFrame(response.json()))
    temp = pd.concat(temp)

    # Merge on map_type info
    temp = pd.merge(temp, cscsmt_df, on='cscsmt_id')
    temp = pd.merge(temp, maptypes_df, on='map_type_id')

    # Merge on causes
    temp = pd.merge(temp, causes_df, on='cause_id')
           
    # Reshape map_type name wide
    temp = temp.ix[:, ['code_id', 'code_system_id', 'name', 'acause', 'cause_name']].rename(columns={'name': 'map_type_name'})
    temp = temp.set_index(['code_id', 'code_system_id', 'map_type_name'])
    temp = temp.unstack(level=-1)
    temp.columns = ['_'.join(col).strip() for col in temp.columns.values]
    for mt in ['YLL', 'YLD']:
        temp = temp.rename(columns={'acause_{}'.format(mt): '{}_cause'.format(mt.lower()), 'cause_name_{}'.format(mt): '{}_cause_name'.format(mt.lower())})
    temp = temp.reset_index()

    # Merge on code information
    temp = pd.merge(temp, codes_df[['code_id', 'value', 'name', 'sort']], on='code_id').rename(columns={'value':'cause_code', 'name':'cause_name'})

    # Merge on code system information
    temp = pd.merge(temp, cs_df, on='code_system_id')

    # Remove decimals if needed
    temp.ix[temp['remove_decimal']==True, 'cause_code'] = temp.ix[temp['remove_decimal']==True, 'cause_code'].map(lambda x: x.replace('.',''))

    # Reformat and add to maps
    temp = temp.sort('sort').reset_index(drop=True).ix[:, ['source_label', 'cause_code', 'cause_name', 'yll_cause', 'yll_cause_name', 'yld_cause']].fillna('')
    map_df.append(temp)

map_df = pd.concat(map_df).reset_index(drop=True)

# Remove source label if they are all blank
keep_vars = ['cause_code', 'cause_name', 'yll_cause', 'yll_cause_name']
if len(map_df.ix[map_df['source_label']!='']) > 0:
    keep_vars.append('source_label')
    
# Remove YLD causes if they are all blank
if len(map_df.ix[map_df['yld_cause']!='']) > 0:
    keep_vars.append('yld_cause')
    keep_vars.append('yld_cause_name')
map_df = map_df.ix[:, keep_vars].fillna('')

# Make cause names no greater than 244 characters
map_df['cause_name'] = map_df['cause_name'].map(lambda x: x[:243])
    
# Make sure all columns are formatted as strings
for c in map_df.columns:
    for i in map_df.index:
        try:
            a = str(map_df.ix[i, c])
        except:
            map_df.ix[i, c] = ''
    map_df[c] = map_df[c].astype('str')
    
for c in cs_df.columns:
    for i in cs_df.index:
        try:
            a = str(cs_df.ix[i, c])
        except:
            cs_df.ix[i, c] = ''
    cs_df[c] = cs_df[c].astype('str')
    
# Save
multi_try_stata_write(map_df, output_folder+"/map_{}.dta".format(code_system))
multi_try_stata_write(cs_df.ix[cs_df['name']==code_system], 
    output_folder+"/codesystems_{}.dta".format(code_system))



""" Save Package Sets for redistribution lookup """
# Get code systems
response = requests.get(url+"/code_system")
cs_df = pd.DataFrame(response.json())

# Get CSCSMT
response = requests.get(url+"/cscsmt")
cscsmt_df = pd.DataFrame(response.json())

# Get package sets
response = requests.get(url+"/package_set")
ps_df = pd.DataFrame(response.json())

# Merge everything together
ps_df = pd.merge(ps_df, cscsmt_df, on='cscsmt_id')
ps_df = pd.merge(ps_df, cs_df, on='code_system_id')

# Keep specific cause set version, map type and code system name
ps_df = ps_df.ix[(ps_df['cause_set_version_id']==cause_set_version_id)&(ps_df['map_type_id']==map_type_id)&(ps_df['name']==code_system)]
for c in ps_df.columns:
    for i in ps_df.index:
        try:
            a = str(ps_df.ix[i, c])
        except:
            ps_df.ix[i, c] = ''
    ps_df[c] = ps_df[c].astype('str')
multi_try_stata_write(ps_df, 
    output_folder+"/packagesets_{}.dta".format(code_system))
