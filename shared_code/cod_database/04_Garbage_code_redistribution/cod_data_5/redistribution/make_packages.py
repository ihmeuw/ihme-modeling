
import sqlalchemy as sql
import pandas as pd
import requests
import json
import os
import sys
import time
import random

""" GBD CoD PACKAGE BUILDER.

The following code produces a suite of redistribution packages and a map
specifically for redistribution (this map does NOT contain GBD mappings but
does contain the restrictions applicable to each individual cause).

This also produces files that STATA code can use in preparing and submitting
redistribution jobs.
"""

# Get arguments for code_system_id and the package_set_id
code_system_id = int(sys.argv[1])
package_set_id = int(sys.argv[2])
# code_system_id = 1
# package_set_id = 1


# Set additional variables
cause_set_version_id = 16
map_type_id = 1
output_folder = "/ihme/cod/prep/01_database/02_programs/redistribution/rdp"
causes_filepath = "/home/j/WORK/00_dimensions/03_causes/gbd2015_causes_all.dta"
url = "strUrl"


# Define useful functions
def write_json(json_dict,file_path):
    je = open(file_path,'w')
    je.write(json.dumps(json_dict))
    je.close()

def run_query(sql_statement, db='strDatabase'):
    conn_info = {'strDatabase':{'database':'strDatabase','user':'strUser','pass':'strPassword','host':'strHost'}}
    engine = sql.create_engine('strConnection'+conn_info[db]['user']+':'+conn_info[db]['pass']+'@'+conn_info[db]['host']+'strPort'+conn_info[db]['database'])
    connection = engine.raw_connection()
    result_df = pd.io.sql.read_sql(sql_statement, connection)
    connection.close()
    return result_df
    
def get_data(url):
    response = requests.get(strUrl + url)
    try:
        data = pd.DataFrame(response.json())
    except:
        data = pd.DataFrame([response.json()])
    # fail = True
    # while fail:
        # try:
            # data = pd.DataFrame(response.json())
            # fail = False
        # except:
            # try:
                # data = pd.DataFrame([response.json()])
                # fail = False
            # except:
                # time.sleep(random.randrange(5,120))
                # pass
    return data

def get_dimensions_df():
    """Return a formatted dimensions dataframe with restrictions.

    Uses the most recent cause_Set_version_id for cause set 4,
    which is currently gbd estimation causes."""

    # pull it from the database
    conn = sql.create_engine("strConnection strUser:strPassword@strHost:strPort").connect()
    result = conn.execute(sql.text(
        """SELECT cause_set_version_id, acause, male, female, yll_age_start, yll_age_end, cause_id
        FROM shared.cause_hierarchy_history
        WHERE cause_set_version_id IN (
            SELECT max(cause_set_version_id)
            FROM shared.cause_set_version
            WHERE cause_set_id=4
        );"""))
    dimensions_df = pd.DataFrame(result.fetchall())
    dimensions_df.columns = result.keys()
    conn.close()

    # now format it
    dimensions_df['age_start'] = dimensions_df['yll_age_start']
    dimensions_df['age_end'] = dimensions_df['yll_age_end']
    dimensions_df['age_start'] = dimensions_df['age_start'].fillna(0)
    dimensions_df['age_end'] = dimensions_df['age_end'].fillna(80)
    dimensions_df.ix[(dimensions_df['age_end']==80), 'age_end'] = ''
    dimensions_df = dimensions_df.ix[:,['cause_id', 'acause', 'male', 'female', 'age_start', 'age_end']]

    for v in ['male','female']:
        dimensions_df[v] = dimensions_df[v].fillna(1)

    dimensions_df.ix[dimensions_df['male']==1, 'sex'] = 1
    dimensions_df.ix[dimensions_df['female']==1, 'sex'] = 2
    dimensions_df.ix[(dimensions_df['male']==1)&(dimensions_df['female']==1), 'sex'] = ''
    for v in ['year_start', 'year_end', 'dev_status', 'super_region', 'region', 'country', 'subnational_level1', 'subnational_level2']:
        dimensions_df[v] = ''

    for v in ['sex', 'age_start', 'age_end', 'year_start', 'year_end', 'dev_status', 'super_region', 'region', 'country', 'subnational_level1', 'subnational_level2']:
        dimensions_df[v] = dimensions_df[v].map(lambda x: str(x))

    keep_cols = ['cause_id', 'acause', 'sex', 'age_start', 'age_end', 'year_start', 'year_end', 'dev_status', 'super_region', 'region', 'country', 'subnational_level1', 'subnational_level2']
    dimensions_df = dimensions_df.ix[:, keep_cols]
    return dimensions_df

def generate_logic_statement(d):
    logic = []
    # Ages
    if d['age_start'] != '' or d['age_end'] != '':
        t = []
        if d['age_start'] != '':
            t.append("age >= "+d['age_start'])
        if d['age_end'] != '':
            t.append("age <= "+d['age_end'])
        logic.append("("+" and ".join(t)+")")
    # Years
    if d['year_start'] != '' or d['year_end'] != '':
        t = []
        if d['year_start'] != '':
            t.append("year >= "+d['year_start'])
        if d['year_end'] != '':
            t.append("year <= "+d['year_end'])
        logic.append("("+" and ".join(t)+")")
    # Geographies and sex
    for v in ['sex', 'dev_status', 'super_region', 'region', 'country', 'subnational_level1', 'subnational_level2']:
        if d[v] != '':
            logic.append("("+v+" == "+d[v]+")")
    return " and ".join(logic)

def create_acause_parent_map(cause_data, causehierarchy_data):
    """
    Create a map that lists all the sub-causes for a given cause.  or example,
    if _inj is listed as the cause, this map will allow you to select all
    sub-causes of injuries.
    """
    # Create data frame that list cause_id and its parent id
    temp = {"cause_id":[],"parent_id":[]}
    for i in causehierarchy_data.index:
        for parent_id in causehierarchy_data.ix[i,'path_to_top_parent'].split(","):
            temp['cause_id'].append(causehierarchy_data.ix[i,'cause_id'])
            temp['parent_id'].append(int(parent_id))
    causehierarchy_data = pd.DataFrame(temp)
    # Merge on acause names
    causehierarchy_data = pd.merge(causehierarchy_data,cause_data, on='cause_id')
    causehierarchy_data = pd.merge(causehierarchy_data,cause_data.ix[:,['cause_id','acause']].rename(columns={'cause_id':'parent_id', 'acause':'parent_acause'}), on='parent_id')
    # Return data
    return causehierarchy_data.ix[:,['acause','parent_acause']].drop_duplicates()


def expand_range_codes(code_start, code_end, code_map):
    """ Expands codes from a start code to an end code.
    
    Extracts all of the codes between the start and end code.  Notes:
      -If the start and end codes are garbage, then take only the garbage codes in-between
      -If the start and end codes are not garbage, then take the non-garbage codes in-between
      -If the start and end codes are mismatched garbage/non-garbage, then take everything
      -Never take cc_code (cc_code), sub_totals (sub_total), or still-borns (_sb)
    """

    # Check to make sure the codes are in the map
    if code_start not in code_map['value'].drop_duplicates().tolist() or code_end not in code_map['value'].drop_duplicates().tolist():
        return False, "missing"

    # Create an index on the value of the code
    code_map = code_map.set_index('value')

    # Filter out cc_codes, sub_totals, and _sb from the code map
    code_map = code_map.ix[~code_map['acause'].isin(['cc_code', 'sub_total', '_sb'])]

    # Get the sorting numbers
    code_start = code_map.ix[code_start, 'sort']
    code_end = code_map.ix[code_end, 'sort']

    # Reset the index to the sorting number
    code_map = code_map.reset_index()

    # Return false if code_end comes before code_start
    if code_end < code_start:
        return False, "range_end_before_start"

    # Pull everything between the start and end code
    range_map = code_map.ix[(code_map['sort']>=code_start)&(code_map['sort']<=code_end)].copy(deep=True).set_index('sort')

    # Keep appropriate codes
    if int(range_map.ix[code_start, 'garbage']) == int(range_map.ix[code_end, 'garbage']):
        return True, range_map.ix[range_map['garbage']==range_map.ix[code_start,'garbage'],'value'].tolist()
    else:
        return True, range_map.ix[:,'value'].tolist()


def validate_code(code, map_data, acause_parent_map, code_type, contains_ranges):
    """
    Does validation of a code
    """
    # Make a list of garbage codes and a list of all codes
    map_data = map_data.ix[map_data['acause']!='_sb', ['sort', 'value', 'acause', 'garbage']]
    full_code_list = map_data.ix[:, 'value'].drop_duplicates().tolist()
    available_cause_list = acause_parent_map['parent_acause'].drop_duplicates().tolist()

    # Strip out extra codes
    code = code.strip()

    # Run a series of checks if there are ranges
    if contains_ranges:
        temp = {"value": [], "code_type": [], "error": []}
        # If this code is an acause name, use all codes mapped to that acause its sub-causes
        if code in available_cause_list:
            for acause in acause_parent_map.ix[acause_parent_map['parent_acause']==code, 'acause']:
                for c in map_data.ix[map_data['acause']==acause,'value'].drop_duplicates().tolist():
                    c = c.strip()
                    temp["value"].append(c)
                    temp["code_type"].append(code_type)
                    temp["error"].append("")
        else:
            # See if any of the codes are ranges.  If so, get all the codes between those ranges
            code_value = code.split("-")
            if len(code_value) > 2:
                temp["value"].append(code_value)
                temp["code_type"].append(code_type)
                temp["error"].append("incorrect_format_range")
            elif len(code_value) == 2:
                # Get the start and end codes
                code_start = code_value[0].strip()
                code_end = code_value[1].strip()
                # Expand codes
                result = expand_range_codes(code_start, code_end, map_data)
                if result[0]:
                    for c in result[1]:
                        temp["value"].append(c)
                        temp["code_type"].append(code_type)
                        temp["error"].append("")
                else:
                    temp["value"].append("-".join(code_value))
                    temp["code_type"].append(code_type)
                    temp["error"].append(result[1])
            else:
                # Check to make sure it's a valid code
                if code in full_code_list:
                    temp["value"].append(code)
                    temp["code_type"].append(code_type)
                    temp["error"].append("")
                else:
                    temp["value"].append(code)
                    temp["code_type"].append(code_type)
                    temp["error"].append("missing")
    else:
        temp = {"value": [], "code_type": [], "error": []}
        # If this code is an acause name, use all codes mapped to that acause its sub-causes
        if code in available_cause_list:
            for acause in acause_parent_map.ix[acause_parent_map['parent_acause']==code, 'acause']:
                for c in map_data.ix[map_data['acause']==acause,'value'].drop_duplicates().tolist():
                    c = c.strip()
                    temp["value"].append(c)
                    temp["code_type"].append(code_type)
                    temp["error"].append("")
        else:
            # Check to make sure it's a valid code
            if code in full_code_list:
                temp["value"].append(code)
                temp["code_type"].append(code_type)
                temp["error"].append("")
            else:
                temp["value"].append(code)
                temp["code_type"].append(code_type)
                temp["error"].append("missing")


    # Merge on causes
    temp = pd.DataFrame(temp)
    temp = pd.merge(temp, map_data, on="value", how="left")

    # Tag missing
    temp.ix[temp['acause'].isnull(), 'error'] = "missing"

    # Tag non-garbage in garbage
    temp.ix[(~temp['acause'].isin(['_gc', '_sb']))&(temp['code_type'].isin(['garbage']))&(temp['error']!='missing'), 'error'] = "non_garbage_in_garbage"

    # Generate a list of errors
    error_list = []
    if len(temp.ix[temp['error']=="missing",'value'].drop_duplicates().tolist()) > 0:
        error_list.append(", ".join(temp.ix[temp['error']=="missing",'value'].drop_duplicates().tolist()) + " are missing from the cause list")
    if len(temp.ix[temp['error']=="non_garbage_in_garbage",'value'].drop_duplicates().tolist()) > 0:
        error_list.append(", ".join(temp.ix[temp['error']=="non_garbage_in_garbage",'value'].drop_duplicates().tolist()) + " are not garbage codes")
    if len(temp.ix[temp['error']=="incorrect_format_range",'value'].drop_duplicates().tolist()) > 0:
        error_list.append(", ".join(temp.ix[temp['error']=="incorrect_format_range",'value'].drop_duplicates().tolist()) + " are not formatted properly for a range")

    # Return result
    if len(error_list) > 0:
        return False, ", ".join(error_list)
    else:
        if code_type in ['except']:
            return True, list(set(temp.ix[temp['code_type'].isin([code_type]),'value'].drop_duplicates().tolist()))
        else:
            return True, list(set(list(set(temp.ix[temp['code_type'].isin([code_type]),'value'].drop_duplicates().tolist()) - set(temp.ix[temp['code_type'].isin(['except']),'value'].drop_duplicates().tolist()))))

# Get cause map
cause_data = get_data("causes").ix[:, ['cause_id', 'acause']]

# Get cause hierarchy
causehierarchy_data = get_data("causehierarchy").ix[:, ['cause_set_version_id', 'cause_id', 'parent_id', 'path_to_top_parent']]
causehierarchy_data = causehierarchy_data.ix[causehierarchy_data['cause_set_version_id']==cause_set_version_id]

# Generate acause parent map
acause_parent_map = create_acause_parent_map(cause_data, causehierarchy_data)
for x in xrange(0,101):
    acause_parent_map = acause_parent_map.ix[(acause_parent_map['acause']!='N{}'.format(x))&(acause_parent_map['acause']!='NL{}'.format(x))]
acause_parent_map = acause_parent_map.ix[(acause_parent_map['acause']!='_sb')]

# Get code system data (set remove_decimal and contains_ranges values)
codesystem_data = get_data('code_system/{}'.format(code_system_id))
remove_decimal = bool(codesystem_data.ix[0, 'remove_decimal'])
contains_ranges = bool(codesystem_data.ix[0, 'contains_ranges'])

# Pull code list
code_data = get_data("code_system/{}/codes/".format(code_system_id))

# Get cscsmt_id for source
cscsmt_df = get_data("cscsmt/")
cscsmt_id = int(cscsmt_df.ix[(cscsmt_df['code_system_id']==code_system_id)&(cscsmt_df['map_type_id']==map_type_id)&(cscsmt_df['cause_set_version_id']==cause_set_version_id), 'cscsmt_id'])

# Get maps for source
map_data = get_data("cscsmt/{}/maps".format(cscsmt_id))

# Merge on causes
map_data = pd.merge(map_data, cause_data, on='cause_id')

# Merge on code information to make final map
map_data = pd.merge(map_data, code_data, on='code_id')
map_data = map_data.ix[:, ['code_id', 'sort', 'value', 'acause']].sort('sort').reset_index(drop=True)
map_data['garbage'] = 0
map_data.ix[map_data['acause'].isin(['_gc']), 'garbage'] = 1

# Make folder
os.system('mkdir {}/{}'.format(output_folder, int(package_set_id)))

# Get package list
package_data = get_data("package_set/{}/packages".format(package_set_id))
package_data = package_data.sort('sequence').reset_index(drop=True)
package_list = []
for i in package_data.index:
    package_list.append('package_{}'.format(int(package_data.ix[i, 'package_id'])))
write_json(package_list, output_folder+'/{}/_package_list.json'.format(int(package_set_id)))
package_data = package_data.set_index('package_id')

for package_id in package_data.index:
    print "Generating package {}".format(package_id)
    fail_flag = False
    """ Start package export """
    # General package info
    package = {}
    package["package_name"] = package_data.ix[package_id, 'package_name']
    if package_data.ix[package_id, 'package_type_id'] == 1:
        package["create_targets"] = 0
    else:
        package["create_targets"] = 1

    """ Format garbage codes for package """
    # Get garbage data
    package["garbage_codes"] = []
    garbage_data = get_data("package/{}/garbage".format(package_id))
    for garbage_codes in garbage_data['garbage_codes'].drop_duplicates():
        result = validate_code(garbage_codes, map_data, acause_parent_map, "garbage", contains_ranges)
        if result[0]:
            for r in result[1]:
                if remove_decimal:
                    r = r.replace('.','')
                package["garbage_codes"].append(r)
        else:
            print "Bad garbage: {}".format(garbage_codes)
            fail_flag = True

    """ Prep weights """
    if pd.notnull(package_data.ix[package_id, 'computed_package_id']):
        computed_package_id = int(package_data.ix[package_id, 'computed_package_id'])
        computedpackageversion_data = get_data('computed_package_version')
        computed_package_version_id = int(computedpackageversion_data.ix[(computedpackageversion_data['is_best']==True)&(computedpackageversion_data['computed_package_id']==computed_package_id), 'computed_package_version_id'])
        # wgts_full_df = get_data('computed_package_version/{}/wgt'.format(computed_package_version_id))
        sql_statement = "SELECT computed_package_wgt_id, computed_group_id_id as computed_group_id, computed_wgt_group_id_id as computed_wgt_group_id, wgt FROM engine_room.rdp_computedwgt w JOIN engine_room.rdp_computedwgtgroup wg WHERE w.computed_wgt_group_id_id = wg.computed_wgt_group_id AND computed_package_version_id_id = {};".format(computed_package_version_id)
        wgts_full_df = run_query(sql_statement)
        wgts_full_df = wgts_full_df.rename(columns={'computed_group_id':'group_id', 'computed_package_wgt_id':'package_wgt_id', 'computed_wgt_group_id':'wgt_group_id'})
    else:
        wgts_full_df = get_data('package/{}/wgt'.format(package_id))
        wgts_full_df['wgt'] = wgts_full_df['wgt'].map(lambda x: str(x))
    # If the weights don't add up to 1, set fail_flag to True
    if int(package_data.ix[package_id, 'package_type_id'])!=4:
        temp = wgts_full_df[['wgt_group_id', 'wgt']].copy(deep=True)
        temp['wgt'] = temp['wgt'].astype('float64')
        temp = temp.groupby('wgt_group_id').sum().reset_index()
        if len(temp.ix[(temp['wgt']>1.000001)|(temp['wgt']<0.999999)]) > 0:
            fail_flag = True
            print "Wgt violations:", len(temp.ix[(temp['wgt']>1.000001)|(temp['wgt']<0.999999)])
            print temp.ix[(temp['wgt']>1.000001)|(temp['wgt']<0.999999)]

    """ Format target codes for package """
    package["target_groups"] = []
    # Get cause groups
    if pd.notnull(package_data.ix[package_id, 'computed_package_id']):
        causegroup_data = get_data('computed_package_version/{}/cause_group'.format(computed_package_version_id))
        causegroup_data = causegroup_data.rename(columns={'computed_group_id':'group_id'})
    else:
        causegroup_data = get_data('package/{}/cause_group'.format(package_id))
    causegroup_data = causegroup_data.sort('group_id').reset_index(drop=True)
    causegroup_data['group'] = causegroup_data.index + 1
    for group_id in causegroup_data['group_id'].drop_duplicates():
        temp_package = {'target_codes':[]}
        # Pull target codes
        if pd.notnull(package_data.ix[package_id, 'computed_package_id']):
            target_data = get_data('computed_cause_group/{}/target'.format(group_id))
        else:
            target_data = get_data('cause_group/{}/target'.format(group_id))
        for target_codes in target_data['target_codes'].drop_duplicates():
            result = validate_code(target_codes, map_data, acause_parent_map, "target", contains_ranges)
            if result[0]:
                for r in result[1]:
                    if remove_decimal:
                        r = r.replace('.','')
                    temp_package["target_codes"].append(r)
            else:
                print "Bad target: {}".format(target_codes)
                fail_flag = True
        # Pull except codes
        if pd.notnull(package_data.ix[package_id, 'computed_package_id']):
            except_data = []
        else:
             except_data = get_data('cause_group/{}/except'.format(group_id))
        if len(except_data) > 0:
            temp_package["except_codes"] = []
            for except_codes in except_data['except_codes'].drop_duplicates():
                result = validate_code(except_codes, map_data, acause_parent_map, "except", contains_ranges)
                if result[0]:
                    for r in result[1]:
                        if remove_decimal:
                            r = r.replace('.','')
                        temp_package["except_codes"].append(r)
                else:
                    print "Bad except: {}".format(except_codes)
                    fail_flag = True
        # Pull weights
        wgts_df = wgts_full_df.copy(deep=True)
        wgts_df = wgts_df.ix[wgts_df['group_id']==group_id].sort('wgt_group_id').reset_index(drop=True)
        temp_package["weights"] = wgts_df.ix[:, 'wgt'].tolist()
        package["target_groups"].append(temp_package)

    """ Prep weight groups """
    package["weight_groups"] = []
    if pd.notnull(package_data.ix[package_id, 'computed_package_id']):
        # packagewgtgroup_df = get_data('computed_package_version/{}/wgt_group'.format(computed_package_version_id))
        sql_statement = "SELECT computed_wgt_group_id, wgt_group_name, wgt_group_logic, computed_package_version_id_id as computed_package_version_id FROM engine_room.rdp_computedwgtgroup WHERE computed_package_version_id_id = {}".format(computed_package_version_id)
        packagewgtgroup_df = run_query(sql_statement)
        packagewgtgroup_df = packagewgtgroup_df.rename(columns={'computed_package_version_id': 'package_id', 'computed_wgt_group_id': 'wgt_group_id'})
        
        sql_statement = "SELECT computed_wgt_group_logic_set_id, computed_wgt_group_id_id as computed_wgt_group_id FROM engine_room.rdp_computedwgtgrouplogicset wgls JOIN engine_room.rdp_computedwgtgroup wg WHERE wgls.computed_wgt_group_id_id = wg.computed_wgt_group_id AND computed_package_version_id_id = {}".format(computed_package_version_id)
        wgtgrouplogicset_data_all = run_query(sql_statement)
        
        sql_statement = "SELECT computed_wgt_group_logic_id, variable, operator, value, computed_wgt_group_logic_set_id_id as computed_wgt_group_logic_set_id FROM engine_room.rdp_computedwgtgrouplogic wgl JOIN engine_room.rdp_computedwgtgrouplogicset wgls JOIN engine_room.rdp_computedwgtgroup wg WHERE wgl.computed_wgt_group_logic_set_id_id = wgls.computed_wgt_group_logic_set_id AND wgls.computed_wgt_group_id_id = wg.computed_wgt_group_id AND computed_package_version_id_id = {}".format(computed_package_version_id)
        wgtgrouplogic_data_all = run_query(sql_statement)
    else:
        packagewgtgroup_df = get_data('package/{}/wgt_group'.format(package_id))
    packagewgtgroup_df = packagewgtgroup_df.sort('wgt_group_id').reset_index(drop=True)
    # Get weight group logic
    for wgt_group_id in packagewgtgroup_df['wgt_group_id'].drop_duplicates():
        wgt_group_parameters = []
        if pd.notnull(package_data.ix[package_id, 'computed_package_id']):
            # wgtgrouplogicset_data = get_data('computed_wgt_group/{}/wgt_group_logic_set'.format(wgt_group_id))
            wgtgrouplogicset_data = wgtgrouplogicset_data_all.ix[wgtgrouplogicset_data_all['computed_wgt_group_id']==wgt_group_id].copy(deep=True)
            wgtgrouplogicset_data = wgtgrouplogicset_data.rename(columns={'computed_wgt_group_logic_set_id': 'wgt_group_logic_set_id', 'computed_wgt_group_id': 'wgt_group_id'})
        else:
            wgtgrouplogicset_data = get_data('wgt_group/{}/wgt_group_logic_set'.format(wgt_group_id))
        for wgt_group_logic_set_id in wgtgrouplogicset_data['wgt_group_logic_set_id'].drop_duplicates():
            temp = []
            if pd.notnull(package_data.ix[package_id, 'computed_package_id']):
                 # wgtgrouplogic_data = get_data('computed_wgt_group_logic_set/{}/wgt_group_logic'.format(wgt_group_logic_set_id))
                 wgtgrouplogic_data = wgtgrouplogic_data_all.ix[wgtgrouplogic_data_all['computed_wgt_group_logic_set_id']==wgt_group_logic_set_id].copy(deep=True)
            else:
                wgtgrouplogic_data = get_data('wgt_group_logic_set/{}/wgt_group_logic'.format(wgt_group_logic_set_id))
            for i in wgtgrouplogic_data.index:
                temp1 = []
                for c in ['variable', 'operator']:
                    temp1.append(wgtgrouplogic_data.ix[i, c])
                if wgtgrouplogic_data.ix[i, 'variable'] in ['year', 'sex', 'age']:
                    temp1.append(wgtgrouplogic_data.ix[i, 'value'])
                else:
                    d = str(wgtgrouplogic_data.ix[i, 'value'])
                    d = d.replace("'", "")
                    temp1.append("'" + d + "'")
                temp.append('(' + ' '.join(temp1) + ')')
            wgt_group_parameters.append('(' + ' and '.join(temp) + ')')
        packagewgtgroup_df.ix[packagewgtgroup_df['wgt_group_id']==wgt_group_id, 'wgt_group_logic'] = ' or '.join(wgt_group_parameters)
    for i in packagewgtgroup_df.index:
        package["weight_groups"].append({"weight_group_name": packagewgtgroup_df.ix[i, "wgt_group_name"], "weight_group_parameter": packagewgtgroup_df.ix[i, "wgt_group_logic"]})
    # Refactor for GBD-Specific style packages
    if int(package_data.ix[package_id, 'package_type_id'])==4:
        # Set up DataFrames
        new_cg_data = []
        new_target_groups = []
        cg_count = 0
        for cg in package['target_groups']:
            new_cg_data.append(cg['weights'])
        new_cg_data = pd.DataFrame(new_cg_data).fillna(0)
        for column in new_cg_data.columns:
            new_cg_data[column] = new_cg_data[column].map(lambda x: int(float(x)))
        # Create new cause groups
        temp = []
        for column in new_cg_data.columns:
            for cg in new_cg_data.ix[new_cg_data[column]==1].index:
                for tc in package['target_groups'][cg]['target_codes']:
                    temp.append({'type': 'target_codes', 'code': tc, column: 1, 'cg': column})
                if 'except_codes' in package['target_groups'][cg].keys():
                    for ec in package['target_groups'][cg]['except_codes']:
                        temp.append({'type': 'except_codes', 'code': ec, column: 1, 'cg': column})
        new_cg_data = pd.DataFrame(temp).fillna(0)
        # Get the weights for these new cause groups
        for cg in new_cg_data['cg'].drop_duplicates():
            new_target_group = {}
            for t in ['target_codes', 'except_codes']:
                if len(new_cg_data.ix[(new_cg_data['cg']==cg)&(new_cg_data['type']==t), 'code'].drop_duplicates()) > 0:
                    new_target_group[t] = new_cg_data.ix[(new_cg_data['cg']==cg)&(new_cg_data['type']==t), 'code'].drop_duplicates().tolist()
            temp = new_cg_data.drop(['code', 'type'], axis=1).copy(deep=True)

            temp = pd.melt(temp, id_vars=['cg'], var_name='wgt_group', value_name='wgt').drop_duplicates().sort(['cg', 'wgt_group']).reset_index(drop=True)
            new_target_group['weights'] = temp.ix[temp['cg']==cg, 'wgt'].tolist()
            new_target_groups.append(new_target_group)
        package['target_groups'] = new_target_groups
    print fail_flag
    """ Finish and export """
    if fail_flag == True:
        print "There are problems with packages.  Please go back and check"
        BREAK
    write_json(package, '{}/{}/package_{}.json'.format(output_folder, int(package_set_id), int(package_id)))


# Format restrictions for cause map
dimensions_df = get_dimensions_df()

# Manual adjustments - no RDP for malaria causes above age 15
acauses = ['malaria', 'malaria_falciparum', 'malaria_vivax', 'malaria_other']
dimensions_df.ix[dimensions_df['acause'].isin(acauses), 'age_end'] = '10'
# Manual adjustments - no RDP for drugs, certain resp causes, and certain cvd causes in ages under 15
acauses = ['mental_drug', 'mental_drug_opioids', 'mental_drug_cocaine', 'mental_drug_amphet', 'mental_drug_cannabis', 'mental_drug_other']
dimensions_df.ix[dimensions_df['acause'].isin(acauses), 'age_start'] = '15'
acauses = ['resp_copd', 'resp_pneum', 'resp_pneum_silico', 'resp_pneum_asbest', 'resp_pneum_coal', 'resp_pneum_other', 'resp_interstitial']
dimensions_df.ix[dimensions_df['acause'].isin(acauses), 'age_start'] = '15'
acauses = ['cvd_aortic', 'cvd_htn', 'cvd_ihd', 'cvd_stroke', 'cvd_stroke_isch', 'cvd_stroke_cerhem']
dimensions_df.ix[dimensions_df['acause'].isin(acauses), 'age_start'] = '15'
# Manual adjustments - no RDP for Chagas outside of Latin America and Caribbean
dimensions_df.ix[dimensions_df['acause']=='ntd_chagas', 'super_region'] = "'Latin America and Caribbean'"
# Manual adjustments - no RDP on ntd_ebola, neuro_ms, hemog_g6pd, hemog_g6pdtrait, nutrition_iodine
acauses = ['ntd_ebola', 'neuro_ms', 'hemog_g6pd', 'hemog_g6pdtrait', 'nutrition_iodine', 'sids']
dimensions_df.ix[dimensions_df['acause'].isin(acauses), 'super_region'] = "'NONE'"

for i in dimensions_df.index:
    dimensions_df.ix[i,'logic'] = generate_logic_statement(dimensions_df.ix[i].to_dict())
dimensions_df = dimensions_df.ix[:, ['cause_id', 'acause', 'logic']]
map_data = pd.merge(map_data, dimensions_df, on='acause').ix[:, ['value', 'garbage', 'logic']].rename(columns={'value': 'cause', 'logic': 'restrictions'})
if remove_decimal:
    map_data['cause'] = map_data['cause'].map(lambda x: x.replace('.', ''))
map_data.to_csv(output_folder+'/{}/cause_map.csv'.format(int(package_set_id)), index=False)