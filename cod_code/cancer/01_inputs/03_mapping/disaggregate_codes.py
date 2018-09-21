# Load common cancer paths
import os
import sys
sys.path.append(h + '/cancer_estimation/00_common')
import set_common_roots as common
roots = common.roots
cancer_workspace = roots['cancer_workspace']

# Import additional libraries
import pandas as pd
import time

""" Run Script """
# assign data location and data type. accept arguments if they are sent.
data_set_group = sys.argv[1]
data_folder = sys.argv[2]
data_type = sys.argv[3]

# Import ICD map according to datatype
cause_map = pd.read_stata(
    roots['registry_parameters'] + '/mapping/map_cancer_' + data_type + '.dta')

# Create a dictionary of the cause map
cause_map = cause_map[cause_map.coding_system == 'ICD10']
cause_map = cause_map[cause_map.cause.str.contains(',') == False]
cause_map = cause_map[cause_map.cause.str.contains('-') == False]
cause_map = cause_map[['cause', 'gbd_cause']].set_index('cause')
cause_map.to_dict()

# create a list of the codes present in the input dataset
data_set_codes = pd.read_stata(
    data_folder + '/03_to_be_disaggregated.dta')['cause'].drop_duplicates()
input_codes = tuple(data_set_codes)

# print if troubleshooting
if j == "[PATH]:
    print 'codes to split:'
    print input_codes.unique()
    time.sleep(1)

# Import list of ICD10 codes and define code index
ICD10_code_list = pd.read_csv(
    '{}/02_subtotal_recalculation/list_of_official_NUMERIC_ICD10_codes.csv'.format(roots['registry_parameters']))
ICD10_code_list.sort(['ICD10_code'], inplace=True)
ICD10_code_list = tuple(ICD10_code_list['ICD10_code'])

# Add codes to the code index and attaches a number indicating their order
# Also mark codes as 'viable' if they are in the input dataset or are on the official list of ICD10 codes
code_index = {}
order_num = 1
print "Creating ICD code list..."
for k in ['C', 'D']:
    under_10_alternate = ['00', '01', '02',
                          '03', '04', '05', '06', '07', '08', '09']

    for o in under_10_alternate + range(10, 100):
        kode = '{}{}'.format(k, o)
        code_index[kode] = {}
        code_index[kode]['order'] = order_num
        if kode in ICD10_code_list or kode in input_codes:
            code_index['{}{}'.format(k, o)]['viable'] = True
        else:
            code_index[kode]['viable'] = False
        order_num += 1

        for d in range(0, 10):
            kode = '{}{}.{}'.format(k, o, d)
            code_index[kode] = {}
            code_index[kode]['order'] = order_num
            if kode in ICD10_code_list or kode in input_codes:
                code_index['{}{}.{}'.format(k, o, d)]['viable'] = True
            else:
                code_index[kode]['viable'] = False
            order_num += 1

            for e in range(0, 10):
                kode = '{}{}.{}{}'.format(k, o, d, e)
                code_index[kode] = {}
                code_index[kode]['order'] = order_num
                if kode in ICD10_code_list or kode in input_codes:
                    code_index[kode]['viable'] = True
                else:
                    code_index[kode]['viable'] = False
                order_num += 1

# Create an empty export dataset
export_df = pd.DataFrame(columns=['orig_cause', 'subcauses'])

# Disaggregate
print "Disaggregating..."
code_df = {}
for c in input_codes:
    print c
    code_df[c] = {}
    code_df[c]['orig'] = c
    code_df[c]['intermediate'] = code_df[c]['orig'].split(
        ',').encode('latin-1')
    code_df[c]['final'] = []

    for i in code_df[c]['intermediate']:
        # skip if no range is present
        if not '-' in i:
            code_df[c]['final'].append(i)
            continue

        # if there are more than two codes in a range, break
        if len(i.split('-')) != 2:
            print "ERROR: More or fewer than 2 items when splitting range for code:\n%s" % (i)
            break

        # set the start and stop codes
        range_start_code = i.split('-')[0]
        range_end_code = i.split('-')[1]

        # Compile list of codes within ranges.
        # for simplicity, decimals are only added to ranges if they are included within the range_start_code or range_end_code
        # a test code is within range if its order number is within the order numbers of range_start_code and range_end_code
        # only 'viable' decimal codes (on the official list of ICD10 codes) will be added
        # codes that are at start or end of the range, or non-viable integer codes will also be added (integer codes are often subtotals)
        for test_code in code_index:
            if (("." in c and "." in test_code) or ("." not in c and "." not in test_code)):
                test_code_order = code_index[test_code]['order']
                if test_code_order >= code_index[range_start_code]['order'] and test_code_order <= code_index[range_end_code]['order']:
                    if not ("." in c and not code_index[test_code]['viable']):
                        code_df[c]['final'].append(test_code)

        # Handle all other codes (if nothing has been added, mark the code as itself)
        if code_df[c]['final'] == []:
            code_df[c]['final'].append(i)

    export_df.loc[len(export_df)] = [c, ','.join(code_df[c]['final'])]

# Create output dataset
output = pd.DataFrame(export_df).set_index('cause')
output.drop_duplicates(inplace=True)

# Update encoding (bug fix to work around pandas to_stata issue)
for col in list(output.columns):
    output[col] = output[col].str.encode('latin-1')

# Export
output.to_stata(
    '{}/03_causes_disaggregated_{}.dta'.format(roots['temp_folder'], data_type))

## ###
# END
## ###
