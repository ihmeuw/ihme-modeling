'''
Description: Determines which codes are contained within an aggregate code
Input(s): split uid subset of original dataset from run_sr.py
Output(s): recalculated uid subset 
How To Use: Pass dataset_id and data_type_id and uid to main()
Contributors: USERNAME
"""
'''


import pandas as pd
import copy
from warnings import warn
from sys import argv
from cancer_estimation.py_utils import common_utils as utils
from cancer_estimation.a_inputs.a_mi_registry import mi_dataset as md
import re


def generate_code_index(input_codes, prep_step = 2):
    ''' Returns an index of all possible ICD10 codes with attached number 
            indicating order of appearance and tag for viability. "Viable" tag
            indicates either an official code or an unofficial code that exists 
            in the data

        Args:
            input_codes - list of all original codes from our dataset
            prep_step - indicate the prep step this is generating codes for

        Returns: a dictionary indexed by containing ICD10 codes 
                with subindecies indicating order of appearance and viabilitiy
    '''
    if not isinstance(input_codes, tuple):
        input_codes = tuple(input_codes)
    # Import list of ICD10 codes and define code index
    code_list_path = '{}/mapping/icd_to_gbd_cod_map/{}.csv'.format(
                    utils.get_path(key="mi_dataset_resources", process="mi_dataset"), 
                    "ICD10")
    ICD10_code_list = pd.read_csv(code_list_path)
    ICD10_code_list['icd_code'] = ICD10_code_list['icd_code'].str.replace('=|"|"', '', regex = True)
    ICD10_code_list.sort_values(by=['icd_code'], inplace=True)
    ICD10_code_list = tuple(ICD10_code_list['icd_code'])

    ICD10_code_index = {}
    order_num = 1
    for k in ['C', 'D']:
        under_10_alternate = ['00', '01', '02',
                              '03', '04', '05', '06', '07', '08', '09']
        for o in under_10_alternate + list(range(10, 100)):
            kode = '{}{}'.format(k, o)
            ICD10_code_index[kode] = {}
            ICD10_code_index[kode]['order'] = order_num
            if kode in ICD10_code_list or kode in input_codes:
                ICD10_code_index['{}{}'.format(k, o)]['viable'] = True
            else:
                ICD10_code_index[kode]['viable'] = False
            order_num += 1
            
            for d in range(0, 10):
                kode = '{}{}.{}'.format(k, o, d)
                ICD10_code_index[kode] = {}
                ICD10_code_index[kode]['order'] = order_num
                if kode in ICD10_code_list or kode in input_codes:
                    ICD10_code_index['{}{}.{}'.format(
                        k, o, d)]['viable'] = True
                else:
                    ICD10_code_index[kode]['viable'] = False
                order_num += 1

                for e in range(0, 10):
                    kode = '{}{}.{}{}'.format(k, o, d, e)
                    ICD10_code_index[kode] = {}
                    ICD10_code_index[kode]['order'] = order_num
                    if kode in ICD10_code_list or kode in input_codes:
                        ICD10_code_index[kode]['viable'] = True
                    else:
                        ICD10_code_index[kode]['viable'] = False
                    order_num += 1
    return(ICD10_code_index)


def determine_components(code_series, ICD10_code_index, prep_step = 2):
    ''' Returns the series with an attached list of the subcodes contained 
            within the original cause
        -- Inputs
            code_series : a pandas series containing 'orig_cause' and 
                'coding_system'
            ICD10_code_index : an dictionary indexed by containing ICD10 codes 
                with subindices indicating order of appearance and viabilitiy
                (either the code is official/exists-in-data or it is not)
            prep_step : integer indicating which prep step this is used for, 
                        [2, 3], 
                        2 - subtotal recalc, add in decimals for everything
                        3 - mapping step only add in integer codes for int code 
                            ranges, leave single integer codes alone
    '''
    coding_system = code_series['coding_system']
    orig_cause = code_series['orig_cause']
    cause_dict = {}
    cause_dict['targets'] = {}
    cause_dict['exclude'] = {}
    c_ix = ICD10_code_index
    # Get list of all cancer codes
    if coding_system != "ICD10":
        warn("Skipping ICD9_detail recalculation because we have not yet built"
             "functionality for it.")
        return(pd.DataFrame())

    # Mark any exclusions (excample: "C10-20 excl C15"). Break if there are
    #   multiple 'excl' clauses
    if len(orig_cause.split('excl')) > 2:
        warn("Skipping %s. Too many exclusions to to process" % (orig_cause))
        return(pd.DataFrame())
    elif len(orig_cause.split('excl')) == 2:
        cause_dict['exclude']['orig_cause'] = orig_cause.split('excl')[
            1].replace(' ', '')
        cause_dict['targets']['orig_cause'] = orig_cause.split('excl')[
            0].replace(' ', '')
    else:
        cause_dict['exclude']['orig_cause'] = ""
        cause_dict['targets']['orig_cause'] = orig_cause.split('excl')[
            0].replace(' ', '')

    # Split comma-separated ICD codes/groups into a list of individual
    #   codes/groups
    for t in ['exclude', 'targets']:
        cause_dict[t]['intermediate'] = cause_dict[t]['orig_cause'].split(',')

    # Create a list of possible subcauses
    first_code_letter = cause_dict['targets']['orig_cause'].split(',')[0][:1]
    for t in ['exclude', 'targets']:
        cause_dict[t]['final'] = []
        for c in cause_dict[t]['intermediate']:
            # Handle single integer codes: if an integer with decimal subcodes,
            #   create a list of decimal subcodes. Add only sub-codes with one
            #   decimal place
            if not '.' in c and len(c) == 3:
                for test_code in c_ix:
                    # NOTE: Removing viable check, because we want all possible
                    # decimal codes within an integer code
                    if prep_step == 2:
                        if (c == test_code[:3] and
                                len(test_code) == 5):
                            cause_dict[t]['final'].append(test_code)
                    # only takes viable codes that are integer codes for 
                    # mapping step
                    elif prep_step == 3:
                        if (c == test_code) and c_ix[test_code]['viable']:
                            cause_dict[t]['final'].append(test_code)

            # Handle single decimal codes: if an integer with decimal subcodes,
            # create a list of decimal subcodes
            # Add only sub-codes with two decimal places 
            if '.' in c and not '-' in c and len(c) == 5:
                for test_code in c_ix:
                    # NOTE: Leaving out viable check and order 
                    # because we don't want to omit codes with 
                    # two decimal places
                    if prep_step == 2:
                        if (c == test_code[:5] and
                                len(test_code) == 5 ): 
                            cause_dict[t]['final'].append(test_code)
                    # we only want viable single decimal codes for mapping step
                    if prep_step == 3:
                        if (c == test_code[:5] and
                                len(test_code) == 5) and c_ix[test_code]['viable']: 
                            cause_dict[t]['final'].append(test_code)

            # Handle cause aggregates: create a list of all codes contained
            # within the aggregate
            elif '-' in c:
                # add first letter to beginning of range. if there are no
                # letters, break
                if c[:1] != "C" and c[:1] != "D" and c != '':
                    if first_code_letter == '':
                        raise AssertionError("Start code in " + c +
                                             " does not have C or D in first character")
                    else:
                        c = first_code_letter.append(c)

                # if there are more than two codes in a range, break
                if len(c.split('-')) != 2:
                    raise AssertionError("Number of codes to split does not "
                                         "equal two in range %s" % (c))

                # set the start and stop codes
                range_start_code = c.split('-')[0]
                range_end_code = c.split('-')[1]

                # add the first letter to the end code of the range, if not
                # present. NOTE: assumes that ranges contain only one letter
                # type
                if range_end_code[:1] not in ['C', 'D']:
                    range_end_code = first_code_letter + range_end_code

                # Ensure that range_start and range_end have at least 3
                #  characters
                if len(range_start_code) <= 2:
                    raise AssertionError(
                        "Start code in {} does not have enough characters".format(c))
                if len(range_end_code) == 2:
                    # This likely is a situation like C74-7 meaning C74-C77
                    if int(range_start_code[2:3]) > int(range_end_code[1:2]):
                        raise AssertionError(
                            "End code in {} is less than start code".format(c))
                    range_end_code_tens = int(range_start_code[1:2])
                    range_end_code = range_end_code[:1] + \
                        str(range_end_code_tens) + range_end_code[1:2]
                elif len(range_end_code) < 2:
                    raise AssertionError(
                        "End code in {} is not in readable format".format(c))

                # Compile list of codes within ranges.
                    # for simplicity, decimals are only added to ranges if
                    # they are included within the range_start_code or
                    # range_end_code. A test code is within range if its
                    # order number is within the order numbers of
                    # range_start_code and range_end_code. Only 'viable'
                    # decimal codes (on the official list of ICD10 codes)
                    # will be added. Codes that are at start or end of the
                    # range, or non-viable integer codes will also be added
                    # (integer codes are often subtotals)
                #NOTE: Section changed to reflect decimal breakdown
                for test_code in c_ix:
                    is_decimal = bool("." in test_code)
                    is_decimal_range = bool("." in range_start_code and
                                            "." in range_end_code)

                    # grab numeric parts of range_start and range_end codes, and 
                    # check if current code is in that range
                    start_num = float(re.findall('\d*\.?\d+', range_start_code)[0])
                    # NOTE: for ints, add 1.0, getting codes for 21 to 22.9 if end_num is 22
                    if '.' in range_end_code:
                        end_num = float(re.findall('\d*\.?\d+', range_end_code)[0])
                    else:
                        end_num = float(re.findall('\d*\.?\d+', range_end_code)[0]) + 1.0
                    code_num = float(re.findall('\d*\.?\d+', test_code)[0])

                    # NOTE conditions: if end code is integer, 
                    # e.g: end_code = C59, then end num is 60.0 only compare against < end code num,
                    # else compare normally, e.g: end_code = C59.5
                    in_range = bool((
                        ((code_num >= start_num) and (code_num < end_num)) or 
                        (('.' in range_end_code) and (code_num >= start_num) and (code_num <= end_num))) and
                        (('C' in test_code and 'C' in range_start_code and 'C' in range_end_code) or 
                        ('D' in test_code and 'D' in range_start_code and 'D' in range_end_code)))
                    # add only decimals to tenth place and decimal codes in the range
                    if prep_step == 2:
                        if (((is_decimal and in_range) or 
                            (test_code in [range_start_code, range_end_code] and 
                                                in_range and is_decimal)) and (len(test_code) == 5)):
                            cause_dict[t]['final'].append(test_code)
                    elif prep_step == 3:  # in mapping step only add appropriate codes in range
                        if is_decimal_range:
                            if ((((is_decimal and in_range) or 
                            (test_code in [range_start_code, range_end_code] and 
                                                in_range and is_decimal)) and (len(test_code) == 5)) and
                                                (c_ix[test_code]['viable'])):
                                cause_dict[t]['final'].append(test_code)
                        else: 
                            if((((not(is_decimal) and in_range) or 
                            (test_code in [range_start_code, range_end_code] and 
                                                in_range and not(is_decimal))) and (len(test_code) == 3)) and
                                                (c_ix[test_code]['viable'])):
                                cause_dict[t]['final'].append(test_code)

            # Handle all other codes (if nothing has been added, mark the code
            #   as itself)
            if cause_dict[t]['final'] == []:
                cause_dict[t]['final'].append(c)
            if prep_step == 2:
                cause_dict[t]['final'].append(cause_dict[t]['orig_cause'])
            else:
                pass

    # Remove from final list those codes that were excluded from aggregates
    final_subcodes = copy.deepcopy(cause_dict['targets']['final'])
    for ec in cause_dict['exclude']['final']:
        if ec in cause_dict['targets']['final']:
            final_subcodes.remove(ec)

    components = pd.DataFrame({'orig_cause': orig_cause,
                               'cause': final_subcodes,
                               'coding_system': coding_system})
    return(components)


def run(input_df, prep_step = 2):
    ''' Generates an index of ICD10 codes, then uses that index to determine
            the component codes extant within the data
    '''
    print("Determining code components...")
    export_df = pd.DataFrame()
    ICD10_code_index = generate_code_index(input_df['orig_cause'].unique(),
                                           prep_step = prep_step)
    # Keep only unique values for each original cause in the dataset
    unique_df = input_df.loc[input_df['coding_system'].eq("ICD10"),
                             ['coding_system', 'orig_cause']].drop_duplicates()
    unique_df.sort_values(by=['orig_cause'], inplace=True)
    # For each uniqe ICD code value...
    for i in unique_df.index:
        these_components = determine_components(unique_df.ix[i],
                                                ICD10_code_index,
                                                prep_step = prep_step)
        export_df = export_df.append(these_components)

    export_df.drop_duplicates(inplace=True)
    return(export_df)


if __name__ == "__main__":
    data_folder = argv[1]
    import_df = pd.read_stata(
        '{}/02_code_components_raw.dta'.format(data_folder), encoding='latin-1')
    export_df = run(import_df)
    export_df.to_stata(
        data_folder+'/02_code_components_split.dta', encoding='latin-1')