
"""
Description: Recalculates data in the cancer prep process to remove subtotals where possible
"""

import pandas as pd
import time
import os
from sys import argv
from warnings import warn
# Import cancer_estimation utilities and modules
from cancer_estimation.py_utils import common_utils as utils
from cancer_estimation._database import cdb_utils as cdb
from cancer_estimation.a_inputs.a_mi_registry import mi_dataset as md


def set_subcauses(recalc_data, subcause_issue_file):
    ''' returns data attached to the codes that are to be removed. If issues 
            are found, saves file containing the data subset with those issues
    '''
    print("setting subcauses...")
    for c in recalc_data:
        recalc_data[c]['subcauses_remaining'] = []
        if (not len(recalc_data[c]['codes']) or
                not len(recalc_data[c]['subcodes'])):
            continue

        # Check if any c2 is a subcause of c.
        for c2 in recalc_data:
            if c2 == c or not len(recalc_data[c2]['codes']):
                continue

            # Add c2 if it has not already been removed from c
            if (set(recalc_data[c2]['codes']) <= set(recalc_data[c]['subcodes'])
                and not any(x in recalc_data[c]['codes_removed']
                            for x in recalc_data[c2]['codes'])):
                recalc_data[c]['subcauses_remaining'].append(c2)

                # If the added single code contains any nested codes, remove
                #   them (for single code subcauses that are totals of decimal
                #   subcauses):
                if ((len(c2) == 3 or (len(c2) == 5 and "." in c2)) and
                        len(recalc_data[c2]['codes_removed'])):
                    for nested in recalc_data[c2]['codes_removed']:
                        recalc_data[c]['subcauses_remaining'].append(nested)
                        recalc_data[c]['subcodes'].append(nested)
                    print('     {} contains subcauses of {}'.format(c2, c))
                else:
                    print('     {} contains subcauses of {} ({})'.format(
                        c2, c, list(recalc_data[c2]['codes'])))

                # Ensure unique entries (probably not necessary)
                recalc_data[c]['subcauses_remaining'] = list(
                    set(recalc_data[c]['subcauses_remaining']))

            # Alert of overlaps
            elif (any(i in recalc_data[c2]['codes']
                      for i in recalc_data[c]['codes']) and
                  len(recalc_data[c2]['codes']) < len(recalc_data[c]['codes'])):
                warn_text = "Overlap detected between {} and {}".format(c, c2)
                warn(warn_text)
                with open(subcause_issue_file, "a+") as f:
                    f.write(warn_text)
                    f.close()
            else:
                pass
    return (recalc_data)


def determine_highest_level(recalc_data):
    ''' Returns the number of members in the longest list of subcauses_remaining
    '''
    # Set cause level
    highest_level = 0
    for c in recalc_data:
        num_subcause_remaining = len(recalc_data[c]['subcauses_remaining'])
        recalc_data[c]['num_subcause_remaining'] = num_subcause_remaining
        if num_subcause_remaining > highest_level:
            highest_level = num_subcause_remaining
    return(highest_level)


def remove_subcauses(recalc_data, uid, error_file):
    ''' subtract cause data (children) from subtotals (parents)
    '''
    print("removing subcauses...")
    subcauses_removed = False
    for parent_cause in recalc_data:
        # only check possible parent codes that still have subcauses and codes
        if not recalc_data[parent_cause]['num_subcause_remaining']:
            continue

        # check against only codes with no subcauses and not against itself
        for child_cause in recalc_data:
            if (recalc_data[child_cause]['num_subcause_remaining'] > 0 or
                    parent_cause == child_cause):
                continue

            # if all codes are contained within the codes of the possible parent
            #   and the cause hasn't already been removed from the parent,
            #   subtract the child data from the parent
            elif (set(recalc_data[child_cause]['codes']) <=
                    set(recalc_data[parent_cause]['subcodes']) and
                  not any(x in recalc_data[parent_cause]['codes_removed']
                          for x in recalc_data[child_cause]['codes'])):
                print("Removing {} from {}".format(child_cause, parent_cause))
                adjusted_data = recalc_data[parent_cause]['data'].sub(
                    recalc_data[child_cause]['data'])
                if any(adjusted_data[metric] < 0):
                    print("negative values detected...")
                    negative_vals = adjusted_data.loc[(
                        adjusted_data[metric] < 0), :]
                    negative_vals.loc[:, 'adjusted_cause'] = parent_cause
                    negative_vals.loc[:, 'uid'] = uid
                    if not os.path.exists(error_file):
                        negative_vals.to_csv(error_file, header=True)
                    else:
                        with open(error_file, 'a') as f:
                            negative_vals.to_csv(f, header=False)
                    if not negative_data_ok:
                        print("ERROR: negative values occur when removing" +
                              "{} from {}".format(child_cause, parent_cause))
                        sys.exit(-1)
                adjusted_data[metric][adjusted_data[metric] < 0] = 0

                # remove the child codes from the parent
                recalc_data[parent_cause]['data'] = adjusted_data
                recalc_data[parent_cause]['codes'] = [
                    x for x in recalc_data[parent_cause]['codes']
                    if x not in recalc_data[child_cause]['codes']]
                recalc_data[parent_cause]['subcodes'] = [
                    x for x in recalc_data[parent_cause]['subcodes']
                    if x not in recalc_data[child_cause]['codes']]
                recalc_data[parent_cause]['codes_removed'] += \
                    recalc_data[child_cause]['codes']
                recalc_data[parent_cause]['causes_removed'].append(child_cause)
                subcauses_removed = True

    return (recalc_data, subcauses_removed)


def remove_duplicates(recalc_data):
    ''' remove duplicates generated by recalculation
    '''
    duplicates = {}
    # determine if any causes now contain equivalent codes (redundancy)
    for c in recalc_data:
        if not len(recalc_data[c]['codes']):
            continue
        for c2 in recalc_data:
            if c == c2 or not len(recalc_data[c2]['codes']):
                continue
            elif recalc_data[c]['codes'] == recalc_data[c2]['codes']:
                print('     {} redundant with {} ({}). Removing redundancy.'.format(
                    c, c2, recalc_data[c]['codes']))
                redundant_codes = str.join(",", recalc_data[c]['codes'])
                if redundant_codes not in duplicates:
                    duplicates[redundant_codes] = []
                duplicates[redundant_codes].append(c)
                duplicates[redundant_codes].append(c2)
                duplicates[redundant_codes] = list(
                    set(duplicates[redundant_codes]))
            # else: print('{} NOT redundant with {}'.format(c,c2))
    # for each redundancy, replace the first of cause with the mean values for
    #   the codes and remove values for the rest
    for redundant_codes in duplicates:
        new_data = recalc_data[duplicates[redundant_codes][0]]['data']
        codes = recalc_data[duplicates[redundant_codes][0]]['codes']
        # add the data for all causes with the redundant code. it will next be
        #   divided by the number of causes to create the mean
        for i in range(1, len(duplicates[redundant_codes])):
            new_data = new_data.add(
                recalc_data[duplicates[redundant_codes][i]]['data'])
            # set the metric data for the cause that will not be kept to 0
            recalc_data[duplicates[redundant_codes][i]]['data'][metric] *= 0
            recalc_data[duplicates[redundant_codes][i]]['codes'] = []
            recalc_data[duplicates[redundant_codes]
                        [i]]['codes_removed'] += codes
            if len(recalc_data[duplicates[redundant_codes][0]]['codes_removed']):
                recalc_data[duplicates[redundant_codes][i]
                            ]['causes_removed'].append("remainder of {} ({})".format(
                                duplicates[redundant_codes][0], redundant_codes))
            else:
                recalc_data[duplicates[redundant_codes][i]]['causes_removed'
                                                            ].append(duplicates[redundant_codes][0])
        # divide the data sum by the number of redundant causes to create the mean
        new_data[metric] = new_data[metric].div(
            len(duplicates[redundant_codes]))
        # set the mean as the data for only the first copy of the code
        recalc_data[duplicates[redundant_codes][0]]['data'] = new_data
    return (recalc_data)


def convert_to_range(code_range):
    ''' Re-formats individual codes and returns them as a range of codes to
            facilitate mapping
    '''
    p = []
    decimal_code_range = []
    has_c_codes = False

    for letter in ['C', 'D']:
        last = -2
        first = -1
        for code in sorted(set(code_range)):
            if letter == 'D' and has_c_codes:
                p = add_codes(p, int(first), int(last), 'C', 1)
                has_c_codes = False
            if code[:1] == letter:
                if "." in code:
                    if int(float(code.replace(letter, "")[:2])) == int(last):
                        continue
                    else:
                        decimal_code_range.append(code)
                        continue
                if float(code.replace(letter, "")[:2]) > last + 1:
                    p = add_codes(p, int(first), int(last), letter, 1)
                    first = float(code.replace(letter, "")[:2])
                    if letter == 'C':
                        has_c_codes = True
                last = float(code.replace(letter, "")[:2])
        p = add_codes(p, int(first), int(last), letter, 1)

    if len(decimal_code_range):
        has_c_codes = False
        for letter in ['C', 'D']:
            last = -2
            first = -1
            for code in sorted(set(decimal_code_range)):
                if letter == 'D' and has_c_codes:
                    p = add_codes(p, first, last, 'C', .1)
                    has_c_codes = False
                if code[:1] == letter:
                    if (round(float(code.replace(letter, "")), 3) >
                            round(last+0.1, 3)):
                        p = add_codes(p, first, last, letter, .1)
                        first = float(code.replace(letter, ""))
                        if letter == 'C':
                            has_c_codes = True
                    last = float(code.replace(letter, ""))

        p = add_codes(p, first, last, letter, .1)
    return(sorted(p))


def add_codes(p, first, last, letter, delta):
    ''' Returns a list of all of the codes within a range
    '''
    s_first = str(first) if first >= 10 else '0{}'.format(first)
    s_last = str(last) if last >= 10 else '0{}'.format(last)
    if first != -1 and first != last and last > round(first + delta, 3):
        p.append('{}{}-{}{}'.format(letter, s_first, letter, s_last))
    elif first != -1 and first != last and last == round(first + delta, 3):
        p.append(letter + s_first)
        p.append(letter + s_last)
    elif first != -1:
        p.append(letter + s_first)
    return(p)


def is_exception(dataset_id, data_type_id):
    ''' Determines if dataset is flagged such that negative values are accepted
    '''
    db_link = cdb.db_api()
    tbl = db_link.get_table("prep_exception")
    is_exception = tbl.loc[tbl['dataset_id'].eq(dataset_id) &
                           tbl['data_type_id'].eq(data_type_id) &
                           tbl['prep_exception_type_id'].eq(1) &
                           tbl['processing_status_id'].eq(2),
                           :].any().any()
    return(is_exception)


def run(dataset_id, data_type_id, uid):
    ''' Preps data for recalculation then recalculates as necessary
    '''
    this_dataset = md.MI_Dataset(dataset_id, 2, data_type_id)
    dataset_name = this_dataset.name
    metric = this_dataset.metric
    input_file = run_sr.get_sr_file(this_dataset, "sr_input")
    # Exit if output already exists
    output_file = run_sr.get_sr_file(this_dataset, 'split_output', uid)
    print(output_file)
    if os.path.exists(output_file):
        print("     output file found for uid " + str(uid))
        return(None)
    #
    negative_data_ok = is_exception(dataset_id, data_type_id)
    error_folder = utils.get_path("mi_input", base_folder='j_temp')
    subcause_issue_file = '{}/subcause_issue/{}_{}_uid_{}.txt'.format(
        error_folder, dataset_name, data_type_id, uid)
    exception_file = '{}/negative_data/{}_{}_uid_{}.csv'.format(
        error_folder, dataset_name, data_type_id, uid)
    for d in [subcause_issue_file, exception_file, error_folder]:
        utils.ensure_dir(d)
    #
    print("    removing subtotals from uid {}...".format(uid))
    # add data for the given uid
    df = pd.read_hdf(input_file, 'split_{}'.format(uid))
    # Create a list of possible codes so that decimal subcauses are only added
    #   if available
    input_cause_list = sorted(df['orig_cause'].unique().tolist())
    # create a dictionary for codes in the selected uid and attach the uid's
    #   data
    uid_subset = {}
    input_data = {}
    # process decimals first and ranges last to ensure that nested causes are
    #   removed
    for c in sorted(df['orig_cause'].unique().tolist()):
        uid_subset[c] = {}
        input_data[c] = {}
        uid_subset[c]['codes'] = []
        uid_subset[c]['subcodes'] = []
        if "-" not in c and "," not in c:
            uid_subset[c]['codes'].append(c)
            # add subcodes to 'subcode' key
            df.loc[df['orig_cause'].eq(c), 'cause'].dropna().unique().tolist()
            for subcode in sorted(df['cause'].where(df['orig_cause'] == c
                                                    ).dropna().unique().tolist()):
                if subcode != c:
                    uid_subset[c]['subcodes'].append(subcode)
            # if none of the subcodes appear in the list, set the cause as a
            #   subcode of itself (prevents the addition of unused decimal
            #   causes)
            if not len(uid_subset[c]['subcodes']):
                uid_subset[c]['subcodes'] = uid_subset[c]['codes']
            elif (not any('{}.'.format(sub[:3]) in check
                          for check in input_cause_list
                          for sub in uid_subset[c]['subcodes'])):
                uid_subset[c]['subcodes'] = uid_subset[c]['codes']
        else:
            for code in sorted(df['cause'].where(
                    df['orig_cause'].eq(c)).dropna().unique().tolist()):
                uid_subset[c]['codes'].append(code)
                uid_subset[c]['subcodes'].append(code)
        # create other lists associated with the cause and add the metric data
        uid_subset[c]['subcauses_remaining'] = []
        uid_subset[c]['codes_removed'] = []
        uid_subset[c]['causes_removed'] = []
        uid_subset[c]['data'] = df.loc[df['cause'].eq(c),
                                       ['age', metric]].set_index('age')
        input_data[c]['data'] = uid_subset[c]['data']
        input_data[c]['codes'] = uid_subset[c]['codes']
    # Determine subcauses and highest number of causes remaining (how many
    #   subcauses are contained within each cause)
    uid_set = set_subcauses(uid_subset, subcause_issue_file)
    highest_level = determine_highest_level(uid_set)
    # remove lowest level codes from parent causes
    if highest_level == 0:
        print('     no subcauses present.')
    else:
        subcauses_removed = True
        while subcauses_removed:
            uid_set, subcauses_removed = remove_subcauses(
                uid_set, uid, exception_file)
            # remove duplicates
            uid_set = remove_duplicates(uid_set)
            # re-set subcauses and num_subcause_remaining
            uid_set, highest_level = set_subcauses(
                uid_set, subcause_issue_file,)
            print("     subcauses removed.")
    # Prepare Output
    print("saving output...")
    output = pd.DataFrame(
        columns=['cause', 'codes_remaining', 'codes_removed', 'age', metric])
    for c in uid_set:
        # format cause information
        cause_data = pd.DataFrame(
            columns=['cause', 'codes_remaining', 'codes_removed'])
        cause_data.loc[0, ['cause']] = c
        # if nothing was removed, or there was only a single cause, or all of
        #   the input codes are still present, set the codes remaining as the
        #   cause
        if (not len(uid_set[c]['codes_removed']) or
            ("-" not in c and "," not in c) or
                set(input_data[c]['codes']) <= set(uid_set[c]['codes'])):
            cause_data.loc[0, ['codes_remaining']] = c
        else:
            cause_data.loc[0, ['codes_remaining']] = ','.join(
                convert_to_range(uid_set[c]['codes']))
        cause_data.loc[0, ['codes_removed']] = ','.join(
            convert_to_range(uid_set[c]['codes_removed']))
        # format output data
        output_data = uid_set[c]['data']
        output_data['age'] = output_data.index
        output_data['cause'] = c
        orig_data = input_data[c]['data']
        orig_data['age'] = orig_data.index
        orig_data = orig_data.rename(
            columns={metric: 'orig_metric_value'})
        orig_data['cause'] = c
        # combine and add to output
        final = pd.merge(output_data, cause_data, on='cause')
        final = pd.merge(final, orig_data, on=['cause', 'age'])
        output = output.append(final)
    # Create output dataset
    output['uniqid'] = uid
    # Update encoding (bug fix to work around pandas to_stata issue)
    output = md.stdz_col_formats(output, additional_float_stubs='uniqid')
    # Export results
    output.to_csv(output_file, index=False)
    print('\n Done!')
    time.sleep(1)
    return(None)


if __name__ == "__main__":
    # accept arguments, or assign them none sent
    dataset_id = int(argv[1])
    data_type_id = int(argv[2])
    uid = int(argv[3])
    run(dataset_id, data_type_id, uid)
