
# -*- coding: utf-8 -*-
"""
Description: Recalculates data in the cancer prep process to 
            remove subtotals where possible
Input(s): split uid subset of original dataset from run_sr.py
Output(s): recalculated uid subset 
How To Use: Pass dataset_id and data_type_id and uid to main()
Contributors: INDIVIDUAL_NAME
"""

import pandas as pd
import time
import os
import sys
from sys import argv
from warnings import warn

# Import cancer_estimation utilities and modules
from cancer_estimation.py_utils import common_utils as utils
from cancer_estimation._database import cdb_utils as cdb
from cancer_estimation.a_inputs.a_mi_registry.subtotal_recalculation import (
    run_sr,
    code_components as code_components
)
from cancer_estimation.a_inputs.a_mi_registry import  mi_dataset as md
from cancer_estimation.py_utils import data_format_tools as dft


def set_subcauses(recalc_data, subcause_issue_file):
    ''' Re/sets the subcodes that need to be removed from each main code 
        (keys of recalc_data) after each iteration of removing subtotals

        Args:
            recalc_data - dict of dicts, each dict's key is a orig_code in orig
                          data, and each dic
            subcause_issue_file - str, filepath of txt file where overlap issues between
                          codes/code ranges that we can't recalc the subtotal anymore
                          are written to
                          e.g: C85-C89, C86-C90

        Returns: data attached to the codes that are to be removed. 
            If issues are found, saves file containing the data subset 
            with those issues
    '''
    print("setting subcauses...")
    count = 0
    for c1 in recalc_data:
        recalc_data[c1]['subcauses_remaining'] = []
        recalc_data[c1]['main_causes_remaining'] = []

        if (not len(recalc_data[c1]['codes']) or
                not len(recalc_data[c1]['subcodes'])):
            continue

        has_subcause_issue = False
        # Check if any c2 is a subcause of c1
        for c2 in recalc_data:
            # ignore when c2 is equal to the main code c1
            if c2 == c1 or not len(recalc_data[c2]['codes']):
                continue
            # Add c2 if it has not already been removed from c
            if (set(recalc_data[c2]['codes']) <= set(recalc_data[c1]['subcodes'])
                and not any(x in recalc_data[c1]['codes_removed']
                            for x in recalc_data[c2]['codes'])):

REDACTED
                # add in orig_cause C2 into main causes
                recalc_data[c1]['subcauses_remaining'] += recalc_data[c2]['codes']
                recalc_data[c1]['main_causes_remaining'].append(c2)

                # If the added single code contains any nested codes, remove
                #   them (for single code subcauses that are totals of decimal
                #   subcauses):
                if ((len(c2) == 3 or (len(c2) == 5 and "." in c2)) and len(recalc_data[c2]['codes_removed'])):
                    for nested in recalc_data[c2]['codes_removed']:
                        recalc_data[c1]['subcauses_remaining'].append(nested)
                        recalc_data[c1]['main_causes_remaining'].append(nested)
                        recalc_data[c1]['subcodes'].append(nested)
                    print('     {} contains subcauses of {}'.format(c2, c1))
                else:
                    print('     {} contains subcauses of {} ({})'.format(
                        c2, c1, list(recalc_data[c2]['codes'])))

                # Ensure unique entries (probably not necessary)
                recalc_data[c1]['subcauses_remaining'] = list(
                    set(recalc_data[c1]['subcauses_remaining']))

            elif (any(i in recalc_data[c2]['codes']
                      for i in recalc_data[c1]['codes']) and not
                        (set(recalc_data[c2]['codes']) <= set(recalc_data[c1]['codes']))
                        and not (set(recalc_data[c1]['codes']) <= set(recalc_data[c2]['codes']))):
                warn_text = "Error: Overlap detected between {} and {}".format(c1, c2)
                warn(warn_text)
                has_subcause_issue = True
                with open(subcause_issue_file, "a+") as f:
                    f.write(warn_text)
                    f.close()
                
            else:
                pass
            if count == len(recalc_data)-1 and has_subcause_issue == True:
                pass
            count += 1
    return (recalc_data)


def determine_highest_level(recalc_data):
    ''' Figures out what is the highest level 
        (longest number of subcauses still to be removed)
        
        Args:
            recalc_data - dict of dicts, each dict's key is a orig_code in orig
                          data, and each dict

        Returns: the number of members in the longest list of main causes remaing
    '''
    # Set cause level
    highest_level = 0
    for c in recalc_data:
        num_subcause_remaining = len(recalc_data[c]['main_causes_remaining'])
        recalc_data[c]['num_subcause_remaining'] = num_subcause_remaining
        if num_subcause_remaining > highest_level:
            highest_level = num_subcause_remaining
    return(highest_level)


def remove_subcauses(recalc_data, uid, error_file, metric):
    ''' Subtract cause data (children) from subtotals (parents) and updates
        appropriate metrics in our dict, codes, subcodes, causes_removed, 
        main_causes_remaining

        Also flags datasets with negative resulting cases/deaths
        
        Args:
            recalc_data - dict of dicts, each dict's key is a orig_code in orig
                          data, and each dict
            uid - int, indicator of which uid group we are removing subcauses from
            error_file - str, filepath of 
            metric - str, [deaths, cases], indicator of our data metric

        Returns: our updated recalc_data dict with subcauses removed and updated
                metrics (cases or deaths) as well as codes removed and remaining, 
                bool (T or F) subcauses were removed
    '''
    print("removing subcauses...")
    subcauses_removed = False
    negative_data_ok = is_exception(dataset_id, data_type_id)
    for parent_cause in recalc_data:
        # only check possible parent codes that still have subcauses and codes
        if not recalc_data[parent_cause]['num_subcause_remaining']:
            recalc_data[parent_cause]['data'] = recalc_data[parent_cause]['data'].drop_duplicates()
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
                        len(recalc_data[child_cause]['codes']) and 
                  not any(x in recalc_data[parent_cause]['codes_removed']
                          for x in recalc_data[child_cause]['codes'])):
                print("Removing {} from {}".format(child_cause, parent_cause))
                # ensure no duplicates exist in order to subtract child from parent
                # without introducing NAs 
                df_parent = recalc_data[parent_cause]['data'].drop_duplicates().rename(columns={
                    metric:'parent_{}'.format(metric)})
                df_child = recalc_data[child_cause]['data'].drop_duplicates().rename(columns={
                        metric:'child_{}'.format(metric)})
                if (df_parent is None):
                    adjusted_data = recalc_data[parent_cause]['data'].sub(
                        recalc_data[child_cause]['data'])
                else: 
                    tmp_sub_df = pd.merge(df_parent, df_child, on='age')
                    tmp_sub_df[metric] = tmp_sub_df['parent_{}'.format(metric)] - tmp_sub_df['child_{}'.format(metric)]
                    adjusted_data = tmp_sub_df[['age',metric]]

                # this section handles negative values when handling subtotals
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
                adjusted_data[metric][adjusted_data[metric] < 0] = 0
                recalc_data[parent_cause]['data'] = adjusted_data
                # remove child from codes 
                recalc_data[parent_cause]['codes'] = [
                    x for x in recalc_data[parent_cause]['codes']
                    if x not in recalc_data[child_cause]['codes']]
                # remove child from subcodes 
                recalc_data[parent_cause]['subcodes'] = [
                    x for x in recalc_data[parent_cause]['subcodes']
                    if x not in recalc_data[child_cause]['codes']]
                # adding child cause to codes_removed 
                recalc_data[parent_cause]['codes_removed'] += \
                    recalc_data[child_cause]['codes']
REDACTED
                recalc_data[parent_cause]['main_causes_remaining'].remove(child_cause)
                recalc_data[parent_cause]['causes_removed'].append(child_cause)

                subcauses_removed = True

    return (recalc_data, subcauses_removed)


def remove_duplicates(recalc_data, metric):
    ''' Removes duplicates generated by recalculation

        Also flags datasets with negative resulting cases/deaths
        
        Args:
            recalc_data - dict of dicts, each dict's key is a orig_code in orig
                          data, and each dict
            metric - str, [deaths, cases], indicator of our data metric

        Returns: updated recalc_data dict with no duplicates
    '''
    duplicates = {}
    # determine if any causes now contain equivalent codes (redundancy)
    for c1 in recalc_data:
        if not len(recalc_data[c1]['codes']):
            continue
        for c2 in recalc_data:
            if c1 == c2 or not len(recalc_data[c2]['codes']):
                continue
            elif recalc_data[c1]['codes'] == recalc_data[c2]['codes']:
                print('     {} redundant with {} ({}). Removing redundancy.'.format(
                    c1, c2, recalc_data[c1]['codes']))
                redundant_codes = str.join(",", recalc_data[c1]['codes'])
                if redundant_codes not in duplicates:
                    duplicates[redundant_codes] = []
                duplicates[redundant_codes].append(c1)
                duplicates[redundant_codes].append(c2)
                duplicates[redundant_codes] = list(
                    set(duplicates[redundant_codes]))
    # for each redundancy, replace the first of cause with the mean values for
    #   the codes and remove values for the rest
    for redundant_codes in duplicates:
        new_data = recalc_data[duplicates[redundant_codes][0]]['data']
        codes = recalc_data[duplicates[redundant_codes][0]]['codes']
        # add the data for all causes with the redundant code. it will next be
        #   divided by the number of causes to create the mean
        for i in range(1, len(duplicates[redundant_codes])):
            new_data[metric] += recalc_data[duplicates[redundant_codes][i]]['data'][metric]
            # set the metric data for the cause that will not be kept to 0
            recalc_data[duplicates[redundant_codes][i]]['data'][metric] *= 0
            recalc_data[duplicates[redundant_codes][i]]['codes'] = []
            recalc_data[duplicates[redundant_codes][i]]['subcodes'] = []
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
        facilitate mapping, and help with formatting output after subtotals are 
        removed
        
REDACTED
        first num and last num in code_range, then adds all of the between
        
        Args:
            code_range - list, of individual codes

        Returns: list, of range(s) from the individual given codes
    '''
    p = []
    decimal_code_range = []
    has_c_codes = False

    # handles integer code ranges
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

    # handles decimal code ranges
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
REDACTED
            # adding any decimal code ranges properly
            p = add_codes(p, first, last, letter, .1)
    return(sorted(p))


def add_codes(p, first, last, letter, delta):
    ''' Adds all the codes between two numbers in a range and returns as formatted
        code range e.g) C30-C32
        
        Args:
            p - list, to hold the range of codes
            first - int, first number in the code range, e.g) 30
            last - int, last number in the code range, e.g) 32
            letter - str, indicator for if it's a C or D code, e.g) 'C'
            delta - float or int, depending on if it's a decimal or integer range
                                  get each code in range by adding this e.g) 0.1 or 1

        Returns: list, of all codes in given range
    '''
    s_first = str(first) if first >= 10 else '0{}'.format(first)
    s_last = str(last) if last >= 10 else '0{}'.format(last)
    # handing numbers 
    if first != -1 and first != last and last > round(first + delta, 3):
        p.append('{}{}-{}{}'.format(letter, s_first, letter, s_last))
    # handling numbers in between a range
    elif first != -1 and first != last and last == round(first + delta, 3):
        p.append(letter + s_first)
        p.append(letter + s_last)
    elif first != -1:
        p.append(letter + s_first)
    return(p)


def is_exception(dataset_id, data_type_id):
    ''' Determines if dataset is flagged such that negative values are accepted

        Args:
            dataset_id - int, our dataset id num
            data_type_id - int, our data type id (1-mor+inc, 2-inc, 3-mor)

        Returns: boolean of if dataset is exception to negative vals or not
    '''
    db_link = cdb.db_api()
    tbl = db_link.get_table("prep_exception")

    both_dtypes = tbl.loc[tbl['data_type_id'].eq(1), ]
    tbl = tbl.loc[~tbl['data_type_id'].eq(1),]
    both_dtypes['data_type_id'] = 2 
    tbl = tbl.append(both_dtypes) 
    both_dtypes['data_type_id'] = 3 
    tbl = tbl.append(both_dtypes)  
    is_exception = tbl.loc[tbl['dataset_id'].eq(dataset_id) &
                           tbl['data_type_id'].eq(data_type_id) &
                           tbl['prep_exception_type_id'].eq(1) &
                           tbl['processing_status_id'].eq(2),
                           :].any().any()
    return(is_exception)


def run(dataset_id, data_type_id, uid):
    ''' Preps data for recalculation then recalculates as necessary,
        formats then saves output
            
        Args:
            dataset_id - int, our dataset id num
            data_type_id - int, our data type id (1-mor+inc, 2-inc, 3-mor)
            uid - int, indicator of which uid split our dataset is from

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
    error_folder = utils.get_path("mi_input", base_folder='storage')

    subcause_issue_folder = error_folder + "/subcause_issue/"
    negative_data_folder = error_folder + "/negative_data/"
    leftover_values_folder = error_folder + "/leftover_values/"

    utils.ensure_dir(subcause_issue_folder)
    utils.ensure_dir(negative_data_folder)
    utils.ensure_dir(leftover_values_folder)

    subcause_issue_file = '{}/{}_{}_uid_{}.txt'.format(
        subcause_issue_folder, dataset_name, data_type_id, uid)
    exception_file = '{}/{}_{}_uid_{}.csv'.format(
        negative_data_folder, dataset_name, data_type_id, uid)
    leftover_vals_file = '{}/{}_{}_uid_{}.txt'.format(
        leftover_values_folder, dataset_name, data_type_id, uid)
    
    # cleaning our exception file folders
    for d in [subcause_issue_file, exception_file, leftover_vals_file]:
        if os.path.exists(d):
            try:
                os.remove(d)
            except:
                print("Error while deleting file : ", d)
    utils.ensure_dir(error_folder)

    print("    removing subtotals from uid {}...".format(uid))
    # add data for the given uid
    df = pd.read_hdf(input_file, 'results', where='uniqid={u}'.format(u=uid))
    df = df.loc[df['uniqid'].eq(uid), ]

    # Create a list of possible codes so that decimal subcauses are only added
    #   if available
    input_cause_list = sorted(df['orig_cause'].unique().tolist())

    # create a dictionary for codes in the selected uid and attach the uid's
    #   data
    uid_subset = {}
    input_data = {}

    # process decimals first and ranges last to ensure that nested causes are
    # removed
    for i in sorted(df['orig_cause'].unique().tolist()):
        uid_subset[i] = {}
        input_data[i] = {}
        uid_subset[i]['codes'] = []
        uid_subset[i]['subcodes'] = []
        if "-" not in i and "," not in i:
            uid_subset[i]['codes'].append(i)
            # add subcodes to 'subcode' key
            df.loc[df['orig_cause'].eq(i), 'cause'].dropna().unique().tolist()
            for subcode in sorted(df['cause'].where(df['orig_cause'] == i
                                                    ).dropna().unique().tolist()):
                if subcode != i:
                    uid_subset[i]['subcodes'].append(subcode)
            # if none of the subcodes appear in the list, set the cause as a
            #   subcode of itself (prevents the addition of unused decimal
            #   causes)
            # if length of subcodes is 0
            if not len(uid_subset[i]['subcodes']):
                uid_subset[i]['subcodes'] = uid_subset[i]['codes']
REDACTED
            elif (not any('{}.'.format(sub[:3]) in check
                        for check in input_cause_list
                        for sub in uid_subset[i]['subcodes'])):
                uid_subset[i]['codes'] = uid_subset[i]['subcodes']
        else:
            for code in sorted(df['cause'].where(
                    df['orig_cause'].eq(i) & (df['cause'] != df['orig_cause'])).dropna().unique().tolist()):
                uid_subset[i]['codes'].append(code)
                uid_subset[i]['subcodes'].append(code)
        # create other lists associated with the cause and add the metric data
        uid_subset[i]['subcauses_remaining'] = []
        uid_subset[i]['codes_removed'] = []
        uid_subset[i]['causes_removed'] = []
REDACTED
        uid_subset[i]['main_causes_remaining'] = []
        uid_subset[i]['data'] = df.loc[df['orig_cause'].eq(i),
                                       ['age', metric]].drop_duplicates()
        input_data[i]['data'] = uid_subset[i]['data']
        input_data[i]['codes'] = uid_subset[i]['codes']

    # Determine subcauses and highest number of causes remaining (how many
    #   subcauses are contained within each cause)
    uid_set = set_subcauses(uid_subset, subcause_issue_file)

REDACTED
    # remove lowest level codes from parent causes
    # continue removing subcauses until highest_level is 0
    highest_level = determine_highest_level(uid_set)
    while highest_level > 0:
        subcauses_removed = True
        while subcauses_removed:
            uid_set, subcauses_removed = remove_subcauses(
                uid_set, uid, exception_file, metric)
            # remove duplicates
            uid_set = remove_duplicates(uid_set, metric)
            # re-set subcauses and num_subcause_remaining
            uid_set = set_subcauses(
                uid_set, subcause_issue_file)
            print("     subcauses removed.")
            highest_level = determine_highest_level(uid_set)
            print(highest_level)

    # Prepare Output
    print("saving output...")
    output = pd.DataFrame(
        columns=['cause', 'codes_remaining', 'codes_removed', 'age', metric])

REDACTED
    # get all unique codes 
    unique_codes = []
    for u in uid_set.keys():
        unique_codes += uid_set[u]['codes']
        # create unique parent integer codes e.g) C00, C03
        uid_set[u]['unique_codes'] = list(set([c1[:3] for c1 in uid_set[u]['codes']]))
    unique_codes = list(set([c1[:3] for c1 in unique_codes]))
    unique_codes.sort()
    # run code components and get dataframe
    ICD10_code_index = code_components.generate_code_index(unique_codes)
    # For each uniquee ICD code value...
    unique_df = pd.DataFrame({'orig_cause': unique_codes,
                               'coding_system': ['ICD10']*len(unique_codes)})
    
    all_code_df = pd.DataFrame()
    # getting range of decimal codes associated with each
    for i in unique_df.index:
        these_components = code_components.determine_components(unique_df.loc[i],
                                                ICD10_code_index)
        all_code_df = all_code_df.append(these_components)
    # this section will replace decimals with parent integer if appropriate
    for u in uid_set.keys():
        codes = uid_set[u]['unique_codes']

        for code in codes: # loop through parent integer codes
            code_sub = all_code_df.loc[all_code_df['orig_cause'].str.contains(code) 
                                        & ~all_code_df['cause'].eq(code), 'cause'].unique()
            # check to see if all possible decimal subcodes under parent integer code
            # is present in this uid's codes
            if len(list(set(code_sub) - set(uid_set[u]['subcodes']))) == 0:
                # preserve other codes, and remove decimals belonging to above parent integer code
                uid_set[u]['subcodes'] = [c1 for c1 in uid_set[u]['subcodes'] if not(code in c1)]
                uid_set[u]['subcodes'] += [code] # add in parent integer codes
                uid_set[u]['subcodes'].sort()
                uid_set[u]['codes'] = uid_set[u]['subcodes'].copy()
            else:
                uid_set[u]['codes'] = uid_set[u]['subcodes'].copy()

    # this section adjusts the codes remaining to a range if needed and formats 
    # for output 
    for u in uid_set:
        # format cause information
        cause_data = pd.DataFrame(
            columns=['cause', 'codes_remaining', 'codes_removed', 'causes_removed'])
        cause_data.loc[0, ['cause']] = u


        if (not len(uid_set[u]['codes_removed']) or
            ("-" not in u and "," not in u and not len(uid_set[u]['codes_removed'])) or
                set(input_data[u]['codes']) <= set(uid_set[u]['codes'])):
            cause_data.loc[0, ['codes_remaining']] = u
        else:
            cause_data.loc[0, ['codes_remaining']] = ','.join(
                convert_to_range(uid_set[u]['codes']))
        cause_data.loc[0, ['codes_removed']] = ','.join(
            convert_to_range(uid_set[u]['codes_removed']))
        cause_data.loc[0, ['causes_removed']] = ','.join(
            uid_subset[u]['causes_removed'])
        # format output data
        output_data = uid_set[u]['data']
        output_data['cause'] = u
        output_data.drop_duplicates(inplace = True)
        orig_data = input_data[u]['data']
        orig_data = orig_data.rename(
            columns={metric: 'orig_metric_value'})
        orig_data['cause'] = u
        orig_data.drop_duplicates(inplace=True)
        # combine and add to output
        final = pd.merge(output_data, cause_data, on='cause')
        orig_data.index.name = None
        final = pd.merge(final, orig_data, on=['cause', 'age'], how='left')
        output = output.append(final)
    # Create output dataset
    output['uniqid'] = uid
    # look for entries where codes has been removed completely for a cause but 
    # final cases/deaths is not 0
    left_vals = output.loc[(output['codes_remaining'].eq('')) & (output[metric] != 0)]
    if len(left_vals) > 0:
        print("ERROR: Cause(s) removed completely but {} is not 0".format(metric))
        if os.path.exists(leftover_vals_file):
            left_vals.to_csv(leftover_vals_file, header=True)

REDACTED
    # otherwise, remove original metric value from dataset
    output = output.loc[~((output['codes_remaining'].eq('')) & 
                (output[metric] != 0) & 
                (output[metric] != output['orig_metric_value']))]

    # check to make sure no extra ages generated
    assert set(list(output['age'].unique())) == set(list(df['age'].unique())), \
        "Extra new ages inserted in SR process"

    # Update encoding (bug fix to work around pandas to_stata issue)
    output = md.stdz_col_formats(output, additional_float_stubs='uniqid')
    # Export results
    output.to_csv(output_file, index=False)
    print('\n Done!')
    time.sleep(1)
    return(None)


if __name__ == "__main__":
    # read in params file and set args
    if len(sys.argv) > 3:
        dataset_id = int(argv[1])
        data_type_id = int(argv[2])
        uid = int(argv[3])
    else:
        task_id = int(os.environ["SLURM_ARRAY_TASK_ID"])
        param_file = str(argv[1])
        params = pd.read_csv(param_file, index_col=False)
        dataset_id = params.loc[task_id-1, 'dataset_id']
        data_type_id = params.loc[task_id-1, 'data_type_id']
        uid = params.loc[task_id-1, 'uid']
    run(dataset_id, data_type_id, uid)