# -*- coding: utf-8 -*-
'''
Description: calculates remainders after removing individual icd codes from code
    ranges
Input(s): cancer registry mortality-incidence data in standardized format
Output(s): dataset with recalculated subtotals
How To Use: Pass dataset_id and data_type_id to main()
'''



import sys
import numpy as np
import pandas as pd
import warnings
from cancer_estimation.a_inputs.a_mortality_incidence import (
    mi_dataset as md,
    cluster_utils_prep as cup
)
from cancer_estimation.a_inputs.a_mortality_incidence.subtotal_recalculation import (
    code_components as code_components,
    remove_subtotals as remove_subtotals
)
from cancer_estimation.utils import (
    data_format_tools as dft,
    common_utils as utils,
    pandas_expansions as pe
)
try:
    # Python3
    from functools import partial
except ImportError:
    # fallback to Python2
    from functools32 import partial


def has_subtotals(df, col_name):
     ''' Returns boolean indicating whether the dataframe contains subtotals in
            the column [col_name]
    '''
    subtot_indicators = [",", "-"]
    has_subtotals = (df.coding_system.eq("ICD10") & 
                     df[col_name].str.contains('|'.join(subtot_indicators)))
    return(df.loc[has_subtotals,:].any().any())


def components_present(df):
    ''' Returns boolean of whether any code-component pairs exist  within
            the dataframe such that data could be recalculated
    '''
    component_codes = df['cause'].str.split(",").to_dict()
    keys = component_codes.keys()
    for k in keys:
        if len(component_codes[k]) > 1:
            c = component_codes.pop(k)
            has_subset = any(set(c2) <= set(c) for key, c2 in component_codes.values)
            # Exit if any subset found
            if has_subset: 
                return(True)
            else:
                pass
            component_codes.append(c)
    return(False)


def remove_allCancer(df):
    ''' Removes entries signifying "all cancers" because they cannot be 
            recalculated 
    '''
    allCancer_entries = ["C00-C43, C45-C97", "C00-43, C45-97",
                         "C00-C80", "C00-C94", "C00-C96", "C00-C97"]
    output = df.loc[~df['cause'].str.contains('|'.join(allCancer_entries)), :]
    return(output)


def get_sr_file(ds_instance, which_file='key', splitNum=None):
    ''' Accepts an MI_Dataset class and returns the file associated with the 
            splitNum
    '''
    dsid = ds_instance.dataset_id
    dtid = ds_instance.data_type_id
    temp_dir = ds_instance.temp_folder
    if which_file == "sr_input":
        this_file = '{}/for_recalculation.h5'.format(temp_dir)
    elif which_file == "split_output":
        this_file = "{}/{}_{}_split{}.csv".format(
            temp_dir, dsid, dtid, splitNum)
    if this_file:
        utils.ensure_dir(this_file)
        return(this_file)


def reformat_input(df, ds_instance):
    ''' Collapse and reshape input data from standardize_format output
    '''
    metric_name = ds_instance.metric
    uid_cols = md.get_uid_cols(2)
    wide_uid_cols = [u for u in uid_cols if 'age' not in u]
    uids_noCause = [u for u in uid_cols if 'cause' not in u]
    df.loc[df.im_frmat.isnull() & df.frmat.isin([9]), 'im_frmat'] = 9
    df = md.stdz_col_formats(df)
    df = dft.collapse(df, by_cols=wide_uid_cols, func='sum', stub=metric_name)
    df = dft.wide_to_long(df, stubnames=metric_name,
                          i=wide_uid_cols, j='age')
    df = df.groupby(uid_cols, as_index=False)[metric_name].sum()
    df[metric_name].fillna(value=0, inplace=True)
    df = md.stdz_col_formats(df)
    df = dft.make_group_id_col(df, uids_noCause, id_col='uniqid')
    return(df)


## #########################
# Define Functions
## ##########################

def submit_sr(calc_df, this_dataset):
    ''' Splits data based on subtotal-recalculation requirement and submits
            jobs as needed to recalculate subtotals. Then returns a re-combined
            dataset with subtotals recalculated
    '''
    def submission_req(df, uid): 
        ''' Returns boolean indicating whether data are to be submitted, 
                qualified by whether subtotals are present and whether any 
                component codes exist that could enable recalculation
        '''
        uid_test = df[df['uniqid'].eq(uid)]
        meets_requirement = bool( has_subtotals(uid_test, 'orig_cause')
                    and components_present(uid_test) )
        return(meets_requirement)

    def output_file_func(id):
        ''' Function fed to get_results relative to the  
        '''
        return(get_sr_file(this_dataset, 'split_output', id[0]))
    
    #
    output_uids = md.get_uid_cols(3)
    metric_name = this_dataset.metric
    job_header = "cnSR_{}_{}".format(dataset_id, data_type_id)
    sr_input_file = get_sr_file(this_dataset, "sr_input")
    worker_script = utils.get_path("subtotal_recalculation_worker",
                                                        process="mi_dataset")
    # convert components to string to enable save in hdf file
    uniqid_map = calc_df[output_uids + ['uniqid', 'orig_cause']
                         ].copy().drop_duplicates()
    submitted_data, unsubmitted_data = cup.split_submission_data(calc_df, 
                                        group_id_col='uniqid',
                                        submission_requirement=submission_req, 
                                        hdf_file=sr_input_file,
                                        regenerate_hdf=False)
    if len(submitted_data) == 0:
        final_results = unsubmitted_data
    else:
        uid_list = submitted_data['uniqid'].unique().tolist()
        sr_jobs = cup.generate_prep_workers(worker_script,
                                    list_of_uids=uid_list,
                                    ds_instance=this_dataset,
                                    job_header=job_header,
                                    is_resubmission=is_resubmission)
        output_files = cup.get_results(sr_jobs, 
                                    output_file_func,
                                    parent_process_name="sr",
                                    noisy_checker=True,
                                    add_resubmission_argument=is_resubmission,
                                    wait_time=5)
        # Re-combine compiled results with the set-aside data, before collapsing
        #   and testing
        results = pe.append_files(output_files)
        results.rename(columns={'cause':'orig_cause','codes_remaining':'cause'},
                       inplace=True)
        results = md.stdz_col_formats(results, additional_float_stubs='uniqid')
        results = results.merge(uniqid_map, how='outer', indicator=True)
        assert results['_merge'].isin(["both", "right_only"]).all(), \
            "Error merging with uids"
        del results['_merge']
        # entries with blank "cause" could not be corrected. replace with the 
        #   original aggregate (will be handled by cause recalculation and rdp).
        results.loc[results['cause'].eq(""), 'cause'] = results['orig_cause']
        #  drop causes that were zeroed in subtotal recalculation 
        results['total'] = results.groupby(output_uids)[metric_name].transform(sum)
        results = results.loc[results['total'].ne(0) &
                                results[metric_name].notnull(), :]
        final_results = results.append(unsubmitted_data)
    # Re-combine with data that were not split
    final_results = dft.collapse(final_results, by_cols=output_uids,
                                    combine_cols=this_dataset.metric)
    return(final_results)


def validate_sr(df, ds_instance):
    ''' Validates result of subtotal-recalculation by testing for negative 
            values. Saves erroneous data to file if present
    '''
    metric = ds_instance.metric
    if df[df[metric] < 0].any().any():
        md.report_error(df[df[metric] < 0], 
                        ds_instance, 
                        error_name="negative_sr_output")
        df.loc[df[metric] < 0, metric] = 0
    return(df)


def main(dataset_id, data_type_id, is_resubmission):
    ''' Tests data for existence of subtotals and recalculates as possible 
            and necessary
    '''
    print(utils.display_timestamp())
    this_dataset = md.MI_Dataset(dataset_id, 2, data_type_id)
    df = reformat_input(this_dataset.load_input(), this_dataset)
    cleaned_input = remove_allCancer(df)
    if not has_subtotals(cleaned_input, 'cause'):
        md.complete_prep_step(cleaned_input, this_dataset)
        print("subtotal_recalculation complete")
        return(None)
    else:
        # Attach cause components to the cleaned input
        calc_df = cleaned_input.rename(columns={'cause': 'orig_cause'})
        sub_causes = code_components.run(calc_df) 
        calc_df = calc_df.merge(sub_causes)
        assert len(calc_df) > len(cleaned_input), \
            "Errorr during merge with subcauses"  
        # Test until a uid is discovered that reqires recalculation
        print("Verifying whether data can be recalculated...")
        any_components = False
        for u in calc_df['uniqid'].unique():
            if components_present(calc_df[calc_df['uniqid'].eq(u)]):
                any_components = True 
                break
            else:
                pass
        # Run recalculation only if necessary. Otherwise, output the cleaned data
        if any_components:
            result_df = submit_sr(calc_df, this_dataset)
            subtotals_recalculated = validate_sr(result_df, this_dataset)
        else:
            print("No recalculation necessary.")
            subtotals_recalculated = cleaned_input
        md.complete_prep_step(subtotals_recalculated, this_dataset)
        print(utils.display_timestamp())
        print("Subtotal_recalculation complete.")
        return(None)


if __name__ == "__main__":
    dataset_id = int(sys.argv[1])
    data_type_id = int(sys.argv[2])
    if len(sys.argv)>= 4:
        is_resubmission = bool(int(sys.argv[3]))
    else:
        is_resubmission = False
    main(dataset_id, data_type_id, is_resubmission)


