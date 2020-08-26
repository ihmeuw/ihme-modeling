'''
Description: * Redistributes metric values for observations to which which multiple
            acause values are assigned, outputting data assigned to a single
            gbd_cause-acause pair.
            * Adjusts Kaposi Sarcoma data to account for HIV-attributed cases.
            * Splits non-melanoma skin cancer data proportionately into subcauses
Arguments: 04_age_sex.dta file
Output: .dta file with split ages
'''

# Import libraries
import os
import sys
import pandas as pd
import numpy as np
import argparse
from sys import argv

# Import cancer_estimation utilities and modules
from cancer_estimation.py_utils import (
    common_utils as utils,
    data_format_tools as dft
)
from cancer_estimation.a_inputs.a_mi_registry import (
    mi_dataset as md,
    pipeline_tests as pt
)
from cancer_estimation.a_inputs.a_mi_registry.cause_disaggregation import(
    get_resources as resc,
    cause_disagg_core as core,
    cause_disagg_tests as cd_test
)


def prep_for_disagg(df, uid_cols, metric):
    '''
    '''
    df.loc[df.coding_system != "ICD9_detail", 'coding_system'] = 'ICD10'
    df = dft.collapse(df, by_cols=uid_cols, func='sum', stub=metric)
    return(df)


def main(dataset_id, data_type_id):
    '''
    '''
    # prep_step 5 = cause_disaggregation
    this_dataset = md.MI_Dataset(dataset_id, 5, data_type_id)
    input_data = this_dataset.load_input()
    metric = this_dataset.metric
    uid_cols = md.get_uid_cols(5)
    input_data = input_data.loc[~input_data['age'].isin(
        [26, 3, 4, 5, 6, 91, 92, 93, 94]), :]
    # Format and add observation numbers
    formatted_input = prep_for_disagg(input_data.copy(), uid_cols, metric)
    # Disaggregate
    disaggregated_df = core.disaggregate_acause(formatted_input, this_dataset)
    # update uid columns to account for reshaped acause
    uid_cols = [u for u in uid_cols if 'acause' not in u] + ['acause']
    #
    kaposi_df = core.redist_kaposi(disaggregated_df, metric, uid_cols)
    if data_type_id == 2:
        adjusted_df = core.redist_nmsc_gc(kaposi_df, metric)
    else:
        adjusted_df = kaposi_df
    final_df = core.map_remaining_garbage(adjusted_df, data_type_id)
    # run test functions and save output
    pt.verify_metric_total(input_data, adjusted_df,
                           metric, "cause disaggregation module")
    # collapse to incorperate newly-split data
    output_uids = md.get_uid_cols(6)
    final_df = md.stdz_col_formats(final_df)
    final_df = dft.collapse(final_df,
                            by_cols=output_uids,
                            func='sum',
                            combine_cols=metric
                            )
    # save
    md.complete_prep_step(final_df, this_dataset)
    print("Acause disaggregated")


if __name__ == "__main__":
    dataset_id = int(argv[1])
    data_type_id = int(argv[2])
    main(dataset_id, data_type_id)
