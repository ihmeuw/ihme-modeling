'''
Description: Manages redistribution for a given uid subset of a larger dataset
How To Use: Called by run_redistribution.py
'''
import os
import sys
import pandas as pd
import numpy as np
# Import cancer_estimation utilities and modules
from cancer_estimation.py_utils import (
    common_utils as utils,
    data_format_tools as dft,
    gbd_cancer_tools as gct
)
from cancer_estimation.registry_pipeline import cause_mapping as cm
from cancer_estimation.a_inputs.a_mi_registry import (
    mi_dataset as md,
    pipeline_tests as pt
)
from cancer_estimation.a_inputs.a_mi_registry.redistribution import (
    rdp_core as redistribute,
    run_redistribution as manager
)


def cannot_redistribute(df):
    ''' Returns a mask of entries that cannot be processed by RDP
        Reasons to set data aside:
            - missing location information (location_id or region)
            - tagged as "hiv"
    '''
    # drop if hiv in the cause
    set_aside = (df['acause'].str.contains("hiv") |
                 df.location_id.isnull() |
                 df.region.isnull() | df.region.isin([""])
                 )
    return(set_aside)


def add_location_hierarchy_info(df):
    ''' Returns the dataframe (df) with added location information: region,
            super_region, subnational_status, etc.
        Stops RDP if there is a problem with the location information
    '''
    print("    Adding location information.")
    input_len = len(df)
    # Reformat/convert variables and ages
    loc_info_dir = utils.get_path('mi_dataset_resources', process="mi_dataset")
    loc_info_path = loc_info_dir + '/redistribution/location_hierarchy.dta'
    location_hierarchy = pd.read_stata(loc_info_path)
    location_hierarchy = location_hierarchy[['location_id', 'dev_status',
                                             'super_region', 'region',
                                             'country', 'subnational_level1',
                                             'subnational_level2']]
    df = df.merge(location_hierarchy, how='left', on='location_id')
    assert not df.location_id.isnull().any(), \
        "Cannot redistribute. Unmapped location ids present."
    assert len(df) == input_len, "ERROR: data lost while adding location metadata"
    return(df)


def load_package_set(df):
    ''' loads the package set linked to the coding system
    '''
    code_version = df.coding_system.unique()[0]
    params_dir = utils.get_path('mi_dataset_resources', process="mi_dataset")
    package_path = params_dir + '/redistribution/packagesets_{}.dta'.format(
        code_version)
    package = pd.read_stata(package_path)
    assert len(package) == 1, "Incorrect number of source labels in "\
        "packagesets_{}. Expected 1, got {}. Redistribution failed."\
        .format(code_version, len(package))
    return(package.package_set_id.unique()[0])


def run_rdp_core(df, this_dataset, split_num):
    ''' Returns redistributed dataset
    '''
    # Load package set
    package_id = load_package_set(df)
    # Prepare data for redistribution core
    df.loc[:, 'acause'] = df['acause'].str.replace('.', '')
    # standardizes existance of age_group_id in datasets
    if not("age_group_id" in df.columns) and 'age' in df.columns:
        df = gct.age_group_id_from_cancer_age(df)
    del df['age']
    df = gct.age_group_id_to_fiveYear(df)
    df.rename(columns={this_dataset.metric: 'freq', 'acause': 'cause'},
              inplace=True)
    # Run the redistribution core
    redistributed_df = redistribute.run(df, PACKAGE_MAP=package_id,
                                        TEMP_FOLDER=this_dataset.temp_folder)
    # Revert data from redistribution core
    redistributed_df.rename(columns={'freq': this_dataset.metric,
                                     'cause': 'acause'}, inplace=True)
 
    redistributed_df = gct.fiveYear_to_age_group_id(redistributed_df)
    redistributed_df = gct.cancer_age_from_age_group_id(redistributed_df)
    #
    fmtd_results = update_redistributed_acause(
        redistributed_df, this_dataset, split_num)
    return(fmtd_results)


def update_redistributed_acause(df, ds_instance, split_num):
    ''' Returns dataframe (df) after merging with maps to update cause information
        -- Maps:
            decimal cause map : used to revert cause names to decimal form
            cause map : used to validate output causes
    '''
    metric_name = ds_instance.metric
    output_uids = md.get_uid_cols(7)
    output_uids = ['sex' if x=='sex_id' else x for x in output_uids]
    #

    def manage_rdp_remnants(df, temp_folder, split_num, metric):
        ''' Verifies if any garbage remains after
        '''
        # Get any codes that didn't merge and save them
        rdp_error = ((df['acause'].isnull() | (df['_merge'] == 'left_only'))
                     & df[ds_instance.metric].isin([np.nan, 0]))
        rdp_error_list = sorted(df.loc[rdp_error, 'cause'].unique().tolist())
        if len(rdp_error_list):
            print("The following causes are not in the cause map:")
            print(rdp_error_list)
        return(None)


    # Convert acause back to cancer cause
    code_format_updates = {  # not necessary once rdp uses the map in the cancer db
        'C0': 'C00', 'C1': 'C01', 'C2': 'C02', 'C3': 'C03', 'C4': 'C04', 'C4A': 'C04',
        'C5': 'C05', 'C6': 'C06', 'C7': 'C07', 'C8': 'C08', 'C9': 'C09',
        'neo_other': 'neo_other_cancer'
    }
    for key, value in code_format_updates.items():
        df.loc[df['acause'] == key, 'acause'] = value
    # Merge with cause map
    df.rename(columns={'acause': 'cause'}, inplace=True)    # No map
    cause_map = cm.load_rdp_cause_map(ds_instance.data_type_id)
    df = df.merge(cause_map, how='left', on=[
                  'coding_system', 'cause'], indicator=True)
    # Check that all data were mapped to cause
    manage_rdp_remnants(df, ds_instance.temp_folder, split_num, metric_name)
    # Reformat to merge data with original source
    df = df.loc[:, output_uids + [metric_name]]
    final_df = dft.collapse(df, by_cols=output_uids, stub=metric_name)
    return(final_df)


def save_worker_output(df, ds_instance, split_number):
    ''' Accepts a dataframe, member of ds_instance class, and the split number,
        then saves an output for the split
    '''
    rdp_output = manager.get_rdp_file(
        ds_instance, 'split_output', split_number)
    df.to_csv(rdp_output, index=False)
    print("  finished rdp for this worker")
    return(None)


def main(dataset_id, data_type_id, split_num):
    '''
    '''
    # Load input
    metric_dict = {'2': 'cases', '3': 'deaths'}
    this_dataset = md.MI_Dataset(dataset_id, 6, data_type_id)
    metric_name = this_dataset.metric
    rdp_input = manager.get_rdp_file(this_dataset, 'rdp_input')
    input_data = pd.read_hdf(rdp_input, 'split_{}'.format(split_num))

    # rename sex_id until rdp packages names are updated 
    input_data.rename(columns={'sex_id':'sex'}, inplace=True)

    # Redistribute data where possible
    if not manager.needs_rdp(input_data, this_dataset):
        print("    no redistribution needed for ds {} type {} split {}".format(
            dataset_id, data_type_id, split_num))
        save_worker_output(input_data, this_dataset, split_num)
        return(input_data)
    else:
        print("    redistributing ds {} type {} split {}".format(
            dataset_id, data_type_id, split_num))
        # Add maps to enable RDP
        input_data.rename(columns={'uid': 'split_group'}, inplace=True)
        mapped = add_location_hierarchy_info(input_data)
        # RDP cannot run without location metadata, and should not run for hiv
        #   Set aside those data
        skip_rdp_mask = cannot_redistribute(mapped)
        set_aside = mapped.loc[skip_rdp_mask, input_data.columns.tolist()]
        to_redistribute = mapped.loc[~skip_rdp_mask, :]
        # Redistribute remaining data
        if to_redistribute.any().any():
            rdp_results = run_rdp_core(
                to_redistribute, this_dataset, split_num)
            # Recombine
            if set_aside.any().any():
                rdp_results = rdp_results.append(set_aside, ignore_index=True)
            to_finalize = rdp_results
        else:
            print("    No data to redistribute. Finalizing.")
            to_finalize = input_data.rename(columns={'cause': 'acause'})
        output_cols = md.get_uid_cols(7)
        # rename sex_id until rdp packages get updated 
        output_cols = ['sex' if x == 'sex_id' else x for x in output_cols]

        to_finalize = cm.correct_causes(to_finalize)
        finalized_df = dft.collapse(
            to_finalize, by_cols=output_cols, stub=metric_name)
        # Check totals (note: because of data precision, data before and after
        #   may not be precisely equivalent)
        diff = finalized_df[metric_name].sum() - input_data[metric_name].sum()
        assert abs(diff/input_data[metric_name].sum()) < 5, \
                    "Difference from input after rdp is too large"
        save_worker_output(finalized_df, this_dataset, split_num)
        return(finalized_df)


if __name__ == '__main__':
    ''' Run RDP on data specific to the split_num for the dataset and data_type
    '''
    dsid = sys.argv[1]
    dtid = sys.argv[2]
    split = sys.argv[3]
    main(dataset_id=dsid, data_type_id=dtid, split_num=split)
