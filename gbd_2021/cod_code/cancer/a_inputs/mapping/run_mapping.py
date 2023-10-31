
# -*- coding: utf-8 -*-
'''
Description: Maps input causes to gbd_cause codes, then adjusts garbage mapping and
        applies restrictions
How To Use: Pass dataset_id and data_type_id to main()

Contributors: USERNAME
'''
import os
import sys
import numpy as np
import pandas as pd
import mapping_tests as mt
from cancer_estimation.a_inputs.a_mi_registry import (
    mi_dataset as md,
    pipeline_tests as pt
)

from cancer_estimation.a_inputs.a_mi_registry.resources import map_update
from cancer_estimation._database import cdb_utils
from cancer_estimation.py_utils import (
    data_format_tools as dft,
    common_utils as utils,
    test_utilities as tests
)
from cancer_estimation.registry_pipeline import (
    icd_codes as icd,
    cause_mapping as cm 
)
from cancer_estimation.a_inputs.a_mi_registry.subtotal_recalculation import code_components


def generate_aggregate_subcodes(input_df, uid_cols, metric_name, 
                                dataset_id, data_type_id):
    ''' This function maps our aggregate ICD10 codes to respective GBD 
        causes/garbage codes, (e.g. C10-C20, C22.2-C22.8)
            1) Disaggregates each code range into all possible and viable 
                integer/decimal ICD10 codes based on ICD10 codes present in COD 
                maps and maps them back to original code range
            2) Maps each disaggregated codes from a code range to a GBD cause
                or _gc (garbage code) back to itself
            3) Maps the original code range to the unique GBD causes + garbage codes
                present in the code range and drops mapped disaggregated codes
            4) Formats (reshape) dataset to have causes the code range is mapped to
                to be wide format
            4) Saves the mapping

        Args: 
            input_df - dataframe, 
                        our input dataframe after CUSTOM and ICD10 mapping
            uid_cols - list, 
                        unique cols to merge on
        
        Returns: DataFrame, fully mapped dataset for unmapped aggregate ICD10
    '''
    uid_cols.append("cause")
    non_icd = input_df[input_df['coding_system'] != "ICD10"]

    # this section maps our aggregate code ranges with individual icd codes 
    icd_codes = input_df[(input_df['coding_system'] == "ICD10")]
    icd_codes.loc[icd_codes['cause'].str.contains("-|,"), 'gbd_cause'] = "_gc"
    icd_codes = icd_codes.rename(columns={'cause': 'orig_cause'})
    sub_causes = code_components.run(icd_codes, prep_step = 3) 
    sub_causes = sub_causes[sub_causes['orig_cause'] != sub_causes['cause']]
    icd_codes = icd_codes.merge(sub_causes, how = "left")

    # drop non existing codes if metric is 0
    icd_codes = icd_codes.loc[~((icd_codes['cause'].isnull()) & (icd_codes[metric_name].eq(0)))]
    
    assert not(icd_codes['cause'].isnull().any()), \
        "Missing mappings in code components for following orig causes \
                {}\n".format(icd_codes[icd_codes['cause'].isnull()]['orig_cause'].unique())

    # this section maps to gbd causes when possible
    cod_map = map_update.load_formatted_cod_maps(data_type_id)
    cod_map['acause'] = np.where(cod_map['gbd_cause'].eq("_gc"), 
                                 cod_map['cause'], cod_map['gbd_cause'])
    # map on cause to get gbd causes, drop individual codes
    icd_codes = icd_codes.merge(cod_map[['coding_system', 'cause', 'acause']],
                                how = "left")

    # remove icd code rows not found in icd code list 
    # (if has metric values, validation will catch)
    icd_codes = icd_codes[~icd_codes['acause'].isnull()]

    del icd_codes['cause'] # remove mapped icd 10 code components col
    icd_codes = icd_codes.rename(columns={'acause': 'cause'})
    # keep only unique mapped gbd causes
    icd_codes.drop_duplicates(subset = uid_cols + ['orig_cause'], inplace = True)

    # drop original unmapped acause cols
    acause_cols = [col for col in icd_codes.columns.values if 'acause' in col]
    icd_codes.drop(acause_cols, 
                    axis = 1, inplace = True)
    icd_codes[['location_id', 'country_id']] = [0, 0] # dummy vals
    
    # reshape dataframe to get all acauses mapped to that cause wide
    icd_codes = icd_codes.rename(columns={'cause': 'acause',
                                          'orig_cause' : 'cause'})

    icd_codes['a_id'] = icd_codes.groupby(uid_cols).cumcount() + 1

    # set _gc to gbd cause directly if only one acause present and is gbd
    icd_codes['acause_count'] = icd_codes.groupby(uid_cols)['a_id'].transform('nunique')
    icd_codes['gbd_cause'] = np.where(icd_codes['acause_count'].eq(1) & 
                                      icd_codes['acause'].str.contains("neo_"),
                                      icd_codes['acause'], icd_codes['gbd_cause'])
    del icd_codes['acause_count']
    # reshape acause to wide
    icd_codes = dft.long_to_wide(df = icd_codes, stub = "acause", 
                                    i = uid_cols, j = "a_id")

    # check for > 33 acause cols, set to sub_total to be dropped later
    cur_acause_cols = [col for col in icd_codes.columns if 'acause' in col]
    icd_codes['acause_count'] = icd_codes[cur_acause_cols].count(axis = 1)
    icd_codes['gbd_cause'] = np.where(icd_codes['acause_count'] > 33, 
                                 'sub_total', icd_codes['gbd_cause'])
    icd_codes.loc[icd_codes['acause_count'] > 33, cur_acause_cols] = ''
    icd_codes['acause1'] = np.where(icd_codes['acause_count'] > 33, 
                                 'sub_total', icd_codes['acause1'])
    icd_codes.drop([col for col in acause_cols if col not in cur_acause_cols], 
                    axis = 1, inplace = True)                      

    # generate missing acause cols and fill NA acause cells with ''
    acause_cols = ['acause' + str(n) for n in range(1,34)] # max 33 acause cols
    missing_cols = [col for col in acause_cols if not(col in icd_codes.columns)]
    if len(missing_cols) > 0:
        icd_codes = icd_codes.reindex(columns= list(icd_codes.columns) + missing_cols, 
                                      fill_value='')
    # save newly mapped changes to temp file
    newly_mapped = icd_codes[(icd_codes['cause'].str.contains("-|,")) & 
                                (~icd_codes['acause1'].eq(''))]
    newly_mapped['dataset_id'] = dataset_id
    newly_mapped.to_csv("{}/new_map_{}.csv".format(
                            utils.get_path(key="mapping",process = "mi_dataset", 
                                                    base_folder="workspace"),
                            dataset_id))

    all_codes = pd.concat([icd_codes, non_icd])
    pt.verify_metric_total(input_df, all_codes, metric_name, 
                            test_location = "Disaggregating aggregated codes")
    return(all_codes)


def check_duplicates(input_df, check_cols):
    ''' Small validation check for duplicated uid cols in dataset
    '''
    duplicates = input_df[input_df.duplicated(subset = check_cols, keep = "first")]
    assert duplicates.shape[0] == 0, \
        "Duplicated mappings in map. \n \
            Check for duplicated registry_indexes or duplicated cause mappings. \
            \n{}".format(duplicates[check_cols])


def check_unmapped(unmapped_df, dataset_id, check_cols):
    ''' Checking for unmapped rows in dataset and writes the rows to 
        a csv for easy mapping troubleshooting
    '''
    # checking if the unmapped file already exists 
    if not(os.path.exists("{}/unmapped.csv".format(
                    utils.get_path(key="mapping",process = "mi_dataset", 
                                            base_folder="workspace")))):

        current_missing = pd.DataFrame(columns = check_cols)
    else:
        try:
            current_missing = pd.read_csv("{}/unmapped.csv".format(
                    utils.get_path(key="mapping",process = "mi_dataset", 
                                            base_folder="workspace")))
        except pd.errors.EmptyDataError:
            print("Found empty file")
            current_missing = pd.DataFrame(columns = check_cols)

    if dataset_id in current_missing['dataset_id'].unique():
        current_missing = current_missing[~current_missing['dataset_id'].eq(dataset_id)]
    missing_map = unmapped_df[['cause', 'cause_name', 'coding_system']].drop_duplicates().copy()
    missing_map['dataset_id'] = dataset_id
    current_missing = pd.concat([current_missing, missing_map[check_cols]])
    current_missing.to_csv("{}/unmapped.csv".format(
                                utils.get_path(key="mapping",process = "mi_dataset", 
                                            base_folder="workspace")))
    # describe error
    assert len(unmapped_df) == 0, \
        "ERROR: could not map all causes. Missing the following: \ncause: {} \ncause_name: {}".format(
            unmapped_df.cause.unique(), unmapped_df.cause_name.unique()
    )
    
def map_data(input_df, this_dataset):
    ''' Returns the input dataframe with attached cause mapping
        Process:
            1) Declares mapping order, then applies maps in that order
                NOTE: there will be no need to iterate this step once 
                    custom codes are mapped at the input stage
            2) Verifies map application 
    '''
    metric_name = this_dataset.metric
    dataset_id = this_dataset.dataset_id
    data_type_id = this_dataset.data_type_id
    print("Applying maps...")
    # Define the order in which maps will be merged with the data
    map_order = {1: 'dataset_id', 2: 'country_id', 3: 'coding_system'}

    # Define the order in which maps will be merged with the data
    merge_cols = ['coding_system', 'cause', 'cause_name']
    uid_cols = [c for c in md.get_uid_cols(3) if c not in merge_cols]
    # Prepare for merge with cause map
    unmapped_df = md.add_location_ids(input_df)

    check_cols = md.get_uid_cols(3)
    check_duplicates(unmapped_df, check_cols)

    unmapped_df['dataset_id'] = this_dataset.dataset_id
    # Generate inputs for iterative mapping
    cause_map = cm.load_cause_map(this_dataset.data_type_id)
    map_cols = ['coding_system', 'cause', 'cause_name', 'gbd_cause'] +\
                [a for a in cause_map.columns if 'acause' in a]
    mapped_df = pd.DataFrame()

    # Apply maps for CUSTOM coding systems
    #   Note that this should run once with only 'coding_system' used to merge
    for o in map_order.values():
        subset_cols = list(set(map_cols + [o]))  # Ensures that each column appears only once in the list
        subset_entries =(cause_map.coding_system.str.contains("CUSTOM") &
                        ~pd.isnull(cause_map[o]) & (cause_map[o] != 0))
        map_subset = cause_map.loc[subset_entries,subset_cols] 
        if len(map_subset) > 0:
            # Note: index is reset and then set to preserve index values of the unmapped_df dataframe
            #       in the newly_mapped dataframe
            newly_mapped = unmapped_df.reset_index().merge(map_subset,
                                             how='inner',
                                             on=merge_cols + [o]).set_index('index')
            mapped_df = mapped_df.append(newly_mapped)
            unmapped_df.drop(list(newly_mapped.index), axis=0, inplace=True)
            cause_map.drop(list(map_subset.index), axis=0, inplace=True)
    # Apply maps by non-CUSTOM coding_system
    final_map = cause_map[map_cols]

    # capitalizes ICCC3 codes in cancer_map to match the causes
    final_map.loc[final_map['coding_system'] == "ICCC3", 'cause'] = final_map.loc[final_map['coding_system'] == "ICCC3", 'cause'].str.title()
    unmapped_df['test'] = unmapped_df.index

    # mapping codes with our cancer map
    newly_mapped = unmapped_df.reset_index().merge(final_map,
                                     how='inner',
                                     on=merge_cols).set_index('index')
    check_duplicates(newly_mapped, check_cols)
    mapped_df = mapped_df.append(newly_mapped)

    # mapping codes with map again: 
    unmapped_df.drop(list(newly_mapped.index), axis=0, inplace=True)
    # disaggregate only if ICD10 present
    if len(unmapped_df) > 0 and unmapped_df['coding_system'].str.contains('ICD10').any():
        newly_mapped = generate_aggregate_subcodes(unmapped_df, uid_cols, 
                                                    metric_name, dataset_id,
                                                    data_type_id)
        # check for completely not mapped acause cols
        unmapped_df = newly_mapped.loc[newly_mapped['gbd_cause'].isnull()]
        mapped_df = mapped_df.append(newly_mapped.loc[~newly_mapped['gbd_cause'].isnull()])

    # Test output
    if len(unmapped_df) != 0:
        check_unmapped(unmapped_df, this_dataset.dataset_id, 
                    check_cols = ['cause', 'cause_name', 'dataset_id', 'coding_system'])
        
    mt.validate_mapping(input_df, mapped_df, this_dataset.metric)

    # special consideration for Germ cell tumors, trophoblastic tumors, and neoplasms of gonads (ICCC code: X)
    # dropping both sexes for now until we have proportions set up
    mapped_df.loc[(mapped_df['acause1'] == "gender_specific") & (mapped_df['sex_id'] == 2), ['gbd_cause', 'acause1']] = "neo_ovarian"
    mapped_df.loc[(mapped_df['acause1'] == "gender_specific") & (mapped_df['sex_id'] == 1), ['gbd_cause', 'acause1']] = "neo_testicular"
    
    # Replace non-icd causes with "CUSTOM"
    # NOTE: this section should be moved to a different script or deleted altogether
    mapped_df.at[~mapped_df.coding_system.isin(
        ["ICD10", "ICD9_detail"]), "coding_system"] = "CUSTOM"
    mapped_df.drop(labels = ['country_id', 'dataset_id', 'location_id'], 
                    axis=1, inplace=True)
    pt.verify_metric_total(input_df, mapped_df, metric_name, 
                            test_location = "Mapping our data to various coding systems")
    # Return
    return(mapped_df)


def main(dataset_id, data_type_id):
    ''' Maps data with the following steps:
            1) imports input data
            2) runs mapping function
            3) expands icd codes to fill garbage acause(n) where necessary
            4) applies sex and cause restrictions
    '''
    this_dataset = md.MI_Dataset(dataset_id, 3, data_type_id)
    metric_name = this_dataset.metric
    uid_cols = md.get_uid_cols(3)
    input_data = this_dataset.load_input().reset_index(drop=True)
    input_data.loc[input_data['im_frmat_id'].isnull() & input_data['frmat_id'].eq(9), 
                            'im_frmat_id'] = 9

     # special handling of registry indexes for some india datasets
    if 'registry_index' in input_data.columns:
        if "ind" in this_dataset.name or "IND" in this_dataset.name or any('163' in index_id for index_id in list(input_data['registry_index'].unique())):
            # load old registries
            old_reg = pd.read_csv("FILEPATH")
            new_reg = pd.read_csv("FILEPATH")
            ind_subset = input_data[input_data['registry_index'].str.startswith("163.")]
            input_data = input_data[~input_data['registry_index'].str.startswith("163.")]
            ind_subset['registry_save'] = ind_subset['registry_index'].copy()

            test = ind_subset.merge(new_reg[['registry_id', "registry_index"]], how = "left", on = "registry_index")
            test.loc[test['registry_id'].isnull(), 'registry_save'] = test.loc[test['registry_id'].isnull(), 'registry_index']
            del test['registry_index']
            ind_subset = test.merge(old_reg[['registry_id', "registry_index"]], how = "left", on = "registry_id")
            ind_subset.loc[ind_subset['registry_index'].isnull(), 'registry_index'] = ind_subset.loc[ind_subset['registry_index'].isnull(), 'registry_save']
            del ind_subset['registry_id']
            del ind_subset['registry_save']
            input_data = pd.concat([input_data, ind_subset])
    input_data.loc[input_data['registry_index'].eq("163.43898.3"), 'registry_index'] = "163.43898.8"

    df = md.stdz_col_formats(input_data)
    #  Ensure that there is no "all age" data. Remove this line after updating 
    #   the preceeding steps to stop using the old cancer age formats
    df = df.loc[df['age'] != 1, :]
    # Map data and apply restrictions
    mapped_df = map_data(df, this_dataset)
    restricted_df = cm.restrict_causes(mapped_df, 
                                        cause_col='gbd_cause',
                                        data_type_id=data_type_id,
                                        restrict_age=False)
    md.complete_prep_step(restricted_df, input_data, this_dataset)

    # substep between mapping and age sex splitting for parents and subtypes
    print("mapping complete.\n")


if __name__ == "__main__":
    dataset_id = int(sys.argv[1])
    data_type_id = int(sys.argv[2])
    print("Mapping data with the following arguments: {} {}...".format(
        dataset_id, data_type_id))
    main(dataset_id, data_type_id)
