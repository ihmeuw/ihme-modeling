# -*- coding: utf-8 -*-

'''
Description: Maps input causes to gbd_cause codes, then adjusts garbage mapping and
        applies restrictions
How To Use: Pass dataset_id and data_type_id to main()
'''


import sys
import pandas as pd
import mapping_tests as mt
from a_inputs.a_mortality_incidence import mi_dataset as md
from _database import cdb_utils
from utils import (
    data_format_tools as dft,
    common_utils as utils,
    test_utilities as tests
)
from registry_pipeline import (
    icd_codes as icd,
    cause_mapping as cm 
)



def map_data(input_df, this_dataset):
    ''' Returns the input dataframe with attached cause mapping
        Process:
            1) Declares mapping order, then applies maps in that order
                NOTE: there will be no need to iterate this step once 
                    custom codes are mapped at the input stage
            2) Verifies map application 
    '''
    print("Applying maps...")
    # Define the order in which maps will be merged with the data
    map_order = {1: 'dataset_id', 2: 'country_id', 3: 'coding_system'}

    # Define the order in which maps will be merged with the data
    merge_cols = ['coding_system', 'cause', 'cause_name']
    uid_cols = [c for c in md.get_uid_cols(3) if c not in merge_cols]
    # Prepare for merge with cause map
    unmapped_df = md.add_location_ids(input_df)
    unmapped_df['dataset_id'] = this_dataset.dataset_id
    # Generate inputs for iterative mapping
    cause_map = cm.load_cause_map(this_dataset.data_type_id)
    map_cols = ['coding_system', 'cause', 'cause_name', 'gbd_cause'] +\
                [a for a in cause_map.columns if 'acause' in a]
    mapped_df = pd.DataFrame()
    # Apply maps for CUSTOM coding systems.
    #   Note that this should run once with only 'coding_system' used to merge
    for o in map_order.values():
        # Ensure that each column appears only once in the list
        subset_cols = list(set(map_cols + [o]))  
        subset_entries =(cause_map.coding_system.str.contains("CUSTOM") &
                        ~pd.isnull(cause_map[o]) & (cause_map[o] != 0))
        map_subset = cause_map.loc[subset_entries,subset_cols] 
        if len(map_subset) > 0:
            # Note: index is reset and then set to preserve index values of the 
            #   unmapped_df dataframe in the newly_mapped dataframe
            newly_mapped = unmapped_df.reset_index().merge(map_subset,
                                             how='inner',
                                             on=merge_cols + [o]).set_index('index')
            mapped_df = mapped_df.append(newly_mapped)
            unmapped_df.drop(list(newly_mapped.index), axis=0, inplace=True)
            cause_map.drop(list(map_subset.index), axis=0, inplace=True)
    # Apply maps by non-CUSTOM coding_system
    final_map = cause_map[map_cols]
    unmapped_df['test'] = unmapped_df.index
    newly_mapped = unmapped_df.reset_index().merge(final_map,
                                     how='inner',
                                     on=merge_cols).set_index('index')
    mapped_df = mapped_df.append(newly_mapped)
    unmapped_df.drop(list(newly_mapped.index), axis=0, inplace=True)
    # Test output
    assert len(unmapped_df) == 0, \
        "ERROR: could not map all causes. Missing the following: \n{} \n{}".format(
            unmapped_df.cause.unique(), unmapped_df.cause_name.unique()
        )
    mt.validate_mapping(input_df, mapped_df, this_dataset.metric)
    # Replace non-icd causes with "CUSTOM"
    mapped_df.at[~mapped_df.coding_system.isin(
        ["ICD10", "ICD9_detail"]), "coding_system"] = "CUSTOM"
    mapped_df.drop(labels = [ 'country_id', 'dataset_id', 'location_id'], axis=1, inplace=True)
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
    input_data.loc[input_data['im_frmat'].isnull() & input_data['frmat'].eq(9), 
                            'im_frmat'] = 9
    df = md.stdz_col_formats(input_data)
    #  Ensure that there is no "all age" data. 
    df = df.loc[df['age'] != 1, :]
    # Map data and apply restrictions
    mapped_df = map_data(df, this_dataset)
    restricted_df = cm.restrict_causes(mapped_df, 
                                        cause_col='gbd_cause',
                                        data_type_id=data_type_id,
                                        restrict_age=False)
    md.complete_prep_step(restricted_df, this_dataset)
    print("mapping complete.\n")


if __name__ == "__main__":
    dataset_id = sys.argv[1]
    data_type_id = int(sys.argv[2])
    print("Mapping data with the following arguments: {} {}...".format(
        dataset_id, data_type_id))
    main(dataset_id, data_type_id)
