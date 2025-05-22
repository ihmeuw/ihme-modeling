'''
Description: Used to run the redistribution step for the prep pipeline (step 06) for GBD2022+

Input(s): Individual step 05 .dta files from the prep pipeline

Output(s): Individual step 06 .dta files from the prep pipeline

Contributors: INDIVIDUAL_NAME
'''
import sys
import time
import pandas as pd
import numpy as np
import os
import glob
import subprocess
from itertools import product

# Cancer team package import
from cancer_estimation.py_utils import(
    cluster_tools as cluster_tools,
    common_utils as utils,
    gbd_cancer_tools as gct,
)
from cancer_estimation.a_inputs.a_mi_registry import (
    mi_dataset as md,
)

class EmptyDataFrame(Exception):
    "Raised when a dataframe is empty when it shouldn't be"
    pass

class UnequalDataFramesLengthException(Exception):
    "Raised when dataframes are not equal in length"
    pass

class NullValuesExist(Exception):
    "Raised when there are null values present"
    pass

class IncompatibleRDPAgeGroupsExist(Exception):
    "Raised when GBD age_group_ids exist that aren't at a granular level and may present issues with CoD's RDP function properly redistributing data for these rows."
    pass

def load_rdp_cause_map(data_type_id):
    """Loads the cached RDP cause map based on the data_type_id and returns a pd.DataFrame.

    Args:
        data_type_id (int): The data_type_id for the dataset to determine which map to load.
                            data_type_ids : [2 : incidence, 3 : mortality]
    Returns:
        df_rdp_cause_map (pd.DataFrame): The dataframe for the respective data_type_id.
    """
    # Load data_type_id specific RDP cause map
    path_rdp_cause_map = utils.get_path(key = 'common', process = 'common')
    file_rdp_cause_map = path_rdp_cause_map + "/rdp_cause_map_data_type_id_{}.csv".format(data_type_id)
    df_rdp_cause_map = pd.read_csv(filepath_or_buffer = file_rdp_cause_map)
    
    return(df_rdp_cause_map)

def needs_rdp(df_input_data, data_type_id):
    """Determines whether a dataset needs to be RDP'd and returns a boolean indicating True / False

    Args:
        df_input_data (pd.DataFrame): A dataframe containing the step 05 data.
        data_type_id (int): A data type id from the data_type table corresponding to the type of data
                               contained within a dataset. For a particular dataset, this is in the dataset table
                               under the "data_type_id" column for a particular row.
    Returns:
        bool_needs_rdp (bool): True / False indicating whether the dataset needs to be ran through CoD's RDP function.

                               If True, need to split the dataset into already mapped GBD causes and non-GBD mapped ICD codesd
                               and then process the non-GBD mapped ICD codes through CoD's RDP function.

                               If False, then we can simply save the dataset as the 06_redistributed_<data_type>.dta file.
    """
    # Load cached RDP cause map based on data_type_id
    df_rdp_cause_map = load_rdp_cause_map(data_type_id)

    # Create list of unique acauses from df_rdp_cause_map
    list_rdp_acauses = df_rdp_cause_map['acause'].unique().tolist()

    # If anything in the 'acause' column exists that isn't in list_rdp_acauses, then we need to 
    # split the dataset by those particular 'acauses' and run RDP on that subset of the dataset.
    bool_needs_rdp = (~df_input_data['acause'].isin(list_rdp_acauses)).any()
    
    return(bool_needs_rdp)

def apply_code_relabeling(df_input_data):
    """ 
    Args:
        df_input_data (pd.DataFrame): The input dataset from step 05.
    Returns:
        df_intput_data (pd.DataFrame): The input dataset from step 05 with particular 'acauses' relabeled / reassigned.
    """
    # Change codes for specific causes that go through RDP for ICD10    
    if 'ICD10' in df_input_data['coding_system'].unique():
        df_input_data.loc[(df_input_data['acause'].eq("neo_leukemia_ll_chronic")) & (df_input_data['coding_system'].eq("ICD10")), 'acause'] = 'C91.1'
        df_input_data.loc[(df_input_data['acause'].eq("neo_lymphoma")) & (df_input_data['coding_system'].eq("ICD10")), 'acause'] = 'C85.9' 
        df_input_data.loc[(df_input_data['acause'].eq("neo_eye")) & (df_input_data['coding_system'].eq("ICD10")), 'acause'] = 'C69'
        df_input_data.loc[(df_input_data['acause'].eq("neo_other_maligt")) & (df_input_data['coding_system'].eq("ICD10")), 'acause'] = 'neo_other_cancer'
        df_input_data.loc[(df_input_data['acause'].eq("neo_neuroblastoma")) & (df_input_data['coding_system'].eq("ICD10")), 'acause'] = 'neo_neuro'
        df_input_data.loc[(df_input_data['acause'].eq("neo_tissues_sarcoma")) & (df_input_data['coding_system'].eq("ICD10")), 'acause'] = 'neo_tissue_sarcoma'
        df_input_data.loc[(df_input_data['acause'].eq("neo_other")) & (df_input_data['coding_system'].eq("ICD10")), 'acause'] = 'neo_other_cancer'
    
    # Change codes for specific causes that go through RDP for ICD9
    if 'ICD9_detail' in df_input_data['coding_system'].unique():
        df_input_data.loc[(df_input_data['acause'].eq("neo_leukemia_ll_chronic")) & (df_input_data['coding_system'].eq("ICD9_detail")), 'acause'] = '204.1'
        df_input_data.loc[(df_input_data['acause'].eq("neo_eye")) & (df_input_data['coding_system'].eq("ICD9_detail")), 'acause'] = '190.9'
        df_input_data.loc[(df_input_data['acause'].eq("neo_lymphoma")) & (df_input_data['coding_system'].eq("ICD9_detail")), 'acause'] = '202.9'
        df_input_data.loc[(df_input_data['acause'].eq("neo_ben_other")) & (df_input_data['coding_system'].eq("ICD9_detail")), 'acause'] = 'neo_other_cancer'
        df_input_data.loc[(df_input_data['acause'].eq("neo_other_maligt")) & (df_input_data['coding_system'].eq("ICD9_detail")), 'acause'] = 'neo_other_cancer'
        df_input_data.loc[(df_input_data['acause'].eq("neo_neuroblastoma")) & (df_input_data['coding_system'].eq("ICD9_detail")), 'acause'] = 'neo_neuro'
        df_input_data.loc[(df_input_data['acause'].eq("neo_tissues_sarcoma")) & (df_input_data['coding_system'].eq("ICD9_detail")), 'acause'] = 'neo_tissue_sarcoma'
        df_input_data.loc[(df_input_data['acause'].eq("neo_other")) & (df_input_data['coding_system'].eq("ICD9_detail")), 'acause'] = 'neo_other_cancer'

    return(df_input_data)

def split_mapped_causes_gbd(df_input_data, data_type_id):
    """Splits the input dataframe into two dataframes:
       -One with acauses that are already mapped to GBD causes based on the RDP cause map
       -One with acauses that are not yet mapped to GBD causes based on the RDP cause map

    Args:
        df_input_data (pd.DataFrame): The input dataset dataframe that needs splitting
    Returns:
        df_gbd_mapped (pd.DataFrame): Dataframe with GBD causes already mapped
        df_non_gbd_mapped (pd.DataFrame): Dataframe with causes not yet mapped to GBD causes which require redistribution
    """
    # Load RDP cause map based on data type id
    df_rdp_cause_map = load_rdp_cause_map(data_type_id = data_type_id)
    
    # Create dataframe split with causes already mapped to GBD causes
    df_gbd_mapped = df_input_data.loc[df_input_data['acause'].isin(df_rdp_cause_map['acause'].unique())].copy()
    
    # Create dataframe split with causes already mapped to GBD causes
    df_non_gbd_mapped = df_input_data.loc[~df_input_data['acause'].isin(df_rdp_cause_map['acause'].unique())].copy()

    # Ensure we haven't dropped any rows when splitting
    if len(df_gbd_mapped) + len(df_non_gbd_mapped) != len(df_input_data):
        raise UnequalDataFramesLengthException("Length of GBD mapped + non-GBD mapped dataframes do not equal length of original dataset! \
                                               Double check split_mapped_causes_gbd() with this dataset for oddities.")
    
    return(df_gbd_mapped, df_non_gbd_mapped)

def split_mapped_causes_icd(df_input_data):
    """Splits the input dataframe into two dataframes:
       -One with ICD9 codes that need RDP
       -One with ICD10 codes that need RDP

    Args:
        df_input_data (pd.DataFrame): The input dataset dataframe with non-GBD mapped acauses that needs splitting into ICD9 and ICD10 dataframes
    Returns:
        df_non_gbd_mapped_icd9 (pd.DataFrame): Dataframe with ICD9 non-GBD mapped acauses
        df_non_gbd_mapped_icd10 (pd.DataFrame): Dataframe with ICD10 non-GBD mapped acauses
    """    
    # Create dataframe split for ICD9 acauses
    df_non_gbd_mapped_icd9 = df_input_data.loc[df_input_data['coding_system'] == 'ICD9_detail'].reset_index()
    
    # Create dataframe split for ICD9 acauses
    df_non_gbd_mapped_icd10 = df_input_data.loc[df_input_data['coding_system'] == 'ICD10'].reset_index()

    # Ensure we haven't dropped any rows when splitting
    if len(df_non_gbd_mapped_icd9) + len(df_non_gbd_mapped_icd10) != len(df_input_data):
        raise UnequalDataFramesLengthException("Length of non-GBD mapped ICD9 & ICD10 dataframes do not equal length of original non-GBD mapped dataset! Double check the split_mapped_causes_icd() function for oddities.")

    return(df_non_gbd_mapped_icd9, df_non_gbd_mapped_icd10)

def assert_age_group_compatibility(df_input_data):
    """Converts Cancer age_group_ids to GBD age_group_ids and asserts that all age_group_ids are compatible with CoD's RDP function.

    Args:
        df_input_data (pd.DataFrame): The dataset's step 05 dataframe
    Returns:
        df_age_checked (pd.DataFrame): Step 05 dataframe with 'age' column converted from cancer age_group_id -> GBD age_group_id
    """
    # retrieve age metadata
    fp_age_metadata = utils.get_path(key = 'common', process = 'common') + "/rdp_get_age_metadata.csv"
    df_age_metadata = pd.read_csv(filepath_or_buffer = fp_age_metadata)

    # convert the 'age' (cancer age_group_id) column in the GBD age group ID column (age_group_id)
    df_age_checked = gct.age_group_id_from_cancer_age(df_input_data)

    # drop 'age' column (cancer age_group_ids) and keep 'age_group_id' (GBD age_group_id)
    df_age_checked.drop(labels = 'age', axis = 1, inplace = True)

    # assert that all age_group_ids exist within df_age_metadata (with the exception of GBD age_group_id = 1)
    age_group_list = np.setdiff1d(ar1 = df_age_checked['age_group_id'].unique().tolist(),
                                   ar2 = df_age_metadata['age_group_id'].unique().tolist()).tolist()

    if age_group_list != [] and age_group_list != [1]:
        raise IncompatibleRDPAgeGroupsExist("Age groups exist that are not at the most granular level! These GBD age groups are present in this dataset: {} \
        Check this dataset's unique list of age group IDs to ensure that aside from the 0-4 age group id (GBD age_group_id = 1), no other aggregate age group ids exist.".format(age_group_list))
    
    return(df_age_checked)

def add_location_id(df_input_data):
    """Adds location_id based off of the 'registry_index' column merged with the SQL 'registry' table.

       Asserts that all 'registry_index's have a matching location_id.

    Args:
        df_input_data (pd.DataFrame): The input dataset dataframe
    Returns:
        df_location_id_added (pd.DataFrame): The input dataset dataframe with the 'location_id' column added
    """
    # Read in cached SQL 'registry' table
    fp_sql_registry = utils.get_path(key = 'common', process = 'common') + "/sql_registry.csv"
    df_sql_registry = pd.read_csv(filepath_or_buffer = fp_sql_registry)
    df_sql_registry = df_sql_registry[['registry_index', 'location_id']]

    # Coerce 'location_id' column to be integer values from float
    df_sql_registry['location_id'] = df_sql_registry['location_id'].astype('Int64')

    # Merge with the input data
    df_location_id_added = df_input_data.merge(right = df_sql_registry,
                                               how = 'left',
                                               on = 'registry_index')

    # assert that all registry_indexes have a matching location_id
    bool_has_missing_location_id = any(pd.isna(df_location_id_added['location_id']))
    if bool_has_missing_location_id:
        print("These registry_indexes are missing a matching location_id from the SQL registry table:")
        print(df_location_id_added.loc[df_location_id_added['location_id'].isna(), 'registry_index'].unique().tolist())
        print("\n")
        raise Exception("Not all registry_indexes have a matching location_id, please check the SQL registry table \
        and ensure that all registry_indexes in this dataset have a matching location_id.")

    return(df_location_id_added)

def add_code_id(df_input_data):
    """Adds in the 'code_id' column from the cached CoD's get_cause_map() function file and merges with the input data
       on the 'acause' column to map the ICD codes that need to be redistributed to the correct code_id value used in CoD's RDP function.

       Asserts that each ICD code must have a corresponding code_id value.

    Args:
        df_input_data (pd.DataFrame): The input data frame
    Returns:
        df_code_id_mapped (pd.DataFrame): The input data frame with the 'code_id' column.
    """
    # Establish dict to determine which cached get_cause_map() file to read in
    DICT_CAUSE_MAP_FILE = {"ICD10" : "rdp_cod_cause_map_icd10.csv",
                           "ICD9_detail" : "rdp_cod_cause_map_icd9.csv"}
    
    # Split dataframe into ICD9 and ICD10 dataframes 
    df_non_gbd_mapped_icd9, df_non_gbd_mapped_icd10 = split_mapped_causes_icd(df_input_data)
    
    # Loop through the ICD9 and ICD10 dataframes
    list_df_code_id_mapped = []

    for df in [df_non_gbd_mapped_icd9, df_non_gbd_mapped_icd10]:
        # Either ICD9 or ICD10 may be empty depending on data frame
        if len(df) == 0:
            pass
        else:
            # Read in the get_cause_map() file corresponding to the ICD version of the dataframe
            dir_cod_cached_files = utils.get_path(key = 'common', process = 'common')
            fn_cause_map = dir_cod_cached_files + "/" + DICT_CAUSE_MAP_FILE[df['coding_system'][0]]

            df_cause_map = pd.read_csv(filepath_or_buffer = fn_cause_map)

            # Merge with df_cause_map to add 'code_id' column
            df_code_id_mapped_icd = df.merge(right = df_cause_map[['value', 'code_id']], 
                                                how = 'left',
                                                left_on = ['acause'],
                                                right_on = ['value'])

            # assert that no new rows were added in merge
            if len(df_code_id_mapped_icd) != len(df):
                raise UnequalDataFramesLengthException("Extra / new rows added when merging with CoD's cause map, please check for erroneous ICD codes in dataset.")

            # assert that all ICD codes have a corresponding code_id
            bool_missing_code_id_match = any(pd.isna(df_code_id_mapped_icd['code_id']))
            if bool_missing_code_id_match:
                print("These ICD codes are missing a code_id match from CoD's cause_map:")
                print(df_code_id_mapped_icd.loc[df_code_id_mapped_icd['code_id'].isna(), 'acause'].unique().tolist())
                print("\n")
                raise NullValuesExist("Not all ICD codes have a matching code_id from CoD's cause map! Please check dataset for odd / incorrect ICD codes.")

            # append to list
            list_df_code_id_mapped.append(df_code_id_mapped_icd)

    # concatenate list of dataframes
    df_code_id_mapped = pd.concat(list_df_code_id_mapped)

    return(df_code_id_mapped)

def remap_secret_causes(df_input_data):
    """
    Args:
        df_input_data (pd.DataFrame): An input dataframe to map secret acauses back to standard GBD causes.
    """
    # 
    REDACTED = ['neo_other_cancer_other', 'neo_colorectal_other', 'neo_anus', 'neo_rectum', 'neo_otherpharynx_other', 'neo_hypopharynx',
                              'neo_mouth_other', 'neo_salivary_gland', 'neo_lip', 'neo_penis', 'neo_vulva', 'neo_vagina']
    
    # Read in flat files needed to map secret causes back to standard public GBD causes
    dir_cod_cached_files = utils.get_path(key = 'common', process = 'common')
    fn_cause_meta_df = dir_cod_cached_files + "/rdp_cod_cause_meta_df.csv"

    df_cause_meta_df = pd.read_csv(filepath_or_buffer = fn_cause_meta_df)

    df_input_data.loc[:,'acause'] = df_input_data['acause'].apply(lambda x: df_cause_meta_df.loc[df_cause_meta_df['acause'] == x, 'acause_parent'].values[0] if x in gbd_2022_secret_causes else x)

    return(df_input_data)

def remove_non_modeled_locations(df_input_data) -> pd.DataFrame:
    """ Drops rows of data containing location_ids that we do not estimate for GBD. 

    Args:
        df_input_data (pd.DataFrame): An input dataframe to drop the rows of data for location_ids that are not modeled by GBD.

    Raises:
        EmptyDataFrame: If the dataset has 0 rows after dropping location_ids that are not modeled by GBD.

    Returns:
        pd.DataFrame: A dataframe with rows of data dropped for location_ids that are not modeled by GBD.
    """
    # List of GBD locations no longer modeled intended to have their rows dropped
    dict_no_longer_gbd_modeled = {'Akrotiri':295,
                                  'Anguilla':299,
                                  'Aruba':300,
                                  'Cayman Islands':313,
                                  'Christmas Island':318,
                                  'Cocos (Keeling) Islands':319,
                                  'Dhekelia':325,
                                  'Faroe Islands':332,
                                  'French Guiana':338,
                                  'French Polynesia':339,
                                  'Gibraltar':345,
                                  'Guadeloupe':350,
                                  'Guernsey':352,
                                  'Isle of Man':355,
                                  'Jersey':356,
                                  'Martinique':363,
                                  'Mayotte':364,
                                  'Midway Islands':366,
                                  'Montserrat':368,
                                  'Netherlands Antilles':370,
                                  'Netherlands Antilles (including Aruba)':371,
                                  'New Caledonia':372,
                                  'Norfolk Island':375,
                                  'Pitcairn Islands':382,
                                  'Reunion':387,
                                  'Saint Barthelemy':391,
                                  'Saint Helena':392,
                                  'Saint Martin':394,
                                  'Saint Pierre and Miquelon':395,
                                  'Sint Maarten':4642,
                                  'Svalbard and Jan Mayen Islands':411,
                                  'Turks and Caicos Islands':415,
                                  'British Virgin Islands':421,
                                  'Wallis and Futuna Islands':423,
                                  'Western Sahara':424} 
    
    list_no_longer_gbd_modeled = list(dict_no_longer_gbd_modeled.values())
    
    # Drop rows of data from input dataframe for rows matching any location_ids in the dict
    df_input_data = df_input_data.loc[~df_input_data['location_id'].isin(list_no_longer_gbd_modeled), ]

    # Check if dataframe is now empty, if empty then raise error
    if len(df_input_data) == 0:
        raise EmptyDataFrame("Dataframe is now empty after dropping location_ids that are no longer modeled by GBD. Consider either re-mapping the location_ids \
                             to valid location_ids for the GBD round, or have a modeler evaluate this dataset to determine how we should process it.")

    return(df_input_data)

def check_rdp_jobs(output_path, output_split_files):
    ''' Checks for completion of RDP jobs and also checks if output csvs are empty or not.

        Args:
            output_path (string): Directory where finished rdp job .csv files are saved
            output_split_files (list): List of output split files that are expected to be processed and created through RDP
    '''
    # Ensure output path directory exists
    try:
        os.mkdir(output_path)
        print("Directory " , output_path,  " Created ") 
    except FileExistsError:
        print("Directory " , output_path,  " already exists")

    rdp_files = []
    loop = 1
    while len(rdp_files) < len(output_split_files):
        rdp_files = [filepath for filepath in output_split_files if filepath is not None and os.path.isfile(filepath)]
        if loop % 5000 == 0:
            print("\nChecking again ... {} files remaining".
                                        format(len(output_split_files) - len(rdp_files)))
            time.sleep(1)
        loop += 1

    time.sleep(5)

    # Once all files exist, read all .csvs into a dataframe
    dfs = [pd.read_csv(f) for f in rdp_files]
    
    # Assert there are no empty .csv files
    empty_files = [df.empty for df in dfs]
    assert not(True in empty_files), \
        "There are empty split rdp files, please try submitting this job again or investigate particular empty split files."
    print("Split RDP .csvs exist and are not empty!")

def submit_rdp(df_input_data, this_dataset, is_resubmission):
    """ Handles submitting non-GBD mapped data from the input dataset to rdp_worker.py which processes the data at a split
        level through CoD's RDP function.
    
    Args:
        df_input_data (pd.DataFrame): A dataframe containing non-GBD mapped acauses that need to be ran through CoD's RDP function.
        this_dataset (md.MI_Dataset()): A MI_Dataset class object containing information about the dataset & data_type_id to be processed 
        is_resubmission (bool): If False cleans the dataset's logs, input, output files during the first time running RDP for this dataset & data_type_id
    """
    def get_rdp_file(ds_instance, which_file='key', splitNum=None, coding_system=None):
        ''' Accepts an MI_Dataset class and returns the appriopriate file name
            depending on rdp process
        '''
        dsid = ds_instance.dataset_id
        dtid = ds_instance.data_type_id
        temp_dir = ds_instance.temp_folder
        if which_file == "rdp_input":
            this_file = "{}/{}_{}.h5".format(temp_dir, dsid, dtid)
        elif which_file == "split_output":
            this_file = "{}/{}_{}_6_split_{}_{}.csv".format(
                temp_dir, dsid, dtid, splitNum, coding_system)
        if this_file:
            utils.ensure_dir(this_file)
            return(this_file)

    def output_file_function(id, coding_system): 
        return get_rdp_file(ds_instance = this_dataset,
                            which_file='split_output', 
                            splitNum=id, 
                            coding_system=coding_system)
    
    def clean_file_dir(path, dataset_id, data_type_id, typeOf = "output", is_resub = False):
        ''' Cleans files in path based on rdp process (logs, input, output)
            if is_resub = False (meaning the first time a dataset is ran through a particular prep step)
        '''

        if not(is_resub):
            print("Cleaning workspace...")
            if typeOf == "output":
                fileList = glob.glob('{}/*{}_{}_6_split*.csv'.format(path, dataset_id, data_type_id))
            elif typeOf == "logs":
                fileList = glob.glob('{}/*'.format(path))
            elif typeOf == "input":
                fileList = glob.glob('{}/{}_{}.h5'.format(path, dataset_id, data_type_id)) + \
                            glob.glob('{}/cnRDP*_{}_*_params.csv'.format(path, data_type_id))

            # Iterate over the list of filepaths & remove each file.
            for filePath in fileList:
                try:
                    os.remove(filePath)
                except:
                    pass

    def expand_grid(dictionary):
        ''' Function to get all combinations of our params job dict
        '''
        return pd.DataFrame([row for row in product(*dictionary.values())], 
                        columns=dictionary.keys())

    # Establish rdp_worker.py path
    rdp_worker_script = utils.get_path("redistribution",
                                       base_folder="code_repo",
                                       process="mi_dataset") + "/rdp_worker.py"

    # Split dataset into ICD9 and ICD10 rows
    df_icd9 = df_input_data.loc[df_input_data['coding_system'] == 'ICD9_detail',].copy().reset_index()
    df_icd10 = df_input_data.loc[df_input_data['coding_system'] == 'ICD10',].copy().reset_index()

    # Split ICD9 and ICD10 dataframes per user defined rows and add split column based on row split length.
    row_split_length = 500

    if len(df_icd9) != 0:
        df_icd9['split'] = (df_icd9.index // row_split_length) + 1
    
    if len(df_icd10) != 0:
        df_icd10['split'] = (df_icd10.index // row_split_length) + 1

    # Save ICD9 and ICD10 dataframes to be read in by rdp_worker.py based on the split number
    coding_system_file = {}
    for coding_system, df_coding_system in {"ICD9_detail" : df_icd9, "ICD10" : df_icd10}.items():
        if len(df_coding_system) == 0:
            print("Coding system: {} has a dataframe length of 0, skipping split dataframe creation for this coding system.".format(coding_system))
            pass
        else:
            fp_df = this_dataset.temp_folder + "/" + str(this_dataset.dataset_id) + "_" + str(this_dataset.data_type_id) + "_6_" + coding_system + '.csv'
            df_coding_system.to_csv(fp_df, index = False)
            coding_system_file[coding_system] = fp_df

    # Determine split output files by split number and ICD coding system type
    output_split_files = []
    for coding_system, df_coding_system in {"ICD9_detail" : df_icd9, "ICD10" : df_icd10}.items():
        if len(df_coding_system) == 0:
            print("Coding system: {} has a dataframe length of 0, skipping split list creation for this coding system.".format(coding_system))
            pass
        else:
            split_list_icd = df_coding_system['split'].unique().tolist()
            output_files_icd = {str(split):output_file_function(int(split), coding_system) for split in split_list_icd}
            output_files_icd = list(output_files_icd.values())
            output_split_files.extend(output_files_icd)

    # Splitting our split list into groups of 500 <- suggested for array jobs
    split_lists = [output_split_files[i:i + 500] for i in range(0, len(output_split_files), 500)]

    # Setting a batch number to keep track of every 500 jobs
    batch_num = 1

    # Cleans logs, input, output for dataset & data_type_id if resubmission = False (meaning first time running we'll perform cleaning)
    clean_file_dir(this_dataset.temp_folder, this_dataset.dataset_id, this_dataset.data_type_id, 
                        typeOf = "output", is_resub= is_resubmission) #clear logs
    
    # Submit jobs based on batch
    for split_batch in split_lists:

        # Save the parameters as a csv so then you can index the rows to find the appropriate parameters for each job submission
        n_jobs = len(split_batch)
        params = {'dataset_id' : [this_dataset.dataset_id],
                    'data_type_id' : [this_dataset.data_type_id],
                    'split_filename' : split_batch}

        params_grid = expand_grid(params)

        # Set coding system based on split_filename
        params_grid['coding_system'] = params_grid['split_filename'].apply(lambda x: "ICD10" if "ICD10" in x else ("ICD9_detail" if "ICD9_detail" in x else None)) 

        # Set coding system file (ICD10 df, ICD9 df) location
        params_grid['coding_system_file'] = params_grid['coding_system'].apply(lambda x: coding_system_file[x])

        # Set split number based on split_filename
        params_grid['split_number'] = params_grid['split_filename'].apply(lambda x: x.split("split_")[1].split("_")[0])

        # Set parameter file name and path
        param_file = "{}/cnRDP_{}_{}_{}_{}_params.csv".format(this_dataset.temp_folder, 
                                                        this_dataset.dataset_id,
                                                        this_dataset.data_type_id,
                                                        6, # representing step 6 (RDP)
                                                        batch_num)

        # Export params_grid to a .csv for rdp_worker.py to read in
        params_grid.to_csv(param_file, index = False)

        # Set output and error log paths for jobs
        log_path = "{}/cnRDP_{}_{}_jobs/".format(
                                    utils.get_path(key="cancer_logs", process = "common"),
                                    this_dataset.dataset_id, 
                                    this_dataset.data_type_id)
        
        output_path_w_name = "{}%x.o%j".format(log_path)
        error_path_w_name = "{}%x.e%j".format(log_path)

        # Ensure log paths exist (output & error logs share the same directory)
        try:
            os.mkdir(log_path)
            print("Directory " , log_path,  " Created ") 
        except FileExistsError:
            print("Directory " , log_path,  " already exists")

        # Set job name, memory, & runtime
        job_name = "cnRDP_{ds}_{dt}_{bn}".format(
                            ds = this_dataset.dataset_id, 
                            dt = this_dataset.data_type_id,
                            bn = batch_num)
        job_memory = 3 # Can keep this low based on row_split_length
        job_runtime = "00:05:00" # Most RDP jobs take ~20-40 seconds per split

        # Create job call
        call = cluster_tools.make_cluster_job_call(job_name,
                                                   rdp_worker_script,
                                                   mem = job_memory,
                                                   run_time = job_runtime,
                                                   num_array_jobs=n_jobs,
                                                   path_to_param_file=param_file,
                                                   err_filepath=error_path_w_name,
                                                   out_filepath=output_path_w_name
                                                   )

        # Submit job call to cluster
        subprocess.call(call, shell=True)

        # Iterate batch number per 500 jobs (if there are more than 500 splits to process through RDP)
        batch_num += 1

    # Print log directory and params file location for user
    print("Logging can be found here: {}".format(log_path))
    print("Array jobs params file can be found here: {}".format(param_file))

    # Check for output files to be created from jobs
    check_rdp_jobs(output_path = this_dataset.temp_folder,
                   output_split_files = output_split_files)
    print("Split RDP results gathered.")
    print("Concatenating split files together.")

    # Concatenate all output files together
    df_post_rdp_list = []
    for post_rdp_split in output_split_files:
        df_split = pd.read_csv(post_rdp_split)
        df_post_rdp_list.append(df_split)
    df_post_rdp = pd.concat(df_post_rdp_list)

    # Check for NULL values
    if df_post_rdp.isnull().any().any():
        raise NullValuesExist("NULL values exist in post RDP dataframe, check for missing values the RDP split files, fix, then re-run.")

    print("Post RDP splits concatenated and retrieved.")

    return(df_post_rdp)

def main(dataset_id, data_type_id, is_resubmission):
    """Manages the redistribution process and determines if a dataset needs to be ran through CoD's RDP function.

    Args:
        dataset_id (int): A dataset id from the SQL dataset table.
        data_type_id (int): A data type id from the SQL dataset table's "data_type_id" column.
        is_resubmission (bool): If False, clears out files (outputs, logs) for the dataset_id & data_type_id combination.
                                If True, doesn't clear out files (outputs, logs).
    """
    # Load dataset
    print("Loading dataset based on data_type_id...")
    this_dataset = md.MI_Dataset(dataset_id, 6, data_type_id)
    input_data = this_dataset.load_input()

    # Add in 'location_id' column the SQL 'registry' table
    input_data = add_location_id(df_input_data = input_data)

    input_data = remove_non_modeled_locations(df_input_data = input_data)

    input_data = apply_code_relabeling(df_input_data = input_data)

    # Check if dataset needs RDP
    bool_needs_rdp = needs_rdp(df_input_data = input_data,
                               data_type_id = data_type_id)

    # Make copy of input_data for use in RDP
    df_input = input_data.copy()

    # Process dataset based on bool_needs_rdp 
    if bool_needs_rdp:
        # Perform RDP for dataset
        print("dataset_id: {}".format(dataset_id))
        print("data_type_id: {}".format(data_type_id))
        print("needs to have the non-GBD mapped rows redistributed, proceeding with the RDP process...")
        
        # Check for NULL values
        if df_input.isnull().any().any():
            raise NullValuesExist("Null values exist in the dataset! Check for null values in the dataframe, fix, then rerun.")

        # Check that only male and female data are present
        sex_set_diff = np.setdiff1d(ar1 = df_input['sex_id'].unique().tolist(),
                                    ar2 = [1,2]).tolist()
        if sex_set_diff != []:
            raise Exception("Sex_id values other than {} are present in dataframe, RDP can only handle data from sex_id = 1 or 2. Check data for both sex (sex_id = 3) data.".format(df_input['sex_id'].unique().tolist()))

        # Check to ensure that age groups ID are compatible with RDP.
        # df_input has Cancer age_group_ids going into this function and will have GBD age_group_ids coming out of the function.
        df_input = assert_age_group_compatibility(df_input_data = df_input)

        # Split dataset into GBD mapped causes & non-GBD mapped causes (can only submit non-GBD mapped causes e.g. ICD codes to CoD's RDP function)
        df_gbd_mapped, df_non_gbd_mapped = split_mapped_causes_gbd(df_input_data = df_input,
                                                                   data_type_id = data_type_id)
        
        # Drop location_id from df_gbd_mapped (only needed for df_non_gbd_mapped, the data going into RDP)
        df_gbd_mapped.drop(columns = 'location_id', inplace = True)

        # Add 'year_id' column needed for RDP as it can't accept a year_start & year_end. Use the midpoint of year_start & year_end as 'year_id', round down for fractional years (e.g. 1978.5 -> 1978)
        df_non_gbd_mapped['year_id'] = round((df_non_gbd_mapped['year_start'] + df_non_gbd_mapped['year_end']) / 2).astype(int)

        df_non_gbd_mapped['year_id'] = df_non_gbd_mapped['year_id'].apply(lambda x: 1980 if x < 1980 else x)
        
        # Check for the columns "cases" and rename it to "deaths", otherwise make no change (need to convert this back to "deaths" if "cases", CoD's RDP function expects 'deaths' even if we're redistributing incidence data)
        if 'cases' in df_non_gbd_mapped.columns.tolist():
            df_non_gbd_mapped.rename({'cases':'deaths'}, axis = 1, inplace = True)

        # Merge dataframe with CoD's cause_map to add 'code_id' column
        df_non_gbd_mapped = add_code_id(df_input_data = df_non_gbd_mapped)

        # Perform RDP by splitting non_gbd_mapped data into specific number of rows based on the ICD coding system, submit jobs to cluster, wait for files to populate, concat, then return
        df_rdp_data = submit_rdp(df_input_data = df_non_gbd_mapped,
                                this_dataset = this_dataset,
                                is_resubmission = is_resubmission) 

        # Assert that all registry_index / year_start / year_end exist in the post RDP dataframe compared against data input to RDP
        bool_all_registry_index_present = set(df_non_gbd_mapped['registry_index'].unique().tolist()) == set(df_rdp_data['registry_index'].unique().tolist())
        if not bool_all_registry_index_present:
            raise Exception('Post RDP data does not contain the same set of registry_indexes prior to RDP, please check RDP split files!')

        bool_all_year_start_present = set(df_non_gbd_mapped['year_start'].unique().tolist()) == set(df_rdp_data['year_start'].unique().tolist())
        if not bool_all_year_start_present:
            raise Exception('Post RDP data does not contain the same set of year_start prior to RDP, please check RDP split files!')
        
        bool_all_year_end_present = set(df_non_gbd_mapped['year_end'].unique().tolist()) == set(df_rdp_data['year_end'].unique().tolist())
        if not bool_all_year_end_present:
            raise Exception('Post RDP data does not contain the same set of year_end prior to RDP, please check RDP split files!')

        # Drop columns from df_gbd_mapped no longer needed
        df_gbd_mapped.drop(labels = ['coding_system', 'dataset_id'], axis = 1, inplace = True)

        df_gbd_mapped = remap_secret_causes(df_input_data = df_gbd_mapped)

        # Combine back together with df_gbd_mapped to create the full dataset with mapped GBD acauses and now RDP'd acauses
        df_gbd_rdp = pd.concat([df_gbd_mapped, df_rdp_data]).reset_index(drop = True)

        # Convert GBD 'age_group_id' back to Cancer 'age' groups
        df_gbd_rdp = gct.cancer_age_from_age_group_id(df_gbd_rdp)

        # Drop GBD 'age_group_id' column
        df_gbd_rdp.drop(labels = ['age_group_id'], axis = 1, inplace = True)

        # Group by summary once more to collapse RDP data with the already-GBD-mapped data
        cols_to_group_by = ['registry_index', 'year_start', 'year_end', 'age', 'sex_id', 'acause']
        df_gbd_rdp = df_gbd_rdp.groupby(cols_to_group_by).sum().reset_index()

        # Save as 06_redistributed_<data_type>.dta file
        print("dataset_id: {}".format(dataset_id))
        print("data_type_id: {}".format(data_type_id))
        print("Finished processing through RDP, proceeding to save the 06_redistributed_{}.dta file.".format(this_dataset.data_type_name))
        md.complete_prep_step(cleaned_df = df_gbd_rdp,
                              input_df = input_data, 
                              ds_instance = this_dataset)

    else:
        # Dataset doesn't need RDP, proceed with saving 06_redistributed_<data_type>.dta file
        print("dataset_id: {}".format(dataset_id))
        print("data_type_id: {}".format(data_type_id))
        print("does not need to have its data redistributed, proceeding to save the 06_redistributed_{}.dta file.".format(this_dataset.data_type_name))
        df_input.drop(columns = 'location_id', inplace = True) # Need to drop location_id, not needed if not performing RDP
        input_data.drop(columns = 'location_id', inplace = True) # Need to drop location_id, not needed if not performing RDP
        md.complete_prep_step(cleaned_df = df_input,
                              input_df = input_data, 
                              ds_instance = this_dataset)
    
if __name__ == "__main__":
    arg_dataset_id = int(sys.argv[1])
    arg_data_type_id = int(sys.argv[2])
    arg_is_resubmission = bool(int(sys.argv[3])) if len(sys.argv) > 3 else False
    main(dataset_id = arg_dataset_id,
         data_type_id = arg_data_type_id,
         is_resubmission = arg_is_resubmission)