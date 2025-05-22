'''
Description: Manages redistribution for a given subset/split of a dataset that needs redistribution for GBD2022+
             Description for functionality and high level steps is under the main() function.

How To Use: Called by run_redistribution.py

Contributors: INDIVIDUAL_NAME
'''
import os
import sys
import pandas as pd
import time

# Import cancer_estimation utilities and modules
from cancer_estimation.py_utils import (
    common_utils as utils,
)

from cancer_estimation.a_inputs.a_mi_registry.redistribution import (
    run_redistribution,
)

# CoD team package import
from cod_prep.claude.redistribution import Redistributor
from cod_prep.claude.configurator import Configurator
from cod_prep.downloaders import (
    get_remove_decimal,
)

# Establish dictionaries to use
DICT_CODE_SYSTEM_ID = {"ICD10" : 1,
                        "ICD9_detail" : 6}
DICT_CAUSE_MAP_FILE = {"ICD10" : "rdp_cod_cause_map_icd10.csv",
                        "ICD9_detail" : "rdp_cod_cause_map_icd9.csv"}
DICT_DATA_TYPE_ID_METRIC = {2 : "cases", 3 : "deaths"}

def run_rdp(df_input_data, coding_system):
    """Runs CoD's RDP process on an ICD9 or ICD10 dataframe containing only non-GBD mapped acauses

    Args:
        df_input_data (pd.DataFrame): An ICD9 or ICD10 dataframe contaning only non-GBD mapped acauses.
    Returns:
        df_rdp (pd.DataFrame): A dataframe with all non-mapped GBD causes now properly redistributed.
    """
    # Read in flat files needed for CoD's RDP function
    dir_cod_cached_files = utils.get_path(key = 'common', process = 'common')
    fn_loc_meta_df = dir_cod_cached_files + "/rdp_cod_loc_meta_df.csv"
    fn_age_meta_df = dir_cod_cached_files + "/rdp_cod_age_meta_df.csv"
    fn_cause_meta_df = dir_cod_cached_files + "/rdp_cod_cause_meta_df.csv"
    fn_cause_map = dir_cod_cached_files + "/" + DICT_CAUSE_MAP_FILE[coding_system]

    df_loc_meta_df = pd.read_csv(filepath_or_buffer = fn_loc_meta_df)
    df_age_meta_df = pd.read_csv(filepath_or_buffer = fn_age_meta_df)
    df_cause_meta_df = pd.read_csv(filepath_or_buffer = fn_cause_meta_df)
    df_cause_map = pd.read_csv(filepath_or_buffer = fn_cause_map)

    # Set configuration for CoD's RDP function
    CONF = Configurator()
    cache_kwargs = {
        "force_rerun": True,
        "block_rerun": False,
        "cache_results": True,
        "cache_dir": CONF.get_directory("db_cache"),
        "verbose": False,
    }
    remove_decimal = get_remove_decimal(DICT_CODE_SYSTEM_ID[coding_system], **cache_kwargs)
    code_system_id = DICT_CODE_SYSTEM_ID[coding_system]

    # Proportional_cols are the demographically relevant columns used by the RDP code to help with distribution
    proportional_cols = {{"location_id": "admin1_or_above_id"}.get(c, c) for c in df_input_data.columns} & set(
    CONF.get_id("potential_rd_proportional_cols")
    )

    proportional_cols.add('registry_index')
    proportional_cols.add('year_start')
    proportional_cols.add('year_end')

    # Subset on df_input_data's columns to match what CoD's RDP function expects as input columns
    df_input_data = df_input_data[['sex_id', 'deaths', 'location_id', 'year_id', 'age_group_id', 'code_id', 'year_start', 'year_end', 'registry_index']]

    # Run dataframe slice through RDP by instantiating the Redistributor class and running the run_redistribution() function
    # and print how long it takes
    st = time.time()
    df_rdp = Redistributor(
            conf=CONF,
            code_system_id=code_system_id,
            proportional_cols=list(proportional_cols),
            loc_meta_df=df_loc_meta_df,
            age_meta_df=df_age_meta_df,
            cause_meta_df=df_cause_meta_df,
            cause_map=df_cause_map,
            remove_decimal=remove_decimal,
            diagnostics_path=(None
            ),
        ).run_redistribution(df_input_data)
    et = time.time()
    elapsed_time = et - st
    print('RDP took:', elapsed_time, 'seconds')
    
    return(df_rdp)

def map_code_id_to_gbd_cause(df_input_data):
    """Maps code_id back to GBD cause_id / acause / cause_name

    Args:
        df_input_data (pd.DataFrame): A post-RDP'd dataframe with the 'code_id' column.
    Returns:
        df_rdp_code_id_mapped (pd.DataFrame): Dataframe now containing cause_id / acause / cause_name
    """
    # Read in flat files needed to map code_id back to gbd cause_id / acause / cause_name
    dir_cod_cached_files = utils.get_path(key = 'common', process = 'common')
    fn_cause_meta_df = dir_cod_cached_files + "/rdp_cod_cause_meta_df.csv"
    fn_cause_map = dir_cod_cached_files + "/" + DICT_CAUSE_MAP_FILE[coding_system]

    df_cause_meta_df = pd.read_csv(filepath_or_buffer = fn_cause_meta_df)
    df_cause_map = pd.read_csv(filepath_or_buffer = fn_cause_map)

    # Merge with CoD's cause map to map code_id -> cause_id
    df_rdp_code_id_mapped = df_input_data.merge(right = df_cause_map[['code_name', 'code_id', 'cause_id']], 
                        how = 'left',
                        on = ['code_id'])

    # Merge with CoD's cause metadata to map cause_id -> acause
    df_rdp_code_id_mapped = df_rdp_code_id_mapped.merge(right = df_cause_meta_df[['cause_id', 'acause', 'cause_name']], 
                            how = 'left',
                            on = ['cause_id'])
    
    # Assert no extra rows were added
    if len(df_input_data) != len(df_rdp_code_id_mapped):
        raise run_redistribution.UnequalDataFramesLengthException("Extra rows were added when mapping code_ids back to GBD causes, check dataframe for one-to-many matches!")

    # Assert all rows have matches (no null values)
    bool_missing_acause = any(pd.isna(df_rdp_code_id_mapped['acause']))
    if bool_missing_acause:
        print("These code_ids are missing an acause match from CoD's cause_map:")
        print(df_rdp_code_id_mapped.loc[df_rdp_code_id_mapped['acause'].isna(), 'code_id'].unique().tolist())
        print("\n")
        raise run_redistribution.NullValuesExist("Not all code_ids have a matching acause from CoD's cause_map! Please check dataset for odd code_ids.")

    return(df_rdp_code_id_mapped)

def lymphoma_subtypes_rdp_fix(df_input_data : pd.DataFrame, rdp_pre_or_post : str) -> pd.DataFrame:
    """ 
    Args:
        df_input_data (pd.DataFrame): Input data that requires RDP.
        rdp_pre_or_post (str): Options ['pre', 'post'] - 'pre' converts GBD age_group_id 1 -> 34, 'post' converts GBD age_group_id 34 -> 1

    Returns:
        pd.DataFrame: A dataframe with rows of lymphoma garbage code data with their age group IDs reassigned from 1 -> 34 (0-4 years old to 2-4 years old)
    """
    lymphoma_garbage_code_p198_icd10_list = ['C83', 'C83.9', 'C85.9', 'C85.1']
    lymphoma_garbage_code_p183_icd9_list = [202.9]
    lymphoma_garbage_code_list = lymphoma_garbage_code_p198_icd10_list + lymphoma_garbage_code_p183_icd9_list

    # Ensure user is passing an acceptable input
    if rdp_pre_or_post not in ['pre', 'post']:
        raise Exception("Must specify 'pre' or 'post' depending if the dataset has already ran through CoD's RDP function for proper GBD age group conversion for lymphoma garbage codes.")
    
    bool_age_34_exists = df_input_data.loc[df_input_data['age_group_id'] == 34, 'age_group_id'].any()
    
    if bool_age_34_exists & (rdp_pre_or_post == 'pre'):
        print("Dataset_id: {}".format(df_input_data['dataset_id'][0]))
        print("Coding_system: {}".format(df_input_data['coding_system'][0]))
        print("Split number: {}".format(df_input_data['split'][0]))
        raise Exception("Dataset contains GBD age_group_id = 34 (2-4 years) - lymphoma RDP package fix will need to be reviewed and possibly modified as the logic relies on this particular \
                        age group not existing in a dataset to properly identify post-redistributed lymphoma subtype data.")

    # Pre - if data needs RDP
    if rdp_pre_or_post == 'pre':
        df_input_data.loc[(df_input_data['age_group_id'] == 1) & (df_input_data['value']).isin(lymphoma_garbage_code_list), 'age_group_id'] = 34
    
    # Post - if data has already passed through RDP
    if rdp_pre_or_post == 'post':
        df_input_data.loc[(df_input_data['age_group_id'] == 34), 'age_group_id'] = 1
    
    return(df_input_data)

def main(dataset_id : int, data_type_id : int, 
         split_number : int, split_filename : str,
         coding_system : str, coding_system_file : str):
    """Performs the heavy lifting to process a non-GBD acause mapped dataset's sliced dataframe based on
       the ICD coding system (ICD9_detail or ICD10) and slice number through CoD's redistribution (RDP) process.
       
    Args:
        dataset_id (int): Dataset_id value, found from the SQL 'dataset' table
        data_type_id (int): Data type id value representing what type of data is being processed (incidence, mortality)
        split_number (int): A split number determining which part of the ICD coding system specific dataframe split to subset and process
        split_filename (string): Filename & path to output the specific processed split
        coding_system (string): Either 'ICD10' or 'ICD9_detail'
        coding_system_file (string): Filename & path to read in the coding_system specific dataframe split data.
    """

    # Read in the correct coding_system_file and filter on split
    df_input = pd.read_csv(coding_system_file)
    df_input = df_input.loc[df_input['split'] == split_number]

    # **REVISIT EACH GBD ROUND** - Fix for lymphoma garbage codes & RDP Package P-198
    df_input = lymphoma_subtypes_rdp_fix(df_input_data = df_input, rdp_pre_or_post = 'pre')

    # Processes dataframe split on CoD's RDP function and returns post-RDP'd dataframe
    df_rdp = run_rdp(df_input_data = df_input,
                     coding_system = coding_system)
    
    # **REVISIT EACH GBD ROUND** - Fix for lymphoma garbage codes & RDP Package P-198
    df_rdp = lymphoma_subtypes_rdp_fix(df_input_data = df_rdp, rdp_pre_or_post = 'post')

    # Map code_id back to cause_id / acause / cause_name
    df_rdp = map_code_id_to_gbd_cause(df_input_data = df_rdp)

    # Subset to the columns we need to retain for Finalization (step 7 of prep)
    cols_to_keep = ['registry_index', 'year_start', 'year_end', 'age_group_id', 'sex_id', 'acause', 'deaths']
    df_rdp = df_rdp[cols_to_keep]

    if DICT_DATA_TYPE_ID_METRIC[data_type_id] == 'cases':
        df_rdp.rename({'deaths':'cases'}, axis = 1, inplace = True)

    df_rdp = run_redistribution.remap_secret_causes(df_rdp)

    # Group by summary to collapse data by registry_index / year_start / year_end / age_group_id / sex_id / acause
    cols_to_group_by = ['registry_index', 'year_start', 'year_end', 'age_group_id', 'sex_id', 'acause']
    df_rdp = df_rdp.groupby(cols_to_group_by).sum().reset_index()


    df_rdp_cause_map = run_redistribution.load_rdp_cause_map(data_type_id = data_type_id)
    df_rdp = df_rdp.loc[df_rdp['acause'].isin(df_rdp_cause_map['acause'].unique().tolist()), ]

    # Write to flat file using slice_filename
    df_rdp.to_csv(split_filename, index = False)

    print("Split run through RDP and saved successfully for:")
    print('dataset_id = {}'.format(dataset_id))
    print('data_type_id = {}'.format(data_type_id))
    print('split_number = {}'.format(split_number))
    print('split_filename = {}'.format(split_filename))
    print('coding_system = {}'.format(coding_system))
    print('coding_system_file = {}'.format(coding_system_file))
    print('Year start set = {}'.format(set(df_rdp['year_start'].unique().tolist())))
    print('Year end set = {}'.format(set(df_rdp['year_end'].unique().tolist())))


if __name__ == '__main__':
    ''' Run RDP on specific split based on 
    -dataset_id (int): The dataset_id, from the SQL 'dataset' table
    -data_type_id (int): The data_type_id, corresponding to the type of data in a dataset, 2 for Incidence (cases), 3 for Mortality (deaths)
    -split_number (int): The split number corresponding to the section of a dataframe's coding system specific section to process RDP on
    -split_filename (string): The output filename to save the processed RDP'd split to
    -coding_system (string): Either 'ICD10' or 'ICD9_detail'
    -coding_system_file (string): The corresponding dataframe based on the coding_system to read in and subset based on the split number
    '''
    if len(sys.argv) > 2:
        dataset_id = int(sys.argv[1])
        data_type_id = int(sys.argv[2])
        split_number = int(sys.argv[3])
        split_filename = int(sys.argv[4])
        coding_system = int(sys.argv[5])
        coding_system_file = int(sys.argv[6])
    else:
        rdp_params = pd.read_csv(sys.argv[1])
        task_id = int(os.environ["SLURM_ARRAY_TASK_ID"])
        index_num = task_id - 1
        dataset_id = rdp_params.loc[index_num, 'dataset_id']
        data_type_id = rdp_params.loc[index_num, 'data_type_id']
        split_number = rdp_params.loc[index_num, 'split_number']
        split_filename = rdp_params.loc[index_num, 'split_filename']
        coding_system = rdp_params.loc[index_num, 'coding_system']
        coding_system_file = rdp_params.loc[index_num, 'coding_system_file']

    print("Preparing to run split through RDP for:")
    print('dataset_id = {}'.format(dataset_id))
    print('data_type_id = {}'.format(data_type_id))
    print('split_number = {}'.format(split_number))
    print('split_filename = {}'.format(split_filename))
    print('coding_system = {}'.format(coding_system))
    print('coding_system_file = {}'.format(coding_system_file))

    main(dataset_id = dataset_id,
         data_type_id = data_type_id,
         split_number = split_number,
         split_filename = split_filename,
         coding_system = coding_system,
         coding_system_file = coding_system_file)


