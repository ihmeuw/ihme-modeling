
'''
Description: Runs the garbage code redistribution step of 
             the cancer prep process
Input(s): 05_cause_disagggregated file
Output(s): 06_redistributed file
Contributors: USERNAME
'''
import sys
import time
import glob
import pandas as pd
import os
from cancer_estimation.py_utils import(
    cluster_tools as cluster_tools,
    common_utils as utils,
    data_format_tools as dft, 
    pandas_expansions as pe,
    gbd_cancer_tools as gct
)
from cancer_estimation.a_inputs.a_mi_registry import (
    mi_dataset as md,
    cluster_utils_prep as cup,
    pipeline_tests as pt
)
from cancer_estimation.registry_pipeline import cause_mapping as cm
from cancer_estimation.a_inputs.a_mi_registry.redistribution import rdp_tests

from os.path import isfile, join
import warnings
import subprocess
from itertools import product, repeat
from functools import partial


def needs_rdp(df, ds_instance):
    ''' Quick check to determine if RDP needs to be run for the dataset
    '''
    rdp_map = pd.read_csv("{}/rdp_cause_map_{}.csv".format(ds_instance.temp_folder,
                                                            ds_instance.data_type_id))
    ok_acause = rdp_map['acause'].unique()
    rdp_needed = (~df['acause'].isin(ok_acause))
    return(df.loc[rdp_needed, :].any().any())


def prep_input(df, ds_instance):
    ''' Returns the dataframe with added locations, corrected mapping, and
        restrictions applied
    '''
    print("    preparing data for redistribution...")
    # format data for redistribution
    data_type_id = ds_instance.data_type_id
    metric_name = ds_instance.metric
    uid_cols = md.get_uid_cols(6)

    # standardizes age and age_group_id columns in rdp input
    if not("age" in df.columns.values):
        df = gct.cancer_age_from_age_group_id(df)

    df.rename(columns={'sex':'sex_id'}, inplace=True)
    restrict_df = cm.restrict_causes(df, cause_col='acause',
                            data_type_id=data_type_id)
    if not("age_group_id" in df.columns.values):
        restrict_df = gct.age_group_id_from_cancer_age(restrict_df)

    group_cols = [c for c in uid_cols if c not in ['age', 'age_group_id', 'acause']]
    uids_added = dft.make_group_id_col(restrict_df, group_cols, id_col='uid')
    output = md.add_location_ids(uids_added)
    # check to make sure dropped restricted cases is not > 10% of original
    diff = restrict_df[metric_name].sum() - df[metric_name].sum()
    if ds_instance.dataset_id not in [527, 442]:
        assert (abs(diff)/df[metric_name].sum()) < 0.1, \
            "Restrictions from rdp are dropping more than 10% of cases"
    return(output)


def get_rdp_file(ds_instance, which_file='key', splitNum=None):
    ''' Accepts an MI_Dataset class and returns the appriopriate file name
        depending on rdp process
    '''
    dsid = ds_instance.dataset_id
    dtid = ds_instance.data_type_id
    temp_dir = ds_instance.temp_folder
    if which_file == "rdp_input":
        this_file = "{}/{}_{}.h5".format(temp_dir, dsid, dtid)
    elif which_file == "split_output":
        this_file = "{}/{}_{}_split{}_rdp.csv".format(
            temp_dir, dsid, dtid, splitNum)
    if this_file:
        utils.ensure_dir(this_file)
        return(this_file)


def check_rdp_jobs(output_path, output_files):
    ''' Quick checker for completion of RDP jobs and also checks if output
        csvs are empty or not
        Args:
            output_path - where finished rdp job files are saved
            uids - int, number of split rdp jobs there are 

        Returns: boolean of whether jobs are finished or not
    '''
    rdp_files = []
    # make sure output path exists
    try:
        os.mkdir(output_path)
        print("Directory " , output_path,  " Created ") 
    except FileExistsError:
        print("Directory " , output_path,  " already exists")

    # runs until all jobs are complete
    loop = 1
    while len(rdp_files) < len(output_files):
        # check every 5 sec for job completion
        rdp_files = [filepath for filepath in output_files if filepath is not None and os.path.isfile(filepath)]
        if loop % 5000 == 0:
            print("\nChecking again ... {} files remaining".
                                        format(len(output_files) - len(rdp_files)))
            time.sleep(1)
        loop += 1

    dfs = [pd.read_csv(f) for f in rdp_files]
    empty_files = [df.empty for df in dfs]
    assert not(True in empty_files), \
        "There are empty split rdp files!"
    print("Split rdp csvs retrieved!")
    return(None)


def submit_rdp(input_data, this_dataset, is_resubmission):
    ''' Returns full dataset after redistribution.
        Separates data by submission requirement before submitting rdp for 
        only those data that require it. 

        Launches array jobs for rdp workers
    '''
    def submission_requirement(df, uid): 
        return needs_rdp(df[df['uid'] == uid], this_dataset)

    def output_file_function(id): 
        return get_rdp_file(this_dataset, which_file='split_output', splitNum=id)


    def clean_file_dir(path, dataset_id, data_type_id, typeOf = "output", is_resub = False):
        ''' Cleans files in path based on rdp process (logs, input, output)'''

        print("Cleaning workspace...")
        if not(is_resub):
            if typeOf == "output":
                fileList = glob.glob('{}/*{}_{}_split*.csv'.format(path, dataset_id, data_type_id))
            elif typeOf == "logs":
                fileList = glob.glob('{}/*'.format(path))
            elif typeOf == "input":
                fileList = glob.glob('{}/{}_{}.h5'.format(path, dataset_id, data_type_id)) + \
                            glob.glob('{}/cnRDP*_{}_*_params.csv'.format(path, data_type_id))

            # iterate over the list of filepaths & remove each file.
            for filePath in fileList:
                try:
                    os.remove(filePath)
                except:
                    pass

    ''' 
    '''

    # create a list of the uids that require redistribution and set aside a
    # dataframe of the uids that do not require redistribution
    rdp_code_location = utils.get_path("redistribution",
                                       base_folder="code_repo",
                                       process="mi_dataset")
    worker_script = rdp_code_location + "/rdp_worker.py"
    output_uids = md.get_uid_cols(7)

    header = "cncRDP_{}_{}".format(this_dataset.dataset_id, this_dataset.data_type_id)
    rdp_input_file = get_rdp_file(this_dataset, which_file='rdp_input')
    print('rdp_input_file directory is....{}'.format(rdp_input_file))
    # submit jobs 
    prepped_df = prep_input(input_data, this_dataset)
    submitted_data, unsubmitted_data = cup.split_submission_data(prepped_df, 'uid', 
                                        submission_requirement, rdp_input_file)
    uid_list = submitted_data['uid'].unique().tolist()
    output_files = {str(uid):output_file_function(int(uid)) for uid in uid_list}
    output_files = output_files.values()

    print('generating prep workers...')

    def expand_grid(dictionary):
        ''' Function to get all combinations of our params job dict
        '''
        return pd.DataFrame([row for row in product(*dictionary.values())], 
                            columns=dictionary.keys())


    # splitting our uid list into groups of 500 <- suggested for array jobs
    uid_lists = [uid_list[i:i + 500] for i in range(0, len(uid_list), 500)]
    # launching our jobs in batches
    batch_num = 1
    clean_file_dir(this_dataset.temp_folder, this_dataset.dataset_id, this_dataset.data_type_id, 
                        typeOf = "output", is_resub= is_resubmission) #clear logs
    for uid_batch in uid_lists:

        ## Save the parameters as a csv so then you can index the rows to find 
        #  the appropriate parameters
        n_jobs = len(uid_batch)
        params = {'dataset_id' : [this_dataset.dataset_id],
                    'data_type_id' : [this_dataset.data_type_id],
                    'uid' : uid_batch}

        params_grid = expand_grid(params)

        # set import. file paths
        param_file = "{}/cnRDP_{}_{}_{}_params.csv".format(this_dataset.temp_folder, 
                                                        this_dataset.dataset_id,
                                                        this_dataset.data_type_id,
                                                        batch_num)
        params_grid.to_csv(param_file, index = False)

        output_path = "{}/cnRDP_{}_jobs/".format(
                                    utils.get_path(key="cancer_logs", process = "common"),
                                    this_dataset.dataset_id, 
                                    this_dataset.data_type_id)

        error_path = "{}/cnRDP_{}_jobs/".format(
                                    utils.get_path(key="cancer_logs", process = "common"),
                                    this_dataset.dataset_id, 
                                    this_dataset.data_type_id)

        # make sure error/ output paths exists
        try:
            os.mkdir(output_path)
            print("Directory " , output_path,  " Created ") 
        except FileExistsError:
            print("Directory " , output_path,  " already exists")

        job_name = "cnRDP_{ds}_{dt}_{ds}_{dt}_{bn}".format(
                                    ds = this_dataset.dataset_id, 
                                    dt = this_dataset.data_type_id,
                                    bn = batch_num)
        
        # Not sure why, but an rdp package for these datasets takes a very
        # long time
        if this_dataset.dataset_id in [268, 250]:
            runtime = "00:60:00" 
        else:
            runtime = "00:20:00" if len(uid_list) < 1000 else "00:25:00"
        mem = 6.0 if len(uid_list) < 1000 else 8.0
        call = ('qsub -l m_mem_free={mem}G -l fthread=1 -l h_rt={rt} -l archive=True -q all.q' 
                    ' -cwd -P proj_cancer_prep'
                    #' -o {o}'
                    #' -e {e}'
                    ' -N {jn}'
                    ' -t 1:{nj}'
                    ' {sh}'
                    ' {s}'
                    ' {p}'.format(mem=mem,
                                rt=runtime,
                                jn=job_name, 
                                #o=output_path, e=error_path,
                                sh=utils.get_path(key="py_shell", process = "common"),
                                nj=n_jobs,
                                s=worker_script,
                                p=param_file))             
        if not(is_resubmission):
            subprocess.call(call, shell=True)
        batch_num += 1
    # keep checking jobs until complete then launch others
    temp_folder = this_dataset.temp_folder
    check_rdp_jobs(temp_folder, output_files)
    print("Results gathered")
    # re-combine compiled results with the set-aside data, before collapsing
    #   and testing
    final_results = pe.read_files(output_files)

    # standardize sex vs sex_id vars
    if 'sex' in final_results.columns and 'sex_id' not in final_results.columns:
        final_results.rename(columns={'sex':'sex_id'}, inplace=True)
    
    if 'sex' in unsubmitted_data.columns and 'sex_id' not in unsubmitted_data.columns:
        final_results.rename(columns={'sex':'sex_id'}, inplace=True)

    final_results = final_results.append(unsubmitted_data)
    assert not final_results['sex_id'].isnull().any(), \
        "Sex columns have null values"
    print("Final results produced")

    # re-set all 'under 5' data, then collapse to combine it with any existing
    #       'under 5' data
    final_results.loc[final_results['age'].lt(7) |
                        (final_results['age'].gt(90) &
                        final_results['age'].lt(95)),
                        'age'] = 2

    # correction to datasets with sex 
    if 'sex' in final_results.columns and 'sex_id' in output_uids:
        output_uids = ['sex' if x=='sex_id' else x for x in output_uids]
    
    if 'sex_id' in final_results.columns and 'sex' in output_uids:
        output_uids = ['sex_id' if x=='sex' else x for x in output_uids]


    final_results = dft.collapse(final_results, by_cols=output_uids,
                            combine_cols = this_dataset.metric)
    pt.verify_metric_total(prepped_df, final_results, this_dataset.metric, 
                                            "finalizing rdp")
    return(final_results)


def main(dataset_id, data_type_id, is_resubmission):
    ''' Manages the redistribution process and runs each subprocess in order
    '''

    # load data
    print("Preparing inputs...")
    this_dataset = md.MI_Dataset(dataset_id, 6, data_type_id)
    input_data = this_dataset.load_input()
    metric_name = this_dataset.metric

    # saving rdp cause map to avoid too many database calls during rdp_worker jobs
    rdp_map = cm.load_rdp_cause_map(this_dataset.data_type_id)
    rdp_map.to_csv("{}/rdp_cause_map_{}.csv".format(this_dataset.temp_folder,
                                                    this_dataset.data_type_id),
                                                    index = False)

    #re-name sex_id variable until rdp packages are updated 
    input_data.rename(columns={'sex_id':'sex'}, inplace=True)
    output_uids = md.get_uid_cols(7)

    # replace sex_id with sex until rdp packages are updated 
    output_uids = ['sex' if x == 'sex_id' else x for x in output_uids]
    
    # change codes for specific causes that go through rdp
    if 'ICD10' in input_data['coding_system'].unique():
        input_data.loc[input_data['acause'].eq("neo_leukemia_ll_chronic"), 'acause'] = 'C91.1'
        input_data.loc[input_data['acause'].eq("neo_lymphoma"), 'acause'] = 'C85.9' 
        input_data.loc[input_data['acause'].eq("neo_eye"), 'acause'] = 'C69'
        input_data.loc[input_data['acause'].eq("neo_ben_brain"), 'acause'] = 'neo_ben_other'
        input_data.loc[input_data['acause'].eq("neo_other_maligt"), 'acause'] = 'neo_other_cancer'
        input_data.loc[input_data['acause'].eq("neo_neuroblastoma"), 'acause'] = 'neo_neuro'
        input_data.loc[input_data['acause'].eq("neo_tissues_sarcoma"), 'acause'] = 'neo_tissue_sarcoma'
        input_data.loc[input_data['acause'] == "neo_other", 'acause'] = 'neo_other_cancer'

        # NOTE: GBD2020 refresh 2 decision: convert all double decimal codes to 
        #       map to single decimal parent: same as cod
        # check for double decimal codes
        is_two_decimal = ((input_data['acause'].str.contains('.')) &
                        (input_data['acause'].str.len() == 6) &
                        ((input_data['acause'].str.contains('C'))|
                        (input_data['acause'].str.contains('D'))))
        input_data.loc[is_two_decimal, 'acause'] = input_data.loc[is_two_decimal, 'acause'].str[:5]
    
    # change codes for specific causes that go through rdp for ICD9
    if 'ICD9_detail' in input_data['coding_system'].unique():
        input_data.loc[input_data['acause'].eq("neo_leukemia_ll_chronic"), 'acause'] = '204.1'
        input_data.loc[input_data['acause'].eq("neo_eye"), 'acause'] = '190.9'
        input_data.loc[input_data['acause'].eq("neo_lymphoma"), 'acause'] = '202.9'
        input_data.loc[input_data['acause'].eq("neo_ben_brain"), 'acause'] = 'neo_brain'
        input_data.loc[input_data['acause'].eq("neo_ben_other"), 'acause'] = 'neo_other_cancer'
        input_data.loc[input_data['acause'].eq("neo_other_maligt"), 'acause'] = 'neo_other_cancer'
        input_data.loc[input_data['acause'].eq("neo_neuroblastoma"), 'acause'] = 'neo_neuro'
        input_data.loc[input_data['acause'].eq("neo_tissues_sarcoma"), 'acause'] = 'neo_tissue_sarcoma'
        input_data.loc[input_data['acause'] == "neo_other", 'acause'] = 'neo_other_cancer'

    # exit if RDP is not needed
    if not needs_rdp(input_data, this_dataset):
        ok_acause = cm.load_rdp_cause_map(data_type_id)['acause'].unique()
        output_data = input_data.loc[input_data['acause'].isin(ok_acause)]

        # temporarily rename sex to sex_id until rdp packages are updated (GBD 2019) 
        output_data.rename(columns={'sex':'sex_id'}, inplace=True)
        md.complete_prep_step(output_data, input_data, this_dataset)
    else:
        final_results = submit_rdp(input_data, this_dataset, is_resubmission)

        # temporarily rename sex to sex_id until rdp packages are updated (GBD 2019) 
        final_results.rename(columns={'sex':'sex_id'}, inplace=True)

        # temp fix
        final_results.loc[final_results['acause'].eq("neo_utr"), 'acause'] = "neo_uterine"
        rdp_tests.compare_rdp_change(this_dataset, input_data, final_results)

        # Save and exit
        md.complete_prep_step(final_results, input_data, this_dataset)
        print("rdp complete.")


if __name__ == "__main__":
    dsid = int(sys.argv[1])
    dtid = int(sys.argv[2])
    is_resubmission = bool(int(sys.argv[3])) if len(sys.argv) > 3 else False
    main(dataset_id=dsid, data_type_id=dtid, is_resubmission=is_resubmission)
