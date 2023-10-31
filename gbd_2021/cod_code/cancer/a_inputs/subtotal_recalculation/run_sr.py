
# -*- coding: utf-8 -*-
'''
Description: calculates remainders when individual icd codes are subtracted from code ranges
Input(s): mi_input in standardized format
Output(s): recalculated dataset
How To Use: Pass dataset_id and data_type_id to main()
Contributors: USERNAME
'''

import os
import sys
import glob
import numpy as np
import pandas as pd
from os.path import isfile, join
import warnings
import subprocess
from cancer_estimation.a_inputs.a_mi_registry import (
    mi_dataset as md,
    cluster_utils_prep as cup,
    pipeline_tests as pt
)
from cancer_estimation.a_inputs.a_mi_registry.subtotal_recalculation import (
    code_components as code_components,
    remove_subtotals as remove_subtotals,
    sr_tests as st
)
from cancer_estimation.py_utils import (
    data_format_tools as dft,
    common_utils as utils,
    pandas_expansions as pe 
)
from cancer_estimation._database import cdb_utils
from multiprocessing import Pool
import multiprocessing as mp
from itertools import product, repeat
from functools import partial
import time
try:
    # Python3
    from functools import partial
except ImportError:
    # fallback to Python2
    from functools32 import partial



def get_iccc_code_sets():
    ''' Returns dict of ICCC code sets by main codes 
            -Keys are main parent codes
            -Value contained lists are child codes of that parent
    '''
    code_sets = {'I': ['Ia', 'Ib', 'Ic', 'Id', 'Ie'],
                 'Ii': ['Iia', 'Iib', 'Iic', 'Iid', 'Iie'],
                 'Iii': ['Iiia', 'Iiib', 'Iiic', 'Iiid', 'Iiie', 'Iiif'],
                 'Iv': ['Iva', 'Ivb'],
                 'V' : [],
                 'Vi': ['Via','Vib', 'Vic'],
                 'Vii':['Viia', 'Viib', 'Viic'],
                 'Viii':['Viiia', 'Viiib', 'Viiic', 'Viiid', 'Viiie'],
                 'Ix': ['Ixa', 'Ixb', 'Ixc', 'Ixd', 'Ixe'],
                 'X': ['Xa', 'Xb', 'Xc', 'Xd', 'Xe'],
                 'Xi': ['Xia', 'Xib', 'Xic', 'Xid', 'Xie', 'Xif'],
                 'Xii': ['Xiia', 'Xiib']
                }
    return(code_sets)


def has_subtotals(df, col_name):
    ''' Check for if a dataset has subtotals/aggregate codes, e.g) C39-C40

        Args:
            df - dataframe, our input dataset
            col_name - str, column name that would contain indicators for 
                            having subtotals/aggregate codes
        Returns: boolean indicating whether the dataframe contains subtotals in
            the column [col_name]
    '''
    subtot_indicators = [",", "-"]
    has_subtotals = (df.coding_system.eq("ICD10") & 
                     df[col_name].str.contains('|'.join(subtot_indicators)))
    return(df.loc[has_subtotals,:].any().any())


def components_present(df):
    ''' Checks for possibility of subtotal recalculation by checking all code
        components in dataset 
        
        Args:
            df - dataframe, our input dataset

        Returns: input dataframe with column of boolean of 
                whether any code-component pairs exist within
                the dataframe such that data could be recalculated    
    '''
    uid_cols = [col for col in md.get_uid_cols(2) if col != "cause"] + ['orig_cause']
    grouped = df.groupby(uid_cols)
    df.loc[:,'comp_present'] = False

    # names is list of current uid col row values
    for names, group in grouped: # group is current subset of our input df
        cur_causes = group['cause'].unique()
        other_causes = df.loc[~df['orig_cause'].eq(names[-1]), 'cause'].unique()
        # checking if our current group of causes overlaps with other causes in group
        if len(set(list(cur_causes)) - set(list(other_causes))) < len(set(list(cur_causes))):
            df.loc[:,'comp_present'] = True
            break
        else:
            continue
    return(df)


def remove_allCancer(df):
    ''' Removes entries signifying "all cancers" because they cannot be 
            recalculated 
    '''
    # grab from cancer cause map all subtotal entries
    # added this cause since cause disagg can't disaggregate this big of a code
    db_link = cdb_utils.db_api()
    custom = db_link.get_table("custom_cancer_map")
    allCancer_entries = list(custom.loc[(custom['gbd_cause'].eq("sub_total")) &
                                        (~custom['cause'].eq('')), 'cause'].unique())
    allCancer_entries = list(set(allCancer_entries + ["C00-C43, C45-C97", "C00-43, C45-97",
                                             "C00-C80", "C00-C94", "C00-C96", "C00-C97",
                                             "C00-C32", 'C00-C43,C45-C49,C51-C60,C62-C97']))
    output = df.loc[~df['cause'].str.contains('|'.join(allCancer_entries)), :]
    return(output)


def get_sr_file(ds_instance, which_file='key', splitNum=None):
    ''' Accepts an MI_Dataset class and returns the file associated with the 
            splitNum
        Args:
            ds_instance - MI_Dataset class, associated with our input dataset
            which_file - str, specifies which file we're grabbing
            splitNum - int, equivalent to the uid associated with the split of 
                            the dataset
        Returns: filename of file to grab, either our subtotal recalc input 
                 or our recalculated split output
    '''
    dsid = ds_instance.dataset_id
    dtid = ds_instance.data_type_id
    temp_dir = ds_instance.temp_folder
    print(temp_dir)
    if which_file == "sr_input":
        this_file = '{}/for_recalculation_{}.h5'.format(
            temp_dir, dtid)
    elif which_file == "split_output":
        this_file = "{}/{}_{}_split{}_sr.csv".format(
            temp_dir, dsid, dtid, splitNum)
    if this_file:
        utils.ensure_dir(this_file)
        return(this_file)


def reformat_input(df, ds_instance):
    ''' Collapse and reshape input data from standardize_format output
        from wide to long
        Args:
            df - dataframe, our input dataset
            ds_instance - MI_Dataset class, associated with our input dataset

        Returns: formatted dataset
    '''
    metric_name = ds_instance.metric
    uid_cols = md.get_uid_cols(2)
    def handle_missingness(df, ds_instance):
        ''' remove missingness in rows generated from codes in one dataset 
             not present in another (GBD2020 step 3 false 0s)'''

        metric_cols = [col for col in df.columns if ds_instance.metric in col]
        df['na_count'] = df[metric_cols].isnull().sum(axis=1) # get count of NAs in metric cols
        na_removed = df[~df['na_count'].eq(len(metric_cols))] # remove if all metric cols = NA
        na_removed.drop('na_count', axis = 1, inplace = True)
        # check to make sure totals match
        assert na_removed[metric_cols].sum(axis = 0).sum() == df[metric_cols].sum(axis = 0).sum(), \
            "{} are being removed".format(ds_instance.metric)
        return(na_removed)
    df = handle_missingness(df, ds_instance)

    input_df = df.copy()
    metric_cols = [col for col in input_df.columns if metric_name in col]
    input_df[metric_name] = input_df[metric_cols].sum(axis=1) # get sum for comparison later

    wide_uid_cols = [u for u in uid_cols if 'age' not in u]    
    tmp = [u for u in uid_cols if 'age' not in u] # wide_uid_cols
    tmp.remove('cause')
    tmp.remove('cause_name')
    uids_noCause = [u for u in uid_cols if 'cause' not in u]
    df.loc[df['im_frmat_id'].isnull() & df['frmat_id'].isin([9]), 'im_frmat_id'] = 9
    df = md.stdz_col_formats(df)
    # reshape e.g) cols cases9 cases10 become columns cases and age

    # handle data with more than one value for uid with collapse to preserve NAs
    if df.duplicated(subset = wide_uid_cols).any():
        # only aggregate duplicated subset
        df = dft.collapse(df, by_cols=wide_uid_cols, func='sum', stub=metric_name,
                              keepNA = True)
    df = dft.wide_to_long(df, stubnames=metric_name,
                          i=wide_uid_cols, j='age')
    df = df.loc[~df[metric_name].isnull()] # remove NAs for partial NAs
    df = df.groupby(uid_cols, as_index=False)[metric_name].sum()
    df = md.stdz_col_formats(df)
    df = dft.make_group_id_col(df, uids_noCause, id_col='uniqid')
    pt.verify_metric_total(input_df, df, metric_name, 
                            test_location="Subtotal reshape wide to long")
    return(df)
    
## #########################
# Define Functions
## ##########################


def count_negative_uids(uid_list, dataset_id, data_type_id): 
    ''' Counts number of uid groups 
        where we have negative subtotals and returns the numbers
    '''
    neg_uids = [] 
    this_dataset = md.MI_Dataset(dataset_id, 2, data_type_id)
    dataset_name = this_dataset.name
    error_folder = utils.get_path("mi_input", base_folder='j_temp')
    for u in uid_list: 
        exception_file = '{}/negative_data/{}_{}_uid_{}.csv'.format(
            error_folder, dataset_name, data_type_id, u)
        if (isfile(exception_file)): 
            neg_uids += [u]
    return(neg_uids)


def check_sr_jobs(output_path, output_files):
    ''' Quick checker for completion of SR jobs and also checks if output
        csvs are empty or not
        Args:
            output_path - where finished sr job files are saved
            uids - int, number of split sr jobs there are 

        Returns: boolean of whether jobs are finished or not
    '''
    sr_files = []
    # make sure output path exists
    try:
        os.mkdir(output_path)
        print("Directory " , output_path,  " Created ") 
    except FileExistsError:
        print("Directory " , output_path,  " already exists")
    #neg_uids = count_negative_uids(uids, dataset_id, data_type_id)
    # runs until all jobs are complete
    loop = 1
    while len(sr_files) < len(output_files):
        # check every 5 sec for job completion
        sr_files = [filepath for filepath in output_files if filepath is not None and os.path.isfile(filepath)]

        print("\nChecking again ... {} files remaining".
                                    format(len(output_files) - len(sr_files)))
        time.sleep(10)
        loop += 1

    # make sure files are not empty
    dfs = [pd.read_csv(f) for f in sr_files]
    empty_files = [df.empty for df in dfs]
    assert not(True in empty_files), \
        "There are empty split sr files!"
    print("Split sr csvs retrieved!")
    return(None)


def submit_sr(calc_df, this_dataset, is_resubmission):
    ''' Splits data based on subtotal-recalculation requirement and submits
            jobs as needed to recalculate subtotals. 
        Args:
            calc_df - dataframe, our input dataset
            this_dataset - MI_Dataset class, associated with our input dataset      
            is_resubmission - bool, [T,F] if we will run jobs in parallel or serially 

        Returns: a re-combined dataset with subtotals recalculated
    '''
    def submission_req(df, uid): 
        ''' Returns boolean indicating whether data are to be submitted, 
                qualified by whether subtotals are present and whether any 
                component codes exist that could enable recalculation
        '''
        uid_test = df[df['uniqid'].eq(uid)]
        meets_requirement = bool(has_subtotals(uid_test, 'orig_cause')
                    and uid_test['comp_present'].unique() == True)
        return(meets_requirement)

    def output_file_func(id):
        ''' Function fed to get_results to retrieve sr file output path
        '''
        return(get_sr_file(this_dataset, 'split_output', id))
    

    def clean_file_dir(path, data_type_id, typeOf = "output", is_resub = False):
        ''' Get a list of all the file paths that ends with .sr.csv 
         from in specified directory'''
        if not(is_resub):
            if typeOf == "output":
                fileList = glob.glob('{}/*_{}_*_sr.csv'.format(path, data_type_id))
            elif typeOf == "logs":
                fileList = glob.glob('{}/*'.format(path))
            elif typeOf == "input":
                fileList = glob.glob('{}/for_recalculation_{}*'.format(path, data_type_id)) + \
                            glob.glob('{}/*_{}_*_params.csv'.format(path, data_type_id))

            # Iterate over the list of filepaths & remove each file
            for filePath in fileList:
                try:
                    os.remove(filePath)
                except:
                    pass

    output_uids = md.get_uid_cols(3)
    dataset_id = this_dataset.dataset_id
    data_type_id = this_dataset.data_type_id
    metric_name = this_dataset.metric
    job_header = "cnSR_{}_{}".format(dataset_id, data_type_id)
    sr_input_file = get_sr_file(this_dataset, "sr_input")
    clean_file_dir(this_dataset.temp_folder, data_type_id, typeOf = "input", is_resub = is_resubmission)

    worker_script = utils.get_path("subtotal_recalculation_worker",
                                                        process="mi_dataset")
    # convert components to string to enable save in hdf file
    uniqid_map = calc_df[output_uids + ['uniqid', 'orig_cause']
                         ].copy().drop_duplicates()
    # save orig causes to uniqid_map
    temp_uids = [u for u in output_uids if u != "cause"]
    orig_causes = uniqid_map[temp_uids + ['uniqid', 'orig_cause']
                             ].copy().drop_duplicates()
    orig_causes['cause'] = orig_causes['orig_cause'].copy()
    uniqid_map = uniqid_map.append(orig_causes)
    uniqid_map = uniqid_map.drop_duplicates()
    # create jobs
    submitted_data, unsubmitted_data = cup.split_submission_data(calc_df, 
                                        group_id_col='uniqid',
                                        submission_requirement=submission_req, 
                                        hdf_file=sr_input_file,
                                        regenerate_hdf=False)
    # drop code components from unsubmitted_data
    unsubmitted_data['cause'] = unsubmitted_data['orig_cause'].copy()
    unsubmitted_data.drop_duplicates(inplace = True)

    if len(submitted_data) == 0:
        final_results = unsubmitted_data
    else:
        uid_list = submitted_data['uniqid'].unique().tolist()
        output_files = {str(uid):output_file_func(int(uid)) for uid in uid_list}
        output_files = output_files.values()

        clean_file_dir(this_dataset.temp_folder, data_type_id, is_resub=is_resubmission) # remove previous files

        def expand_grid(dictionary):
            ''' Function to get all combinations of our params job dict
            '''
            return pd.DataFrame([row for row in product(*dictionary.values())], 
                                columns=dictionary.keys())


        # splitting our uid list into groups of 500 <- suggested for array jobs
        uid_lists = [uid_list[i:i + 500] for i in range(0, len(uid_list), 500)]
        # launching our jobs in batches
        batch_num = 1
        clean_file_dir(output_path, data_type_id, typeOf = "logs", is_resub= is_resubmission) #clear logs
        for uid_batch in uid_lists:

            ## Save the parameters as a csv so then you can index the rows to find 
            #  the appropriate parameters
            n_jobs = len(uid_batch)
            params = {'dataset_id' : [this_dataset.dataset_id],
                        'data_type_id' : [this_dataset.data_type_id],
                        'uid' : uid_batch}

            params_grid = expand_grid(params)

            # set import. file paths
            param_file = "{}/cnSR_{}_{}_{}_params.csv".format(this_dataset.temp_folder, 
                                                            this_dataset.dataset_id,
                                                            this_dataset.data_type_id,
                                                            batch_num)
            params_grid.to_csv(param_file, index = False)

            output_path = "{}/cnSR_{}_jobs/".format(
                                        utils.get_path(key="cancer_logs", process = "common"),
                                        this_dataset.dataset_id, 
                                        this_dataset.data_type_id)

            error_path = "{}/cnSR_{}_jobs/".format(
                                        utils.get_path(key="cancer_logs", process = "common"),
                                        this_dataset.dataset_id, 
                                        this_dataset.data_type_id)

            # make sure error/ output paths exists
            try:
                os.mkdir(output_path)
                print("Directory " , output_path,  " Created ") 
            except FileExistsError:
                print("Directory " , output_path,  " already exists")

            job_name = "cnSR_{ds}_{dt}_{ds}_{dt}_{bn}".format(
                                        ds = this_dataset.dataset_id, 
                                        dt = this_dataset.data_type_id,
                                        bn = batch_num)
            
            runtime = "00:10:00" if len(uid_list) < 1000 else "00:20:00"
            mem = 5.0 if len(uid_list) < 1000 else 20.0
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
            subprocess.call(call, shell=True)
            batch_num += 1
            # keep checking jobs until complete then launch others
        check_sr_jobs(this_dataset.temp_folder, output_files)

        # keep checking jobs until complete

        # Re-combine compiled results with the set-aside data, before collapsing
        # and testing
        input_results= pe.read_files(output_files)
        input_results.rename(columns={'cause':'orig_cause','codes_remaining':'cause'},
                       inplace=True)
        results = md.stdz_col_formats(input_results, additional_float_stubs='uniqid')

        # remove entries where cause is empty since it has been fully removed
        results = results.loc[~results['cause'].eq('')]
        
        # don't want to merge on cause col because orig_causes with codes removed
        # won't have the same causes as the uniqid_map
        cols_to_keep = [col for col in uniqid_map.columns if col != "cause"]
        results = results.merge(uniqid_map[cols_to_keep].drop_duplicates(), 
                                how='left', indicator=True,
                                on = ['age', 'uniqid', 'orig_cause'])

        # drop unmatched entries from uniqid map's side
        # keep everything else, left_only merge are causes that had their
        # orig code changed
        results = results.loc[~results['_merge'].eq('right_only')]

        assert results['_merge'].isin(["both", "left_only"]).all(), \
            "Error merging with uids"
        del results['_merge']

        assert not(results.isnull().values.any()), \
            "There are null values in columns!"
        # entries with blank "cause" could not be corrected. replace with the 
        #   original aggregate (will be handled by cause recalculation and rdp).
        #results.loc[results['cause'].eq(""), 'cause'] = results['orig_cause']
        st.verify_sr(input_results, results, metric_name, 0.0000055, 0.01, 
                                            "Finished compiling subtotal results")
        # GBD2020: decision to not to drop zeroes from subtotal -> losing true 0s in data
        results = results.loc[results[metric_name].notnull(), :]
        final_results = results.append(unsubmitted_data)

    # Re-combine with data that were not split
    final_results = dft.collapse(final_results, by_cols=output_uids,
                                    combine_cols=this_dataset.metric)
    
    # check length to make sure not creating more rows
    assert len(final_results) <= len(calc_df), \
            "Additional rows generated after removing subtotals"
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


def drop_ICCC_aggregates(df, dataset_id):
    ''' Checks if dataset contains ICCC aggregates and allows user to manually 
        remove them before outputting a finalized file'''

    agg_subtotals = ['I-Xii', 'I-Xiib', 'I-X,Xii']
    has_agg = bool(df['cause'].str.contains('-').any() or 
                    df['cause'].str.contains(',').any()) and (
                    set(agg_subtotals).isdisjoint(set(df['cause'].unique())))
    # drop the large ICCC subtotal codes
    df = df.loc[~df['cause'].str.contains('|'.join(agg_subtotals)), :] 
    if has_agg:
        cont = input("Please check for and remove aggregate codes in \
                        dataset {}, then continue".format(dataset_id))
    return(df)


def remove_codes(group, dt_id, subcodes, main_code):
    ''' Helper worker function called by remove_ICCC_codes

        Removes main ICCC codes if subcodes are present and add up to main
        otherwise, removes subcodes and keeps main codes

        Args:
            group - dataframe, uid group subset removing codes on
            dt_id - int, data type of dataset
            subcodes - list of subcodes under main code
            main_code - str, current parent code we are checking subtotals on

        Returns: dataframe group with ICCC subtotals removed 
    '''
    dt_types = {2: "cases", 3: "deaths"}
    exist_codes = group['cause'].unique().tolist() # get current list of codes
    orig_group = group.copy().reset_index()

    main_sum = orig_group.loc[orig_group['is_main'].eq(1), dt_types[dt_id]].sum()
    subtype_sum = orig_group.loc[~orig_group['is_main'].eq(1), dt_types[dt_id]].sum()
    # when we have all subcodes and main code present
    if set(subcodes + [main_code]) == set(exist_codes) and len(subcodes) > 0:
        # create subtypes ds with sum of metric cols
        if main_sum == subtype_sum:
            # keep subtypes if subtypes add up to parent
            new_group = orig_group[~orig_group['cause'].eq(main_code)]
        else:
            # check if main code sum is 0
            if main_sum <= subtype_sum and main_sum == 0:
                # remove main codes that have sum = 0
                new_group = orig_group[~orig_group['cause'].eq(main_code)]
            else:
                # keep parent if subtypes don't add up to parent
                new_group = orig_group[orig_group['cause'].eq(main_code)]

    elif set(subcodes + [main_code]) > set(exist_codes) and main_code in exist_codes and len(exist_codes) > 1:
        # when subcodes are incomplete and main code is present in data, remove subcodes
        # and any other codes based on condition
        if (main_sum <= subtype_sum and main_sum == 0) or main_sum == subtype_sum:
            # delete main if main = 0 exception for main codes == 0, keep subcodes
            # or subtype sum = main
            new_group = orig_group[~orig_group['cause'].eq(main_code)]
        else:
            # keep main if subtype sum > main, or subtype sum < main
            new_group = orig_group[orig_group['cause'].eq(main_code)]
    else:
        # when we only have subcodes or we only have main codes
        new_group = orig_group.copy()
    return(new_group)


def remove_ICCC_codes(df, code_set, main_code, dt_id, metric):
    ''' Iterates through input dataset and checks for the prescence of 
        main ICCC codes e.g) III and child codes e.g) IIIa, IIIb

        Calls helper removal function for removing 
        main ICCC codes if subcodes are present and add up to main
        otherwise, removes subcodes and keeps main codes

        Args:
            df - dataframe, our input dataset
            code_set - list, children codes of main parent code
            main_code - str, main parent code in that subset of df
            dt_id - int, datatype_id
            metric - str, [cases, deaths]

        Returns: dataframe with subtotals in ICCC codes removed
    '''
    subcodes = code_set
    uid_cols = md.get_uid_cols(2)
    uid_cols = [uid_col for uid_col in uid_cols if not 'cause' in uid_col]
    exist_codes = df['cause'].unique().tolist()

    # defining parent codes
    df['is_main'] = np.where(df['cause'] == main_code, 1, 0)
    input_df = df[df['cause'].isin(subcodes + [main_code])]

    # checks for if the set of subcodes is present along with main code
    if not(set(subcodes).isdisjoint(exist_codes)) and main_code in exist_codes:
        final_df = input_df.groupby(uid_cols).apply(lambda x: 
                                                remove_codes(x, dt_id, subcodes, 
                                                main_code)).reset_index(drop=True)
    else:
        # when we only have subcodes or we have just the main code
        final_df = input_df.copy()

    # checks for data quality after removing ICCC codes
    assert (final_df[metric].sum() >= (0.5*input_df[metric].sum() - 0.0005) and \
                final_df[metric].sum() <= (input_df[metric].sum() + 0.0005)), \
                    "Dropping too many {} in removing ICCC codes".format(metric)
    return(final_df)


def main(dataset_id, data_type_id, is_resubmission):
    ''' Tests data for existence of subtotals and recalculates as possible 
            and necessary
    '''
    print(utils.display_timestamp())
    this_dataset = md.MI_Dataset(dataset_id, 2, data_type_id)
    metric = this_dataset.metric
    input_df = this_dataset.load_input()
    reformat_df = reformat_input(input_df, this_dataset)
    cleaned_input = remove_allCancer(reformat_df)

    orig_cols = input_df.columns
    # main section for subtotal for ICCC3 codes (Created in GBD2019)
    if 'ICCC3' in cleaned_input['coding_system'].unique():
        cleaned_input = drop_ICCC_aggregates(cleaned_input, this_dataset.dataset_id)
        iccc_df = cleaned_input[cleaned_input['coding_system'].eq("ICCC3")]
        iccc_codes = get_iccc_code_sets()
        total_iccc = []

        for main_code in iccc_codes.keys():
            iccc_recoded = remove_ICCC_codes(iccc_df, 
                                iccc_codes[main_code], main_code, data_type_id, metric)
            total_iccc.append(iccc_recoded)
        total_iccc = pd.concat(total_iccc)

        # check to make sure that we didn't drop any major parent codes
        uid_cols = md.get_uid_cols(2)
        total_iccc['main_cause'] = total_iccc['cause'].str.upper().str.split(r'[^(?!.*(X|V|I))]', 
                                            n=1, expand=True)[0]
        iccc_df['main_cause'] = iccc_df['cause'].str.upper().str.split(r'[^(?!.*(X|V|I))]', 
                                            n=1, expand=True)[0]
        total_iccc['is_present'] = 1
        uid_noCause = [uid for uid in uid_cols if 'cause' not in uid]
        check_iccc = total_iccc.merge(iccc_df[uid_noCause + ['main_cause']].drop_duplicates(),
                                        how = "right", on = uid_noCause + ['main_cause'])

        # data quality checks for after ICCC codes
        assert not check_iccc['is_present'].isnull().any(), \
            "Dropping ICCC codes for all of one group" 
        del total_iccc['is_present']

        assert len(total_iccc[total_iccc[metric].eq(0)]) <= len(iccc_df[iccc_df[metric].eq(0)]), \
            "Additional zeroes after removing ICCC codes"
    
        assert len(total_iccc) <= len(iccc_df), \
            "Additional rows generated after removing ICCC codes"

        assert total_iccc[metric].sum() >= (0.5*iccc_df[metric].sum() - 0.0005) and \
                total_iccc[metric].sum() <= (iccc_df[metric].sum() + 0.0005), \
                    "Dropping too many {} in removing ICCC codes".format(metric)
                
        # quick fix for subcodes with 1, 2, 3.. at the end by removing them
        remove_sub_subcodes = total_iccc[(total_iccc['coding_system'].eq("ICCC3")) 
                                                & (total_iccc['cause'].str.isalpha())]
        orig_code = cleaned_input[~cleaned_input['coding_system'].eq("ICCC3")]
        cleaned_input = pd.concat([orig_code, remove_sub_subcodes])
        
        cleaned_input = cleaned_input[orig_cols]
    # main section for subtotal for ICD10 codes
    if not has_subtotals(cleaned_input, 'cause'):
        print("No recalculation necessary.")
        md.complete_prep_step(cleaned_input, input_df, this_dataset)
        print("subtotal_recalculation complete")
        return(None)
    else:
        non_icd = cleaned_input[~cleaned_input.coding_system.eq("ICD10")]
        icd_total = cleaned_input[cleaned_input.coding_system.eq("ICD10")]

        # making sure all original uids are preserved
        orig_ages = icd_total['age'].unique()
        orig_ys = icd_total['year_start'].unique()
        orig_ye = icd_total['year_end'].unique()
        orig_reg = icd_total['registry_index'].unique()
        orig_sex = icd_total['sex_id'].unique()

        # Attach cause components to the cleaned input
        calc_df = icd_total.rename(columns={'cause': 'orig_cause'})

        sub_causes = code_components.run(calc_df) 
        calc_df = calc_df.merge(sub_causes)
        assert len(calc_df) > len(icd_total), \
            "Error during merge with subcauses"  

        # Test until a uid is discovered that reqires recalculation
        print("Verifying whether data can be recalculated...")
        uids = calc_df['uniqid'].unique()

        #mp.cpu_count()
        with Pool(processes = 8) as pool: 
            # have your pool map the file names to dataframes
            total_dfs = pool.map(components_present,
                                    [group for name, group in calc_df.groupby(['uniqid'])])

        print("Finished checking for sr components")
        calc_df = pd.concat(total_dfs) # append all dfs
        any_components = False
        if True in calc_df['comp_present'].unique().tolist():
            any_components = True 
        
        # Run recalculation only if necessary. Otherwise, output the cleaned data
        if any_components:
            # keep track of datasets with subtotal recalculation needed
            result_df = submit_sr(calc_df, this_dataset, is_resubmission)
            subtotals_recalculated = validate_sr(result_df, this_dataset)

            uid_cols = md.get_uid_cols(2)
            assert not(result_df[uid_cols].duplicated().any()), \
                        "Duplicated uids found!"

            # checking to make sure all original possible variable values 
            # were kept
            assert ((set(result_df['age'].unique()) == set(orig_ages)) &
                    (set(result_df['year_start'].unique()) == set(orig_ys)) &
                    (set(result_df['year_end'].unique()) == set(orig_ye)) &
                    (set(result_df['registry_index'].unique()) == set(orig_reg)) &
                    (set(result_df['sex_id'].unique()) == set(orig_sex))), \
                        "Rows were dropped! Not all age, year, registries, sex present"
            subtotals_recalculated = pd.concat([non_icd, subtotals_recalculated])
            print("Old total {}".format(icd_total[this_dataset.metric].sum()))

        else:
            print("\nNo recalculation necessary.")
            result_df = cleaned_input.copy()
            subtotals_recalculated = cleaned_input
            print("Old total {}".format(cleaned_input[this_dataset.metric].sum()))
        print("New total {}".format(result_df[this_dataset.metric].sum()))
        st.verify_sr(remove_allCancer(reformat_df), subtotals_recalculated, 
                                          this_dataset.metric, 
                                          upper_thres = 0.0000055,
                                          lower_thres = 0.40,
                                          test_location = "subtotals finishing up")
        md.complete_prep_step(subtotals_recalculated, input_df, this_dataset)
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
