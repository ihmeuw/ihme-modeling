#!  FILEPATH
# shebang forces use of specific environment
# -*- coding: utf-8 -*-
'''
Description: Splits the parent by cause_id (cid) into child causes by cid using
    the modelable_entity_ids (meids) of the proportion models. 
Note: Currently contains split_manager and function that submits ids to manager
    for liver cancer. However, split_manager can accept ids for any cause.
How To Use: call script from production cluster: must use cancer environment or
    gbd environment and  must request significant memory for the qlogin
Contributors: NAME
'''

from split_models.split_cod import split_cod_model
from save_results import save_results_cod
from cancer_estimation.py_utils import common_utils as utils
from cancer_estimation.py_utils import cluster_tools
import time
import os



def manage_split(source_cid, target_cids, proportion_meids, work_dir, description):
    ''' Manages the split of the source_cid followed by saving of the targets, 
            then returns boolean indication of success
    '''
    utils.ensure_dir(work_dir)
    # split model
    d_step = utils.get_gbd_parameter('current_decomp_step')
    df = split_cod_model(source_cause_id=source_cid,
                         target_cause_ids=target_cids,
                         target_meids=proportion_meids,
                         output_dir=work_dir,
                         decomp_step=d_step
                         )
    print(
       print("Split data saved to " + work_dir + " at " +
             utils.display_timestamp()))
    # Generate a list of arguments (one for each child me)
    save_args_template = "--target {targ} --desc {desc} --indir {dir}"
    save_arg_list = []
    for t in target_cids:
        save_arg_list += [save_args_template.format(targ=t,
                                                    desc=description,
                                                    dir=work_dir)
                          ]
    # Start jobs
    header = description.replace(" ", "_")
    save_worker = utils.get_path("save_cod_worker", process="cancer_model")
    job_dict = cluster_tools.create_jobs(script_path=save_worker,
                                         job_header=description,
                                         memory_request=50,
                                         id_list=target_cids,
                                         script_args=save_arg_list,
                                         use_argparse=True,
                                         project_name="cancer")
    for i in job_dict:
        job_dict[i]['job'].launch()

    # Check for results
    job_descrip = description + " upload"
    success_df = cluster_tools.wait_for_results(job_dict,
                                                jobs_description=job_descrip,
                                                noisy_checker=False,
                                                max_minutes=30)
    success = cluster_tools.validate_success(success_df, description)
    return(success)


def split_liver():
    ''' Submits the liver-cancer-specific information to the split manager
    '''
    # set source and targets
    source_cid = 417  # parent cause_id
    target_cids = [996, 418, 419, 420, 421]  # cause_ids
    proportion_meids = [18763, 2470, 2471, 2472, 2473]  # proportion me_ids
    years = list(range(1980, int(utils.get_gbd_parameter("max_year")) + 1))
    description = "lvr_cncr_split"
    liver_model_path = utils.get_path(
        'cod_splits', process='cancer_model', base_folder='workspace')
    work_dir = "{}/{}".format(liver_model_path, utils.display_timestamp())
    # Run split
    success = manage_split(source_cid, target_cids, proportion_meids, work_dir,
                           description)
    if success:
        print("All CoD liver splits uploaded. " + utils.display_timestamp())
    else:
        print("Error during CoD splits for liver cancer")


if __name__ == "__main__":
    split_liver()
