'''
Description: Uses shared function to apply proportions that split a 
    modelable entity, then submits jobs to individually upload the results of 
    those splits
How To Use: call script from production cluster: must use cancer environment or
    gbd environment and  must request significant memory for the qlogin
'''

import subprocess
import sys
import os
import getpass
from cancer_estimation.py_utils import (
    common_utils as utils,
    cluster_tools
)
from cancer_estimation._database import cdb_utils as cdb
from cancer_estimation.c_models.e_nonfatal import nonfatal_dataset as nd
from collections import OrderedDict
from split_models.split_epi import split_epi_model



def get_work_dir(parent_meid, cnf_model_version_id):
    ''' Returns the work directory for the current split
        -- Inputs
            parent_meid : modelable_entity_id of the parent modelable_entity 
                that is being split
            cnf_model_version_id : the run_id of the cancer nonfatal model that is
                to be split
    '''
    work_dir = "{root}/cnf_run_{model}/{parent}".format(
        root=utils.get_path('epi_splits', process='cancer_model'),
        model=cnf_model_version_id,
        parent=parent_meid)
    return(work_dir)


def get_me_info(modelable_entity_id,parent_meid):
    ''' Returns the modelable_entity_ids (meids) of the "child" (component) MEs,
            meids of the proportions to generate those children MEs, and the 
            me_tag of the survival phase associated with the modelable entity
    '''
    # Load modelable_entity_ids from the cnf_model_entity_table.
    db_link = cdb.db_api("cancer_db")
    me_table = db_link.get_table('cnf_model_entity')
    parent_info = me_table.loc[me_table['modelable_entity_id'].eq(parent_meid), :
                               ].sort_values('me_tag').copy().reset_index()
    parent_cause = parent_info['acause'].item()
    me_tag = parent_info['me_tag'].item()
    me_info = me_table[me_table['is_active'].eq(1) &
                       me_table['acause'].str.startswith(parent_cause) &
                       me_table['me_tag'].isin([me_tag, "subcause_proportion"]) &
                       me_table['cancer_model_type'].eq("split_custom_epi")
                       ].sort_values(['acause', 'me_tag']).copy().reset_index()
    children_mes = me_info.loc[me_info['me_tag'].eq(me_tag),
                               'modelable_entity_id'].to_dict(into=OrderedDict)
    proportion_mes = me_info.loc[me_info['me_tag'].eq("subcause_proportion"),
                                 'modelable_entity_id'].to_dict(into=OrderedDict)
    return([c for c in children_mes.values()],
           [p for p in proportion_mes.values()],
           me_tag)


def get_measures(me_tag):
    ''' Returns expected measure_ids associated with the me_tag for the 
            modelable_entity (ids for "prevalence" and possibly "incidence")
    '''
    if me_tag == "primary_phase":
        measures = [5, 6]
    else:
        measures = [5]
    return(measures)


def split_estimates(modelable_entity_id, cnf_model_version_id):
    ''' Call shared function to apply proportions that split the parent 
            modelable entity into component modelable_entities
    '''
    def output_file_func(id):
        return (nd.nonfatalDataset("upload", id).get_output_file('upload'))

    parent_me = modelable_entity_id
    work_dir = get_work_dir(parent_me, cnf_model_version_id)
    utils.ensure_dir(work_dir)
    children_mes, proportion_mes, me_tag = get_me_info(
        modelable_entity_id, parent_me)
    measures = get_measures(me_tag)
    # Clear the work directory (required), then split the model
    utils.clean_directory_tree(work_dir)
    d_step = utils.get_gbd_parameter('current_decomp_step')
    gbd_id = utils.get_gbd_parameter('current_gbd_round')
    
    if modelable_entity_id == 1678: 
        meas_ids = [5,6] 
    else: 
        meas_ids = [5]
    split_epi_model(source_meid=parent_me,
                    target_meids=children_mes,
                    prop_meids=proportion_mes,
                    decomp_step=d_step,
                    split_measure_ids=meas_ids,
                    gbd_round_id=gbd_id,
                    output_dir=work_dir
                    )
    print("split data saved to " + work_dir)


def save_splits(modelable_entity_id, cnf_model_version_id):
    ''' Launch jobs to upload each of the "split" modelable entities, generated 
            by splitting the parent modelable entity
    '''
    def output_file_func(id):
        return (nd.nonfatalDataset("upload", id).get_output_file('upload'))
    parent_me = modelable_entity_id
    work_dir = get_work_dir(parent_me, cnf_model_version_id)
    this_step = nd.nonfatalDataset("split", parent_me)
    success_file = this_step.get_output_file('upload')
    children_mes, skip_mes, me_tag = get_me_info(
        modelable_entity_id, parent_me)
    measures = get_measures(me_tag)
    save_worker = utils.get_path('save_epi_worker', process='cancer_model')
    # Generate a list of arguments (one for each child me)
    description = "{}_run_{}".format(me_tag, cnf_model_version_id)
    save_args_template = ("--meid {meid} --meas_id {meas} --indir {input_dir}"
                          " --cnf_run_id {cnf_rid} --desc {desc}")
    save_arg_list = []
    for cm in children_mes:
        save_arg_list += [save_args_template.format(meid=cm,
                                                    meas=" ".join(
                                                        [str(m) for m in measures]),
                                                    desc=description,
                                                    input_dir="{}/{}".format(
                                                        work_dir, cm),
                                                    cnf_rid=cnf_model_version_id)]
    # Start jobs
    job_dict = cluster_tools.create_jobs(script_path=save_worker,
                                         job_header="lvr_save_epi",
                                         memory_request=90,
                                         id_list=children_mes,
                                         script_args=save_arg_list,
                                         use_argparse=True,
                                         project_name="cancer")
    for i in job_dict:
        job_dict[i]['job'].launch()
    # Check for results
    job_description = str(modelable_entity_id) + " split upload"
    success_df = cluster_tools.wait_for_results(job_dict,
                                                jobs_description=job_description,
                                                noisy_checker=False,
                                                output_file_function=output_file_func,
                                                max_minutes=30)
    success = cluster_tools.validate_success(success_df, job_description)
    if success:
        success_df.to_csv(success_file, index=False)
        return(True)
    else:
        print("Error during split")
        return(False)


def main():
    parent_meid = int(sys.argv[1])
    cnf_model_version_id = int(sys.argv[2])
    split_estimates(parent_meid, cnf_model_version_id)
    save_splits(parent_meid, cnf_model_version_id)



if __name__ == '__main__':
    main()
