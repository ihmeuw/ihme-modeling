import logging
import os

from codem.devQueries.best_query import get_feeders, get_cause_ids
from codem.reference.db_connect import query
from codem.joblaunch.CODEmTriple import CODEmTriple
from codem.joblaunch.CODEmTask import CODEmTask
from codem.joblaunch.CODEmWorkflow import CODEmWorkflow
from hybridizer.joblaunch.HybridTask import HybridTask
from codem.joblaunch.run_utils import new_models, list_check, change_model_status, get_model_dir
from codem.joblaunch.profiling import get_all_parameters

logger = logging.getLogger(__name__)


def check_completed_cov_selection(model_version_id, db_connection):
    model_dir = get_model_dir(model_version_id, db_connection=db_connection)
    return os.path.exists(os.path.join(model_dir, 'cv_selected.txt'))


def check_uploaded_submodels(model_version_id, db_connection):
    call = f"SELECT submodel_version_id FROM cod.submodel_version WHERE model_version_id = {model_version_id}"
    ids = query(call, db_connection)
    return not ids.empty


def get_hybrids_to_launch(central_run, decomp_step_id, gbd_round_id, sex_id,
                          db_connection='ADDRESS'):
    """
    Get all hybrids that need to be run
    :param central_run: (int)
    :param decomp_step_id: (int)
    :param gbd_round_id: (int)
    :param sex_id: (int) only this because we want to run multiple workflows at same time
    :param db_connection: (str)
    :return:
    """
    call = f'''
            SELECT MAX(cmv.model_version_id) AS model_version_id, cmv.cause_id, cmv.sex_id,
                cmv.model_version_type_id, cmv.age_start, cmv.age_end
            FROM cod.model_version cmv
            WHERE cmv.model_version_id NOT IN (
              SELECT cmr.child_id FROM cod.model_version_relation cmr
              INNER JOIN cod.model_version cmvv
              ON cmvv.model_version_id = cmr.parent_id
              WHERE cmvv.status = 1
            )
            AND cmv.description LIKE "central codem run {central_run}%%"
            AND cmv.gbd_round_id = {gbd_round_id}
            AND cmv.decomp_step_id = {decomp_step_id}
            AND cmv.status = 1
            AND cmv.sex_id = {sex_id}
            GROUP BY cmv.cause_id, cmv.sex_id, cmv.model_version_type_id, cmv.age_start, cmv.age_end
           '''
    df = query(call, db_connection)
    df = df.pivot_table(index=['cause_id', 'sex_id', 'age_start', 'age_end'],
                        columns=['model_version_type_id'],
                        values=['model_version_id'])
    df.columns = df.columns.droplevel()
    df.reset_index(inplace=True)
    df.columns = ['cause_id', 'sex_id', 'age_start', 'age_end', 'global', 'data_rich']
    valid_df = df.loc[(~df['global'].isnull()) & (~df['data_rich'].isnull())]
    return valid_df


def launch_hybrids(central_run, decomp_step_id, gbd_round_id, sex_id, db_connection,
                   conn_def='codem', user='USERNAME', min_padding=0, gb_padding=0, core_padding=0):
    """
    Launch the hybrid models for a central run, with two running at a time.
    :param central_run: (int)
    :param decomp_step_id: (int)
    :param gbd_round_id: (int)
    :param sex_id: (int)
    :param db_connection: (str)
    :param conn_def: (str)
    :param user: (str)
    :param min_padding: (int)
    :param gb_padding: (int)
    :param core_padding: (int)
    :return:
    """
    hybrids = get_hybrids_to_launch(central_run=central_run, gbd_round_id=gbd_round_id,
                                    sex_id=sex_id,
                                    decomp_step_id=decomp_step_id, db_connection=db_connection)
    tasks = []
    try:
        for index, row in hybrids.iterrows():
            tasks.append(HybridTask(global_model_version_id=int(row['global']),
                                    developed_model_version_id=int(row['data_rich']),
                                    conn_def=conn_def,
                                    user=user,
                                    upstream_tasks=tasks,
                                    min_padding=min_padding,
                                    gb_padding=gb_padding,
                                    core_padding=core_padding))
        wf = CODEmWorkflow(name=f'HybridLaunch_cr_{central_run}_decomp_{decomp_step_id}_sex_{sex_id}')
        wf.add_tasks(tasks)
        wf.run()
    except:
        logger.info("This has failed. Deleting all the hybrids!")
        for t in tasks:
            change_model_status(t.model_version_id, 6, db_connection)


def batch_relaunch(model_version_id, decomp_step_id, description='', wf_desc='',
                   db_connection='ADDRESS', use_covariates=True,
                   gbd_round_id=6,
                   gb_padding=20, min_padding=60, num_cores=20, create_new_models=False):
    logger.info("Batch relaunching.")
    model_versions = list_check(model_version_id)
    wf = CODEmWorkflow(name=f'failed_relaunch' + wf_desc, description=wf_desc)
    tasks = []
    for m in model_versions:
        uploaded = check_uploaded_submodels(m, db_connection)
        if uploaded:
            logger.info(f"This model {m} has uploaded submodel info already. Need a fresh model version.")
        if create_new_models or uploaded:
            logger.info(f"Creating a new model version for {m}.")
            desc = description + f", relaunch of failed model {m}"
            completed_covariate_selection = check_completed_cov_selection(m, db_connection)
            skip_covariate_selection = completed_covariate_selection and use_covariates
            run_covariate_selection = not skip_covariate_selection
            if run_covariate_selection:
                logger.info("Will be running covariate selection for this new model.")
            else:
                logger.info("This new model will go not through covariate selection.")
                desc = desc + ' using its already completed covariates'
            new_m = new_models(list_of_models=[m], db_connection=db_connection, gbd_round_id=gbd_round_id,
                               decomp_step_id=decomp_step_id, desc=desc,
                               run_covariate_selection=run_covariate_selection)[0]
            logger.info(f"Change model status for {m} to failed.")
            change_model_status(m, status=6, db_connection=db_connection)
        else:
            logger.info(f"DO NOT NEED A NEW MODEL, REUSING {m}")
            new_m = m
        tasks.append(CODEmTask(model_version_id=new_m, gbd_round_id=gbd_round_id, db_connection=db_connection,
                               cores=num_cores, gb_padding=gb_padding, min_padding=min_padding))
    wf.add_tasks(tasks)
    return wf


def check_model_attribute(model_version_id, model_attribute_name,
                          model_attribute, db_connection='ADDRESS'):
    """
    Checks that the specific model version is truly associated with the model attribute
    that is specified.
    :param model_version_id:
    :param model_attribute_name:
    :param model_attribute:
    :param db_connection:
    :return:
    """
    call = '''
        SELECT {} FROM cod.model_version WHERE model_version_id = {}
        '''.format(model_attribute_name, model_version_id)
    if not query(call, db_connection)[model_attribute_name].iloc[0] == model_attribute:
        raise ValueError('The model attribute for {} in model_version {} does not match up!'.
                         format(model_attribute_name, model_version_id))


def check_sex_restrictions(cause_id, sex_id, gbd_round_id=5, db_connection='ADDRESS'):
    sex = {1: 'male', 2: 'female'}[sex_id]
    call = '''
        SELECT 
            {}
        FROM
            shared.cause_hierarchy_history ch
                INNER JOIN
            shared.cause_set_version_active ca ON ca.cause_set_version_id = ch.cause_set_version_id
        WHERE
            cause_id = {}
            AND ca.gbd_round_id = {}
            AND ca.cause_set_id = 3
        '''.format(sex, cause_id, gbd_round_id)
    valid = query(call, db_connection)[sex].iloc[0]
    if valid is None:
        valid = 0
    return bool(valid)


def demographic_launch(acause, age_start=None,
                       age_end=None, sex_id=1,
                       description='null',
                       gbd_round_id=5,
                       decomp_step_id=1,
                       db_connection='ADDRESS',
                       pre_gbd_round_id=None,
                       pre_decomp_step_id=None,
                       num_cores=20,
                       codem_parameter_dict=None,
                       hybrid_parameter_dict=None):
    """
    Launch a full cause-age-sex datarich, global pair plus a hybrid model.
    """
    logger.info("Demographic launch for {}".format(acause))
    sex_id = list_check(sex_id)
    acause = list_check(acause)
    if pre_gbd_round_id is None:
        pre_gbd_round_id = gbd_round_id
    if pre_decomp_step_id is None:
        pre_decomp_step_id = decomp_step_id
    if age_start is None and age_end is None:
        age_range = None
    elif age_start is not None and age_end is not None:
        age_range = [age_start, age_end]
    else:
        raise ValueError("Need to pass either both valid age start / ends, or both as None.")
    tasks = []
    for cause in acause:
        logger.info(f"Getting models for {cause}")
        if len(get_cause_ids(cause)) > 1:
            raise RuntimeError("There are more than 1 cause IDs for this acause! Fix!")
        cause_id = get_cause_ids(cause)[0]
        if codem_parameter_dict is None:
            codem_params = get_all_parameters(cause_ids=cause_id, hybridizer=False)
        else:
            codem_params = codem_parameter_dict
        if hybrid_parameter_dict is None:
            hybrid_params = get_all_parameters(cause_ids=cause_id, hybridizer=True)
        else:
            hybrid_params = hybrid_parameter_dict
        for s in sex_id:
            global_model = get_feeders(cause_ids=cause_id,
                                       age_range=age_range,
                                       sex_id=s,
                                       model_version_type_id=1,
                                       gbd_round_id=pre_gbd_round_id,
                                       decomp_step_id=pre_decomp_step_id,
                                       db_connection=db_connection)['child_id'].iloc[0]
            developed_model = get_feeders(cause_ids=cause_id,
                                          age_range=age_range,
                                          sex_id=s,
                                          model_version_type_id=2,
                                          gbd_round_id=pre_gbd_round_id,
                                          decomp_step_id=pre_decomp_step_id,
                                          db_connection=db_connection)['child_id'].iloc[0]
            task_list = CODEmTriple(developed_model_version_id=developed_model,
                                    global_model_version_id=global_model,
                                    description=description,
                                    db_connection=db_connection,
                                    gbd_round_id=gbd_round_id,
                                    decomp_step_id=decomp_step_id,
                                    num_cores=num_cores,
                                    codem_params=codem_params[cause_id],
                                    hybridizer_params=hybrid_params[cause_id]).task_list()
            tasks.extend(task_list)
    return tasks


def version_launch(global_model_version_id, developed_model_version_id,
                   description,
                   gbd_round_id=5,
                   decomp_step_id=1,
                   num_cores=20,
                   db_connection='ADDRESS'):
    logger.info("Version launch for {} and {}".format(global_model_version_id, developed_model_version_id))
    task_list = CODEmTriple(developed_model_version_id=developed_model_version_id,
                            global_model_version_id=global_model_version_id,
                            db_connection=db_connection,
                            description=description,
                            gbd_round_id=gbd_round_id,
                            decomp_step_id=decomp_step_id,
                            num_cores=num_cores).task_list()
    return task_list


def individual_launch(model_version_id, description, db_connection,
                      gbd_round_id=5, decomp_step_id=1,
                      gb_padding=50, min_padding=60*12, core_padding=0,
                      num_cores=20):
    model_versions = new_models(model_version_id, db_connection=db_connection,
                                desc=description,
                                gbd_round_id=gbd_round_id, decomp_step_id=decomp_step_id)
    task_list = [CODEmTask(model_version_id=m,
                           db_connection=db_connection,
                           gbd_round_id=gbd_round_id,
                           gb_padding=gb_padding,
                           min_padding=min_padding,
                           core_padding=core_padding,
                           cores=num_cores) for m in model_versions]
    return task_list

