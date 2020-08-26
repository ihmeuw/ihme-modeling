import pandas as pd
import argparse
import logging
from datetime import datetime

from codem.joblaunch.CODEmWorkflow import CODEmWorkflow
from codem.joblaunch.batch_launch import demographic_launch, version_launch, individual_launch
from codem.joblaunch.batch_launch import check_model_attribute, check_sex_restrictions
from codem.joblaunch.profiling import get_all_parameters

logger = logging.getLogger(__name__)


def read_file(wave, filepath):
    df = pd.read_csv(filepath)
    df = df.loc[df.wave == wave]
    return df


def get_tasks(df, db_connection, gbd_round_id, decomp_step_id, description,
              pre_decomp_step_id=None, pre_gbd_round_id=None,
              num_cores=20,
              start_date=datetime(2019, 1, 11), end_date=datetime(2019, 3, 6)):
    model_cols = ['global_male', 'global_female',
                  'developed_male', 'developed_female']
    manual = df.loc[~df[model_cols].isnull().all(axis=1)]
    auto = df.loc[df[model_cols].isnull().all(axis=1)]

    pre_decomp_step_id = decomp_step_id if pre_decomp_step_id is None else pre_decomp_step_id
    pre_gbd_round_id = gbd_round_id if pre_gbd_round_id is None else pre_gbd_round_id

    task_list = []

    cause_ids = auto['cause_id'].unique().tolist()
    codem_parameter_dict = get_all_parameters(cause_ids, hybridizer=False,
                                              start_date=start_date, end_date=end_date)
    hybrid_parameter_dict = get_all_parameters(cause_ids, hybridizer=True,
                                               start_date=start_date, end_date=end_date)

    for index, row in auto.iterrows():
        for sex_id in [1, 2]:
            if not check_sex_restrictions(row['cause_id'], sex_id):
                continue
            task_list.extend(demographic_launch(acause=row['acause'],
                                                age_start=row['age_start'],
                                                age_end=row['age_end'],
                                                sex_id=sex_id,
                                                db_connection=db_connection,
                                                pre_decomp_step_id=pre_decomp_step_id,
                                                decomp_step_id=decomp_step_id,
                                                pre_gbd_round_id=pre_gbd_round_id,
                                                gbd_round_id=gbd_round_id,
                                                description=description,
                                                num_cores=num_cores,
                                                codem_parameter_dict=codem_parameter_dict,
                                                hybrid_parameter_dict=hybrid_parameter_dict))
    for index, row in manual.iterrows():
        for sex_id in [1, 2]:
            if not check_sex_restrictions(row['cause_id'], sex_id):
                continue
            if sex_id == 1:
                developed_model_version_id = row['developed_male']
                global_model_version_id = row['global_male']
            elif sex_id == 2:
                developed_model_version_id = row['developed_female']
                global_model_version_id = row['global_female']
            else:
                raise ValueError("Need to pass sex_id 1 or 2!")
            for mv in [global_model_version_id, developed_model_version_id]:
                check_model_attribute(mv, 'sex_id', sex_id)
                check_model_attribute(mv, 'age_start', row['age_start'])
                check_model_attribute(mv, 'age_end', row['age_end'])
                check_model_attribute(mv, 'cause_id', row['cause_id'])
            check_model_attribute(global_model_version_id, 'model_version_type_id', 1)
            check_model_attribute(developed_model_version_id, 'model_version_type_id', 2)
            task_list.extend(version_launch(global_model_version_id=global_model_version_id,
                                            developed_model_version_id=developed_model_version_id,
                                            db_connection=db_connection,
                                            gbd_round_id=gbd_round_id,
                                            decomp_step_id=decomp_step_id,
                                            num_cores=num_cores,
                                            description=description))
    return task_list


def collect_tasks(wave, filepath,
                  db_connection, gbd_round_id,
                  decomp_step_id,
                  description,
                  addmodels_filepath=None,
                  pre_decomp_step_id=None,
                  pre_gbd_round_id=None,
                  num_cores=20,
                  start_date=datetime(2019, 1, 11),
                  end_date=datetime.now()):
    df = read_file(wave, filepath)
    tasks = get_tasks(df, db_connection=db_connection,
                      gbd_round_id=gbd_round_id,
                      decomp_step_id=decomp_step_id,
                      pre_gbd_round_id=pre_gbd_round_id,
                      pre_decomp_step_id=pre_decomp_step_id,
                      description=description,
                      start_date=start_date,
                      end_date=end_date,
                      num_cores=num_cores)

    if addmodels_filepath is not None:
        add_models = pd.read_csv(addmodels_filepath)['model_version_id'].tolist()
        add_ons = individual_launch(add_models, db_connection=db_connection,
                                    gbd_round_id=gbd_round_id,
                                    decomp_step_id=decomp_step_id,
                                    description=description,
                                    num_cores=num_cores)
        tasks.extend(add_ons)

    return tasks


def create_workflow(wave, central_run):
    wf = CODEmWorkflow(name='codem_central_run_{}_wave_{}'.
                       format(central_run, wave),
                       description='central codem run {}, wave {}'.
                       format(central_run, wave))
    return wf


def main():

    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument('central_run', type=int)
    parser.add_argument('wave', type=int)
    parser.add_argument('gbd_round_id', type=int)
    parser.add_argument('decomp_step_id', type=int)
    parser.add_argument('wave_filepath', type=str)
    parser.add_argument('num_cores', type=int)
    parser.add_argument('-add', '--addmodels', type=str, required=False)
    parser.add_argument('-pre_decomp', '--pre_decomp_step_id', type=int, required=False)
    parser.add_argument('-pre_gbd', '--pre_gbd_round_id', type=int, required=False)
    args = parser.parse_args()

    central_run = int(args.central_run)
    wave = int(args.wave)
    gbd_round_id = int(args.gbd_round_id)
    decomp_step_id = int(args.decomp_step_id)
    wave_filepath = str(args.wave_filepath)
    num_cores = int(args.num_cores)
    addmodels_filepath = None if isinstance(args.addmodels, type(None)) \
        else str(args.addmodels)
    pre_decomp_step_id = decomp_step_id if isinstance(args.pre_decomp_step_id, type(None)) \
        else int(args.pre_decomp_step_id)
    pre_gbd_round_id = gbd_round_id if isinstance(args.pre_gbd_round_id, type(None)) \
        else int(args.pre_gbd_round_id)

    wf = create_workflow(wave=wave, central_run=central_run)
    tasks = collect_tasks(wave=wave, filepath=wave_filepath,
                          addmodels_filepath=addmodels_filepath,
                          description='central codem run {}, wave {}'.format(central_run, wave),
                          db_connection='ADDRESS',
                          gbd_round_id=gbd_round_id,
                          decomp_step_id=decomp_step_id,
                          pre_gbd_round_id=pre_gbd_round_id,
                          pre_decomp_step_id=pre_decomp_step_id,
                          num_cores=num_cores)
    wf.add_tasks(tasks)
    wf.run()


if __name__ == '__main__':
    main()
