from argparse import ArgumentParser, Namespace
import datetime
import getpass
import os
import pandas as pd

from db_queries import get_demographics
from gbd.estimation_years import estimation_years_from_gbd_round_id
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.python_task import PythonTask
from jobmon.client.swarm.workflow.workflow import Workflow

path_to_directory = os.path.dirname(os.path.abspath(__file__))
print(path_to_directory)

def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument('--gbd_round_id', type=int)
    parser.add_argument('--decomp_step', type=str)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    years = estimation_years_from_gbd_round_id(args.gbd_round_id)
    run_id = '10107'
    out_dir = f'FILEPATH'
    user = getpass.getuser()
    workflow = Workflow(
        workflow_args=f'1anemia_causal_attribution_GBD20_{run_id}_runandsave',
        name=f'anemia_causal_attribution_{run_id}',
        description=f'Anemia: Causal attribution for decomp {args.decomp_step}',
        project="proj_anemia",
        stderr=f'FILEPATH',
        stdout=f'FILEPATH',
        working_dir=path_to_directory,
        resume=True)

    causal_attribution_tasks = []
    demo = get_demographics("epi", gbd_round_id=args.gbd_round_id)
    for location_id in demo['location_id']:
        prev_year_task = None
        for year in years:

            cmd = (
                f'FILEPATH '
                f'-s {path_to_directory}/causal_attribution.R '
                f'{location_id} {year} {args.gbd_round_id} {args.decomp_step} '
                f'{path_to_directory}/ {out_dir}'
            )
            if prev_year_task:
                task = BashTask(
                    command=cmd,
                    name=f'causal_attribution_{location_id}_{year}',
                    tag='causal_attribution',
                    upstream_tasks=[prev_year_task],
                    num_cores=1,
                    m_mem_free='4G',
                    max_attempts=3,
                    max_runtime_seconds=60*60*2,
                    queue='all.q')
            else:
                task = BashTask(
                    command=cmd,
                    name=f'causal_attribution_{location_id}_{year}',
                    tag='causal_attribution',
                    num_cores=1,
                    m_mem_free='4G',
                    max_attempts=3,
                    max_runtime_seconds=60*60*2,
                    queue='all.q')
            causal_attribution_tasks.append(task)
            prev_year_task = task
    workflow.add_tasks(causal_attribution_tasks)

    # once the draws exist, save results
    output_meids = pd.read_excel(f"{path_to_directory}/in_out_meid_map_2020.xlsx", "out_meids")[['subtype','meid_mild','meid_moderate','meid_severe','prop_mild','prop_moderate','prop_severe','prop_without']]
    saveresults_model_source_mes = pd.read_excel(f"in_out_meid_map_2020.xlsx", "in_meids")[['subtype', 'modelable_entity_id']]

    residual_saveresults_model_source = 10487
    saveresults_model_source_mes = saveresults_model_source_mes.fillna(residual_saveresults_model_source)
    saveresults_model_source_mes.rename(columns={'modelable_entity_id':'source_meid'}, inplace=True)
    meid_map_me_columns = ['meid_mild', 'meid_moderate', 'meid_severe', 'prop_mild','prop_moderate','prop_severe','prop_without']  
    meids_to_save = output_meids.merge(saveresults_model_source_mes, how='left', on='subtype')
    for df in [meids_to_save[[x, 'source_meid']] for x in meid_map_me_columns]:
        for (_, output_me, saveresults_data_source_me) in df.rename(columns={df.columns[0]:'target_me'}).dropna().itertuples():
            task = PythonTask(
                script=f"{path_to_directory}/save_causal_attribution.py",
                args=[
                    "--modelable_entity_id", int(output_me),
                    "--year_id", " ".join([str(yr) for yr in years]),
                    "--gbd_round_id", args.gbd_round_id,
                    "--decomp_step", args.decomp_step,
                    "--modelversion_source", int(saveresults_data_source_me),
                    "--save_dir", f"{out_dir}/{int(output_me)}"
                ],
                name=f"save_{int(output_me)}",
                tag="save",
                upstream_tasks=causal_attribution_tasks,
                num_cores=8,
                m_mem_free="100G",
                max_attempts=1,
                max_runtime_seconds=60*60*24,
                queue='all.q')
            workflow.add_task(task)
    
    status = workflow.run()
    print(f'Workflow finished with status {status}')

if __name__ == '__main__':
    main()
