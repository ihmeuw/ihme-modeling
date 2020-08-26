"""
This part of the code is responsible to queueing jobs that need be run.
"""

from argparse import ArgumentParser, Namespace
import datetime
import getpass
import os
import pandas as pd

from jobmon.client.swarm.workflow.python_task import PythonTask
from jobmon.client.swarm.workflow.workflow import Workflow

path_to_directory = "FILEPATH"


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument('--year_id', nargs='*', type=int)
    parser.add_argument('--gbd_round_id', type=int)
    parser.add_argument('--decomp_step', type=str)
    parser.add_argument('--out_dir', type=str)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    user = getpass.getuser()
    today_string = datetime.date.today().strftime('%m%d%y')
    workflow = Workflow(
        workflow_args=f'anemia_post_interp_temp_{args.decomp_step}_{today_string}',
        name=f'anemia_post_{args.decomp_step}_{today_string}',
        description=f'Anemia: Post-processing for decomp {args.decomp_step}',
        project="proj_anemia",
        stderr="FILEPATH",
        stdout="FILEPATH",
        working_dir=path_to_directory,
        resume=True)

    anemia_causes = ('hiv', 'pud', 'gastritis', 'esrd_dialysis', 'ckd3', 'ckd4', 'ckd5', 'cirrhosis')

    for anemia_cause in anemia_causes:

        # load in the info table
        info_df = pd.read_csv("FILEPATH")
        new_me_id_list = info_df['proportion_me'].tolist()

        # submit compute job for each me_id
        compute_prop_tasks = []
        for year in args.year_id:
            task = PythonTask(
                script="FILEPATH",
                args=[
                    "--year_id", year,
                    "--anemia_cause", anemia_cause,
                    "--gbd_round_id", args.gbd_round_id,
                    "--decomp_step", args.decomp_step,
                    "--out_dir", args.out_dir
                ],
                name=f"make_{anemia_cause}_props_{year}",
                tag="compute_props",
                num_cores=1,
                m_mem_free="12G",
                max_attempts=3,
                max_runtime_seconds=60*60*2,
                queue='all.q')
            compute_prop_tasks.append(task)
        workflow.add_tasks(compute_prop_tasks)

        # submit save result jobs after compute jobs finish
        for new_me_id in new_me_id_list:
            task = PythonTask(
                script="FILEPATH",
                args=[
                    "--modelable_entity_id", new_me_id,
                    "--year_ids", " ".join([str(yr) for yr in args.year_id]),
                    "--gbd_round_id", args.gbd_round_id,
                    "--decomp_step", args.decomp_step,
                    "--save_dir", 'FILEPATH'
                ],
                name=f"save_props_{new_me_id}",
                tag="save_props",
                upstream_tasks=compute_prop_tasks,
                num_cores=8,
                m_mem_free="90G",
                max_attempts=3,
                max_runtime_seconds=60*60*8,
                queue='long.q')
            workflow.add_task(task)

    status = workflow.run()
    print(f'Workflow finished with status {status}')


if __name__ == '__main__':
    main()
