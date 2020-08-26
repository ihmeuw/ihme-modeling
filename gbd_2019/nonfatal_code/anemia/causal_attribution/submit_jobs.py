from argparse import ArgumentParser, Namespace
import datetime
import getpass
import os
import pandas as pd

from db_queries import get_demographics
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.python_task import PythonTask
from jobmon.client.swarm.workflow.workflow import Workflow

path_to_directory = os.path.dirname(os.path.abspath(__file__))
print(path_to_directory)


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
        workflow_args=f'anemia_causal_attribution_new_{args.decomp_step}_{today_string}',
        name=f'anemia_causal_attribution_{args.decomp_step}_{today_string}',
        description=f'Anemia: Causal attribution for decomp {args.decomp_step}',
        project="proj_anemia",
        stderr="FILEPATH",
        stdout="FILEPATH",
        working_dir=path_to_directory,
        resume=True)

    causal_attribution_tasks = []
    demo = get_demographics("epi", gbd_round_id=args.gbd_round_id)
    for location_id in demo['location_id']:
        prev_year_task = None
        for year in args.year_id:
            cmd = (
                f'FILEPATH '
                f'FILEPATH '
                f'FILEPATH '
                f'{location_id} {year} {args.gbd_round_id} {args.decomp_step} '
                f'{path_to_directory}/ {args.out_dir}'
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
    meids = pd.read_excel("FILEPATH")
    meids = meids.filter(like='modelable_entity').values.flatten()
    for modelable_entity_id in meids.tolist():
        task = PythonTask(
            script="FILEPATH",
            args=[
                "--modelable_entity_id", modelable_entity_id,
                "--year_id", " ".join([str(yr) for yr in args.year_id]),
                "--gbd_round_id", args.gbd_round_id,
                "--decomp_step", args.decomp_step,
                "--save_dir", "FILEPATH"
            ],
            name=f"save_{modelable_entity_id}",
            tag="save",
            upstream_tasks=causal_attribution_tasks,
            num_cores=8,
            m_mem_free="100G",
            max_attempts=3,
            max_runtime_seconds=60*60*24,
            queue='all.q')
        workflow.add_task(task)

    status = workflow.run()
    print(f'Workflow finished with status {status}')


if __name__ == '__main__':
    main()
