from argparse import ArgumentParser, Namespace
import datetime
import getpass
import os

from db_queries import get_demographics
from jobmon.client.swarm.workflow.python_task import PythonTask
from jobmon.client.swarm.workflow.workflow import Workflow

path_to_directory = "FILEPATH"


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument('--gbd_round_id', type=int)
    parser.add_argument('--decomp_step', type=str)
    parser.add_argument('--out_dir', type=str)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    user = getpass.getuser()
    today_string = datetime.date.today().strftime('%m%d%y')
    workflow = Workflow(
        workflow_args=f'anemia_malaria_{args.decomp_step}_{today_string}',
        name=f'anemia_malaria_{args.decomp_step}_{today_string}',
        description=f'Anemia: Malaria pre-processing for decomp {args.decomp_step}',
        project="proj_anemia",
        stderr="FILEPATH",
        stdout="FILEPATH",
        working_dir=path_to_directory,
        resume=True)

    # first submit the subtract clinical jobs
    subtract_tasks = []
    demo = get_demographics("epi", gbd_round_id=args.gbd_round_id)
    for loc in demo['location_id']:
        task = PythonTask(
            script="FILEPATH",
            args=[
                "--location_id", loc,
                "--gbd_round_id", args.gbd_round_id,
                "--decomp_step", args.decomp_step,
                "--out_dir", args.out_dir
            ],
            name=f"malaria_subtract_{loc}",
            tag="malaria_subtract",
            num_cores=2,
            m_mem_free="8G",
            max_attempts=3,
            max_runtime_seconds=60*60*3,
            queue='all.q')
        subtract_tasks.append(task)
    workflow.add_tasks(subtract_tasks)

    # once the new draws exist, save results
    for modelable_entity_id in [19390, 19394]:
        task = PythonTask(
            script="FILEPATH",
            args=[
                "--modelable_entity_id", modelable_entity_id,
                "--gbd_round_id", args.gbd_round_id,
                "--decomp_step", args.decomp_step,
                "--out_dir", args.out_dir
            ],
            name=f"malaria_save_{modelable_entity_id}",
            tag="malaria_save",
            upstream_tasks=subtract_tasks,
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
