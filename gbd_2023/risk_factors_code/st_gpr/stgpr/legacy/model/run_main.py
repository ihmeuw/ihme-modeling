import argparse
import dataclasses
import os
import sys

import stgpr_helpers
from stgpr_helpers import parameters
from stgpr_helpers.legacy import old

import stgpr
from stgpr.legacy.model import jobmon_workflow


@dataclasses.dataclass
class MainArgs:
    run_id: int
    error_log_path: str
    output_log_path: str
    project: str
    nparallel: int
    is_standard_log_path: bool


def parse_args() -> MainArgs:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_id", type=int)
    parser.add_argument("error_log_path", type=str)
    parser.add_argument("output_log_path", type=str)
    parser.add_argument("project", type=str)
    parser.add_argument("nparallel", type=int)
    parser.add_argument("is_standard_log_path", type=bool)
    args = vars(parser.parse_args())
    return MainArgs(**args)


def run_job_swarm(
    run_id: int,
    error_log_path: str,
    output_log_path: str,
    project: str,
    nparallel: int,
    is_standard_log_path: bool,
) -> None:
    # To launch R jobs if model was launched from RStudio,
    # we need to add slurm singularity to PATH env var
    if "/opt/singularity/bin" not in os.getenv("PATH"):
        os.environ["FILEPATH"] = "/opt/singularity/bin:" + os.getenv("FILEPATH")

    file_utility = stgpr_helpers.StgprFileUtility(run_id)
    if is_standard_log_path:
        file_utility.make_logs_directory()

    params = file_utility.read_parameters()
    n_param_sets = old.determine_n_parameter_sets(params)
    swarm = jobmon_workflow.STGPRJobSwarm(
        run_id=run_id,
        run_type=params[parameters.MODEL_TYPE],
        holdouts=params[parameters.HOLDOUTS],
        draws=params[parameters.GPR_DRAWS],
        prediction_location_set_version=params[parameters.PREDICTION_LOCATION_SET_VERSION_ID],
        standard_location_set_version=params[parameters.STANDARD_LOCATION_SET_VERSION_ID],
        nparallel=nparallel,
        n_parameter_sets=n_param_sets,
        cluster_project=project,
        error_log_path=error_log_path,
        output_log_path=output_log_path,
        location_set_id=params[parameters.LOCATION_SET_ID],
        custom_stage1=params[parameters.CUSTOM_STAGE_1],
        rake_logit=params[parameters.RAKE_LOGIT],
        code_version=stgpr.__version__,
        modelable_entity_id=params[parameters.MODELABLE_ENTITY_ID],
        random_seed=params[parameters.RANDOM_SEED]
    )
    status = swarm.run()
    sys.exit(status)


def main() -> None:
    args = parse_args()
    run_job_swarm(
        args.run_id,
        args.error_log_path,
        args.output_log_path,
        args.project,
        args.nparallel,
        args.is_standard_log_path,
    )


if __name__ == "__main__":
    main()
