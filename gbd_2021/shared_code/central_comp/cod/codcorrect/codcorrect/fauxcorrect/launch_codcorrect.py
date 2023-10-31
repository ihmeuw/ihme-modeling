from argparse import ArgumentParser, Namespace
import logging

import gbd.constants as gbd
from jobmon.client.workflow import WorkflowRunStatus

from fauxcorrect.job_swarm.codcorrect_workflow import CodCorrectWorkflow
from fauxcorrect.parameters import base_cause_set
from fauxcorrect.parameters.machinery import CoDCorrectParameters
from fauxcorrect.utils import constants, slack
from fauxcorrect.utils.logging import list_args, setup_logging
from fauxcorrect.validations import input_args


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        '--restart',
        action='store_true',
        help=("Restart a run, starting from the beginning. Expects parameters and db "
              "metadata already exists.")
    )
    parser.add_argument(
        '--resume',
        action='store_true',
        help=("Resume a run, starting from the previous point of failure. "
              "Expects parameters and db metadata already exists.")
    )
    parser.add_argument(
        '--version_id',
        type=int,
        default=None,
        help="If restarting or resuming a CoDCorrect run, provide a version id."
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help=("Is this a test run of CodCorrect? Base cause and location sets "
              "will be overwritten.")
    )
    parser.add_argument(
        '--gbd_round_id',
        type=int,
        default=gbd.GBD_ROUND_ID,
        help="The gbd round id for this CoDCorrect run."
    )
    parser.add_argument(
        '--decomp_step',
        type=str,
        help="The decomposition step of this CoDCorrect run."
    )
    parser.add_argument(
        '--n_draws',
        type=int,
        default=constants.Draws.MAX_DRAWS,
        help=(f"The number of draws to run with. Default is "
              f"{constants.Draws.MAX_DRAWS}")
    )
    parser.add_argument(
        '--base_location_set_id',
        type=int,
        default=None,
        help=("The base location set, central to this run of CoDCorrect. "
              "Default is selected based on the GBD round.")
    )
    parser.add_argument(
        '--aggregation_location_set_ids',
        type=int,
        nargs='+',
        default=None,
        help=("List of additional location sets for this run of CoDCorrect. "
              "For aggregation purposes. Defaults to None.")
    )
    parser.add_argument(
        '--base_cause_set_id',
        type=int,
        default=None,
        help=("The base cause set, central to this run of CoDCorrect. "
              "Must be a subset of the computation cause set (2). "
              "Defaults to the computation cause set (2).")
    )
    parser.add_argument(
        '--aggregation_cause_set_ids',
        type=int,
        nargs='+',
        default=None,
        help=("List of additional causes sets for this run of CodCorrect. "
              "These sets are aggregated alongside the base cause set. "
              "Defaults to none (an empty list)")
    )
    parser.add_argument(
        '--year_ids',
        type=int,
        nargs='+',
        default=None,
        help=("List of year_ids for this run of CoDCorrect. Default, "
              "all demographics years for the GBD round.")
    )
    parser.add_argument(
        '--year_start_ids',
        type=int,
        nargs='+',
        help=("Year ids used to calculate pct_change. "
              "List must be the same length as argument "
              "'year_end_ids'")
    )
    parser.add_argument(
        '--year_end_ids',
        type=int,
        nargs='+',
        help=("Year ids used to calculate pct_change. "
              "List must be the same length as argument "
              "'year_start_ids'")
    )
    parser.add_argument(
        '--measure_ids',
        type=int,
        nargs='+',
        default=[gbd.measures.DEATH, gbd.measures.YLL],
        help=("List of measure_ids for this run of CoDCorrect. Default, "
              "[1, 4].")
    )
    parser.add_argument(
        '--databases',
        type=str,
        nargs='+',
        default=constants.DataBases.DATABASES,
        help=("List of databases to upload results into. Default, all three. "
              "Valid options, 'cod', 'gbd', 'codcorrect'.")
    )
    parser.add_argument(
        '--scatter_version_id',
        type=int,
        default=None,
        help=("Optionally pass in a previous CodCorrect version to scatter the results of "
              "this run against; if nothing is passed in, no scatters are made.")
    )
    args = parser.parse_args()
    input_args.set_reactive_defaults(args)
    return args


def main():
    args = parse_args()
    input_args.run_input_validations(
        constants.GBD.Process.Name.CODCORRECT, args
    )

    if not (args.restart or args.resume):
        codcorrect = CoDCorrectParameters(
            gbd_round_id=args.gbd_round_id,
            decomp_step=args.decomp_step,
            process=constants.GBD.Process.Name.CODCORRECT,
            test=args.test,
            n_draws=args.n_draws,
            base_location_set_id=args.base_location_set_id,
            aggregation_location_set_ids=args.aggregation_location_set_ids,
            base_cause_set_id=args.base_cause_set_id,
            aggregation_cause_set_ids=args.aggregation_cause_set_ids,
            year_ids=args.year_ids,
            year_start_ids=args.year_start_ids,
            year_end_ids=args.year_end_ids,
            measure_ids=args.measure_ids,
            databases=args.databases,
            scatter_version_id=args.scatter_version_id
        )

        logger = setup_logging(
            codcorrect.parent_dir,
            "launch",
            args
        )

        logger.info(
            f"Beginning CoDCorrect version {codcorrect.version_id} run.\n"
            f"Arguments:\n{list_args(args)}"
        )
        logger.info("Generating filesystem assets.")
        codcorrect.generate_project_directories()
        logger.info("Caching CoDCorrect parameters.")
        codcorrect.cache_parameters()
        logger.info("Validating models")
        codcorrect.validate_model_versions()
        codcorrect.create_input_model_tracking_report()
        logger.info("Validating the correction hierarchy")
        base_cause_set.validate_correction_hierarchy(
            codcorrect.correction_hierarchy,
            args.base_cause_set_id,
            args.gbd_round_id,
            args.decomp_step
        )
        codcorrect.cache_correction_hierarchy()
    else:
        codcorrect = CoDCorrectParameters.recreate_from_version_id(
            args.version_id
        )
        logger = setup_logging(
            codcorrect.parent_dir,
            "launch",
            args
        )

        if args.restart:
            logger.info(
                f"Restarting CoDCorrect version {codcorrect.version_id} run."
            )
        else:
            logger.info(
                f"Resuming CoDCorrect version {codcorrect.version_id} run."
            )

    slack.send_slack_greeting(codcorrect, args.restart, args.resume)

    # Wrap jobmon calls so we can message out failure in case of jobmon issue
    try:
        swarm = CodCorrectWorkflow(parameters=codcorrect, resume=args.resume)
        logger.info("Constructing workflow.")
        swarm.create_workflow()
        logger.info("Running workflow.")
        status = swarm.run()
    except Exception as e:
        logger.info(f"Uncaught exception attempting to set up and run jobmon workflow: {e}")
        status = WorkflowRunStatus.ERROR

    slack.send_slack_goodbye(codcorrect, status=status)


if __name__ == '__main__':
    main()
