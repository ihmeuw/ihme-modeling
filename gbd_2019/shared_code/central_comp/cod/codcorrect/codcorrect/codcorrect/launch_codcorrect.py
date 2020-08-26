from argparse import ArgumentParser, Namespace
import logging

import gbd.constants as gbd

from fauxcorrect.job_swarm.codcorrect_job_swarm import CoDCorrectSwarm
from fauxcorrect.parameters.master import CoDCorrectParameters
from fauxcorrect.utils import constants
from fauxcorrect.utils.logging import setup_logging
from fauxcorrect.validations.input_args import validate_pct_change_years


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        '--resume',
        type=bool,
        default=False,
        help="Restart a run or start a new one."
    )
    parser.add_argument(
        '--version_id',
        type=int,
        default=None,
        help="If resuming a CoDCcorrect run, provide a version id."
    )
    parser.add_argument(
        '--decomp_step',
        type=str,
        required=True,
        help="The decomposition step of this CoDCcorrect run."
    )
    parser.add_argument(
        '--gbd_round_id',
        type=int,
        default=gbd.GBD_ROUND_ID,
        help="The gbd round id for this CoDCcorrect run."
    )
    parser.add_argument(
        '--location_set_ids',
        type=int,
        nargs='+',
        default=[constants.LocationSetId.OUTPUTS],
        help=("List of location_set_versions for this run of CoDCcorrect. "
              "Default, [89].")
    )
    parser.add_argument(
        '--year_ids',
        type=int,
        nargs='+',
        default=constants.Years.ALL,
        help=("List of year_ids for this run of CoDCcorrect. Default, "
              "[1990, 2000, 2017].")
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
        help=("List of measure_ids for this run of CoDCcorrect. Default, "
              "[1, 4].")
    )
    parser.add_argument(
        '--databases',
        type=str,
        nargs='+',
        default=[constants.DataBases.GBD, constants.DataBases.COD],
        help=("List of databases to upload results into. Default, 'gbd'. "
              "Valid options, 'cod' and 'gbd'.")
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.resume and not args.version_id:
        raise RuntimeError(
            "Cannot resume a run without a CoDCcorrect version id."
        )

    validate_pct_change_years(args.year_start_ids, args.year_end_ids)

    if not args.resume:
        codcorrect = CoDCorrectParameters.new(
            year_ids=args.year_ids,
            year_start_ids=args.year_start_ids,
            year_end_ids=args.year_end_ids,
            location_set_ids=args.location_set_ids,
            measure_ids=args.measure_ids,
            gbd_round_id=args.gbd_round_id,
            decomp_step=args.decomp_step,
            process=constants.GBD.Process.Name.CODCORRECT,
            databases=args.databases
        )

        setup_logging(
            codcorrect.parent_dir,
            constants.DAG.Tasks.Type.LAUNCH,
            args
        )

        logging.info(
            f"Beginning CoDCcorrect version {codcorrect.version_id} run.\n"
            f"Arguments: decomp_step: {args.decomp_step}, location_set_ids: "
            f"{args.location_set_ids}, year_ids: {args.year_ids}, measure_ids: "
            f"{args.measure_ids}. Uploading to the following databases: "
            f"{args.databases}."
        )
        logging.info("Generating filesystem assets.")
        codcorrect.generate_project_directories()
        logging.info("Caching CoDCorrect parameters.")
        codcorrect.cache_parameters()
        logging.info("Validating models")
        codcorrect.validate_model_versions(constants.CauseSetId.COMPUTATION)
        codcorrect.create_input_model_tracking_report()
    else:
        codcorrect = CoDCorrectParameters.recreate_from_version_id(
            args.version_id
        )
        setup_logging(
            codcorrect.parent_dir,
            constants.DAG.Tasks.Type.LAUNCH,
            args
        )
        logging.info(
            f"Resuming CoDCorrect version {codcorrect.version_id} run."
        )

    swarm = CoDCorrectSwarm(parameters=codcorrect, resume=args.resume)
    logging.info("Constructing workflow.")
    swarm.construct_workflow()
    logging.info("Running workflow.")
    swarm.run()


if __name__ == '__main__':
    main()
