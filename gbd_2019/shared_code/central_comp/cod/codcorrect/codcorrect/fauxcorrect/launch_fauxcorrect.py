from argparse import ArgumentParser, Namespace
import logging

import gbd.constants as gbd

from fauxcorrect.job_swarm.fauxcorrect.job_swarm import FauxCorrectSwarm
from fauxcorrect.parameters.master import FauxCorrectParameters
from fauxcorrect.utils import constants
from fauxcorrect.utils.logging import setup_logging


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
        help="If resuming a fauxcorrect run, provide a version id."
    )
    parser.add_argument(
        '--decomp_step',
        type=str,
        required=True,
        help="The decomposition step of this FauxCorrect run."
    )
    parser.add_argument(
        '--location_set_ids',
        type=int,
        nargs='+',
        default=[constants.LocationSetId.STANDARD],
        help=("List of location_set_versions for this run of FauxCorrect. "
              "Default, [35].")
    )
    parser.add_argument(
        '--year_ids',
        type=int,
        nargs='+',
        default=constants.Years.ALL,
        help=("List of year_ids for this run of FauxCorrect. Default, "
              "[1990, 2000, 201].")
    )
    parser.add_argument(
        '--measure_ids',
        type=int,
        nargs='+',
        default=[gbd.measures.DEATH, gbd.measures.YLL],
        help=("List of measure_ids for this run of FauxCorrect. Default, "
              "[1, 4].")
    )
    parser.add_argument(
        '--databases',
        type=str,
        nargs='+',
        default=[constants.DataBases.GBD],
        help=("List of databases to upload results into. Default, 'gbd'. "
              "Valid options, 'cod' and 'gbd'.")
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.resume and not args.version_id:
        raise RuntimeError(
            "Cannot resume a run without a fauxcorrect version id."
        )

    if not args.resume:
        fauxcorrect = FauxCorrectParameters.new(
            year_ids=args.year_ids,
            location_set_ids=args.location_set_ids,
            measure_ids=args.measure_ids,
            gbd_round_id=gbd.GBD_ROUND_ID,
            decomp_step=args.decomp_step,
            process=constants.GBD.Process.Name.FAUXCORRECT,
            databases=args.databases
        )

        setup_logging(
            fauxcorrect.parent_dir,
            constants.DAG.Tasks.Type.LAUNCH,
            args
        )

        logging.info(
            f"Beginning Fauxcorrect version {fauxcorrect.version_id} run.\n"
            f"Arguments: decomp_step: {args.decomp_step}, location_set_ids: "
            f"{args.location_set_ids}, year_ids: {args.year_ids}, measure_ids: "
            f"{args.measure_ids}. Uploading to the following databases: "
            f"{args.databases}."
        )
        logging.info("Generating filesystem assets.")
        fauxcorrect.generate_project_directories()
        logging.info("Caching fauxcorrect parameters.")
        fauxcorrect.cache_parameters()
        logging.info("Validating models")
        fauxcorrect.validate_model_versions(constants.CauseSetId.FAUXCORRECT)
    else:
        fauxcorrect = FauxCorrectParameters.recreate_from_version_id(
            args.version_id
        )
        setup_logging(
            fauxcorrect.parent_dir,
            constants.DAG.Tasks.Type.LAUNCH,
            args
        )
        logging.info(
            f"Resuming FauxCorrect version {fauxcorrect.version_id} run."
        )

    swarm = FauxCorrectSwarm(parameters=fauxcorrect, resume=args.resume)
    logging.info("Constructing workflow.")
    swarm.construct_workflow()
    logging.info("Running workflow.")
    swarm.run()


if __name__ == '__main__':
    main()
