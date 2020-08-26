from argparse import ArgumentParser, Namespace
import logging

from fauxcorrect.parameters import master
from fauxcorrect.shocks import append_shocks
from fauxcorrect.utils import constants
from fauxcorrect.utils.logging import setup_logging


def parse_arguments() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        '--parent_dir',
        type=str,
        required=True,
        help='path to base directory for the run'
    )
    parser.add_argument(
        '--machine_process',
        type=str,
        required=True,
        choices=constants.GBD.Process.Name.OPTIONS,
        help='codcorrect or fauxcorrect'
    )
    parser.add_argument(
        '--measure_ids',
        type=int,
        nargs="+",
        required=True,
        choices=[constants.Measures.Ids.DEATHS,constants.Measures.Ids.YLLS],
        help='1 and/or 4, e.g. [1], [4], or [1,4]'
    )
    parser.add_argument(
        '--location_id',
        type=int,
        required=True,
        help='location_id to append shocks'
    )
    parser.add_argument(
        '--most_detailed_location',
        type=bool,
        required=True,
        help='whether or not the location_id is a most-detailed location'
    )
    parser.add_argument(
        '--sex_id',
        type=int,
        required=True,
        help='sex_id to append shocks'
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    setup_logging(
        args.parent_dir,
        constants.DAG.Tasks.Type.APPEND,
        args
    )
    logging.info(
        f"Appending shocks for location_id: {args.location_id}, "
        f"sex_id: {args.sex_id}, machine_process: {args.machine_process}, "
        f"measure_ids: {args.measure_ids}."
    )
    append_shocks(
        args.parent_dir,
        args.machine_process,
        args.measure_ids,
        args.location_id,
        args.most_detailed_location,
        args.sex_id
    )
    logging.info("Appending shocks completed.")


if __name__ == '__main__':
    main()