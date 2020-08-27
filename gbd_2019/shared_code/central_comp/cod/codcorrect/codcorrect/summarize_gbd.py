from argparse import ArgumentParser, Namespace
import logging

from fauxcorrect.summarize import summarize
from fauxcorrect.utils.constants import DAG, GBD
from fauxcorrect.utils.logging import setup_logging


def parse_arguments() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        '--parent_dir',
        type=str,
        required=True,
        help='path to base fauxcorrect directory'
    )
    parser.add_argument(
        '--gbd_round_id',
        type=int,
        required=True,
        help='gbd_round_id to summarize.'
    )
    parser.add_argument(
        '--location_id',
        type=int,
        required=True,
        help='location_id to summarize'
    )
    parser.add_argument(
        '--year_id',
        type=int,
        required=True,
        help='year_id to summarize'
    )
    parser.add_argument(
        '--measure_id',
        type=int,
        required=True,
        help='measure_id to summarize'
    )
    parser.add_argument(
        '--machine_process',
        type=str,
        required=True,
        help='fauxcorrect or codcorrect')
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    setup_logging(args.parent_dir, DAG.Tasks.Name.SUMMARIZE_GBD, args)
    logging.info(
        f"Summarizing machine_process: {args.machine_process}, location_id: "
        f"{args.location_id}, measure_id: {args.measure_id}, and year_id: "
        f"{args.year_id}."
    )
    summarize(
        args.machine_process,
        args.parent_dir,
        args.gbd_round_id,
        args.location_id,
        args.measure_id,
        args.year_id
    )
    logging.info("Summarization completed.")


if __name__ == '__main__':
    main()
