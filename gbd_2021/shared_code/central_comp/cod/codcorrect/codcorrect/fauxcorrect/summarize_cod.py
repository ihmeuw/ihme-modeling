from argparse import ArgumentParser, Namespace
import logging

from fauxcorrect.job_swarm import task_templates
from fauxcorrect.parameters import machinery
from fauxcorrect.summarize import summarize_cod
from fauxcorrect.utils.constants import GBD
from fauxcorrect.utils.logging import setup_logging


def parse_arguments() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        '--version_id',
        type=int,
        required=True
    )
    parser.add_argument(
        '--machine_process',
        type=str,
        required=True,
        help='fauxcorrect or codcorrect'
    )
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
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    setup_logging(args.parent_dir, task_templates.SummarizeCod.template_name, args)
    logging.info(
        f"Summarizing location_id: {args.location_id}, "
        f"and year_id: {args.year_id}."
    )
    params = machinery.recreate_from_process_and_version_id(
        version_id=args.version_id, machine_process=args.machine_process
    )
    summarize_cod(
        args.parent_dir,
        args.gbd_round_id,
        args.location_id,
        args.year_id,
        params
    )
    logging.info("Summarization completed.")


if __name__ == '__main__':
    main()
