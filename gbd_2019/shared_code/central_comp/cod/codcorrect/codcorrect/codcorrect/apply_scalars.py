from argparse import ArgumentParser, Namespace
import logging

from fauxcorrect.parameters import master
from fauxcorrect.scalars import apply_scalars
from fauxcorrect.utils.constants import DAG
from fauxcorrect.utils.logging import setup_logging


def parse_arguments() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        '--version_id',
        type=int,
        required=True
    )
    parser.add_argument(
        '--parent_dir',
        type=str,
        required=True,
        help='path to base fauxcorrect directory'
    )
    parser.add_argument(
        '--location_id',
        type=int,
        required=True,
        help='location_id to apply fauxcorrect scalars'
    )
    parser.add_argument(
        '--sex_id',
        type=int,
        required=True,
        help='sex_id to apply fauxcorrect scalars'
    )
    parser.add_argument(
        '--scalar_version_id',
        type=int,
        required=True,
        help='source CoDCorrect version for scalars'
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    fauxcorrect = master.FauxCorrectParameters.recreate_from_version_id(
        args.version_id
    )
    setup_logging(args.parent_dir, DAG.Tasks.Type.SCALE, args)
    logging.info(
        f"Applying CoDCorrect scalars to location_id: {args.location_id} and "
        f"sex_id: {args.sex_id} using CoDCorrect version: "
        f"{args.scalar_version_id}."
    )
    apply_scalars(
        args.parent_dir, args.location_id, args.sex_id, fauxcorrect.year_ids,
        args.scalar_version_id
    )
    logging.info("Applying scalars completed.")


if __name__ == '__main__':
    main()
