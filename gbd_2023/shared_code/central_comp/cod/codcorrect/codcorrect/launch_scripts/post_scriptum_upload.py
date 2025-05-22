from argparse import ArgumentParser, Namespace
import logging

from codcorrect.legacy import post_scriptum
from codcorrect.legacy.parameters import machinery
from codcorrect.lib.utils import logs

logger = logging.getLogger(__name__)


def parse_arguments() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        '--version_id',
        type=int,
        required=True,
        help='CodCorrect version id',
    )
    return parser.parse_args()


def main() -> None:
    logs.configure_logging()
    args = parse_arguments()
    parameters = machinery.MachineParameters.load(args.version_id)
    logger.info(f"Making post-scriptum updates for CodCorrect version {args.version_id}")
    compare_version_id = post_scriptum.post_scriptum_upload(
        version_id=parameters.version_id,
        process_version_id=parameters.gbd_process_version_id,
        cod_output_version_id=parameters.cod_output_version_id,
        release_id=parameters.release_id,
        test=parameters.test,
    )
    logger.info(f"Compare version {compare_version_id} has been created.")


if __name__ == '__main__':
    main()
