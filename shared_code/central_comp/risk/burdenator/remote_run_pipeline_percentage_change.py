import logging
from sys import stderr

from dalynator.run_pipeline_percentage_change import run_pipeline_percentage_change
import dalynator.get_input_args as get_input_args
from dalynator.jobmon_config import get_SGEJobInstance
from dalynator.jobmon_config import create_connection_config_from_args

from dalynator.get_input_args import construct_parser_outdir
from dalynator.get_input_args import construct_parser_jobmon


def main():
    try:
        odparser = construct_parser_outdir()
        odargs, _ = odparser.parse_known_args()

        jobmon_parser = construct_parser_jobmon()
        jobmon_args, _ = jobmon_parser.parse_known_args()

        sj = get_SGEJobInstance(odargs.out_dir, create_connection_config_from_args(jobmon_args))
        sj.log_started()

        logger = None
        location_id = 'unparsed'
        start_year = 'unparsed'
        end_year = 'unparsed'

        parser = get_input_args.construct_parser_pct_change()
        args = get_input_args.get_args_pct_change(parser)

        location_id = args.location_id
        start_year = args.start_year
        end_year = args.end_year

        logger = logging.getLogger(__name__)

        logger.info("Arguments passed to remote_run_pipeline_percentage_change: {}".format(args))

        shape = run_pipeline_percentage_change(args)
        logger.debug(" shape= {}".format(shape))
        sj.log_completed()
    except Exception as e:
        # Add the exception to the log file, and also report it centrally to the job monitor.
        # Do not send to stderr
        message = "remote run_pipeline_percentage_change {l} {start}:{end} failed with an uncaught exception: {ex}" \
            .format(l=location_id, start=start_year, end=end_year, ex=e)
        if logger:
            logger.error(message)
        else:
            stderr(message)
        sj.log_error(message)
        sj.log_failed()


if __name__ == "__main__":
    main()
