import logging
from sys import stderr

import dalynator.run_pipeline_burdenator_cleanup as pipeline
import dalynator.get_input_args as get_input_args
from dalynator.jobmon_config import get_SGEJobInstance
from dalynator.jobmon_config import create_connection_config_from_args
from dalynator.get_input_args import construct_parser_outdir
from dalynator.get_input_args import construct_parser_jobmon

logger = None

measure_id = 'unparsed'
location_id = 'unparsed'
year_id = 'unparsed'

try:
    odparser = construct_parser_outdir()
    odargs, _ = odparser.parse_known_args()

    jobmon_parser = construct_parser_jobmon()
    jobmon_args, _ = jobmon_parser.parse_known_args()

    sj = get_SGEJobInstance(odargs.out_dir, create_connection_config_from_args(jobmon_args))
    sj.log_started()

    parser = get_input_args.construct_parser_burdenator_cleanup()
    args = get_input_args.construct_args_burdenator_cleanup(parser)

    measure_id = args.measure_id
    location_id = args.location_id
    year_id = args.year_id

    logger = logging.getLogger(__name__)
    logger.info(
        "Arguments passed to run_pipeline_burdenator_cleanup {}".format(args))

    pipeline.run_burdenator_cleanup(args.out_dir, args.location_id,
                                    args.year_id, args.n_draws, args.measure_id,
                                    args.cod_dir, args.epi_dir,
                                    args.turn_off_null_and_nan_check,
                                    args.gbd_round_id, args.cache_dir)
    sj.log_completed()
except Exception as e:
    # Add the exception to the log file, and also report it centrally to the job monitor.
    # Do not send to stderr
    message = "remote run_pipeline_burdenator_cleanup {m}: {l}, {y} failed with an uncaught exception: {ex}" \
        .format(m=measure_id, l=location_id, y=year_id, ex=e)
    if logger:
        logger.error(message)
    else:
        stderr(message)
    sj.log_error(message)
    sj.log_failed()
