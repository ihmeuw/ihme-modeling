import logging
from sys import stderr

import dalynator.run_pipeline_burdenator_loc_agg as pipeline
import dalynator.get_input_args as get_input_args
from dalynator.jobmon_config import get_SGEJobInstance
from dalynator.jobmon_config import create_connection_config_from_args
from dalynator.get_input_args import construct_parser_outdir
from dalynator.get_input_args import construct_parser_jobmon

logger = None

measure_id = 'unparsed'
location_set_id = 'unparsed'
year_id = 'unparsed'

try:
    odparser = construct_parser_outdir()
    odargs, _ = odparser.parse_known_args()

    jobmon_parser = construct_parser_jobmon()
    jobmon_args, _ = jobmon_parser.parse_known_args()

    sj = get_SGEJobInstance(odargs.out_dir, create_connection_config_from_args(jobmon_args))
    sj.log_started()

    parser = get_input_args.construct_parser_burdenator_loc_agg()
    args = get_input_args.get_args_burdenator_loc_agg(parser)

    measure_id = args.measure_id
    location_set_id = args.location_set_id
    year_id = args.year_id

    logger = logging.getLogger(__name__)
    logger.info(
        "Arguments passed to run_pipeline_burdenator_loc_agg {}".format(args))

    pipeline.run_loc_agg(args.out_dir, args.location_set_id, args.year_id,
                         args.sex_id, args.rei_id, args.measure_id,
                         args.gbd_round_id)
    sj.log_completed()
except Exception as e:
    # Add the exception to the log file, and also report it centrally to the
    # job monitor. Do not send to stderr
    message = "remote run_loc_agg {m}: {l}, {y} failed with an uncaught exception: {ex}" \
        .format(m=measure_id, l=location_set_id, y=year_id, ex=e)
    if logger:
        logger.error(message)
    else:
        stderr(message)
    sj.log_error(message)
    sj.log_failed()
