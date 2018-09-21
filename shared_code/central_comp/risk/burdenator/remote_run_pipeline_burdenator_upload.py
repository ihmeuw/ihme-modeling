import logging
from sys import stderr

import dalynator.run_pipeline_burdenator_upload as pipeline
import dalynator.get_input_args as get_input_args
from dalynator.jobmon_config import get_SGEJobInstance
from dalynator.jobmon_config import create_connection_config_from_args
from dalynator.get_input_args import construct_parser_outdir
from dalynator.get_input_args import construct_parser_jobmon

logger = None
location_ids = 'unparsed'
measure_id = 'unparsed'

try:
    odparser = construct_parser_outdir()
    odargs, _ = odparser.parse_known_args()

    jobmon_parser = construct_parser_jobmon()
    jobmon_args, _ = jobmon_parser.parse_known_args()

    sj = get_SGEJobInstance(odargs.out_dir, create_connection_config_from_args(jobmon_args))
    sj.log_started()

    parser = get_input_args.construct_parser_burdenator_upload()
    args = get_input_args.construct_args_burdenator_upload(parser)

    location_ids = args.location_ids
    measure_id = args.measure_id

    logger = logging.getLogger(__name__)
    logger.info(
        "Arguments passed to run_pipeline_burdenator_upload {}".format(args))

    pipeline.run_burdenator_upload(args.out_dir, args.gbd_process_version_id,
                                   args.location_ids, args.measure_id,
                                   args.table_type, args.upload_to_test)
    sj.log_completed()
except Exception as e:
    # Add the exception to the log file, and also report it centrally to the job monitor.
    # Do not send to stderr
    message = "run_pipeline_burdenator_upload {m} failed with an uncaught exception: {ex};  locations: {ls}" \
        .format(m=measure_id, ls=location_ids, ex=e)
    if logger:
        logger.error(message)
    else:
        stderr(message)
    sj.log_error(message)
    sj.log_failed()
