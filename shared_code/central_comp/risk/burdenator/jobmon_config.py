# A single place to store the config for connecting to central job monitor
# server

import json
import os

from jobmon.executors.sge_exec import SGEJobInstance
from jobmon.connection_config import ConnectionConfig

MONITOR_HOST = 'ADDRESS'
MONITOR_PORT = 'FOO'
DEFAULT_RETRIES = 3
DEFAULT_TIMEOUT = 30000

PUBLISHER_HOST = 'ADDRESS'
PUBLISHER_PORT = 'FOO'


def get_SGEJobInstance(info_file_dir, connection_config):
    batch_info_file = os.path.join(info_file_dir, "batch_info.json")
    has_batch = os.path.isfile(batch_info_file)
    if has_batch:
        with open(batch_info_file, "r") as f:
            batch_info = json.load(f)
        batch_id = int(batch_info['batch_id'])
    else:
        batch_id = None

    sj = SGEJobInstance(monitor_connection=connection_config, batch_id=batch_id)
    return sj


def create_connection_config_from_args(args):
    cc = ConnectionConfig(monitor_host=args.monitor_host,
                          monitor_port=args.monitor_port,
                          request_timeout=args.request_timeout,
                          request_retries=args.request_retries)
    return cc
