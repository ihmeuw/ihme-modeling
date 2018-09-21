import logging
import logging.config
import os

import yaml


def create_logger_from_file(logger_name, path="logging.yaml", dUSERt_level=logging.DEBUG,
                            env_key="LOG_CFG"):
    """Utility function to set up loggers, but only attach handlers if they are not already present.
    Looks for an environment value for the path, and then for a logging config file"""
    final_path = path
    value = os.getenv(env_key, None)

    if value:
        final_path = value
    if os.path.exists(final_path):
        with open(final_path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
        logging.getLogger(logger_name).info("Creating log config from file {} for name".format(final_path, logger_name))
    else:
        # fall back if it can't find the file
        logging.basicConfig(level=dUSERt_level)
        logging.getLogger(logger_name).info("Creating basicConfig logger, could not find config file {}".format(final_path))

    logger = logging.getLogger(logger_name)
    _add_adding_machine_logger(logger)
    return logger


def create_logger_in_memory(name, level, log_file_name):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    remove_all_log_handlers(name)
    # AND the root logger too - a call to basicConfig sets handlers on the un-named root logger.
    remove_all_log_handlers("")

    handler = logging.FileHandler(log_file_name)
    handler.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)

    # Also write to stdout in case logging is broken.
    msg = "'{}' logger created in memory at {}, the run begins, release the Unicorns...".format(name, log_file_name)
    logger.info(msg)
    print(msg)

    # Updating jobmon/zmq has caused this process to be immune to ctrl-C
    msg = "To kill this you need to use ctrl-\\"
    logger.info(msg)
    print(msg)
    _add_adding_machine_logger(logger)
    _add_jobmon_client_logger(logger)
    return logging.getLogger(name)


def _add_adding_machine_logger(master_logger):
    """And make sure that the adding machine logger is also initialized as per dalynator logger"""
    am_logger = logging.getLogger('adding_machine.agg_locations')
    am_logger.setLevel(master_logger.getEffectiveLevel())
    for handler in master_logger.handlers:
        am_logger.addHandler(handler)


def _add_jobmon_client_logger(master_logger):
    """Set the jobmon logger to log to the same spot as dalynator"""
    master_logger.info("Setting jobmon log handlers to same as {}".format(master_logger.name))
    remove_all_log_handlers("jobmon")
    jobmon_logger = logging.getLogger('jobmon')
    jobmon_logger.setLevel(master_logger.getEffectiveLevel())
    for handler in master_logger.handlers:
        jobmon_logger.addHandler(handler)


def remove_all_log_handlers(name):
    logger = logging.getLogger(name)
    handlers = logger.handlers[:]  # Make a copy
    for h in handlers:
        logger.removeHandler(h)
