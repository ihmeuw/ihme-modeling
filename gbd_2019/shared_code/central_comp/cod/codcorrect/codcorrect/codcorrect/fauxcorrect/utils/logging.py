from argparse import Namespace
import logging
import os
from typing import List


logging.basicConfig(level=logging.INFO)


def setup_logging(
        parent_dir: str,
        module: str,
        arguments: Namespace,
) -> None:
    """
    Generates the log file for each application script of FauxCorrect.

    We use the information from the module name and the command line arguments
    passed to generate a unique log file for each script that is run in
    FauxCorrect.

    For example, the apply_scalars job for location_id 101, sex_id 1, year_id
    2000 would produce the log file name:
        apply_scalars_location_id_101_sex_id_1_year_id_2000.log

    Arguments:
        parent_dir (str): our parent directory for the FauxCorrect run. Used to
            infer the log file path.

        module (str): the name of the module we are logging/running

        arguments (Namespace): the namespace object returned from parsing
            command line arguments. Used to generate a unique file name for
            each script run.
    """
    string_args = _process_args(arguments)
    module_name = _process_module(module)
    logfile = os.path.join(
        parent_dir,
        'logs',
        ('_'.join([module_name] + string_args) + '.log')
    )
    os.makedirs(os.path.dirname(logfile), exist_ok=True)
    logger = logging.getLogger(logfile)
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S'
    )

    file_handler = logging.FileHandler(logfile, mode='w')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.ERROR)
    logger.addHandler(stream_handler)


def _process_args(args: Namespace) -> List[str]:
    """
    Processes the namespace object to make a better log file string name.

    For a few scripts the parent_dir is passed in as an argument, we don't want
    to include the full parent_dir in our log file path, so we remove it below.

    Arguments:
        args (argparse.Namespace): the input arguments from the command line.

    Returns:
        A list of strings containing the Namespace object's key-value pairs of
        command line arguments.
    """
    namespace_dict = vars(args)
    string_args = []
    for key, val in namespace_dict.items():
        if key != 'parent_dir':
            string_args.append(f'{key}')
            if isinstance(val, list):
                if key == 'year_ids':
                    num_years = len(namespace_dict[key])
                    if num_years > 5:
                        string_args.append('all')
                    else:
                        string_args.append(','.join([str(v) for v in val]))
                else:
                    string_args.append(','.join([str(v) for v in val]))
            else:
                string_args.append(str(val))
    return string_args


def _process_module(module: str) -> str:
    """Return the string of the module's name with the suffix '.py' removed."""
    return str(module).replace('.py', '')
