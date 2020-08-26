import os
import json

from cascade_ode.argument_parser import cascade_parser

CUSTOM_CONDA_PATH_FILE = os.path.expanduser("PATH")
CUSTOM_CONDA_ENV_FILE = os.path.expanduser("PATH")


def load(check_for_custom_conda=False):
    """Get configuration"""
    this_file = os.path.realpath(__file__)
    config_path = os.path.abspath(
        os.path.join(os.path.dirname(this_file), ".."))

    # if pip install -e ., config is one level up from this file
    # if pip install ., config is same level as this file
    try:
        settings = read_config_file(config_path)
    except IOError:
        settings = read_config_file(
            os.path.abspath(os.path.dirname(this_file)))

    if check_for_custom_conda:
        add_custom_conda_env_variables()

    # Conda environment settings
    if 'DISMOD_CONDA_PATH' in os.environ and 'DISMOD_CONDA_ENV' in os.environ:
        conda_root = os.environ['DISMOD_CONDA_PATH']
        conda_env = os.environ['DISMOD_CONDA_ENV']
        assert 'ENVIRONMENT_NAME' in os.environ, \
               "Conda paths set; expected ENVIRONMENT_NAME set"
        environment_name = os.environ['ENVIRONMENT_NAME']
    elif os.environ.get('ENVIRONMENT_NAME') == 'prod':
        conda_root = 'PATH'
        conda_env = 'cascade_ode'
        environment_name = 'prod'
    elif os.environ.get('ENVIRONMENT_NAME') == 'dev':
        conda_root = 'PATH'
        conda_env = 'cascade_ode_dev'
        environment_name = 'dev'
    else:
        raise RuntimeError((
            "Expected either environment variables ENVIRONMENT_NAME or "
            "(DISMOD_CONDA_PATH + DISMOD_CONDA_ENV)"))

    settings['code_dir'] = os.path.dirname(this_file)
    settings['conda_root'] = conda_root
    settings['conda_env'] = os.path.join(conda_root, 'envs', conda_env)
    settings['env_variables'] = {
        'ENVIRONMENT_NAME': environment_name,
        'SGE_CLUSTER_NAME': os.environ['SGE_CLUSTER_NAME'],
        'SGE_ENV': os.environ['SGE_ENV'],
        'DISMOD_CONDA_PATH': conda_root,
        'DISMOD_CONDA_ENV': conda_env}

    return settings


def read_config_file(config_path):
    args, unknown_args = cascade_parser().parse_known_args()
    if args.config_file is not None:
        config_file = args.config_file
    elif os.path.isfile(os.path.join(config_path, "config.local")):
        config_file = os.path.join(config_path, "config.local")
    else:
        config_file = os.path.join(config_path, "config.default")
    settings = json.load(open(config_file))
    return settings


def add_custom_conda_env_variables():
    '''Inspect homes directory of user to see if custom conda environment
    files are set. If so, add them to environment variables
    '''
    if is_custom_conda_env_set():
        conda_root = get_custom_value(CUSTOM_CONDA_PATH_FILE)
        conda_env = get_custom_value(CUSTOM_CONDA_ENV_FILE)
        os.environ['conda_root'] = conda_root
        os.environ['conda_env'] = conda_env


def is_custom_conda_env_set():
    return (os.path.isfile(CUSTOM_CONDA_PATH_FILE) and
            os.path.isfile(CUSTOM_CONDA_ENV_FILE))


def get_custom_value(fpath):
    with open(fpath) as f:
        return f.read()
