import json
import os
import sys

from cascade_ode.legacy.argument_parser import cascade_parser
from cascade_ode.legacy.constants import EnvironmentVariables as EnvVar

CUSTOM_CONDA_PATH_FILE = 
CUSTOM_CONDA_ENV_FILE = 


def load(check_for_custom_conda=False):
    """Get configuration"""
    this_file = 
    config_path = 

    settings = read_config_file(config_path)

    if check_for_custom_conda:
        add_custom_conda_env_variables()

    # Conda environment settings
    if EnvVar.DISMOD_CONDA_PATH in os.environ and EnvVar.DISMOD_CONDA_ENV in os.environ:
        conda_root = os.environ[EnvVar.DISMOD_CONDA_PATH]
        conda_env = os.environ[EnvVar.DISMOD_CONDA_ENV]
        assert (
            EnvVar.ENVIRONMENT_NAME in os.environ
        ), f"Conda paths set; expected {EnvVar.ENVIRONMENT_NAME} set"
        environment_name = os.environ[EnvVar.ENVIRONMENT_NAME]
    elif os.environ.get(EnvVar.ENVIRONMENT_NAME) == "prod":
        conda_root = 
        conda_env = "cascade_ode"
        environment_name = "prod"
    elif os.environ.get(EnvVar.ENVIRONMENT_NAME) == "dev" or is_pytest_run():
        conda_root = 
        conda_env = "cascade_ode_dev"
        environment_name = "dev"
    else:
        raise RuntimeError(
            f"Expected either environment variables {EnvVar.ENVIRONMENT_NAME} or "
            f"({EnvVar.ENVIRONMENT_NAME} + {EnvVar.DISMOD_CONDA_PATH} + {EnvVar.DISMOD_CONDA_ENV})"
        )

    settings["code_dir"] = os.path.realpath(os.path.dirname(this_file))
    settings["conda_root"] = conda_root
    settings["conda_env"] = 
    env_vars = {
        EnvVar.ENVIRONMENT_NAME: environment_name,
        EnvVar.DISMOD_CONDA_PATH: conda_root,
        EnvVar.DISMOD_CONDA_ENV: conda_env,
    }

    try:
        env_vars[EnvVar.SGE_CLUSTER_NAME] = os.environ["SGE_CLUSTER_NAME"]
        env_vars[EnvVar.SGE_ENV] = os.environ["SGE_ENV"]
    except KeyError:
        pass

    settings["env_variables"] = env_vars

    return settings


def read_config_file(config_path):
    # DB passwords and filepath settings
    # This picks up a config filename from command-line arguments in ARGV.
    # Prefer this to config.local because it cannot fail silently.
    args, unknown_args = cascade_parser().parse_known_args()
    if args.config_file is not None:
        config_file = args.config_file
    elif :
        config_file = 
    else:
        config_file = 
    with open(config_file) as f:
        settings = json.load(f)
    return settings


def add_custom_conda_env_variables():
    """Inspect homes directory of user to see if custom conda environment
    files are set. If so, add them to environment variables
    """
    if is_custom_conda_env_set():
        conda_root = read_one_line(CUSTOM_CONDA_PATH_FILE)
        conda_env = read_one_line(CUSTOM_CONDA_ENV_FILE)
        os.environ[EnvVar.DISMOD_CONDA_PATH] = conda_root
        os.environ[EnvVar.DISMOD_CONDA_ENV] = conda_env


def is_custom_conda_env_set():
    return os.path.isfile(CUSTOM_CONDA_PATH_FILE) and os.path.isfile(CUSTOM_CONDA_ENV_FILE)


def read_one_line(fpath):
    """Reads one line from a given file."""
    with open(fpath) as f:
        return f.read().splitlines()[0]


def is_pytest_run() -> bool:
    """Is this a pytest run? 

    This is a function instead of being inline'd because it makes mocking easier.
    """
    return "pytest" in sys.modules
