import functools
from dataclasses import make_dataclass

import yaml


class RunConfig:
    def __init__(self, path: str):
        """
        Args:
            path (str): Path to a controller YAML
        """
        self.yaml_path = str(path)

    def _parse_yaml(self):
        """Reads in all fields of YAML."""
        with open(self.yaml_path, "r") as file:
            self.info = yaml.safe_load(file)
            file.close()

    def _access_run_params(self):
        """Creates a dataclass of params accesible via an attribute named
        after the functions they apply to.
        This requires the YAML to have a section named 'run_params'
        """

        if "run_params" in self.info.keys():
            for func_name, params in self.info["run_params"].items():
                if isinstance(params, dict):
                    fields = ((param, type(val)) for param, val in params.items())
                    dc = make_dataclass(func_name, fields=fields)
                    setattr(self, func_name, dc(**params))
                else:
                    setattr(self, func_name, None)
            self.info.pop("run_params")

    def _make_config(self):
        """Reads YAML and parses any methods into dataclasses.
        Saves the dataclasses as self attributes.
        """
        self._parse_yaml()
        self._access_run_params()


@functools.lru_cache()
def parser(path: str) -> RunConfig:
    """Reads a designated .yaml config. Caches so that reading only happens once.
    run_params are available as attributes and all other fields are accessible
    through the 'info' attribute.

    Args:
        path (str): Path to a controller YAML

    Returns:
        RunConfig: Callable parameters and metadata accessible as attributes.
    """
    config = RunConfig(path=path)
    config._make_config()

    return config
