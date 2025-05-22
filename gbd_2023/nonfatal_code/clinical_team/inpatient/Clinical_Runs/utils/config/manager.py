import datetime
import importlib
import inspect
import math
import re
from collections.abc import Iterable
from dataclasses import is_dataclass, replace
from getpass import getuser
from pathlib import Path, PosixPath
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
import yaml

from inpatient.Clinical_Runs.utils.config.parser import RunConfig


class BuildYaml:
    """Builds a YAML configuration file from scratch.
    Workflow:
        - Instantiate BuildYaml with an output directory and name for the new config file.
        - Run BuildYaml.template() passing the callables that need parameters and any
            optional args.
        - Review the output template file driectly or the return DataFrame for distinct
            parameters and any type hints/default values.
        - Using BuildYaml.param() assign any parameter values needing updated.
        - Call BuildYaml.make() to write the configuration file with the updated parameters.

    """

    def __init__(self, out_dir: Union[str, Path, PosixPath], workflowname: str):
        """
        Args:
            out_dir (str): Directory to write the config file to.
            workflowname (str): Name for the config file.
                *Should not include any file extension.*

        Raises:
            ValueError: A file extension was part of workflowname.

        """
        if len(workflowname.split(".")) > 1:
            s = workflowname.split(".")[-1]
            raise ValueError(f"workflowname should not have a suffix. Found '{s}'.")

        self.base_yaml_path = FILEPATH
        self.out_path = str(out_dir) + "/" + str(workflowname) + ".yaml"

    def _inspect_steps(self):
        """Gets the parameters and default values from the imports
        defined in the base yaml as 'steps' keys.
        sets self.param_type_dict and self.steps attributes.
        param_type_dict example =
        {'module1': {'param1': {'dtype': type, 'default': val},
                    'param2': {'dtype': type, 'default': val},
                    ...
                    }
         'module2': ...
         ...
        }
        """
        sigs: Dict[str, inspect.signature] = {}
        steps: Dict[str, str] = {}

        # Get input parameters for defined steps.
        for step in self.settings.info["steps"]:

            # Keyed as absolute imports
            import_target = Path(step)

            top_level = import_target.stem
            obj = import_target.suffix.split(".")[-1]

            # Imports and inspects function's parent module
            # Fails with ModuleNotFoundError if parent of import statement is a class.
            try:
                tmp = importlib.import_module(str(top_level))
                sigs[obj] = inspect.signature(getattr(tmp, obj))
                steps[step] = inspect.getdoc(getattr(tmp, obj))
                # No function docstring returns null.
                if steps[step]:
                    steps[step] = self._format_docstring(docstring=steps[step])

            # Handles if function is a class method.
            except ModuleNotFoundError:
                class_name = Path(top_level).suffix
                # Fails here if exception is not what is expected
                tmp = importlib.import_module(Path(top_level).stem)
                tmp_class = getattr(tmp, class_name.split(".")[-1])

                if not inspect.isclass(tmp_class):
                    raise TypeError(f"{class_name} was expected to be a class.")

                sigs[obj] = inspect.signature(getattr(tmp_class, obj))
                steps[step] = inspect.getdoc(getattr(tmp_class, obj))
                # No function docstring returns null.
                if steps[step]:
                    steps[step] = self._format_docstring(docstring=steps[step])

        # Save signature info as nested dict keyed by module
        param_type_dict: Dict[str, Dict[str, dict]] = {}
        for name, signature in sigs.items():
            params = dict(signature.parameters)
            param_type_dict[name] = {}
            for key, val in params.items():
                param_type_dict[name][key] = {"dtype": val.annotation, "default": val.default}
                for k, v in param_type_dict[name][key].items():
                    if v == inspect._empty:
                        param_type_dict[name][key][k] = np.nan

        # Remove self attribute from func params for called class methods.
        for mod in param_type_dict.keys():
            if "self" in param_type_dict[mod].keys():
                param_type_dict[mod].pop("self")

        self.param_type_dict = param_type_dict
        self.steps = steps

    def _format_docstring(self, docstring: str, unitags: List[str] = ["\n", "\t"]) -> str:
        """Docstrings in signatures are formatted with unicode and
        extra spaces.

        Args:
            docstring (str): Docstring pulled from signature
            unitags (str): unicode tags to remove from the docstring.
                Defaults to line break and tab characters.

        Returns:
            str: Refomoratted docstring.
        """
        # Remove any unicode tags in the docstring.
        for tag in unitags:
            docstring = docstring.replace(tag, " ")
        # Remove extra spaces
        docstring = re.sub(" +", " ", docstring)

        return docstring

    def _fill_metadata(self, title: str) -> None:
        """Fills in basic metadata for the yaml."""
        self.metadata: Dict[str, str] = self.settings.info["metadata"]
        self.metadata["title"] = title
        self.metadata["author"] = getuser()

        fmt = "%Y/%m/%d"
        dt_label = datetime.datetime.strftime(datetime.datetime.today(), fmt)
        self.metadata["created_on"] = dt_label

    def _param_value_helper(self) -> None:
        """Pair param metadata with params.
        Make a dictionary keyed by param listing each function's version of
        param's. Lists each version as a dictionary with 'dtype' and 'defaults'
        as keys. The length of each list will be the count of callables that
        require the parameter. Sets to self.run_params_values[parameter] attribute.
        """
        self.run_params_values: Dict[str, List[dict]] = {}
        for param in self.unique_params:
            self.run_params_values[param] = []

        for mod in self.param_type_dict.keys():
            for param in self.param_type_dict[mod].keys():
                self.run_params_values[param].append(self.param_type_dict[mod][param])

    def _get_unique_params(self) -> None:
        """Get distinct parameters across all functions listed in 'steps'.
        saves the unique set of parameters as a class attribute.
        Sets self.unique_params attribute.
        """

        self.unique_params = set.union(
            *[
                set(self.param_type_dict[run_key].keys())
                for run_key in self.param_type_dict.keys()
            ]
        )

    def _make_run_params(self) -> None:
        """Condense to only distinct parameters, consolidate parameter metadata.
        Sets self.param_dict[module][parameter] attribute."""
        # Make each unique param a dict key as base template.
        run_params = {}
        for param in self.unique_params:
            run_params[param] = None

        # Oraganize by module with default values.
        self.param_dict = {}
        for mod in self.param_type_dict.keys():
            self.param_dict[mod] = {}
            for param in self.param_type_dict[mod].keys():
                self.param_dict[mod][param] = run_params[param]
                # Look at all function param meta and keep ones with default value
                checks = len(self.run_params_values[param])
                # For each param with a default value
                for i in range(checks):
                    # if not None or False
                    if self.run_params_values[param][i]["default"]:

                        # For numeric values determine if null.
                        if isinstance(
                            self.run_params_values[param][i]["default"], float
                        ) or isinstance(self.run_params_values[param][i]["default"], int):
                            if not math.isnan(self.run_params_values[param][i]["default"]):
                                # If not null add the value
                                self.param_dict[mod][param] = self.run_params_values[param][i][
                                    "default"
                                ]
                            else:
                                # Add null if value not already present
                                if not self.param_dict[mod][param]:
                                    self.param_dict[mod][param] = None
                        else:
                            self.param_dict[mod][param] = self.run_params_values[param][i][
                                "default"
                            ]
                    # Handle False
                    elif isinstance(self.run_params_values[param][i]["default"], bool):

                        self.param_dict[mod][param] = False

                    else:
                        self.param_dict[mod][param] = None

    def _make_type_dict(self) -> Dict[str, dict]:
        """Make formatted dictionary of type hints from functions.
        Sets self.type_dict attribute.

        Returns:
            Dict[str, dict]: _description_
        """
        for key, val in self.run_params_values.items():
            for name, hint in val[0].items():
                self.run_params_values[key][0][name] = str(hint)

        self.type_dict: Dict[str, dict] = {}
        for key, val in self.run_params_values.items():
            self.type_dict[key] = val[0]

    def _add_steps_to_base(self) -> None:
        """Add the input list of callables to the template."""
        template = get_yaml(path=self.base_yaml_path)
        template.info["steps"] = self.callables

        with open(self.out_path, "w") as yaml_file:
            yaml.dump(template.info, yaml_file, Dumper=CustomDumper, sort_keys=False)

    def _make_update_dict(self) -> None:
        """Make dictionary of distinct params to help assign values to.
        Param values will be mapped to any callable that needs them.
        Facilitates 'global' style parameters between functions.
        Sets self.update_dict attribute.
        """

        self.update_dict: Dict[str, Any] = {}

        for param in self.unique_params:
            self.update_dict[param] = None

        # param_dict is keyed by module
        for params in self.param_dict.values():
            for param in self.unique_params:

                if param in params.keys():
                    if not self.update_dict[param]:
                        self.update_dict[param] = params[param]

    def _write_data(self) -> None:
        """Write a formatted yaml with desired data."""
        data = {"metadata": self.metadata, "steps": self.steps, "run_params": self.param_dict}
        self._make_type_dict()
        if self.type_hints:
            data["type_hints"] = self.type_dict

        data = replace_nulls(data)

        with open(self.out_path, "w") as yaml_file:
            yaml.dump(data, yaml_file, Dumper=CustomDumper, sort_keys=False)

    def _add_type_hints(self) -> None:
        """Add type hints for each param to helper DataFrame."""
        self.to_fill_df["type_hint"] = ""
        self.to_fill_df = self.to_fill_df[["param", "type_hint", "val"]]

        for param in self.update_dict.keys():
            self.to_fill_df.loc[
                self.to_fill_df["param"] == param, "type_hint"
            ] = self.type_dict[param]["dtype"]

    def template(
        self, callables: List[str], title: str = "Configuration File", type_hints: bool = False
    ) -> pd.DataFrame:
        """Build a yaml template for a given workflow of callables.

        Args:
            callables (List[str]): Functions running in workflow to gather parameters for.
                ex= ["clinical_metadata_utils.api.pipeline_wrappers.ClaimsWrappers", ...]
            title (str): Title to give the workflow.
                Defaults to 'Configuration File'
            type_hints (bool, optional): True includes type hint section of parameters in yaml.
                This information can be found in the return object 'to_fill_df'.
                Defaults to False which does not add type_hint section to the YAML.

        Returns:
            pd.DataFrame: Helper table showing distinct parameters across workflow.
                The table has columns "param", "type_hint", "val" where param is the
                name of the parameters and type_hint is an inferred/expected datatype hint
                parsed from the docstrings. val will be null if the parameter value is requred,
                otherwise it will contain the default value parsed from the function.
                These values can still be overridden/reassigned.
        """
        self.callables = callables
        self.type_hints = type_hints

        # Get template, add steps, and save.
        self._add_steps_to_base()
        # Get base run yaml with steps added
        self.settings = get_yaml(path=self.out_path)
        # Get signatures for each callable in steps.
        self._inspect_steps()
        # add basic user metadata
        self._fill_metadata(title=title)
        # Get set of parameters across steps.
        self._get_unique_params()
        # Organize all function param info to parse
        self._param_value_helper()
        # Make dictionary of run params with any default values.
        self._make_run_params()
        # Write the template yaml
        self._write_data()
        # Make dictionary of DISTINCT run params with any default values.
        self._make_update_dict()
        # Attempt to make helper df
        try:
            # Make helper DataFrame to show what params to add/fill values for.
            self.to_fill_df = pd.DataFrame.from_dict(
                self.update_dict, orient="index"
            ).reset_index()
            self.to_fill_df.columns = ["param", "val"]
            # Adds column "type_hint"
            self._add_type_hints()

            return self.to_fill_df
        # Some combinations of objects (ex: objects with differing levels) can't be coerced
        # nicely into a helper DataFrame, but are still configurable.
        # Return the dictionary the DataFrame was to represent.
        except AttributeError:
            return self.update_dict

    def param(self, param: str, val: Any) -> None:
        """Helper to add parameter values to the keys that will write the config yaml.

        Args:
            param (str): Name of the parameter. ex: 'run_id'
            val (Any): Configurable parameter value.
        """
        if param not in self.update_dict.keys():
            raise KeyError(f"param '{param}' not found in the build.")
        self.update_dict[param] = val

    def make(self) -> None:
        """Once new parameter values have been added, update to the yaml config
        file and write out.
        """
        for mod, params in self.param_dict.items():
            for param in params.keys():
                self.param_dict[mod][param] = self.update_dict[param]

        self._write_data()


class FromBuild:
    """Builds a YAML configuration file from scratch.
    Workflow:
        - Instantiate FromBuild with a path to a config yaml, and optionally,
        an output directory.
        - Optional: View current configuration parameter value(s) by
        calling view_params().
        - To copy parameter configuration entirely, run replicate() with no
        arguments. Otherwise to change any parameters, pass a dictionary of
        parameter:value pairs. If any parameter value needs to change datatype,
        set 'strict' to False.
        - Optional: View updated configuration parameter value(s) by
        calling view_params().
    """

    def __init__(
        self,
        prior_build_path: Union[str, Path, PosixPath],
        out_dir: Optional[Union[str, Path, PosixPath]] = None,
    ):
        """
        Args:
            prior_build_path (Union[str, Path, PosixPath]): Path to a .yaml build
                for replication.
            out_dir (Optional[Union[str, Path, PosixPath]]): Directory to store new
                version of the yaml. Defaults to None. Which uses the same directory
                of prior_build_path and deprecates the prior build.
        """

        # template_path renamed yaml_path to align with RunConfig attribute.
        self._manage_paths(prior_build_path=prior_build_path, out_dir=out_dir)
        self.settings = get_yaml(path=self.yaml_path)
        self.settings_exclude = [
            "_access_run_params",
            "_parse_yaml",
            "_make_config",
            "yaml_path",
            "info",
        ]

    def _manage_paths(
        self,
        prior_build_path: Union[str, Path, PosixPath],
        out_dir: Optional[Union[str, Path, PosixPath]] = None,
    ) -> None:
        """Sets I/O paths based on instantiated inputs.
        If the intended output is the same as the current config, this will save
        the prior version with the prefix 'depr_' to the filename.
        Sets self.yaml_path and self.out_path attributes.

        Args:
            prior_build_path (Union[str, Path, PosixPath]): Path to a .yaml build
                for replication.
            out_dir (Optional[Union[str, Path, PosixPath]]): Directory to store new
                version of the yaml. Defaults to None. Which uses the same directory
                of prior_build_path and deprecates the prior build.
        """

        prior_build_path = Path(prior_build_path)

        if not out_dir or prior_build_path.parent == Path(out_dir):
            self.settings = get_yaml(prior_build_path)
            self.out_path = str(prior_build_path.parent / f"depr_{prior_build_path.name}")
            self._write_data()
            del self.settings
            self.out_path = str(prior_build_path.parent / prior_build_path.name)
        else:
            self.yaml_path = str(prior_build_path)
            self.out_path = str(Path(out_dir) / prior_build_path.name)

    def _get_items(self, obj: Any, exclude: List[str] = []) -> List[str]:
        """Get any non-hidden attributes/methods for an object that are not explicitly
        marked to exlcude.

        Args:
            obj (Any): An object.
            exclude (List[str], optional): Object attributes to ignore. Defaults to [].

        Returns:
            List[str]: An objects non-hidden attributes.
        """
        attributes = [
            att[0]
            for att in inspect.getmembers(obj)
            if not att[0].startswith("__") and att[0] not in exclude
        ]

        return attributes

    def _cast_dtype(self, val: Any, expected_type: type) -> Any:
        """Check if 'val' is the expected dtype and will attempt to cast
        to it if it is not.

        Args:
            val (Any): Any singular value.
            expected_type (type): Type the val ought to be.

        Raises:
            TypeError: If 'val' isn't the expected dtype and can't be cast to it.

        Returns:
            Any: Value with the correct dtype.
        """

        if not isinstance(val, expected_type):
            casted_val = expected_type(val)
            if isinstance(casted_val, expected_type):
                return casted_val
            else:
                raise TypeError(f"Expected {expected_type}, found '{type(val)}'")
        else:
            return val

    def _check_update_types(
        self, funcs: List[str], new_param_dict: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Makes sure the parameter that are being passed to update are of
        the same type as the current config. Will attempt to coerce to correct
        dtype.

        Args:
            funcs (List[str]): Functions/steps in the worklfow.
            new_param_dict (Dict[str, Any]): Any parameters to map into config.

        Returns:
            Dict[str, Any]: Validated map of new parameter and values.
        """

        dl = [vars(getattr(self.settings, att)) for att in funcs]
        old_params_types = dict(
            set(tuple([k, type(v)]) for dict_elem in dl for k, v in dict_elem.items())
        )
        for param, dtype in old_params_types.items():
            if param in new_param_dict.keys():
                new_param_dict[param] = self._cast_dtype(
                    val=new_param_dict[param], expected_type=dtype
                )

        return new_param_dict

    def _add_metadata(self) -> None:
        """Adds update info basic metadata for the yaml."""
        self.settings.info["metadata"]["replicated_by"] = getuser()
        fmt = "%Y/%m/%d"
        dt_label = datetime.datetime.strftime(datetime.datetime.today(), fmt)
        self.settings.info["metadata"]["replicated_on"] = dt_label
        self.settings.info["metadata"]["parameters_changed"] = self.new_param_names

    def _make_vars(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Looks through a set of nested dictionaries to find any
        dataclasses and convert them to a dictionary of param:value pairs.

        Args:
            data (Dict[str, Any]): Nested dictionary from parsed yaml.

        Returns:
            Dict[str, Any]: Input dictionary with and dataclasses unpacked
            into dictionaries.
        """
        for k, v in data.items():
            if isinstance(v, dict):
                for val in v.values():
                    if isinstance(val, dict):
                        data[k] = val
                    else:
                        data[k] = self._make_vars(v)
            else:
                if is_dataclass(data[k]):
                    data[k] = vars(data[k])
                else:
                    data[k] = data[k]
        return data

    def _write_data(self) -> None:
        """Write a formatted yaml with desired data to self.out_path."""

        data = vars(self.settings)

        # Don't update when doing deprecation write out.
        if data["yaml_path"][:5] != "depr_":
            data["info"]["metadata"]["previous_build_path"] = data.pop("yaml_path")

        update = {"metadata": data["info"]["metadata"], "steps": data["info"]["steps"]}

        data = self._make_vars(data)
        data = replace_nulls(data)
        data.pop("info")

        update["run_params"] = data

        with open(self.out_path, "w") as yaml_file:
            yaml.dump(update, yaml_file, Dumper=CustomDumper, sort_keys=False)

        # Reset base on new write out for recursive updating/viewing.
        self.yaml_path = self.out_path

    def replicate(self, new_param_map: Dict[str, Any] = {}, strict: bool = True) -> None:
        """Reuse a YAML config file with option to modify parameter values.
        Writes to self.out_dir with the same name.

        Args:
            new_param_map (Dict[str, Any], optional): Any parameters that need
                modified with the new values.
                Defaults to {}. This will keep all prior parameter values.
            strict (bool, optional): Indicate if any new params must match the
                dtype of the parameter value in the previous config.
                Defaults to True which enforces dtype matching.
        """

        funcs = self._get_items(obj=self.settings, exclude=self.settings_exclude)
        if strict:
            new_param_map = self._check_update_types(funcs=funcs, new_param_dict=new_param_map)

        self.new_param_names = list(new_param_map.keys())

        for att in funcs:
            tmp = getattr(self.settings, att)
            dc_atts = self._get_items(tmp)
            updater = {}
            for param in dc_atts:
                if param in new_param_map.keys():
                    updater[param] = new_param_map[param]
            if len(updater) > 0:
                setattr(self.settings, att, replace(tmp, **updater))

        self._add_metadata()
        self._write_data()

    def view_params(self) -> List[Any]:
        """Create a viewable list of parameters in their named dataclasses.

        Returns:
            List[Any]: Dynamic collection of dataclasses with defined values.
        """

        self.settings = get_yaml(path=str(self.yaml_path))
        funcs = self._get_items(obj=self.settings, exclude=self.settings_exclude)

        data_params = []
        for d in [{func: vars(self.settings)[func]} for func in funcs]:
            for v in d.values():
                data_params.append(v)

        return data_params


class CustomDumper(yaml.SafeDumper):
    """Small class for yaml writer formatting."""

    def write_line_break(self, data: Any = None) -> None:
        """Adds a line break between main levels during write out."""
        super().write_line_break(data)

        if len(self.indents) == 1:
            super().write_line_break()


def get_yaml(path: Union[str, Path, PosixPath]) -> RunConfig:
    """Read in and parse a yaml into a config object.

    Args:
        path (Union[str, Path, PosixPath]): Path to the yaml.

    Returns:
        RunConfig: Config object with defined dataclass attributes.
    """
    config = RunConfig(path=path)
    config._make_config()

    return config


def replace_nulls(obj: Any, replacement: Optional[Any] = None) -> Any:
    """For any size series of nested objects, replace any nulls with 'replacement'.
    NOTE: This does not make a copy of 'obj'. If 'obj' is a dictionary the oringinal
    object will be modified.

    Args:
        obj (Any): Any object to parse and replace any null values.
        replacement (Optional[Any]): Value to replace nulls with.
            Defaults to None which replaces with python's NoneType.

    Returns:
        Any: Input 'obj' with same structure and type, but nulls replaced.
    """

    if isinstance(obj, dict):
        for key, val in obj.items():
            if not isinstance(val, Iterable):
                if pd.isnull(val):
                    obj[key] = replacement
            else:
                obj[key] = replace_nulls(obj[key], replacement)

    elif isinstance(obj, list):
        for i, e in enumerate(obj):
            if isinstance(e, Iterable):
                replace_nulls(e, replacement)
            elif pd.isnull(e):
                obj[i] = replacement
    else:
        if pd.isnull(obj):
            obj = replacement

    return obj
