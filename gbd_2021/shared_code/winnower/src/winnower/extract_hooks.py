"""
Subsystem for allowing users to write per-extraction code to add behavior.

To customize an extraction users should create a file named nid_XXX.py in the
location defined in __path__ (see below). XXX should be the NID of the
extraction in question.

Within that file users should define an ExtractionHook subclass. That subclass
must have the following attributes:

    SURVEY_NAME
    NID
    IHME_LOC_ID
    YEAR_START
    YEAR_END
    SURVEY_MODULE
    FILE_PATH

In addition the ExtractionHook subclass should define *at least 1* decorated by
one of the hook methods pre_extraction, post_custom, or post_extraction.
"""
import collections
import importlib
import inspect
import logging
from pathlib import Path
import sys

import pandas

from winnower.errors import (
    DecorationError,
    Error,
    MultipleValues,
    ValidationError,
)
from winnower.extract import get_column
from winnower.logging import get_class_logger
from winnower.transform.base import OpaqueComponent
from winnower.util.path import J_drive, fix_survey_path

hook_logger = logging.getLogger(__file__)

_HOOK_DIR = "<FILEPATH>"
# This tells Python that e.g., winnower.extract_hooks.my_module will be found
# in the specified system path INSTEAD of in a directory in this package.
# https://www.python.org/dev/peps/pep-0302/#packages-and-the-role-of-path
__path__ = [  # values must be str, not pathlib.Path
    str(_HOOK_DIR),
]

REQUIRED_HOOK_ATTRS = (
    'SURVEY_NAME',
    'NID',
    'IHME_LOC_ID',
    'YEAR_START',
    'YEAR_END',
    'SURVEY_MODULE',
    'FILE_PATH',
)

# All ExtractionHook subclasses with REQUIRED_HOOK_ATTRS will be in this list
_IMPORTED_HOOKS = []
_MATCH_ALL_TOPICS = 'ALL'
_HOOK_METHOD_NAME = '_winnower_hook_name'
_HOOK_METHOD_TOPICS = '_winnower_hook_topics'
_HOOK_METHOD_ADDS_COLUMNS = '_winnower_hook_adds_columns'


class HookMethod:
    """
    A hook method that can be used to decorate functions to run for some or all
    extractions based on topic.
    """
    def __set_name__(self, owner, name):
        """
        Sets the name of this method.

        This uses the descriptor protocol to assign the name to reduce errors.
        The name is automatically assigned when an instance of this appears in
        a class definition.
        """
        self.name = name

    def for_all_topics(self, func):
        "Decorate a custom code method to run for all hooks."
        self._decorate(func)
        getattr(func, _HOOK_METHOD_TOPICS).add(_MATCH_ALL_TOPICS)
        return func

    def for_topics(self, *topics):
        "Decorate a custom code method to run for only certain hooks."
        def inner(func):
            self._decorate(func)
            getattr(func, _HOOK_METHOD_TOPICS).update(topics)
            return func
        return inner

    def _decorate(self, func):
        """
        Prepares a method for further decoration.

        Validates the object is correct and errors if usage is invalid.
        """
        error_if_not_function(func)

        if hasattr(func, _HOOK_METHOD_NAME):
            name = func._winnower_hook_name
            if name != self.name:
                msg = (f"{func!r} is already registered for hook method {name}"
                       f" - cannot register for {self.name}")
            else:
                msg = (f"{func!r} is already registered for {name} - update "
                       "the other decoration.")
            raise ValueError(msg)

        setattr(func, _HOOK_METHOD_NAME, self.name)
        setattr(func, _HOOK_METHOD_TOPICS, set())


def error_if_not_function(func):
    "Helper method to enforce correct practices and provide clear messages."
    if not inspect.isfunction(func):
        raise DecorationError(f"{func!r} is not a function. Cannot decorate")


class ExtractionHook:
    """
    Superclass for all extraction hooks associated with one extraction.

    Subclasses should define each of the seven REQUIRED_HOOK_ATTRS to identify
    the extraction these hooks apply to. In addition one or more methods should
    be defined that alter the extracted data. These methods should be decorated
    as belonging to one of the three hook methods: pre_extraction, post_custom,
    or post_extraction.
    """
    pre_extraction = HookMethod()
    post_custom = HookMethod()
    post_extraction = HookMethod()

    @staticmethod
    def adds_columns(*columns):
        """
        Decorates an extraction hook method to add new columns.

        See docsource/extraction_hooks.md for full examples.
        """
        def decorator(func):
            "Returned decorator that actually modifies a function."
            error_if_not_function(func)

            if not hasattr(func, _HOOK_METHOD_ADDS_COLUMNS):
                setattr(func, _HOOK_METHOD_ADDS_COLUMNS, set())

            getattr(func, _HOOK_METHOD_ADDS_COLUMNS).update(columns)
            return func

        return decorator

    def __init_subclass__(cls, hook_manager=_IMPORTED_HOOKS):
        # Register class with manager
        missing_attrs = [attr for attr in REQUIRED_HOOK_ATTRS
                         if not hasattr(cls, attr)]
        if missing_attrs:  # warn user of invalid hook that will be ignored
            msg = (f"Hook {cls.__name__} is missing required ExtractionHook "
                   f"attributes {missing_attrs!r} - IT CAN NOT BE USED!. "
                   "Please consult winnower documentation for pre and post "
                   "extraction hooks.")
            hook_logger.critical(msg)
            return

        # assign logger to hook class
        cls.logger = get_class_logger(cls)
        hook_manager.append(cls)

        # Coerce file path to same type as will be in UniversalForm
        cls.FILE_PATH = Path(fix_survey_path(cls.FILE_PATH))

    def get_column_name(self, name):
        """
        Returns the best possible column in the DataFrame given `name`.

        * If the codebook value for "psu" is "sample_code", then
          get_column_name('psu') will return the correct column
        * If the column "my_data" is in the DataFrame then
          get_column_name('my_data') will return "my_data"

        This function attempts to support the common use cases for getting a
        column name so that users can use a straightforward and consistent API.
        """
        # If `name` is a codebook column, use that value. Otherwise use as-is
        cfg_val = self.config.get(name, name)
        if not cfg_val:
            raise ValidationError(f"Column {name!r} not configured")
        col_name, _, err = get_column(cfg_val, self.df.columns)
        if col_name is None:
            raise ValidationError(err.args[0])
        return col_name


def get_hooks(universal, topics, config_dict):
    """
    Returns (pre_extraction, post_custom, post_extraction) hook components.

    If no hook methods are defined for a stage None will be returned for that
    component.
    """
    hook_methods = ('pre_extraction', 'post_custom', 'post_extraction')
    hook_module = f'winnower.extract_hooks.nid_{universal.nid}'
    # import the module - ExtractionHook.__init_subclass__ registers subclasses
    try:
        importlib.import_module(hook_module)
    except ModuleNotFoundError:
        return tuple(None for _ in hook_methods)
    except SyntaxError as e:
        err_msg, (filename, line_no, char, text) = e.args

        msg = (f"Syntax error encountered importing {filename} on line "
               f"{line_no}: {text}")
        hook_logger.critical(msg)
        raise Error(msg) from None
    else:
        # get the hook class specific for the configuration
        hook_cls = hook_for_universal_config(_IMPORTED_HOOKS, universal)
        if hook_cls is None:
            return tuple(None for _ in hook_methods)

    method_map = hook_method_map(hook_cls, topics)

    hooks = []
    for hook_method in hook_methods:
        method_names = method_map.get(hook_method, None)
        if method_names is None:
            hooks.append(None)
        else:
            hook = transform_from_ExtractionHook(hook_cls,
                                                 hook_methods=method_names,
                                                 # avoid sharing `config`
                                                 config=config_dict.copy())
            hooks.append(hook)
    return hooks


def hook_for_universal_config(hooks, universal):
    """
    Returns hook class matching universal config.

    Args:
        hooks: collection of hooks to search. Probably _IMPORTED_HOOKS.
        universal: a UniversalForm instance describing the extraction.

    Returns:
        None: no hooks found matching the extraction defined by `universal`.
        hook_cls: the class definition of the hook.

    Raises:
        MultipleValues: 2 or more hooks were defined that match `universal`.
    """
    valid = []

    for hook in hooks:
        name = f"{hook.__module__}.{hook.__name__}"

        for attr in REQUIRED_HOOK_ATTRS:
            uattr = attr.lower()
            if getattr(hook, attr) != getattr(universal, uattr):
                msg = (f"Hook {name} does not match {attr}: "
                       "{} vs {}".format(getattr(hook, attr),
                                         getattr(universal, uattr)))
                hook_logger.critical(msg)
                break
        else:  # only triggers if above loop not broken out of
            valid.append(hook)

    if not valid:
        return None
    if len(valid) == 1:
        return valid[0]
    # 2+ matched - error
    names = "\n".join(f"\t{X.__module__}.{X.__name__}" for X in valid)
    # 13 is the longest string in REQUIRED_HOOK_ATTRS
    values = "\n".join(f"\t{attr:<13}: {getattr(valid[0], attr)}"
                       for attr in REQUIRED_HOOK_ATTRS)
    msg = f"Multiple matches for hook with values\n{values}\nHooks:\n{names}"
    raise MultipleValues(msg)


def hook_method_map(hook_cls, topics):
    """
    Returns a dictionary of (hook_name: [hook, methods]) pairs for hook_cls.

    Args:
        hook_cls: a subclass of ExtractionHook
        topics: collection of str topic names e.g., ['wash', 'diarrhea']
    """
    def is_hook_method(member):
        if not inspect.isfunction(member):
            return False
        member_topics = getattr(member, _HOOK_METHOD_TOPICS, None)
        if member_topics is None:
            return False  # undecorated method
        return _MATCH_ALL_TOPICS in member_topics or any(t in member_topics
                                                         for t in topics)

    result = collections.defaultdict(list)
    for method_name, method in inspect.getmembers(hook_cls, is_hook_method):
        result[getattr(method, _HOOK_METHOD_NAME)].append(method_name)
    return result


def transform_from_ExtractionHook(hook_cls, hook_methods, config):
    """
    Factory for creating transforms used in ExtractionChain's.

    Dispatches based off of hook_cls' configuration.
    """
    def hook_method_adds_columns(hook_method_name: str):
        hook_method = getattr(hook_cls, hook_method_name)
        return hasattr(hook_method, _HOOK_METHOD_ADDS_COLUMNS)

    if any(hook_method_adds_columns(method) for method in hook_methods):
        return HookTransform(hook_cls, hook_methods, config)
    else:
        return OpaqueHookTransform(hook_cls, hook_methods, config)


class OpaqueHookTransform(OpaqueComponent):
    """
    Wraps a single extraction hook with an OpaqueComponent.
    """
    def __init__(self, hook_cls, hook_methods, config):
        self.hook_cls = hook_cls
        self.hook_methods = hook_methods
        self.config = config

    def execute(self, df):
        "Perform setup and then run the extraction hook."
        try:
            hook = self.hook_cls()
        except Exception as e:
            tb = sys.exc_info()[2]
            msg = f"Error in {self.hook_cls.__name__}.__init__: {e}"
            raise Error(msg).with_traceback(tb.tb_next) from None

        for method_name in self.hook_methods:
            # assign `df` for convenience in use of get_column_name
            hook.df = df
            # assign `config` for use of get_column_name.
            hook.config = self.config.copy()  # avoid accidental mutation
            method = getattr(hook, method_name)
            try:
                df = method(df)
            except Exception as e:
                tb = sys.exc_info()[2]
                msg = f"Error in {self.hook_cls.__name__}.{method_name}: {e}"
                raise Error(msg).with_traceback(tb.tb_next) from None

            if not isinstance(df, pandas.DataFrame):
                msg = (f"ERROR: {self.hook_cls}.{method_name} did not return "
                       "a DataFrame. Fix the code to return a DataFrame!")
                raise Error(msg)
        return df


class HookTransform(OpaqueHookTransform):
    """
    Similar to OpaqueHookTransform, but not opaque.

    This means it provides the full Component API.
    """
    input_columns = None

    def _validate(self, input_columns):
        """
        Validates if the columns in uses_extra_columns are actually
        present in the survey data:
        """
        extra_columns = getattr(self.hook_cls, 'USES_EXTRA_COLUMNS', [])

        if extra_columns:
            not_found = set(extra_columns).difference(input_columns)
            if not_found:
                msg = ("These columns in uses_extra_columns could not "
                       f"be found: {not_found}")
                raise Error(msg)
        self.input_columns = extra_columns

    def get_uses_columns(self):
        return self.input_columns

    def output_columns(self, input_columns):
        result = list(input_columns)
        for method_name in self.hook_methods:
            method = getattr(self.hook_cls, method_name)
            added_cols = getattr(method, _HOOK_METHOD_ADDS_COLUMNS, [])
            result.extend(added_cols)
        return result
