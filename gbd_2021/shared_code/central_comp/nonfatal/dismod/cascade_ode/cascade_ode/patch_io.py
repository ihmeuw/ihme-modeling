"""
This module patches all functions in a module
so that they do nothing.
"""
import logging
import sys
from inspect import getmembers, getmodule, isfunction

import cascade_ode.upload  # noqa: F401

LOGGER = logging.getLogger(__name__)


def _do_nothing(*_args, **_kwargs):
    """A mock function that does nothing."""


def mock_functions_in_module(module_name):
    """
    Make every function defined in the given
    module a do-nothing function. If the module has a function that is
    imported from another module, it will not be mocked.

    Args:
        module_name (str): Dotted name of the module,
        so ``cascade_ode.upload``.
    """
    try:
        module_to_mock = sys.modules[module_name]
    except KeyError:
        raise ValueError(f"Cannot mock functions in {module_name} because "
                         f"it is not yet imported.")
    func_names = [
        name for (name, value) in getmembers(module_to_mock)
        if isfunction(value) and getmodule(value) == module_to_mock
        ]
    LOGGER.info(
        f"mocking functions in {module_name}: {', '.join(func_names)}")
    for func_name in func_names:
        setattr(module_to_mock, func_name, _do_nothing)


def setup_io_patches(no_upload):
    """
    Makes all uploads into dummy functions for the upload module.

    Args:
        no_upload (bool): Whether to turn off uploads to the database.
    """
    if no_upload:
        mock_functions_in_module("cascade_ode.upload")
