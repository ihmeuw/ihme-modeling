import types

from functools import wraps
from importlib.metadata import version
from typing import Any, Callable, Dict, Optional

from pydantic import BaseModel, validate_arguments


def clinical_typecheck(
    arg: Optional[Callable] = None,
    *,
    config: Optional[Dict[str, Any]] = None,
    coerce: Optional[bool] = True,
) -> Callable:
    """Main function for Clinical type checking.  This operates as helper function
    for _type_checker calls. Enables the same behavior as the core tooling.

    Args:
        config: Config dictionary to override the pydantic defaults.
            Ex: 'arbitrary_types_allowed' or 'validate_return'.
            Defaults to None which keeps all defaults, but will dynamically add
            arbitrary_types_allowed.
        coerce: If parameter dataype casting should take place.
            Passing coerce=True (default) will try to cast any parameters to the
            defined datatype(s) and break if not correctly typed afterwards when being
            passed into the decorated function. Can not be turned off in pydantic < 2.
            NOTE: Casting feature is not available on arbitrary types.

    Returns:
        Typechecking decorator for Callables.
    """
    if arg is not None and callable(arg):
        return _type_checker(config=config, coerce=coerce)(arg)
    else:
        return lambda func: _type_checker(config=config, coerce=coerce)(func)


def _type_checker(
    _func: Optional[Callable] = None,
    *,
    config: Optional[Dict[str, Any]] = None,
    coerce: Optional[bool] = True,
) -> Callable:
    """Wrapper around pydantic.validate_arguments to create
    a centralized decorator/implementation.

    Args:
        config: Config dictionary to override the pydantic defaults.
            Ex: 'arbitrary_types_allowed' or 'validate_return'.
            Defaults to None which keeps all defaults.
        coerce: If parameter dataype casting should take place.
            Passing coerce=True (default) will try to cast any parameters to the
            defined datatype(s) and break if not correctly typed afterwards when being
            passed into the decorated function. Can not be turned off in pydantic < 2.
            NOTE: Casting feature is not available on arbitrary types.

    Returns:
        Typechecking decorator for Callables.
    """

    def _set_config(
        config: Optional[Dict[str, Any]] = None, coerce: Optional[bool] = None
    ) -> Dict[str, Any]:
        """Creates the configuration dictionary and adds any user input."""
        config_dict = dict
        if not config:
            config = {}
        configuration = config_dict(**config)
        package_version = version("pydantic")
        if int(package_version[0]) < 2:
            if not coerce:
                raise VersionError("Pyndantic < 2 can only coerce input parameters.")
        else:
            if not coerce:
                configuration["strict"] = True
        return configuration

    def _arbitrary_types_present(*args, **kwargs) -> bool:
        """True if any args or kwargs are arbitrary types."""
        if any([_check_if_arbitrary(obj) for obj in kwargs.values()]) or any(
            [_check_if_arbitrary(obj) for obj in args]
        ):
            return True
        else:
            return False

    def _check_if_arbitrary(obj: Any) -> bool:
        """True if object is non-built-in or non-pydantic type."""
        if not _python_type(obj=obj) or not _pydantic_type(obj=obj):
            return True
        else:
            return False

    def _python_type(obj: Any) -> bool:
        """True if the object has a built-in type."""
        return type(obj) in types.__dict__["__builtins__"].values()

    def _pydantic_type(obj: Any) -> bool:
        """True if the object has a pydantic type."""
        return issubclass(type(obj), BaseModel)

    def _wrapper(_func: Callable, *, config=config, coerce=coerce) -> Callable:
        """Creates config and sets validator for decorator."""
        # Set a base config dictionary from passed
        configuration = _set_config(config=config, coerce=coerce)

        validator_func = validate_arguments

        @wraps(_func)
        def _inner_wrapper(*args, **kwargs) -> None:
            """Decorator with desired configuration inherited from _wrapper."""
            if (
                _arbitrary_types_present(*args, **kwargs)
                and "arbitrary_types_allowed" not in configuration.keys()
            ):
                configuration["arbitrary_types_allowed"] = True
            validator = validator_func(config=configuration)(_func)
            return validator(*args, **kwargs)

        return _inner_wrapper

    return _wrapper


class VersionError(Exception):
    pass