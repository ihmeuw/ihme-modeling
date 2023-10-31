from bdb import BdbQuit
import functools
from pathlib import Path
from typing import Any, Callable, Dict, NamedTuple, Optional, Tuple, Union

from covid_shared import cli_tools, paths


MaybePathLike = Union[str, Path, None]


class VersionInfo(NamedTuple):
    """Tiny struct for processing input versions from cli args and specs."""
    cli_arg: MaybePathLike
    spec_arg: MaybePathLike
    default: MaybePathLike
    metadata_key: str
    allow_default: bool


def resolve_version_info(specification, run_metadata: cli_tools.RunMetadata, input_versions: Dict[str, VersionInfo]):
    """Resolves cli args, spec values, and defaults and makes the
    specification and metadata consistent.

    """
    for version_key, version_info in input_versions.items():
        if not hasattr(specification.data, version_key):
            raise TypeError(f'Invalid key {version_key} for specification data {specification.data}.')
        if not (version_info.cli_arg or version_info.spec_arg or version_info.allow_default):
            continue
        input_root = get_input_root(version_info.cli_arg, version_info.spec_arg, version_info.default)
        setattr(specification.data, version_key, str(input_root))
        run_metadata.update_from_path(version_info.metadata_key, input_root / paths.METADATA_FILE_NAME)
    return specification, run_metadata


def handle_exceptions(func: Callable, logger: Any, with_debugger: bool) -> Callable:
    """Drops a user into an interactive debugger if func raises an error."""

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (BdbQuit, KeyboardInterrupt):
            raise
        except Exception as e:
            logger.exception("Uncaught exception {}".format(e))
            if with_debugger:
                import pdb
                import traceback
                traceback.print_exc()
                pdb.post_mortem()
            else:
                raise

    return wrapped


def get_input_root(cli_argument: Optional[str], specification_value: Optional[str],
                   last_stage_root: Union[str, Path]) -> Path:
    """Determine the version to use hierarchically.

    CLI args override spec args.  Spec args override the default 'best'.

    """
    version = _get_argument_hierarchically(cli_argument, specification_value, paths.BEST_LINK)
    root = cli_tools.get_last_stage_directory(version, last_stage_root=last_stage_root)
    return root


def get_output_root(cli_argument: Optional[str], specification_value: Optional[str]) -> Path:
    """Determine the output root hierarchically.

    CLI arguments override specification args.  Specification args override
    the default.

    """
    # Default behavior handled by CLI.
    version = _get_argument_hierarchically(cli_argument, specification_value, cli_argument)
    version = Path(version).resolve()
    return version


def get_location_info(location_specification: Optional[str],
                      spec_lsvid: Optional[int],
                      spec_location_file: Optional[str]) -> Tuple[int, str]:
    """Resolves a location specification from the cli args and run spec.

    Parameters
    ----------
    location_specification
        Either a location set version  id or a path to a location
        hierarchy file.
    spec_lsvid
        The location set version id provided in the run spec.
    spec_location_file
        The location file provided in the run spec.

    Returns
    -------
        A valid lsvid and location file specification constructed from the
        input arguments.  CLI args take precedence over run spec parameters.
        If nothing is provided, return 0 and '' for the lsvid and location
        file, respectively, and let the model stage code set sensible
        default operations.

    """
    if spec_lsvid and spec_location_file:
        raise ValueError('Both a location set version id and a location file were provided in '
                         'the specification. Only one option may be used.')
    location_specification = _get_argument_hierarchically(location_specification, spec_lsvid, 0)
    location_specification = _get_argument_hierarchically(location_specification, spec_location_file, '')
    try:
        lsvid = int(location_specification)
        location_file = ''
    except ValueError:  # The command line argument was a path
        lsvid = 0
        location_file = location_specification
    return lsvid, location_file


def _get_argument_hierarchically(cli_argument: Optional,
                                 specification_value: Optional,
                                 default: Any) -> Any:
    """Determine the argument to use hierarchically.

    Prefer cli args over values in a specification file over the default.
    """
    if cli_argument:
        output = cli_argument
    elif specification_value:
        output = specification_value
    else:
        output = default
    return output
