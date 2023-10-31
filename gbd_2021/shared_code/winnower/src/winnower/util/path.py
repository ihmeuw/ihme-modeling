import os
from pathlib import Path
import re
import sys

import pandas

from winnower import errors

# Override these for alternative J and L mount locations
LINUX_MOUNT_PATHS = {
    "J:": "<FILEPATH>",
    "L:": "<FILEPATH>"
}

OSX_MOUNT_PATHS = {
    "J:": "<FILEPATH>",
    "L:": "<FILEPATH>",
}

PATH_ROOTS = {*LINUX_MOUNT_PATHS,  # windows roots
              *LINUX_MOUNT_PATHS.values(),
              *OSX_MOUNT_PATHS.values()}

_strip_root_re = re.compile("^({})".format(
    "|".join(re.escape(root) for root in PATH_ROOTS)))


def strip_root_and_standardize(val: Path):
    """
    Strip root from path and standardize across platforms.

    This helper function is used in conjunction with path arguments to create
    strings that are consistent across platforms. This makes them easier to
    compare.
    """
    return _strip_root_re.sub("", str(val).replace("\\", "/"), count=1)


def fix_survey_path(val: str):
    """
    Fix Windows-based Drive prefix to match platform.

    Also normalize to forward slashes.
    """
    if pandas.isnull(val):  # handle NaN values
        return val

    if sys.platform == 'win32':
        val = val.replace('/', '\\')  # normalize to back slashes
    elif sys.platform == 'linux':
        val = val.replace('\\', '/')  # normalize to forward slashes
        for win_root, linux_root in LINUX_MOUNT_PATHS.items():
            val = val.replace(win_root, linux_root, 1)
    elif sys.platform == 'darwin':
        val = val.replace('\\', '/')  # normalize to forward slashes
        for win_root, osx_root in OSX_MOUNT_PATHS.items():
            val = val.replace(win_root, osx_root, 1)
    else:
        raise RuntimeError(f"No rule to support platform {sys.platform!r}")

    return val


def resolve_path_case(path: Path):
    """
    Resolves path to the case-correct version.

    This is necessary because most clients are on Windows/OS X (which uses
    case-insensitive paths) but the client must also work on Linux (which
    requires case-correct paths).
    """
    if sys.platform in {'darwin', 'win32'}:
        return path  # assume correct

    try:
        if path.exists():
            return path
    except OSError:
        # OSError: [Errno 107] Transport endpoint is not connected:
        msg = (f"Errored checking if path {path} exists. "
               "Are remote drives working?")
        raise errors.ValidationError(msg)
    # Create stack of incorrect pieces
    orig_path = path
    stack = []
    while not path.exists():
        stack.append(path.name.lower())
        path = path.parent
    # Assemble working path from pieces
    while stack:
        piece_lower = stack.pop()
        candidates = [X for X in os.listdir(path) if X.lower() == piece_lower]
        try:
            piece, = candidates
        except ValueError:  # list comprehension len != 1
            preamble = f'Path {orig_path} invaid. Component {piece_lower!r}'
            if candidates:
                specific = f'matches multiple things: {candidates!r}'
                msg = ' '.join([preamble, specific])
            else:
                if path == J_drive():
                    msg = f"Path {orig_path} invalid. Is J: mounted?"
                elif path == L_drive():
                    msg = f"Path {orig_path} invalid. Is L: mounted?"
                else:
                    specific = f'matches nothing in {path}'
                    msg = ' '.join([preamble, specific])
            raise errors.ValidationError(msg) from None
        else:
            path = path / piece

    return path


def J_drive():
    """Return path to <FILEPATH> AKA the <FILEPATH> drive."""
    try:
        return Path(fix_survey_path('J:'))
    except RuntimeError:
        msg = ("Unsure where <FILEPATH> AKA <FILEPATH> should be located "
               f"on OS {sys.platform!r}")
        raise errors.Error(msg)


def L_drive():
    """Return path to <FILEPATH> AKA the <FILEPATH> drive."""
    try:
        return Path(fix_survey_path('L:'))
    except RuntimeError:
        msg = ("Unsure where <FILEPATH> AKA <FILEPATH> should be located "
               f"on OS {sys.platform!r}")
        raise errors.Error(msg)
