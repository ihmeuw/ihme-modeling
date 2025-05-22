from configparser import ConfigParser, ExtendedInterpolation
from pathlib import Path
from typing import Any, Dict, Union


class Parser:
    """Parses file path .ini files given a path to a config file and a
    key after instantiation."""

    def __init__(self, config_file: Union[Path, str]):
        self.config_file = config_file

    @property
    def parser(self):
        """Creates parser and reads config file on disk.
        Ensures pathlib.Path types are returned."""
        parser = ConfigParser(interpolation=ExtendedInterpolation(), converters={"path": Path})
        self.config_file = parser.read(self.config_file)

        return parser

    def __getitem__(self, key):
        return self.parser[key]


def _get_parser(ini: str) -> Parser:
    """Initalize the parser class using the input ini file."""
    try:
        ini_group, ini_file_stem = ini.split(".")
    except ValueError:
        raise ValueError(
            "The ini argument for this function requires the pattern "
            "'ini_group.ini_file_stem'. Please see the docstring for an example of this."
        )

    ini_disk_path = Path(__file__).with_name(f"{ini_group}") / f"{ini_file_stem}.ini"
    parser_instance = Parser(ini_disk_path)
    return parser_instance


def filepath_parser(ini: str, section: str, section_key: str) -> Path:
    """Returns a file path from a given ini. Wraps around the Parser class. Expects the `ini`
    string to match the following pattern - "{ini_group}.{ini_file_stem}" so for the CMS
    pipeline an example call would be-
    cms_path = filepath_parser(ini="pipeline.cms", section="table_outputs", section_key="age")

    Args:
        ini: Specific ini file containing file paths for a certain use case.
        section: The organizational concept above a specific file path. Usually groups
                 multiple paths together by purpose.
        section_key: The name of a specific filepath. Must exist within the section of an
                     ini file.

    Returns:
        The file path as defined in the ini, section, and section_key
    """
    parser_ini = _get_parser(ini=ini)
    filepath = parser_ini[section].getpath(section_key)

    if not filepath:
        raise KeyError(
            f"The section_key {section_key} does not seem to exist in the {section} entry "
            f"of the {ini} .ini file"
        )
    return filepath


def section_parser(ini: str, section: str) -> Dict[Any, Any]:
    """Given an ini name and a section within it, return entries in the section in the form
    of a dictionary.

    Args:
        ini: Specific ini file containing file paths for a certain use case.
        section: The organizational concept above a specific file path. Usually groups
                 multiple paths together by purpose.

    Returns:
        Dictionary containing all entries stored in the ini's requested section.
    """
    parser_ini = _get_parser(ini=ini)

    return dict(parser_ini[section])