import pandas as pd

from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser


def pull_poland_year_source_table() -> pd.DataFrame:
    """Gets a DataFrame of the current Poland file tracker table."""
    path = filepath_parser(ini="pipeline.pol_nhf", section="logs", section_key="base")
    return pd.read_csv("FILEPATH")