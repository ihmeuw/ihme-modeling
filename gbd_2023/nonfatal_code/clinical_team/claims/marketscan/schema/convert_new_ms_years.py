"""Controller script to convert new years of Marketscan data from the raw schema into the
updated ICD-mart schema, create sample data for e2e tests and update this in a tracking
table if everything goes well.
"""

from typing import List

import pandas as pd
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser

from marketscan.schema import build_schema_tasks, manage_year_tracker


def convert_ms(new_years: List[int]) -> str:
    """
    Parent interface for converting new years of Marketscan data to the ICD-mart schema.
    This should handle all aspects of prepping new data-
        1) e2e test data is created, but only if it doesn't already exist on disk.
        2) Jobmon workflow sends out jobs to convert data.
        3) year_table tracker is updated once 1 and 2 are complete.
    """

    # get the year_table and make sure there's no intersection between all years and new years
    year_path = (
        filepath_parser(ini="pipeline.marketscan", section="data", section_key="write_dir")
        / "year_tracking_table.parquet"
    )
    if year_path.exists():
        year_table = pd.read_parquet(year_path)
        overlap = set(year_table.year).intersection(new_years)
        if overlap:
            raise RuntimeError(
                "You cannot re-process ICD-Mart data without manually deleting existing "
                f"year's data first. Overlapping years {overlap}"
            )
    else:
        year_table = pd.DataFrame()

    # run build schema tasks for jobmon workflow of main conversion code
    wr = build_schema_tasks.create_ms_schema_years(years=new_years)

    # when workflow completes and is done run the update tracker stuff.
    if wr == "D":
        year_table = manage_year_tracker.update_table(year_table, new_years)
        manage_year_tracker.save_ingestion_table(year_table)

    return wr
