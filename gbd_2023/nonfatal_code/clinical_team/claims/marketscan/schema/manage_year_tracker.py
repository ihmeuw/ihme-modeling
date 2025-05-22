from datetime import datetime
from typing import List

import pandas as pd
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser


def update_table(year_table: pd.DataFrame, new_years: List[int]) -> pd.DataFrame:
    """Update the tracking table for converting raw marketscan data."""
    if len(year_table) == 0:
        event_id = 1  # initial conversion event
    else:
        event_id = year_table["conversion_event_id"].max() + 1
        overlap_years = set(new_years).intersection(year_table.year)
        if overlap_years:
            raise RuntimeError(f"Cannot re-process an already processed year {overlap_years}")

    new_year_df = pd.DataFrame(
        {"year": new_years, "conversion_event_id": event_id, "date": datetime.now()}
    )

    year_table = pd.concat(
        [year_table, new_year_df], sort=False, ignore_index=True
    ).sort_values(["conversion_event_id", "year"])

    return year_table


def save_ingestion_table(year_table: pd.DataFrame) -> None:
    """Save the conversion year tracking table."""
    file_path = (
        "FILEPATH"
    )
    if file_path.exists():
        bak = pd.read_parquet(file_path)
        # Validate that years are not removed from tracking table
        missing_bak_years = set(bak.year) - set(year_table.year)
        if missing_bak_years:
            raise RuntimeError(
                "This does not support removing an already processed "
                "year from the tracking table"
            )

    year_table.to_parquet(file_path)
    year_table.to_parquet(
        "FILEPATH"
    )


def get_all_years() -> List[int]:
    """Returns a list of all years that have been converted to the updated schema."""
    path = (
        "FILEPATH"
    )
    all_years = pd.read_parquet(path)["year"]

    if len(all_years) != all_years.unique().size:
        raise RuntimeError("Duplicate year values are present")

    return all_years.tolist()
