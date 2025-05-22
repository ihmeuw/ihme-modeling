from typing import Dict, List

import pandas as pd

from clinical_io.schema.file_handlers import pull_poland_year_source_table


def get_poland_best_year_file_source_id(year_ids: List[int]) -> Dict[int, List[int]]:
    """Create a dict where the key is a bested file source ID and the value is
    the years that correspond to that `file_source_id`.

    Args:
        year_ids: Years of data being processed.

    Raises:
        ValueError: No bested file source ID for any year.
        ValueError: More than one bested file source ID for any year.

    Returns:
        `file_source_id` for the bested year(s).
    """
    df = pull_poland_year_source_table()
    df = df[df["is_best"] == 1]

    _validate_year_source_table(df=df, year_ids=year_ids)

    file_source_years = df[["file_source_id", "year_id"]].to_dict("records")
    lookup_dict: Dict[int, List[int]] = {}
    for e in file_source_years:
        file_source = e["file_source_id"]
        year = e["year_id"]
        if year not in year_ids:
            continue
        if file_source not in lookup_dict.keys():
            lookup_dict.update({file_source: [year]})
        else:
            v = lookup_dict[file_source]
            lookup_dict[file_source] = v + [year]
    return lookup_dict


def _validate_year_source_table(df: pd.DataFrame, year_ids: List[int]) -> None:
    """Validate the year source table.

    Arguments:
        df: The year source table.
        year_ids: The years of data being processed.

    Raises:
        ValueError: No bested file source ID for any year.
        ValueError: More than one bested file source ID for any year.
    """
    if not year_ids:
        raise ValueError("No years to process.")

    validation_df = df[(df["year_id"].isin(year_ids))]
    missing_years = set(year_ids) - set(validation_df["year_id"].tolist())
    if missing_years:
        raise ValueError(f"Missing best file source IDs for years: {missing_years}")

    too_many_best_sources_df = pd.DataFrame(
        validation_df.groupby("year_id")["is_best"].count()
    )
    too_many_best_sources_df = df[
        df["year_id"].isin(
            too_many_best_sources_df[too_many_best_sources_df["is_best"] > 1].index
        )
    ]
    if not too_many_best_sources_df.empty:
        raise ValueError(
            "There are years with more than one best file source ID: "
            f"{set(too_many_best_sources_df['year_id'])}"
        )