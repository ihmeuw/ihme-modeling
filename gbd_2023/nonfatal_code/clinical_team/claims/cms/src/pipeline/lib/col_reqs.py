"""
Apply column requirements to CMS data before it's finished with the pipeline
"""
import pandas as pd

from cms.utils.process_types import Deliverable


def reshape_race_col(df: pd.DataFrame, deliverable: Deliverable) -> pd.DataFrame:
    """Renames race_col to generalizable 'race_cd' col name and creates a new type
    column with former col name.

    Args:
        df (pd.DataFrame): Table being processed.
        deliverable (Deliverable):
            Deliverable specific processing object from process_types.

    Returns:
        pd.DataFrame: Input table with standardized race column added.
    """

    if deliverable.race_deliverable:
        race_col = deliverable.race_deliverable
        df["race_cd_type"] = race_col
        df = df.rename(columns={race_col: "race_cd"})
    else:
        pass

    return df


def manage_cols(df: pd.DataFrame, deliverable: Deliverable) -> pd.DataFrame:
    """Drops columns depending on deliverable.
    USHD prefers year_id over year_start/end while GBD requires year_start/end for the db.

    Args:
        df (pd.DataFrame): Table being processed.
        deliverable (Deliverable):
            Deliverable specific processing object from process_types.

    Returns:
        pd.DataFrame: Input table with specific columns removed.
    """

    drops = [
        "pre_nr_mean",
        "age_start",
        "age_end",
    ]

    drops += deliverable.col_reqs_drop_cols

    drops = [d for d in drops if d in df.columns]
    df = df.drop(drops, axis=1)
    return df


def set_col_order(df: pd.DataFrame) -> pd.DataFrame:
    """Orders the columns of the final data base on deliverable.
    NOTE: This happens before columns are dropped based on deliverable.

    Args:
        df (pd.DataFrame): Table being processed.

    Raises:
        ValueError: Differing column name values after reorder.

    Returns:
        pd.DataFrame: Input table with columns set to specific order.
    """

    pre_cols = df.columns
    disease = ["bundle_id", "estimate_id"]
    demo = [
        "age",
        "age_group_id",
        "age_start",
        "age_end",
        "sex_id",
        "location_id",
        "fips_6_4_code",
        "year_id",
        "year_start",
        "year_end",
        "race_cd",
        "race_cd_type",
    ]
    other = [
        "val",
        "mean",
        "pre_nr_mean",
        "sample_size",
        "metric_id",
        "facility_id",
        "estimate_type",
        "nid",
        "is_identifiable",
    ]

    col_order = disease + demo + other
    col_order = [c for c in col_order if c in pre_cols]
    df = df[col_order]

    diff = set(pre_cols).symmetric_difference(df.columns)
    if diff:
        raise ValueError(
            f"The current column order is not sufficient. Column difference {diff}"
        )
    return df


def sort_row_order(df: pd.DataFrame) -> pd.DataFrame:
    """Orders the rows of the final data based on the deliverable.

    Args:
        df (pd.DataFrame): Table being processed.

    Returns:
        pd.DataFrame: Input table with rows sorted by columns found in
            'row_order' in its order.
    """
    row_order = [
        "location_id",
        "fips_6_4_code",
        "year_id",
        "sex_id",
        "age",
        "age_start",
        "age_end",
        "race_cd",
    ]
    row_order = [c for c in row_order if c in df.columns]
    df = df.sort_values(row_order)
    return df


def clean_cols(df: pd.DataFrame, deliverable: Deliverable) -> pd.DataFrame:
    """Runs: reshape_race_col, sort_row_order, set_col_order, and manage_cols.

    Args:
        df (pd.DataFrame): Table being processed in CMS run.
        deliverable (Deliverable):
            Deliverable specific processing object from process_types.

    Raises:
        RuntimeError: Change in row count after cleaning.

    Returns:
        pd.DataFrame: Input table standardized based on deliverable.
    """
    pre = len(df)

    # assign all 3 types of year columns, to be filtered in manage_cols
    df["year_start"], df["year_end"] = df.year_id, df.year_id

    df = reshape_race_col(df, deliverable)
    df = sort_row_order(df)
    df = set_col_order(df)
    df = manage_cols(df, deliverable)

    if len(df) != pre:
        raise RuntimeError(f"Processing changed row count by '{pre - len(df)}' rows.")
    return df


def remove_locs(df: pd.DataFrame, deliverable: Deliverable) -> pd.DataFrame:
    """Removes certain locations from the table based on the given deliverable.

    Args:
        df (pd.DataFrame): Table being processed.
        deliverable (Deliverable):
            Deliverable specific processing object from process_types.

    Returns:
        pd.DataFrame: Location subset of input table.
    """
    rm_locs = deliverable.rm_locs
    df = df[~df[deliverable.location_col].isin(rm_locs)]

    return df
