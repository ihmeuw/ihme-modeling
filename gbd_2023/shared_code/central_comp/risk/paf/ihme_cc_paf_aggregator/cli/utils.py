import os
import warnings
from pathlib import Path
from typing import List, Optional

import click
import pandas as pd

import db_tools_core
from db_queries.lib.model_version_utils import get_best_releases
from db_queries.lib.wormhole_utils import get_linked_wormhole_mvid
from gbd import conn_defs, estimation_years
from gbd import wormhole as gbd_wormhole

from ihme_cc_paf_aggregator.lib import constants, io, logging_utils

logger = logging_utils.module_logger(__name__)


def read_access_path(
    ctx: click.Context, param: click.Parameter, value: Optional[str]
) -> Optional[Path]:
    """Parse a command line string to Path. Allow None to pass."""
    if value is None:
        return value
    value_path = Path(value).resolve()
    if not value_path.exists():
        raise ValueError(f"path {value_path} does not exist.")
    if not os.access(value_path, os.R_OK):
        raise ValueError(f"path {value_path} is not readable.")
    return value_path


def write_access_dir(ctx: click.Context, param: click.Parameter, value: str) -> Path:
    """Parse a command line string to Path, creating it if it doesn't exist."""
    value_path = Path(value).resolve()
    if not value_path.exists():
        value_path.mkdir(parents=True)
        logger.info(f"Created directory {value_path}")
    logger.info(f"{param.human_readable_name} set to {value_path}")
    return value_path


def validate_location_test(location_id: Optional[List[int]], test: bool) -> None:
    """Check that location_id spec is only used in test mode."""
    if (location_id is not None) and (not test):
        raise ValueError(
            "Specifying specific location IDs is only supported when setting --test. "
            "This limitation is to prevent attempted runs of the burdenator on PAF "
            "Aggregator results that do not have all the most-detailed locations for "
            "the specified release and location set."
        )


def validate_years(year_id: List[int], years_from_release: List[int]) -> None:
    """Check user-specified years are in years from release."""
    input_set = set(year_id)
    release_set = set(years_from_release)
    if not release_set.issuperset(input_set):
        raise ValueError(
            f"Years requested contained years {input_set - release_set} that "
            f"do not exist from get_demographics."
        )


def validate_hierarchy_rei_set_version(
    hierarchy_path: Optional[Path],
    rei_set_id: Optional[int],
    rei_set_version_id: Optional[int],
) -> None:
    """The arguments hierarchy_path and rei_set_id/version_id are mutually exclusve."""
    if (hierarchy_path is not None) and (rei_set_id is not None):
        raise ValueError("Please specify one and only one of hierarchy_path and rei_set_id.")
    if (rei_set_version_id is not None) and (rei_set_id is None):
        raise ValueError(
            "Specifying rei_set_version_id is only supported when also specifying rei_set_id, "
            "not hierarchy_path."
        )


def warn_on_agg_with_reis_specified(
    input_models_path: Optional[Path],
    rei_id: Optional[List[int]],
    input_rei_set_id: Optional[List[int]],
    skip_aggregation: bool,
) -> None:
    """Warn the user if aggregating while using a custom REI list. If the user is selecting
    custom REIs using the --input_models_path, --rei_id, or --rei_set_id arguments, we don't
    guarantee that the selected REIs correspond to the aggregation hierarchy, or that all
    most-detailed REIs required for aggregation are present.

    This is also caught during aggregation, and jobs will fail if required most-detailed
    REIs are not included in the run.
    """
    if (
        input_models_path is not None or rei_id is not None or input_rei_set_id is not None
    ) and (not skip_aggregation):
        warnings.warn(
            "Aggregation was requested with user-specified REIs. This machinery does not "
            "ensure that all most-detailed child REIs are present before aggregation. If "
            "jobs fail, you may need to revise your input REIs or set --skip_aggregation "
            "to True."
        )


def validate_exclusive_risk_inputs(
    input_models_path: Optional[Path],
    rei_id: Optional[List[int]],
    input_rei_set_id: Optional[List[int]],
) -> None:
    """Input models path, REI list, and REI set list all determine which input models to
    include, and are mutually exclusive.
    """
    if (
        sum(
            [
                (input_models_path is not None),
                (rei_id is not None),
                (input_rei_set_id is not None),
            ]
        )
        > 1
    ):
        raise ValueError(
            "If specifying input models, please provide only one of input_models_path,"
            " rei_id, or input_rei_set_id."
        )


def validate_location_id(
    location_set_id: int,
    release_id: int,
    user_location_id: Optional[List[int]],
    release_locations: List[int],
) -> None:
    """Check locations specified are a subset of the release locations."""
    extra_locs = set(user_location_id) - set(release_locations)
    if len(extra_locs) != 0:
        raise ValueError(
            f"The input locations {user_location_id} had values {extra_locs} that are not in "
            f"the most-detailed locations for location set ID {location_set_id} and release "
            f"ID {release_id}."
        )


def increment_latest_version() -> int:
    """Determine the latest PAF compile version and return one more than that."""
    current_highest_version = max(
        int(folder.name) for folder in constants.PAF_ROOT.iterdir() if folder.name.isdigit()
    )
    return current_highest_version + 1


def validate_and_format_compile_inputs(
    raw_inputs_df: pd.DataFrame, n_draws: int, year_id: List[int], release_id: int
) -> pd.DataFrame:
    """Validation and format for the input model specification.

    A PAF compile input versions csv is expected to meet the following requirements:
        * have columns rei_id/model_version_id/draw_type
        * Each PAF model version has at least the number of draws and years requested

    Arguments:
        raw_inputs_df: unformatted dataframe of a PAF_input_versions csv
        n_draws: minimum expected number of draws
        year_id: minimum expexted set of modeled years

    Returns:
        A dataframe of model version inputs, the model n_draws and year_id are included.
    """
    minimum_expected_columns = {
        constants.REI_ID,
        constants.MODEL_VERSION_ID,
        constants.DRAW_TYPE,
    }
    if not minimum_expected_columns.issubset(set(raw_inputs_df.columns)):
        raise RuntimeError(
            f"Did not find all of {minimum_expected_columns} in {raw_inputs_df.columns}"
        )

    raw_inputs_df = _append_and_validate_wormhole_mvids(raw_inputs_df, release_id)

    # Read number of draws and years for each PAF model
    raw_inputs_df["n_draws"], raw_inputs_df[constants.YEAR_ID] = zip(
        *raw_inputs_df[constants.MODEL_VERSION_ID].map(io.read_paf_model_n_draws_and_years)
    )
    # We assume that all years are available if we have a Wormhole model
    est_years = estimation_years.estimation_years_from_release_id(release_id)
    annual_years = list(range(min(est_years), max(est_years) + 1))
    raw_inputs_df.loc[raw_inputs_df["wormhole_mvid"].notnull(), constants.YEAR_ID] = (
        pd.Series([annual_years] * len(raw_inputs_df))
    )

    lacking_draws = raw_inputs_df.query("n_draws < @n_draws")
    if not lacking_draws.empty:
        raise RuntimeError(
            f"{len(lacking_draws)} best PAF models have less draws than the requested n_draws"
            f", {n_draws}:\n{lacking_draws}"
        )

    MISSING_YEAR_COL = "missing_year_ids"
    # Check for missing years, maintain which are missing for error message
    raw_inputs_df[MISSING_YEAR_COL] = raw_inputs_df["year_id"].apply(
        lambda model_years: set(year_id) - set(model_years)
    )
    lacking_years_mask = (
        raw_inputs_df[MISSING_YEAR_COL].apply(lambda missing_year_ids: len(missing_year_ids))
        > 0
    )
    lacking_years = raw_inputs_df[lacking_years_mask]
    if not lacking_years.empty:
        raise RuntimeError(
            f"{len(lacking_years)} best PAF models have less years than the requested years:"
            f"\n{lacking_years}"
        )
    raw_inputs_df = raw_inputs_df.drop(columns=MISSING_YEAR_COL)

    return raw_inputs_df


def _append_and_validate_wormhole_mvids(
    raw_inputs_df: pd.DataFrame, release_id: int
) -> pd.DataFrame:
    """Append columns for best_release_id and wormhole_mvid to the input models dataframe.
    We use these columns for validation and to include in the human-readable PAF inputs
    report.

    Confirm that any PAF models from prior releases have a linked Wormhole model.
    """
    # Add the best release for all input PAF models
    raw_inputs_df = raw_inputs_df.assign(
        best_release_id=get_best_releases(
            entity="rei",
            id_list=raw_inputs_df[constants.REI_ID],
            release_id=release_id,
            draw_types=raw_inputs_df[constants.DRAW_TYPE],
        )
    )

    # Add the Wormhole mvid where available
    with db_tools_core.session_scope(conn_def=conn_defs.WORMHOLE) as wh_session:
        raw_inputs_df = raw_inputs_df.assign(
            wormhole_mvid=raw_inputs_df[constants.MODEL_VERSION_ID].apply(
                get_linked_wormhole_mvid,
                base_model_type_id=[
                    gbd_wormhole.BASE_MODEL_TYPES.PAF,
                    gbd_wormhole.BASE_MODEL_TYPES.PAF_UNMEDIATED,
                ],
                release_id=release_id,
                summaries=False,
                session=wh_session,
            )
        )
    raw_inputs_df["wormhole_mvid"] = raw_inputs_df["wormhole_mvid"].astype("Int64")

    # Confirm that all prior-release PAFs have a linked Wormhole model
    pafs_missing_wormhole = raw_inputs_df.query(
        f"best_release_id != {release_id} & wormhole_mvid.isnull()"
    )
    if not pafs_missing_wormhole.empty:
        raise RuntimeError(
            f"{len(pafs_missing_wormhole)} PAF models are out of rotation but missing "
            f"Wormhole models for release {release_id}:\n"
            f"\n{pafs_missing_wormhole[[constants.REI_ID, constants.MODEL_VERSION_ID]]}"
        )

    return raw_inputs_df
