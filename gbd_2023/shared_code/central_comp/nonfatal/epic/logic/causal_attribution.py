import os
import pathlib
import re
from typing import Dict, Final, List, Optional

import numpy as np
import pandas as pd
from loguru import logger

import db_queries
from db_queries.api import internal as db_queries_internal
from gbd import constants as gbd_constants
from get_draws.api import internal_get_draws

from epic.legacy.util import constants
from epic.lib.logic import causal_attribution_helpers as ca_helpers
from epic.lib.util import common

_THIS_DIR = pathlib.Path(__file__).resolve().parent

_THRESHOLD_FILE = pathlib.Path("FILEPATH")

_HEMO_PREG_SCALAR: Final[float] = 0.919325
_NON_RESID_PCT: Final[float] = 0.9
_PREG_AGES: Final[List[int]] = list(range(7, 16))
_ASFR_COV: Final[int] = 13
_STILL_COV: Final[int] = 2267
_NTD_OTHER: Final[str] = "ntd_other"
_NTD_NON_OTHER: Final[List[str]] = ["ntd_schisto", "ntd_nema_hook"]
_RESIDUAL_MERGE_PATTERN: Final[re.Pattern] = re.compile("^csp_.|^prevalence$|^anemia_cause$")


def pull_residuals(flatfile_dir: pathlib.Path) -> pd.DataFrame:
    """Reads and returns proportions of remaining anemia to assign to residual causes."""
    return pd.read_csv(str(flatfile_dir / constants.FilePaths.Anemia.RESIDUAL_PROPORTIONS))


def pull_anemia_map(flatefile_dir: pathlib.Path) -> pd.DataFrame:
    """Reads and returns the full Anemia CA map, including shift mappings."""
    return pd.read_csv(str(flatefile_dir / constants.FilePaths.Anemia.CA_MAP))


def get_anemia_cause_output_map_prev(anemia_map: pd.DataFrame) -> pd.DataFrame:
    """Returns relevant anemia_cause/ME output maps for prevalence outputs."""
    anemia_cause_output_prev = anemia_map[
        [
            "anemia_cause",
            "mild_modelable_entity_id",
            "moderate_modelable_entity_id",
            "severe_modelable_entity_id",
            "without_modelable_entity_id",
        ]
    ]
    anemia_cause_output_prev = anemia_cause_output_prev.rename(
        columns={
            "mild_modelable_entity_id": "mild",
            "moderate_modelable_entity_id": "moderate",
            "severe_modelable_entity_id": "severe",
            "without_modelable_entity_id": "without",
        }
    )
    anemia_cause_output_prev = pd.melt(
        anemia_cause_output_prev,
        id_vars=["anemia_cause"],
        value_name=gbd_constants.columns.MODELABLE_ENTITY_ID,
        var_name="severity",
    )
    return anemia_cause_output_prev


def get_anemia_cause_output_map_prop(anemia_map: pd.DataFrame) -> pd.DataFrame:
    """Returns relevant anemia_cause/ME output maps for proportion outputs."""
    anemia_cause_output_prop = anemia_map[
        [
            "anemia_cause",
            "prop_mild_modelable_entity_id",
            "prop_moderate_modelable_entity_id",
            "prop_severe_modelable_entity_id",
            "prop_without_modelable_entity_id",
        ]
    ]
    anemia_cause_output_prop = anemia_cause_output_prop.loc[
        ~anemia_cause_output_prop["prop_mild_modelable_entity_id"].isna()
    ]
    anemia_cause_output_prop = anemia_cause_output_prop.rename(
        columns={
            "prop_mild_modelable_entity_id": "mild",
            "prop_moderate_modelable_entity_id": "moderate",
            "prop_severe_modelable_entity_id": "severe",
            "prop_without_modelable_entity_id": "without",
        }
    )
    anemia_cause_output_prop = pd.melt(
        anemia_cause_output_prop,
        id_vars=["anemia_cause"],
        value_name=gbd_constants.columns.MODELABLE_ENTITY_ID,
        var_name="severity",
    )
    return anemia_cause_output_prop


def pull_hemo_shifts(
    anemia_map: pd.DataFrame,
    output_dir: str,
    location_id: int,
    year_id: int,
    release_id: int,
    demographics: Dict[str, List[int]],
    demographic_cols: List[str],
    n_draws: int,
    residual_anemia_causes: List[int],
) -> pd.DataFrame:
    """Reads and returns hemoglobin shifts by anemia_cause."""
    draw_cols = [f"draw_{i}" for i in range(n_draws)]
    hemo_shifts: List[pd.DataFrame] = []
    for row in anemia_map.itertuples():
        # Drop residual anemia_causes from hemo shifts
        if row.anemia_cause in residual_anemia_causes:
            continue
        hemo_shift = internal_get_draws(
            gbd_id_type="modelable_entity_id",
            gbd_id=row.shift_modelable_entity_id,
            source="epi",
            location_id=location_id,
            year_id=year_id,
            sex_id=demographics[gbd_constants.columns.SEX_ID],
            age_group_id=demographics[gbd_constants.columns.AGE_GROUP_ID],
            release_id=release_id,
            n_draws=n_draws,
            downsample=True,
            diff_cache_dir=os.path.join(output_dir, constants.FilePaths.GET_DRAWS_DIFF_CACHE),
        )
        hemo_shift["anemia_cause"] = row.anemia_cause
        hemo_shift = hemo_shift[["anemia_cause"] + demographic_cols + draw_cols]
        hemo_shifts.append(hemo_shift)
    hemo_shifts: pd.DataFrame = pd.concat(hemo_shifts)
    hemo_shifts = common.make_long(
        df=hemo_shifts, new_col="hemo_shift", id_cols=["anemia_cause"] + demographic_cols
    )
    # Hemo shifts are negative for decreases, but we want them positive for counterfactual
    hemo_shifts["hemo_shift"] = -1 * hemo_shifts["hemo_shift"]
    # Clip any protective shifts to 0 for now
    hemo_shifts["hemo_shift"] = hemo_shifts["hemo_shift"].clip(lower=0)
    return hemo_shifts


def pull_anemia_thresholds() -> pd.DataFrame:
    """Reads and returns anemia thresholds by age, sex, and pregnancy status."""
    thresholds = pd.read_csv(_THRESHOLD_FILE)
    thresholds[
        [
            gbd_constants.columns.AGE_GROUP_ID,
            gbd_constants.columns.SEX_ID,
            "pregnant",
            "hgb_lower_mild",
            "hgb_upper_mild",
            "hgb_lower_moderate",
            "hgb_upper_moderate",
            "hgb_lower_severe",
            "hgb_upper_severe",
        ]
    ]
    return thresholds


def pull_input_prevalence(
    anemia_map: pd.DataFrame,
    output_dir: str,
    location_id: int,
    year_id: int,
    release_id: int,
    demographics: Dict[str, List[int]],
    demographic_cols: List[str],
    n_draws: int,
    quick_run: bool,
) -> pd.DataFrame:
    """Pull input prevalence by anemia_cause."""
    # Select ME IDs
    meid_measures = anemia_map.loc[~anemia_map["input_modelable_entity_id"].isna()][
        ["input_modelable_entity_id", "input_measure_id"]
    ]
    meids_list = [int(me) for me in set(meid_measures["input_modelable_entity_id"])]
    meids_list = meids_list[:3] if quick_run else meids_list

    # Pull input prevalence via get_draws
    draw_cols = [f"draw_{i}" for i in range(n_draws)]
    anemia_cause_prevalence_draws: List[pd.DataFrame] = []
    for meid in meids_list:
        measure_id = list(
            meid_measures[meid_measures["input_modelable_entity_id"] == meid][
                "input_measure_id"
            ]
        )
        anemia_cause = internal_get_draws(
            gbd_id_type="modelable_entity_id",
            gbd_id=meid,
            source="epi",
            location_id=location_id,
            year_id=year_id,
            sex_id=demographics[gbd_constants.columns.SEX_ID],
            age_group_id=demographics[gbd_constants.columns.AGE_GROUP_ID],
            measure_id=measure_id,
            release_id=release_id,
            n_draws=n_draws,
            downsample=True,
            diff_cache_dir=os.path.join(output_dir, constants.FilePaths.GET_DRAWS_DIFF_CACHE),
        )

        for demo in [gbd_constants.columns.SEX_ID, gbd_constants.columns.AGE_GROUP_ID]:
            demographics_to_append = list(set(demographics[demo]) - set(anemia_cause[demo]))
            if demographics_to_append:
                anemia_cause = db_queries_internal.append_demographics(
                    df=anemia_cause,
                    demographic=demo,
                    demographic_values=demographics_to_append,
                    demographic_columns=demographic_cols,
                )
        anemia_cause["anemia_cause"] = anemia_map[
            anemia_map["input_modelable_entity_id"] == meid
        ]["anemia_cause"].iloc[0]
        anemia_cause = anemia_cause[["anemia_cause"] + demographic_cols + draw_cols]
        anemia_cause_prevalence_draws.append(anemia_cause)
    anemia_cause_prevalence_draws: pd.DataFrame = pd.concat(anemia_cause_prevalence_draws)
    anemia_cause_prevalence_draws = common.make_long(
        df=anemia_cause_prevalence_draws,
        new_col="prevalence",
        id_cols=["anemia_cause"] + demographic_cols,
    )
    anemia_cause_prevalence_draws["prevalence"] = anemia_cause_prevalence_draws[
        "prevalence"
    ].fillna(0)

    return anemia_cause_prevalence_draws


def pull_hemo_draws(
    location_id: int,
    year_id: int,
    release_id: int,
    demographics: Dict[str, List[int]],
    demographic_cols: List[str],
    index_cols: List[str],
    output_dir: str,
    n_draws: int,
) -> pd.DataFrame:
    """Read in hemoglobin draws.

    Read in mean hemoglobin and hemoglobin standard-deviation draws and combine them into a
    single dataframe for use in causal attribution. Also add rows for pregnant individuals.
    """
    mean_hemo = internal_get_draws(
        gbd_id_type="modelable_entity_id",
        gbd_id=constants.MEIDS.MEAN_HEMO,
        source="epi",
        location_id=location_id,
        year_id=year_id,
        sex_id=demographics[gbd_constants.columns.SEX_ID],
        age_group_id=demographics[gbd_constants.columns.AGE_GROUP_ID],
        release_id=release_id,
        n_draws=n_draws,
        downsample=True,
        diff_cache_dir=os.path.join(output_dir, constants.FilePaths.GET_DRAWS_DIFF_CACHE),
    )
    mean_hemo = common.make_long(df=mean_hemo, new_col="hemo", id_cols=demographic_cols)

    sd_hemo = internal_get_draws(
        gbd_id_type="modelable_entity_id",
        gbd_id=constants.MEIDS.HEMO_SD,
        source="epi",
        location_id=location_id,
        year_id=year_id,
        sex_id=demographics[gbd_constants.columns.SEX_ID],
        age_group_id=demographics[gbd_constants.columns.AGE_GROUP_ID],
        release_id=release_id,
        n_draws=n_draws,
        downsample=True,
        diff_cache_dir=os.path.join(output_dir, constants.FilePaths.GET_DRAWS_DIFF_CACHE),
    )
    sd_hemo = common.make_long(df=sd_hemo, new_col="hemo_sd", id_cols=demographic_cols)
    sd_hemo["hemo_vr"] = sd_hemo["hemo_sd"] ** 2

    mean_hemo = mean_hemo.merge(sd_hemo, how="left", on=index_cols)
    mean_hemo["pregnant"] = 0

    # Create rows for pregnant women and apply adjustment
    mean_hemo_p = mean_hemo.query(
        f"{gbd_constants.columns.SEX_ID} == 2 "
        f"& {gbd_constants.columns.AGE_GROUP_ID} in {_PREG_AGES}"
    )
    mean_hemo_p["hemo"] = mean_hemo_p["hemo"] * _HEMO_PREG_SCALAR
    mean_hemo_p["pregnant"] = 1
    mean_hemo = pd.concat([mean_hemo, mean_hemo_p])

    return mean_hemo


def generate_pregnancy_prevalence(
    location_id: int, year_id: int, release_id: int, demographic_cols: List[str]
) -> pd.DataFrame:
    """Use ASFR and stillbirth rate to generate pregnancy prevalence."""
    asfr = db_queries.get_covariate_estimates(
        covariate_id=_ASFR_COV,
        location_id=location_id,
        year_id=year_id,
        sex_id=2,
        age_group_id=_PREG_AGES,
        release_id=release_id,
    )[demographic_cols + ["mean_value"]]
    asfr = asfr.rename(columns={"mean_value": "asfr"})

    still = db_queries.get_covariate_estimates(
        covariate_id=_STILL_COV,
        location_id=location_id,
        year_id=year_id,
        release_id=release_id,
    )[[gbd_constants.columns.LOCATION_ID, gbd_constants.columns.YEAR_ID, "mean_value"]]
    still = still.rename(columns={"mean_value": "sbr_mean"})

    df_preg = asfr.merge(
        still,
        how="left",
        on=[gbd_constants.columns.LOCATION_ID, gbd_constants.columns.YEAR_ID],
    )
    df_preg["prev_pregnant"] = (df_preg["asfr"] + (df_preg["sbr_mean"] * df_preg["asfr"])) * (
        46 / 52
    )
    df_preg = df_preg.drop(["asfr", "sbr_mean"], axis=1)
    return df_preg


def generate_residuals(
    df: pd.DataFrame, residuals: pd.DataFrame, anemia_cause_index_cols: List[str]
) -> pd.DataFrame:
    """Generate residual anemia_cause results using remainders and residual proportions."""
    # Merge required columns onto residual proportions
    residual_cols = [col for col in df.columns if not _RESIDUAL_MERGE_PATTERN.match(col)]
    residuals = residuals.merge(
        df[residual_cols].drop_duplicates(),
        how="outer",
        on=[gbd_constants.columns.AGE_GROUP_ID, gbd_constants.columns.SEX_ID],
    )
    # Assign residuals using residual proportions.
    residuals["csp_mild"] = residuals["resid_prop"] * residuals["remaining_mild"]
    residuals["csp_moderate"] = residuals["resid_prop"] * residuals["remaining_moderate"]
    residuals["csp_severe"] = residuals["resid_prop"] * residuals["remaining_severe"]
    residuals["prevalence"] = np.nan
    residuals = residuals[list(df.columns) + ["resid_prop"]].set_index(
        anemia_cause_index_cols
    )
    return residuals


def main(
    location_id: int,
    year_id: int,
    release_id: int,
    n_draws: int,
    output_dir: str,
    quick_run: bool = False,
    in_mem: bool = False,
) -> Optional[pd.DataFrame]:
    """Run anemia causal attribution.

    Args:
        location_id (int): Location ID to run causal attribution for.
        year_id (int): Year ID to run causal attribution for.
        release_id (int): Release ID to run causal attribution for.
        n_draws (int): Number of draws to read in/generate.
        output_dir (str): Directory to save causal attribution outputs to. Outputs are saved
            by modelable entity, year, and location.
        quick_run (bool): Whether or not to run a smaller run, only generating non-residual
            results for the first three modelable entities in the anemia map CSV. Default
            False, full run.
        in_mem (bool): Whether or not to return a dataframe instead of writing results to
            disk. Default False, write results to disk.
    """
    demo = common.get_demographics(path=output_dir)

    # Pull anemia flatfile inputs
    flatfile_dir = _THIS_DIR.parent / "maps" / "anemia"
    anemia_map = pull_anemia_map(flatefile_dir=flatfile_dir)
    residuals = pull_residuals(flatfile_dir=flatfile_dir)
    anemia_cause_output_prev = get_anemia_cause_output_map_prev(anemia_map=anemia_map)
    anemia_cause_output_prop = get_anemia_cause_output_map_prop(anemia_map=anemia_map)

    # Define demographic columns and index columns with and without anemia_cause
    demo_cols = [
        gbd_constants.columns.LOCATION_ID,
        gbd_constants.columns.YEAR_ID,
        gbd_constants.columns.AGE_GROUP_ID,
        gbd_constants.columns.SEX_ID,
    ]
    index_cols = demo_cols + ["draw"]
    anemia_cause_index_cols = index_cols + ["anemia_cause"]

    # Pull hemo shift draws by anemia cause
    hemo_shifts = pull_hemo_shifts(
        anemia_map=anemia_map,
        output_dir=output_dir,
        location_id=location_id,
        year_id=year_id,
        release_id=release_id,
        demographics=demo,
        demographic_cols=demo_cols,
        n_draws=n_draws,
        residual_anemia_causes=list(residuals["anemia_cause"].unique()),
    )

    # Pull input prevalence draws by anemia_cause
    anemia_causes = pull_input_prevalence(
        anemia_map=anemia_map,
        output_dir=output_dir,
        location_id=location_id,
        year_id=year_id,
        release_id=release_id,
        demographics=demo,
        demographic_cols=demo_cols,
        n_draws=n_draws,
        quick_run=quick_run,
    )

    # Combine data and calculate cause-specific hgb shift
    anemia_causes = anemia_causes.merge(hemo_shifts, how="left", on=anemia_cause_index_cols)

    # Pull and combine mean and SD hemo draws
    hemo = pull_hemo_draws(
        location_id=location_id,
        year_id=year_id,
        release_id=release_id,
        demographics=demo,
        demographic_cols=demo_cols,
        index_cols=index_cols,
        output_dir=output_dir,
        n_draws=n_draws,
    )

    # Read in thresholds and define threshold columns for later null checks
    thresholds = pull_anemia_thresholds()
    threshold_columns = [col for col in set(thresholds.columns).difference(set(hemo.columns))]
    # Merge on anemia thresholds by age/sex/pregnancy status
    hemo = hemo.merge(
        thresholds,
        how="left",
        on=[gbd_constants.columns.AGE_GROUP_ID, gbd_constants.columns.SEX_ID, "pregnant"],
    )

    # Stop if nulls introduced after merging threshold values
    if hemo[threshold_columns].isna().any().any():
        example_row = hemo[hemo[threshold_columns].isna().any()].iloc[0]
        raise RuntimeError(
            f"Nulls introduced in threshold merging, example row: {example_row}."
        )

    # Calculate anemia envelope prevalence by severity
    for sev in ["mild", "moderate", "severe"]:
        hemo[f"prev_{sev}"] = ca_helpers.ensemble_prevalence(
            df=hemo, q_col=f"hgb_upper_{sev}", mean_cols=["hemo"], variance_col="hemo_vr"
        ) - ca_helpers.ensemble_prevalence(
            df=hemo, q_col=f"hgb_lower_{sev}", mean_cols=["hemo"], variance_col="hemo_vr"
        )
    hemo["prev_modsev"] = ca_helpers.ensemble_prevalence(
        df=hemo, q_col="hgb_upper_moderate", mean_cols=["hemo"], variance_col="hemo_vr"
    ) - ca_helpers.ensemble_prevalence(
        df=hemo, q_col="hgb_lower_severe", mean_cols=["hemo"], variance_col="hemo_vr"
    )
    hemo["prev_total"] = ca_helpers.ensemble_prevalence(
        df=hemo, q_col="hgb_upper_mild", mean_cols=["hemo"], variance_col="hemo_vr"
    )

    # Merge input prevalence by anemia_cause on to hemo draws
    df = hemo.merge(anemia_causes, how="outer", on=index_cols)

    # Calculate prevalence shift
    df["prev_weighted_hemo_shift"] = df["prevalence"] * df["hemo_shift"]

    # Calculate counterfactual distribution prevalences
    df["cf_severe"] = ca_helpers.ensemble_prevalence(
        df=df,
        q_col="hgb_upper_severe",
        mean_cols=["hemo", "prev_weighted_hemo_shift"],
        variance_col="hemo_vr",
    ) - ca_helpers.ensemble_prevalence(
        df=df,
        q_col="hgb_lower_severe",
        mean_cols=["hemo", "prev_weighted_hemo_shift"],
        variance_col="hemo_vr",
    )
    df["cf_modsev"] = ca_helpers.ensemble_prevalence(
        df=df,
        q_col="hgb_upper_moderate",
        mean_cols=["hemo", "prev_weighted_hemo_shift"],
        variance_col="hemo_vr",
    ) - ca_helpers.ensemble_prevalence(
        df=df,
        q_col="hgb_lower_severe",
        mean_cols=["hemo", "prev_weighted_hemo_shift"],
        variance_col="hemo_vr",
    )
    df["cf_total"] = ca_helpers.ensemble_prevalence(
        df=df,
        q_col="hgb_upper_mild",
        mean_cols=["hemo", "prev_weighted_hemo_shift"],
        variance_col="hemo_vr",
    )

    # Drop shift-related columns
    df = df.drop(["hemo_shift", "prev_weighted_hemo_shift"], axis=1)

    # Check that shifted prevalence isn't greater than true prevalence
    for sev in ["severe", "modsev", "total"]:
        if (df[f"cf_{sev}"] > df[f"prev_{sev}"]).any():
            logger.warning(
                f"Shifted {sev} prevalence is greater than true {sev} prevalence, scaling "
                "down."
            )
            df.loc[df[f"cf_{sev}"] > df[f"prev_{sev}"], f"cf_{sev}"] = df[f"prev_{sev}"]
        df[f"csp_{sev}"] = df[f"prev_{sev}"] - df[f"cf_{sev}"]

    # Generate pregnancy prevalence
    df_preg = generate_pregnancy_prevalence(
        location_id=location_id,
        year_id=year_id,
        release_id=release_id,
        demographic_cols=demo_cols,
    )

    # Keep track of data columns relevant for pregnancy merge
    sev_cols = [
        "prev_mild",
        "prev_moderate",
        "prev_severe",
        "prev_modsev",
        "prev_total",
        "csp_severe",
        "csp_modsev",
        "csp_total",
    ]

    # Generate and merge pregnant dataset onto non-pregnant
    df_preg = df.loc[df["pregnant"] == 1].merge(df_preg, how="left", on=demo_cols)[
        demo_cols + ["anemia_cause", "draw"] + sev_cols + ["prev_pregnant"]
    ]
    df_preg = df_preg.rename(columns={col: f"{col}_preg" for col in sev_cols})
    df = df.loc[df["pregnant"] == 0].merge(df_preg, how="outer", on=anemia_cause_index_cols)

    # Take a weighted sum
    for col in sev_cols:
        df.loc[~df["prev_pregnant"].isna(), col] = (
            df[col] * (1 - df["prev_pregnant"]) + df[f"{col}_preg"] * df["prev_pregnant"]
        )
        df = df.drop(f"{col}_preg", axis=1)
    df = df.drop(["pregnant", "prev_pregnant"], axis=1)

    # Calculate mild and moderate cause-specific prevalence
    df["csp_moderate"] = df["csp_modsev"] - df["csp_severe"]
    df["csp_mild"] = df["csp_total"] - df["csp_modsev"]

    # Adjust where attributable sev > modsev or modsev > total
    if (df["csp_moderate"] < 0).any() or (df["csp_mild"] < 0).any():
        logger.warning("Mild and/or moderate CSP is less than 0.")
        df.loc[df["csp_moderate"] < 0, "csp_moderate"] = 0
        df.loc[df["csp_mild"] < 0, "csp_mild"] = 0

    # Write out intermediate files for diagnostics
    if not in_mem:
        logger.info("Printing intermediate diagnostic file.")
        df_diag = df.copy(deep=True).set_index(anemia_cause_index_cols)
        df_diag = (
            df_diag[["csp_mild", "csp_moderate", "csp_severe", "csp_total"]]
            .groupby(
                [
                    gbd_constants.columns.AGE_GROUP_ID,
                    gbd_constants.columns.SEX_ID,
                    "anemia_cause",
                ]
            )
            .mean()
        )
        df_diag = df_diag.reset_index()
        df_diag["sub_total"] = (
            df_diag["csp_mild"] + df_diag["csp_moderate"] + df_diag["csp_severe"]
        )
        df_diag[gbd_constants.columns.LOCATION_ID] = location_id
        df_diag[gbd_constants.columns.YEAR_ID] = year_id
        diag_dir = os.path.join(output_dir, constants.FilePaths.Anemia.DIAGNOSTICS)
        df_diag.to_csv(os.path.join(diag_dir, f"{location_id}_{year_id}_pre_squeeze.csv"))
        logger.info("Printed intermediate diagnostic file.")
        del df_diag

    # Set index for easier groupby operations down the line
    df = df.set_index(anemia_cause_index_cols)

    # Don't track super tiny numbers
    round_pattern = re.compile("^sub_.|^csp_.|^prev_.|^cf_.|^prevalence$")
    round_cols = [col for col in df.columns if round_pattern.match(col)]
    df[round_cols] = df[round_cols].round(decimals=40)

    df[["mild_sum", "moderate_sum", "severe_sum", "total_sum"]] = (
        df[["csp_mild", "csp_moderate", "csp_severe", "csp_total"]].groupby(index_cols).sum()
    )

    # Squeeze severe and recalculate sum
    # Scale sum of severe to always be <= envelope
    df["csp_sqz_severe"] = np.where(
        df["severe_sum"] <= df["prev_severe"],
        df["csp_severe"],
        df["csp_severe"] * (df["prev_severe"] / df["severe_sum"]),
    )
    df["csp_sqz_severe_sum"] = df[["csp_sqz_severe"]].groupby(index_cols).sum()
    # Small number issue in a few rows
    df.loc[df["csp_sqz_severe_sum"] > df["prev_severe"], "csp_sqz_severe_sum"] = df[
        "prev_severe"
    ]

    # Add squeezed severe cases into moderate bin and re-sum
    # Where we squeezed sev, add cases to moderate bin
    df["csp_modprime"] = np.where(
        df["severe_sum"] <= df["prev_severe"],
        df["csp_moderate"],
        df["csp_moderate"] + (df["csp_severe"] - df["csp_sqz_severe"]),
    )
    # Re-sum moderate cases
    df["modprime_sum"] = df[["csp_modprime"]].groupby(index_cols).sum()

    # Squeeze moderate and recalculate sum
    # Scale sum of moderate to always be <= envelope
    df["csp_sqz_moderate"] = np.where(
        df["modprime_sum"] <= df["prev_moderate"],
        df["csp_modprime"],
        df["csp_modprime"] * (df["prev_moderate"] / df["modprime_sum"]),
    )
    # Re-sum moderate cases
    df["csp_sqz_moderate_sum"] = df[["csp_sqz_moderate"]].groupby(index_cols).sum()
    # Adjust for very small number differences
    df.loc[df["csp_sqz_moderate_sum"] > df["prev_moderate"], "csp_sqz_moderate_sum"] = df[
        "prev_moderate"
    ]

    # Add squeezed moderate cases in mild bin and re-sum
    # Where we squeezed mod, add cases to mild bin
    df["csp_mildprime"] = np.where(
        df["modprime_sum"] <= df["prev_moderate"],
        df["csp_mild"],
        df["csp_mild"] + (df["csp_modprime"] - df["csp_sqz_moderate"]),
    )
    # Re-sum mild cases
    df["mildprime_sum"] = df[["csp_mildprime"]].groupby(index_cols).sum()

    # Calculate total and squeeze to residual
    # Calculate interim total sum
    df["csp_totalprime"] = df["csp_sqz_severe"] + df["csp_sqz_moderate"] + df["csp_mildprime"]
    # Sum interim total cases
    df["totalprime_sum"] = df[["csp_totalprime"]].groupby(index_cols).sum()
    # Scale sum of total cases to always be <= envelope
    df["csp_sqz_total"] = np.where(
        df["totalprime_sum"] <= (_NON_RESID_PCT * df["prev_total"]),
        df["csp_totalprime"],
        df["csp_totalprime"] * (_NON_RESID_PCT * df["prev_total"] / df["totalprime_sum"]),
    )
    # Calculate cause-specific mild and re-sum
    # Subtract mod+sev from total to get mild
    df["csp_sqz_mild"] = df["csp_sqz_total"] - (df["csp_sqz_moderate"] + df["csp_sqz_severe"])
    # Small numbers issue again
    df.loc[df["csp_sqz_mild"] < 0, "csp_sqz_mild"] = 0
    df["csp_sqz_mild_sum"] = df[["csp_sqz_mild"]].groupby(index_cols).sum()

    # Calculate residuals
    df["remaining_mild"] = df["prev_mild"] - df["csp_sqz_mild_sum"]
    df["remaining_moderate"] = df["prev_moderate"] - df["csp_sqz_moderate_sum"]
    df["remaining_severe"] = df["prev_severe"] - df["csp_sqz_severe_sum"]

    # Replace with squeezed values
    df[["csp_mild", "csp_moderate", "csp_severe"]] = df[
        ["csp_sqz_mild", "csp_sqz_moderate", "csp_sqz_severe"]
    ]

    # Reformat columns for residual assignment
    residual_cols = [
        "prev_mild",
        "prev_moderate",
        "prev_severe",
        "csp_mild",
        "csp_moderate",
        "csp_severe",
        "remaining_mild",
        "remaining_moderate",
        "remaining_severe",
        "prevalence",
    ]
    df = df[residual_cols].reset_index()

    # Generate residuals
    residuals = generate_residuals(
        df=df, residuals=residuals, anemia_cause_index_cols=anemia_cause_index_cols
    )

    # Append residual anemia causes onto other anemia causes
    df = pd.concat([df.set_index(anemia_cause_index_cols), residuals])

    # Zero out the ntd_other proportion if schisto and hookworm are zero
    df["check_col"] = np.nan
    df.loc[df.index.get_level_values("anemia_cause").isin(_NTD_NON_OTHER), "check_col"] = (
        df[df.index.get_level_values("anemia_cause").isin(_NTD_NON_OTHER)][["prevalence"]]
        .groupby(index_cols)
        .sum()
    )
    df.loc[
        (df.index.get_level_values("anemia_cause") == _NTD_OTHER) & (df["check_col"] == 0),
        ["csp_mild", "csp_moderate", "csp_severe"],
    ] = 0
    df = df.drop("check_col", axis=1)

    # Rescale if cause-specific anemia is greater than cause prevalence
    df["sub_anemic"] = df["csp_mild"] + df["csp_moderate"] + df["csp_severe"]

    # Adjust cause-specific anemia prevalence if greater than cause prevalence
    df.loc[df["sub_anemic"] > df["prevalence"], "csp_mild"] = (
        df["csp_mild"] / df["sub_anemic"] * df["prevalence"]
    )
    df.loc[df["sub_anemic"] > df["prevalence"], "csp_moderate"] = (
        df["csp_moderate"] / df["sub_anemic"] * df["prevalence"]
    )
    df.loc[df["sub_anemic"] > df["prevalence"], "csp_severe"] = (
        df["csp_severe"] / df["sub_anemic"] * df["prevalence"]
    )
    df["sub_anemic"] = df["csp_mild"] + df["csp_moderate"] + df["csp_severe"]

    # Then re-assign residuals based on re-scaled results
    for sev in ["mild", "moderate", "severe"]:
        # Squeeze/expand residuals so that severities sum
        df.loc[df["resid_prop"].isna(), "target_resid_sum"] = df[f"prev_{sev}"] - (
            df[df["resid_prop"].isna()][[f"csp_{sev}"]]
            .groupby(index_cols)
            .sum()[f"csp_{sev}"]
        )
        # Sometimes target_resid_sum is negative due to small numbers (i.e. 10^-18)
        df.loc[df["target_resid_sum"] < 0, "target_resid_sum"] = 0
        df["target_resid_sum"] = df[["target_resid_sum"]].groupby(index_cols).mean()
        df.loc[~df["resid_prop"].isna(), f"csp_{sev}"] = (
            df["resid_prop"] * df["target_resid_sum"]
        )
        df = df.drop("target_resid_sum", axis=1)

    # Calculate cause-specific anemia proportions
    df["csp_without"] = (
        df["prevalence"] - df["csp_mild"] - df["csp_moderate"] - df["csp_severe"]
    )
    # Small numbers problem where some draws negative 10^-17 or less where should be 0
    df.loc[df["csp_without"] < 0, "csp_without"] = 0
    for sev in ["mild", "moderate", "severe", "without"]:
        df.loc[df["prevalence"] != 0, f"prop_{sev}"] = df[f"csp_{sev}"] / df["prevalence"]
        df.loc[df["prevalence"] == 0, f"prop_{sev}"] = 0

    # Format prevalence dataframe
    df_prev = df[["csp_mild", "csp_moderate", "csp_severe", "csp_without"]].reset_index()
    df_prev = df_prev.melt(
        id_vars=anemia_cause_index_cols, value_name="mean", var_name="severity"
    )
    df_prev["severity"] = df_prev["severity"].str.replace("csp_", "")
    df_prev = df_prev.merge(
        anemia_cause_output_prev, how="left", on=["anemia_cause", "severity"]
    )
    df_prev = df_prev[
        [gbd_constants.columns.MODELABLE_ENTITY_ID] + index_cols + ["mean", "anemia_cause"]
    ]
    df_prev = common.make_wide(
        df=df_prev,
        value_col="mean",
        id_cols=[gbd_constants.columns.MODELABLE_ENTITY_ID, "anemia_cause"] + demo_cols,
    )
    df_prev[gbd_constants.columns.MEASURE_ID] = gbd_constants.measures.PREVALENCE
    df_prev[gbd_constants.columns.METRIC_ID] = gbd_constants.metrics.RATE
    df_prev[gbd_constants.columns.MODELABLE_ENTITY_ID] = df_prev[
        gbd_constants.columns.MODELABLE_ENTITY_ID
    ].astype(int)
    df_inc = df_prev.query("anemia_cause != 'maternal_hem'")
    df_inc[gbd_constants.columns.MEASURE_ID] = gbd_constants.measures.INCIDENCE
    df_inc[df_inc.filter(regex=r"^draw_[\d]+$").columns] = 0
    df_prev = df_prev.drop(columns=["anemia_cause"])
    df_inc = df_inc.drop(columns=["anemia_cause"])

    # Format proportion dataframe
    df_prop = df[["prop_mild", "prop_moderate", "prop_severe", "prop_without"]].reset_index()
    df_prop = df_prop.melt(
        id_vars=anemia_cause_index_cols, value_name="mean", var_name="severity"
    )
    df_prop["severity"] = df_prop["severity"].str.replace("prop_", "")
    df_prop = df_prop.merge(
        anemia_cause_output_prop, how="left", on=["anemia_cause", "severity"]
    )
    df_prop[gbd_constants.columns.MEASURE_ID] = gbd_constants.measures.PREVALENCE
    df_prop.loc[
        df_prop["anemia_cause"] == "maternal_hem", gbd_constants.columns.MEASURE_ID
    ] = 18
    df_inc_prop = df_prop.query("anemia_cause != 'maternal_hem'")
    df_inc_prop[gbd_constants.columns.MEASURE_ID] = gbd_constants.measures.INCIDENCE
    df_inc_prop["mean"] = 0
    df_inc_prop.loc[df_inc_prop["severity"] == "without", "mean"] = 1
    df_prop = pd.concat([df_prop, df_inc_prop])
    df_prop = df_prop[
        [gbd_constants.columns.MODELABLE_ENTITY_ID]
        + index_cols
        + [gbd_constants.columns.MEASURE_ID, "mean"]
    ]
    df_prop = common.make_wide(
        df=df_prop,
        value_col="mean",
        id_cols=[gbd_constants.columns.MODELABLE_ENTITY_ID, gbd_constants.columns.MEASURE_ID]
        + demo_cols,
    )
    df_prop[gbd_constants.columns.MODELABLE_ENTITY_ID] = df_prop[
        gbd_constants.columns.MODELABLE_ENTITY_ID
    ].astype(int)
    df_prop[gbd_constants.columns.METRIC_ID] = gbd_constants.metrics.RATE

    # Combine prevalence and poportion dataframes
    df = pd.concat([df_prev, df_inc, df_prop])
    if df.duplicated(
        subset=demo_cols
        + [gbd_constants.columns.MEASURE_ID, gbd_constants.columns.MODELABLE_ENTITY_ID]
    ).any():
        raise ValueError("Duplicate rows found in the final df.")

    # Return prev and prop DFs
    if in_mem:
        return df
    # Write out files for save_results
    else:
        for me in set(df[gbd_constants.columns.MODELABLE_ENTITY_ID]):
            me_dir = os.path.join(output_dir, str(me), str(year_id))
            os.makedirs(str(me_dir), exist_ok=True)
            df[df[gbd_constants.columns.MODELABLE_ENTITY_ID] == me].to_csv(
                os.path.join(me_dir, f"{location_id}.csv"), index=False
            )
