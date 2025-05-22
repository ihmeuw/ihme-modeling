"""Custom PAFs for lead exposure in blood."""

import pathlib
from typing import Dict, List

import numpy as np
import pandas as pd
import scipy

from gbd.constants import measures, metrics
from get_draws import api as get_draws_api

from ihme_cc_paf_calculator.lib import constants, io_utils

# Idiopathic developmental intellectual disability sequela IDs
_BORDERLINE_ID_SEQUELA_ID: int = 487
_MILD_ID_SEQUELA_ID: int = 488
_MODERATE_ID_SEQUELA_ID: int = 489
_SEVERE_ID_SEQUELA_ID: int = 490
_PROFOUND_ID_SEQUELA_ID: int = 491

# All intellectual disability sequela IDs
ID_SEQUELA_IDS: List[int] = [
    _BORDERLINE_ID_SEQUELA_ID,
    _MILD_ID_SEQUELA_ID,
    _MODERATE_ID_SEQUELA_ID,
    _SEVERE_ID_SEQUELA_ID,
    _PROFOUND_ID_SEQUELA_ID,
]

# IQ cutoffs for each severity level
ID_SEQUELA_TO_IQ_CUTOFF: Dict[int, int] = {
    _BORDERLINE_ID_SEQUELA_ID: 85,
    _MILD_ID_SEQUELA_ID: 70,
    _MODERATE_ID_SEQUELA_ID: 50,
    _SEVERE_ID_SEQUELA_ID: 35,
    _PROFOUND_ID_SEQUELA_ID: 20,
}

# Idiopathic developmental intellectual disability
_INTELLECTUAL_DISABILITY_CAUSE_ID: int = 582

_INDEX_COLS: List[str] = ["location_id", "year_id", "age_group_id", "sex_id"]


def calculate_envir_lead_blood(
    root_dir: pathlib.Path, location_id: int, settings: constants.PafCalculatorSettings
) -> None:
    """Calculates custom PAFs for lead exposure in blood, envir_lead_blood, REI ID 242.

    Blood lead PAFs are estimated to measure the non-fatal burden of intellectual disability
    attributable to lead exposure in the blood. No relative risk model exists. Instead, IQ
    shifts modeled as exposure and an estimated distribution of intellectual disability are
    used to compute the PAFs.

    Intended only for use within the PAF Calculator.
    """
    draw_cols = [f"draw_{i}" for i in range(settings.n_draws)]

    if settings.rei_id != constants.BLOOD_LEAD_REI_ID:
        raise RuntimeError(
            "Internal error: function can only be run for blood lead exposure, REI ID "
            f"{constants.BLOOD_LEAD_REI_ID}. Got rei_id {settings.rei_id}"
        )

    # IQ shifts modeler uploads as exposure
    iq_shifts = io_utils.get_location_specific(
        root_dir, constants.LocationCacheContents.EXPOSURE, location_id=location_id
    ).drop(columns="parameter")

    # Pull idiopathic developmental intellectual disability severity sequela YLDs
    severity = get_draws_api.get_draws(
        source="como",
        gbd_id_type="sequela_id",
        gbd_id=ID_SEQUELA_IDS,
        location_id=location_id,
        year_id=settings.year_id,
        measure_id=measures.YLD,
        metric_id=metrics.RATE,
        release_id=settings.release_id,
        n_draws=settings.n_draws,
        downsample=True,
        version_id=settings.como_version_id,
    )[_INDEX_COLS + ["sequela_id"] + draw_cols]

    # Melt long by draw
    iq_shifts_long = iq_shifts.melt(
        id_vars=_INDEX_COLS, value_vars=draw_cols, var_name="draw", value_name="iq_shift"
    )
    severity_long = severity.melt(
        id_vars=_INDEX_COLS + ["sequela_id"],
        value_vars=draw_cols,
        var_name="draw",
        value_name="yld",
    )

    paf = pd.merge(iq_shifts_long, severity_long, on=_INDEX_COLS + ["draw"])

    # Intellectual disability sequelas are in order from borderline -> profound, so sort by
    # sequela_id. In descending order, so that cumulative YLD sum represents profound,
    # profound or severe, profound/severe/moderate, etc. In other words, cumulative YLDs
    # will represent 'probablility' of intellectual severity at the given level or below.
    paf = paf.sort_values(by="sequela_id", ascending=False)

    # Take cumulative sum to find cumulative severities and overall sum
    paf["yld_cumsum"] = paf.groupby(_INDEX_COLS + ["draw"])["yld"].cumsum()
    yld_sum = (
        paf.groupby(_INDEX_COLS + ["draw"])["yld"]
        .sum()
        .reset_index()
        .rename(columns={"yld": "yld_sum"})
    )
    paf = pd.merge(paf, yld_sum, on=_INDEX_COLS + ["draw"])

    # Set IQ cutoff for each severity level
    paf["iq_cutoff"] = paf["sequela_id"].apply(lambda seq_id: ID_SEQUELA_TO_IQ_CUTOFF[seq_id])

    # Assume mean of human intelligence as measured by IQ is 100, as indicated by
    # the expert group in Comparative Qualification of Health Risks paper
    paf = paf.assign(mean_iq=100)

    # Calculate the standard deviation for a normal distribution using the z-score formula:
    #   z = (x - mean) / sd,
    #   sd = (x - mean) / z,
    # where x = IQ cutoff, z = PPF(cumulative YLD), PPF = percent-point function.
    # Here cumulative YLD is interpretted as a probability. Each demographic combination
    # is assumed to have its own IQ distribution.
    paf["sd"] = (paf["iq_cutoff"] - paf["mean_iq"]) / scipy.stats.norm.ppf(paf["yld_cumsum"])

    # The PPF (percent-point function) returns non-infinite values if its input is > 0 and < 1.
    # We set the standard deviation to NaN where yld_cumsum == 0 to omit these standard
    # deviations from the sd_mean calculation below. If all yld_cumsums are zero for a
    # particular demographic and draw, then yld_sum will also be zero, sd_mean will be NaN, and
    # the PAF will be set to zero lower down in this function.
    paf.loc[paf["yld_cumsum"] == 0, "sd"] = np.nan

    # Use the mean SD observed across severities as a conservative approach to our assumed
    # IQ distribution. Essentially, we estimate SD using IQ cutoff and z-score
    # (from cumulative YLDs) at all IQ cutoff points and then take the mean.
    sd_mean = (
        paf.groupby(_INDEX_COLS + ["draw"])["sd"]
        .mean()
        .reset_index()
        .rename(columns={"sd": "sd_mean"})
    )
    paf = pd.merge(paf, sd_mean, on=_INDEX_COLS + ["draw"])

    # Using original distribution (not_shifted) and theoretical distribution without
    # any blood lead exposure (shifted), calculate the probability for each sequela at or
    # below the IQ cutoff. First a z-score is computed and then ran through the cumulative
    # distribution function (CDF) to get a probability.
    paf["shifted"] = scipy.stats.norm.cdf(
        (paf["iq_cutoff"] - (paf["mean_iq"] + paf["iq_shift"])) / paf["sd_mean"]
    )
    paf["not_shifted"] = scipy.stats.norm.cdf(
        (paf["iq_cutoff"] - paf["mean_iq"]) / paf["sd_mean"]
    )

    # Calculate the PAF using the standard PAF formula where the shifted distribution
    # represents risk at the ideal level of exposure, similar to RR at the TMREL.
    # Weight by YLDs.
    paf["paf"] = (
        (paf["not_shifted"] - paf["shifted"])
        * paf["yld"]
        / (paf["not_shifted"] * paf["yld_sum"])
    )
    paf.loc[paf["yld_sum"] == 0, "paf"] = 0

    # Sum across severities (already weighted by YLDs)
    paf = paf.groupby(_INDEX_COLS + ["draw"])["paf"].sum().reset_index()

    # Transform wide by draw
    paf_wide = pd.pivot(paf, index=_INDEX_COLS, columns="draw")
    paf_wide.columns = paf_wide.columns.droplevel(0)
    paf_wide = paf_wide.reset_index()

    # Apply to intellectual disability, YLD only and save
    paf_wide = paf_wide.assign(
        rei_id=constants.BLOOD_LEAD_REI_ID,
        cause_id=_INTELLECTUAL_DISABILITY_CAUSE_ID,
        mortality=0,
        morbidity=1,
    )
    io_utils.write_paf(paf_wide, root_dir, location_id)
