r"""Compute and export all the cause-risk-pair PAFs for given acause.

That involves:

1) Finding all risks associated with said acause
2) Pulling SEVs and cause-risk-specific RRs from inputs
3) Compute PAF as ``paf = 1 - 1 / (sev * (rr - 1) + 1)``

About the two inputs:

1) SEV.  There's a separate directory for non-vaccine SEV and for vaccine SEV.
2) RR.  Like SEV, this is further divided into a non-vaccine and vaccine.

Example call (pulling gbd paf from get_draws):

.. code:: bash

    python compute_paf.py --acause cvd_ihd --rei metab_bmi --version test \
        --directly-modeled-paf 20190419_dm_pafs \
        --sev 20180412_trunc_widerbounds --rrmax 20180407_paf1_update \
        --vaccine-sev 20180319_new_sdi --vaccine-rrmax 20171205_refresh \
        --gbd-round-id 4 --years 1990:2017:2040 --draws 100

If there's already a cleaned version of gbd cause-risk PAFs stored,
one may access it via the --gbd-paf-version flag to bypass using get_draws():

.. code:: bash

    python compute_paf.py --acause cvd_ihd --rei metab_bmi --version test \
        --directly-modeled-paf 20190419_dm_pafs \
        --sev 20180412_trunc_widerbounds --rrmax 20180407_paf1_update \
        --vaccine-sev 20180319_new_sdi --vaccine-rrmax 20171205_refresh \
        --gbd-paf-version 20180521_2016_gbd \
        --gbd-round-id 4 --years 1990:2017:2040 --draws 100

If there's already a cleaned version of gbd cause-risk PAFs stored,
one may access it via the --gbd-paf-version flag to bypass using get_draws()

Note that the 'draws' input arg is not only required, it also entails
up/down-sampling if any of the input files have number of draws not equal
to 'draws'.
"""

import gc
from pathlib import Path
from typing import List, Optional, Tuple

import xarray as xr
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_database_interface.lib.query import cause
from fhs_lib_file_interface.lib.query import mediation
from fhs_lib_file_interface.lib.query import rrmax as rrmax_functions
from fhs_lib_file_interface.lib.symlink_file import symlink_file_to_directory
from fhs_lib_file_interface.lib.version_metadata import (
    FHSDirSpec,
    FHSFileSpec,
    VersionMetadata,
)
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr_scenario
from fhs_lib_year_range_manager.lib.year_range import YearRange
from scipy.special import expit, logit
from tiny_structured_logger.lib.fhs_logging import get_logger

from fhs_pipeline_scalars.lib.constants import PAFConstants
from fhs_pipeline_scalars.lib.forecasting_db import (
    get_most_detailed_acause_related_risks,
    is_maybe_negative_paf,
)
from fhs_pipeline_scalars.lib.utils import (
    conditionally_triggered_transformations,
    product_of_mediation,
    save_paf,
)

logger = get_logger()


def _read_sev(
    rei: str,
    sev: str,
    past_sev: str,
    vaccine_sev: str,
    past_vaccine_sev: str,
    gbd_round_id: int,
    years: YearRange,
    draws: int,
) -> xr.DataArray:
    """Read in SEV [from the vaccine subdir if `sev` is a vaccine SEV].

    Args:
        rei (str): risk, could also be vaccine intervention.
        sev (str): upstrem sev version.
        past_sev (str): past (GBD) sev version.
        vaccine_sev (str): input vaccine sev version.
        gbd_round_id (int): gbd round id
        years (YearRange): [past_start, forecast_start, forecast_end] years.
        draws (int): number of draws for output file.  This means input files
            will be up/down-sampled to meet this criterion.

    Returns:
        (xr.DataArray): SEV in dataarray form.
    """
    filename = f"{rei}.nc"

    # Determine the effective versions and stage
    if rei in PAFConstants.VACCINE_RISKS:
        effective_future_version = vaccine_sev
        effective_past_version = past_vaccine_sev
        effective_stage = "vaccine"
    else:
        effective_future_version = sev
        effective_past_version = past_sev
        effective_stage = "sev"

    # Create the past and future file specifications
    future_version_metadata = VersionMetadata.make(
        data_source=gbd_round_id,
        epoch="future",
        stage=effective_stage,
        version=effective_future_version,
    )
    future_file_spec = FHSFileSpec(version_metadata=future_version_metadata, filename=filename)

    past_version_metadata = VersionMetadata.make(
        data_source=gbd_round_id,
        epoch="past",
        stage=effective_stage,
        version=effective_past_version,
    )
    past_file_spec = FHSFileSpec(version_metadata=past_version_metadata, filename=filename)

    # Load the past and future data
    future = open_xr_scenario(future_file_spec).sel(year_id=years.forecast_years)
    past = open_xr_scenario(past_file_spec).sel(year_id=years.past_years)

    # Vaccines are treated as an anti-risk, so we subtract it from 1.0
    if rei in PAFConstants.VACCINE_RISKS:
        future = 1.0 - future
        past = 1.0 - past

    # It is expected that the past and the future are exlusive only in
    # year_id and scenario dims.  Hence we can find the overlap in loc-age-sex.
    past = past.sel(
        location_id=future["location_id"],
        age_group_id=future["age_group_id"],
        sex_id=future["sex_id"],
    )

    past = resample(past, draws)
    future = resample(future, draws)

    if "scenario" in past.dims:
        past = past.sel(scenario=0).drop_vars("scenario")

    out = xr.concat([past, future], dim="year_id", coords="minimal", join="inner")

    del past, future
    gc.collect()

    out = out.where(out.year_id <= years.forecast_end, drop=True)
    out = conditionally_triggered_transformations(out, gbd_round_id, years)

    if rei in PAFConstants.VACCINE_RISKS:
        if "scenario" in out.dims:
            non_reference_scenarios = [s for s in out.scenario.values if s != 0]
            for scenario in non_reference_scenarios:
                out.loc[dict(scenario=scenario, year_id=years.past_years)] = out.sel(
                    scenario=0, year_id=years.past_years
                )

    return out


def _read_and_process_rrmax(
    acause: str,
    rei: str,
    rrmax: str,
    vaccine_rrmax: str,
    gbd_round_id: int,
    years: YearRange,
    draws: int,
) -> xr.DataArray:
    """Identify correct parameters for `read_rrmax` and postprocess returned xarray.

    Args:
        acause (str): analytical cause.
        rei (str): risk, could also be vaccine intervention.
        rrmax (str): input rrmax version
        vaccine_rrmax (str): input vaccine rrmax version.
        gbd_round_id (int): gbd round id.
        years (YearRange): [past_start, forecast_start, forecast_end] years.
        draws (int): number of draws for output file.  This means input files
            will be up/down-sampled to meet this criterion.

    Returns:
        xr.DataArray: RRmax in dataarray form.
    """
    if acause == "tb":  # NOTE: RRMax from arbitrary child cause only for TB.
        cause_id = PAFConstants.TB_OTHER_CAUSE_ID
    else:
        cause_id = cause.get_cause_id(acause=acause)

    if rei in PAFConstants.VACCINE_RISKS:
        version = vaccine_rrmax
    else:
        version = rrmax

    out = rrmax_functions.read_rrmax(
        acause=acause,
        cause_id=cause_id,
        rei=rei,
        gbd_round_id=gbd_round_id,
        version=version,
        draws=draws,
    )
    out = out.where(out != 0, drop=True)

    out = conditionally_triggered_transformations(out, gbd_round_id, years)

    if rei in PAFConstants.VACCINE_RISKS:
        # The values stored in vaccine data files are actually not RR,
        # but rather
        # r = Incidence[infection | vax] / Incidence[infection | no vax],
        # as "percent reduction of diseased cases if vaccinated",
        # and should be r < 1.
        # We compute the actual RR as 1/r.
        # Any value > 1 should be capped.
        out = out.where(out <= PAFConstants.PAF_UPPER_BOUND).fillna(
            PAFConstants.PAF_UPPER_BOUND
        )
        out = 1.0 / out  # as mentioned earlier, we compute RR as 1/r.

    if "draw" in out.dims and len(out["draw"]) != draws:
        out = resample(out, draws)

    return out


def _get_gbd_paf(
    acause: str,
    rei: str,
    gbd_paf_version: str,
    gbd_round_id: int,
) -> Optional[xr.DataArray]:
    """Load PAF from the given gbd_paf_version.

    Certain acauses may be given that don't exist in the GBD, and for these we will actually
    load the *parent*'s PAF data.

    Args:
        acause (str): analytical cause.
        rei (str): risk, could also be vaccine intervention.
        gbd_paf_version (str): the version name to load past (GBD) PAF data from.
        gbd_round_id (int): gbd round id
        location_ids (list[int]): locations to get pafs from.
        draws (int): number of draws for output file. This means input files
            will be up/down-sampled to meet this criterion.

    Returns:
        Optional[xr.DataArray]: DataArray with complete demographic indices, or None if the rei
            is in `PAFConstants.VACCINE_RISKS`.
    """
    acause_rei_version_metadata = VersionMetadata.make(
        data_source=gbd_round_id, epoch="past", stage="paf", version=gbd_paf_version
    )
    acause_rei_file_spec = FHSFileSpec(
        version_metadata=acause_rei_version_metadata,
        sub_path=("risk_acause_specific",),
        filename=f"{acause}_{rei}.nc",
    )

    if rei in PAFConstants.VACCINE_RISKS:
        return None

    if acause_rei_file_spec.data_path().exists():
        return open_xr_scenario(acause_rei_file_spec)

    # "etiology causes" must be pulled from the parent cause.
    elif acause in cause.non_gbd_causes():
        parent_acause = cause.get_parent(acause=acause, gbd_round_id=gbd_round_id)
        parent_acause_rei_file_spec = FHSFileSpec(
            version_metadata=acause_rei_version_metadata,
            sub_path=("risk_acause_specific",),
            filename=f"{parent_acause}_{rei}.nc",
        )

        if parent_acause_rei_file_spec.data_path().exists():
            return open_xr_scenario(parent_acause_rei_file_spec)

        else:  # the etiology's parent file does not exist
            raise OSError(f"{parent_acause_rei_file_spec.data_path()} does not exist.")

    else:  # the file does not exist
        raise OSError(f"{acause_rei_file_spec.data_path()} does not exist.")


def _data_cleaning_for_paf(
    paf: xr.DataArray, maybe_negative_paf: Optional[bool] = False
) -> xr.DataArray:
    """Encode data cleaning customized for PAF.

    1.) set non-finite values (nan, -inf, inf) to 0
    2.) set > UPPER_BOUND to UPPER_BOUND
    3.) set < LOWER_BOUND to LOWER_BOUND

    Non-finite PAF values likely come from outer-join mismatches between
    sev and rr, and we set those to 0
    for (2) and (3), per discussion with central comp, PAF values over
    boundaries are simply capped, not resampled.

    Args:
        paf (xr.DataArray): dataarray of PAF values
        maybe_negative_paf (Optional[bool]): ``True`` is PAF is allowed to be
            negative. Defaults to ``False``.

    Returns:
        (xr.DataArray):
            cleaned dataarray.
    """
    if maybe_negative_paf:
        lower_bound = PAFConstants.PAF_LOWER_BOUND
    else:
        lower_bound = 1 - PAFConstants.PAF_UPPER_BOUND

    return paf.fillna(lower_bound).clip(min=lower_bound, max=PAFConstants.PAF_UPPER_BOUND)


def _compute_correction_factor(
    fhs_paf: xr.DataArray, gbd_paf: xr.DataArray, maybe_negative_paf: Optional[bool] = False
) -> xr.DataArray:
    r"""Bias-correct Forecasted PAF by GBD PAF.

    This is essentially
    an "intercept-shift", and it happens at the last year of past (gbd round),
    and hence the input args should be single-year arrays.

    Even though PAF values should logically be in the closed interval
    :math:`[-1, 1]`, we expect both ``fhs_paf`` and ``gbd_paf`` to be in the
    open interval :math:`(-1, 1)` due to upstream data cleaning. Furthermore,
    most cause-risk pairs are *not* protective (i.e. non-negative), so are
    actually expected to be in the open interval :math:`(0, 1)`.

    This method computes correction factor in logit space by transforming
    the PAF values via :math:`x_{\text{corrected}} = (1 + x) / 2`, and with the
    correction factor being the difference between fhs and gbd in logit space:

    .. math::

        \text{correction-factor} =
            \text{logit}(\frac{1 + \mbox{PAF}_{\text{gbd}}}{2})
            - \text{logit}(\frac{1 + \mbox{PAF}_{\text{fhs}}}{2})

    For cause-risk pairs that *cannot* have protective (i.e. negative PAFs),
    the correction factor equation is:

    .. math::

        \text{correction-factor} =
            \text{logit}(\mbox{PAF}_{\text{gbd}})
            - \text{logit}(\mbox{PAF}_{\text{fhs}})

    This correction factor will later be added to the forecasted PAF values
    in logit space, prior to back-transformation.

    By default, non-finite correction factor values are reset to 0.  These
    non-finite values could come from mismatched cells from outer-join
    arithmetic, commonly found along the ``age_group_id`` dimension between
    GBD and FHS (np.nan)

    Args:
        fhs_paf (xr.DataArray): forecasted PAF at gbd round year.
        gbd_paf (xr.DataArray): gbd PAF.  Only contains the gbd round year.
        maybe_negative_paf (Optional[bool]): ``True`` is PAF is allowed to be
            negative. Defaults to ``False``.

    Returns:
        (xr.DataArray):
            correction factor.
    """
    # first make sure the input args are year-agnostic
    if ("year_id" in fhs_paf.coords or "year_id" in fhs_paf.dims) and fhs_paf[
        "year_id"
    ].size > 1:
        raise ValueError("fhs_paf has year dim larger than size=1")

    if ("year_id" in gbd_paf.coords or "year_id" in gbd_paf.dims) and gbd_paf[
        "year_id"
    ].size > 1:
        raise ValueError("gbd_paf has year dim larger than size=1")

    with xr.set_options(arithmetic_join="outer"):
        if maybe_negative_paf:
            correction_factor = logit((1 + gbd_paf) / 2) - logit((1 + fhs_paf) / 2)
        else:
            correction_factor = logit(gbd_paf) - logit(fhs_paf)

    # the above outer-join could result in nulls.
    # by default, null correction factor values are reset to 0
    correction_factor = correction_factor.fillna(0)

    # We required the inputs to have just one year_id; now we remove that dim/coord from the
    # result.
    if "year_id" in correction_factor.dims:
        correction_factor = correction_factor.squeeze("year_id")
    if "year_id" in correction_factor.coords:
        correction_factor = correction_factor.drop_vars("year_id")

    return correction_factor


def _apply_paf_correction(
    fhs_paf: xr.DataArray, cf: xr.DataArray, maybe_negative_paf: bool = False
) -> xr.DataArray:
    r"""Correct forecasted PAF in logit space and back-transform.

    .. math::

        \mbox{PAF}_{\text{corrected}} = 2 * \text{expit}(\text{logit}
            (\frac{1 + \mbox{PAF}_{\text{FHS}}}{2} + \text{correction-factor}
            ) - 1

    for cause-risk pairs that *can* be protective. If cause-risk pairs are not
    allowed to protective (the majority of them), then the equation for the
    corrected PAF is

    .. math::

        \mbox{PAF}_{\text{corrected}} =
            \mbox{PAF}_{\text{FHS}} + \text{correction-factor}

    Because a logit function's argument x is :math:`[0, 1]` and a protective
    PAF can be in the range :math:`[-1, 1]`, a natural mapping from PAF space
    to logit space is :math:`x_{\text{corrected}} = (1 + x) / 2`. The
    back-transform to PAF space is hence :math:`2 * expit( logit(x) ) + 1`.

    The correction factor ``cf`` is also computed within the same logit space.
    Once the correction is made in logit space, the resultant quantity
    is mapped back to PAF-space via the aforementioned back-transform.

    Args:
        fhs_paf (xr.DataArray): forecasted PAF.  Has many years along year_id.
        cf (xr.DataArray): correction factor, should not have year_id dim.
        maybe_negative_paf (bool): ``True`` is PAF is allowed to be
            negative. Defaults to ``False``.

    Returns:
        (xr.DataArray):
            correct PAF.
    """
    if maybe_negative_paf:
        corrected_paf = 2 * expit(logit((1 + fhs_paf) / 2) + cf) - 1
    else:
        corrected_paf = expit(logit(fhs_paf) + cf)

    return corrected_paf


def _get_paf(
    acause: str,
    rei: str,
    years: YearRange,
    gbd_round_id: int,
    draws: int,
    sev: str,
    past_sev: str,
    rrmax: str,
    vaccine_sev: str,
    past_vaccine_sev: str,
    vaccine_rrmax: str,
) -> Tuple[xr.DataArray, List[int]]:
    """Calculate a PAF from 1) the SEV for the risk, and 2) the RRMax for the cause-risk pair.

    In particular, we load SEVs from the specified future versions ``sev``, ``vaccine_sev``,
    plus past versions ``past_sev``, ``past_vaccine_sev``. And we load the RRMax data from the
    ``rrmax``, ``vaccine_rrmax`` versions.
    """
    sev_da = _read_sev(
        rei=rei,
        sev=sev,
        past_sev=past_sev,
        vaccine_sev=vaccine_sev,
        past_vaccine_sev=past_vaccine_sev,
        gbd_round_id=gbd_round_id,
        years=years,
        draws=draws,
    )

    rrmax_da = _read_and_process_rrmax(
        acause=acause,
        rei=rei,
        rrmax=rrmax,
        vaccine_rrmax=vaccine_rrmax,
        gbd_round_id=gbd_round_id,
        years=years,
        draws=draws,
    )

    # Make 0 values into 1s
    defaulted_values = rrmax_da.where(rrmax_da.values != 0)
    rrmax_da = defaulted_values.fillna(1)

    # estimated cause-risk-specific paf
    with xr.set_options(arithmetic_join="inner"):
        paf = 1 - 1 / (sev_da * (rrmax_da - 1) + 1)

    location_ids = sev_da["location_id"].values.tolist()

    del sev_da, rrmax_da
    gc.collect()

    return paf, location_ids


def compute_paf(
    acause: str,
    rei: str,
    version: str,
    years: YearRange,
    gbd_round_id: int,
    draws: int,
    sev: str,
    past_sev: str,
    rrmax: str,
    vaccine_sev: str,
    past_vaccine_sev: str,
    vaccine_rrmax: str,
    gbd_paf_version: str,
    save_past_data: bool,
) -> None:
    r"""Compute and export PAF for the given acause-risk pair.

    Said PAF is exported to FILEPATH.

    Args:
        acause (str): analytical cause.
        rei (str): rei, or commonly called risk.
        version (str): version to export to.
        years (YearRange): [past_start, forecast_start, forecast_end] years.
        gbd_round_id (int): gbd round id.
        draws (int): number of draws for output file.  This means input files
            will be up/down-sampled to meet this criterion.
        sev (str): input future sev version.
        past_sev (str): input past sev version.
        rrmax (str): input rrmax version.
        vaccine_sev (str): input vaccine sev version.
        past_vaccine_sev (str): input past vaccine sev version.
        vaccine_rrmax (str): input vaccine rrmax version.
        gbd_paf_version (str): gbd_paf version to read from,
            if not downloading from get_draws().
        save_past_data (bool): if true,
            save files for past data in FILEPATH.
    """
    paf, location_ids = _get_paf(
        acause,
        rei,
        years,
        gbd_round_id,
        draws,
        sev,
        past_sev,
        rrmax,
        vaccine_sev,
        past_vaccine_sev,
        vaccine_rrmax,
    )

    maybe_negative_paf = is_maybe_negative_paf(acause, rei, gbd_round_id)

    # Forecasted PAFs are cleaned first before further processing
    paf = _data_cleaning_for_paf(paf, maybe_negative_paf)

    # now ping get_draws for gbd paf values
    logger.info(f"Got estimated paf for {acause}_{rei}.  Pulling gbd paf...")

    paf, correction_factor = _correct_paf(
        gbd_round_id,
        acause,
        rei,
        draws,
        paf,
        maybe_negative_paf,
        gbd_paf_version,
        location_ids,
    )

    paf_unmediated = _apply_mediation(acause, rei, gbd_round_id, paf)

    # we need to save the results separately in "past" and "future"
    if save_past_data:
        past_future_dict = {"past": years.past_years, "future": years.forecast_years}
    else:
        past_future_dict = {"future": years.forecast_years}

    for p_or_f, yrs in past_future_dict.items():
        save_paf(
            paf=paf.sel(year_id=yrs),
            gbd_round_id=gbd_round_id,
            past_or_future=p_or_f,
            version=version,
            acause=acause,
            cluster_risk=rei,
            sev=sev,
            rrmax=rrmax,
            vaccine_sev=vaccine_sev,
            vaccine_rrmax=vaccine_rrmax,
            gbd_paf_version=gbd_paf_version,
        )

        if paf_unmediated is not None:
            save_paf(
                paf=paf_unmediated.sel(year_id=yrs),
                gbd_round_id=gbd_round_id,
                past_or_future=p_or_f,
                version=version,
                acause=acause,
                cluster_risk=rei,
                file_suffix="_unmediated",
                sev=sev,
                rrmax=rrmax,
                vaccine_sev=vaccine_sev,
                vaccine_rrmax=vaccine_rrmax,
                gbd_paf_version=gbd_paf_version,
            )

        # now saving cause-risk-specific correction factor
        if p_or_f == "past":
            save_paf(
                paf=correction_factor,
                gbd_round_id=gbd_round_id,
                past_or_future=p_or_f,
                version=version,
                acause=acause,
                cluster_risk=rei,
                file_suffix="_cf",
                space="logit",
                sev=sev,
                rrmax=rrmax,
                vaccine_sev=vaccine_sev,
                vaccine_rrmax=vaccine_rrmax,
                gbd_paf_version=gbd_paf_version,
            )

    del paf, correction_factor
    gc.collect()


def _apply_mediation(
    acause: str, rei: str, gbd_round_id: int, paf: xr.DataArray
) -> xr.DataArray:
    """Apply to paf the mediation values for the given acause, rei."""
    # Now determine if we need to save unmediated PAF as well
    med = mediation.get_mediation_matrix(gbd_round_id)

    if acause in med["acause"].values and rei in med["rei"].values:
        mediator_matrix = med.sel(acause=acause, rei=rei)
        if (mediator_matrix > 0).any():  # if there is ANY mediation
            all_risks = get_most_detailed_acause_related_risks(acause, gbd_round_id)
            # this determines how to compute the unmediated RR
            mediation_prod = product_of_mediation(acause, rei, all_risks, gbd_round_id)
            # reverse engineer: sev * (rrmax - 1) = 1 / (1 - paf) - 1
            # rrmax^U - 1 = (rrmax - 1) * mediation_prod
            # so sev * (rrmax^U - 1) = (1 / (1 - paf) - 1) * mediation_prod
            sev_rrmax_1_u = (1 / (1 - paf) - 1) * mediation_prod
            paf_unmediated = 1 - 1 / (sev_rrmax_1_u + 1)
        else:
            paf_unmediated = None
    else:
        paf_unmediated = None
    return paf_unmediated


def _correct_paf(
    gbd_round_id: int,
    acause: str,
    rei: str,
    draws: int,
    paf: xr.DataArray,
    maybe_negative_paf: bool,
    gbd_paf_version: str,
    location_ids: List[int],
) -> Tuple[xr.DataArray, xr.DataArray]:
    """Determine and apply correction to ``paf`` so that it matches data in gbd_paf_version.

    Essentially, the correction is the difference, in logit space, betweeen the paf and values
    in the last-past year, and it is simply added in logit space. The ``paf`` data is taken in
    identity space, so logit is applied and reversed.
    """
    gbd_paf = _get_gbd_paf(acause, rei, gbd_paf_version, gbd_round_id)
    if gbd_paf is not None:
        gbd_paf = gbd_paf.sel(location_id=location_ids)
        gbd_paf = resample(gbd_paf, draws)

        logger.info(f"Pulled gbd paf for {acause}_{rei}. Computing adjusted paf...")

        # compute correction factor and perform adjustment

        # First make sure there's no COMPLETE mismatch between paf and gbd_paf.
        # If so, an error should be raised
        paf.load()
        gbd_paf.load()  # need to force load() because dask is lazy
        if (paf - gbd_paf).size == 0:  # normal arithmetic is inner-join
            error_message = (
                "Complete mismatch between computed and GBD in "
                f"{acause}-{rei} PAF.  Are you sure you used the correct "
                "version of GBD PAF?"
            )
            logger.error(error_message)
            raise ValueError(error_message)

        gbd_paf = _data_cleaning_for_paf(gbd_paf, maybe_negative_paf)
        gbd_year = max(gbd_paf.year_id.values)

        correction_factor = _compute_correction_factor(
            paf.sel(year_id=gbd_year), gbd_paf.sel(year_id=gbd_year), maybe_negative_paf
        )

        del gbd_paf
        gc.collect()

        paf = _apply_paf_correction(paf, correction_factor, maybe_negative_paf)

        logger.info(f"Adjusted paf for {acause}_{rei}.  Now saving...")
    else:  # correction factor is 0, and we leave paf as is
        correction_factor = xr.zeros_like(paf)
        logger.info(f"paf for {acause}_{rei} not adjusted because gbd_paf is None")

    return paf, correction_factor


def symlink_paf_file(
    acause: str,
    rei: str,
    gbd_paf_version: str,
    calculated_paf_version: str,
    pre_calculated_paf: str,
    gbd_round_id: int,
    paf_type: str,
    save_past_data: bool,
) -> None:
    """Create symlink to files with directly-modeled PAF data.

    Creates symlinks of past and future directly-modeled PAF data files to the
    directory with PAFs calculated from SEVs and RRmaxes.

    Args:
        acause (str):
            Indicates the cause of the cause-risk pair
        rei (str):
            Indicates the risk of the cause-risk pair
        calculated_paf_version (str):
            Output version of this script where directly-modeled PAFs are
            symlinked, and calculated PAFs are saved.
        pre_calculated_paf (str):
            The version of PAFs with the pre-calculated PAF to be symlinked
            resides. Either a directly modeled PAF version or temperature PAF
            version.
        gbd_round_id (int):
            The numeric ID representing the GBD round.
        paf_type (str):
            What type of PAF is this? Right now can be "directly_modeled" or
            "custom_forecast".
        save_past_data (bool): if true,
            save files for past data in FILEPATH

    Raises:
        RuntimeError:
            If symlink sub-process fails.
    """
    effective_pre_calc_versions = {"future": pre_calculated_paf}
    if save_past_data:
        effective_pre_calc_versions["past"] = gbd_paf_version

    for epoch, effective_pre_calc_version in effective_pre_calc_versions.items():
        # Currently, we only get custom forecasts for temperature PAFs.
        # As of GBD2019, they are not saved in a sub-directory.
        sub_dir_str = (
            ""
            if paf_type == "custom_forecast" and epoch == "future"
            else "risk_acause_specific"
        )

        # Define the calculated-PAF version and directory specifications
        calculated_paf_version_metadata = VersionMetadata.make(
            data_source=gbd_round_id, epoch=epoch, stage="paf", version=calculated_paf_version
        )
        calculated_paf_dir_spec = FHSDirSpec(
            version_metadata=calculated_paf_version_metadata,
            sub_path=("risk_acause_specific",),
        )

        # Define the pre-calculated-PAF version, directory and file specifications
        pre_calculated_paf_version_metadata = VersionMetadata.make(
            data_source=gbd_round_id,
            epoch=epoch,
            stage="paf",
            version=effective_pre_calc_version,
        )
        pre_calculated_paf_dir_spec = FHSDirSpec(
            version_metadata=pre_calculated_paf_version_metadata, sub_path=(sub_dir_str,)
        )
        pre_calculated_paf_file_spec = FHSFileSpec.from_dirspec(
            dir=pre_calculated_paf_dir_spec, filename=f"{acause}_{rei}.nc"
        )

        # Symlink the pre-calculated-PAF data into the calculated-PAF directory, if the
        # pre-calculated-PAF data already exists
        if pre_calculated_paf_file_spec.data_path().exists():
            _attempt_symlink(
                source=pre_calculated_paf_file_spec.data_path(),
                target=calculated_paf_dir_spec.data_path(),
                target_is_directory=True,
            )

        # Otherwise, symlink the user-specifed ``acause``'s **parent** cause from the
        # pre-calculated-PAF data into the calculated-PAF directory **as** the child cause
        else:
            # Check if cause is an etiology in GBD.
            if acause not in cause.non_gbd_causes():
                raise FileNotFoundError(
                    f"{pre_calculated_paf_file_spec.data_path()} not found."
                )

            _symlink_parent_cause(
                acause=acause,
                rei=rei,
                gbd_round_id=gbd_round_id,
                calculated_paf_dir_spec=calculated_paf_dir_spec,
                pre_calculated_paf_dir_spec=pre_calculated_paf_dir_spec,
            )


def _attempt_symlink(source: Path, target: Path, target_is_directory: bool) -> None:
    """Tries to symlink; catches the error if the target already exists."""
    try:
        symlink_file_to_directory(
            source_path=source, target_path=target, target_is_directory=target_is_directory
        )

    except FileExistsError:
        filename = source.name if target_is_directory else target.name
        directory = target if target_is_directory else target.parent
        logger.info(f"{filename} already exists in {directory}.")


def _symlink_parent_cause(
    acause: str,
    rei: str,
    gbd_round_id: int,
    calculated_paf_dir_spec: FHSDirSpec,
    pre_calculated_paf_dir_spec: FHSDirSpec,
) -> None:
    # Pull the parent acause from the database
    parent_acause = cause.get_parent(acause=acause, gbd_round_id=gbd_round_id)

    # Define the full file specifications using the provided directory specifications
    # Note that the pre-calculated-PAF data uses the parent acause while the calculated-PAF
    # data uses the actual acause
    pre_calculated_paf_file_spec = FHSFileSpec.from_dirspec(
        dir=pre_calculated_paf_dir_spec, filename=f"{parent_acause}_{rei}.nc"
    )
    calculated_paf_file_spec = FHSFileSpec.from_dirspec(
        dir=calculated_paf_dir_spec, filename=f"{acause}_{rei}.nc"
    )

    _attempt_symlink(
        source=pre_calculated_paf_file_spec.data_path(),
        target=calculated_paf_file_spec.data_path(),
        target_is_directory=False,
    )
