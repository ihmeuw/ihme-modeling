"""Functions to run cod model.
"""

import sys
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import xarray as xr
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_data_transformation.lib.post_hoc_relu import post_hoc_relu
from fhs_lib_data_transformation.lib.processing import invlog_with_offset, log_with_offset
from fhs_lib_data_transformation.lib.quantiles import Quantiles
from fhs_lib_data_transformation.lib.truncate import cap_forecasts
from fhs_lib_database_interface.lib import db_session
from fhs_lib_database_interface.lib.constants import (
    CauseConstants,
    DimensionConstants,
    LocationConstants,
    ScenarioConstants,
    SexConstants,
)
from fhs_lib_database_interface.lib.strategy_set import strategy
from fhs_lib_file_interface.lib.version_metadata import FHSFileSpec, VersionMetadata
from fhs_lib_file_interface.lib.versioning import validate_versions_scenarios_listed
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr_scenario, save_xr_scenario
from fhs_lib_model.lib.gk_model.GKModel import ConvergenceError, GKModel
from fhs_lib_model.lib.repeat_last_year import repeat_last_year
from fhs_lib_year_range_manager.lib.year_range import YearRange
from sksparse.cholmod import CholmodNotPositiveDefiniteError
from tiny_structured_logger.lib import fhs_logging

from fhs_pipeline_mortality.lib.config_dataclasses import (
    CauseSpecificModelingArguments,
    CauseSpecificVersionArguments,
)
from fhs_pipeline_mortality.lib.downloaders import load_cod_dataset, load_paf_covs

logger = fhs_logging.get_logger()

ASFR = "asfr"
BETA_GLOBAL = "beta_global"
CUTOFF = 1000
GAMMA_AGE = "gamma_age"
GAMMA_LOCATION_AGE = "gamma_location_age"
GAMMA_LOCATION = "gamma_location"
INTERCEPT = "intercept"
LN_RISK_SCALAR = "ln_risk_scalar"
NLOCS_CUTOFF = 150
OMEGA_AMP = 0.0
# Post-hoc constraint on the sum of all coefficients on time to be <= 0
# ie, beta_global and gamma_age slopes on time will be set to 0 if
# the mean beta_global < 0 and beta_global + gamma_age > 0
# called a "rectified linear unit" (RELU) in machine learning parlance.
# Constraint options are nonnegative and nonpositive.
RELU = dict(time_var="nonpositive")
SAVE_FUTURE = True
SAVE_PAST = True
SDI = "sdi"
SDI_KNOT = 0.8
SDI_PART1 = "sdi_part1"
SDI_PART2 = "sdi_part2"
SDI_TIME = "sdi_time"
TIME_VAR = "time_var"
Y = "y"


def main(
    modeling_args: CauseSpecificModelingArguments,
    version_args: CauseSpecificVersionArguments,
    scenarios: Optional[Iterable[int]],
) -> None:
    """Run a cause-of-death model using appropriate strategy for input acause & save results.

    Args:
        modeling_args (CauseSpecificModelingArguments): dataclass containing
            cause specific modeling arguments
        version_args (CauseSpecificVersionArguments): dataclass containing
            cause specific version arguments
        scenarios (Optional[Iterable[int]]): scenarios to model on
    """
    if scenarios and version_args.output_scenario:
        raise ValueError(
            "The --scenarios flag cannot be used in single-scenario mode (with the "
            "--output-scenario flag)."
        )

    output_version = version_args.versions.get("future", "death")
    output_underlying_version = output_version.append_version_suffix("_underlying")

    validate_versions_scenarios_listed(
        versions=[v for v in version_args.versions] + [output_version],
        output_versions=[version_args.versions.get("future", "death")],
        output_scenario=version_args.output_scenario,
    )

    sex_name = _get_sex_name(sex_id=modeling_args.sex_id)
    addcovs = _get_addcovs(modeling_args=modeling_args, acause=modeling_args.acause)

    ds = load_cod_dataset(
        addcovs=addcovs,
        modeling_args=modeling_args,
        versions=version_args.versions,
        logspace_conversion_flags=version_args.logspace_conversion_flags,
        scenarios=scenarios,
    )
    # Get list of SEVs to include as covariates (where PAF=1)
    # Don't need to supply past versions or scenarios to get this list
    reis = load_paf_covs(
        modeling_args=modeling_args,
        versions=version_args.versions,
        scenarios=[0],
        listonly=True,
    )

    skip_gk_causes = get_strategy_causes(
        modeling_args=modeling_args, gk_strategy_id=CauseConstants.SKIP_GK_STRATEGY_ID
    )

    # ntd_nema and ntd_dengue aren't stable enough to have a regular gk model run on them.
    if modeling_args.acause in skip_gk_causes:
        _model_ntd(modeling_args, sex_name, output_version, ds, scenarios)

    if modeling_args.spline:
        ds[SDI_PART1] = np.minimum(ds[SDI], SDI_KNOT)
        ds[SDI_PART2] = np.maximum(ds[SDI] - SDI_KNOT, 0.0)

    interaction_causes = get_strategy_causes(
        modeling_args=modeling_args, gk_strategy_id=CauseConstants.INTERACTION_STRATEGY_ID
    )

    if modeling_args.acause in interaction_causes:
        ds[SDI_TIME] = ds[SDI] * ds[TIME_VAR]

    past_da = ds["y"].copy()
    ds["y"] = ds["y"].mean("draw")

    no_fixed_effect_risk_cause = get_strategy_causes(
        modeling_args=modeling_args,
        gk_strategy_id=CauseConstants.NO_FIXED_EFFECT_RISK_STRATEGY_ID,
    )

    if not (
        modeling_args.acause == no_fixed_effect_risk_cause[0] and modeling_args.sex_id == 1
    ):
        for r in reis:
            ds[r].values[np.isnan(ds[r].values)] = 0.0

    # if forecasting subnationals but not using them for the fit,
    # hold out the subnationals until the prediction step
    if not modeling_args.fit_on_subnational and modeling_args.subnational:
        locs = ds["level"].to_dataframe("level").reset_index()
        locs_national = locs.query("level == 3").location_id.tolist()
        locs_subnational = locs.query("level == 4").location_id.tolist()
        ds_subnat = ds.sel(location_id=locs_subnational)
        ds = ds.sel(location_id=locs_national)

    if SAVE_PAST:
        scalar_da = ds[LN_RISK_SCALAR]
        if output_underlying_version.scenario is None:
            scalar_da = scalar_da.sel(scenario=0, drop=True)
        past_underlying = past_da - scalar_da
        past_underlying = past_underlying.sel(year_id=modeling_args.years.past_years)

        past_underlying_save_path = FHSFileSpec(
            output_underlying_version.with_epoch("past"),
            f"{modeling_args.acause}{sex_name}.nc",
        )
        save_xr_scenario(
            past_underlying, past_underlying_save_path, metric="rate", space="identity"
        )

    fxeff = _make_fixed_effects(modeling_args=modeling_args, ds=ds, reis=reis)

    weight_decay09_causes = get_strategy_causes(
        modeling_args=modeling_args, gk_strategy_id=CauseConstants.WEIGHT_DECAY09_STRATEGY_ID
    )

    weight_decay = 0.9 if modeling_args.acause in weight_decay09_causes else 0

    gkmodel, params = _fit_gkmodel(
        modeling_args=modeling_args,
        ds=ds,
        fxeff=fxeff,
        reis=reis,
        weight_decay=weight_decay,
    )

    # if predicting at subnationals but fitting on national only, need to add
    # subnats back into the data and apply coefficients for the level 3 to
    # their level 4 children
    if (
        not modeling_args.fit_on_subnational
        and modeling_args.subnational
        and len(locs_subnational) > 0
    ):
        ds = xr.concat([ds, ds_subnat], dim=DimensionConstants.LOCATION_ID)
        gkmodel.dataset = ds
        gkmodel.coefficients = _apply_location_parent_coefs_to_children(params=params, ds=ds)

    preds = gkmodel.predict()

    cap_causes = get_strategy_causes(
        modeling_args=modeling_args, gk_strategy_id=CauseConstants.GK_CAP_STRATEGY_ID
    )

    # cap maternal_hiv and drug causes
    if modeling_args.acause in cap_causes:
        past_mortality = (
            open_xr_scenario(
                FHSFileSpec(
                    version_args.versions.get(past_or_future="past", stage="death"),
                    f"{modeling_args.acause}.nc",
                )
            )["mean"]
            .sel(sex_id=modeling_args.sex_id, year_id=modeling_args.years.past_years)
            .sel(acause=modeling_args.acause, drop=True)
        )
        # data is in log space - capping happens in normal space
        last_past_year = invlog_with_offset(preds).sel(year_id=modeling_args.years.past_end)
        preds = log_with_offset(
            cap_forecasts(
                forecast=invlog_with_offset(preds),
                past=past_mortality,
                quantiles=Quantiles(0.01, 0.99),
                last_past_year=last_past_year,
            )
        )

    # save future results without scalar
    if SAVE_FUTURE:
        scalar_da = ds[LN_RISK_SCALAR]
        if output_underlying_version.scenario is None:
            scalar_da = scalar_da.sel(scenario=0)
            scalar_da = expand_dimensions(scalar_da, scenario=[-1, 0, 1])
        future_underlying = preds - scalar_da

        future_underlying_save_path = FHSFileSpec(
            output_underlying_version, f"{modeling_args.acause}{sex_name}.nc"
        )
        save_xr_scenario(
            future_underlying, future_underlying_save_path, metric="rate", space="log"
        )

    preds_model_path = FHSFileSpec(output_version, f"{modeling_args.acause}{sex_name}.nc")
    write_cod_forecast(data=preds, file_spec=preds_model_path)
    params = (
        xr.DataArray([1], dims=[DimensionConstants.SEX_ID], coords=[[modeling_args.sex_id]])
        * params
    )
    params_betas_path = FHSFileSpec(
        output_version, sub_path=("betas",), filename=f"{modeling_args.acause}{sex_name}.nc"
    )
    write_cod_betas(
        model_params=params,
        file_spec=params_betas_path,
        acause=modeling_args.acause,
    )


def _model_ntd(
    modeling_args: CauseSpecificModelingArguments,
    sex_name: str,
    output_version: VersionMetadata,
    ds: xr.Dataset,
    scenarios: Optional[Iterable[int]],
) -> None:
    """Special case modeling for ntd_nema.

    It isn't stable enough to have a regular gk model run on it.
    """
    logger.info(
        "Running repeat_last_year model",
        bindings=dict(acause=modeling_args.acause, sex_name=sex_name),
    )

    output_underlying_version = output_version.append_version_suffix("_underlying")

    if SAVE_PAST:
        past_underlying = ds["y"].copy()
        past_underlying = past_underlying.sel(year_id=modeling_args.years.past_years)
        past_underlying.name = "value"
        past_underlying.coords[DimensionConstants.SEX_ID] = modeling_args.sex_id

        past_underlying_save_path = FHSFileSpec(
            output_underlying_version.with_epoch("past"),
            f"{modeling_args.acause}{sex_name}.nc",
        )
        save_xr_scenario(
            past_underlying, past_underlying_save_path, metric="rate", space="identity"
        )

    preds = _repeat_last_year(ds=ds, years=modeling_args.years, draws=modeling_args.draws)

    if output_underlying_version.scenario is None:  # "Legacy" multi-scenario mode.
        scenarios = scenarios or ScenarioConstants.SCENARIOS
        preds = expand_dimensions(preds, scenario=scenarios)

    preds.coords[DimensionConstants.SEX_ID] = modeling_args.sex_id

    if SAVE_FUTURE:
        future_underlying_save_path = FHSFileSpec(
            output_underlying_version, f"{modeling_args.acause}{sex_name}.nc"
        )
        save_xr_scenario(preds, future_underlying_save_path, metric="rate", space="log")

    preds_model_path = FHSFileSpec(output_version, f"{modeling_args.acause}{sex_name}.nc")
    write_cod_forecast(data=preds, file_spec=preds_model_path)
    sys.exit()


def _make_fixed_effects(
    modeling_args: CauseSpecificModelingArguments, ds: xr.Dataset, reis: List[str]
) -> Dict:
    """Generate the correct fixed effects to use for the GK Model."""
    # Don't put a spline on SDI for causes that aren't modeled for very many
    # locations.
    nlocs = len(ds.location_id.values)
    if (nlocs > NLOCS_CUTOFF) and modeling_args.spline:
        fxeff = {
            BETA_GLOBAL: [
                (INTERCEPT, 0),
                (SDI_PART1, 0),
                (SDI_PART2, 0),
                (TIME_VAR, 0),
            ]
        }
    else:
        fxeff = {
            BETA_GLOBAL: [
                (INTERCEPT, 0),
                (SDI, 0),
                (TIME_VAR, 0),
            ]
        }

    interaction_causes = get_strategy_causes(
        modeling_args=modeling_args, gk_strategy_id=CauseConstants.INTERACTION_STRATEGY_ID
    )

    if modeling_args.acause in interaction_causes:
        fxeff[BETA_GLOBAL] += [(SDI_TIME, 0)]

    no_fixed_effect_risk_cause = get_strategy_causes(
        modeling_args=modeling_args,
        gk_strategy_id=CauseConstants.NO_FIXED_EFFECT_RISK_STRATEGY_ID,
    )

    if not (
        modeling_args.acause == no_fixed_effect_risk_cause[0] and modeling_args.sex_id == 1
    ):
        for r in reis:
            fxeff[BETA_GLOBAL] += [(r, 1)]

    road_traffic_causes = get_strategy_causes(
        modeling_args=modeling_args, gk_strategy_id=CauseConstants.ROAD_TRAFFIC_STRATEGY_ID
    )

    if modeling_args.acause in road_traffic_causes:
        # drop time trend
        for cov in [
            (TIME_VAR, 0),
        ]:
            try:
                fxeff[BETA_GLOBAL].remove(cov)
            except ValueError:
                pass

    cause_maternal_hiv = get_strategy_causes(
        modeling_args=modeling_args, gk_strategy_id=CauseConstants.MATERNAL_HIV_STRATEGY_ID
    )
    if modeling_args.acause == cause_maternal_hiv[0]:
        fxeff[BETA_GLOBAL] += [("hiv", 1)]
        fxeff[BETA_GLOBAL].remove((SDI, 0))

    if modeling_args.acause == "malaria":
        fxeff[BETA_GLOBAL].remove((SDI, 0))
        fxeff[BETA_GLOBAL].remove((TIME_VAR, 0))

    vaccine_causes = get_strategy_causes(
        modeling_args=modeling_args, gk_strategy_id=CauseConstants.VACCINE_STRATEGY_ID
    )

    if modeling_args.acause in vaccine_causes:
        try:
            fxeff[BETA_GLOBAL].remove((SDI_PART1, 0))
            fxeff[BETA_GLOBAL].remove((SDI_PART2, 0))
        except ValueError:
            fxeff[BETA_GLOBAL].remove((SDI, 0))

    maternal_causes = get_strategy_causes(
        modeling_args=modeling_args, gk_strategy_id=CauseConstants.MATERNAL_CAUSES_STRATEGY_ID
    )

    parent_maternal = get_strategy_causes(
        modeling_args=modeling_args, gk_strategy_id=CauseConstants.PARENT_MATERNAL_STRATEGY_ID
    )

    if modeling_args.acause in maternal_causes:
        if modeling_args.acause != parent_maternal[0]:
            fxeff[BETA_GLOBAL].append(("asfr", 1))

    notime_maternal_causes = get_strategy_causes(
        modeling_args=modeling_args, gk_strategy_id=CauseConstants.MATERNAL_NOTIME_STRATEGY_ID
    )
    nosdi_maternal_causes = get_strategy_causes(
        modeling_args=modeling_args, gk_strategy_id=CauseConstants.MATERNAL_NOSDI_STRATEGY_ID
    )

    if modeling_args.acause in notime_maternal_causes:
        fxeff[BETA_GLOBAL].remove((TIME_VAR, 0))

    if modeling_args.acause in nosdi_maternal_causes:
        fxeff[BETA_GLOBAL].remove((SDI, 0))

    return fxeff


def _fit_gkmodel(
    modeling_args: CauseSpecificModelingArguments,
    ds: xr.Dataset,
    fxeff: Dict,
    reis: List[str],
    weight_decay: float,
) -> Tuple[GKModel, xr.Dataset]:
    gkmodel = _first_gk_model(
        modeling_args=modeling_args, ds=ds, fxeff=fxeff, weight_decay=weight_decay
    )
    # Try fitting with all the specified fixed effects. If a convergence
    # error is given, drop all but the location-age intercepts, and set time
    # to be a global variable. If the model technically converges but any of
    # the variables have coefficients with unreasonably high standard
    # deviations, drop said variables and try again.
    try:
        params = gkmodel.fit()
        cov_to_drop = _covariates_to_drop(
            modeling_args=modeling_args, reis=reis, params=params
        )
        if cov_to_drop:
            logger.info("refitting after dropping covariates")
            gkmodel = _second_gk_model(
                modeling_args=modeling_args,
                ds=ds,
                fxeff=fxeff,
                weight_decay=weight_decay,
                cov_to_drop=cov_to_drop,
            )
            params = gkmodel.fit()
    except (ConvergenceError, CholmodNotPositiveDefiniteError):
        logger.info("refitting after dropping sdi and all covs")
        # drop sdi and covariates if model still doesn't converge
        gkmodel = _third_gk_model(
            modeling_args=modeling_args, ds=ds, fxeff=fxeff, weight_decay=weight_decay
        )
        params = gkmodel.fit()

    # apply post-hoc RELU if the dictionary defined in settings is non-empty
    if RELU:
        params = post_hoc_relu(params=params, cov_dict=RELU)
    return gkmodel, params


def _first_gk_model(
    modeling_args: CauseSpecificModelingArguments,
    ds: xr.Dataset,
    fxeff: Dict,
    weight_decay: float,
) -> GKModel:
    """Set up our ideal model."""
    raneff = _make_random_effects(modeling_args)
    constants = _make_constants(modeling_args)

    gkmodel = GKModel(
        ds,
        years=modeling_args.years,
        fixed_effects=fxeff,
        random_effects=raneff,
        draws=modeling_args.draws,
        constants=constants,
        y=Y,
        omega_amp=OMEGA_AMP,
        weight_decay=weight_decay,
        seed=modeling_args.seed,
    )

    return gkmodel


def get_strategy_causes(
    modeling_args: CauseSpecificModelingArguments,
    gk_strategy_id: int,
) -> List[str]:
    """Return cause list based on the strategy id."""
    with db_session.create_db_session() as session:
        causes = strategy.get_cause_set(
            session=session,
            strategy_id=gk_strategy_id,
            gbd_round_id=modeling_args.gbd_round_id,
        ).acause.values.tolist()

    return causes


def _make_random_effects(modeling_args: CauseSpecificModelingArguments) -> Dict:
    """Set up the random effects for the ideal model."""
    raneff = {
        GAMMA_LOCATION_AGE: [INTERCEPT],
        GAMMA_AGE: [TIME_VAR],
    }

    road_traffic_causes = get_strategy_causes(
        modeling_args=modeling_args, gk_strategy_id=CauseConstants.ROAD_TRAFFIC_STRATEGY_ID
    )

    if modeling_args.acause in road_traffic_causes:
        # drop time if present in gamma_age
        raneff = {GAMMA_LOCATION_AGE: [INTERCEPT]}

    notime_maternal_causes = get_strategy_causes(
        modeling_args=modeling_args, gk_strategy_id=CauseConstants.MATERNAL_NOTIME_STRATEGY_ID
    )

    if modeling_args.acause in notime_maternal_causes:
        raneff[GAMMA_AGE].remove(TIME_VAR)
    if modeling_args.acause == "malaria":
        raneff = {
            GAMMA_LOCATION_AGE: [INTERCEPT],
            GAMMA_LOCATION: [TIME_VAR],
        }

    return raneff


def _make_constants(modeling_args: CauseSpecificModelingArguments) -> List[str]:
    """Set up the constants for the ideal model."""
    parent_maternal = get_strategy_causes(
        modeling_args=modeling_args, gk_strategy_id=CauseConstants.PARENT_MATERNAL_STRATEGY_ID
    )
    if modeling_args.acause == parent_maternal[0]:
        constants = [LN_RISK_SCALAR, ASFR]
    else:
        constants = [LN_RISK_SCALAR]
    return constants


def _second_gk_model(
    modeling_args: CauseSpecificModelingArguments,
    ds: xr.Dataset,
    fxeff: Dict,
    weight_decay: float,
    cov_to_drop: List[str],
) -> GKModel:
    """Set up the GK Model, dropping some number of covariates.

    Intended for the case in which some number of covariates produced weird results.
    """
    for covariate in cov_to_drop:
        if covariate in [
            SDI,
            SDI_PART1,
            SDI_PART2,
        ]:
            fxeff[BETA_GLOBAL].remove((covariate, 0))
        else:
            fxeff[BETA_GLOBAL].remove((covariate, 1))

    raneff = {GAMMA_LOCATION_AGE: [INTERCEPT]}
    gkmodel = GKModel(
        ds,
        years=modeling_args.years,
        fixed_effects=fxeff,
        random_effects=raneff,
        draws=modeling_args.draws,
        constants=[LN_RISK_SCALAR],
        y=Y,
        omega_amp=OMEGA_AMP,
        weight_decay=weight_decay,
        seed=modeling_args.seed,
    )
    return gkmodel


def _third_gk_model(
    modeling_args: CauseSpecificModelingArguments,
    ds: xr.Dataset,
    fxeff: Dict,
    weight_decay: float,
) -> GKModel:
    """Set up the GK model for the case in which the original model didn't converge."""
    raneff = {GAMMA_LOCATION_AGE: [INTERCEPT]}

    road_traffic_causes = get_strategy_causes(
        modeling_args=modeling_args, gk_strategy_id=CauseConstants.ROAD_TRAFFIC_STRATEGY_ID
    )

    if modeling_args.acause in road_traffic_causes:
        fxeff[BETA_GLOBAL] = [(INTERCEPT, 0)]
    else:
        fxeff[BETA_GLOBAL] = [
            (TIME_VAR, 0),
            (INTERCEPT, 0),
        ]
    gkmodel = GKModel(
        ds,
        years=modeling_args.years,
        fixed_effects=fxeff,
        random_effects=raneff,
        draws=modeling_args.draws,
        constants=[LN_RISK_SCALAR],
        y=Y,
        omega_amp=OMEGA_AMP,
        weight_decay=weight_decay,
        seed=modeling_args.seed,
    )
    return gkmodel


def _covariates_to_drop(
    modeling_args: CauseSpecificModelingArguments, reis: List[str], params: xr.Dataset
) -> List[str]:
    """Determine which covariates during model fitting need to be dropped."""
    cov_list = [SDI, SDI_PART1, SDI_PART2] + reis

    maternal_causes = get_strategy_causes(
        modeling_args=modeling_args, gk_strategy_id=CauseConstants.MATERNAL_CAUSES_STRATEGY_ID
    )

    if modeling_args.acause in maternal_causes:
        cov_list += ["asfr"]

    return [c for c in cov_list if _is_cov_bad(c, params)]


def _is_cov_bad(covariate: str, params: xr.Dataset) -> bool:
    """Determine if a covariate produced weird results when fitting.

    Currently we test if the SD is NaN or too large.
    """
    try:
        sd = params[BETA_GLOBAL].sel(cov=covariate).std()
        median_coeff = params[BETA_GLOBAL].sel(cov=covariate).median()
    except (ValueError, IndexError, KeyError):
        logger.warning(f"Something went wrong processing covariate {covariate}; ignoring.")
        return False

    sd_too_large = abs(sd / median_coeff) > CUTOFF
    return np.isnan(sd) or sd_too_large


def _repeat_last_year(ds: xr.Dataset, years: YearRange, draws: int) -> xr.DataArray:
    """Run a model for mortality using the RepeatLastYear model.

    Args:
        ds (xr.Dataset): Dataset containing y as the response variable
            and time_var as the time variable
        years (YearRange): past and forecasted years (e.g. 1990:2017:2040)
        draws (int): number of draws to return (will all be identical)

    Returns:
        xr.DataArray: the past and projected values (held constant)
    """
    logger.info("running repeat_last_year")

    rly = repeat_last_year.RepeatLastYear(ds["y"], years=years)
    rly.fit()
    preds = expand_dimensions(rly.predict(), draw=np.arange(draws))
    return preds


def _get_addcovs(modeling_args: CauseSpecificModelingArguments, acause: str) -> List[str]:
    """Returns a list of the additional covariates associated with a given acause.

    Args:
        acause (str): Cause to find covariates for

    Returns:
        List[str]: The non-sev covariates associated with the input acause
    """
    addcovs = []

    maternal_causes = get_strategy_causes(
        modeling_args=modeling_args, gk_strategy_id=CauseConstants.MATERNAL_CAUSES_STRATEGY_ID
    )
    cause_maternal_hiv = get_strategy_causes(
        modeling_args=modeling_args, gk_strategy_id=CauseConstants.MATERNAL_HIV_STRATEGY_ID
    )

    if acause in maternal_causes:
        addcovs.append("asfr")
    if acause == cause_maternal_hiv[0]:
        addcovs.append("hiv")
    logger.info("Additional covariates", bindings=dict(addcovs=addcovs))

    return addcovs


def _get_sex_name(sex_id: int) -> str:
    """Gets the sex name associated with a given sex_id.

    Args:
        sex_id (int): Numeric sex ID.

    Returns:
        str: '_male' for 1 or '_female' for 2.

    Raises:
        ValueError: If ``sex_id`` is invalid.
    """
    try:
        return "_" + SexConstants.SEX_DICT[sex_id]
    except KeyError:
        raise ValueError(f"sex_id must be {SexConstants.SEX_DICT.keys()}; {sex_id} given")


def _apply_location_parent_coefs_to_children(params: xr.Dataset, ds: xr.Dataset) -> xr.Dataset:
    """Apply location parent coefs to children.

    Adds coefficient estimates for subnational locations to a dataset of
    national coefficients by setting the level 4 subnational values to have
    the same values as their level 3 national parent.

    Args:
        params (xr.Dataset): coefficient/parameter dataset output from the
            GKModel class.
        ds (xr.Dataset): cause of death dataset output from load_cod_dataset,
            which includes the level 4 subnational locations in need of their
            parent parameters.

    Returns:
        xr.Dataset: coefficient/parameter dataset with values for all locations in ds.

    Raises:
        ValueError: If no subnationals in ``ds`` or if missing level 3 parent
            locations in the supplied cod dataset.
    """
    if not np.isin(LocationConstants.SUBNATIONAL_LEVEL, ds["level"].values):
        raise ValueError("No subnationals in ds.")

    # get parents and location_ids of level 4 locations present in ds
    locs = (
        ds["parent_id"]
        .where(ds.level == LocationConstants.SUBNATIONAL_LEVEL)
        .to_dataframe("parent_id")
        .reset_index()
        .dropna()
    )
    parents = params[DimensionConstants.LOCATION_ID].values
    parents_with_children = locs.parent_id.unique()

    if not np.isin(parents_with_children, parents).all():
        raise ValueError("Missing level 3 parent locations in the supplied cod dataset.")

    # replicate the parent parameters for all children, so that the parent
    # location random effects get applied to the children in GKModel predict
    add_params = [params]
    for parent in parents_with_children:
        parent_params = params.sel(location_id=int(parent), drop=True)
        children = locs.query("parent_id == @parent").location_id.values
        child_params = expand_dimensions(parent_params, location_id=children)
        add_params.append(child_params)

    add_params = xr.concat(add_params, dim=DimensionConstants.LOCATION_ID)

    return add_params


def write_cod_forecast(data: xr.DataArray, file_spec: FHSFileSpec) -> None:
    """Save a cod model and assert that it has the appropriate dimensions.

    Args:
        data (xr.DataArray): Dataarray with cod mortality rate.
        file_spec (FHSFileSpec): the spec where data should be saved (includes path and
            scenario ID).

    Raises:
        ValueError: If data is missing dimensions.
    """
    keys = ["year_id", "age_group_id", "location_id", "sex_id", "draw"]
    missing = [k for k in keys if k not in list(data.coords.keys())]
    if len(missing) > 0:
        raise ValueError(f"Data is missing dimensions: {missing}.")
    save_xr_scenario(
        xr_obj=data,
        file_spec=file_spec,
        metric="rate",
        space="log",
    )


def write_cod_betas(
    model_params: xr.Dataset,
    file_spec: FHSFileSpec,
    acause: str,
    save_draws: bool = True,
) -> None:
    """Write the betas from a gk cod model run.

    Args:
        model_params (xr.Dataset): Xarray Dataset with covariate and draw information.
        file_spec (FHSFileSpec): the path to save betas into, as a scenario-aware FHSFileSpec.
        acause (str): The string of the acause to place saved results in correct location.
        save_draws (bool): Whether to save the estimated regression coefficients
            (means) or samples from their posterior distribution (draws).
    """
    model_params *= xr.DataArray([1], dims=["acause"], coords=[[acause]])
    if not save_draws:
        model_params = model_params.mean("draw")
    save_xr_scenario(
        xr_obj=model_params,
        file_spec=file_spec,
        metric="rate",
        space="log",
    )
