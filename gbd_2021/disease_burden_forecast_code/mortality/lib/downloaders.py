"""Functions used by run_cod_model.py to import past data.

Including cause-specific mx data, SEVS, scalars, and covariates.
"""

from typing import Dict, Iterable, List, Optional, Union

import fhs_lib_database_interface.lib.query.risk as query_risk
import numpy as np
import xarray as xr
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_database_interface.lib import db_session
from fhs_lib_database_interface.lib.constants import (
    CauseConstants,
    CauseRiskPairConstants,
    DimensionConstants,
    RiskConstants,
    ScenarioConstants,
)
from fhs_lib_database_interface.lib.query import age, location
from fhs_lib_database_interface.lib.query.cause import get_acause, get_cause_id
from fhs_lib_database_interface.lib.strategy_set import strategy
from fhs_lib_file_interface.lib.version_metadata import FHSFileSpec
from fhs_lib_file_interface.lib.versioning import Versions
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr_scenario
from tiny_structured_logger.lib import fhs_logging

from fhs_pipeline_mortality.lib.config_dataclasses import CauseSpecificModelingArguments

logger = fhs_logging.get_logger()

DIRECT_MODELED_PAFS = ["drugs_illicit_direct", "unsafe_sex"]
SCALAR_CAP = 1000000.0
FLOOR = 1e-28


def validate_cause(
    acause: str,
    gbd_round_id: int,
) -> None:
    """Check whether a cause is in the cod cause list.

    Throws an assertion error if not.

    Args:
        acause (str): Cause to check whether it is a modeled cod acause.
        gbd_round_id (int): round of GBD to pull cause hierarchy from

    Raises:
        ValueError: if acause not in causes.
    """
    logger.debug("Validating cause", bindings=dict(acause=acause))

    with db_session.create_db_session() as session:
        causes = strategy.get_cause_set(
            session=session,
            strategy_id=CauseConstants.FATAL_GK_STRATEGY_ID,
            gbd_round_id=gbd_round_id,
        ).acause.values

        maternal_cause = strategy.get_cause_set(
            session=session,
            strategy_id=CauseConstants.PARENT_MATERNAL_STRATEGY_ID,
            gbd_round_id=gbd_round_id,
        ).acause.values

        ckd_cause = strategy.get_cause_set(
            session=session,
            strategy_id=CauseConstants.CKD_STRATEGY_ID,
            gbd_round_id=gbd_round_id,
        ).acause.values

    causes = np.append(causes, maternal_cause)

    causes = np.append(causes, ckd_cause)

    if acause not in causes:
        raise ValueError("acause must be a valid modeled COD acause")


def validate_version(versions: Versions, past_or_future: str, stage: str) -> None:
    """Check whether or not the version to load from exists.

    Throws an assertion error if not.

    Args:
        versions (Versions): the versions object to query a version for
        stage (str): the metric to check the version under (e.g. scalar)
        past_or_future (str): past or future of the version

    Raises:
        ValueError: if version not valid.
    """
    version_path = versions.get(past_or_future=past_or_future, stage=stage).data_path()

    if not version_path.exists():
        raise ValueError(f"Version {version_path} is not valid - path does not exist.")


def empty_dem_xarray(
    gbd_round_id: int,
    locs: Union[int, List[int]],
    sex_ids: Union[int, List[int]],
    val: float = 0,
    draws: int = 100,
    start: int = 1990,
    end: int = 2040,
    scenarios: List[int] = ScenarioConstants.SCENARIOS,
) -> xr.DataArray:
    """Build an empty xarray which has all dimensions required for modeling.

    i.e. location_id, age_group_id, year_id, sex_id, draw, & scenario.

    Args:
        gbd_round_id (int): round of GBD to build data array around
        locs (Union[int, List[int]]): list or array of locations to use as location_id
            coordinates
        sex_ids (Union[int, List[int]]): the sex_id values to create
        val (float): value to fill the array with, default 0.
        draws (int): what the length of the draw dimension should be.
        start (int): the beginning of the year_id dimension, default 1990
        end (int): the end of the year_id dimension, default 2040
        scenarios (list): the scenarios to create

    Returns:
        DataArray: Six-dimensional data array.
    """
    demog = dict(
        age_group_id=age.get_ages(gbd_round_id=gbd_round_id).age_group_id.values,
        year_id=np.arange(start, end + 1),
        location_id=locs,
        scenario=scenarios,
        draw=np.arange(draws),
        sex_id=sex_ids,
    )
    logger.debug("Building empty demographic array", bindings=demog)

    dims = [
        DimensionConstants.LOCATION_ID,
        DimensionConstants.AGE_GROUP_ID,
        DimensionConstants.YEAR_ID,
        DimensionConstants.SEX_ID,
        DimensionConstants.DRAW,
        DimensionConstants.SCENARIO,
    ]
    size = [len(demog[x]) for x in dims]
    vals = np.ones(size) * val
    dem_array = xr.DataArray(vals, coords=demog, dims=dims)
    return dem_array


def replace_scenario_dim(da: xr.DataArray, scenarios: List[int]) -> xr.DataArray:
    """Discard nonreference scenarios, replacing them with the reference one."""
    if DimensionConstants.SCENARIO in da.dims:
        da = da.sel(scenario=ScenarioConstants.REFERENCE_SCENARIO_COORD, drop=True)
    return expand_dimensions(da, scenario=scenarios)


def enforce_scenario_dim(
    modeling_args: CauseSpecificModelingArguments,
    covariate: Optional[str],
    scenarios: List[int],
    da: xr.DataArray,
) -> xr.DataArray:
    """Transform `da` so that it has the desired scenarios, enforcing "drivers_at_reference".

    Transform `da` so that it has the desired scenarios, enforcing the "drivers_at_reference"
    policy from `modeling_args`. When using drivers_at_reference, the reference scenario may be
    duplicated in place of the nonreference scenarios, depending on the covariate.
    """
    if (
        modeling_args.drivers_at_reference and covariate != "asfr"
    ) or DimensionConstants.SCENARIO not in da.dims:
        return replace_scenario_dim(da, scenarios)
    else:
        return da.sel(scenario=scenarios)


def acause_has_scalar(gbd_round_id: int, acause: str) -> bool:
    """True iff the given acause "has scalar data" in the gbd_round_id."""
    with db_session.create_db_session() as session:
        causes_with_scalars = strategy.get_cause_risk_pair_set(
            session=session,
            strategy_id=CauseRiskPairConstants.CALC_PAF_STRATEGY_ID,
            gbd_round_id=gbd_round_id,
        )

    acauses_with_scalars = causes_with_scalars.cause_id.map(get_acause).unique()

    # NOTE: Change the list of causes-with-scalars from GBD round ID 6 to GBD round ID 7.
    acauses_with_scalars_list = acauses_with_scalars.tolist()

    acause_has_scalar = acause in acauses_with_scalars_list
    return acause_has_scalar


def load_scalar(
    modeling_args: CauseSpecificModelingArguments,
    versions: Versions,
    scenarios: List[int],
    log: bool = True,
) -> xr.DataArray:
    """Load scalar scenario data as an xarray.

    Args:
        modeling_args (CauseSpecificModelingArguments): dataclass containing
            cause specific modeling arguments
        versions (Versions): the versions object to query a version for
        scenarios (List[int]): the scenarios you want to subset scalar to
        log (bool): Whether to take the natural log of the values, default True

    Returns:
        DataArray: array with scalar data for acause
    """
    logger.debug(
        "Loading scalars for a cause-sex",
        bindings=dict(acause=modeling_args.acause, sex_id=modeling_args.sex_id),
    )

    national_only = not modeling_args.subnational
    locs = location.get_location_set(
        gbd_round_id=modeling_args.gbd_round_id, national_only=national_only
    )[DimensionConstants.LOCATION_ID].tolist()

    validate_cause(acause=modeling_args.acause, gbd_round_id=modeling_args.gbd_round_id)

    future_file_spec = FHSFileSpec(
        versions.get(past_or_future="future", stage="scalar"), f"{modeling_args.acause}.nc"
    )
    past_file_spec = FHSFileSpec(
        versions.get(past_or_future="past", stage="scalar"), f"{modeling_args.acause}.nc"
    )

    if acause_has_scalar(modeling_args.gbd_round_id, modeling_args.acause):
        future = open_xr_scenario(file_spec=future_file_spec).sel(
            sex_id=modeling_args.sex_id,
            year_id=modeling_args.years.forecast_years,
            location_id=locs,
        )
        future = enforce_scenario_dim(modeling_args, None, scenarios, future)
        past = open_xr_scenario(file_spec=past_file_spec).sel(
            sex_id=modeling_args.sex_id,
            year_id=modeling_args.years.past_years,
            location_id=locs,
        )
        past = replace_scenario_dim(past, scenarios)
        past = resample(past, modeling_args.draws)
        future = resample(future, modeling_args.draws)

        da = xr.concat([past, future], dim=DimensionConstants.YEAR_ID)
    else:
        da = empty_dem_xarray(
            gbd_round_id=modeling_args.gbd_round_id,
            locs=locs,
            sex_ids=[modeling_args.sex_id],
            val=1.0,
            draws=modeling_args.draws,
            start=modeling_args.years.past_start,
            end=modeling_args.years.forecast_end,
            scenarios=scenarios,
        )

    da = _drop_scenario_in_single_scenario_mode(da, scenarios)

    da.name = "risk_scalar"
    da = da.where(da <= SCALAR_CAP).fillna(SCALAR_CAP)
    if log:
        da = np.log(da)
        da.name = "ln_risk_scalar"
    return da


def load_sdi(
    modeling_args: CauseSpecificModelingArguments,
    versions: Versions,
    scenarios: List[int],
    log: bool = False,
) -> xr.DataArray:
    """Loads and returns sociodemographic index.

    Args:
        modeling_args (CauseSpecificModelingArguments): dataclass containing
            cause specific modeling arguments
        versions (Versions): the versions object to query a version for
        scenarios (List[int]): the scenarios you want to subset SDI to
        log (bool): whether to take the log of SDI, default False

    Returns:
        DataArray: array with sdi information.
    """
    logger.debug("Loading SDI")

    national_only = not modeling_args.subnational
    locs = location.get_location_set(
        gbd_round_id=modeling_args.gbd_round_id, national_only=national_only
    )[DimensionConstants.LOCATION_ID].tolist()

    future_sdi_spec = FHSFileSpec(versions.get(past_or_future="future", stage="sdi"), "sdi.nc")
    future_da = open_xr_scenario(file_spec=future_sdi_spec).sel(
        year_id=modeling_args.years.forecast_years,
        location_id=locs,
    )
    if DimensionConstants.SCENARIO in future_da.dims:
        future_da = future_da.sel(scenario=scenarios)

    past_sdi_spec = FHSFileSpec(versions.get(past_or_future="past", stage="sdi"), "sdi.nc")
    past_da = open_xr_scenario(file_spec=past_sdi_spec).sel(
        year_id=modeling_args.years.past_years, location_id=locs
    )

    past_da = replace_scenario_dim(past_da, scenarios)

    past_da = _drop_scenario_in_single_scenario_mode(past_da, scenarios)

    da = xr.concat([past_da, future_da], dim="year_id")
    da = resample(data=da, num_of_draws=modeling_args.draws)
    da.name = "sdi"

    if log:
        da = np.log(da)
        da.name = "ln_sdi"

    return da


def load_cod_data(
    modeling_args: CauseSpecificModelingArguments,
    versions: Versions,
    past_or_future: str,
    stage: str,
    log: bool = True,
) -> xr.DataArray:
    """Load in cause-sex specific mortality rate.

    Args:
        modeling_args (CauseSpecificModelingArguments): dataclass containing
            cause specific modeling arguments
        versions (Versions): the versions object to query a version for
        past_or_future (str): whether to use past or future
        stage (str): the stage to load for
        log (bool): Whether to take the natural log of the mortality values.

    Returns:
        DataArray: array with acause-sex specific death rate information.
    """
    logger.debug(
        "Loading cause-sex-specific mortality",
        bindings=dict(acause=modeling_args.acause, sex_id=modeling_args.sex_id),
    )

    validate_cause(
        acause=modeling_args.acause,
        gbd_round_id=versions.get_effective_gbd_round_id(past_or_future, stage),
    )

    cod_file_spec = FHSFileSpec(
        versions.get(past_or_future=past_or_future, stage=stage), f"{modeling_args.acause}.nc"
    )
    da = open_xr_scenario(file_spec=cod_file_spec)

    if DimensionConstants.ACAUSE in da.dims:
        da = da.sel(acause=modeling_args.acause, drop=True)

    # Make an array of means that replicates it for as many as there are draws in the raw data.
    # leave the last year off for draws
    mean_array = da["mean"].loc[{"year_id": modeling_args.years.past_years[:-1]}]
    # Note: Some legacy data does in fact have draws in the mean variable. For those cases, we
    # use the draws directly.
    # In normal cases, we will expand out the draw dim on the means, to combine them
    # with draw-level data from the last past year.
    if "draw" not in mean_array.dims:
        mean_array = mean_array.expand_dims(draw=range(modeling_args.draws))

    draw_array = da["value"].loc[{"year_id": [modeling_args.years.past_end]}]
    draw_array = resample(data=draw_array, num_of_draws=modeling_args.draws)
    da_draw = xr.concat([mean_array, draw_array], dim="year_id")

    locdiv = ("draw", "age_group_id", "year_id", "sex_id")
    locidx = np.where(~(da_draw == 0).all(locdiv))[0]
    locs = da_draw.location_id.values[locidx]

    agediv = ("draw", "location_id", "year_id", "sex_id")
    ageidx = np.where(~(da_draw == 0).all(agediv))[0]
    ages = da_draw.age_group_id.values[ageidx]

    demdict = dict(location_id=locs, age_group_id=ages, sex_id=modeling_args.sex_id)

    if modeling_args.acause == "ntd_nema":
        da_draw += FLOOR

    if log:
        da_draw = np.log(da_draw)

    da_draw_sub = da_draw.loc[demdict]
    return da_draw_sub


def load_cov(
    modeling_args: CauseSpecificModelingArguments,
    versions: Versions,
    scenarios: List[int],
    cov: str,
    log: bool,
) -> xr.DataArray:
    """Read a covariate and format for cod modeling.

    Fills in the country-level covariates for subnational locations if applicable. Takes
    natural log of values if applicable.

    Args:
        modeling_args (CauseSpecificModelingArguments): General modeling arguments.
        versions (Versions): Catalog of versions having entries for the given `cov`.
        scenarios (List[int]): Select these scenarios from future; expand past data to match.
            Typically, just let these be the same scenarios that are in the future data.
        cov (str): Covariate name to load.
        log (bool): Whether to take the natural log of the covariate values.

    Returns:
        DataArrray: array with covariate information loaded and formatted
    """
    logger.debug(
        "Loading sex specific covariate data",
        bindings=dict(sex_id=modeling_args.sex_id, cov=cov),
    )

    national_only = not modeling_args.subnational
    locs = location.get_location_set(
        gbd_round_id=modeling_args.gbd_round_id, national_only=national_only
    )[DimensionConstants.LOCATION_ID].tolist()

    cov_file = "mort_rate" if cov == "hiv" else cov
    future_cov_file_spec = FHSFileSpec(
        versions.get(past_or_future="future", stage=cov), f"{cov_file}.nc"
    )

    if cov in versions["past"].keys():
        past_cov_file_spec = FHSFileSpec(
            versions.get(past_or_future="past", stage=cov), f"{cov_file}.nc"
        )

        raw_past = open_xr_scenario(file_spec=past_cov_file_spec).sel(
            year_id=modeling_args.years.past_years, location_id=locs
        )

        raw_past = replace_scenario_dim(raw_past, scenarios)

        raw_future = open_xr_scenario(file_spec=future_cov_file_spec).sel(
            year_id=modeling_args.years.forecast_years, location_id=locs
        )

        raw_future = enforce_scenario_dim(modeling_args, cov, scenarios, raw_future)

        raw = xr.concat([raw_past, raw_future], dim="year_id")
    else:
        # don't select locations until later because some covs only read in
        # from the past may not have all locations?
        raw = open_xr_scenario(file_spec=future_cov_file_spec).sel(
            year_id=modeling_args.years.years
        )

    raw = resample(data=raw, num_of_draws=modeling_args.draws)

    badkeys = np.setdiff1d(list(raw.coords.keys()), list(raw.coords.indexes.keys()))
    for k in badkeys:
        raw = raw.drop_vars(k)

    raw = _remove_aggregate_age_groups(raw)
    raw = _project_sex_id(raw=raw, sex_id=modeling_args.sex_id)

    if "location_id" in raw.coords.keys():
        raw = raw.loc[{"location_id": np.intersect1d(locs, raw["location_id"].values)}]

    # NOTE this was not previously enforcing the scenario dim in the case where we're not using
    # drivers_at_reference.
    raw = enforce_scenario_dim(modeling_args, cov, scenarios, raw)

    raw = _drop_scenario_in_single_scenario_mode(raw, scenarios)

    if log:
        raw = raw.where(raw >= FLOOR).fillna(FLOOR)
        raw = np.log(raw)

    return raw


def load_sev(
    modeling_args: CauseSpecificModelingArguments,
    versions: Versions,
    scenarios: List[int],
    rei: str,
    log: bool,
    include_uncertainty: bool = False,
) -> xr.DataArray:
    """Read in summary exposure value information.

    Args:
        modeling_args (CauseSpecificModelingArguments): dataclass containing
            cause specific modeling arguments
        versions (Versions): the versions object to query a version for
        scenarios (List[int]): the scenarios to subset SEV to
        rei (str): Risk, etiology, or impairment to load.
        log (bool): whether to take the natural log of sev values
        include_uncertainty (bool): whether to include uncertainty

    Returns:
        DataArray: xarray with sev data loaded and formatted
    """
    logger.debug(
        "Reading risk-sex specific SEV",
        bindings=dict(rei=rei, sex_id=modeling_args.sex_id),
    )
    national_only = not modeling_args.subnational
    locs = location.get_location_set(
        gbd_round_id=modeling_args.gbd_round_id, national_only=national_only
    )[DimensionConstants.LOCATION_ID].tolist()

    future_file_spec = FHSFileSpec(
        versions.get(past_or_future="future", stage="sev"), f"{rei}.nc"
    )
    da = open_xr_scenario(file_spec=future_file_spec).sel(
        sex_id=modeling_args.sex_id, location_id=locs
    )

    da = enforce_scenario_dim(modeling_args, None, scenarios, da)

    if "sev" in versions["past"].keys():
        past_file_spec = FHSFileSpec(
            versions.get(past_or_future="past", stage="sev"), f"{rei}.nc"
        )
        past_da = open_xr_scenario(file_spec=past_file_spec).sel(
            sex_id=modeling_args.sex_id,
            location_id=locs,
            year_id=modeling_args.years.past_years,
        )

        past_da = replace_scenario_dim(past_da, scenarios)

        da = da.sel(year_id=modeling_args.years.forecast_years)
        da = xr.concat([past_da, da], dim="year_id")
    else:
        da = da.sel(year_id=modeling_args.years.years)

    if not include_uncertainty:
        da = da.mean("draw").expand_dims({"draw": np.arange(modeling_args.draws)})
    else:
        da = resample(data=da, num_of_draws=modeling_args.draws)

    if log:
        da = da.where(da >= FLOOR).fillna(FLOOR)
        da = np.log(da)

    da = _drop_scenario_in_single_scenario_mode(da, scenarios)

    single_coords = np.setdiff1d(list(da.coords.keys()), da.dims)
    da = da.drop_vars(single_coords)
    return da


def load_paf_covs(
    modeling_args: CauseSpecificModelingArguments,
    versions: Versions,
    scenarios: List[int],
    listonly: bool = False,
    include_uncertainty: bool = False,
) -> xr.Dataset:
    """Return a Dataset of sev data for a cause or alternatively a list of applicable sevs.

    Args:
        modeling_args (CauseSpecificModelingArguments): dataclass containing
            cause specific modeling arguments
        versions (Versions): the versions object to query a version for
        scenarios (List[int]): the scenarios you want to subset for PAFs
        listonly (bool): if True, return just a list of applicable sevs,
            otherwise return the full dataset
        include_uncertainty (bool): Whether to include past uncertainty
            (otherwise just copy the mean)

    Returns:
        Dataset: dataset whose datavars are the arrays for each relevant sev
    """
    logger.debug(
        "Reading cause-sex-specific PAF",
        bindings=dict(acause=modeling_args.acause, sex_id=modeling_args.sex_id),
    )

    with db_session.create_db_session() as session:
        paf1_risk_pairs = strategy.get_cause_risk_pair_set(
            session=session,
            strategy_id=CauseRiskPairConstants.PAF_OF_ONE_STRATEGY_ID,
            gbd_round_id=modeling_args.gbd_round_id,
        )

        cause_id = get_cause_id(acause=modeling_args.acause)
        rei_ids = paf1_risk_pairs.query("cause_id == @cause_id").rei_id.values
        logger.debug(
            f"Reading all risk specifc PAFs for {modeling_args.acause}",
            bindings=dict(acause=modeling_args.acause, cause_id=cause_id, rei_ids=rei_ids),
        )

        all_reis = [query_risk.get_rei(int(rei_id)) for rei_id in rei_ids]

        most_detailed_risks = strategy.get_risk_set(
            session=session,
            strategy_id=RiskConstants.FATAL_DETAILED_STRATEGY_ID,
            gbd_round_id=modeling_args.gbd_round_id,
        )[DimensionConstants.REI].unique()

        # subset to most detailed reis
        reis = [
            rei
            for rei in all_reis
            if (rei in most_detailed_risks) and not (rei in DIRECT_MODELED_PAFS)
        ]

        if listonly:
            return reis

        reis_to_log = strategy.get_cause_risk_pair_set(
            session=session,
            strategy_id=CauseRiskPairConstants.SEV_LOG_TRANSFORM_STRATEGY_ID,
            gbd_round_id=modeling_args.gbd_round_id,
        )[DimensionConstants.REI_ID].unique()
    reis_to_log = [query_risk.get_rei(int(rei_id)) for rei_id in reis_to_log]

    ds = xr.Dataset()
    for r in reis:
        log_rei = r in reis_to_log
        da = load_sev(
            modeling_args=modeling_args,
            versions=versions,
            scenarios=scenarios,
            rei=r,
            log=log_rei,
            include_uncertainty=include_uncertainty,
        )
        ds[r] = da

    return ds


def load_cod_dataset(
    addcovs: List[str],
    modeling_args: CauseSpecificModelingArguments,
    versions: Versions,
    logspace_conversion_flags: Dict[str, bool],
    scenarios: Optional[Iterable[int]],
    sev_covariate_draws: bool = False,
) -> xr.Dataset:
    """Load in acause-sex specific mortality rate.

    Along with scalars, sevs, sdi, and other covariates if applicable.

    Args:
        addcovs (list[str]): list of cause-specific covariates to add to the
            dataset
        modeling_args (CauseSpecificModelingArguments): dataclass containing
            cause specific modeling arguments
        versions (Versions): set of versions passed in at runtime. Will either be OOS versions
            or standard ones based on user input ``oos`` CLI arg.
        logspace_conversion_flags (Dict[str, bool]): Mapping of version name to whether or not
            it should be logged.
        sev_covariate_draws (bool): Whether to include draws of the past SEVs
            used covariates or simply take the mean.

    Returns:
        Dataset: xarray dataset with cod mortality rate and scalar, sev, sdi,
            and other covariate information
    """
    logger.debug(
        "Loading cause-sex-specific mortality dataset",
        bindings=dict(acause=modeling_args.acause, sex_id=modeling_args.sex_id),
    )

    # check validity of inputs
    validate_cause(acause=modeling_args.acause, gbd_round_id=modeling_args.gbd_round_id)
    for past_or_future in ["past", "future"]:
        for stage in list(versions[past_or_future].keys()):
            if past_or_future != "future" and stage != "death":
                # don't check the output; it doesn't exist yet.
                validate_version(versions=versions, past_or_future=past_or_future, stage=stage)

    # parent_id, level info is needed when fitting on nationals only
    national_only = not modeling_args.subnational
    loc_df = location.get_location_set(
        gbd_round_id=modeling_args.gbd_round_id, national_only=national_only
    )
    regdf = loc_df[["location_id", "region_id", "super_region_id", "parent_id", "level"]]
    if not modeling_args.subnational:
        regdf = loc_df.query("level==3")[["location_id", "region_id", "super_region_id"]]
    regdf.set_index("location_id", inplace=True)
    year_list = modeling_args.years.years
    time_array = xr.DataArray(
        year_list - modeling_args.years.past_start,
        dims=["year_id"],
        coords=[year_list],
    )
    codda = load_cod_data(
        modeling_args=modeling_args,
        versions=versions,
        past_or_future="past",
        stage="death",
        log=logspace_conversion_flags["death"],
    )
    agevals = codda.coords["age_group_id"].values
    locvals = codda.coords["location_id"].values
    demdict = dict(year_id=year_list, age_group_id=agevals, location_id=locvals)

    sdi_scenarios = scenarios or _decide_scenario_to_sel(versions, "future", "sdi")
    scalar_scenarios = scenarios or _decide_scenario_to_sel(versions, "future", "scalar")

    ds = xr.Dataset(
        dict(
            y=codda,
            sdi=load_sdi(
                modeling_args=modeling_args,
                versions=versions,
                scenarios=sdi_scenarios,
                log=logspace_conversion_flags["sdi"],
            ),
            ln_risk_scalar=load_scalar(
                modeling_args=modeling_args,
                versions=versions,
                scenarios=scalar_scenarios,
                log=logspace_conversion_flags["scalar"],
            ),
            intercept=xr.DataArray(1),
            time_var=time_array,
        )
    )

    for cov in addcovs:
        cov_scenarios = scenarios or _decide_scenario_to_sel(versions, "future", cov)
        ds[cov] = load_cov(
            modeling_args=modeling_args,
            versions=versions,
            scenarios=cov_scenarios,
            cov=cov,
            log=logspace_conversion_flags[cov],
        )

    sev_scenarios = scenarios or _decide_scenario_to_sel(versions, "future", "sev")
    ds.update(
        load_paf_covs(
            modeling_args=modeling_args,
            versions=versions,
            scenarios=sev_scenarios,
            include_uncertainty=sev_covariate_draws,
        )
    )

    ds_sub = ds.loc[demdict]
    ds_sub.update(xr.Dataset(regdf))
    ds_sub.y.values[(ds_sub.y == -np.inf).values] = np.nan
    ds_sub.ln_risk_scalar.values[(np.isnan(ds_sub.ln_risk_scalar.values))] = 0.0

    # select just non-aggregate values
    loc_vals = list(
        set(regdf.reset_index().location_id.values.tolist())
        & set(ds_sub.location_id.values.tolist())
    )
    ds_sub = ds_sub.loc[{"location_id": loc_vals}]
    return ds_sub


def _remove_aggregate_age_groups(raw: xr.DataArray) -> xr.DataArray:
    """Remove aggregate age-groups, or if we only have aggregate age-groups, keep just one."""
    agg_ages = [22, 27]
    if "age_group_id" in raw.coords.keys():
        # drop point coordinate
        if len(raw["age_group_id"]) == 1:
            return raw.squeeze("age_group_id", drop=True)
        # keep all-ages, not age-standardized
        elif raw.age_group_id.values.tolist() == agg_ages:
            return raw.loc[{"age_group_id": 22}]
        # given the choice between age-specific or all-ages, choose age-specific
        else:
            remaining_ages = np.setdiff1d(raw["age_group_id"].values, agg_ages)
            return raw.loc[{"age_group_id": remaining_ages}]
    else:
        return raw


def _project_sex_id(raw: xr.DataArray, sex_id: int) -> xr.DataArray:
    """Project data down to a single sex_id, knowing sex_id 3 represents data for either."""
    if "sex_id" in raw.coords.keys():
        if sex_id in raw["sex_id"].values:
            return raw.sel(sex_id=sex_id, drop=True)
        elif raw["sex_id"].values == 3:
            return raw.squeeze("sex_id", drop=True)
        else:
            print("this covariate doesn't have the sex_id you're looking for")
            raise SystemExit
    else:
        return raw


def _decide_scenario_to_sel(versions: Versions, past_or_future: str, stage: str) -> List[int]:
    if versions.get(past_or_future, stage).scenario is not None:
        return [versions.get(past_or_future, stage).scenario]
    return ScenarioConstants.SCENARIOS


def _drop_scenario_in_single_scenario_mode(
    da: xr.DataArray, scenarios: List[int]
) -> xr.DataArray:
    if DimensionConstants.SCENARIO in da.dims and len(da[DimensionConstants.SCENARIO]) == 1:
        da = da.sel(scenario=scenarios[0], drop=True)
    return da
