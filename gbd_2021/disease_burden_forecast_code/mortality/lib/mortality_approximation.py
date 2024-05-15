"""Perform mortality approximation for every cause."""

import gc
from typing import Iterable, List, Optional, Union

import numpy as np
import pandas as pd
import xarray as xr
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_data_transformation.lib.processing import strip_single_coord_dims
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_data_transformation.lib.validate import check_dataarray_shape
from fhs_lib_database_interface.lib import db_session
from fhs_lib_database_interface.lib.constants import (
    CauseConstants,
    DimensionConstants,
    SexConstants,
)
from fhs_lib_database_interface.lib.query import location
from fhs_lib_database_interface.lib.strategy_set.query import get_property_values
from fhs_lib_database_interface.lib.strategy_set.strategy import get_cause_set
from fhs_lib_file_interface.lib.check_input import check_versions
from fhs_lib_file_interface.lib.provenance import ProvenanceManager
from fhs_lib_file_interface.lib.version_metadata import (
    FHSDirSpec,
    FHSFileSpec,
    VersionMetadata,
)
from fhs_lib_file_interface.lib.versioning import Versions
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr_scenario, save_xr_scenario
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib.fhs_logging import get_logger

logger = get_logger()

SUPERFLUOUS_DIMS = {"variable", "acause", "cov"}
INJ_TRANS_ROAD = "inj_trans_road"
NTD_EXCEPTIONS = ["ntd_nema", "ntd_dengue"]

SCALAR_RATIO_DEFAULT = 1
SDI_BETA_COEFF_DEFAULT = 0  # goes into the exponential --> exp(0) = 1
NON_SEV_COVARIATES = {
    "asfr",
    "sdi",
    "time_var",
    "sdi_time",
    "hiv",
}
EXPECTED_STAGES = (
    "sdi",
    "scalar",
    "sev",
    "asfr",
    "hiv",
    "death",
)
NO_COVS_SCALARS = [
    "ntd_afrtryp",
    "ntd_dengue",
]

# .nc file variable specifics for SDI and SDI * t
BETA_GLOBAL = "beta_global"  # what the beta coeff is named in .nc
SDI_TIME = "sdi_time"
SDI = "sdi"


def get_fatal_sex_availability(gbd_round_id: int) -> pd.DataFrame:
    """Get sex availability by acause for fatal causes."""
    with db_session.create_db_session() as session:
        fatal_gk_causes = get_cause_set(
            session=session,
            gbd_round_id=gbd_round_id,
            strategy_id=CauseConstants.FATAL_GK_STRATEGY_ID,
        )[[DimensionConstants.ACAUSE]]

        fatal_sex_availability = (
            get_property_values(session, DimensionConstants.CAUSE, gbd_round_id)
            .reset_index()[[DimensionConstants.ACAUSE, "fatal_available_sex_id"]]
            .rename(columns={"fatal_available_sex_id": DimensionConstants.SEX_ID})
        )

    fatal_gk_causes_with_sex_availability = fatal_gk_causes.merge(fatal_sex_availability)
    return fatal_gk_causes_with_sex_availability


def _decide_base_scenario(
    da: xr.DataArray, base_scenario: Optional[int] = None
) -> xr.DataArray:
    """Transform the base dataaarray according to prescribed base scenario.

    If base_scenario is None, return original array.
    If prescribed, return that scenario, and remove the scenario dim.

    Args:
        da (xr.DataArray): DataArray with scenario dimension.
        base_scenario (int): Base scenario to operate on.
            If specified, performs all-to-one operation on given base scenario.
            If not specified (None), performs one-to-one operation on
            scenarios.

    Returns:
        (xr.DataArray): Original da if base_scenario is None, otherwise return
            da.sel(scenario=base_scenario).drop_vars("scenario")
    """
    if base_scenario is not None:
        return da.sel(scenario=base_scenario, drop=True)
    else:
        return da


def _load_scalar_ratio(
    gbd_round_id: int,
    past_or_future: str,
    base_versions: Versions,
    versions: Versions,
    acause: str,
    base_scenario: Optional[int] = None,
    draws: Optional[int] = None,
    run_on_means: bool = False,
    national_only: bool = False,
) -> Union[xr.DataArray, int]:
    """Compute scalar / scalar_0.

    Args:
        gbd_round_id (int): GBD round id.
        past_or_future (str): Either ``"past"`` or ``"future"``.
        base_versions (Versions): Versions object with baseline scalar version.
        versions (Versions): Versions object with new scalar version.
        acause (str): Name of analytical cause of death.
        base_scenario (Optional[int]): Base scenario to operate on.
            If specified, performs all-to-one operation on given base scenario.
            If not specified (None), performs one-to-one operation on
            scenarios.
        draws (Optional[int]): Number of draws needed.
        run_on_means (bool): Use mean of draws.
        national_only (bool): national locations only.

    Returns:
        (xr.DataArray | int): scalar / scalar_0
    """
    base_scalar_version = base_versions.get(past_or_future, "scalar").default_data_source(
        gbd_round_id
    )
    base_scalar_file = FHSFileSpec(base_scalar_version, f"{acause}.nc")

    # not every cause has a scalar
    if ProvenanceManager.exists(base_scalar_file):
        scalar_base = open_xr_scenario(base_scalar_file)

        scalar = open_xr_scenario(
            FHSFileSpec(
                versions.get(past_or_future, "scalar").default_data_source(gbd_round_id),
                f"{acause}.nc",
            )
        )
        scalar = strip_single_coord_dims(scalar)

        missing_dims = list(set(scalar_base.dims).difference(scalar.dims))
        new_dims = {dim: scalar_base[dim].values.tolist() for dim in missing_dims}
        scalar = scalar.expand_dims(new_dims)

        # If all approximations are done on a single base scenario, then we remove all other
        # scenarios, drop the scenario dim, to allow broadcast arithmetic to take over
        scalar_base = _decide_base_scenario(scalar_base, base_scenario)

        if draws:  # only resample if asked to
            scalar_base = resample(scalar_base, num_of_draws=draws)
            scalar = resample(scalar, num_of_draws=draws)

        if run_on_means:
            scalar_base = scalar_base.mean("draw")
            scalar = scalar.mean("draw")

        if national_only:
            national_locations = (
                location.get_location_set(gbd_round_id)
                .query("level == 3")
                .location_id.tolist()
            )
            scalar_base = scalar_base.sel(location_id=national_locations)
            scalar = scalar.sel(location_id=national_locations)

        scalar_ratio = scalar / scalar_base  # inner join arithmetic

    else:
        logger.warning(f"Base scalar for {acause} does not exist")

        scalar_ratio = SCALAR_RATIO_DEFAULT  # 1

    return scalar_ratio


def _copy_national_coefficients_to_subnationals(
    da: xr.DataArray, gbd_round_id: int
) -> xr.DataArray:
    """Copy national coefficients to subnational locations.

    Args:
        da (xr.DataArray): Betas array
        gbd_round_id (int): GBD round ID

    Returns:
        xr.DataArray: betas array with all subnational location IDs included
    """
    # get parents and location_ids of level 4 locations present in ds
    locs = location.get_location_set(gbd_round_id=gbd_round_id).query("level in [3, 4]")

    nationals = locs.query("level== 4").parent_id.unique().tolist()
    nationals_in_da = [loc for loc in nationals if loc in da.location_id]

    if not np.isin(nationals, nationals_in_da).all():
        raise ValueError("Missing level 3 locations in death dataset.")

    ds_all_locs = [da]
    for nat_loc in nationals_in_da:
        nat_ds = da.sel(location_id=nat_loc, drop=True)
        subnats = locs.query("parent_id == @nat_loc").location_id.values
        subnat_ds = expand_dimensions(nat_ds, location_id=subnats)
        ds_all_locs.append(subnat_ds)

    ds_all_locs = xr.concat(ds_all_locs, dim=DimensionConstants.LOCATION_ID)
    return ds_all_locs


def _load_beta_dataset(
    gbd_round_id: int,
    gk_version: str,
    acause: str,
    past_or_future: str,
    draws: Optional[int] = None,
    run_on_means: bool = False,
    national_only: bool = False,
) -> xr.Dataset:
    """Get beta coefficients dataset from betas directory.

    Args:
        gbd_round_id (int): GBD round ID for data being approximated.
        gk_version (str): 1st stage mortality results. Contains `betas`
            directory.
        acause (str): Cause
        past_or_future (str): `past` or `future`
        draws (int): Optional. Number of draws in data.
        run_on_means (bool): Flag for running on means only.
        national_only (bool): National locations only.

    Returns:
        (xr.Dataset | int): betas for all covariates.
    """
    gk_version_metadata = VersionMetadata.make(
        data_source=gbd_round_id, epoch=past_or_future, stage="death", version=gk_version
    )
    betas_dir = FHSDirSpec(gk_version_metadata, ("betas",))
    # most detailed data is saved by sex with suffix
    sex_avail = (
        get_fatal_sex_availability(gbd_round_id).query("acause==@acause").sex_id.values.item()
    )

    betas_list = []

    for sex_id in sex_avail:
        beta_path = FHSFileSpec.from_dirspec(
            betas_dir, f"{acause}_{SexConstants.SEX_DICT[sex_id]}.nc"
        )

        beta_tmp = open_xr_scenario(beta_path)[BETA_GLOBAL]

        if draws:
            beta_tmp = resample(beta_tmp, draws)

        if run_on_means:
            beta_tmp = beta_tmp.mean("draw")

        betas_list.append(beta_tmp)

    beta = _make_beta(betas_list)

    if national_only:
        national_locations = (
            location.get_location_set(gbd_round_id).query("level == 3").location_id.tolist()
        )
        beta_full = beta.sel(location_id=national_locations)
    else:
        # copy values in national location ids to subnationals
        beta_full = _copy_national_coefficients_to_subnationals(
            da=beta,
            gbd_round_id=gbd_round_id,
        )

    return beta_full


def _load_covariates(
    beta: xr.Dataset,
    versions: Versions,
    gbd_round_id: int,
    past_or_future: str,
    years: YearRange,
    base_scenario: Optional[int] = None,
    draws: Optional[int] = None,
    run_on_means: bool = False,
    national_only: bool = False,
) -> xr.Dataset:
    """Load all covariates in a Dataset.

    Args:
        beta (Union[xr.Dataset, int]): Covariate dataset or 0. If there are
            no covariates in the betas file, this value is 0.
        versions (Versions): Versions object containing all covariate
            versions e.g. sdi/fake_version.
        gbd_round_id (int): GBD round ID.
        past_or_future (str): `past` or `future`.
        years (YearRange): A YearRange object of relevant years.
        base_scenario (Optional[int]): Base scenario to operate on. If
            specified, performs all-to-one operation on given base scenario. If
            not specified (None), performs one-to-one operation on
            scenarios.
        draws (Optional[int]): Number of draws needed.
        run_on_means (bool): Take the mean of the covariate data.
        national_only (bool): Run on national locations only.

    Returns:
        (xr.Dataset): All covariates in a dataset
    """
    beta_covs = beta.cov.values.tolist()
    sdi_time = True if SDI_TIME in beta_covs else False
    covs = [cov for cov in beta_covs if cov not in ["intercept", "sdi_time", "time_var"]]

    covariates = xr.Dataset()
    for cov in covs:
        if cov in NON_SEV_COVARIATES:
            cov_version = versions.get(past_or_future, cov).default_data_source(gbd_round_id)
            if cov == "hiv":
                covariate = open_xr_scenario(
                    FHSFileSpec(cov_version.with_stage("death"), f"{cov}.nc")
                )
            else:
                covariate = open_xr_scenario(FHSFileSpec(cov_version, f"{cov}.nc"))
        else:
            # these are sev covariates
            cov_version = versions.get(past_or_future, "sev").default_data_source(gbd_round_id)
            covariate = open_xr_scenario(FHSFileSpec(cov_version, f"{cov}.nc"))

        # remove age/sex dims if they are aggregates
        covariate = strip_single_coord_dims(covariate)

        covariate = _decide_base_scenario(covariate, base_scenario)

        if draws:
            covariate = resample(covariate, num_of_draws=draws)

        if run_on_means:
            covariate = covariate.mean("draw")

        if national_only:
            national_locations = (
                location.get_location_set(gbd_round_id)
                .query("level == 3")
                .location_id.tolist()
            )
            covariate = covariate.sel(location_id=national_locations)

        covariates[cov] = covariate

    # add sdi_time covariate if it exists
    if sdi_time:
        # time is treated as t_0, t_0 + 1, t_0 + 2,...t_n + k for t_0 = 0
        # and k = (number of years in time series - 1)
        year_list = years.years
        time_array = xr.DataArray(
            year_list - years.past_start, dims=["year_id"], coords=[year_list]
        )
        sdi_time_cov = covariates[SDI] * time_array
        covariates[SDI_TIME] = sdi_time_cov

    return covariates


def get_exp_beta_cov_diff(
    gbd_round_id: int,
    past_or_future: str,
    base_versions: Versions,
    versions: Versions,
    base_gk_version: str,
    acause: str,
    years: YearRange,
    base_scenario: Optional[int] = None,
    draws: Optional[int] = None,
    run_on_means: bool = False,
    national_only: bool = False,
) -> xr.Dataset:
    """Compute exp(beta * (cov - cov_0)).

    Args:
        gbd_round_id (int): GBD round ID
        past_or_future (str): `past` or `future`
        base_versions (Versions): Versions object containing base versions
            for all input data.
        versions (Versions): Versions object containing new versions for
            approximated data.
        base_gk_version (str): Version directory with saved betas files.
        acause (str): Cause.
        years (YearRange): YearRange object with forecast years
        base_scenario (Optional[int]): Base scenario to operate on. If
            specified, performs all-to-one operation on given base scenario. If
            not specified (None), performs one-to-one operation on
            scenarios.
        draws (Optional[int]): Number of draws needed.
        run_on_means (bool): Take the mean of the covariate data.
        national_only (bool): Run on national locations only.

    Returns:
        (xr.Dataset): exp(beta * cov - cov_0) for each covariate
    """
    beta = _load_beta_dataset(
        gbd_round_id,
        base_gk_version,
        acause,
        past_or_future,
        draws,
        run_on_means,
        national_only,
    )

    original_covariates = _load_covariates(
        beta,
        base_versions,
        gbd_round_id,
        past_or_future,
        years,
        base_scenario,
        draws,
        run_on_means,
        national_only,
    )
    new_covariates = _load_covariates(
        beta,
        versions,
        gbd_round_id,
        past_or_future,
        years,
        None,
        draws,
        run_on_means,
        national_only,
    )

    cov_diff = new_covariates - original_covariates

    exp_beta_cov_diff = xr.Dataset()

    for cov in original_covariates:
        if acause.startswith(INJ_TRANS_ROAD):
            beta_cov = beta.sel(cov=cov)
        else:
            beta_cov = beta.sel(cov=cov, drop=True)

        _exp_beta_cov_diff = np.exp(beta_cov * cov_diff[cov])
        _exp_beta_cov_diff.name = cov
        exp_beta_cov_diff = exp_beta_cov_diff.combine_first(_exp_beta_cov_diff.to_dataset())

    exp_beta_cov_diff = exp_beta_cov_diff.fillna(1)

    return exp_beta_cov_diff


def _make_beta(
    beta_list: Union[List[xr.DataArray], List[xr.Dataset]]
) -> Union[xr.DataArray, xr.Dataset]:
    """Concat a list of sex-specific beta arrays into a single array.

    Used for both SDI (beta) and SDI * t (beta_t) coefficients.

    Args:
        beta_list (List[xr.DataArray]): list of sex-specific beta arrays.

    Returns:
        (Union[xr.DataArray, int]):
        Either dataarray of both sexes of betas, or 0.
    """
    # There are three cases:
    #   * If beta/beta_t actually exist, then combine the sexes into a single array.
    #   * Otherwise, beta/beta_t are set to 0.
    #   * Sometimes beta/beta_t exist, but are all nulls.  Set these to 0 as well.

    if len(beta_list) > 0:
        # might have only one sex_id
        beta = xr.concat(beta_list, dim=DimensionConstants.SEX_ID)
        del beta_list
        if isinstance(beta, xr.Dataset):
            for var in beta:
                if beta[var].isnull().all():
                    beta[var] = SDI_BETA_COEFF_DEFAULT
        else:
            if beta.isnull().all():  # it could be that they're all nulls
                beta = SDI_BETA_COEFF_DEFAULT
    else:
        beta = SDI_BETA_COEFF_DEFAULT  # exp(0) = 1

    return beta


def covariate_approximation(
    base_death: xr.DataArray,
    exp_beta_cov_diff: xr.Dataset,
) -> xr.Dataset:
    """Perform approximation calculation by covariate.

    SEV covariates may not match dimensions of mortality exactly across all
    dimensions.
    First calculate approximation with non-age-specific covariates. Then use
    .combine_first
    to default to base_mx dimensions.

    Args:
        base_death (xr.DataArray): Base mortality array
        exp_beta_cov_diff (xr.Dataset): exp(beta * cov_diff) for each
            covariate in betas file. Where there is no covariate data,
            this is just 1.

    Returns:
        (xr.Dataset): death * exp_beta_cov_diff[cov] for each cov
    """
    sev_covariates = set(exp_beta_cov_diff) - NON_SEV_COVARIATES

    non_sev_covariates = set(exp_beta_cov_diff) & NON_SEV_COVARIATES

    death_post_non_sev_covs = base_death
    for cov in non_sev_covariates:
        death_post_non_sev_covs = death_post_non_sev_covs * exp_beta_cov_diff[cov]

    death = death_post_non_sev_covs
    for cov in sev_covariates:
        death = death * exp_beta_cov_diff[cov]

    death = death.combine_first(death_post_non_sev_covs)

    return death


def has_covs(acause: str, gk_version: str, gbd_round_id: int) -> bool:
    """Check betas file for existence of covariate coefficients.

    Args:
        acause (str): Cause
        gk_version (str): version name with betas sub directory
        gbd_round_id (int): GBD round ID

    Returns:
        (Bool): True if cov dimension in betas file for acause has
        covariates other than intercept and time_var.
    """
    if acause in NTD_EXCEPTIONS:
        return False
    sex_avail = (
        get_fatal_sex_availability(gbd_round_id).query("acause==@acause").sex_id.values.item()
    )
    covs = []
    for sex in sex_avail:
        sex_name = SexConstants.SEX_DICT[sex]
        cov_tmp = open_xr_scenario(
            FHSFileSpec(
                version_metadata=VersionMetadata.make(
                    data_source=gbd_round_id, epoch="future", stage="death", version=gk_version
                ),
                sub_path=("betas",),
                filename=f"{acause}_{sex_name}.nc",
            )
        ).cov.values.tolist()
        for cov in cov_tmp:
            if cov not in ["intercept", "time_var"]:
                covs.append(cov)

    if len(covs) > 0:
        return True
    else:
        return False


def _validate_draws(run_on_means: bool, draws: int) -> None:
    """Validate that the `draws` and `run_on_means` parameters have been passed properly.

    Valid specification of these parameters would be where one but not the other was provided.

    Args:
        run_on_means (bool): optionally specified argument from `mortality_approximation`
        draws (int): optionally specified argument from `mortality_approximation`

    Raises:
        ValueError: if neither `run_on_means` nor `draws` have been specified
    """
    if not run_on_means:
        if draws is None:
            raise ValueError("draws arg must be specified if not run_on_means")


def mortality_approximation_calculate(
    gbd_round_id: int,
    past_or_future: str,
    base_gk_version: str,
    base_versions: Versions,
    versions: Versions,
    acause: str,
    years: YearRange,
    expected_scenarios: Iterable[int],
    base_scenario: Optional[int] = None,
    draws: Optional[int] = None,
    run_on_means: bool = True,
    national_only: bool = False,
) -> xr.DataArray:
    """See mortality_approximation."""
    check_versions(base_versions, "future", EXPECTED_STAGES)
    check_versions(versions, "future", EXPECTED_STAGES)

    _validate_draws(run_on_means=run_on_means, draws=draws)

    if has_covs(acause=acause, gbd_round_id=gbd_round_id, gk_version=base_gk_version):
        exp_beta_cov_diff = get_exp_beta_cov_diff(
            gbd_round_id,
            past_or_future,
            base_versions,
            versions,
            base_gk_version,
            acause,
            years,
            base_scenario,
            draws,
            run_on_means,
            national_only,
        )
    else:
        exp_beta_cov_diff = None

    base_death_version = base_versions.get(past_or_future, "death").with_data_source(
        gbd_round_id
    )
    base_death = open_xr_scenario(FHSFileSpec(base_death_version, f"{acause}.nc"))
    last_past_year = base_death.sel(year_id=years.past_end)
    base_death = base_death.sel(year_id=years.forecast_years)

    if draws:
        last_past_year = resample(last_past_year, draws)
        base_death = resample(base_death, draws)

    if national_only:
        national_locations = (
            location.get_location_set(gbd_round_id).query("level == 3").location_id.tolist()
        )
        base_death = base_death.sel(location_id=national_locations)

    # get coords of base death for post approximation validation
    base_death_coords = dict(base_death.coords)

    # NOTE as it stands now, GK-produced mortality dataarrays have these "variable" and
    # "acause" point dims that can be dropped.
    for dim in ["variable", "acause"]:
        if dim in base_death_coords:
            del base_death_coords[dim]
    expected_coords = {
        dim: list(base_death_coords[dim].values) for dim in base_death_coords.keys()
    }
    expected_coords["scenario"] = expected_scenarios

    base_death = _decide_base_scenario(base_death, base_scenario)

    # first we perform the covariate adjustments if applicable.
    if exp_beta_cov_diff is not None:
        death_post_cov = covariate_approximation(base_death, exp_beta_cov_diff)
    else:
        death_post_cov = base_death

    del base_death, exp_beta_cov_diff
    gc.collect()

    # Next we perform adjustments along the scalars axis.
    scalar_ratio = _load_scalar_ratio(
        gbd_round_id,
        past_or_future,
        base_versions,
        versions,
        acause,
        base_scenario=base_scenario,
        draws=draws,
        run_on_means=run_on_means,
        national_only=national_only,
    )

    # Because scalars may not match death exactly across dims (age_group_id), we use
    # .combine_first to default to _post_sdi if coordinate is missing. If scalar_ratio has more
    # age_group_id than base_death, then the following inner-join removes extra ones.  If
    # scalar_ratio has fewer, then the combine_first will restore the defaults.
    death = death_post_cov * scalar_ratio  # inner-join

    death = death.combine_first(death_post_cov)

    del death_post_cov, scalar_ratio
    gc.collect()

    death = death.drop_vars(SUPERFLUOUS_DIMS & set(death.coords)).squeeze()

    # convert year_id to int
    if death.year_id.dtype != "int64":
        death = death.assign_coords(year_id=death.year_id.astype("int"))

    # expand sex_id if squeezed
    if "sex_id" not in death.dims and "sex_id" in expected_coords.keys():
        death = death.expand_dims("sex_id")
        if len(expected_coords["sex_id"]) > 1:
            death = expand_dimensions(death, sex_id=[1, 2], fill_value=np.nan)

    # ntd_dengue and ntd_afrtryp have no covariates (other than time and intercept) and no
    # scalars - no approximation
    if acause in NO_COVS_SCALARS:
        death = xr.concat(
            [death.assign_coords({"scenario": x}) for x in expected_scenarios], dim="scenario"
        )

    # validate all coords as expected
    check_dataarray_shape(death, expected_coords)

    # include last past year
    death = xr.concat([death, last_past_year], dim="year_id")
    return death


def mortality_approximation(
    gbd_round_id: int,
    past_or_future: str,
    base_gk_version: str,
    base_versions: Versions,
    versions: Versions,
    acause: str,
    years: YearRange,
    expected_scenarios: Iterable[int],
    base_scenario: Optional[int] = None,
    draws: Optional[int] = None,
    run_on_means: bool = True,
    national_only: bool = False,
) -> None:
    """Perform Mortality Approximation on given cause.

    Args:
        gbd_round_id (int): The GBD round for this run.
        past_or_future (str): Either ``"past"`` or ``"future"``.
        base_gk_version (str): GK results with betas directory.
        base_versions (Versions): Base versions of all inputs.
        versions (Versions): Approximation versions of all inputs.
        acause (str): Name of analytical cause of death.
        years (YearRange): Past and forecast years.
        expected_scenarios (Iterable[int]): Expected scenarios in final output.
        base_scenario (Optional[int]): base scenario to operate on.
            If specified, performs all-to-one operation on given base scenario.
            If not specified (None), performs one-to-one operation on
            scenarios.
        draws (Optional[int]): Number of draws needed.
        run_on_means (bool): Use means of draws.
        national_only (bool): National locations only

    Returns:
        None
    """
    death = mortality_approximation_calculate(
        gbd_round_id,
        past_or_future,
        base_gk_version,
        base_versions,
        versions,
        acause,
        years,
        expected_scenarios,
        base_scenario,
        draws,
        run_on_means,
        national_only,
    )

    save_file = FHSFileSpec(
        versions.get(past_or_future, "death").default_data_source(gbd_round_id), f"{acause}.nc"
    )

    save_xr_scenario(
        death,
        save_file,
        metric="rate",
        space="identity",
        base_death_version=base_versions["future"]["death"],
        base_scenario=str(base_scenario),
    )
