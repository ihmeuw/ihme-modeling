r"""Compute cause-risk-specific future total SEV, given acause and risk.

Example call:

.. code:: bash

    python compute_future_mediator_total_sev.py \
        --acause cvd_ihd \
        --rei metab_sbp \
        --gbd-round-id 6 \
        --sev-version 20210124_test \
        --past-sev-version 20201105_etl_sevs \
        --rr-version 20200831_hard_coded_inference

Note that, to save memory footprint, this code will only analyze and
export future years.
"""

import gc
import glob
from functools import reduce
from pathlib import Path
from typing import Optional

import numpy as np
import xarray as xr
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_data_transformation.lib.intercept_shift import unordered_draw_intercept_shift
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_database_interface.lib.query import cause
from fhs_lib_file_interface.lib.pandas_wrapper import read_csv
from fhs_lib_file_interface.lib.version_metadata import (
    FHSDirSpec,
    FHSFileSpec,
    VersionMetadata,
)
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr_scenario, save_xr_scenario
from fhs_lib_genem.lib.constants import OrchestrationConstants
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib import fhs_logging

from fhs_pipeline_sevs.lib import rrmax as rrmax_module
from fhs_pipeline_sevs.lib.compute_past_intrinsic_sev import (
    get_product_on_right_hand_side,
    newton_solver,
)
from fhs_pipeline_sevs.lib.constants import FutureSEVConstants, PastSEVConstants

logger = fhs_logging.get_logger()


def keep_minimum_k(
    k0: xr.DataArray,
    acause: str,
    rei: str,
    gbd_round_id: int,
    past_sev_version: str,
    years: YearRange,
    draws: int,
    draw_start: Optional[int] = 0,
) -> xr.DataArray:
    """Compare past and future k coefficients and keep the lower ones.

    `k0` contains ARC-computed past years.
    For the past years, we simply replace.
    For the future years, we compare to last past year first, and then keep
    the lower ones.

    Args:
        k0 (xr.DataArray): k coefficients freshly computed from newton solver.
        acause (str): acause.
        rei (str): risk.
        gbd_round_id (int): gbd round id.
        sev_version (str): version of future SEV.
        rr_version (str): version of RR (past).
        years (YearRange): past_start:future_start:future_end.
        draws (int): number of draws kept in process.
        draw_start (Optional[int]): starting index of draws selected.

    Returns:
        (xr.DataArray): k coefficients of min(past_end, forecast_years).
    """
    k_past_version_metadata = VersionMetadata.make(
        data_source=gbd_round_id,
        epoch="past",
        stage="sev",
        version=past_sev_version,
    )
    k_past_file_spec = FHSFileSpec(
        version_metadata=k_past_version_metadata,
        sub_path=(OrchestrationConstants.SUBFOLDER,),
        filename=f"{acause}_{rei}_k_coeff.nc",
    )

    k_past = (
        open_xr_scenario(k_past_file_spec)
        .load()
        .sel(
            draw=range(draw_start, draw_start + draws, 1),
            location_id=k0["location_id"].values,
            age_group_id=k0["age_group_id"].values,
            sex_id=k0["sex_id"].values,
            year_id=years.past_years,
        )
    )

    if not (k_past["draw"] == k0["draw"]).all():
        raise ValueError("draw coordinate mismatch between k0 and k_last_past")

    # first make sure k_past has the same scenarios as k0
    k_past = expand_dimensions(k_past, scenario=k0["scenario"].values.tolist())

    # replace forecasted values with past where last past < forecast
    k_last_past = k_past.sel(year_id=years.past_end)

    needed_years = np.concatenate(([years.past_end], years.forecast_years))
    k0_future = k0.sel(year_id=needed_years)

    # compare to last past year first, and then keep the lower ones.
    k0.loc[dict(year_id=needed_years)] = k0_future.where(k0_future < k_last_past).fillna(
        k_last_past
    )

    return k0


def future_cause_risk_sev(
    acause: str,
    rei: str,
    gbd_round_id: int,
    sev_version: str,
    past_sev_version: str,
    rr_version: str,
    years: YearRange,
    draws: int,
) -> None:
    """Produce cause-risk specific SEV forecast.

    Export to the same version as where the intrinsic SEVs are.

    Args:
        acause (str): acause.
        rei (str): risk.
        gbd_round_id (int): gbd round id.
        sev_version (str): version of future SEV.
        past_sev_version (str): version of past SEV.
        rr_version (str): version of RR (past).
        years (YearRange): past_start:future_start:future_end.
        draws (int): number of draws kept in process.
    """
    cause_id = cause.get_cause_id(acause=acause)

    # where to pick up the cause-risk specific *future* intrinsic SEV
    in_out_version_metadata = VersionMetadata.make(
        data_source=gbd_round_id,
        epoch="future",
        stage="sev",
        version=sev_version,
    )
    in_out_dir_spec = FHSDirSpec(
        version_metadata=in_out_version_metadata, sub_path=(OrchestrationConstants.SUBFOLDER,)
    )

    chunk_size = PastSEVConstants.DRAW_CHUNK_SIZE  # compute k in chunks of 100 draws

    k = xr.DataArray()
    rei_product = xr.DataArray()

    for i, draw_start in enumerate(range(0, draws, chunk_size)):
        # burden is on the user to make sure draws & chunk_size are consistent
        sev_intrinsic = open_xr_scenario(
            file_spec=FHSFileSpec.from_dirspec(
                dir=in_out_dir_spec, filename=f"{acause}_{rei}_intrinsic.nc"
            )
        ).sel(draw=range(draw_start, draw_start + chunk_size, 1))

        med_age_ids = sev_intrinsic.age_group_id.values

        a_is = get_product_on_right_hand_side(
            acause=acause,
            cause_id=cause_id,
            rei=rei,
            gbd_round_id=gbd_round_id,
            past_or_future="future",
            sev_version=sev_version,
            rr_version=rr_version,
            draws=chunk_size,
            draw_start=draw_start,
        )

        a_is = expand_dimensions(a_is, age_group_id=med_age_ids, fill_value=0)

        # this is a more memory-friendly way to make the product
        rei_product_i = reduce(
            lambda x, y: x * y,
            (a_is.sel(rei=risk) + 1 for risk in a_is["rei"].values),
        )

        # Note get_product_on_right_hand_side does the same read_rrmax call.
        rrmax = rrmax_module.read_rrmax(
            acause=acause,
            cause_id=cause_id,
            rei=rei,
            gbd_round_id=gbd_round_id,
            version=rr_version,
            draws=chunk_size,
            draw_start=draw_start,
        )

        sev = (((rrmax - 1) * sev_intrinsic + 1) * rei_product_i - 1) / (rrmax - 1)

        del rei_product_i
        gc.collect()

        # some sev values might be greater than 1.  Here we solve for their k's
        sev_reduced = sev.where(sev <= 1).fillna(1)

        b_const = ((rrmax - 1) * sev_reduced + 1) / ((rrmax - 1) * sev_intrinsic + 1)

        del sev_reduced
        gc.collect()

        a_is = a_is.sel(age_group_id=b_const["age_group_id"].values)

        b_const = b_const.where(sev > 1).fillna(1)  # so log(b) = 0

        # now compute the initial k guess
        rei_sum = a_is.sum(dim="rei")

        k_i = np.log(b_const) / rei_sum  # the initial k

        del rei_sum
        gc.collect()

        k_i = k_i.where(sev > 1).fillna(0)  # so log(a * k + 1) = 0

        for dim in k_i.dims:  # make sure all indices are matched along axis
            b_const = b_const.sel(**{dim: k_i[dim].values})
            a_is = a_is.sel(**{dim: k_i[dim].values})

        # align everything to k_i's dim order before newton solver
        b_const = b_const.transpose(*k_i.dims)
        a_is = a_is.transpose(*[a_is.dims[0]] + list(b_const.dims))

        k_i = newton_solver(a_is, b_const, k_i)

        del b_const
        gc.collect()

        k_i = k_i.where(sev > 1).fillna(1)  # k=1 for non-problematic cells

        del sev
        gc.collect()

        # Per CJLM, pick the min of K between last past year and just-computed
        k_i = keep_minimum_k(
            k0=k_i,
            acause=acause,
            rei=rei,
            gbd_round_id=gbd_round_id,
            past_sev_version=past_sev_version,
            years=years,
            draws=chunk_size,
            draw_start=draw_start,
        )

        # need to recompute rei_product, now with k
        rei_product_i = reduce(
            lambda x, y: x * y,
            (k_i * a_is.sel(rei=risk) + 1 for risk in a_is["rei"].values),
        )

        if i == 0:
            k = k_i
            rei_product = rei_product_i
        else:
            k = xr.concat([k, k_i], dim="draw")
            rei_product = xr.concat([rei_product, rei_product_i], dim="draw")
            del k_i, rei_product_i

        del a_is
        gc.collect()

    # re-compute sev_intrinsic with k.
    sev_intrinsic = open_xr_scenario(
        file_spec=FHSFileSpec.from_dirspec(
            dir=in_out_dir_spec, filename=f"{acause}_{rei}_intrinsic.nc"
        )
    )
    sev_intrinsic = resample(sev_intrinsic, draws)

    rrmax = rrmax_module.read_rrmax(
        acause=acause,
        cause_id=cause_id,
        rei=rei,
        gbd_round_id=gbd_round_id,
        version=rr_version,
        draws=draws,
    )

    sev = (((rrmax - 1) * sev_intrinsic + 1) * rei_product - 1) / (rrmax - 1)

    del rrmax, sev_intrinsic, rei_product
    gc.collect()

    if (sev > 1 + PastSEVConstants.TOL).any():
        max_val = float(sev.max())
        logger.warning(
            (
                f"There are values in final SEV > 1 + {PastSEVConstants.TOL}, "
                f"the max being {max_val}."
            )
        )

    sev = sev.where(sev <= 1).fillna(1)
    sev = sev.where(sev >= 0).fillna(0)

    # remove some redundant point dims
    redundants = list(set(sev.coords.keys()) - set(sev.dims))
    redundants.remove("acause")  # we're keeping "acause"

    for redundant in redundants:  # only keep dims and "acause"
        k = k.drop_vars(redundant)
        sev = sev.drop_vars(redundant)

    save_xr_scenario(
        xr_obj=k,
        file_spec=FHSFileSpec.from_dirspec(
            dir=in_out_dir_spec, filename=f"{acause}_{rei}_k_coeff.nc"
        ),
        metric="number",
        space="identity",
        gbd_round_id=gbd_round_id,
        sev_version=sev_version,
        rr_version=rr_version,
        tol=PastSEVConstants.TOL,
        rtol=PastSEVConstants.RTOL,
        maxiter=PastSEVConstants.MAXITER,
    )

    save_xr_scenario(
        xr_obj=sev,
        file_spec=FHSFileSpec.from_dirspec(dir=in_out_dir_spec, filename=f"{acause}_{rei}.nc"),
        metric="rate",
        space="identity",
        gbd_round_id=gbd_round_id,
        sev_version=sev_version,
        rr_version=rr_version,
    )


def combine_cause_risk_sevs_to_sev(
    rei: str,
    sev_version: str,
    past_sev_version: str,
    rr_version: str,
    gbd_round_id: int,
    years: YearRange,
) -> None:
    """Combine cause-risk SEVs to make risk-only SEVs, for given risk.

    Cause-risk SEVs are in sev_version/risk_acause_specific/, with risk-only
    SEVs exported to sev_version/

    Args:
        rei (str): risk whose cause-risk SEVs will be averaged out.
        sev_version (str): future SEV version.
        past_sev_version (str): past SEV vdersion.
        rr_version (str): version of rr, from FILEPATH.
        gbd_round_id (int): gbd round id.
        draws (int): number of draws to keep.
        years (YearRange): past_start:forecast_start:forecast_end.
    """
    daly_df = read_csv(FutureSEVConstants.DALY_WEIGHTS_FILE_SPEC, keep_default_na=False)

    output_version_metadata = VersionMetadata.make(
        data_source=gbd_round_id,
        epoch="future",
        stage="sev",
        version=sev_version,
    )

    glob_str = (
        output_version_metadata.data_path() / OrchestrationConstants.SUBFOLDER / f"*{rei}.nc"
    )
    file_names = {Path(f).name for f in glob.glob(str(glob_str))}

    if len(file_names) == 0:
        raise ValueError(f"No cause-risk files found for *{rei}.nc")

    acauses = [Path(x).name.replace(f"_{rei}.nc", "") for x in file_names]
    total_daly = daly_df.query("acause in {}".format(tuple(acauses)))["DALY"].sum()

    sev = xr.DataArray(0)

    for file_name in file_names:
        sev_i = open_xr_scenario(
            FHSFileSpec(
                version_metadata=output_version_metadata,
                sub_path=(OrchestrationConstants.SUBFOLDER,),
                filename=file_name,
            )
        )

        acause = file_name.replace(f"_{rei}.nc", "")
        acause_daly = float(daly_df.query(f"acause == '{acause}'")["DALY"].values)
        fraction = acause_daly / total_daly
        sev = sev + (sev_i * fraction)
        del sev_i
        gc.collect()

    if "acause" in sev.coords:
        sev = sev.drop_vars("acause")

    past_sev_version_metadata = VersionMetadata.make(
        data_source=gbd_round_id,
        epoch="past",
        stage="sev",
        version=past_sev_version,
    )
    past_file_spec = FHSFileSpec(
        version_metadata=past_sev_version_metadata, filename=f"{rei}.nc"
    )
    past_data = open_xr_scenario(past_file_spec)

    # Need to subset past_data by sev coordinates
    past_data = past_data.sel(
        age_group_id=sev.age_group_id.values,
        sex_id=sev.sex_id.values,
        location_id=sev.location_id.values,
    )

    past_data = resample(past_data, len(sev.draw.values))

    sev = unordered_draw_intercept_shift(sev, past_data, years.past_end, years.forecast_end)

    sev = sev.clip(min=FutureSEVConstants.FLOOR, max=1 - FutureSEVConstants.FLOOR)

    # Now we can save it
    save_xr_scenario(
        xr_obj=sev,
        file_spec=FHSFileSpec(version_metadata=output_version_metadata, filename=f"{rei}.nc"),
        metric="rate",
        space="identity",
        gbd_round_id=gbd_round_id,
        sev_version=sev_version,
        rr_version=rr_version,
    )
