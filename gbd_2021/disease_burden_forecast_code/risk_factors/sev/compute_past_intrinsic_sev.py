"""Compute Intrinsic SEV of a mediator.
"""

import gc
from functools import reduce
from typing import Any, List, Optional, Union

import fhs_lib_database_interface.lib.query.cause as query_cause
import fhs_lib_database_interface.lib.query.risk as query_risk
import numpy as np
import xarray as xr
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_database_interface.lib.query.age import get_ages
from fhs_lib_file_interface.lib.query.mediation import get_mediation_matrix
from fhs_lib_file_interface.lib.version_metadata import (
    FHSDirSpec,
    FHSFileSpec,
    VersionMetadata,
)
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr_scenario, save_xr_scenario
from fhs_lib_genem.lib.constants import OrchestrationConstants
from scipy.optimize import newton
from tiny_structured_logger.lib import fhs_logging

from fhs_pipeline_sevs.lib import rrmax as rrmax_module
from fhs_pipeline_sevs.lib.constants import PastSEVConstants

logger = fhs_logging.get_logger()


def get_product_on_right_hand_side(
    acause: str,
    cause_id: int,
    rei: str,
    gbd_round_id: int,
    past_or_future: str,
    sev_version: str,
    rr_version: str,
    draws: int,
    age_group_ids: Optional[List[int]] = None,
    draw_start: int = 0,
) -> xr.DataArray:
    r"""Compute the product term of the right-hand side of Equation (3) from the background_.

    .. _background: compute_intrinsic_background.html

    The right-hand side again:

    .. math::
        \left[ \left( RR_j^{max} \ - \ 1 \right) \ SEV_j^U \ + \ 1 \right] \
        \prod_{i=1}^n \left[ \left( RR_{i}^{M, max} \ - \ 1 \right) \ SEV_i \
        + \ 1\right]

    where :math:`i` stands for the antecedent risk.

    We take the product term as

    .. math::
        \prod_{i=1}^n \left( \ a_i \ k \ + 1 \right)

    and return :math:`a_i` as a dataarray with a risk-dimension.

    Args:
        acause (str): acause.
        cause_id (int): cause id for acause.
        rei (str): the mediator, risk j's formal name.
        gbd_round_id (int): gbd round id.
        past_or_future (str): "past" or "future".
        sev_version (str): version of where SEV is.
        rr_version (str): version of etl-ed RR.
        draws (int): number of draws kept in process.
        draw_start (Optional[int]): starting index of draws selected.

    Returns:
        xr.DataArray: The a_i terms of the product term.
    """
    # med_da dims are ('acause', 'rei', 'med_', 'draw')
    # want to filter to only slices pertaining to the acause-mediator pair
    med_da = get_mediation_matrix(gbd_round_id).sel(
        acause=acause, med=rei, draw=range(draw_start, draw_start + draws, 1)
    )

    risks = list(med_da["rei"].values)

    # Note that constructing a list of the single-risk arrays is not bad for memory.
    # Intuitively: The xr.concat function needs to have all the source arrays in memory at once
    # to copy them into the new, concatenated array, so it doesn't help to try to generate one
    # at a time.
    risk_a_is = [
        _single_risk_a_i(
            risk=risk,
            med_da=med_da,
            acause=acause,
            cause_id=cause_id,
            rei=rei,
            gbd_round_id=gbd_round_id,
            past_or_future=past_or_future,
            sev_version=sev_version,
            rr_version=rr_version,
            draws=draws,
            age_group_ids=age_group_ids,
            draw_start=draw_start,
        )
        for risk in risks
    ]
    a_is = xr.concat([a_i for a_i in risk_a_is if a_i is not None], dim="rei")

    a_is = a_is.fillna(0)

    return a_is


def _single_risk_a_i(
    risk: str,
    med_da: xr.DataArray,
    acause: str,
    cause_id: int,
    rei: str,
    gbd_round_id: int,
    past_or_future: str,
    sev_version: str,
    rr_version: str,
    draws: int,
    age_group_ids: Optional[List[int]],
    draw_start: int,
) -> Optional[xr.DataArray]:
    mediation_factor = med_da.sel(rei=risk)  # now should have only one dim: draw
    if mediation_factor.mean() <= 0:
        logger.info(f"{acause}-{rei} mediation is 0 for {risk}, skip...")
        return None

    logger.info("Computing for {} in product...".format(risk))

    sev_i = open_xr_scenario(
        FHSFileSpec(
            version_metadata=VersionMetadata.make(
                data_source=gbd_round_id,
                epoch=past_or_future,
                stage="sev",
                version=sev_version,
            ),
            filename=f"{risk}.nc",
        )
    ).sel(draw=range(draw_start, draw_start + draws, 1))

    if age_group_ids:  # if we stipulate age group ids to compute
        sev_age_ids = sev_i["age_group_id"].values

        # we don't need age group id 27.  Only need the detailed ones.
        keep_age_ids = list(set(age_group_ids) & set(sev_age_ids))

        sev_i = sev_i.sel(age_group_id=keep_age_ids)

    # Because mediation factor (mf) is the fraction of excess risk mediated,
    # mf ~ (RR_m - 1) / (RR - 1); hence RR_m ~ (RR - 1) * mf + 1
    rrmax_ij = rrmax_module.read_rrmax(
        acause, cause_id, risk, gbd_round_id, rr_version, draws, draw_start
    )

    a_i = _calculate_ai(mediation_factor, sev_i, rrmax_ij)

    a_i["rei"] = risk
    return a_i


def _calculate_ai(
    mediation_factor: xr.DataArray, sev_i: xr.DataArray, rrmax_ij: Union[xr.DataArray, int]
) -> xr.DataArray:
    rrmax_ij_med = (rrmax_ij - 1) * mediation_factor + 1
    a_i = (rrmax_ij_med - 1) * sev_i  # a_i constant for k-calculation
    return a_i


def newton_solver(
    a_is: xr.DataArray, b_const: xr.DataArray, initial_k: xr.DataArray
) -> xr.DataArray:
    """Find the k that satisfies the description under "The k Problem" in the background docs.

    Concretely, find the k such that the sum of log(k * a_is[r] + 1) across risks r is equal to
    log(b_const).

    Starts with a "guess" for k, which if too far off, the solving might not converge.

    Args:
        a_is (xr.DataArray): product of distal contributions, without k.
        b_const (xr.DataArray): total/intrinsic contribution.
        k (xr.DataArray): the initial k coefficients.

    Returns:
        (xr.DataArray): the final k coefficients.
    """
    k_coords = initial_k.coords

    a_is = a_is.fillna(0)

    def fun(k: np.ndarray) -> np.ndarray:  # the function to find root of
        val = sum(np.log(k * a_is.sel(rei=risk) + 1) for risk in a_is["rei"].values)
        val = val - np.log(b_const)
        return val

    def dfun(k: np.ndarray) -> np.ndarray:  # derivative of fun w/ k
        # deriv = (a_is / (a_is * k + 1)).sum(dim="rei")
        deriv = sum(
            a_is.sel(rei=risk) / (k * a_is.sel(rei=risk) + 1) for risk in a_is["rei"].values
        )
        return deriv

    def d2fun(k: xr.DataArray) -> xr.DataArray:
        return (-(a_is**2) / ((a_is * k + 1) ** 2)).sum(dim="rei")

    k = newton(
        fun,
        initial_k,
        fprime=dfun,
        tol=PastSEVConstants.TOL,
        rtol=PastSEVConstants.RTOL,
        maxiter=PastSEVConstants.MAXITER,
    )

    return xr.DataArray(k, coords=k_coords)


def main(
    acause: str,
    rei: str,
    gbd_round_id: int,
    sev_version: str,
    rr_version: str,
    draws: int,
    **kwargs: Any,
) -> None:
    """Compute and export intrinsic SEV.

    Args:
        acause (str): acause.
        rei (str): risk j fomr (1).
        gbd_round_id (int): gbd round id.
        sev_version (str): version of past SEV.
        rr_version (str): version of past RR.
        draws (int): number of draws kept in process.
    """
    cause_id = query_cause.get_cause_id(acause=acause)
    rei_id = query_risk.get_rei_id(rei=rei)

    # will filter to these detailed age group ids
    age_group_ids = get_ages(gbd_round_id=gbd_round_id)["age_group_id"].unique().tolist()

    logger.info("computing read in RRmax and SEV...")

    input_sev_version_metadata = VersionMetadata.make(
        data_source=gbd_round_id,
        epoch="past",
        stage="sev",
        version=sev_version,
    )

    chunk_size = PastSEVConstants.DRAW_CHUNK_SIZE  # compute k in chunks of 100 draws

    for i, draw_start in enumerate(range(0, draws, chunk_size)):
        rrmax = rrmax_module.read_rrmax(
            acause,
            cause_id,
            rei,
            gbd_round_id,
            rr_version,
            draws=chunk_size,
            draw_start=draw_start,
        )

        sev = open_xr_scenario(
            FHSFileSpec(version_metadata=input_sev_version_metadata, filename=f"{rei}.nc")
        ).sel(draw=range(draw_start, draw_start + chunk_size, 1))

        sev_age_ids = sev["age_group_id"].values.tolist()

        # we don't need age group id 27.  Only need the detailed ones.
        keep_age_ids = list(set(age_group_ids) & set(sev_age_ids))

        sev = sev.sel(age_group_id=keep_age_ids)

        logger.info(f"Read in RRmax and SEV chunk {i}. " "Now right hand product...")

        a_is = get_product_on_right_hand_side(
            acause,
            cause_id,
            rei,
            gbd_round_id,
            "past",
            sev_version,
            rr_version,
            draws=chunk_size,
            age_group_ids=age_group_ids,
            draw_start=draw_start,
        )

        # a_is can be missing age groups if a mediator has few distals
        # with different age restrictions than the mediator
        # EX. drugs_alcohol starts with age_group_id 6
        # but its mediators start with age_group_id 7
        a_is = expand_dimensions(a_is, age_group_id=keep_age_ids, fill_value=0)

        # We use a generator to produce the multiplicands to save memory.
        rei_product_i = reduce(
            lambda x, y: x * y,
            (a_is.sel(rei=risk) + 1 for risk in a_is["rei"].values.tolist()),
        )

        sev_intrinsic = ((((rrmax - 1) * sev + 1) / rei_product_i) - 1) / (rrmax - 1)

        del rei_product_i
        gc.collect()

        # we are setting negative SEV's to 0, to compute k
        sev_intrinsic_lifted = sev_intrinsic.where(sev_intrinsic >= 0).fillna(0)

        b_const = ((rrmax - 1) * sev + 1) / ((rrmax - 1) * sev_intrinsic_lifted + 1)

        del sev_intrinsic_lifted
        gc.collect()

        a_is = a_is.sel(age_group_id=b_const["age_group_id"].values)

        # we only want to update the parts where sev_intrinsic < 0 so we manipulate the initial
        # values to make sure only those are touched.
        b_const = b_const.where(sev_intrinsic < 0).fillna(1)  # so log(b) = 0

        # now compute the initial k guess
        rei_sum = a_is.sum(dim="rei")

        rei_sum = rei_sum.sel(age_group_id=b_const["age_group_id"].values)

        k_i = np.log(b_const) / rei_sum  # the initial k

        del rei_sum
        gc.collect()

        k_i = k_i.where(sev_intrinsic < 0).fillna(0)  # so log(a * k + 1) = 0
        k_i = k_i.where(k_i <= 1).fillna(0.9999)  # adjustment to avoid -inf k

        for dim in k_i.dims:  # make sure all indices are matched along axis
            b_const = b_const.sel(**{dim: k_i[dim].values.tolist()})
            a_is = a_is.sel(**{dim: k_i[dim].values.tolist()})

        # align everything to k_i's dim order before newton solver
        b_const = b_const.transpose(*k_i.dims)
        a_is = a_is.transpose(*[a_is.dims[0]] + list(b_const.dims))

        k_i = newton_solver(a_is, b_const, k_i)

        k_i = k_i.where(sev_intrinsic < 0).fillna(1)  # k=1 for "good" cells

        del sev_intrinsic, b_const  # free up some space in memory
        gc.collect()

        # need to re-compute sev with k
        rei_product_i = reduce(
            lambda x, y: x * y,
            (k_i * a_is.sel(rei=risk) + 1 for risk in a_is["rei"].values.tolist()),
        )

        if i == 0:
            k = k_i
            rei_product = rei_product_i
        else:
            k = xr.concat([k, k_i], dim="draw")
            rei_product = xr.concat(
                [rei_product, rei_product_i], dim="draw"
            )

        del a_is, k_i, rei_product_i
        gc.collect()

    # re-compute sev_intrinsic with k
    rrmax = rrmax_module.read_rrmax(
        acause, cause_id, rei, gbd_round_id, rr_version, draws=draws
    )

    sev = open_xr_scenario(
        FHSFileSpec(version_metadata=input_sev_version_metadata, filename=f"{rei}.nc")
    ).sel(age_group_id=keep_age_ids)
    sev = resample(sev, draws)

    sev_intrinsic = ((((rrmax - 1) * sev + 1) / rei_product) - 1) / (rrmax - 1)

    del rrmax, sev, rei_product
    gc.collect()

    # There will be some sev_intrinsic_final values that are slightly < 0
    # due to numerical precision issues.  We set them to 0 here.
    # For values less than -tol, we raise a flag.  We use tol as a threshold.
    if (sev_intrinsic < -PastSEVConstants.TOL).any():
        min_val = float(sev_intrinsic.min())
        logger.warning(
            f"There are values in final intrinsic SEV < -{PastSEVConstants.TOL}, "
            f"the min being {min_val}."
        )

    sev_intrinsic = sev_intrinsic.where(sev_intrinsic >= 0).fillna(0)
    sev_intrinsic = sev_intrinsic.where(sev_intrinsic <= 1).fillna(1)

    # let's remove some redundant point dims
    redundants = list(set(sev_intrinsic.coords.keys()) - set(sev_intrinsic.dims))
    redundants.remove("acause")  # we're keeping "acause"

    for redundant in redundants:  # only keep dims and "acause"
        k = k.drop_vars(redundant)
        sev_intrinsic = sev_intrinsic.drop_vars(redundant)

    output_dir_spec = FHSDirSpec(
        version_metadata=input_sev_version_metadata,
        sub_path=(OrchestrationConstants.SUBFOLDER,),
    )

    save_xr_scenario(
        xr_obj=k,
        file_spec=FHSFileSpec.from_dirspec(
            dir=output_dir_spec, filename=f"{acause}_{rei}_k_coeff.nc"
        ),
        metric="number",
        space="identity",
        cause_id=cause_id,
        rei_id=rei_id,
        gbd_round_id=gbd_round_id,
        sev_version=sev_version,
        rr_version=rr_version,
        tol=PastSEVConstants.TOL,
        rtol=PastSEVConstants.RTOL,
        maxiter=PastSEVConstants.MAXITER,
    )

    save_xr_scenario(
        xr_obj=sev_intrinsic,
        file_spec=FHSFileSpec.from_dirspec(
            dir=output_dir_spec, filename=f"{acause}_{rei}_intrinsic.nc"
        ),
        metric="rate",
        space="identity",
        cause_id=cause_id,
        rei_id=rei_id,
        gbd_round_id=gbd_round_id,
        sev_version=sev_version,
        rr_version=rr_version,
    )
