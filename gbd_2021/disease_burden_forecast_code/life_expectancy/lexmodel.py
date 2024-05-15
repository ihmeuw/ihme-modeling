"""Models of life expectancy.

Models of life expectancy take mortality rate as input and return
a dataset of life expectancy, mortality rate, and mean age of death.
They are models because they have to estimate the mean age of death.

* There are two kinds of models here, those for GBD 2015 and those
  for GBD 2016. They differ by what the input and output age group IDs are.

* For GBD 2015, the input age groups start with IDs (2, 3, 4) for ENN,
  PNN, and LNN. They end with IDs (20, 21) for 75-79 and 80+.

* For GBD 2016, the input age groups start with IDs (2, 3, 4) for ENN,
  PNN, and LNN. They end with IDs 235 for 95+.

* GBD 2017 uses the same age group IDs as 2016.

* For both 2015/2016, we predict life expectancy on young ages (28, 5),
  meaning 0-1 and 1-5. For older ages, we predict to age ID 148 for 110+.

* For 2017, we take forecasted mx straight up to compute the the life table.
  That is, no extrapolation/extension is done to ameliorate the older ages.
"""

import gc
import warnings
from typing import Tuple, Union

import numpy as np
import xarray as xr
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_database_interface.lib.constants import DimensionConstants
from fhs_lib_database_interface.lib.query import age
from tiny_structured_logger.lib.fhs_logging import get_logger

from fhs_lib_demographic_calculation.lib.constants import (
    AgeConstants,
    LexmodelConstants,
    LifeTableConstants,
)
from fhs_lib_demographic_calculation.lib.construct import (
    age_id_older_than,
    consistent_age_group_ids,
    nx_contiguous_round,
    nx_from_age_group_ids,
)
from fhs_lib_demographic_calculation.lib.lifemodel import (
    old_age_fit_qx_95_plus,
    old_age_fit_us_counties,
)
from fhs_lib_demographic_calculation.lib.lifetable import (
    ax_graduation_cubic,
    cm_mean_age,
    fm_mortality,
    fm_period_life_expectancy,
    fm_person_years,
    fm_population,
)

logger = get_logger()


def without_point_coordinates(
    ds: Union[xr.DataArray, xr.Dataset]
) -> Tuple[xr.DataArray, dict]:
    r"""Remove point coordinates and return them, so you can add them back later.

    The code would look like this:
        no_point, saved_coords = without_point_coordinates(ds)
        # Do things
        return results.assign_coords(\**saved_coords)

    Args:
        ds (Union[xr.DataArray, xr.Dataset]): A dataarray that may have point coords

    Returns:
        Tuple[xr.DataArray, dict]: The point coordinates are copied and returned.
    """
    point = dict(
        (pname, ds.coords[pname].values.copy())
        for pname in ds.coords
        if ds.coords[pname].shape == ()
    )
    return ds.drop_vars(list(point)), point


def append_nLx_lx(ds: xr.Dataset) -> xr.Dataset:
    r"""Adds :math:`{}_nL_x` and :math:`l_x` to the dataset.

    It's sometimes called :math:`{}_nU_x`.

    Args:
        ds (xr.Dataset): Dataset containing mx and ax

    Returns:
        xr.DataSet: ds (xr.Dataset): Dataset containing mx and ax
    """
    nx = nx_from_age_group_ids(ds.mx.age_group_id)
    nLx = fm_person_years(ds.mx, ds.ax, nx)
    lx, dx = fm_population(
        ds.mx, ds.ax, nx, LifeTableConstants.DEFAULT_INITIAL_POPULATION_SIZE
    )
    return ds.assign(nLx=nLx, lx=lx)


def under_5_ax_preston_rake(
    gbd_round_id: int,
    mx: xr.DataArray,
    ax: xr.DataArray,
    nx: xr.DataArray,
) -> xr.DataArray:
    r"""Uses :math:`m_x` and Preston's Table 3.3 to compute ax for FHS under-5 age groups.

    The steps are as follows:

    (1) Aggregate neonatal mx's to make :math:`{}_1m_0`
        This assumes the nLx values are approximately correct.

    (2) Use Preston's Table 3.3 to compute :math:`{}_1a_0`
        using :math:`{}_1m_0`

    (3) "Rake" the ``*neonatal`` ax's such that they aggregate to :math:`{}_1a_0`.
        We start with the definition

        .. math::
            {}_1a_0 = \frac{a_2 \ d_2 + (a_3 + n_2) \ d_3 + (a_4 + n_2 + n_3)
                            \ d_4}{d_2 + d_3 + d_4}

        where subscripts 2, 3, and 4 denote enn, lnn, and pnn age groups.

        The above equation can be rewritten as

        .. math::
            {}_1a_0 - \frac{n_2 \ d_3 + (n_2 + n_3) \ d_4}{d_2 + d_3 + d_4} =
            \frac{a_2 \ d_2 + a_3 \ d_3 + a_4 \ d_4}{d_2 + d_3 + d_4}

        Because we have :math:`{}_1a_0` and assume that the dx values are
        approximately correct, the left-hand side is fixed at this point.
        The entire right-hand side, where the :math:`{}_na_x` values are,
        needs to be "raked" to satisfy the above equation.  We simply multiply
        all three :math:`{}_na_x` by the same ratio.

    Args:
        mx (xr.DataArray): forecasted mortality rate.
        ax (xr.DataArray): ax assuming constant mortality for under 5 age
            groups.
        nx (xr.DataArray): nx for FHS age groups, in years.

    Raises:
        ValueError: If under 5 age group is not a subset of the `mx` data

    Returns:
        xr.DataArray: ax, with under-5 age groups (2, 3, 4, 5) optimized.
    """
    age_group_ids_under_5_years_old = age.get_most_detailed_age_group_ids_in_age_spans(
        gbd_round_id=gbd_round_id,
        start=0,
        end=5,
        include_birth_age_group=False,
    )
    if not set(age_group_ids_under_5_years_old).issubset(
        mx[DimensionConstants.AGE_GROUP_ID].values
    ):
        raise ValueError(
            f"age group {age_group_ids_under_5_years_old} is not "
            f"subset of {mx[DimensionConstants.AGE_GROUP_ID].values}"
        )

    # first compute the approximately correct dx and nLx values.
    # they are "approximately" correct because they's not sensitive to ax.
    lx, dx = fm_population(mx, ax, nx, 1.0)
    nLx = fm_person_years(mx, ax, nx)

    under_1_dx_sum = dx.sel(age_group_id=AgeConstants.AGE_GROUP_IDS_UNDER_ONE).sum(
        DimensionConstants.AGE_GROUP_ID
    )

    ax4 = ax.sel(age_group_id=AgeConstants.AGE_1_TO_4_ID)  # the 1-4yr ax

    mx_u5 = make_under_one_group_for_preston_with_nLx(mx, nLx)  # 1m0 and 4m1
    # now compute the 1a0 and 4a1 from Preston's Table 3.3
    ax_u5 = preston_ax_fit(mx_u5)  # 2 age groups: 0-1 (28) and 1-5 (5)
    ax1_p = ax_u5.sel(age_group_id=AgeConstants.AGE_UNDER_ONE_ID)  # Preston's 1a0
    ax4_p = ax_u5.sel(age_group_id=AgeConstants.AGE_1_TO_4_ID)  # Preston's 4a1

    left_hand_side = (
        ax1_p
        - (
            nx.sel(age_group_id=2) * dx.sel(age_group_id=3)
            + nx.sel(age_group_id=[2, 3]).sum(DimensionConstants.AGE_GROUP_ID)
            * dx.sel(age_group_id=4)
        )
        / under_1_dx_sum
    )

    right_hand_side_sum = 0
    for age_group_id in AgeConstants.AGE_GROUP_IDS_UNDER_ONE:
        right_hand_side_sum += ax.sel(age_group_id=age_group_id) * dx.sel(
            age_group_id=age_group_id
        )

    right_hand_side = right_hand_side_sum / under_1_dx_sum

    ax1_raking_const = left_hand_side / right_hand_side
    ax1_raking_const[DimensionConstants.AGE_GROUP_ID] = AgeConstants.AGE_UNDER_ONE_ID

    ax4_ratio = ax4_p / ax4

    # now modify our under-5 ax values
    ax.loc[dict(age_group_id=AgeConstants.AGE_GROUP_IDS_UNDER_ONE)] = (
        ax.sel(age_group_id=AgeConstants.AGE_GROUP_IDS_UNDER_ONE) * ax1_raking_const
    )
    ax.loc[dict(age_group_id=AgeConstants.AGE_1_TO_4_ID)] = (
        ax.sel(age_group_id=AgeConstants.AGE_1_TO_4_ID) * ax4_ratio
    )

    # safeguard against wild ax values, as a result of wild mx values
    for age_group_id in AgeConstants.AGE_GROUP_IDS_UNDER_ONE + [AgeConstants.AGE_1_TO_4_ID]:
        ax_age = ax.sel(age_group_id=age_group_id)
        nx_age = nx.sel(age_group_id=age_group_id)
        ax.loc[dict(age_group_id=age_group_id)] = ax_age.where(ax_age <= nx_age).fillna(
            nx_age / 2
        )

    return ax


def make_under_one_group_for_preston_with_nLx(
    mx: xr.DataArray,
    nLx: xr.DataArray,
) -> xr.DataArray:
    r"""Create an under one age group (id 28).

    From the 0-6 days, 7-27 days, and 28-364 days (ids 2,3,4) groups. This age group is needed
    for the Preston young age fit. If this is called with scalar, string dimensions on
    mx, then slicing won't work and it will fail.

    Note that this method using nLx as the weight, instead of nx, based
    on the definition

    .. math::
        {}_nm_x = \frac{{}_nd_x}{{}_nL_x}

    Args:
        mx (xr.DataArray): Mortality data with age groups 2, 3, 4, and 5.
        nLx (xr.DataArray): nLx that has age groups 2, 3, and 4

    Returns:
        xr.DataArray: mx for the under one age group (28) and
            the 1-4 years age group (5).

    Raises:
        RuntimeError: if age groups ids 2, 3, and 4 are NOT in input.
        ValueError: if `mx_under_on` contains unexpected age group ids
    """
    if all(age_x in mx.age_group_id.values for age_x in AgeConstants.AGE_GROUP_IDS_UNDER_ONE):
        nLx_sum = nLx.sel(age_group_id=AgeConstants.AGE_GROUP_IDS_UNDER_ONE).sum(
            DimensionConstants.AGE_GROUP_ID
        )
        mx_other_ages = mx.loc[dict(age_group_id=[AgeConstants.AGE_1_TO_4_ID])]

        mx_under_one_sum = 0
        for age_group_id in AgeConstants.AGE_GROUP_IDS_UNDER_ONE:
            mx_under_one_sum += nLx.sel(age_group_id=age_group_id) * mx.sel(
                age_group_id=age_group_id
            )

        mx_under_one = mx_under_one_sum / nLx_sum
        mx_under_one.coords[DimensionConstants.AGE_GROUP_ID] = AgeConstants.AGE_UNDER_ONE_ID
        mx_under_one = xr.concat(
            [mx_under_one, mx_other_ages],
            dim=DimensionConstants.AGE_GROUP_ID,
        )

        if not np.array_equal(
            mx_under_one.age_group_id,
            AgeConstants.GBD_AGE_UNDER_FIVE,
        ):
            raise ValueError(
                f"mx_under_one contains unexpected "
                f"age group ids {mx_under_one.age_group_id}"
            )

        return mx_under_one
    elif all(age_x in mx.age_group_id.values for age_x in AgeConstants.GBD_AGE_UNDER_FIVE):
        return mx.sel(age_group_id=AgeConstants.GBD_AGE_UNDER_FIVE)
    else:
        raise RuntimeError("No known young ages in input")


def preston_ax_fit(mx: xr.DataArray) -> xr.DataArray:
    r"""This fit is from Preston, Heuveline, and Guillot, Table 3.3.

    It comes from a fit that Coale-Demeney made in their life tables for
    :math:`({}_1a_0, {}_4a_1)` from `{}_1q_0`. PHG turned it into a fit from :math:`m_x` to
    :math:`a_x`. This is explored in a notebook in docs to ``fbd_core.demog``.

    Args:
        mx (xr.DataArray): Mortality rate. Must have age group IDs (28,5).

    Raises:
        ValueError: if an incorrect age group id is in `mx`

    Returns:
        xr.DataArray: Mean age, for only those age groups where predicted.
    """
    if not all(ax in mx.age_group_id.values for ax in AgeConstants.GBD_AGE_UNDER_FIVE):
        raise ValueError(f"Incorrect input with age ID {mx.age_group_id.values}")

    sexes = mx.sex_id
    msub = mx.loc[dict(age_group_id=AgeConstants.GBD_AGE_UNDER_FIVE)]
    msub_above = msub.where(msub >= LexmodelConstants.MCUT)
    msub_below = msub.where(msub < LexmodelConstants.MCUT)

    ax_fit_above = msub_above * LexmodelConstants.PHG.sel(
        domain="above", const="m", sex_id=sexes
    ) + LexmodelConstants.PHG.sel(domain="above", const="c", sex_id=sexes)
    ax_fit_below = msub_below * LexmodelConstants.PHG.sel(
        domain="below", const="m", sex_id=sexes
    ) + LexmodelConstants.PHG.sel(domain="below", const="c", sex_id=sexes)

    ax_fit = ax_fit_above.combine_first(ax_fit_below)

    return ax_fit


def gbd5_no_old_age_fit(mx: xr.DataArray) -> xr.Dataset:
    """This is based on :func:`fbd_research.lex.model.gbd4_all_youth`.

    Except that age groups [31, 32, 235] are not replaced with fitted values.

    Args:
        mx (xr.DataArray): Mortality rate

    Raises:
        RuntimeError: if `mx` is missing some age groups
        ValueError: if age group ids in `mx` are not consistent

    Returns:
        xr.Dataset: Period life expectancy.
    """
    hard_coded_gbd_round_id = 5
    nx_gbd = nx_contiguous_round(gbd_round_id=hard_coded_gbd_round_id)

    try:
        mx = mx.sel(age_group_id=nx_gbd[DimensionConstants.AGE_GROUP_ID].values)
    except KeyError:
        raise RuntimeError(
            f"Not all ages in incoming data. Have {mx.age_group_id.values} "
            f"Want {nx_gbd.age_group_id.values}"
        )

    if not consistent_age_group_ids(mx.age_group_id.values):
        raise ValueError(f"age group id {mx.age_group_id.values} are not consistent")

    nx_base = nx_from_age_group_ids(mx.age_group_id)

    # We want a baseline on nulls. If there is one null in a draw, we
    # null out the whole draw, so this algorithm may increase null
    # count, but it won't increase the bounding box on nulls.
    mx_null_count = mx.where(mx.isnull(), drop=True).size
    if mx_null_count > 0:
        warnings.warn(f"Incoming mx has {mx_null_count} nulls")

    # first compute ax assuming constant mortality over interval
    ax = cm_mean_age(mx, nx_base)

    # Graduation applies only where it's 5-year age groups.
    middle_ages = nx_base.where(nx_base == nx_base.median()).dropna(
        dim=DimensionConstants.AGE_GROUP_ID
    )
    graduation_ages = middle_ages.age_group_id.values
    # graduation ages are the age group ids that have 5-year neighbors:
    # array([ 6,  7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30,
    #         31, 32])
    # Note that 6 and 32 will remain unchanged through graduation method,
    # because they do not have 5-year neighbors on both sides

    # use graduation method from Preston to fine-tune ax using neighboring
    # ax values.  ax at age groups [2-6, 32, 235] will be set to cm_mean_age
    ax = ax_graduation_cubic(mx, ax, nx_base, graduation_ages)

    # now fix the under-5 age groups to match GBD methodology
    ax = under_5_ax_preston_rake(hard_coded_gbd_round_id, mx, ax, nx_base)

    ex = fm_period_life_expectancy(mx, ax, nx_gbd)

    ex_null_count = ex.where(ex.isnull(), drop=True).size
    if ex_null_count > mx_null_count:
        ex_null = ex.where(ex.isnull(), drop=True)
        raise RuntimeError(f"Graduation created {ex_null.coords} nulls")

    ds = xr.Dataset(dict(ex=ex, mx=mx, ax=ax))

    return append_nLx_lx(ds)


def gbd4_all_youth(mx: xr.DataArray) -> xr.Dataset:
    """Period life expectancy that matches GBD 2016 except young ages aren't fit.

    So age groups ENN, PNN, and LNN remain. This is called
    the "baseline" set of ages, not the "lifetable" set of ages.
    Input ages include ENN, PNN, and LNN, and the terminal age group
    is 95+. This includes the old age fit from US Counties code
    and uses the graduation method.

    Args:
        mx (xr.DataArray): Mortality rate

    Raises:
        RuntimeError: `mx` is missing some age groups
        ValueError: `mx` contains inconsistent age group ids, or `qxp` contains values outside
            what is expected, or `qxp` contains values outside what is expected

    Returns:
        xr.Dataset: Period life expectancy.
    """
    nx_gbd = nx_contiguous_round(gbd_round_id=4)
    # nx_gbd looks like array([1.917808e-02, 5.753425e-02, 9.232877e-01,
    # 4.000000e+00, 5.000000e+00, ... 5.000000e+00, 5.000000e+00, 4.500000e+01]
    try:
        mx = mx.loc[dict(age_group_id=nx_gbd.age_group_id.values)]
    except KeyError:
        raise RuntimeError(
            f"Not all ages in incoming data "
            f"have {mx.age_group_id.values} "
            f"want {nx_gbd.age_group_id.values}"
        )
    if not consistent_age_group_ids(mx.age_group_id.values):
        raise ValueError("`mx` contains inconsistent age group ids.")
    nx_base = nx_from_age_group_ids(mx.age_group_id)
    # >>> nx_base['age_group_id']
    # <xarray.DataArray 'age_group_id' (age_group_id: 21)>
    # array([ 28,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16,
    #         17,  18,  19,  20,  30,  31,  32, 235])
    # 28 is 1 year wide, 5 is 4 years wide

    # We want a baseline on nulls. If there is one null in a draw, we
    # null out the whole draw, so this algorithm may increase null
    # count, but it won't increase the bounding box on nulls.
    mx_null_count = mx.where(mx.isnull(), drop=True).size
    if mx_null_count > 0:
        warnings.warn(f"Incoming mx has {mx_null_count} nulls")
    expected_good = mx.size - mx_null_count

    # first compute ax assuming constant mortality over interval
    ax = cm_mean_age(mx, nx_base)
    # Graduation applies only where it's 5-year age groups.
    middle_ages = nx_base.where(nx_base == nx_base.median()).dropna(
        dim=DimensionConstants.AGE_GROUP_ID
    )
    graduation_ages = middle_ages.age_group_id.values
    # graduation ages are the age group ids that have 5-year neighbors:
    # array([ 6,  7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30,
    #         31, 32])

    # use graduation method from Preston to fine-tune ax using neighboring
    # ax values.  The boundary ax's will be set to cm_mean_age
    ax = ax_graduation_cubic(mx, ax, nx_base, graduation_ages)

    # Mortality from mx is interval-by-interval, so we
    # can leave young out of it.
    # mortality {}_nq_x = \frac{m_x n_x}{1+ m_x(n_x - a_x)}
    qxp = fm_mortality(mx, ax, nx_base)
    if not (qxp < 1.0001).sum() >= expected_good:
        raise ValueError("`qxp` contains values outside what is expected")
    if not (qxp > -0.0001).sum() >= expected_good:
        raise ValueError("`qxp` contains values outside what is expected")
    qxf, axf, mxf = old_age_fit_us_counties(qxp)
    # qxp['age_group_id'] == [2..20, 30..32, 235]
    # qxf['age_group_id'] == [ 31,  32,  33,  44,  45, 148]
    del qxp  # frees up 1G for 100 draws
    gc.collect()

    # This goes past the 95+ limit, so reduce it.
    mxp, axp = condense_ages_to_terminal(
        qxf, axf, AgeConstants.AGE_95_TO_100_ID, AgeConstants.AGE_95_PLUS_ID
    )
    v = mxf.age_group_id.values  # array([ 31,  32,  33,  44,  45, 148])
    term = np.argwhere(v == AgeConstants.AGE_95_TO_100_ID)[0][0]  # 2
    mxf_drop = xr.concat(
        [mxf.loc[dict(age_group_id=v[:term])], mxp], dim=DimensionConstants.AGE_GROUP_ID
    )  # 31, 32, 235
    axf_drop = xr.concat(
        [axf.loc[dict(age_group_id=v[:term])], axp], dim=DimensionConstants.AGE_GROUP_ID
    )  # 32, 32, 235

    del qxf, axf, mxf
    gc.collect()

    mx_combined = combine_age_ranges(None, mx, mxf_drop)
    ax_combined = combine_age_ranges(None, ax, axf_drop)
    if not (mx_combined.age_group_id.values[-5:] == nx_gbd.age_group_id.values[-5:]).all():
        raise ValueError("`mx_combined` and `nx_gbd` age groups aren't aligned")

    del ax, mx  # frees up 2G for 100 draws
    gc.collect()

    ex = fm_period_life_expectancy(mx_combined, ax_combined, nx_gbd)

    ex_null_count = ex.where(ex.isnull(), drop=True).size
    if ex_null_count > mx_null_count:
        ex_null = ex.where(ex.isnull(), drop=True)
        raise RuntimeError(f"graduation introduced null draws with bounds {ex_null.coords}")

    ds = xr.Dataset(dict(ex=ex, mx=mx_combined, ax=ax_combined))

    del mx_combined, ax_combined  # frees up 2G for 100 draws
    gc.collect()

    return append_nLx_lx(ds)


def combine_age_ranges(
    young: xr.DataArray,
    middle: xr.DataArray,
    old: xr.DataArray,
) -> xr.DataArray:
    """Combine differenr age groups.

    Three Dataarrays have different age groups in them.
    Combine them to make one dataarray. There may be overlap
    among age_group IDs, so anything in the middle
    is overwritten by the young or old. The dims and coords in the middle
    are used to determine the dims and coords in
    output. Point coordinates are kept.
    This works even when young, middle, and old have different age intervals,
    for instance [28, 5] versus [2, 3, 4].

    Args:
        young (xr.DataArray): fit for young ages, or None if there is no fit
        middle (xr.DataArray): middle age groups
        old (xr.DataArray): fit for old ages

    Returns:
        xr.DataArray: With dims in the same order as the middle.
    """
    if young is not None:
        with_young = age_id_older_than(
            young.age_group_id[-1].values.tolist(), middle.age_group_id.values
        )
        young_ages = young.age_group_id.values
    else:
        with_young = middle.age_group_id.values
        young_ages = "none"
    # The age IDs are just IDs, and younger ages may have larger IDs, so sort.
    edge_ages = age_id_older_than(old.age_group_id[0].values.tolist(), with_young, True)
    logger.debug(
        f"combine_age_ranges young {young_ages} "
        f"middle {middle.age_group_id.values} "
        f"old {old.age_group_id.values} "
        f"keep {edge_ages}"
    )
    mid_cut = middle.loc[{DimensionConstants.AGE_GROUP_ID: edge_ages}]
    if young is not None:
        return xr.concat([young, mid_cut, old], dim=DimensionConstants.AGE_GROUP_ID)
    else:
        return xr.concat([mid_cut, old], dim=DimensionConstants.AGE_GROUP_ID)


def condense_ages_to_terminal(
    qx: xr.DataArray,
    ax: xr.DataArray,
    terminal: int,
    new_terminal: int,
) -> Tuple[xr.DataArray, xr.DataArray]:
    r"""Given a life table that includes later ages, truncate it to a terminal age group.

    For the terminal age group, we know
    :math:`{}_nm_x = 1/{}_na_x`, so use that as our guide.

    .. math::

       {}_na_x = {}_na_{x_0} + {}_np_{x_0}(n_{x_0}+{}_na_{x_1})
       +{}_np_{x_0}\:{}_np_{x_1}(n_{x_0}+n_{x_1}+{}_na_{x_2})

    Note we get :math:`q_x` and return :math:`m_x`.

    Args:
        qx (xr.DataArray): Mortality with age group ids.
        ax (xr.DataArray): Mean age of death.
        terminal (int): Which age group will become the terminal one.
        new_terminal (int): The age group to assign to the last interval.

    Raises:
        ValueError: unexpected `v.shape`

    Returns:
         Tuple[xr.DataArray, xr.DataArray]:
            mx: :math:`{}_nm_x` for the terminal age group.
            ax: :math:`{}_na_x` for the terminal age group.
    """
    v = qx.age_group_id.values
    term = np.argwhere(v == terminal)[0][0]
    if not term + 1 < v.shape[0]:
        raise ValueError("unexpected `v.shape`")
    axp = ax.loc[dict(age_group_id=[terminal])]
    # This will be survival from the terminal age group
    # to the current age group in the loop.
    npx = 1 - qx.loc[dict(age_group_id=[terminal])]
    # This will be the total time from the terminal age group
    # to the current age group in the loop.
    nx = nx_from_age_group_ids(ax.age_group_id)
    # This is a C-number, no index.
    nx_running = float(nx.loc[dict(age_group_id=[terminal])])
    logger.debug(f"nx_running {type(nx_running)} {nx_running}")

    for avx in v[term + 1 :]:
        # Have to set indices so that multiplication can happen.
        axp.coords[DimensionConstants.AGE_GROUP_ID] = [avx]
        npx.coords[DimensionConstants.AGE_GROUP_ID] = [avx]
        # Here, the invariant is now true.
        # npx is \prod_{i<avx} {}_np_{x_i}
        # nx_running = \sum_{i<avx} n_{x_i}
        axp += npx * (ax.loc[dict(age_group_id=[avx])] + nx_running)
        npx *= 1 - qx.loc[dict(age_group_id=[avx])]
        # This is a C-number, no index.
        nx_running += float(nx.loc[dict(age_group_id=[avx])])

    axp.coords[DimensionConstants.AGE_GROUP_ID] = [new_terminal]
    return 1 / axp, axp


def demography_team_extrapolation_of_lx(mx: xr.DataArray, ax: xr.DataArray) -> xr.DataArray:
    r"""Computes diff of logit-:math:`q_x` iteratively.

    Starting from age 90-94 (id 32), to compute the :math:`q_x` for 95-99 (id 33), 100-104
    (id 44), 105-109 (id 45).

    Starting with :math:`{}_5q_{90}`, one first computes

    .. math::
        \Delta_{90} = c_{90} + \beta_{90} + \beta\_logit\_q_{90} *
                      \text{logit}(q_{90})

    where :math:`c`, :math:`\beta`, and :math:`\beta\_logit\_q_x` are all
    regression constants available in
    :func:`fbd_core.demog.lifemodel.old_age_fit_qx_95_plus`.
    From then, for every sex, we have::

        for i in [95, 100]:

    .. math::
        q_i = \text{expit}( \text{logit}(q_{i-5}) + \Delta_{i-5} )

    .. math::
        \Delta_i = c_i + \beta_i + \beta\_logit\_q_{90} * \text{logit}(q_{90})

    Args:
        mx (xr.DataArray): mortality rate, with age_group_id dim.
        ax (xr.DataArray): ax.

    Returns:
        (xr.DataArray): lx where age group id 235 is replaced with
            age group ids 33 (95-100), 44 (100-105), 45 (105-110),
            and 148 (110+, where lx is set to 0).
    """
    # here we compute qx from mx
    nx_base = nx_from_age_group_ids(mx["age_group_id"])

    # make a baseline qx assuming constant mortality
    qx = fm_mortality(mx, ax, nx_base)

    # replace age group id 235 of qx with (33, 44) via demog team extrapolation
    qx = old_age_fit_qx_95_plus(qx)  # overwrite qx
    # make lx from qx
    lx = _qx_to_lx(qx)
    # Another adjustment based on self-consistency requirement
    qx = _self_consistent_qx_adjustment(qx, lx, mx)  # overwrite qx
    lx = _qx_to_lx(qx)  # make lx again, based on adjusted qx

    return lx


def _qx_to_lx(qx: xr.DataArray) -> xr.DataArray:
    r"""Computes :math:`l_x` based on :math:`q_x`.

    Where :math:`q_x` already contains the 95-100 (33) and 100-105 (44) age groups.  Also
    computes :math:`l_x` for 105-110 (45), and then set :math:`l_x` for 110+ to be 0.

    Args:
        qx (xr.DataArray): Probability of dying.

    Raises:
        ValueError: if `qx` is missing ages 33 and/or 34, or if `lx` has an unexpected number
            of age group ids, or if final lx should have age group ids 33, 44, 45, and 148.

    Returns:
        (xr.DataArray): lx.
    """
    if tuple(qx["age_group_id"].values[-2:]) != (
        AgeConstants.AGE_95_TO_100_ID,
        AgeConstants.AGE_100_TO_105_ID,
    ):
        raise ValueError("qx must have age group ids 33 and 44")

    px = 1.0 - qx  # now we have survival all the way to 100-105 (44) age group

    # Because l{x+n} = lx * px, we can compute all lx's if we start with
    # l_0 = 1 and iteratively apply the px's of higher age groups.
    # So we compute l_105-110, since we have p_100-105 from extrapolated qx.
    # We start with a set of lx's that are all 1.0
    lx = xr.full_like(px, 1)
    # now expand lx to have age groups 105-110 (45)
    lx = expand_dimensions(lx, fill_value=1, age_group_id=[AgeConstants.AGE_105_TO_110_ID])

    # Since l{x+n} = lx * px, we make cumulative prduct of px down age groups
    # and apply the product to ages[1:] (since ages[0]) has lx = 1.0
    ages = lx["age_group_id"]

    ppx = px.cumprod(dim="age_group_id")  # the cumulative product of px
    ppx.coords["age_group_id"] = ages[1:]  # need to correspond to ages[1:]
    lx.loc[dict(age_group_id=ages[1:])] *= ppx  # lx all the way to 100-105

    # now artificially sets lx to be 0 for the 110+ age group.
    lx = expand_dimensions(lx, fill_value=0, age_group_id=[AgeConstants.AGE_110_PLUS_ID])

    if not (lx.sel(age_group_id=2) == 1).all():
        raise ValueError("`lx` has an unexpected number of age group ids")
    if not tuple(lx["age_group_id"].values[-4:]) == (
        AgeConstants.AGE_95_TO_100_ID,
        AgeConstants.AGE_100_TO_105_ID,
        AgeConstants.AGE_105_TO_110_ID,
        AgeConstants.AGE_110_PLUS_ID,
    ):
        raise ValueError("final lx should have age group ids 33, 44, 45, and 148.")

    return lx


def _self_consistent_qx_adjustment(
    qx: xr.DataArray,
    lx: xr.DataArray,
    mx: xr.DataArray,
) -> xr.DataArray:
    r"""A universal relationship exists between the following formula.

    :math:`l_x`, :math:`q_x`, and :math:`m_x` at the terminal age group (95+):

    .. math::
        {}_{\infty}m_{95} = \frac{l_{95}}{T_{95}}

    where :math:`T_{95} = \int_{95}^{\infty} l_x dx`.

    Because we forecast :math:`l_{95}` and :math:`m_{95}`, and that
    :math:`l_{x}` for :math:`x > 95` is extrapolated independently
    (via :func:`fbd_research.lex.model.demography_team_extrapolation_of_lx`),
    the above relationship does not hold.  We therefore need to adjust our
    values of :math:`_5l_{100}`, :math:`{}_5l_{105}` so that the relationship
    holds.

    Note that we set :math:`l_{110} = 0`.

    We begin with the approximation of :math:`T_{95}`,
    using Simpson's 3/8 rule:

    .. math::
        T_{95} = \int_{95}^{\infty} \ l_{x} dx \
               \approx \ \frac{3 \ n}{8}({}_5l_{95} + 3 {}_5l^{\prime}_{100} +
                                         3 {}_5l^{\prime}_{105} +
                                         3 {}_5l^{\prime}_{110}) \
               = \ T^{\prime}_{95}

    where :math:`n` is 5 years, the age group bin size, and :math:`{}^{\prime}`
    denotes the current tentative value.
    Also note that :math:`{}_5l_{110} = 0` in our case.

    The above formula allows us to define

    .. math::
        \alpha &= \frac{T_{95}}{T^{\prime}_{95}} \\
               &= \frac{\frac{l_{95}}{m_{95}}}{T^{\prime}_{95}}
        :label: 1

    as a "mismatch factor".

    We also declare that the ratio between :math:`{}_5q_{95}`
    and :math:`{}_5q_{100}` is fixed:

    .. math::
        \beta = \frac{{}_5q_{95}}{{}_5q_{100}}
              = \frac{{}_5q_{95}^{\prime}}{{}_5q_{100}^{\prime}}
              < 1
        :label: 2

    Hence we may proceed with the following derivation:

    .. math::
        {}_5l_{100} &= {}_5l_{95} \ (1 - {}_5q_{95}) \\
                    &= {}_5l_{95} \ (1 - \beta \ {}_5q_{100})
        :label: 3

    .. math::
        {}_5l_{105} &= {}_5l_{100} \ (1 - {}_5q_{100}) \\
                    &= {}_5l_{95} \ (1 - {}_5q_{95}) \ (1 - {}_5q_{100}) \\
                    &= {}_5l_{95} \ (1 - \beta \ {}_5q_{100}) \
                                    (1 - {}_5q_{100})
        :label: 4

    .. math::
        \alpha &\approx \frac{\frac{15}{8} \ ({}_5l_{95} + 3 \ {}_5l_{100} +
                        3 \ {}_5l_{105})}{\frac{15}{8} ( {}_5l_{95} +
                        3 \ {}_5l^{\prime}_{100} + 3 \ {}_5l^{\prime}_{105})}\\
               &= \frac{{}_5l_{95} + 3 \ {}_5l_{95} \ (1 - {}_5q_{95}) +
                  3 \ {}_5l_{95} \ (1 - {}_5q_{95})(1 - {}_5q_{100})}
                  {{}_5l_{95} + 3 \ {}_5l_{95}(1 - {}_5q^{\prime}_{95}) +
                   3 \ {}_5l_{95}(1 - {}_5q^{\prime}_{95})
                                 (1 - {}_5q^{\prime}_{100})} \\
               &= \frac{{}_5l_{95} + 3 \ {}_5l_{95} \ (1 - \beta \ {}_5q_{100})
                  + 3 \ {}_5l_{95} \ (1 - \beta \ {}_5q_{100})
                                     (1 - {}_5q_{100})}
                  {{}_5l_{95} +
                   3 \ {}_5l_{95} \ (1 - \beta \ {}_5q^{\prime}_{100}) +
                   3 \ {}_5l_{95} \ (1 - \beta \ {}_5q^{\prime}_{100})
                                 (1 - {}_5q^{\prime}_{100})} \\
               &= \frac{1 + 3 \ (1 - \beta \ {}_5q_{100}) +
                  3 \ (1 - \beta \ {}_5q_{100})(1 - {}_5q_{100})}
                  {1 + 3 \ (1 - \beta \ {}_5q^{\prime}_{100}) +
                   3 \ (1 - \beta {}_5q^{\prime}_{100})
                       (1 - {}_5q^{\prime}_{100})} \\
               &= \frac{4 - 3 \ \beta \ {}_5q_{100} + 3 - 3 \ {}_5q_{100} -
                  3 \ \beta \ {}_5q_{100} + 3 \ \beta \ {{}_5q_{100}}^2}
                  {4 - 3 \ \beta {}_5q^{\prime}_{100} + 3 -
                   3 \ {}_5q^{\prime}_{100} -
                   3 \ \beta \ {}_5q^{\prime}_{100} +
                   3 \ \beta \ {{}_5q^{\prime}_{100}}^2} \\
               &= \frac{\frac{7}{3} - (2 \ \beta + 1) \ {}_5q_{100} +
                  \beta \ {{}_5q_{100}}^2}{\frac{7}{3} -
                  (2 \ \beta + 1) \ {}_5q^{\prime}_{100} +
                  \beta \ {{}_5q^{\prime}_{100}}^2}
        :label: 5

    where the denominator is known.
    If we define

    .. math::
        \gamma = \frac{7}{3} - \alpha \ (\frac{7}{3} -
                 (2 \beta + 1) \ {}_5q^{\prime}_{100} +
                 \beta \ {{}_5q^{\prime}_{100}}^2)
        :label: 6

    then we have the quadratic equation

    .. math::
        \beta \ {{}_5q_{100}}^2 - (2 \beta + 1) \ {}_5q_{100} + \gamma = 0
        :label: 7

    with the solution

    .. math::
        {}_5q_{100} = \frac{(2 \ \beta + 1) \pm \sqrt{(2 \ \beta + 1)^2 -
                      4 \beta \ \gamma}}{2 \ \beta}
        :label: 8

    Because :math:`{}_5q_{100} \leq 1`, subtraction in the numerator of :eq:`8`
    is the only viable solution.

    Args:
        qx (xr.DataArray): qx that has age groups (..., 33, 44),
            with 33 (95-100) and 44 (100-105).  Should not have 235 (95+).
        lx (xr.DataArray): lx that has age groups (..., 33, 44, 45, 148),
            with 45 (105-110) and 148 (110+),
            where (lx.sel(age_group_id=148) == 0).all().
        mx (xr.DataArray): mx, needed to compute :math: `T_{95}`.
            Has age_group_id=235 (95+) instead of (33, 44, 45, 148).

    Returns:
        (xr.DataArray): qx where age groups 33 (95-100) and 44 (100-105) are
            "adjusted".
    """
    n = 5.0  # 5 year age group width
    # 33 = 95-100 yrs, 44 = 100-105 yrs, 45 = 105-110 yrs, 148 = 110+ yrs.
    T_95_prime = lx.sel(age_group_id=AgeConstants.AGE_95_TO_100_ID) * (3.0 / 8.0 * n) + lx.sel(
        age_group_id=AgeConstants.AGE_100_TO_110
    ).sum(DimensionConstants.AGE_GROUP_ID) * (9.0 / 8.0 * n)
    alpha = (
        lx.sel(age_group_id=AgeConstants.AGE_95_TO_100_ID)
        / mx.sel(age_group_id=AgeConstants.AGE_95_PLUS_ID)
    ) / T_95_prime

    # these are the original, unadulterated q95 & q100
    q95_prime = qx.sel(age_group_id=AgeConstants.AGE_95_TO_100_ID)
    q100_prime = qx.sel(age_group_id=AgeConstants.AGE_100_TO_105_ID)
    beta = q95_prime / q100_prime

    gamma = 7.0 / 3.0 - alpha * (
        7.0 / 3.0 - (2 * beta + 1) * q100_prime + beta * (q100_prime**2)
    )

    q100 = ((2 * beta + 1) - np.sqrt((2 * beta + 1) ** 2 - 4 * beta * gamma)) / (2 * beta)

    # unfortunately, ~20% of q100 adjusted via this approx will be > 1.
    # it's even possible to end up with q100 < 0.
    # it makes no sense to cap q100 to 1, because that means l105 == 0,
    # which we do not want.  The only option left is the following
    q100 = q100.where((q100 < 1) & (q100 > 0)).fillna(q100_prime)
    q95 = q100 * beta  # always <= 1 because beta is always <= 1

    # Update original qx with adjusted q95 and q100 values
    qx.loc[dict(age_group_id=AgeConstants.AGE_95_TO_100_ID)] = q95
    qx.loc[dict(age_group_id=AgeConstants.AGE_100_TO_105_ID)] = q100

    return qx