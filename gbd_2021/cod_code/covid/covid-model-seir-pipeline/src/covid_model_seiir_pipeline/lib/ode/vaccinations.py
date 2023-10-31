import numpy as np
import numba

from covid_model_seiir_pipeline.lib import (
    math,
)
from covid_model_seiir_pipeline.lib.ode.constants import (
    AGGREGATES,
    COMPARTMENTS,
    DEBUG,
    NEW_E,
    PARAMETERS,
    UNVACCINATED,
    VACCINE_TYPES,
)


@numba.njit
def allocate(group_y: np.ndarray,
             params: np.ndarray,
             aggregates: np.ndarray,
             group_vaccines: np.ndarray,
             new_e: np.ndarray) -> np.ndarray:
    """Allocate vaccines to compartments by effectiveness.

    The input `group_vaccines` tells us about the number of vaccine doses
    by efficacy to be delivered on the next time step. For a vaccine to be
    effective, though, it has to both be efficacious and be delivered to
    someone who who hasn't already been infected. Our algorithm assumes
    indiscriminate vaccination so a number of efficacious vaccines will end
    up being ineffective.

    Parameters
    ----------
    group_y
        The state of the group system on the last ODE step. The first
        elements in this array align with the :obj:`COMPARTMENTS` index
        map.
    params
        An array that aligns with the :obj:`PARAMETERS` index map representing
        the standard set of ODE parameters.
    aggregates
        An array that with values that can be indexed by the AGGREGATES
        index mapping.
    group_vaccines
        An array that aligns with the :obj:`VACCINE_TYPES` index map
        representing the doses of vaccine to be delivered to the group split
        by theoretical efficacy.
    new_e
        An array aligning with the :obj:`NEW_E` index mapping representing
        new infections split by variant and source.

    Returns
    -------
    vaccinations
        An array with a row for every compartment in `group_y` representing
        who the vaccine is delivered to and a column for every vaccine
        efficacy in `group_vaccines` representing how effective the vaccines
        were (and therefore what compartment folks will move to on the next
        step).

    """
    # Allocate our output space.
    vaccines_out = np.zeros((group_y.size, group_vaccines.size))

    n_vaccines_group = group_vaccines.sum()
    n_unvaccinated_group = group_y[UNVACCINATED].sum()

    # Don't vaccinate if no vaccines to deliver or there is noone to vaccinate.
    if not n_vaccines_group or not n_unvaccinated_group:
        return vaccines_out

    # S has many kinds of effective and ineffective vaccines
    s_frac = math.safe_divide(group_y[COMPARTMENTS.S], aggregates[AGGREGATES.susceptible_wild])
    vaccines_out = _allocate_from_s(
        group_y,
        group_vaccines,
        (new_e[NEW_E.wild] + new_e[NEW_E.variant_naive]) * s_frac,
        n_vaccines_group, n_unvaccinated_group,
        vaccines_out,
    )
    # S_variant has many kinds of effective and ineffective vaccines
    s_variant_frac = math.safe_divide(group_y[COMPARTMENTS.S_variant], aggregates[AGGREGATES.susceptible_variant_only])
    vaccines_out = _allocate_from_s_variant(
        group_y,
        group_vaccines,
        new_e[NEW_E.variant_reinf] * s_variant_frac,
        n_vaccines_group, n_unvaccinated_group,
        vaccines_out,
    )
    # Folks in E, I1, I2, R only unprotected.
    vaccines_out = _allocate_from_not_s(
        group_y,
        params,
        n_vaccines_group, n_unvaccinated_group,
        COMPARTMENTS.E, COMPARTMENTS.I1, COMPARTMENTS.I2, COMPARTMENTS.R,
        vaccines_out,
    )
    vaccines_out = _allocate_from_not_s(
        group_y,
        params,
        n_vaccines_group, n_unvaccinated_group,
        COMPARTMENTS.E_variant, COMPARTMENTS.I1_variant, COMPARTMENTS.I2_variant, COMPARTMENTS.R_variant,
        vaccines_out,
    )

    if DEBUG:
        assert np.all(np.isfinite(vaccines_out))
        assert np.all(vaccines_out >= 0)

    return vaccines_out


@numba.njit
def _allocate_from_s(group_y: np.ndarray,
                     group_vaccines: np.ndarray,
                     new_e_from_s: float,
                     v_total: float, n_unvaccinated: float,
                     vaccines_out: np.ndarray):
    """Allocate vaccine effectiveness among those never infected."""
    expected_total_vaccines_s = group_y[COMPARTMENTS.S] / n_unvaccinated * v_total
    # Infections take precedence over vaccinations.  Only vaccinate if there are
    # people left after infection.
    total_vaccines_s = min(group_y[COMPARTMENTS.S] - new_e_from_s, expected_total_vaccines_s)

    if expected_total_vaccines_s:
        for vaccine_type in VACCINE_TYPES:
            expected_vaccines = group_y[COMPARTMENTS.S] / n_unvaccinated * group_vaccines[vaccine_type]
            vaccine_ratio = expected_vaccines / expected_total_vaccines_s
            vaccines_out[COMPARTMENTS.S, vaccine_type] = vaccine_ratio * total_vaccines_s

    if DEBUG:
        assert np.all(np.isfinite(vaccines_out))
        assert np.all(vaccines_out >= 0)

    return vaccines_out


@numba.njit
def _allocate_from_s_variant(group_y: np.ndarray,
                             group_vaccines: np.ndarray,
                             new_e_from_s_variant: float,
                             v_total: float, n_unvaccinated: float,
                             vaccines_out: np.ndarray) -> np.ndarray:
    """Allocate vaccine effectiveness among previously wild-type infected."""
    expected_total_vaccines_s_variant = group_y[COMPARTMENTS.S_variant] / n_unvaccinated * v_total
    # Infections take precedence over vaccinations.  Only vaccinate if there are
    # people left after infection.
    total_vaccines_s_variant = min(group_y[COMPARTMENTS.S_variant] - new_e_from_s_variant,
                                   expected_total_vaccines_s_variant)

    if expected_total_vaccines_s_variant:
        total_ineffective = (group_vaccines[VACCINE_TYPES.u]
                             + group_vaccines[VACCINE_TYPES.p]
                             + group_vaccines[VACCINE_TYPES.m])
        expected_u_vaccines = group_y[COMPARTMENTS.S_variant] / n_unvaccinated * total_ineffective
        vaccine_ratio = expected_u_vaccines / expected_total_vaccines_s_variant
        vaccines_out[COMPARTMENTS.S_variant, VACCINE_TYPES.u] = vaccine_ratio * total_vaccines_s_variant

        for vaccine_type in [VACCINE_TYPES.pa, VACCINE_TYPES.ma]:
            expected_vaccines = group_y[COMPARTMENTS.S_variant] / n_unvaccinated * group_vaccines[vaccine_type]
            vaccine_ratio = expected_vaccines / expected_total_vaccines_s_variant
            vaccines_out[COMPARTMENTS.S_variant, vaccine_type] = vaccine_ratio * total_vaccines_s_variant

    if DEBUG:
        assert np.all(np.isfinite(vaccines_out))
        assert np.all(vaccines_out >= 0)

    return vaccines_out


@numba.njit
def _allocate_from_not_s(group_y: np.ndarray,
                         params: np.ndarray,
                         v_total: float, n_unvaccinated: float,
                         exposed: int, infectious1: int, infectious2: int, removed: int,
                         vaccines_out: np.ndarray) -> np.ndarray:
    """Allocate vaccine effectiveness among active infections."""
    param_map = (
        (exposed, params[PARAMETERS.sigma]),
        (infectious1, params[PARAMETERS.gamma1]),
        (infectious2, params[PARAMETERS.gamma2]),
    )

    for compartment, param in param_map:
        vaccines_out[compartment, VACCINE_TYPES.u] = min(
            (1 - param) * group_y[compartment],
            group_y[compartment] / n_unvaccinated * v_total
        )
    vaccines_out[removed, VACCINE_TYPES.u] = min(
        group_y[removed],
        group_y[removed] / n_unvaccinated * v_total
    )

    if DEBUG:
        assert np.all(np.isfinite(vaccines_out))
        assert np.all(vaccines_out >= 0)

    return vaccines_out
