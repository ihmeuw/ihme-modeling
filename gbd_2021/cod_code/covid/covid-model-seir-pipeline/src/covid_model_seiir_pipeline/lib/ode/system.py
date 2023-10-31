import numba
import numpy as np

from covid_model_seiir_pipeline.lib import (
    math,
)
from covid_model_seiir_pipeline.lib.ode.constants import (
    AGGREGATES,
    COMPARTMENTS,
    DEBUG,
    N_GROUPS,
    NEW_E,
    PARAMETERS,
    VACCINE_TYPES,
)
from covid_model_seiir_pipeline.lib.ode import (
    accounting,
    escape_variant,
    parameters,
    vaccinations,
)


@numba.njit
def fit_system(t: float, y: np.ndarray, input_parameters: np.ndarray):
    return _system(t, y, input_parameters, forecast=False)


@numba.njit
def forecast_system(t: float, y: np.ndarray, input_parameters: np.ndarray):
    return _system(t, y, input_parameters, forecast=True)


@numba.njit
def _system(t: float, y: np.ndarray, input_parameters: np.ndarray, forecast: bool):
    """The COVID ODE system.

    This is a shared representation of the COVID ODE system meant for use in
    both the beta fit and beta forecast stages of the pipelines. It accepts
    multiple parameterizations of the system.

    Parameters
    ----------
    t
        The current time indexed from 0 in units of days.
    y
        The state of the system at the end of the previous ODE step (or the
        initial condition if this is the first step). The structure of `y`
        should correspond to the concatenation of the :obj:`COMPARTMENTS`
        index map and the :obj:`TRACKING_COMPARTMENTS` index map duplicated
        :obj:`N_GROUPS` times.
    input_parameters
        An array whose first several fields align with the :obj:`PARAMETERS`
        index mapping, whose middle elements align with either the
        :obj:`FIT_PARAMETERS` or :obj:`FORECAST_PARAMETERS`, and whose
        remaining elements are the :obj:`VACCINE_TYPES` for each of the
        :obj:`N_GROUPS`.
    forecast
        The `input_parameters` are for the forecast if `True`, otherwise
        `input_parameters` are for the beta fit.

    Returns
    -------
    dy
        The change in `y` on the next ODE step.

    """
    aggregates = parameters.make_aggregates(y)
    params, vaccines, new_e = parameters.normalize_parameters(input_parameters, aggregates, forecast)

    system_size = len(y) // N_GROUPS
    dy = np.zeros_like(y)
    for i in range(N_GROUPS):
        group_start = i * system_size
        group_end = (i + 1) * system_size
        group_vaccine_start = i * len(VACCINE_TYPES)
        group_vaccine_end = (i + 1) * len(VACCINE_TYPES)

        group_y = y[group_start:group_end]
        group_vaccines = vaccines[group_vaccine_start:group_vaccine_end]

        group_dy = _single_group_system(
            t,
            group_y,
            new_e,
            aggregates,
            params,
            group_vaccines,
        )

        group_dy = escape_variant.maybe_invade(
            group_y,
            group_dy,
            aggregates,
            params,
        )

        dy[group_start:group_end] = group_dy

    if DEBUG:
        assert np.all(np.isfinite(dy))

    return dy


@numba.njit
def _single_group_system(t: float,
                         group_y: np.ndarray,
                         new_e: np.ndarray,
                         aggregates: np.ndarray,
                         params: np.ndarray,
                         group_vaccines: np.ndarray):
    """The COVID ODE system for a single demographic subgroup."""
    # Allocate our working space.  Transition matrix map.
    # Each row is a FROM compartment and each column is a TO compartment
    transition_map = np.zeros((group_y.size, group_y.size))
    # Vaccinations from each compartment indexed by out compartment and
    # vaccine category (u, p, pa, m, ma)
    vaccines_out = vaccinations.allocate(
        group_y,
        params,
        aggregates,
        group_vaccines,
        new_e,
    )

    ################
    # Unvaccinated #
    ################
    # Epi transitions
    transition_map = _seiir_transition_wild(
        group_y, params, aggregates, new_e,
        COMPARTMENTS.S, COMPARTMENTS.E, COMPARTMENTS.I1, COMPARTMENTS.I2, COMPARTMENTS.R,
        COMPARTMENTS.S_variant, COMPARTMENTS.E_variant,
        transition_map,
    )
    # Vaccines
    # S is complicated
    transition_map[COMPARTMENTS.S, COMPARTMENTS.S_u] += vaccines_out[COMPARTMENTS.S, VACCINE_TYPES.u]
    transition_map[COMPARTMENTS.S, COMPARTMENTS.S_p] += vaccines_out[COMPARTMENTS.S, VACCINE_TYPES.p]
    transition_map[COMPARTMENTS.S, COMPARTMENTS.S_pa] += vaccines_out[COMPARTMENTS.S, VACCINE_TYPES.pa]
    transition_map[COMPARTMENTS.S, COMPARTMENTS.S_m] += vaccines_out[COMPARTMENTS.S, VACCINE_TYPES.m]
    transition_map[COMPARTMENTS.S, COMPARTMENTS.R_m] += vaccines_out[COMPARTMENTS.S, VACCINE_TYPES.ma]

    # Other compartments are simple.
    transition_map[COMPARTMENTS.E, COMPARTMENTS.E_u] += vaccines_out[COMPARTMENTS.E, VACCINE_TYPES.u]
    transition_map[COMPARTMENTS.I1, COMPARTMENTS.I1_u] += vaccines_out[COMPARTMENTS.I1, VACCINE_TYPES.u]
    transition_map[COMPARTMENTS.I2, COMPARTMENTS.I2_u] += vaccines_out[COMPARTMENTS.I2, VACCINE_TYPES.u]
    transition_map[COMPARTMENTS.R, COMPARTMENTS.R_u] += vaccines_out[COMPARTMENTS.R, VACCINE_TYPES.u]

    ###############
    # Unprotected #
    ###############
    # Epi transitions only
    transition_map = _seiir_transition_wild(
        group_y, params, aggregates, new_e,
        COMPARTMENTS.S_u, COMPARTMENTS.E_u, COMPARTMENTS.I1_u, COMPARTMENTS.I2_u, COMPARTMENTS.R_u,
        COMPARTMENTS.S_variant_u, COMPARTMENTS.E_variant_u,
        transition_map,
    )

    #################################
    # Protected from ancestral type #
    #################################
    # Epi transitions only
    transition_map = _seiir_transition_wild(
        group_y, params, aggregates, new_e,
        COMPARTMENTS.S_p, COMPARTMENTS.E_p, COMPARTMENTS.I1_p, COMPARTMENTS.I2_p, COMPARTMENTS.R_p,
        COMPARTMENTS.S_variant_u, COMPARTMENTS.E_variant_u,
        transition_map,
    )

    ############################
    # Protected from all types #
    ############################
    # Epi transitions only
    transition_map = _seiir_transition_wild(
        group_y, params, aggregates, new_e,
        COMPARTMENTS.S_pa, COMPARTMENTS.E_pa, COMPARTMENTS.I1_pa, COMPARTMENTS.I2_pa, COMPARTMENTS.R_pa,
        COMPARTMENTS.S_variant_pa, COMPARTMENTS.E_variant_pa,
        transition_map,
    )

    ########################
    # Unvaccinated variant #
    ########################
    # Epi transitions
    transition_map = seiir_transition_variant(
        group_y, params, aggregates, new_e,
        COMPARTMENTS.S_variant, COMPARTMENTS.E_variant, COMPARTMENTS.I1_variant,
        COMPARTMENTS.I2_variant, COMPARTMENTS.R_variant,
        transition_map,
    )

    # Vaccinations
    # S is complicated
    transition_map[COMPARTMENTS.S_variant, COMPARTMENTS.S_variant_u] += vaccines_out[
        COMPARTMENTS.S_variant, VACCINE_TYPES.u]
    transition_map[COMPARTMENTS.S_variant, COMPARTMENTS.S_variant_pa] += vaccines_out[
        COMPARTMENTS.S_variant, VACCINE_TYPES.pa]
    transition_map[COMPARTMENTS.S_variant, COMPARTMENTS.R_m] += vaccines_out[
        COMPARTMENTS.S_variant, VACCINE_TYPES.ma]

    # Other compartments are simple
    transition_map[COMPARTMENTS.E_variant, COMPARTMENTS.E_variant_u] += vaccines_out[
        COMPARTMENTS.E_variant, VACCINE_TYPES.u]
    transition_map[COMPARTMENTS.I1_variant, COMPARTMENTS.I1_variant_u] += vaccines_out[
        COMPARTMENTS.I1_variant, VACCINE_TYPES.u]
    transition_map[COMPARTMENTS.I2_variant, COMPARTMENTS.I2_variant_u] += vaccines_out[
        COMPARTMENTS.I2_variant, VACCINE_TYPES.u]
    transition_map[COMPARTMENTS.R_variant, COMPARTMENTS.R_variant_u] += vaccines_out[
        COMPARTMENTS.R_variant, VACCINE_TYPES.u]

    #######################
    # Unprotected variant #
    #######################
    # Epi transitions only
    transition_map = seiir_transition_variant(
        group_y, params, aggregates, new_e,
        COMPARTMENTS.S_variant_u, COMPARTMENTS.E_variant_u, COMPARTMENTS.I1_variant_u,
        COMPARTMENTS.I2_variant_u, COMPARTMENTS.R_variant_u,
        transition_map,
    )

    #####################
    # Protected variant #
    #####################
    # Epi transitions only
    transition_map = seiir_transition_variant(
        group_y, params, aggregates, new_e,
        COMPARTMENTS.S_variant_pa, COMPARTMENTS.E_variant_pa, COMPARTMENTS.I1_variant_pa,
        COMPARTMENTS.I2_variant_pa, COMPARTMENTS.R_variant_pa,
        transition_map,
    )

    #############
    # Immunized #
    #############
    # Epi transition only
    transition_map[COMPARTMENTS.S_m, COMPARTMENTS.E_variant_pa] = math.safe_divide(
        new_e[NEW_E.variant_reinf] * group_y[COMPARTMENTS.S_m],
        aggregates[AGGREGATES.susceptible_variant_only],
    )

    inflow = transition_map.sum(axis=0)
    outflow = transition_map.sum(axis=1)
    group_dy = inflow - outflow

    if DEBUG:
        assert np.all(np.isfinite(group_dy))
        assert np.all(group_y + group_dy >= -1e-10)
        assert group_dy.sum() < 1e-5

    group_dy = accounting.compute_tracking_columns(
        group_dy,
        transition_map,
        vaccines_out,
    )

    return group_dy


@numba.njit
def _seiir_transition_wild(group_y: np.ndarray,
                           params: np.ndarray,
                           aggregates: np.ndarray,
                           new_e: np.ndarray,
                           susceptible: int, exposed: int, infectious1: int, infectious2: int, removed: int,
                           susceptible_variant: int, exposed_variant: int,
                           transition_map: np.ndarray) -> np.ndarray:
    """Epi transitions from the wild type compartments among a vaccination subgroup."""
    total_susceptible = aggregates[AGGREGATES.susceptible_wild]

    new_e_wild = math.safe_divide(group_y[susceptible] * new_e[NEW_E.wild], total_susceptible)
    new_e_variant = math.safe_divide(group_y[susceptible] * new_e[NEW_E.variant_naive], total_susceptible)

    transition_map[susceptible, exposed] += new_e_wild
    transition_map[exposed, infectious1] += params[PARAMETERS.sigma] * group_y[exposed]
    transition_map[infectious1, infectious2] += params[PARAMETERS.gamma1] * group_y[infectious1]
    transition_map[infectious2, removed] += params[PARAMETERS.chi] * params[PARAMETERS.gamma2] * group_y[infectious2]

    transition_map[susceptible, exposed_variant] += new_e_variant
    transition_map[infectious2, susceptible_variant] += (
        (1 - params[PARAMETERS.chi]) * params[PARAMETERS.gamma2] * group_y[infectious2]
    )

    if DEBUG:
        assert np.all(transition_map >= 0)

    return transition_map


@numba.njit
def seiir_transition_variant(group_y: np.ndarray,
                             params: np.ndarray,
                             aggregates: np.ndarray,
                             new_e: np.ndarray,
                             susceptible, exposed, infectious1, infectious2, removed,
                             transition_map):
    """Epi transitions from the escape variant compartments among a vaccination subgroup."""
    total_susceptible = aggregates[AGGREGATES.susceptible_variant_only]

    new_e_variant = math.safe_divide(group_y[susceptible] * new_e[NEW_E.variant_reinf], total_susceptible)

    transition_map[susceptible, exposed] += new_e_variant
    transition_map[exposed, infectious1] += params[PARAMETERS.sigma] * group_y[exposed]
    transition_map[infectious1, infectious2] += params[PARAMETERS.gamma1] * group_y[infectious1]
    transition_map[infectious2, removed] += params[PARAMETERS.gamma2] * group_y[infectious2]

    if DEBUG:
        assert np.all(transition_map >= 0)

    return transition_map
