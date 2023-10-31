import numba
import numpy as np

from covid_model_seiir_pipeline.lib.ode.constants import (
    COMPARTMENTS,
    DEBUG,
    TRACKING_COMPARTMENTS,
    VACCINE_TYPES,
)


@numba.njit
def compute_tracking_columns(group_dy: np.ndarray,
                             transition_map: np.ndarray,
                             vaccines_out: np.ndarray) -> np.ndarray:
    """Computes some aggregates that don't directly correspond to compartments.

    We need to do some accounting to capture, e.g., total new wild type
    infections. The only things we're tracking here are quantities that cannot
    be inferred from the time-series of the ODE compartments after the model
    is run. These quantities come about when there are compartments that have
    multiple in-paths.

    Parameters
    ----------
    group_dy
        An array who's first set of elements correspond to the
        :obj:`COMPARTMENTS` index map representing the resulting change in
        state from an ODE step. The remaining set of elements correspond to
        the :obj:`TRACKING_COMPARTMENTS` index map and should all be zero when
        passed to this function.
    transition_map
        An 2-d matrix representation whose rows and columns both correspond
        to the :obj:`COMPARTMENTS` index map. The value at position (i, j)
        in the matrix is the quantity of people moving from compartment i
        to compartment j.
    vaccines_out
        An array with a row for every compartment in `group_dy` representing
        who the vaccine is delivered to and a column for every vaccine
        efficacy in `group_vaccines` representing how effective the vaccines
        were (and therefore what compartment folks will move to on the next
        step).

    """
    # New wild type infections
    group_dy[TRACKING_COMPARTMENTS.NewE_wild] = (
        transition_map[COMPARTMENTS.S, COMPARTMENTS.E]
        + transition_map[COMPARTMENTS.S_u, COMPARTMENTS.E_u]
        + transition_map[COMPARTMENTS.S_p, COMPARTMENTS.E_p]
        + transition_map[COMPARTMENTS.S_pa, COMPARTMENTS.E_pa]
    )
    # New wild type infections among unvaccinated
    group_dy[TRACKING_COMPARTMENTS.NewE_unvax_wild] = (
        transition_map[COMPARTMENTS.S, COMPARTMENTS.E]
    )
    # New variant type infections
    group_dy[TRACKING_COMPARTMENTS.NewE_variant] = (
        transition_map[COMPARTMENTS.S, COMPARTMENTS.E_variant]
        + transition_map[COMPARTMENTS.S_variant, COMPARTMENTS.E_variant]
        + transition_map[COMPARTMENTS.S_u, COMPARTMENTS.E_variant_u]
        + transition_map[COMPARTMENTS.S_variant_u, COMPARTMENTS.E_variant_u]
        + transition_map[COMPARTMENTS.S_p, COMPARTMENTS.E_variant_u]
        + transition_map[COMPARTMENTS.S_pa, COMPARTMENTS.E_variant_pa]
        + transition_map[COMPARTMENTS.S_variant_pa, COMPARTMENTS.E_variant_pa]
        + transition_map[COMPARTMENTS.S_m, COMPARTMENTS.E_variant_pa]
    )
    # New variant type infections among unvaccinated
    group_dy[TRACKING_COMPARTMENTS.NewE_unvax_variant] = (
        transition_map[COMPARTMENTS.S, COMPARTMENTS.E_variant]
        + transition_map[COMPARTMENTS.S_variant, COMPARTMENTS.E_variant]
    )
    # New wild type protected infections
    group_dy[TRACKING_COMPARTMENTS.NewE_p_wild] = (
        transition_map[COMPARTMENTS.S_p, COMPARTMENTS.E_p]
        + transition_map[COMPARTMENTS.S_pa, COMPARTMENTS.E_pa]
    )
    # New variant type protected infections
    group_dy[TRACKING_COMPARTMENTS.NewE_p_variant] = (
        transition_map[COMPARTMENTS.S_pa, COMPARTMENTS.E_variant_pa]
        + transition_map[COMPARTMENTS.S_variant_pa, COMPARTMENTS.E_variant_pa]
        + transition_map[COMPARTMENTS.S_m, COMPARTMENTS.E_variant_pa]
    )

    # New variant type infections breaking through natural immunity
    group_dy[TRACKING_COMPARTMENTS.NewE_nbt] = (
        transition_map[COMPARTMENTS.S_variant, COMPARTMENTS.E_variant]
        + transition_map[COMPARTMENTS.S_variant_u, COMPARTMENTS.E_variant_u]
        + transition_map[COMPARTMENTS.S_variant_pa, COMPARTMENTS.E_variant_pa]
    )
    group_dy[TRACKING_COMPARTMENTS.NewE_unvax_nbt] = (
            transition_map[COMPARTMENTS.S_variant, COMPARTMENTS.E_variant]
    )
    # New variant type infections breaking through vaccine immunity
    group_dy[TRACKING_COMPARTMENTS.NewE_vbt] = transition_map[COMPARTMENTS.S_m, COMPARTMENTS.E_variant_pa]

    # Proportion cross immune checks
    group_dy[TRACKING_COMPARTMENTS.NewS_v] = (
        transition_map[COMPARTMENTS.I2, COMPARTMENTS.S_variant]
        + transition_map[COMPARTMENTS.I2_u,  COMPARTMENTS.S_variant_u]
        + transition_map[COMPARTMENTS.I2_p, COMPARTMENTS.S_variant_u]
        + transition_map[COMPARTMENTS.I2_pa, COMPARTMENTS.S_variant_pa]
    )

    group_dy[TRACKING_COMPARTMENTS.NewR_w] = (
        transition_map[COMPARTMENTS.I2, COMPARTMENTS.R]
        + transition_map[COMPARTMENTS.I2_u,  COMPARTMENTS.R_u]
        + transition_map[COMPARTMENTS.I2_p, COMPARTMENTS.S_u]
        + transition_map[COMPARTMENTS.I2_pa, COMPARTMENTS.S_u]
    )

    group_dy[TRACKING_COMPARTMENTS.V_u] = vaccines_out[:, VACCINE_TYPES.u].sum()
    group_dy[TRACKING_COMPARTMENTS.V_p] = vaccines_out[:, VACCINE_TYPES.p].sum()
    group_dy[TRACKING_COMPARTMENTS.V_m] = vaccines_out[:, VACCINE_TYPES.m].sum()
    group_dy[TRACKING_COMPARTMENTS.V_pa] = vaccines_out[:, VACCINE_TYPES.pa].sum()
    group_dy[TRACKING_COMPARTMENTS.V_ma] = vaccines_out[:, VACCINE_TYPES.ma].sum()

    if DEBUG:
        assert np.all(np.isfinite(group_dy))

    return group_dy
