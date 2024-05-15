"""Functions for understanding the mediation hierarchy."""

from typing import Dict, List, Tuple

from fhs_lib_database_interface.lib.fhs_lru_cache import fhs_lru_cache
from fhs_lib_database_interface.lib.query.risk import get_sev_forecast_reis
from fhs_lib_file_interface.lib.query import mediation


def get_mediator_upstreams(gbd_round_id: int) -> Dict:
    """Return mediators and their upstreams in a list of tuples.

    Args:
        gbd_round_id (int): gbd round id.

    Returns:
        (Dict): Dict of (mediator, upstreams) pairs, ascending-ordered by number of
            upstreams.
    """
    mediation_matrix = mediation.get_mediation_matrix(gbd_round_id).mean("draw")

    # for this task, it's easier to deal with a linear array
    stacked = mediation_matrix.stack(dims=mediation_matrix.dims)
    nonzeros = stacked[stacked != 0]  # where mediation actually exists

    result = []
    for mediator in mediation_matrix["med"].values:
        upstreams = list(set(nonzeros.sel(med=mediator)["rei"].values))
        result.append((mediator, upstreams))

    return dict(sorted(result, key=lambda x: len(x[1])))


@fhs_lru_cache(1)
def get_cause_mediator_pairs(gbd_round_id: int) -> List[tuple]:
    """Provide cause-risk pairs where risk is a mediator.

    Args:
        gbd_round_id (int): gbd round id.

    Returns:
        (List[tuple]): list of (cause, mediator) pairs.
    """
    mediation_matrix = mediation.get_mediation_matrix(gbd_round_id)

    mean = mediation_matrix.mean(["rei", "draw"])
    stacked = mean.stack(pairs=["acause", "med"])  # making a 1-D xarray allows us to filter
    greater_than_zero = stacked[stacked > 0]
    return greater_than_zero.pairs.values.tolist()


@fhs_lru_cache(1)
def get_intrinsic_sev_pairs(gbd_round_id: int) -> List[Tuple]:
    """Return all intrinsic SEV cause-risk pairs.

    Args:
        gbd_round_id (int): gbd round id.

    Returns:
        (List[Tuple]): list of (cause, mediator) pairs.
    """
    pairs = get_cause_mediator_pairs(gbd_round_id)

    # remove PAF=1 and metab_bmd pairs (RR not available for either)
    # pairs = [x for x in pairs
    #          if not property_values.loc[x[0], x[1]]["paf_equals_one"]]
    pairs = [x for x in pairs if x[1] != "metab_bmd"]

    return pairs


def get_sev_intrinsic_map(gbd_round_id: int) -> Dict[str, bool]:
    """Makes a dictionary mapping all the sevs to whether or not they're intrinsic."""
    # now figure out all the sevs and isevs we need to ensemble for
    sevs = get_sev_forecast_reis(gbd_round_id)

    # this part deals with the intrinsic SEVs
    intrinsic_pairs = get_intrinsic_sev_pairs(gbd_round_id)

    # because we ensemble mediator isevs, so should remove them from sevs
    mediators = list(set(x[1] for x in intrinsic_pairs))
    sevs = list(set(sevs) - set(mediators))

    # these mediator isevs will be ensembled
    isevs = [x[0] + "_" + x[1] for x in intrinsic_pairs]

    intrinsic_map = dict(**{sev: False for sev in sevs}, **{isev: True for isev in isevs})

    return intrinsic_map
