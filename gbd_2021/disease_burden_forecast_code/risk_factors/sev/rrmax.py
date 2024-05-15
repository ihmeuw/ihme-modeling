import xarray as xr
from fhs_lib_database_interface.lib import db_session
from fhs_lib_database_interface.lib.constants import CauseRiskPairConstants
from fhs_lib_database_interface.lib.fhs_lru_cache import fhs_lru_cache
from fhs_lib_database_interface.lib.query import risk as query_risk
from fhs_lib_database_interface.lib.strategy_set import strategy
from fhs_lib_file_interface.lib.query import rrmax

PAF_OF_ONE_RRMAX = 1000


def read_rrmax(
    acause: str,
    cause_id: int,
    rei: str,
    gbd_round_id: int,
    version: str,
    draws: int,
    draw_start: int = 0,
) -> xr.DataArray:
    """A wrapper around the central read_rrmax, that handles the PAFs of 1 case.

    PAFs of 1 don't have rrmax files on disk, and should have their rrmax treated as 1000.
    """
    pafs_of_one = _get_pafs_of_one(gbd_round_id=gbd_round_id)
    rei_id = query_risk.get_rei_id(rei=rei)

    if (cause_id, rei_id) in pafs_of_one:
        return xr.DataArray(PAF_OF_ONE_RRMAX)

    return rrmax.read_rrmax(
        acause=acause,
        cause_id=cause_id,
        rei=rei,
        gbd_round_id=gbd_round_id,
        version=version,
        draws=draws,
        draw_start=draw_start,
    )


@fhs_lru_cache(1)
def _get_pafs_of_one(gbd_round_id: int) -> set[tuple[int, int]]:
    """Get the set of cause-risk pairs with PAFs of 1."""
    with db_session.create_db_session() as session:
        pafs_of_one = strategy.get_cause_risk_pair_set(
            session=session,
            gbd_round_id=gbd_round_id,
            strategy_id=CauseRiskPairConstants.CAUSE_RISK_PAF_EQUALS_ONE_SET_ID,
        )

    return set(zip(pafs_of_one["cause_id"], pafs_of_one["rei_id"]))
