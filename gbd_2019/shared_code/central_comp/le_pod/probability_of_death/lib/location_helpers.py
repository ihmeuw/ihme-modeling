from typing import Dict, List

# Location sets for which to produce results, by GBD round.
_LOCATION_SETS: Dict[int, List[int]] = {
    5: [3, 5, 11, 20, 24, 26, 28, 31, 32, 35, 40, 46],
    6: [3, 5, 11, 20, 24, 26, 28, 31, 32, 46, 40, 89],
}


def get_default_locations(gbd_round_id: int) -> List[int]:
    """Looks up default location sets associated with a GBD round ID."""
    if gbd_round_id not in _LOCATION_SETS:
        raise RuntimeError(f"No default location sets for GBD round {gbd_round_id}")
    return _LOCATION_SETS[gbd_round_id]
