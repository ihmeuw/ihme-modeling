from typing import List

import db_queries


def get_default_years(gbd_round_id: int) -> List[int]:
    """Looks up default year IDs with a GBD round ID."""
    demographics = db_queries.get_demographics("cod", gbd_round_id)
    return [year_id for year_id in demographics["year_id"] if year_id >= 1990]
