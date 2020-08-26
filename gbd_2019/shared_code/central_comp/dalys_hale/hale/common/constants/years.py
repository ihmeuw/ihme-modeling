from typing import Dict, List, Tuple

DEFAULT_YEAR_START: int = 1980

# Maps GBD round ID to list of (start year, end year) pairs for which percent
# change should be calculated.
# NOTE: this dictionary needs to be updated each GBD round
PERCENT_CHANGE_YEARS: Dict[int, List[Tuple[int, int]]] = {
    5: [
        (1990, 2017),
        (1990, 2007),
        (2007, 2017)
    ],
    6: [
        (1990, 2019),
        (2010, 2019)
    ]
}
