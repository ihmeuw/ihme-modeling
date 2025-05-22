from dataclasses import dataclass
from typing import Dict, List, Optional

from gbd import constants as gbd_constants

from cascade_ode.legacy.shared_functions import get_demographics

CONTEXT_COLS = ["context_location_id", "context_year_id", "context_sex_id"]


@dataclass
class DemographicContext:
    """A DemographicContext identifies where in the cascade you are. Each
    submodel has a DemographicContext. It is used determine where data is
    saved and how to identify submodel-specific data inside each file.
    """

    parent_location_id: Optional[int]
    location_id: int
    year_id: int
    sex_id: int

    def identifiers(self) -> Dict[str, int]:
        """
        Return a mapping of column name to id that uniquely identifies
        a node in the cascade graph
        """
        return dict(zip(CONTEXT_COLS, [self.location_id, self.year_id, self.sex_id]))


class Demographics:
    """Provides all demographic variables that
    Dismod needs."""

    DEFAULT_LOCATION_SET_ID = 9
    USA_RE_LOCATION_SET_ID = 106

    # revisit if age groups change
    terminal_dismod_age = 33
    terminal_gbd_age = 235
    # NOTE: If you change this, please also change the equivalent in save_results:
    age_midpoints_to_age_group_id = {
        "0.0096": 2,
        "0.0479": 3,
        "0.5384": 4,
        "3.0": 5,
        "7.5": 6,
        "12.5": 7,
        "17.5": 8,
        "22.5": 9,
        "27.5": 10,
        "32.5": 11,
        "37.5": 12,
        "42.5": 13,
        "47.5": 14,
        "52.5": 15,
        "57.5": 16,
        "62.5": 17,
        "67.5": 18,
        "72.5": 19,
        "77.5": 20,
        "82.5": 30,
        "87.5": 31,
        "92.5": 32,
        "97.5": 33,
        "0.289": 388,  # GBD 2021+ ages
        "0.7507": 389,
        "1.5": 238,
        "3.5": 34,
    }

    def __init__(self, model_version_id):
        from cascade_ode.legacy import importer

        mvm = importer.get_model_version(model_version_id)
        self.gbd_round_id = mvm["gbd_round_id"].iat[0]
        self.release_id = mvm["release_id"].iat[0]
        self._demo_dict = get_demographics(gbd_team="epi", release_id=self.release_id)
        self.age_group_ids = self._demo_dict["age_group_id"]
        self.sex_ids: List[int] = [int(id) for id in self._demo_dict["sex_id"]]
        self.year_ids: List[int] = [int(id) for id in self._demo_dict["year_id"]]

    @property
    def mortality_age_grid(self):
        """
        Defines age group ids used for ASDR and CSMR inputs.

        Also defines default age grid.

        Note:
            Although 235 (95+) is in this list, it is labeled
            as having a midpoint of 97.5, so its treated effectively
            like 95-100 (ie age group 33). But results for that
            age do not exist in ASDR and CSMR outputs.
        """
        return self.age_group_ids

    @property
    def mortality_years(self):
        return [1985] + self.year_ids

    @property
    def location_set_id(self):
        if self.release_id == gbd_constants.release.USRE:
            return self.USA_RE_LOCATION_SET_ID
        return self.DEFAULT_LOCATION_SET_ID
