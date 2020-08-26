from functools import lru_cache

from cascade_ode.db import execute_select
from cascade_ode.shared_functions import get_demographics


class Demographics:
    '''Provides all demographic variables that
    Dismod needs.'''

    LOCATION_SET_ID = 9

    # revisit if age groups change
    terminal_dismod_age = 33
    terminal_gbd_age = 235
    age_midpoints_to_age_group_id = {
                '0.0096': 2,
                '0.0479': 3,
                '0.5384': 4,
                '3.0': 5,
                '7.5': 6,
                '12.5': 7,
                '17.5': 8,
                '22.5': 9,
                '27.5': 10,
                '32.5': 11,
                '37.5': 12,
                '42.5': 13,
                '47.5': 14,
                '52.5': 15,
                '57.5': 16,
                '62.5': 17,
                '67.5': 18,
                '72.5': 19,
                '77.5': 20,
                '82.5': 30,
                '87.5': 31,
                '92.5': 32,
                '97.5': 33,
                '0.289':  388, # GBD 2020+ ages
                '0.7507':  389,
                '1.5': 238,
                '3.5': 34
    }

    def __init__(self, model_version_id):
        self.gbd_round_id, self._demo_dict = self._read_db(model_version_id)

        self.age_group_ids = self._demo_dict['age_group_id']
        self.sex_ids = self._demo_dict['sex_id']
        self.year_ids = self._demo_dict['year_id']

    @classmethod
    @lru_cache()
    def _read_db(self, model_version_id):
        qry = """
        SELECT gbd_round_id
        FROM epi.model_version
        WHERE
        model_version_id = :mvid"""
        results = execute_select(
            qry, params={'mvid': model_version_id})

        if not results.empty:
            gbd_round_id = results.at[0, 'gbd_round_id']
        else:
            raise RuntimeError(
                f"Could not find mvid {model_version_id} in epi db")
        demo_dict = get_demographics(gbd_team="epi", gbd_round_id=gbd_round_id)
        return gbd_round_id, demo_dict

    @property
    def mortality_age_grid(self):
        '''
        Defines age group ids used for ASDR and CSMR inputs.

        Also defines default age grid.

        Note:
            Although 235 (95+) is in this list, it is labeled
            as having a midpoint of 97.5, so its treated effectively
            like 95-100 (ie age group 33). But results for that
            age do not exist in ASDR and CSMR outputs.
        '''
        return self.age_group_ids

    @property
    def mortality_years(self):
        return [1985] + self.year_ids


class AgeGroupSet:
    GBD_2019_AGE_GROUP_SET_ID = 12
    GBD_2020_AGE_GROUP_SET_ID = 19

    @classmethod
    def from_gbd_round_id(cls, gbd_round_id):
        if gbd_round_id == 6:
            return cls.GBD_2019_AGE_GROUP_SET_ID
        if gbd_round_id == 7:
            return cls.GBD_2020_AGE_GROUP_SET_ID
        raise ValueError(
            f"No known age group set id for gbd_round_id {gbd_round_id}")
