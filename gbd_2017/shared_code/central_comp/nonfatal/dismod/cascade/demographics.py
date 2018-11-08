from functools32 import lru_cache

from db_queries import get_demographics


class Demographics(object):
    '''Provides all demographic variables that
    Dismod needs.'''

    LOCATION_SET_ID = 9

    terminal_dismod_age = 33
    terminal_gbd_age = 235

    def __init__(self, gbd_round_id=5):
        self._demo_dict = self._read_db(gbd_round_id)

        self.gbd_round_id = gbd_round_id
        self.age_group_ids = self._demo_dict['age_group_id']
        self.sex_ids = self._demo_dict['sex_id']
        self.year_ids = self._demo_dict['year_id']

    @classmethod
    @lru_cache()
    def _read_db(self, gbd_round_id):
        return get_demographics(gbd_team="epi", gbd_round_id=gbd_round_id)

    @property
    def mortality_age_grid(self):
        '''
        Defines age group ids used for ASDR and CSMR inputs.

        Also defines default age grid.

        '''
        return self.age_group_ids

    @property
    def mortality_years(self):
        return [1985] + self.year_ids

    @property
    def age_midpoints_to_age_group_id(self):
        age_dict = {
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
                '97.5': 33}

        return age_dict
