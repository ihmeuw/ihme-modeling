from pathlib import Path

import pandas

from winnower import errors
from winnower.util.dataframe import set_default_values
from winnower.util.dtype import is_numeric_dtype
from winnower.util.path import J_drive

from .base import TransformBase


class Transform(TransformBase):
    uses_extra_columns = None

    who_whs_surveys = {21455, 21460, 21468, 21473, 21481, 21489, 21502,
                       21510, 21519, 21527, 21535, 21543, 21551, 21494, 21567, 21575,
                       21583, 21591, 21596, 21601, 21609, 21614, 21622, 21627,
                       21635, 21643, 21653, 21658, 21663, 21668, 21676, 21684,
                       21692, 21700, 21705, 21713, 21721, 21729, 21737,
                       21745, 21753, 21761, 21769, 21777, 21785, 21790,
                       21795, 21803, 21811, 21819, 21824, 21832,
                       21840, 21848, 21856, 21864, 21872, 21880,
                       21888, 21893, 21901, 21909, 21962,
                       21917, 21922, 21930, 21938,
                       21946, 21954}

    def validate(self, input_columns):
        """
        If keep_cols are in columns, include them.
        """
        keep_cols = self._get_keep_cols()
        # assume case sensitive
        extra_columns = [X for X in keep_cols if X in self.columns]
        self.uses_extra_columns = extra_columns

    def output_columns(self, input_columns):
        return input_columns

    def execute(self, df):
        df = self._keep_for_specific_nids(df)
        return df

    def _get_keep_cols(self):
        keep_cols = ["file_path", "ihme_loc_id", "nid", "survey_module",
                     "survey_name", "year_end", "year_start", "age_year", "int_year",
                     "geospatial_id", "hh_id", "hhweight", "psu", "pweight",
                     "strata", "cooking_fuel", "heating_fuel", "housing_wall",
                     "housing_floor", "housing_wall_num",
                     "housing_floor_num", "cooking_fuel_mapped"]
        return keep_cols

    def _keep_for_specific_nids(self, df):
        """
        from hap.do
        Custom code to only keep household indicators for the WHO_WHS surveys.
        The INDIV (master) module respresent the person who answered the door, 
            all of their demographic and health info, 
            AND the indicators pertaining to their household (q4040-q4052). 
        We are only keeping those HH indicators so that the first person's data 
            aren't merged onto the rest of the HH_ROSTER (using).
        """
        keep_cols = self._get_keep_cols()
        if self.config['nid'] in self.who_whs_surveys:
            df = df[keep_cols]

        return df
