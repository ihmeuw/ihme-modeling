import os
from copy import deepcopy
import pandas as pd

from gbd.gbd_round import gbd_round_from_gbd_round_id
from ihme_dimensions import gbdize
from dataframe_io.io_control.h5_io import read_hdf
from core_maths.interpolate import pchip_interpolate


class DisabilityWeightInputs:

    _draw_cols = [f"draw_{i}" for i in range(1000)]

    def __init__(self, como_version, dimensions):
        self.cv = como_version
        self.dims = deepcopy(dimensions)
        self.dims.index_dim.drop_level("measure_id")
        self.dims.index_dim.drop_level("sex_id")
        self.dims.index_dim.add_level(
            "healthstate_id",
            list(self.cv.sequela_list.healthstate_id.unique()))

        self.dws = None
        self.id_dws = None

    def get_dws(self):
        dws = self._get_standard_dws()
        dws = dws.append(self._get_custom_dws())
        dws = dws.append(self._get_inj_dws())
        dws = dws.append(self._get_epi_dws())
        dws = dws.append(self._get_mnd_dws())
        dws = dws.append(self._get_autism_dws())
        dws = dws.append(self._get_uro_dws())

        # healthstate_id 799 - Asymptomatic
        asymp_row = {draw_col: 0 for draw_col in self._draw_cols}
        asymp_row['healthstate_id'] = 799
        dws = dws.append(pd.DataFrame([asymp_row]))
        dws = dws.reset_index(drop=True)
        dws = dws[['healthstate_id'] + self._draw_cols]

        # resample
        if len(self._draw_cols) != len(self.dims.data_list()):
            dimensions = deepcopy(self.dims)
            dimensions.index_dim.drop_level("age_group_id")
            dimensions.index_dim.drop_level("year_id")
            dimensions.index_dim.drop_level("location_id")
            gbdizer = gbdize.GBDizeDataFrame(dimensions)
            dws = gbdizer.correlated_percentile_resample(dws)

        self.dws = dws

    def get_id_dws(self):
        dws = pd.read_csv(
            "FILEPATH/combined_id_dws.csv")
        dimensions = deepcopy(self.dims)
        dimensions.index_dim.drop_level("year_id")
        dimensions.index_dim.drop_level("location_id")
        gbdizer = gbdize.GBDizeDataFrame(dimensions)
        dws = dws.reset_index(drop=True)
        if len(self._draw_cols) != len(self.dims.data_list()):
            dws = gbdizer.correlated_percentile_resample(dws)
        self.id_dws = dws

    def _get_standard_dws(self):
        dws = pd.read_csv(
            "FILEPATH/dw.csv")
        return dws

    def _get_custom_dws(self):
        dws = pd.read_csv(
            "FILEPATH/combined_dws.csv")
        return dws

    def _get_mnd_dws(self):
        dws = pd.read_csv(
            "FILEPATH/combined_mnd_dws.csv")
        return dws

    def _get_autism_dws(self):
        dws = pd.read_csv(
            "FILEPATH/autism_dws.csv")
        return dws

    def _get_epi_dws(self):
        # these disability weights are generated in version when running
        # disability_weights.combine_epilepsy_* at the beginning of every
        # como run
        eafp = os.path.join(self.cv.como_dir, 'info', 'epilepsy_any_dws.h5')
        ecfp = os.path.join(self.cv.como_dir, 'info', 'epilepsy_combo_dws.h5')
        loc_id = self.dims.index_dim.get_level("location_id")[0]
        year_id = self.dims.index_dim.get_level("year_id")[0]
        epi_any = pd.read_hdf(
            eafp, 'draws',
            where=f'location_id == {loc_id} & year_id == {year_id}')
        epi_combos = pd.read_hdf(
            ecfp, 'draws',
            where=f'location_id == {loc_id} & year_id == {year_id}')
        epi_dws = pd.concat([epi_any, epi_combos])
        return epi_dws[['healthstate_id'] + self._draw_cols]

    def _get_uro_dws(self):
        # these disability weights are generated in version when running
        # disability_weights.urolitiasis at the beginning of every como run
        fp = os.path.join(self.cv.como_dir, 'info', 'urolith_dws.h5')
        loc_id = self.dims.index_dim.get_level("location_id")[0]
        year_id = self.dims.index_dim.get_level("year_id")[0]
        uro_dws = pd.read_hdf(
            fp, 'draws',
            where=f'location_id == {loc_id} & year_id == {year_id}')
        return uro_dws[['healthstate_id'] + self._draw_cols]

    def _get_inj_dws(self):
        loc_id = self.dims.index_dim.get_level("location_id")[0]
        year_id = self.dims.index_dim.get_level("year_id")
        fp = "FILEPATH/lt_dw.h5".format(
            gbd_round_from_gbd_round_id(self.cv.gbd_round_id))
        inj_dws = read_hdf(fp, "draws", hdf_filters={"location_id": loc_id})
        inj_dws = inj_dws.reset_index()

        # interpolate
        interp = pchip_interpolate(
            df=inj_dws,
            id_cols=["location_id", "ncode"],
            value_cols=self._draw_cols,
            time_col="year_id",
            time_vals=year_id)
        inj_dws = inj_dws.append(interp)
        inj_dws = inj_dws[inj_dws.year_id.isin(year_id)]

        seq_map = self.cv.injury_dws_by_sequela[
            ["sequela_id", "healthstate_id", "n_code"]].drop_duplicates()
        inj_dws = inj_dws.merge(
            seq_map,
            left_on="ncode",
            right_on="n_code")
        inj_dws = inj_dws[['sequela_id', 'healthstate_id'] + self._draw_cols]
        return inj_dws
