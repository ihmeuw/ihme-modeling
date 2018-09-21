import sys
import os
from multiprocessing import Process, Queue

from copy import deepcopy
import pandas as pd

from transmogrifier.super_gopher import SuperGopher, InvalidFilter
from ihme_dimensions import gbdize
from ihme_dimensions.dimensionality import DataFrameDimensions
from ihme_dimensions.cython_modules.flatten import flatten
from ihme_dimensions.fill import MissingGBDemographics

from como.common import cap_val

sentinel = None

# Set dUSERt file mask to readable-for all users
os.umask(0o0002)


class ModelableEntityInputWalker(object):

    def __init__(self, env="prod"):
        self.readers = {}
        self.reader_results = {}
        self.env = env

    def add_reader_to_q(self, meid, mvid, dimensions):
        reader = self.readers.get(
            (meid, mvid), ModelableEntityInput(meid, mvid, env=self.env))
        reader.add_dimensions_to_q(dimensions)
        self.readers[(meid, mvid)] = reader

    def read_single_modelable_entity(self, readkey):
        reader = self.readers[readkey]
        draws = reader.read_inputs()
        return {readkey: draws}

    def _q_read_single_modelable_entity(self, inq, outq):
        for arglist in iter(inq.get, sentinel):
            try:
                result = self.read_single_modelable_entity(arglist)
            except Exception as e:
                print(
                    'Could not find (meid, mvid): ({arg1}, {arg2})'.format(
                        arg1=arglist[0], arg2=arglist[1]))
                result = {(arglist): (e, sys.exc_info()[1])}
            result = result.items()[0]
            outq.put(result)

    def _read_all_inputs_mp(self, n_processes=20):
        """read in all como sequela draws using multiprocessing"""
        inq = Queue()
        outq = Queue()

        # Create and feed reader procs
        read_procs = []
        for i in range(min([n_processes, len(self.readers)])):
            p = Process(target=self._q_read_single_modelable_entity,
                        args=(inq, outq))
            read_procs.append(p)
            p.start()

        for readkey in self.readers.keys():
            inq.put(readkey)

        # make the workers die after
        for _ in read_procs:
            inq.put(sentinel)

        # get results
        for _ in self.readers.keys():
            proc_result = outq.get()
            self.reader_results[proc_result[0]] = proc_result[1]

        # close up the queue
        for p in read_procs:
            p.join()

    def _read_all_inputs_sp(self):
        """read in all como sequela draws in a loop"""
        for readkey in self.readers.keys():
            result = self.read_single_modelable_entity(readkey)
            self.reader_results.update(result)

    def read_all_inputs(self, n_processes=20):
        if n_processes > 1:
            self._read_all_inputs_mp(n_processes)
        else:
            self._read_all_inputs_sp()


class ModelableEntityInput(object):

    def __init__(self, meid, mvid, env="prod"):
        self.meid = meid
        self.mvid = mvid
        self.env = env
        self.dimensions_q = []
        self.meid_data_dir = (
            'filepath/{e}/{mvid}/full/draws'.format(
                e=env, mvid=mvid))
        self.super_gopher = None
        self.gbd_years = [1990, 1995, 2000, 2005, 2010, 2016]

    def add_dimensions_to_q(self, dimensions):
        self.dimensions_q.append(dimensions)
        self.dimensions_q = list(set(self.dimensions_q))

    @staticmethod
    def missing_dimensions(df, dimensions, prefer=['age_group_id', 'year_id']):
        """compare a dataframe and a dimensions object
           returns the diff as a dimensions object"""
        assert all(name in df.columns for name in dimensions.index_names), (
            "The dimensions object passed contain "
            "columns names not present in the datafame passed.")
        copy_df = df.copy(deep=True)
        copy_df.set_index(dimensions.index_names, inplace=True)
        diff_df = dimensions.index_df()
        diff_df['dummy'] = 1
        diff_df.set_index(dimensions.index_names, inplace=True)

        diff_df = diff_df.loc[~diff_df.index.isin(
            list(copy_df.index))].reset_index()

        group_by_these = [
            col for col in dimensions.index_names if col not in prefer]
        grouped = diff_df.groupby(by=group_by_these)

        missing_dim_list = []
        for name, group in grouped:
            index_dict = {}
            for col in group_by_these:
                try:
                    index_dict[col] = [group[col].iloc[0]]
                except IndexError:
                    raise ValueError(
                        "There are missing values in the index columns "
                        "of the dataframe passed.")
            for index in prefer:
                index_dict[index] = group[index].unique().tolist()
            missing_dim = DataFrameDimensions(
                index_dict, dimensions.data_dim.to_dict()['levels'])
            missing_dim_list.append(missing_dim)

        return missing_dim_list

    @staticmethod
    def gbdize_dimensions(df, gbdizer, demo="age_group_id"):
        df = gbdizer.add_missing_index_cols(df)
        df = gbdizer.gbdize_any_by_dim(df, demo)
        df.fillna(0, inplace=True)
        return df

    @staticmethod
    def resample_if_needed(df, dimensions, gbdizer):
        df = df.set_index(dimensions.index_names)
        # subset data columns
        draw_cols = ["draw_{}".format(i) for i in range(1000)]
        draw_cols = [d for d in draw_cols if d in df.columns]
        df = df[draw_cols]
        df = df.reset_index()

        # resample if ndraws is less than 1000
        if len(dimensions.data_list()) != len(draw_cols):
            df = gbdizer.random_choice_resample(df)
        return df

    def get_interpolation_draws(self, reference_df, dimensions):
        # find out what years we will need for interpolation
        rank_df = None
        ref_years = cap_val(
            dimensions.index_dim.get_level("year_id"),
            self.gbd_years)

        query_str = []
        not_years = [
            index for index in dimensions.index_names
            if index != 'year_id']

        # build query string to find years we already have
        if len(reference_df) > 0:
            for index in not_years:
                q = '{index} in {ids}'.format(
                    index=index, ids=dimensions.index_dim.get_level(index))
                query_str.append(q)
            query_str.append('year_id in {}'.format(ref_years))
            query_str = " & ".join(query_str)
            ref_draws = reference_df.query(query_str)
            present_years = ref_draws['year_id'].tolist()
        else:
            present_years = []
            ref_draws = pd.DataFrame(columns=dimensions.index_names)
        # get years we don't have from the file system
        absent_years = [
            year for year in ref_years if year not in present_years]

        if len(absent_years) >= 1:
            interp_draws = self.super_gopher.content(
                location_id=dimensions.index_dim.get_level("location_id"),
                year_id=absent_years,
                sex_id=dimensions.index_dim.get_level("sex_id"),
                measure_id=dimensions.index_dim.get_level("measure_id"),
                age_group_id=dimensions.index_dim.get_level(
                    "age_group_id"))
        else:
            interp_draws = pd.DataFrame(columns=dimensions.index_names)

        # if we are going to interpolate, we need 2005 as a reference year
        if not interp_draws.empty:
            interp_years = interp_draws['year_id'].unique().tolist()
        else:
            interp_years = []
        if 2005 in present_years:
            rank_df = ref_draws.loc[ref_draws['year_id'] == 2005]
        elif (2005 in absent_years) and (2005 in interp_years):
            rank_df = interp_draws.loc[interp_draws['year_id'] == 2005]
        else:
            rank_df = self.super_gopher.content(
                location_id=dimensions.index_dim.get_level("location_id"),
                year_id=[2005],
                sex_id=dimensions.index_dim.get_level("sex_id"),
                measure_id=dimensions.index_dim.get_level("measure_id"),
                age_group_id=dimensions.index_dim.get_level(
                    "age_group_id"))

        return interp_draws.append(ref_draws), rank_df

    def read_inputs(self):
        """get como draws for a single modelable_entity/model_version"""
        print('Reading draws for (meid, mvid): ({}, {})'.format(self.meid,
                                                                self.mvid))
        if self.super_gopher is None:
            self.super_gopher = SuperGopher.auto(self.meid_data_dir)

        all_draws = []
        reference_draws = []
        missing_dim_q = []
        for dimensions in self.dimensions_q:

            gbdizer = gbdize.GBDizeDataFrame(dimensions)
            try:
                draws = self.super_gopher.content(
                    location_id=dimensions.index_dim.get_level("location_id"),
                    year_id=dimensions.index_dim.get_level("year_id"),
                    sex_id=dimensions.index_dim.get_level("sex_id"),
                    measure_id=dimensions.index_dim.get_level("measure_id"),
                    age_group_id=dimensions.index_dim.get_level(
                        "age_group_id"))
            except InvalidFilter:
                draws = pd.DataFrame(columns=dimensions.index_names)

            if not draws.empty:
                # gbdize. aka fill in missing dimensions
                draws = self.gbdize_dimensions(draws, gbdizer)
                # keep a copy of all 1000 draws for interpolation
                reference_draws.append(draws)

                # resample
                draws = self.resample_if_needed(draws, dimensions, gbdizer)

            if len(draws) != dimensions.total_cardinality:
                missing = self.missing_dimensions(draws, dimensions)
                missing_dim_q.append(missing)

            all_draws.append(draws)

        # prep for interpolation of missing demographics
        if len(reference_draws) > 0:
            reference_draws = pd.concat(reference_draws)
        else:
            reference_draws = pd.DataFrame(columns=dimensions.index_names)
        missing_dim_q = list(flatten(missing_dim_q))

        for dimensions in missing_dim_q:

            gbdizer = gbdize.GBDizeDataFrame(dimensions)
            interp_draws, rank_df = self.get_interpolation_draws(
                reference_draws, dimensions)

            if not interp_draws.empty:
                # gbdize. aka fill in missing dimensions
                interp_draws = self.gbdize_dimensions(interp_draws, gbdizer)
                rank_df = self.gbdize_dimensions(rank_df, gbdizer)

                # case where years are stored as floats, breaks interpolate
                interp_draws['year_id'] = interp_draws['year_id'].astype(int)
                try:
                    data_cols = ["draw_{}".format(i) for i in range(1000)]
                    interp_draws = gbdizer.fill_year_by_interpolating(
                        interp_draws, rank_df, data_cols)
                except MissingGBDemographics:
                    print (
                        "(meid: {meid}, mvid: {mvid}) "
                        " Could not interpolate for years: {years}, "
                        "measure: {meas} "
                        "location_id: {loc} "
                        "sex_id: {sex}".format(
                            meid=self.meid,
                            mvid=self.mvid,
                            years=dimensions.index_dim.get_level("year_id"),
                            meas=dimensions.index_dim.get_level("measure_id"),
                            loc=dimensions.index_dim.get_level("location_id"),
                            sex=dimensions.index_dim.get_level("sex_id")))
                    interp_draws = self.gbdize_dimensions(
                        interp_draws, gbdizer, "year_id")

                # append draws to reference
                reference_draws = reference_draws.append(
                    interp_draws, ignore_index=True)

                draws = interp_draws.loc[interp_draws['year_id'].isin(
                    dimensions.index_dim.get_level('year_id'))]

                # resample
                draws = self.resample_if_needed(draws, dimensions, gbdizer)
                all_draws.append(draws)

            # if dimensions overlap, drop duplicates from reference draws
            reference_draws.drop_duplicates(
                subset=dimensions.index_names, inplace=True)

        # concatenate all the results
        draws = pd.concat(all_draws)
        # in case dimensions overlap, drop duplicates
        draws.drop_duplicates(inplace=True)
        draws['modelable_entity_id'] = self.meid

        return draws.reset_index(drop=True)


class DisabilityWeightInputs(object):

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

        # Asymp
        draw_cols = ["draw_{}".format(i) for i in range(1000)]
        asymp_row = {'draw_%s' % i: 0 for i in range(1000)}
        asymp_row['healthstate_id'] = 799
        dws = dws.append(pd.DataFrame([asymp_row]))
        dws = dws.reset_index(drop=True)
        dws = dws[['healthstate_id'] + draw_cols]

        # resample
        if len(draw_cols) != len(self.dims.data_list()):
            dimensions = deepcopy(self.dims)
            dimensions.index_dim.drop_level("age_group_id")
            dimensions.index_dim.drop_level("year_id")
            dimensions.index_dim.drop_level("location_id")
            gbdizer = gbdize.GBDizeDataFrame(dimensions)
            dws = gbdizer.random_choice_resample(dws)

        self.dws = dws

    def get_id_dws(self):
        draw_cols = ["draw_{}".format(i) for i in range(1000)]

        dws = pd.read_csv(
            "filepath/03_custom/"
            "combined_id_dws.csv")
        dws['age_end'] = dws['age_end'] + 1
        dws['age_end'] = dws.age_end.replace({101: 200})
        dws.rename(
            columns={d.replace("_", ""): d for d in draw_cols},
            inplace=True)
        dws = dws[["age_start", "age_end", "healthstate_id"] + draw_cols]
        dimensions = deepcopy(self.dims)
        dimensions.index_dim.drop_level("year_id")
        dimensions.index_dim.drop_level("location_id")
        gbdizer = gbdize.GBDizeDataFrame(dimensions)

        dws = gbdizer.fill_age_from_continuous_range(
            dws, 12, "age_start", "age_end")
        dws = dws.reset_index(drop=True)
        if len(draw_cols) != len(self.dims.data_list()):
            dws = gbdizer.random_choice_resample(dws)
        self.id_dws = dws

    def _get_standard_dws(self):
        draw_cols = ["draw_{}".format(i) for i in range(1000)]
        dws = pd.read_csv(
            "filepath/02_standard/dw.csv")
        dws.rename(
            columns={d.replace("_", ""): d for d in draw_cols},
            inplace=True)
        return dws

    def _get_custom_dws(self):
        draw_cols = ["draw_{}".format(i) for i in range(1000)]
        dws = pd.read_csv(
            "filepath/03_custom/"
            "combined_dws.csv")
        dws.rename(
            columns={d.replace("_", ""): d for d in draw_cols},
            inplace=True)
        return dws

    def _get_mnd_dws(self):
        dws = pd.read_csv(
            "filepath/03_custom/"
            "combined_mnd_dws.csv")
        return dws

    def _get_autism_dws(self):
        dws = pd.read_csv(
            "filepath/03_custom/"
            "autism_dws.csv")
        return dws

    def _get_epi_dws(self):
        draw_cols = ["draw_{}".format(i) for i in range(1000)]
        eafp = os.path.join(self.cv.como_dir, 'info', 'epilepsy_any_dws.h5')
        ecfp = os.path.join(self.cv.como_dir, 'info', 'epilepsy_combo_dws.h5')
        loc_id = self.dims.index_dim.get_level("location_id")[0]
        year_id = self.dims.index_dim.get_level("year_id")[0]
        epi_any = pd.read_hdf(
            eafp, 'draws',
            where='location_id == {lid} & year_id == {y}'.format(
                lid=loc_id, y=year_id))
        epi_combos = pd.read_hdf(
            ecfp, 'draws',
            where='location_id == {lid} & year_id == {y}'.format(
                lid=loc_id, y=year_id))
        epi_dws = pd.concat([epi_any, epi_combos])
        return epi_dws[['healthstate_id'] + draw_cols]

    def _get_uro_dws(self):
        draw_cols = ["draw_{}".format(i) for i in range(1000)]
        fp = os.path.join(self.cv.como_dir, 'info', 'urolith_dws.h5')
        loc_id = self.dims.index_dim.get_level("location_id")[0]
        year_id = self.dims.index_dim.get_level("year_id")[0]
        uro_dws = pd.read_hdf(
            fp, 'draws',
            where='location_id == {lid} & year_id == {y}'.format(
                lid=loc_id, y=year_id))
        return uro_dws[['healthstate_id'] + draw_cols]

    def _get_inj_dws(self):
        draw_cols = ["draw_{}".format(i) for i in range(1000)]
        loc_id = self.dims.index_dim.get_level("location_id")[0]
        year_id = self.dims.index_dim.get_level("year_id")[0]
        inj_dws = pd.read_csv(
            "filepath/injuries/lt_dws_2016/draws/{lid}_{y}.csv".format(
                lid=loc_id, y=year_id))
        inj_dws = inj_dws.merge(
            self.cv.injury_dws_by_sequela,
            left_on="healthstate",
            right_on="n_code")
        inj_dws['healthstate_id'] = inj_dws.sequela_id
        inj_dws = inj_dws[['sequela_id', 'healthstate_id'] + draw_cols]
        return inj_dws
