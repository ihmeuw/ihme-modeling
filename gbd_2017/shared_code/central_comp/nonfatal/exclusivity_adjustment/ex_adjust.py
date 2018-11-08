import sys
from multiprocessing import Process, Queue

import tblib.pickling_support
import pandas as pd

from hierarchies import dbtrees
from ihme_dimensions import dimensionality, gbdize
from get_draws.sources.epi import Epi
from get_draws.transforms.automagic import automagic_age_sex_agg


from dataframe_io.pusher import SuperPusher
from dataframe_io.io_control.h5_io import H5IO


sentinel = None
tblib.pickling_support.install()


# https://stackoverflow.com/questions/6126007/python-getting-a-traceback-from-a-multiprocessing-process
class ExceptionWrapper(object):

    def __init__(self, ee):
        self.ee = ee
        _, _, self.tb = sys.exc_info()

    def re_raise(self):
        if sys.version_info[0] >= 3:
            raise self.ee.with_traceback(self.tb)
        else:
            raise (self.ee, None, self.tb)


class ExAdjust(object):

    def __init__(
            self, me_map, output_dir, location_id=[], year_id=[],
            age_group_id=[], sex_id=[], measure_id=[], location_set_id=35,
            gbd_round_id=5, n_draws=1000, copy_env_inc=False):
        # set static values
        self.me_map = me_map
        self.location_set_id = location_set_id
        self.gbd_round_id = gbd_round_id
        self.copy_env_inc = copy_env_inc

        # dimensions are derived unless explicit
        if not location_id:
            location_id = [
                node.id for node in dbtrees.loctree(
                    location_set_id=location_set_id,
                    gbd_round_id=gbd_round_id).leaves()]
        if not year_id:
            year_id = [1990, 1995, 2000, 2005, 2010, 2017]
        if not age_group_id:
            age_group_id = range(2, 21) + [30, 31, 32, 235]
        if not sex_id:
            sex_id = [1, 2]
        if not measure_id:
            measure_id = [5, 6]

        index_dict = {
            "location_id": location_id,
            "year_id": year_id,
            "age_group_id": age_group_id,
            "sex_id": sex_id,
            "measure_id": measure_id
        }
        data_dict = {"data": ['draw_{}'.format(i) for i in range(n_draws)]}
        self.dimensions = dimensionality.DataFrameDimensions(index_dict,
                                                             data_dict)

        # draws that are imported or computed are stored here
        self.draws = {}

        # objects for reading data
        self._importers = {}
        for me_id in me_map["sub"].keys() + [me_map["env"]]:
            me_source = Epi.create_modelable_entity_draw_source(
                n_workers=1,
                modelable_entity_id=me_id,
                gbd_round_id=gbd_round_id)
            me_source.remove_transform(automagic_age_sex_agg)
            self._importers[me_id] = me_source

        # object for pushing results to disk
        self._pusher = SuperPusher(
            spec={'file_pattern': "{modelable_entity_id}/{location_id}.h5",
                  'h5_tablename': 'draws'},
            directory=output_dir)

    @property
    def filters(self):
        return {
            "location_id": self.dimensions.index_dim.get_level("location_id"),
            "age_group_id": self.dimensions.index_dim.get_level("age_group_id"
                                                                ),
            "year_id": self.dimensions.index_dim.get_level("year_id"),
            "sex_id": self.dimensions.index_dim.get_level("sex_id"),
            "measure_id": self.dimensions.index_dim.get_level("measure_id")
        }

    def _import_draws(self):
        gbdizer = gbdize.GBDizeDataFrame(self.dimensions)

        # import draws
        for me_id in self._importers.keys():
            draw_source = self._importers[me_id]
            draws = draw_source.content(filters=self.filters)
            draws = gbdizer.fill_empty_indices(draws, 0)
            self.draws[me_id] = draws.set_index(self.dimensions.index_names)

    def _calc_sigma_sub(self):
        """calculate the sum of the sub sequela"""
        # concatenate all required frames
        sub_dfs = []
        for me_id in self.me_map["sub"].keys():
            sub_dfs.append(self.draws[me_id])
        sub_df = pd.concat(sub_dfs)

        # return the sum
        sub_df.reset_index(inplace=True)
        if self.copy_env_inc:
            draw_cols = self.dimensions.data_dim.get_level("data")
            sub_df.loc[sub_df['measure_id'] == 6, draw_cols] = 0
        return sub_df.groupby(self.dimensions.index_names).sum()

    def _resid(self):
        """calculate the residual numbers"""
        # get the needed data
        sigma_sub_df = self.draws["sigma_sub"]
        env_df = self.draws[self.me_map["env"]]

        # if it is a squeeze type then we use the absolute value of the diff
        resid_df = (env_df - sigma_sub_df)[(sigma_sub_df <= env_df)].fillna(0)
        return resid_df

    def _excess(self, sub_me):
        """calculate the excess proportions"""
        # get the needed data
        sub_me_df = self.draws[sub_me]
        env_df = self.draws[self.me_map["env"]]
        sigma_sub_df = self.draws["sigma_sub"]

        # create a boolean dataframe for our 2 cases
        more = (sigma_sub_df > env_df)

        # now calculate the excess values
        excess_df = (
            (sigma_sub_df[more] - env_df[more]) * sub_me_df[more] /
            sigma_sub_df[more]
        ).fillna(value=0)
        return excess_df

    def _squeeze(self, sub_me):
        """calculate the squeezed proportions"""
        # get the needed data
        sub_me_df = self.draws[sub_me]
        env_df = self.draws[self.me_map["env"]]
        sigma_sub_df = self.draws["sigma_sub"]

        # create a boolean dataframe for our 2 cases
        more = (sigma_sub_df > env_df)

        # get the squeezed values when
        squeeze_more = env_df[more] * sub_me_df[more] / sigma_sub_df[more]
        squeeze_less = sub_me_df[~more]
        squeeze_df = squeeze_more.fillna(squeeze_less)
        return squeeze_df

    def _export(self):
        """export all data"""
        # export residual
        me_id = self.me_map["resid"]
        resid_df = self.draws[me_id].reset_index()
        resid_df["modelable_entity_id"] = me_id
        self._pusher.push(resid_df, append=False)

        # export any subcause adjustments
        for sub_me in self.me_map["sub"].keys():
            if "squeeze" in self.me_map["sub"][sub_me].keys():
                me_id = self.me_map["sub"][sub_me]["squeeze"]
                squeeze_df = self.draws[me_id].reset_index()
                squeeze_df["modelable_entity_id"] = me_id
                self._pusher.push(squeeze_df, append=False)

            if "excess" in self.me_map["sub"][sub_me].keys():
                me_id = self.me_map["sub"][sub_me]["excess"]
                excess_df = self.draws[me_id].reset_index()
                excess_df["modelable_entity_id"] = me_id
                self._pusher.push(excess_df, append=False)

    def adjust(self):
        """run exclusivity adjustment on all MEs"""
        self._import_draws()
        self.draws["sigma_sub"] = self._calc_sigma_sub()
        self.draws[self.me_map["resid"]] = self._resid()
        for sub_me in self.me_map["sub"].keys():
            if "squeeze" in self.me_map["sub"][sub_me].keys():
                self.draws[self.me_map["sub"][sub_me]["squeeze"]] = (
                    self._squeeze(sub_me))
            if "excess" in self.me_map["sub"][sub_me].keys():
                self.draws[self.me_map["sub"][sub_me]["excess"]] = (
                    self._excess(sub_me))
        self._export()

    def _q_adjust(self, inq, outq):
        for location_id in iter(inq.get, sentinel):
            try:
                self.dimensions.index_dim.replace_level("location_id",
                                                        location_id)
                self.adjust()
                outq.put((False, location_id))
            except Exception as e:
                outq.put((ExceptionWrapper(e), location_id))

    def run_all_adjustments_mp(self, n_processes=23):
        inq = Queue()
        outq = Queue()

        # Create and feed sim procs
        adjust_procs = []
        min_procs = min(
            [n_processes, self.dimensions.index_dim.cardinality("location_id")]
        )
        for i in range(min_procs):
            p = Process(target=self._q_adjust, args=(inq, outq))
            adjust_procs.append(p)
            p.start()

        # run the silulations
        for location_id in self.dimensions.index_dim.get_level("location_id"):
            inq.put(location_id)

        # make the workers die after
        for _ in adjust_procs:
            inq.put(sentinel)

        # get results
        results = []
        for location_id in self.dimensions.index_dim.get_level("location_id"):
            proc_result = outq.get()
            results.append(proc_result)

        # close up the queue
        for p in adjust_procs:
            p.join()

        for exc, location_id in results:
            if exc:
                exc.re_raise()
