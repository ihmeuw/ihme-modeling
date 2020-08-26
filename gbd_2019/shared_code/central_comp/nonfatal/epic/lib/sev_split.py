import os
import sys
from multiprocessing import Process, Queue

import tblib.pickling_support

from core_maths.scale_split import merge_split
from dataframe_io.pusher import SuperPusher
from dataframe_io.io_control.h5_io import H5IO
from epic.util.common import (
    get_best_model_version_and_decomp_step,
    group_and_downsample
)
import gbd.constants as gbd
from gbd.estimation_years import estimation_years_from_gbd_round_id
from get_draws.sources.epi import Epi
from get_draws.sources.severity_prop import (SevPropFormula,
                                             split_prop_read_func)
from get_draws.transforms.automagic import automagic_age_sex_agg
from hierarchies import dbtrees
from ihme_dimensions import dimensionality
from ihme_dimensions.gbdize import get_age_group_set

sentinel = None
tblib.pickling_support.install()


class ExceptionWrapper(object):

    def __init__(self, ee):
        self.ee = ee
        _, _, self.tb = sys.exc_info()

    def re_raise(self):
        if sys.version_info[0] >= 3:
            raise self.ee.with_traceback(self.tb)
        else:
            raise (self.ee, None, self.tb)


class SevSplitter(object):

    def __init__(
            self, split_version_id, output_dir, decomp_step, location_id=None,
            year_id=None, age_group_id=None, sex_id=None, measure_id=None,
            location_set_id=35, gbd_round_id=gbd.GBD_ROUND_ID,
            n_draws=1000):

        # static ids
        self.split_version_id = split_version_id
        self.decomp_step = decomp_step
        self.location_set_id = location_set_id
        self.gbd_round_id = gbd_round_id

        # read func is derived from static values. we call it to initialize the
        # internal caching
        self._read_func = split_prop_read_func()
        cached_props = self._read_func(
            params={}, filters=self.ss_filters)

        # dimensions are derived unless explicit
        if not location_id:
            location_id = [
                node.id for node in dbtrees.loctree(
                    location_set_id=location_set_id,
                    gbd_round_id=gbd_round_id).leaves()]
        if not year_id:
            year_id = estimation_years_from_gbd_round_id(gbd_round_id)
        if not age_group_id:
            # this has the advantage of instantiating the lru cache in the main
            # process before multiprocessing
            age_group_id = get_age_group_set(12)["age_group_id"].tolist()
            if -1 in cached_props["age_start"].unique().tolist():
                age_group_id.append(164)
        if not sex_id:
            sex_id = [gbd.sex.MALE, gbd.sex.FEMALE]
        if not measure_id:
            measure_id = [gbd.measures.PREVALENCE, gbd.measures.INCIDENCE]

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

        sp_formula = SevPropFormula()
        sp_formula.build_custom_draw_source(
            params={}, read_func=self._read_func)
        sp_formula.add_transforms()
        self._ss_draw_source = sp_formula.draw_source

        # epi draws source
        mvid, dstep = get_best_model_version_and_decomp_step(
            output_dir,
            int(self.parent_meid)
        )
        src = Epi.create_modelable_entity_draw_source(
            n_workers=1,
            modelable_entity_id=int(self.parent_meid),
            model_version_id=mvid,
            gbd_round_id=gbd_round_id,
            decomp_step=dstep
        )
        src.remove_transform(automagic_age_sex_agg)

        if n_draws < 1000:
            src.add_transform(group_and_downsample, n_draws)

        self._epi_draw_source = src

        self.pusher = SuperPusher(
            spec={'file_pattern': "{modelable_entity_id}/{location_id}.h5",
                  'h5_tablename': 'draws'},
            directory=output_dir)

    @property
    def ss_filters(self):
        return {"split_version_id": self.split_version_id}

    @property
    def demo_filters(self):
        return {
            "location_id": self.dimensions.index_dim.get_level("location_id"),
            "age_group_id": self.dimensions.index_dim.get_level("age_group_id"
                                                                ),
            "year_id": self.dimensions.index_dim.get_level("year_id"),
            "sex_id": self.dimensions.index_dim.get_level("sex_id"),
            "measure_id": self.dimensions.index_dim.get_level("measure_id")
        }

    @property
    def parent_meid(self):
        df = self._read_func(params={}, filters=self.ss_filters)
        return df.parent_meid.unique()[0]

    @property
    def child_meid(self):
        df = self._read_func(params={}, filters=self.ss_filters)
        return df.child_meid.unique().tolist()

    def split(self):
        # get input draws
        draws = self._epi_draw_source.content(filters=self.demo_filters.copy())
        # get split props
        filters = self.ss_filters
        filters.update(self.demo_filters)
        gprops = self._ss_draw_source.content(filters=filters)
        splits = merge_split(
            draws,
            gprops,
            group_cols=self.dimensions.index_names,
            value_cols=self.dimensions.data_list())
        splits = splits.assign(modelable_entity_id=splits['child_meid'])
        splits = splits[self.dimensions.index_names + ["modelable_entity_id"] +
                        self.dimensions.data_list()]
        splits = splits.fillna(0)
        self.pusher.push(splits, append=False)

    def _q_split(self, inq, outq):
        for location_id in iter(inq.get, sentinel):
            print(location_id)
            try:
                self.dimensions.index_dim.replace_level("location_id",
                                                        location_id)
                self.split()
                outq.put((False, location_id))
            except Exception as e:
                outq.put((ExceptionWrapper(e), location_id))

    def run_all_splits_mp(self, n_processes=23):
        inq = Queue()
        outq = Queue()

        # Create and feed sim procs
        split_procs = []
        min_procs = min(
            [n_processes, self.dimensions.index_dim.cardinality("location_id")]
        )
        for i in range(min_procs):
            p = Process(target=self._q_split, args=(inq, outq))
            split_procs.append(p)
            p.start()

        # run the simulations
        for location_id in self.dimensions.index_dim.get_level("location_id"):
            inq.put(location_id)

        # make the workers die after
        for _ in split_procs:
            inq.put(sentinel)

        # get results
        results = []
        for location_id in self.dimensions.index_dim.get_level("location_id"):
            proc_result = outq.get()
            results.append(proc_result)

        # close up the queue
        for p in split_procs:
            p.join()

        for exc, location_id in results:
            if exc:
                exc.re_raise()
