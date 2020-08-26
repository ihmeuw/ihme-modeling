import sys
from multiprocessing import Process, Queue

import tblib.pickling_support

from core_maths.scale_split import merge_split
from hierarchies import dbtrees
from ihme_dimensions import dimensionality
from ihme_dimensions.gbdize import get_age_group_set
from get_draws.sources.epi import Epi
from get_draws.sources.severity_prop import (SevPropFormula,
                                             split_prop_read_func)
from dataframe_io.pusher import SuperPusher
from dataframe_io.io_control.h5_io import H5IO
import argparse

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

class SevSplitter(object):

    def __init__(
            self, split_version_id, output_dir, location_id=[], year_id=[],
            age_group_id=[], sex_id=[], measure_id=[], location_set_id=35,
            gbd_round_id=5, n_draws=1000):

        # static ids
        self.split_version_id = split_version_id
        self.location_set_id = location_set_id
        self.gbd_round_id = gbd_round_id

        # dimensions are derived unless explicit
        if not location_id:
            location_id = [
                node.id for node in dbtrees.loctree(
                    location_set_id=location_set_id,
                    gbd_round_id=gbd_round_id).leaves()]
        if not year_id:
            year_id = [1990, 1995, 2000, 2005, 2010, 2017]
        if not age_group_id:
            # this has the advantage of intantiating the lru cache in the main
            # process before multiprocessing
            age_group_id = get_age_group_set(12)["age_group_id"].tolist()
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

        # read func is derived from static values. we call it to initialize the
        # internal caching
        self._read_func = split_prop_read_func()
        self._read_func(params={}, filters=self.ss_filters)

        # ss draw source is derived from static values
        sp_formula = SevPropFormula(
            location_set_id=location_set_id, n_draws=n_draws)
        sp_formula.build_custom_draw_source(
            params={}, read_func=self._read_func)
        sp_formula.add_transforms()
        self._ss_draw_source = sp_formula.draw_source

        # epi draws source
        self._epi_draw_source = Epi.create_modelable_entity_draw_source(
            n_workers=1,
            modelable_entity_id=self.parent_meid,
            gbd_round_id=gbd_round_id)

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

        # run the silulations
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--split_version_id", required=True, type=int)
    parser.add_argument("--output_dir", required=True, type=str)
    args = parser.parse_args()

    splitter = SevSplitter(
        split_version_id=args.split_version_id,
        output_dir=args.output_dir)
    splitter.run_all_splits_mp()