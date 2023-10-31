import argparse
import sys
import pandas as pd
import glob
from multiprocessing import Process, Queue

import tblib.pickling_support

from core_maths.scale_split import merge_split
from hierarchies import dbtrees
from ihme_dimensions import dimensionality

from get_draws.sources.epi import Epi
from get_draws.sources.severity_prop import (SevPropFormula,
                                             split_prop_read_func)
from dataframe_io.api.public import SuperPusher
from dataframe_io.io_control.h5_io import H5IO

from job_utils import draws, parsers

from db_queries import get_best_model_versions, get_location_metadata, get_age_metadata

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
            self, split_version_id, output_dir, decomp_step, location_id,
            year_id=[], age_group_id=[], sex_id=[], measure_id=[],
            gbd_round_id=7, release_id=9, n_draws=1000):

        # static ids
        self.split_version_id = split_version_id
        self.decomp_step = decomp_step
        self.gbd_round_id = gbd_round_id
        self.output_dir = output_dir

        # dimensions are derived unless explicit
        if not year_id:
            year_id = [1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022]
        if not age_group_id:
            # this has the advantage of intantiating the lru cache in the main
            # process before multiprocessing
            #age_group_id = get_age_group_set(19)["age_group_id"].tolist()
            age_group_id= get_age_metadata(age_group_set_id=19, release_id=9)["age_group_id"].tolist()
        if not sex_id:
            sex_id = [1,2]
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
        sp_formula = SevPropFormula()
        sp_formula.build_custom_draw_source(
            params={}, read_func=self._read_func)
        sp_formula.add_transforms()
        self._ss_draw_source = sp_formula.draw_source

        # epi draws source
        self._epi_draw_source = Epi.create_modelable_entity_draw_source(
            n_workers=1,
            modelable_entity_id=self.parent_meid,
            #gbd_round_id=gbd_round_id,
            #decomp_step=decomp_step,
            release_id=release_id)

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

        expanded_gprops = pd.concat([gprops.assign(year_id=year) for year in draws.year_id.unique()])
        expanded_gprops = pd.concat([expanded_gprops.assign(age_group_id=age) for age in draws.age_group_id.unique()])

        print(draws.columns)
        print(self.dimensions.data_list())

        splits = merge_split(
            draws,
            gprops,
            group_cols=['year_id', 'age_group_id', 'measure_id', 'sex_id', 'location_id'],
            value_cols=self.dimensions.data_list(), force_scale=False)
        splits = splits.assign(modelable_entity_id=splits['child_meid'])
        splits = splits[self.dimensions.index_names + ["modelable_entity_id"] +
                        self.dimensions.data_list()]
        splits = splits.fillna(0)
        self.pusher.push(splits, append=False)

        # Drop duplicates in hdf files that are appearing for some reason
        # location_id = self.demo_filters['location_id']
        # for filepath in glob.glob(self.output_dir + f'/*/{location_id}.h5'):
        #     df = pd.read_hdf(filepath)
        #     df = df.drop_duplicates()
        #     df.to_hdf(filepath, key='/draws')
        #     print(f"Dropped duplicates from {filepath}")

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
        print('Creating processes')
        # Create and feed sim procs
        split_procs = []
        min_procs = min(
            [n_processes, self.dimensions.index_dim.cardinality("location_id")]
        )
        for i in range(min_procs):
            p = Process(target=self._q_split, args=(inq, outq))
            split_procs.append(p)
            p.start()
        print('Running simulations')
        # run the simulations
        for location_id in self.dimensions.index_dim.get_level("location_id"):
            inq.put(location_id)
        print('Making workers die')
        # make the workers die after
        for _ in split_procs:
            inq.put(sentinel)
        print('Getting results')
        # get results
        results = []
        for location_id in self.dimensions.index_dim.get_level("location_id"):
            proc_result = outq.get()
            results.append(proc_result)
        print('Closing up queue')
        # close up the queue
        for p in split_procs:
            p.join()

        for exc, location_id in results:
            if exc:
                exc.re_raise()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("gbd_round_id", type=int)
    parser.add_argument("decomp_step")
    parser.add_argument("--split_version_id", required=True, type=int)
    parser.add_argument("--output_dir", required=True, type=str)
    parser.add_argument("--location_id", required=True, type=int)
    args = parser.parse_args()

    splitter = SevSplitter(
        split_version_id=args.split_version_id,
        output_dir=args.output_dir,
        gbd_round_id=args.gbd_round_id,
        decomp_step=args.decomp_step,
        location_id=args.location_id)
    splitter.split()
