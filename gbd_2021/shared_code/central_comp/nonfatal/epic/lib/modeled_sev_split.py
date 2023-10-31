import sys
import multiprocessing

import pandas as pd
import tblib.pickling_support

from core_maths.scale_split import merge_split
from dataframe_io.pusher import SuperPusher
from db_queries import get_age_spans
from db_queries.api.internal import get_age_group_set as get_age_group_set_id
import gbd.constants as gbd_constants
from gbd.estimation_years import estimation_years_from_gbd_round_id
from get_draws.sources.epi import Epi
from get_draws.transforms.automagic import automagic_age_sex_agg
from hierarchies import dbtrees
from ihme_dimensions import dimensionality

from epic.util.common import (
    get_best_model_version_and_decomp_step,
    group_and_downsample
)
import epic.util.constants as epic_constants
from epic.util.exceptions import ExceptionWrapper

tblib.pickling_support.install()

_SENTINEL = None


class ModeledSevSplitter():

    def __init__(
            self, parent_id, child_id, proportion_id, output_dir, decomp_step,
            location_id=None, year_id=None, age_group_id=None, sex_id=None, measure_id=None,
            proportion_measure_id=None, location_set_id=35, n_draws=1000,
            gbd_round_id=gbd_constants.GBD_ROUND_ID
    ):

        # static ids
        self.parent_id = parent_id
        self.child_id = child_id
        self.proportion_id = proportion_id
        self.decomp_step = decomp_step
        self.location_set_id = location_set_id
        self.gbd_round_id = gbd_round_id
        self.n_draws = n_draws

        # dimensions are derived unless explicit
        if not location_id:
            location_id = [
                node.id for node in dbtrees.loctree(
                    location_set_id=location_set_id,
                    gbd_round_id=gbd_round_id
                ).leaves()
            ]
        if not year_id:
            year_id = estimation_years_from_gbd_round_id(gbd_round_id)
        if not age_group_id:
            # this has the advantage of instantiating the lru cache in the main
            # process before multiprocessing
            age_group_set_id = get_age_group_set_id(gbd_round_id)
            age_group_id = get_age_spans(age_group_set_id)["age_group_id"].tolist()
            if self.child_id in epic_constants.MEIDS.INCLUDE_BIRTH_PREV:
                age_group_id.append(gbd_constants.age.BIRTH)
        if not sex_id:
            sex_id = [gbd_constants.sex.MALE, gbd_constants.sex.FEMALE]
        if not measure_id:
            measure_id = [gbd_constants.measures.PREVALENCE, gbd_constants.measures.INCIDENCE]
        if not proportion_measure_id:
            proportion_measure_id = [
                gbd_constants.measures.PREVALENCE,
                gbd_constants.measures.INCIDENCE,
                gbd_constants.measures.PROPORTION,
            ]

        # generate two different dimension objects, one for each source
        data_dict = {"data": ['draw_{}'.format(i) for i in range(n_draws)]}
        parent_index_dict = {
            "location_id": location_id,
            "year_id": year_id,
            "age_group_id": age_group_id,
            "sex_id": sex_id,
            "measure_id": measure_id
        }
        self.parent_dimensions = dimensionality.DataFrameDimensions(
            parent_index_dict, data_dict
        )
        proportion_index_dict = {
            "location_id": location_id,
            "year_id": year_id,
            "age_group_id": age_group_id,
            "sex_id": sex_id,
            "measure_id": proportion_measure_id
        }
        self.proportion_dimensions = dimensionality.DataFrameDimensions(
            proportion_index_dict, data_dict
        )

        # parent draw source
        self.parent_mvid, dstep = (
            get_best_model_version_and_decomp_step(output_dir, self.parent_id)
        )
        parent_src = Epi.create_modelable_entity_draw_source(
            n_workers=1,
            modelable_entity_id=self.parent_id,
            model_version_id=self.parent_mvid,
            gbd_round_id=self.gbd_round_id,
            decomp_step=dstep
        )
        try:
            parent_src.remove_transform(automagic_age_sex_agg)
        except ValueError:
            pass

        # proportion draw source
        self.proportion_mvid, dstep = (
            get_best_model_version_and_decomp_step(output_dir, self.proportion_id)
        )
        proportion_src = Epi.create_modelable_entity_draw_source(
            n_workers=1,
            modelable_entity_id=self.proportion_id,
            model_version_id=self.proportion_mvid,
            gbd_round_id=self.gbd_round_id,
            decomp_step=dstep
        )
        try:
            proportion_src.remove_transform(automagic_age_sex_agg)
        except ValueError:
            pass

        # add downsampling transform if necessary
        if n_draws < 1000:
            parent_src.add_transform(group_and_downsample, n_draws)
            proportion_src.add_transform(group_and_downsample, n_draws)
        self._parent_draw_source = parent_src
        self._proportion_draw_source = proportion_src

        self.pusher = SuperPusher(
            spec={'file_pattern': "{modelable_entity_id}/{location_id}.h5",
                  'h5_tablename': 'draws'},
            directory=output_dir)

    def demo_filters(self, dimensions):
        return {
            "location_id": dimensions.index_dim.get_level("location_id"),
            "age_group_id": dimensions.index_dim.get_level("age_group_id"),
            "year_id": dimensions.index_dim.get_level("year_id"),
            "sex_id": dimensions.index_dim.get_level("sex_id"),
            "measure_id": dimensions.index_dim.get_level("measure_id")
        }

    def split(self, location_id: int) -> None:
        # get parent draws
        parent_draws = self._parent_draw_source.content(
            filters=self.demo_filters(self.parent_dimensions).copy()
        )
        parent_draws = parent_draws.drop(
            ['model_version_id', 'modelable_entity_id'], axis=1)
        # get prop draws
        proportion_draws = self._proportion_draw_source.content(
            filters=self.demo_filters(self.proportion_dimensions).copy()
        )
        proportion_draws = proportion_draws.drop(
            ['model_version_id', 'modelable_entity_id'], axis=1)
        expanded_proportion_draws = []
        for measure in self.parent_dimensions.index_dim.get_level("measure_id"):
            prop_copy = proportion_draws.copy()
            prop_copy["measure_id"] = measure
            expanded_proportion_draws.append(prop_copy)
        expanded_proportion_draws = pd.concat(expanded_proportion_draws)
        # run the actual split.
        group_cols = [col for col in parent_draws if 'draw_' not in col]
        value_cols = [col for col in parent_draws if 'draw_' in col]
        splits = merge_split(
            parent_draws,
            expanded_proportion_draws,
            group_cols=group_cols,
            value_cols=value_cols,
            force_scale=False
        )
        splits['modelable_entity_id'] = self.child_id
        splits = splits[
            self.parent_dimensions.index_names +
            ["modelable_entity_id"] +
            self.parent_dimensions.data_list()
        ]
        splits = splits.fillna(0)
        if self.child_id in epic_constants.MEIDS.INCIDENCE_TO_PREVALENCE:
            splits = splits[splits['measure_id'] == gbd_constants.measures.INCIDENCE]
            splits['measure_id'] = gbd_constants.measures.PREVALENCE
        print("Pushing draws")
        self.pusher.push(splits, append=False)

    def _q_split(
        self, in_queue: multiprocessing.Queue, out_queue: multiprocessing.Queue
    ) -> None:
        for location_id in iter(in_queue.get, _SENTINEL):
            print(location_id)
            try:
                self.parent_dimensions.index_dim.replace_level("location_id", location_id)
                self.proportion_dimensions.index_dim.replace_level("location_id", location_id)
                self.split(location_id)
                out_queue.put((False, location_id))
            except Exception as e:
                out_queue.put((ExceptionWrapper(e), location_id))

    def run_all_splits_mp(self, n_processes: int=23) -> None:
        in_queue = multiprocessing.Queue()
        out_queue = multiprocessing.Queue()

        # Create and feed sim procs
        split_procs = []
        min_procs = min(
            [n_processes, self.parent_dimensions.index_dim.cardinality("location_id")]
        )
        for i in range(min_procs):
            p = multiprocessing.Process(target=self._q_split, args=(in_queue, out_queue))
            split_procs.append(p)
            p.start()

        # run the simulations
        for location_id in self.parent_dimensions.index_dim.get_level("location_id"):
            in_queue.put(location_id)

        # make the workers die after
        for _ in split_procs:
            in_queue.put(_SENTINEL)

        # get results
        results = []
        for location_id in self.parent_dimensions.index_dim.get_level("location_id"):
            proc_result = out_queue.get()
            results.append(proc_result)

        # close up the queue
        for p in split_procs:
            p.join()

        for exc, location_id in results:
            if exc:
                exc.re_raise()
