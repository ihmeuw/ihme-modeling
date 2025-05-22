import json
import os
import pathlib
from multiprocessing import Process, Queue
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
import tblib.pickling_support

import draw_sources
import gbd.constants as gbd_constants
from dataframe_io.api.public import SuperPusher
from db_queries import get_age_spans
from db_queries.api.internal import get_age_group_set as get_age_group_set_id
from gbd.estimation_years import estimation_years_from_release_id
from get_draws.sources import epi as epi_draw_sources
from get_draws.transforms.automagic import automagic_age_sex_agg
from hierarchies import dbtrees
from ihme_dimensions import dimensionality, gbdize

import epic.legacy.util.constants as epic_constants
from epic.legacy.util.parsers import json_parser
from epic.lib.util.common import (
    get_annualized_year_set_by_release_id,
    get_best_model_version_and_release,
    group_and_downsample,
)
from epic.lib.util.exceptions import ExceptionWrapper

tblib.pickling_support.install()

_SENTINEL = None
_THIS_DIR = pathlib.Path(__file__).resolve().parent


class ExAdjust(object):
    """Exclusivity adjustment computation object."""

    def __init__(  # noqa: C901
        self,
        process_name: str,
        output_dir: str,
        location_id: Optional[List[int]] = None,
        year_id: Optional[List[int]] = None,
        age_group_id: Optional[List[int]] = None,
        sex_id: Optional[List[int]] = None,
        measure_id: Optional[List[int]] = None,
        location_set_id: int = 35,
        release_id: int = gbd_constants.RELEASE_ID,
        n_draws: int = 1000,
        annual: bool = False,
    ) -> None:
        # Load final epic map in this script, filter to
        # process and me_map, and then run through json parser in util
        # folder (to turn strings to ints). json requires string keys so we
        # can't convert all the modelable_entity_ids to ints.
        with open("FILEPATH") as f:
            emap = json.load(f)
        me_map = emap[process_name]["kwargs"]["me_map"]

        # set static values
        self.me_map = json_parser(json.dumps(me_map))
        self.location_set_id = location_set_id
        self.release_id = release_id
        self.annual_years = (
            get_annualized_year_set_by_release_id(self.release_id) if annual else None
        )
        try:
            self.copy_env_inc = emap[process_name]["kwargs"].pop("copy_env_inc")
        except KeyError:
            self.copy_env_inc = False

        # dimensions are derived unless explicit
        if not location_id:
            location_id = [
                node.id
                for node in dbtrees.loctree(
                    location_set_id=location_set_id, release_id=release_id
                ).leaves()
            ]
        if not year_id:
            year_id = estimation_years_from_release_id(release_id)
        if not age_group_id:
            # this has the advantage of instantiating the lru cache in the main
            # process before multiprocessing
            age_group_set_id = get_age_group_set_id()
            age_group_id = get_age_spans(
                age_group_set_id=age_group_set_id, release_id=release_id
            )[gbd_constants.columns.AGE_GROUP_ID].tolist()
            if self.me_map["resid"] in epic_constants.MEIDS.INCLUDE_BIRTH_PREV:
                age_group_id.append(gbd_constants.age.BIRTH)
        if not sex_id:
            sex_id = [gbd_constants.sex.MALE, gbd_constants.sex.FEMALE]
        if not measure_id:
            measure_id = [gbd_constants.measures.PREVALENCE, gbd_constants.measures.INCIDENCE]

        index_dict = {
            gbd_constants.columns.LOCATION_ID: location_id,
            gbd_constants.columns.YEAR_ID: year_id,
            gbd_constants.columns.AGE_GROUP_ID: age_group_id,
            gbd_constants.columns.SEX_ID: sex_id,
            gbd_constants.columns.MEASURE_ID: measure_id,
        }
        data_dict = {"data": ["draw_{}".format(i) for i in range(n_draws)]}
        self.dimensions = dimensionality.DataFrameDimensions(index_dict, data_dict)

        # draws that are imported or computed are stored here
        self.draws: Dict[Union[int, str], pd.DataFrame] = {}

        # objects for reading data
        self._importers: Dict[int, draw_sources.DrawSource] = {}
        for me_id in list(self.me_map["sub"].keys()) + [self.me_map["env"]]:
            mvid, _ = get_best_model_version_and_release(output_dir, me_id)
            me_source = epi_draw_sources.Epi.create_modelable_entity_draw_source(
                n_workers=1,
                modelable_entity_id=me_id,
                model_version_id=mvid,
                release_id=self.release_id,
                diff_cache_dir=os.path.join(
                    output_dir, epic_constants.FilePaths.GET_DRAWS_DIFF_CACHE
                ),
            )
            me_source.remove_transform(automagic_age_sex_agg)
            if n_draws < 1000:
                me_source.add_transform(group_and_downsample, n_draws)
            self._importers[me_id] = me_source

        # object for pushing results to disk
        self._pusher = SuperPusher(
            spec={
                "file_pattern": "{modelable_entity_id}/{location_id}.h5",
                "h5_tablename": "draws",
            },
            directory=output_dir,
        )

    @property
    def filters(self) -> Dict[str, List[int]]:
        """Dictionary with demographic filters."""
        return {
            gbd_constants.columns.LOCATION_ID: (
                self.dimensions.index_dim.get_level(gbd_constants.columns.LOCATION_ID)
            ),
            gbd_constants.columns.AGE_GROUP_ID: (
                self.dimensions.index_dim.get_level(gbd_constants.columns.AGE_GROUP_ID)
            ),
            gbd_constants.columns.YEAR_ID: (
                self.dimensions.index_dim.get_level(gbd_constants.columns.YEAR_ID)
            ),
            gbd_constants.columns.SEX_ID: (
                self.dimensions.index_dim.get_level(gbd_constants.columns.SEX_ID)
            ),
            gbd_constants.columns.MEASURE_ID: (
                self.dimensions.index_dim.get_level(gbd_constants.columns.MEASURE_ID)
            ),
        }

    def _import_draws(self) -> None:
        """Read in all required draws, filling the draws attribute dictionary."""
        gbdizer = gbdize.GBDizeDataFrame(self.dimensions)
        # If EPIC is run with the --annual argument we want to try and retrieve draws
        # for all years if possible. If draws are not available for all years then
        # we fall back to using the passed year IDs.
        missing_annual = False
        for me_id in self._importers.keys():
            draw_source = self._importers[me_id]
            if self.annual_years:
                draw_filters = self.filters.copy()
                draw_filters[gbd_constants.columns.YEAR_ID] = self.annual_years
                draws = draw_source.content(filters=draw_filters)
                all_draw_years = set(draws[gbd_constants.columns.YEAR_ID].unique())
                if not set(self.annual_years).issubset(all_draw_years):
                    missing_annual = True
                    draws = draws.loc[
                        draws[gbd_constants.columns.YEAR_ID].isin(
                            self.filters[gbd_constants.columns.YEAR_ID]
                        )
                    ]

            else:
                draws = draw_source.content(filters=self.filters)
            draws = gbdizer.fill_empty_indices(draws, 0)

            # Put draws in draws dict for access later
            self.draws[me_id] = draws

        # We need to track if any of the me_ids for an exclusivity adjustment are missing
        # a year_id. If so then we revert the draws to using the fallback year id
        for me_id in self._importers.keys():
            current_draws = self.draws[me_id]
            current_draws = (
                current_draws.loc[
                    current_draws[gbd_constants.columns.YEAR_ID].isin(
                        self.filters[gbd_constants.columns.YEAR_ID]
                    )
                ]
                if missing_annual
                else current_draws
            )
            current_draws = current_draws.set_index(self.dimensions.index_names)
            self.draws[me_id] = current_draws

    def _calc_sigma_sub(self) -> pd.DataFrame:
        """Calculate the sum of the sub sequela."""
        # concatenate all required frames
        sub_dfs: List[pd.DataFrame] = []
        for me_id in self.me_map["sub"].keys():
            sub_dfs.append(self.draws[me_id])
        sub_df = pd.concat(sub_dfs)

        # return the sum
        sub_df.reset_index(inplace=True)
        if self.copy_env_inc:
            draw_cols = self.dimensions.data_dim.get_level("data")
            sub_df.loc[sub_df[gbd_constants.columns.MEASURE_ID] == 6, draw_cols] = 0
        return sub_df.groupby(self.dimensions.index_names).sum()

    def _resid(self) -> pd.DataFrame:
        """Calculate the residual numbers."""
        # get the needed data
        sigma_sub_df = self.draws["sigma_sub"]
        env_df = self.draws[self.me_map["env"]]

        # if it is a squeeze type then we use the absolute value of the diff
        resid_df = (env_df - sigma_sub_df)[(sigma_sub_df <= env_df)].fillna(0)
        return resid_df

    def _excess(self, sub_me: int) -> pd.DataFrame:
        """Calculate the excess proportions."""
        # get the needed data
        sub_me_df = self.draws[sub_me]
        env_df = self.draws[self.me_map["env"]]
        sigma_sub_df = self.draws["sigma_sub"]

        # create a boolean dataframe for our 2 cases
        more = sigma_sub_df > env_df

        # now calculate the excess values
        excess_df = (
            (sigma_sub_df[more] - env_df[more]) * sub_me_df[more] / sigma_sub_df[more]
        ).fillna(value=0)
        return excess_df

    def _squeeze(self, sub_me: int) -> pd.DataFrame:
        """Calculate the squeezed proportions."""
        # get the needed data
        sub_me_df = self.draws[sub_me]
        env_df = self.draws[self.me_map["env"]]
        sigma_sub_df = self.draws["sigma_sub"]

        # create a boolean dataframe for our 2 cases
        more = sigma_sub_df > env_df

        # get the squeezed values when
        squeeze_more = env_df[more] * sub_me_df[more] / sigma_sub_df[more]
        squeeze_less = sub_me_df[~more]
        squeeze_df = squeeze_more.fillna(squeeze_less)
        return squeeze_df

    def _export(self) -> None:
        """Export all data."""
        # export residual
        me_id = self.me_map["resid"]
        resid_df = self.draws[me_id].reset_index()
        resid_df["modelable_entity_id"] = me_id
        self._pusher.push(resid_df, append=False)

        # export any subcause adjustments
        for sub_me in self.me_map["sub"].keys():
            if "squeeze" in list(self.me_map["sub"][sub_me].keys()):
                me_id = self.me_map["sub"][sub_me]["squeeze"]
                squeeze_df = self.draws[me_id].reset_index()
                squeeze_df[gbd_constants.columns.MODELABLE_ENTITY_ID] = me_id
                self._pusher.push(squeeze_df, append=False)

            if "excess" in list(self.me_map["sub"][sub_me].keys()):
                me_id = self.me_map["sub"][sub_me]["excess"]
                excess_df = self.draws[me_id].reset_index()
                excess_df[gbd_constants.columns.MODELABLE_ENTITY_ID] = me_id
                self._pusher.push(excess_df, append=False)

    def adjust(self) -> None:
        """Run exclusivity adjustment on all MEs."""
        self._import_draws()
        self.draws["sigma_sub"] = self._calc_sigma_sub()
        self.draws[self.me_map["resid"]] = self._resid()
        for sub_me in self.me_map["sub"].keys():
            if "squeeze" in list(self.me_map["sub"][sub_me].keys()):
                self.draws[self.me_map["sub"][sub_me]["squeeze"]] = self._squeeze(sub_me)
            if "excess" in list(self.me_map["sub"][sub_me].keys()):
                self.draws[self.me_map["sub"][sub_me]["excess"]] = self._excess(sub_me)
        self._export()

    def _q_adjust(self, in_queue: Queue, out_queue: Queue) -> None:
        """Wrapper around a single adjustment process, for catching errors."""
        for location_id in iter(in_queue.get, _SENTINEL):
            try:
                self.dimensions.index_dim.replace_level(
                    gbd_constants.columns.LOCATION_ID, location_id
                )
                self.adjust()
                out_queue.put((False, location_id))
            except Exception as e:
                out_queue.put((ExceptionWrapper(e), location_id))

    def run_all_adjustments_mp(self, n_processes: int = 23) -> None:
        """Multiprocess run of all exclusivity adjustments."""
        in_queue = Queue()
        out_queue = Queue()

        # Create and feed sim procs
        adjust_procs: List[Process] = []
        min_procs = min(
            [
                n_processes,
                self.dimensions.index_dim.cardinality(gbd_constants.columns.LOCATION_ID),
            ]
        )
        for _ in range(min_procs):
            p = Process(target=self._q_adjust, args=(in_queue, out_queue))
            adjust_procs.append(p)
            p.start()

        # run the silulations
        for location_id in self.dimensions.index_dim.get_level(
            gbd_constants.columns.LOCATION_ID
        ):
            in_queue.put(location_id)

        # make the workers die after
        for _ in adjust_procs:
            in_queue.put(_SENTINEL)

        # get results
        results: List[Tuple[ExceptionWrapper, int]] = []
        for _ in self.dimensions.index_dim.get_level(gbd_constants.columns.LOCATION_ID):
            proc_result = out_queue.get()
            results.append(proc_result)

        # close up the queue
        for p in adjust_procs:
            p.join()

        for exc, _ in results:
            if exc:
                exc.re_raise()
