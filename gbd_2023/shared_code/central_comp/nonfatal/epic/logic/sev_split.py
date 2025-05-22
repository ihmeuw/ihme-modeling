import os
from multiprocessing import Process, Queue
from typing import Dict, List, Optional, Tuple

import tblib.pickling_support

import draw_validation
from core_maths.scale_split import merge_split
from dataframe_io.api.public import SuperPusher
from db_queries import get_age_spans
from db_queries.api.internal import get_age_group_set as get_age_group_set_id
from gbd import constants as gbd_constants
from gbd import estimation_years
from get_draws.sources.epi import Epi
from get_draws.sources.severity_prop import SevPropFormula, split_prop_read_func
from get_draws.transforms.automagic import automagic_age_sex_agg
from hierarchies import dbtrees
from ihme_dimensions import dimensionality

import epic.legacy.util.constants as epic_constants
from epic.lib.util.common import (
    get_annualized_year_set_by_release_id,
    get_best_model_version_and_release,
    group_and_downsample,
)
from epic.lib.util.exceptions import ExceptionWrapper

tblib.pickling_support.install()

_SENTINEL = None


class SevSplitter(object):
    """Standard severity split computation object."""

    def __init__(
        self,
        split_version_id: int,
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
        self.split_version_id = split_version_id
        self.location_set_id = location_set_id
        self.release_id = release_id
        self.annual_years = (
            get_annualized_year_set_by_release_id(self.release_id) if annual else None
        )

        # read func is derived from static values. we call it to initialize the
        # internal caching
        self._read_func = split_prop_read_func()
        _ = self._read_func(params={}, filters=self.ss_filters)

        # dimensions are derived unless explicit
        if not location_id:
            location_id = [
                node.id
                for node in dbtrees.loctree(
                    location_set_id=location_set_id, release_id=release_id
                ).leaves()
            ]
        if not year_id:
            year_id = estimation_years.estimation_years_from_release_id(release_id)
        if not age_group_id:
            # this has the advantage of instantiating the lru cache in the main
            # process before multiprocessing
            age_group_set_id = get_age_group_set_id()
            age_group_id = get_age_spans(
                age_group_set_id=age_group_set_id, release_id=release_id
            )[gbd_constants.columns.AGE_GROUP_ID].tolist()
            if set(self.child_meid).intersection(
                set(epic_constants.MEIDS.INCLUDE_BIRTH_PREV)
            ):
                age_group_id.append(gbd_constants.age.BIRTH)
        if not sex_id:
            sex_id = [gbd_constants.sex.MALE, gbd_constants.sex.FEMALE]
            if set(self.child_meid).issubset(set(epic_constants.MEIDS.FEMALE_ONLY)):
                sex_id = [gbd_constants.sex.FEMALE]
        if not measure_id:
            measure_id = [gbd_constants.measures.PREVALENCE, gbd_constants.measures.INCIDENCE]
            if set(self.child_meid).issubset(set(epic_constants.MEIDS.PREVALENCE_ONLY)):
                measure_id = [gbd_constants.measures.PREVALENCE]

        index_dict = {
            gbd_constants.columns.LOCATION_ID: location_id,
            gbd_constants.columns.YEAR_ID: year_id,
            gbd_constants.columns.AGE_GROUP_ID: age_group_id,
            gbd_constants.columns.SEX_ID: sex_id,
            gbd_constants.columns.MEASURE_ID: measure_id,
        }
        data_dict = {"data": ["draw_{}".format(i) for i in range(n_draws)]}
        self.dimensions = dimensionality.DataFrameDimensions(index_dict, data_dict)

        sp_formula = SevPropFormula(
            release_id=self.release_id, n_draws=n_draws, location_set_id=self.location_set_id
        )
        sp_formula.build_custom_draw_source(params={}, read_func=self._read_func)
        sp_formula.add_transforms()
        self._ss_draw_source = sp_formula.draw_source

        # epi draws source
        mvid, _ = get_best_model_version_and_release(output_dir, int(self.parent_meid))
        src = Epi.create_modelable_entity_draw_source(
            n_workers=1,
            modelable_entity_id=int(self.parent_meid),
            model_version_id=mvid,
            release_id=self.release_id,
            diff_cache_dir=os.path.join(
                output_dir, epic_constants.FilePaths.GET_DRAWS_DIFF_CACHE
            ),
        )
        src.remove_transform(automagic_age_sex_agg)

        if n_draws < 1000:
            src.add_transform(group_and_downsample, n_draws)

        self._epi_draw_source = src

        self.pusher = SuperPusher(
            spec={
                "file_pattern": "{modelable_entity_id}/{location_id}.h5",
                "h5_tablename": "draws",
            },
            directory=output_dir,
        )

    @property
    def ss_filters(self) -> Dict[str, int]:
        """Dictionary with split version ID."""
        return {"split_version_id": self.split_version_id}

    @property
    def demo_filters(self) -> Dict[str, List[int]]:
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

    @property
    def parent_meid(self) -> int:
        """Parent ME ID."""
        df = self._read_func(params={}, filters=self.ss_filters)
        return df["parent_meid"].unique()[0]

    @property
    def child_meid(self) -> List[int]:
        """List of child ME IDs."""
        df = self._read_func(params={}, filters=self.ss_filters)
        return df["child_meid"].unique().tolist()

    def split(self) -> None:
        """Runs the actual standard sev split code.

        Standard severity splits take an input (parent) model and severity proportions from
        the sev splits database, and apply the severity proportions to the parent model in
        order to generate severity specific child models. As an example, for a parent Cause
        A, we apply the mild/moderate/severe proportions and end up with Mild Cause A
        Moderate Cause A, and Severe Cause A as our outputs.

        For standard severity splits, all child models are generated in a single job.
        """
        # Split filter year_id will be updated by the results of the epi input draws
        split_filters = self.ss_filters
        split_filters.update(self.demo_filters)
        # If EPIC is run with the --annual argument we want to try and retrieve draws
        # for all years if possible. If draws are not available for all years then
        # we fall back to using the passed year IDs. The years that are available from
        # the epi draw source will be used as the year filter for split proportion draws.
        if self.annual_years:
            draw_filters = self.demo_filters.copy()
            draw_filters[gbd_constants.columns.YEAR_ID] = self.annual_years
            draws = self._epi_draw_source.content(filters=draw_filters)
            all_draw_years = set(draws[gbd_constants.columns.YEAR_ID].unique())
            if not set(self.annual_years).issubset(all_draw_years):
                draws = draws.loc[
                    draws[gbd_constants.columns.YEAR_ID].isin(
                        self.demo_filters[gbd_constants.columns.YEAR_ID]
                    )
                ]
                split_filters[gbd_constants.columns.YEAR_ID] = self.demo_filters[
                    gbd_constants.columns.YEAR_ID
                ]
            else:
                split_filters[gbd_constants.columns.YEAR_ID] = self.annual_years

        else:
            draws = self._epi_draw_source.content(filters=self.demo_filters)
            all_draw_years = set(draws[gbd_constants.columns.YEAR_ID].unique())
            split_filters[gbd_constants.columns.YEAR_ID] = list(all_draw_years)

        # If birth is expected, check it exists
        if set(self.child_meid).intersection(set(epic_constants.MEIDS.INCLUDE_BIRTH_PREV)):
            draw_validation.check_vals_present(
                df=draws,
                col=gbd_constants.columns.AGE_GROUP_ID,
                values=[gbd_constants.age.BIRTH],
            )

        # Validate expected column values exist
        for col in epic_constants.DRAW_VALIDATION_COLUMNS:
            draw_validation.check_vals_present(
                df=draws, col=col, values=self.demo_filters[col]
            )

        # get split props
        gprops = self._ss_draw_source.content(filters=split_filters)

        splits = merge_split(
            draws,
            gprops,
            group_cols=self.dimensions.index_names,
            value_cols=self.dimensions.data_list(),
        )
        splits = splits.assign(modelable_entity_id=splits["child_meid"])
        splits = splits[
            self.dimensions.index_names
            + [gbd_constants.columns.MODELABLE_ENTITY_ID]
            + self.dimensions.data_list()
        ]
        splits = splits.fillna(0)
        self.pusher.push(splits, append=False)

    def _q_split(self, inq: Queue, outq: Queue) -> None:
        """Standard sev split wrapper for an individual process."""
        for location_id in iter(inq.get, _SENTINEL):
            print(location_id)
            try:
                self.dimensions.index_dim.replace_level(
                    gbd_constants.columns.LOCATION_ID, location_id
                )
                self.split()
                outq.put((False, location_id))
            except Exception as e:
                outq.put((ExceptionWrapper(e), location_id))

    def run_all_splits_mp(self, n_processes: int = 23) -> None:
        """Multiprocessed standard sev split run."""
        in_queue = Queue()
        out_queue = Queue()

        # Create and feed sim procs
        split_procs: List[Process] = []
        min_procs = min(
            [
                n_processes,
                self.dimensions.index_dim.cardinality(gbd_constants.columns.LOCATION_ID),
            ]
        )
        for _ in range(min_procs):
            p = Process(target=self._q_split, args=(in_queue, out_queue))
            split_procs.append(p)
            p.start()

        # run the simulations
        for location_id in self.dimensions.index_dim.get_level(
            gbd_constants.columns.LOCATION_ID
        ):
            in_queue.put(location_id)

        # make the workers die after
        for _ in split_procs:
            in_queue.put(_SENTINEL)

        # get results
        results: List[Tuple[ExceptionWrapper, int]] = []
        for _ in self.dimensions.index_dim.get_level(gbd_constants.columns.LOCATION_ID):
            proc_result = out_queue.get()
            results.append(proc_result)

        # close up the queue
        for p in split_procs:
            p.join()

        for exc, _ in results:
            if exc:
                exc.re_raise()
