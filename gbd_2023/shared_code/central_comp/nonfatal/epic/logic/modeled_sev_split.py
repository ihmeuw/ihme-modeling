import copy
import multiprocessing
import os
from typing import Dict, List, Optional, Tuple

import pandas as pd
import tblib.pickling_support

import draw_validation
from core_maths.interpolate import pchip_interpolate
from core_maths.scale_split import merge_split
from dataframe_io.api.public import SuperPusher
from dataframe_io.legacy import exceptions as df_io_exceptions
from db_queries import get_age_spans
from db_queries.api.internal import get_age_group_set as get_age_group_set_id
from gbd import constants as gbd_constants
from gbd import estimation_years
from get_draws.sources import epi as epi_draw_sources
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


class ModeledSevSplitter:
    """Modeled severity split computation object."""

    def __init__(
        self,
        parent_id: int,
        child_id: int,
        proportion_id: int,
        output_dir: str,
        location_id: Optional[List[int]] = None,
        year_id: Optional[List[int]] = None,
        age_group_id: Optional[List[int]] = None,
        sex_id: Optional[List[int]] = None,
        measure_id: Optional[List[int]] = None,
        proportion_measure_id: Optional[List[int]] = None,
        location_set_id: int = 35,
        n_draws: int = 1000,
        release_id: int = gbd_constants.RELEASE_ID,
        annual: bool = False,
    ) -> None:
        # static ids
        self.parent_id = parent_id
        self.child_id = child_id
        self.proportion_id = proportion_id
        self.location_set_id = location_set_id
        self.release_id = release_id
        self.n_draws = n_draws
        self.annual_years = (
            get_annualized_year_set_by_release_id(self.release_id) if annual else None
        )

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
            if self.child_id in epic_constants.MEIDS.INCLUDE_BIRTH_PREV:
                age_group_id.append(gbd_constants.age.BIRTH)
        if not sex_id:
            sex_id = [gbd_constants.sex.MALE, gbd_constants.sex.FEMALE]
            if self.child_id in epic_constants.MEIDS.FEMALE_ONLY:
                sex_id = [gbd_constants.sex.FEMALE]
        if not measure_id:
            measure_id = [gbd_constants.measures.PREVALENCE, gbd_constants.measures.INCIDENCE]
            if self.child_id in epic_constants.MEIDS.PREVALENCE_ONLY:
                measure_id = [gbd_constants.measures.PREVALENCE]
                if self.child_id in epic_constants.MEIDS.INCIDENCE_TO_PREVALENCE:
                    measure_id = [gbd_constants.measures.INCIDENCE]

        if not proportion_measure_id:
            proportion_measure_id = [
                gbd_constants.measures.PREVALENCE,
                gbd_constants.measures.INCIDENCE,
                gbd_constants.measures.PROPORTION,
            ]
            if self.child_id in epic_constants.MEIDS.PREVALENCE_ONLY:
                proportion_measure_id = [
                    gbd_constants.measures.PREVALENCE,
                    gbd_constants.measures.PROPORTION,
                ]

        # generate two different dimension objects, one for each source
        data_dict = {"data": ["draw_{}".format(i) for i in range(n_draws)]}
        parent_index_dict = {
            gbd_constants.columns.LOCATION_ID: location_id,
            gbd_constants.columns.YEAR_ID: year_id,
            gbd_constants.columns.AGE_GROUP_ID: age_group_id,
            gbd_constants.columns.SEX_ID: sex_id,
            gbd_constants.columns.MEASURE_ID: measure_id,
        }
        self.parent_dimensions = dimensionality.DataFrameDimensions(
            parent_index_dict, data_dict
        )
        proportion_index_dict = {
            gbd_constants.columns.LOCATION_ID: location_id,
            gbd_constants.columns.YEAR_ID: year_id,
            gbd_constants.columns.AGE_GROUP_ID: age_group_id,
            gbd_constants.columns.SEX_ID: sex_id,
            gbd_constants.columns.MEASURE_ID: proportion_measure_id,
        }
        self.proportion_dimensions = dimensionality.DataFrameDimensions(
            proportion_index_dict, data_dict
        )

        # parent draw source
        self.parent_mvid, _ = get_best_model_version_and_release(output_dir, self.parent_id)
        parent_src = epi_draw_sources.Epi.create_modelable_entity_draw_source(
            n_workers=1,
            modelable_entity_id=self.parent_id,
            model_version_id=self.parent_mvid,
            release_id=self.release_id,
            diff_cache_dir=os.path.join(
                output_dir, epic_constants.FilePaths.GET_DRAWS_DIFF_CACHE
            ),
        )
        try:
            parent_src.remove_transform(automagic_age_sex_agg)
        except ValueError:
            pass

        # proportion draw source
        self.proportion_mvid, _ = get_best_model_version_and_release(
            output_dir, self.proportion_id
        )
        proportion_src = epi_draw_sources.Epi.create_modelable_entity_draw_source(
            n_workers=1,
            modelable_entity_id=self.proportion_id,
            model_version_id=self.proportion_mvid,
            release_id=self.release_id,
            diff_cache_dir=os.path.join(
                output_dir, epic_constants.FilePaths.GET_DRAWS_DIFF_CACHE
            ),
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
            spec={
                "file_pattern": "{modelable_entity_id}/{location_id}.h5",
                "h5_tablename": "draws",
            },
            directory=output_dir,
        )

    def demo_filters(
        self, dimensions: dimensionality.DataFrameDimensions
    ) -> Dict[str, List[int]]:
        """Dictionary with demographic filters."""
        return {
            gbd_constants.columns.LOCATION_ID: (
                dimensions.index_dim.get_level(gbd_constants.columns.LOCATION_ID)
            ),
            gbd_constants.columns.AGE_GROUP_ID: (
                dimensions.index_dim.get_level(gbd_constants.columns.AGE_GROUP_ID)
            ),
            gbd_constants.columns.YEAR_ID: (
                dimensions.index_dim.get_level(gbd_constants.columns.YEAR_ID)
            ),
            gbd_constants.columns.SEX_ID: (
                dimensions.index_dim.get_level(gbd_constants.columns.SEX_ID)
            ),
            gbd_constants.columns.MEASURE_ID: (
                dimensions.index_dim.get_level(gbd_constants.columns.MEASURE_ID)
            ),
        }

    def split(self, location_id: int) -> None:  # noqa: C901
        """Runs the actual modeled sev split code.

        Modeled severity splits take a single parent model and a single proportion model, and
        apply the proportion to the parent in order to generate a single severity specific
        child model. As an example, for a parent Cause A, we apply the mild proportion and
        end up with Mild Cause A as our output.
        """
        parent_filters = copy.deepcopy(self.demo_filters(self.parent_dimensions))
        parent_filters[gbd_constants.columns.MODELABLE_ENTITY_ID] = [self.parent_id]
        parent_filters[gbd_constants.columns.LOCATION_ID] = [location_id]
        prop_filters = copy.deepcopy(self.demo_filters(self.proportion_dimensions))
        prop_filters[gbd_constants.columns.MODELABLE_ENTITY_ID] = [self.proportion_id]
        prop_filters[gbd_constants.columns.LOCATION_ID] = [location_id]
        # If EPIC is run with the --annual argument we want to try and retrieve draws
        # for all years if possible. If draws are not available for all years then
        # we fall back to using the passed year IDs. The years that are available from
        # the parent draw source will be used as the year filter for split proportion draws.
        if self.annual_years:
            parent_filters[gbd_constants.columns.YEAR_ID] = self.annual_years
            try:
                parent_draws = self._parent_draw_source.content(filters=parent_filters)
            except df_io_exceptions.InvalidFilter as e:
                if gbd_constants.columns.MODELABLE_ENTITY_ID not in str(e):
                    raise
                else:
                    parent_filters.pop(gbd_constants.columns.MODELABLE_ENTITY_ID)
                    parent_draws = self._parent_draw_source.content(filters=parent_filters)
            all_draw_years = set(parent_draws[gbd_constants.columns.YEAR_ID].unique())
            if not set(self.annual_years).issubset(all_draw_years):
                parent_draws = parent_draws.loc[
                    parent_draws[gbd_constants.columns.YEAR_ID].isin(
                        self.demo_filters(self.parent_dimensions)[
                            gbd_constants.columns.YEAR_ID
                        ]
                    )
                ]
                prop_filters[gbd_constants.columns.YEAR_ID] = self.demo_filters(
                    self.parent_dimensions
                )[gbd_constants.columns.YEAR_ID]
            else:
                prop_filters[gbd_constants.columns.YEAR_ID] = self.annual_years

        else:
            try:
                parent_draws = self._parent_draw_source.content(filters=parent_filters)
            except df_io_exceptions.InvalidFilter as e:
                if gbd_constants.columns.MODELABLE_ENTITY_ID not in str(e):
                    raise
                else:
                    parent_filters.pop(gbd_constants.columns.MODELABLE_ENTITY_ID)
                    parent_draws = self._parent_draw_source.content(filters=parent_filters)
            all_draw_years = set(parent_draws[gbd_constants.columns.YEAR_ID].unique())
            prop_filters[gbd_constants.columns.YEAR_ID] = list(all_draw_years)

        # Drop unused columns
        parent_draws = parent_draws.drop(
            [
                gbd_constants.columns.MODEL_VERSION_ID,
                gbd_constants.columns.MODELABLE_ENTITY_ID,
                "wormhole_model_version_id",
            ],
            axis=1,
            errors="ignore",
        )

        # If birth is expected, check it exists
        if self.child_id in epic_constants.MEIDS.INCLUDE_BIRTH_PREV:
            draw_validation.check_vals_present(
                df=parent_draws,
                col=gbd_constants.columns.AGE_GROUP_ID,
                values=[gbd_constants.age.BIRTH],
            )

        # Validate expected column values exist
        parent_validation_filters = copy.deepcopy(prop_filters)
        parent_validation_filters[gbd_constants.columns.MEASURE_ID] = parent_filters[
            gbd_constants.columns.MEASURE_ID
        ]
        for col in epic_constants.DRAW_VALIDATION_COLUMNS:
            draw_validation.check_vals_present(
                df=parent_draws, col=col, values=parent_validation_filters[col]
            )

        # If running for an ME where we convert incidence to prevalence, subset to incidence
        # only.
        if self.child_id in epic_constants.MEIDS.INCIDENCE_TO_PREVALENCE:
            parent_draws = parent_draws.loc[
                parent_draws[gbd_constants.columns.MEASURE_ID]
                == (gbd_constants.measures.INCIDENCE)
            ]
            if len(parent_draws) == 0:
                raise RuntimeError(
                    f"No incidence draws for parent ME {self.parent_id}, cannot convert "
                    f"incidence draws to prevalence draws for child ME {self.child_id}"
                )
            parent_draws[gbd_constants.columns.MEASURE_ID] = gbd_constants.measures.PREVALENCE

        # Get prop draws
        try:
            proportion_draws = self._proportion_draw_source.content(filters=prop_filters)
        except df_io_exceptions.InvalidFilter as e:
            if gbd_constants.columns.MODELABLE_ENTITY_ID not in str(e):
                raise
            else:
                prop_filters.pop(gbd_constants.columns.MODELABLE_ENTITY_ID)
                proportion_draws = self._parent_draw_source.content(filters=prop_filters)

        # Drop unused columns
        proportion_draws = proportion_draws.drop(
            [
                gbd_constants.columns.MODEL_VERSION_ID,
                gbd_constants.columns.MODELABLE_ENTITY_ID,
                "wormhole_model_version_id",
            ],
            axis=1,
            errors="ignore",
        )

        # If we're running with annual and the parent draws have annual results, interpolate
        # the proportions if they don't already have annual results. Note that this doesn't
        # extrapolate, and we assume that any annual run will have all years beyond the GBD
        # round year as year inputs. E.G for GBD 2020, we assume for an annual run the
        # year_id arg contains 2020, 2021, and 2022.
        all_prop_years = set(proportion_draws[gbd_constants.columns.YEAR_ID].unique())
        if (prop_filters[gbd_constants.columns.YEAR_ID] == self.annual_years) and not set(
            self.annual_years
        ).issubset(all_prop_years):
            prop_group_cols = [
                col
                for col in proportion_draws
                if (("draw_" not in col) and (col != gbd_constants.columns.YEAR_ID))
            ]
            prop_draw_cols = [col for col in proportion_draws if "draw_" in col]
            proportion_draws = pd.concat(
                [
                    proportion_draws,
                    pchip_interpolate(
                        df=proportion_draws,
                        id_cols=prop_group_cols,
                        value_cols=prop_draw_cols,
                        time_col=gbd_constants.columns.YEAR_ID,
                    ),
                ]
            )
        # get the list of all proportion draw measures, for making proportion draws square
        # with parent draws
        proportion_draw_measures = (
            proportion_draws[gbd_constants.columns.MEASURE_ID].unique().tolist()
        )
        expanded_proportion_draws = []
        # loop over parent draw measures, making sure that corresponding proportion draw
        # measures exist, and using the actual proportion measure_id if they don't
        for measure in parent_draws[gbd_constants.columns.MEASURE_ID].unique():
            if measure in proportion_draw_measures:
                prop_subset = proportion_draws.loc[
                    proportion_draws[gbd_constants.columns.MEASURE_ID] == measure
                ]
            elif gbd_constants.measures.PROPORTION in proportion_draw_measures:
                prop_subset = proportion_draws.loc[
                    proportion_draws[gbd_constants.columns.MEASURE_ID]
                    == (gbd_constants.measures.PROPORTION)
                ]
            else:
                raise ValueError(
                    f"No appropriate proportion ME measures for parent measure {measure}, "
                    f"location_id {location_id}, parent ME {self.parent_id}, proportion ME "
                    f"{self.proportion_id}."
                )
            prop_subset[gbd_constants.columns.MEASURE_ID] = measure
            expanded_proportion_draws.append(prop_subset)
        expanded_proportion_draws = pd.concat(expanded_proportion_draws)

        # If birth is expected, check it exists
        if self.child_id in epic_constants.MEIDS.INCLUDE_BIRTH_PREV:
            bp_measures = [
                gbd_constants.measures.PREVALENCE,
                gbd_constants.measures.PROPORTION,
            ]
            bp_mask = expanded_proportion_draws[gbd_constants.columns.MEASURE_ID].isin(
                bp_measures
            )
            draw_validation.check_vals_present(
                df=expanded_proportion_draws.loc[bp_mask],
                col=gbd_constants.columns.AGE_GROUP_ID,
                values=[gbd_constants.age.BIRTH],
            )

        # Validate expected column values exist
        for col in epic_constants.DRAW_VALIDATION_COLUMNS:
            if col == gbd_constants.columns.MEASURE_ID:
                draw_validation.check_vals_present(
                    df=expanded_proportion_draws,
                    col=col,
                    values=parent_draws[gbd_constants.columns.MEASURE_ID].unique().tolist(),
                )
            else:
                draw_validation.check_vals_present(
                    df=expanded_proportion_draws, col=col, values=prop_filters[col]
                )

        # run the actual split. since anemia causal attribution scales severities to one
        # and we run on severity specific sets of draws, force_scale must be False
        group_cols = [col for col in parent_draws if "draw_" not in col]
        value_cols = [col for col in parent_draws if "draw_" in col]
        splits = merge_split(
            parent_draws,
            expanded_proportion_draws,
            group_cols=group_cols,
            value_cols=value_cols,
            force_scale=False,
        )
        if splits.empty:
            raise RuntimeError("Split results empty.")
        splits[gbd_constants.columns.MODELABLE_ENTITY_ID] = self.child_id
        splits = splits[
            self.parent_dimensions.index_names
            + [gbd_constants.columns.MODELABLE_ENTITY_ID]
            + self.parent_dimensions.data_list()
        ]
        splits = splits.fillna(0)
        print(f"Pushing draws for location_id {location_id}")
        self.pusher.push(splits, append=False)

    def _q_split(
        self, in_queue: multiprocessing.Queue, out_queue: multiprocessing.Queue
    ) -> None:
        """Modeled sev split wrapper for an individual process."""
        for location_id in iter(in_queue.get, _SENTINEL):
            print(location_id)
            try:
                self.parent_dimensions.index_dim.replace_level(
                    gbd_constants.columns.LOCATION_ID, location_id
                )
                self.proportion_dimensions.index_dim.replace_level(
                    gbd_constants.columns.LOCATION_ID, location_id
                )
                self.split(location_id)
                out_queue.put((False, location_id))
            except Exception as e:
                out_queue.put((ExceptionWrapper(e), location_id))

    def run_all_splits_mp(self, n_processes: int = 23) -> None:
        """Multiprocessed modeled sev split run."""
        in_queue = multiprocessing.Queue()
        out_queue = multiprocessing.Queue()

        # Create and feed sim procs
        split_procs: List[multiprocessing.Process] = []
        min_procs = min(
            [
                n_processes,
                self.parent_dimensions.index_dim.cardinality(
                    gbd_constants.columns.LOCATION_ID
                ),
            ]
        )
        for _ in range(min_procs):
            p = multiprocessing.Process(target=self._q_split, args=(in_queue, out_queue))
            split_procs.append(p)
            p.start()

        # run the simulations
        for location_id in self.parent_dimensions.index_dim.get_level(
            gbd_constants.columns.LOCATION_ID
        ):
            in_queue.put(location_id)

        # make the workers die after
        for _ in split_procs:
            in_queue.put(_SENTINEL)

        # get results
        results: List[Tuple[ExceptionWrapper, int]] = []
        for _ in self.parent_dimensions.index_dim.get_level(
            gbd_constants.columns.LOCATION_ID
        ):
            proc_result = out_queue.get()
            results.append(proc_result)

        # close up the queue
        for p in split_procs:
            p.join()

        for exc, _ in results:
            if exc:
                exc.re_raise()
