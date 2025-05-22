import getpass
import os
import subprocess  # noqa: S404
from copy import deepcopy
from pathlib import Path
from typing import ContextManager, List, Optional, Tuple, Type, TypeVar, Union

import pandas as pd
from loguru import logger
from sqlalchemy import text

import aggregator as ag
import db_tools_core
from aggregator.operators import Sum
from db_queries import get_ids, get_population, get_rei_metadata
from db_queries.api.internal import (
    get_active_cause_set_version,
    get_active_location_set_version,
    get_active_sequela_set_version,
)
from draw_sources.draw_sources import DrawSink, DrawSource
from draw_sources.io import mem_read_func, mem_write_func
from gbd import conn_defs
from gbd.constants import (
    age,
    gbd_metadata_type,
    gbd_process,
    gbd_process_version_status,
    measures,
)
from gbd.release import get_previous_release
from gbd_outputs_versions import (
    CompareVersion,
    DBEnvironment,
    GBDProcessVersion,
    internal_to_process_version,
)
from gbd_outputs_versions.convenience import bleeding_edge
from hierarchies import dbtrees
from ihme_cc_gbd_schema.common import ModelStorageMetadata

import como
import como.lib.database_io as dbio
import como.lib.resource_file_io as fileio
from como.legacy.common import propagate_hierarchy, validate_best_models
from como.legacy.model_versions import get_best_model_versions
from como.legacy.residuals import compute_global_ratios
from como.legacy.summarize import ComoSummaries
from como.lib import constants
from como.lib.cache_utils import FCache
from como.lib.cached_properties import CachedPropertyMixin
from como.lib.disability_weights import urolithiasis
from como.lib.nonfatal_dimensions import NonfatalDimensions

ComoVersionType = TypeVar("ComoVersionType", bound="ComoVersionType")


class ComoVersion(CachedPropertyMixin):
    """File-backed cache for COMO simulation."""

    def __init__(self, como_dir: str, read_only: bool = False):
        """Minimal initialization for ComoVersion.

        ComoVersion.new() is a full contstructor.
        """
        if not read_only:
            self.generate_version_directories(como_dir)
        self.como_dir = como_dir
        self._cache = FCache(self._cache_dir)
        if not read_only:
            self.export_environment(como_dir)
            self.write_storage_metadata(como_dir=como_dir)

    @property
    def _cache_dir(self) -> str:
        """Cache directory."""
        return os.path.join(self.como_dir, "info")

    @property
    def nonfatal_dimensions(self) -> NonfatalDimensions:
        """Dimensions of the simulation."""
        sim_idx = deepcopy(self.simulation_index)
        draw_idx = deepcopy(self.draw_index)
        cause_idx = deepcopy(self.cause_index)
        seq_idx = deepcopy(self.sequela_index)
        imp_idx = deepcopy(self.impairment_index)
        inj_idx = deepcopy(self.injuries_index)
        return NonfatalDimensions(sim_idx, draw_idx, cause_idx, seq_idx, imp_idx, inj_idx)

    @classmethod
    def new(
        cls: Type[ComoVersionType],
        release_id: int,
        gbd_process_version_note: str,
        location_set_id: int,
        nonfatal_cause_set_id: int,
        incidence_cause_set_id: int,
        impairment_rei_set_id: int,
        injury_rei_set_id: int,
        year_id: Union[int, List[int]],
        measure_id: Union[int, List[int]],
        n_draws: int,
        n_simulants: int,
        components: List[str],
        change_years: List[Tuple[int]],
        agg_loc_sets: List[int],
        codcorrect_version: int,
        internal_upload: bool,
        public_upload: bool,
        public_upload_test: bool,
        include_reporting_cause_set: bool,
        skip_sequela_summary: bool,
        minimum_incidence_cod: bool = True,
        como_base_dir: str = "FILEPATH",
    ) -> ComoVersionType:
        """Construct a new ComoVersion file-backed cache object with explicit args."""
        logger.info("Determining como_version and adding to the EPI DB.")
        code_version = como.__version__
        como_version_id = dbio.generate_new_version(
            release_id=release_id, code_version=code_version
        )
        como_dir = os.path.join(como_base_dir, str(como_version_id))
        inst = cls(como_dir)

        inst.como_base_dir = como_base_dir
        inst.como_version_id = como_version_id
        inst.components = components
        inst.release_id = release_id
        inst.year_id = year_id
        inst.change_years = change_years
        inst.measure_id = measure_id
        inst.n_simulants = n_simulants
        inst.minimum_incidence_cod = minimum_incidence_cod
        inst.codcorrect_version = codcorrect_version
        gbd_db_env = "ENV"
        if public_upload_test:
            gbd_db_env = "ENV"
        inst.codcorrect_process_version = internal_to_process_version(
            version_id=codcorrect_version, gbd_process_id=gbd_process.COD, env="ENV"
        )
        inst.epic_version = GBDProcessVersion.get_best(
            gbd_process_id=gbd_process.EPIC, release_id=release_id, env="ENV"
        ).metadata[gbd_metadata_type.EPIC]
        inst.public_upload = public_upload
        inst.public_upload_test = public_upload_test
        inst.include_reporting_cause_set = include_reporting_cause_set
        inst.skip_sequela_summary = skip_sequela_summary
        inst.internal_upload = internal_upload
        inst.nonfatal_cause_set_id = nonfatal_cause_set_id
        inst.incidence_cause_set_id = incidence_cause_set_id
        inst.impairment_rei_set_id = impairment_rei_set_id
        inst.injury_rei_set_id = injury_rei_set_id

        logger.info("Pulling active set version for given entities.")
        inst.cause_set_version_id = get_active_cause_set_version(
            cause_set_id=inst.nonfatal_cause_set_id, release_id=inst.release_id
        )
        inst.reporting_cause_set_version_id = (
            get_active_cause_set_version(
                cause_set_id=constants.REPORTING_CAUSE_SET_ID, release_id=inst.release_id
            )
            if include_reporting_cause_set
            else None
        )
        inst.sequela_set_version_id = get_active_sequela_set_version(
            sequela_set_id=2, release_id=inst.release_id
        )

        # Temporary validation while updating the sequela hierarchy
        # is a manual process, this validation can be removed when
        # a script is created that updates the hierarchies.
        if "impairment" in inst.components:
            imp_seq = dbio.get_impairment_sequela(inst.sequela_set_version_id)
            if len(imp_seq) == 0:
                raise ValueError(
                    "No impairment sequela in sequela set version"
                    f" {inst.sequela_set_version_id}, check the "
                    "sequela_rei_history table."
                )

        inst.inc_cause_set_version_id = get_active_cause_set_version(
            cause_set_id=inst.incidence_cause_set_id, release_id=inst.release_id
        )
        inst.location_set_version_id = get_active_location_set_version(
            location_set_id=location_set_id, release_id=inst.release_id
        )

        loc_info_list = [
            {
                "location_set_id": location_set_id,
                "location_set_version_id": get_active_location_set_version(
                    location_set_id=location_set_id, release_id=inst.release_id
                ),
            }
        ]
        if agg_loc_sets:
            for set_id in agg_loc_sets:
                loc_info_list.append(
                    {
                        "location_set_id": set_id,
                        "location_set_version_id": get_active_location_set_version(
                            location_set_id=set_id, release_id=inst.release_id
                        ),
                    }
                )
        inst.location_info = loc_info_list

        logger.info("Getting list of best model versions, active causes & sequela.")
        inst.mvid_list = get_best_model_versions(inst.release_id)
        inst.cause_list = get_ids(table="cause")[["cause_id", "acause"]]
        inst.sequela_list = dbio.get_sequela_list(
            sequela_set_version_id=inst.sequela_set_version_id
        )

        logger.info("Pulling a bunch of lists of exceptions and one off things.")
        inst.agg_cause_exceptions = dbio.get_agg_cause_exceptions(
            release_id=inst.release_id, cause_set_version_id=inst.cause_set_version_id
        )
        inst.cause_restrictions = dbio.get_cause_restrictions(
            cause_set_version_id=inst.cause_set_version_id
        )
        inst.birth_prev = fileio.get_birth_prevalence()
        inst.injury_sequela = fileio.get_injury_sequela()
        inst.sexual_violence_sequela = fileio.get_sexual_violence_sequela()
        inst.injury_dws_by_sequela = fileio.get_injury_dws_by_sequela()
        inst.impairment_sequela = dbio.get_impairment_sequela(
            sequela_set_version_id=inst.sequela_set_version_id
        )
        inst.validate_inputs()

        logger.info("Generating impairment & ncode hierarchy dfs.")
        inst.impairment_hierarchy = get_rei_metadata(
            rei_set_id=inst.impairment_rei_set_id, release_id=inst.release_id
        )
        inst.ncode_hierarchy = dbio.get_ncode_hierarchy(
            injury_rei_set_id=inst.injury_rei_set_id, release_id=inst.release_id
        )

        logger.info("Generating dictionaries of dimensions/indexes for different entities.")
        inst.simulation_index = dbio.get_simulation_index(
            location_set_version_id=inst.location_set_version_id,
            release_id=inst.release_id,
            year_id=year_id,
        )
        inst.draw_index = {"draws": [f"draw_{d}" for d in range(n_draws)]}
        inst.cause_index = dbio.get_cause_index(
            cause_set_version_id=inst.cause_set_version_id, release_id=inst.release_id
        )
        inst.sequela_index = dbio.get_sequela_index(
            sequela_set_version_id=inst.sequela_set_version_id, release_id=inst.release_id
        )
        inst.new_impairment_index()
        inst.new_injuries_index()

        logger.info("Reading some other dfs & caching them accordingly.")
        inst.new_global_ratios(year_id=year_id, n_draws=n_draws)
        inst.new_population(location_set_id, agg_loc_sets=agg_loc_sets)
        inst.new_urolith_dws(location_set_id)

        logger.info("Creating a matching process version in the GBD database.")
        inst.create_gbd_process_version(
            code_version=code_version,
            year_id=year_id,
            n_draws=n_draws,
            gbd_process_version_note=gbd_process_version_note,
        )

        logger.info("Dumping cache configuration to json.")
        inst._cache.dump_config()
        return inst

    def generate_version_directories(self, como_dir: str) -> None:
        """Generate directory tree."""
        como_dir_path = Path(como_dir)
        if not como_dir_path.exists():
            como_dir_path.mkdir(mode=0o775)
        for d in fileio.get_file_cache_dirs():
            full_path = como_dir_path / d
            if not full_path.exists():
                full_path.mkdir(mode=0o775, parents=True)

    def write_storage_metadata(self, como_dir: str) -> None:
        """Writes storage metadata to a given directory."""
        draw_storage_metadata = ModelStorageMetadata.from_dict(
            {
                "storage_pattern": (
                    "FILEPATH"
                ),
                "h5_tablename": "draws",
            }
        )
        draw_storage_metadata.to_file(directory=como_dir)

    @staticmethod
    def export_environment(como_dir: str) -> None:
        """Export environment configs."""
        user = getpass.getuser()
        subprocess.Popen(  # noqa
            ["FILEPATH"],
            cwd="FILEPATH",
        )
        os.system(f"conda env export > FILEPATH")  # noqa

    def load_cache(self) -> None:
        """Loads the cache."""
        self._cache.load_config()

    def new_cause_index(self) -> None:
        """New cause index."""
        ctree = dbtrees.causetree(
            cause_set_version_id=self.cause_set_version_id, release_id=self.release_id
        )
        cause_id = [node.id for node in ctree.nodes]
        self.cause_index = {"cause_id": cause_id}

    def new_impairment_index(self) -> None:
        """New impairment index."""
        rei_keys = ["rei_id", "cause_id"]
        sequela_cause = self.sequela_list[["sequela_id", "cause_id"]]
        imp_pairs = sequela_cause.merge(self.impairment_sequela)
        imp_pairs = imp_pairs[rei_keys]
        imp_pairs = imp_pairs[~imp_pairs.duplicated()]
        cause_tree = dbtrees.causetree(
            cause_set_version_id=self.cause_set_version_id, release_id=self.release_id
        )
        imp_pairs = propagate_hierarchy(cause_tree, imp_pairs, "cause_id")
        rei_tree = dbtrees.reitree(
            rei_set_id=self.impairment_rei_set_id, release_id=self.release_id
        )
        imp_pairs = propagate_hierarchy(rei_tree, imp_pairs, "rei_id")
        # rei_id 191 - Impairments (this is the aggregate, that's why we drop)
        imp_pairs = imp_pairs[imp_pairs.rei_id != 191]
        rei_vals = list(set(tuple(x) for x in imp_pairs[rei_keys].values))
        self.impairment_index = {tuple(rei_keys): rei_vals}

    def new_injuries_index(self) -> None:
        """New injuries index."""
        inj_keys = ["cause_id", "rei_id"]
        inj_pairs = self.injury_sequela[inj_keys]
        inj_pairs = inj_pairs[~inj_pairs.duplicated()]
        cause_tree = dbtrees.causetree(
            cause_set_version_id=self.cause_set_version_id, release_id=self.release_id
        )
        inj_pairs = propagate_hierarchy(cause_tree, inj_pairs, "cause_id")
        agg_pairs = inj_pairs.merge(self.ncode_hierarchy)
        agg_pairs = agg_pairs.drop("rei_id", axis=1)
        agg_pairs = agg_pairs.rename(columns={"parent_id": "rei_id"})
        inj_pairs = pd.concat([inj_pairs, agg_pairs[inj_keys]])
        inj_pairs = inj_pairs[~inj_pairs.duplicated()]
        inj_vals = list(set(tuple(x) for x in inj_pairs[inj_keys].values))
        self.injuries_index = {tuple(inj_keys): inj_vals}

    def new_global_ratios(self, year_id: Union[int, List[int]], n_draws: int) -> None:
        """New global ratios."""
        df = compute_global_ratios(
            cause_id=self.cause_list["cause_id"].values,
            year_id=year_id,
            release_id=self.release_id,
            n_draws=n_draws,
            codcorrect_version=self.codcorrect_version,
        )
        self.global_ratios = df

    def new_population(
        self, location_set_id: int, agg_loc_sets: Optional[List[int]] = None
    ) -> None:
        """Cache a population hdf5 file."""
        dim = self.nonfatal_dimensions.get_simulation_dimensions(self.measure_id)
        df = get_population(
            age_group_id=(dim.index_dim.get_level("age_group_id") + [age.BIRTH]),
            location_id=dbtrees.loctree(
                location_set_id=location_set_id, release_id=self.release_id
            ).node_ids,
            sex_id=dim.index_dim.get_level("sex_id"),
            year_id=dim.index_dim.get_level("year_id"),
            release_id=self.release_id,
            use_rotation=False,
        )
        self.population_run_id = int(df["run_id"].unique()[0])
        index_cols = ["location_id", "year_id", "age_group_id", "sex_id"]
        data_cols = ["population"]

        io_mock = {}
        source = DrawSource({"draw_dict": io_mock, "name": "tmp"}, mem_read_func)
        sink = DrawSink({"draw_dict": io_mock, "name": "tmp"}, mem_write_func)
        sink.push(df[index_cols + data_cols])

        # location
        if agg_loc_sets:
            for set_id in agg_loc_sets:
                loc_trees = dbtrees.loctree(
                    location_set_id=set_id, release_id=self.release_id, return_many=True
                )
                operator = Sum(
                    index_cols=[col for col in index_cols if col != "location_id"],
                    value_cols=data_cols,
                )
                aggregator = ag.aggregators.AggSynchronous(
                    draw_source=source,
                    draw_sink=sink,
                    index_cols=[col for col in index_cols if col != "location_id"],
                    aggregate_col="location_id",
                    operator=operator,
                )
                for loc_tree in loc_trees:
                    aggregator.run(loc_tree)

            # get rid of duplicate location entries (from loc sets 35 and 138 together)
            sink.params["draw_dict"]["tmp"] = sink.params["draw_dict"]["tmp"].drop_duplicates(
                subset=["location_id", "year_id", "age_group_id", "sex_id"], keep="first"
            )

        # age
        for age_group_id in ComoSummaries._gbd_compare_age_group_list:
            age_tree = dbtrees.agetree(age_group_id=age_group_id, release_id=self.release_id)
            operator = Sum(
                index_cols=[col for col in index_cols if col != "age_group_id"],
                value_cols=data_cols,
            )
            aggregator = ag.aggregators.AggSynchronous(
                draw_source=source,
                draw_sink=sink,
                index_cols=[col for col in index_cols if col != "age_group_id"],
                aggregate_col="age_group_id",
                operator=operator,
            )
            aggregator.run(age_tree)

        # get rid of duplicate age entries (has never actually happended)
        sink.params["draw_dict"]["tmp"] = sink.params["draw_dict"]["tmp"].drop_duplicates(
            subset=["location_id", "year_id", "age_group_id", "sex_id"], keep="first"
        )

        # sex
        sex_tree = dbtrees.sextree()
        operator = Sum(
            index_cols=[col for col in index_cols if col != "sex_id"], value_cols=data_cols
        )
        aggregator = ag.aggregators.AggSynchronous(
            draw_source=source,
            draw_sink=sink,
            index_cols=[col for col in index_cols if col != "sex_id"],
            aggregate_col="sex_id",
            operator=operator,
        )
        aggregator.run(sex_tree)
        df = source.content()

        df.to_hdf(
            "FILEPATH",
            "draws",
            mode="w",
            format="table",
            data_columns=["location_id", "year_id", "age_group_id", "sex_id"],
        )

    def new_urolith_dws(self, location_set_id: int) -> None:
        """Create a cached urolithiasis disability weight hdf5 file."""
        urolithiasis.to_como(
            como_dir=self.como_dir,
            location_set_id=location_set_id,
            release_id=self.release_id,
        )

    def unmark_current_best(self, scoped_session: ContextManager) -> None:
        """Unset the currently marked best output version and timestamp the set
        operation.
        """
        scoped_session.execute(
            """
            UPDATE epi.output_version
            SET
                best_end=NOW(),
                is_best=:not_best
            WHERE
                is_best=:best AND
                release_id=:release_id AND
                output_version_id <> :version_id
            """,
            params={
                "not_best": 0,
                "best": 1,
                "release_id": self.release_id,
                "version_id": self.como_version_id,
            },
        )
        scoped_session.commit()

    def mark_epi_best(self) -> None:
        """Internal PHP diagnostics use this to pull COMO verisons."""
        with db_tools_core.session_scope(conn_def=conn_defs.EPI) as scoped_session:
            self.unmark_current_best(scoped_session=scoped_session)
            scoped_session.execute(
                """
                UPDATE epi.output_version
                SET
                    status=:complete,
                    best_start=NOW(),
                    best_end=NULL,
                    is_best=:best,
                    best_description=:description,
                    best_user=:user
                WHERE output_version_id=:version
                """,
                params={
                    "complete": 1,
                    "best": 1,
                    "description": "Central COMO run",
                    "user": getpass.getuser(),
                    "version": self.como_version_id,
                },
            )
            scoped_session.commit()

    def generate_cause_scatters(self) -> None:
        """Output cause scatter plots."""
        try:
            script_dir = (
                "FILEPATH"
            )
            plot_dir = f"{self.como_dir}/scatters"

            # get last release cv
            with db_tools_core.session_scope(conn_def=conn_defs.EPI) as scoped_session:
                query = """
                    SELECT
                        output_version_id
                    FROM
                        epi.output_version
                    WHERE
                        release_id = :prior_release_id
                        AND is_best = 1
                        ORDER BY release_id DESC;
                    """

                parameters = {"prior_release_id": get_previous_release(self.release_id)}
                prior_cv_id = db_tools_core.query_2_df(
                    query=text(query), session=scoped_session, parameters=parameters
                ).output_version_id.tolist()[0]

            prior_cv = ComoVersion(os.path.join(self.como_base_dir, str(prior_cv_id)))
            prior_cv.load_cache()

            # set last release inputs
            x_name = f"COMO v{prior_cv.como_version_id}"
            x_pvid = prior_cv.gbd_process_version_id
            x_release_id = prior_cv.release_id

            # set current release inputs
            y_name = f"COMO v{self.como_version_id}"
            y_pvid = self.gbd_process_version_id
            y_release_id = self.release_id

            os.system("umask 002")  # noqa: S605, S607
            for measure_id in [measures.YLD, measures.PREVALENCE]:
                command = (
                    f"python3 {script_dir} --x-name '{x_name}' --y-name '{y_name}' "
                    f"--x-pvid {x_pvid} --y-pvid {y_pvid} "
                    f"--x-release-id {x_release_id} "
                    f"--y-release-id {y_release_id} "
                    f" --year-ids 2010 "
                    f"--measure-id {measure_id} --metric-id 3 --plotdir {plot_dir}"
                )
                os.system(command)  # noqa: S605
            logger.info(f"finished generating cause scatter plots: {plot_dir}")
        except Exception as exc:
            logger.error(f"generating cause scatter plot failed with exception: {exc}")
            pass

    def create_gbd_process_version(
        self,
        code_version: str,
        year_id: Union[int, List[int]],
        n_draws: int,
        gbd_process_version_note: str,
    ) -> GBDProcessVersion:
        """Create a new process version to associate with the current COMO run."""
        dim = self.nonfatal_dimensions.get_simulation_dimensions(self.measure_id)
        age_group_ids = sorted(
            dim.index_dim.get_level("age_group_id")
            + ComoSummaries._gbd_compare_age_group_list
        )
        compare_contexts = [
            constants.COMPONENT_TO_COMPARE_CONTEXT[component] for component in self.components
        ]
        gbd_metadata = {
            gbd_metadata_type.COMO: self.como_version_id,
            gbd_metadata_type.CODCORRECT: self.codcorrect_version,
            gbd_metadata_type.EPIC: self.epic_version,
            gbd_metadata_type.POPULATION: self.population_run_id,
            gbd_metadata_type.AGE_GROUP_IDS: {
                measures.YLD: age_group_ids,
                measures.PREVALENCE: age_group_ids,
                measures.INCIDENCE: (age_group_ids + [age.BIRTH]),
            },
            gbd_metadata_type.YEAR_IDS: year_id,
            gbd_metadata_type.N_DRAWS: n_draws,
            gbd_metadata_type.N_SIMULANTS: self.n_simulants,
            # NOTE: This will not include aggregation or special location set versions
            gbd_metadata_type.LOCATION_SET_VERSION_ID: self.location_set_version_id,
            gbd_metadata_type.MEASURE_IDS: self.measure_id,
            gbd_metadata_type.COMPARE_CONTEXT_ID: compare_contexts,
            gbd_metadata_type.INTERNAL_UPLOAD: int(self.internal_upload),
            gbd_metadata_type.PUBLIC_UPLOAD: int(self.public_upload),
        }
        # Only add change years if they exist and have content. Otherwise we break some table
        # expectations on the public host.
        if self.change_years:
            start_years, end_years = zip(*self.change_years)
            gbd_metadata[gbd_metadata_type.YEAR_START_IDS] = list(start_years)
            gbd_metadata[gbd_metadata_type.YEAR_END_IDS] = list(end_years)
        env = "ENV"
        if self.public_upload_test:
            env = "ENV"
        pv = GBDProcessVersion.add_new_version(
            gbd_process_id=gbd_process.EPI,
            gbd_process_version_note=gbd_process_version_note,
            code_version=code_version,
            release_id=self.release_id,
            metadata=gbd_metadata,
            env=env,
            validate_env="ENV",
        )
        self.gbd_process_version_id = pv.gbd_process_version_id
        logger.info(f"created GBDProccessVersion: v{pv.gbd_process_version_id}")
        return pv

    def mark_test(self) -> None:
        """Marks the process version associated with a COMO run as test."""
        pv = GBDProcessVersion(self.gbd_process_version_id)
        pv.update_status(status=gbd_process_version_status.INTERNAL_TEST)

    def mark_best(self) -> None:
        """Marks the process version associated with a COMO run as best."""
        pv = GBDProcessVersion(self.gbd_process_version_id)
        pv.mark_best()

    def create_compare_version(self) -> CompareVersion:
        """Create a compare version associated with the current
        CodCorrect, EpiC, and COMO runs.
        """
        env = "ENV"
        if self.public_upload_test:
            env = "ENV"
        cv = CompareVersion.add_new_version(release_id=self.release_id, env=env)

        cv.add_process_version(self.codcorrect_process_version)

        df = bleeding_edge(cv.compare_version_id)
        cv.add_process_version(df.gbd_process_version_id.tolist())
        cv.update_status(gbd_process_version_status.ACTIVE)
        logger.info(f"created CompareVersion: {cv.compare_version_id}")
        return cv

    def validate_inputs(self) -> None:
        """Validates the comoversion inputs."""
        df = pd.concat(
            [
                self.sequela_list,
                self.birth_prev,
                self.injury_sequela,
                self.sexual_violence_sequela,
            ]
        )
        validate_best_models(df, self.mvid_list, self.como_dir)
