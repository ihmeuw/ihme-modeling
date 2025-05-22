"""
Processing Class for the CMS pipeline
"""
import uuid
from getpass import getuser
from pathlib import Path, PosixPath
from typing import Any, Optional, Union

import git
import pandas as pd
from crosscutting_functions.clinical_constants.pipeline import cms as constants
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser
from crosscutting_functions import demographic
from crosscutting_functions.validations import validate_bundle_estimate
from clinical_info.Corrections.pipeline_integration import reshape_for_cfs
from crosscutting_functions import (
    maternal as maternal_functions,
    pipeline as pipeline_functions,
)
from crosscutting_functions.noise-reduction.clinical_nr import FloorError, RakingError
from crosscutting_functions.mapping import clinical_mapping_db
from db_connector.database import Database
from db_tools.ezfuncs import query
from patsy import PatsyError
from pydantic import validate_arguments

from cms.src.pipeline.lib import (
    apply_elig_filters,
    assign_age_and_agg,
    col_reqs,
    create_rates,
    file_storer,
    manage_icd_mart_data,
    merge_denoms,
    noise_reduction,
    pull_denoms,
)
from cms.utils import logger, process_types, readers
from cms.validations.pipeline_plots.plotter import PlotFactory


class CreateCmsEstimates:
    """Interface class for creating bundle level estimates for CMS data"""

    @validate_arguments()
    def __init__(
        self,
        cms_system: str,
        run_id: int,
        bundle_id: int,
        estimate_id: int,
        deliverable_name: str = "gbd",
        noise_reduce: bool = False,
        write_intermediate_data: bool = False,
        write_final_data: bool = False,
        force_age_group_set_id: Optional[int] = None,
    ):
        """
        cms_system (str):
            identifies whether to pull Medicare or Medicaid data using cms
            system abbreviations 'mdcr' and 'max' respectively.
        bundle_id (int):
            Bundle ID from the map version associated to 'run_id'.
        estimate_id (int):
            Valid clinical estimate_id
        force_age_group_set_id (int or None):
            Valid clinical age group set or None. The default (None) behavior uses
            cms_system to define the age group set
        run_id (int or None):
            Valid clinical run_id that has been created in "FILEPATH" to write
            final data to. This must align with a run_id in DATABASE.
        deliverable_name (str):
            Identifies how the data will be used, ie 'gbd', 'ushd', etc. This determines
            which cols will be used in the groupby statement to aggregate rows of data
            and which denominator values will be pulled in. Col names are in
            clinical_constants. The deliverable dataclass this name points to may
            also remove some locations that are found in the denominator table.
        noise_reduce (bool):
            should the clinical_info noise reduction module be applied?
        write_intermediate_data (bool):
            should the intermediate data be stored to LU_CMS? If so multiple intermediate data
            files will be written along with automated plots from the CMS plotting factory
        write_final_data (bool):
            should the final data be stored to LU_CMS? This requires a run_id. These files are
            then compiled and moved to an appropriate folder depending on deliverable_name
        """

        # Assign input args to self attributes
        self.cms_system = cms_system
        self.bundle_id = bundle_id
        self.estimate_id = estimate_id
        self.force_age_group_set_id = force_age_group_set_id
        self.run_id = run_id
        self.deliverable_name = deliverable_name
        self.noise_reduce = noise_reduce
        self.write_intermediate_data = write_intermediate_data
        self.write_final_data = write_final_data
        self.release_id = pipeline_functions.get_release_id(run_id=run_id)
        self._check_args()

        # Assign other important attributes that are too detailed to be parent level args
        self.get_type = "spark_parquet"  # where to pull the numer bundle mart data from
        self.denom_source = "intermediate_parquet"  # where to pull the denom data from
        self.name = f"{self.cms_system}_bundle_{self.bundle_id}_estimate_{self.estimate_id}_for_{self.deliverable_name}"
        self.uuid = uuid.uuid4()
        self.create_inst_dir()
        self.ff = "parquet"  # file storage format when writing data
        self.log = logger.setup("FILEPATH")

        self.create_log_header()
        self.map_version = self.get_map_version()

    def git_info(self, repo_path: Union[str, PosixPath]) -> str:
        """For a given repo return the current branch and last commit hash"""
        repo = git.Repo(f"{repo_path}")
        branch = repo.active_branch.name
        commit = repo.head.commit.hexsha
        state = f"""
        Current branch and commit for {repo_path}
        branch: {branch}
        commit: {commit}"""
        return state

    def create_log_header(self):
        code_base = "FILEPATH"
        header = f"""
        user: {getuser()}
        {self.git_info(repo_path="FILEPATH")}
        {self.git_info(repo_path="FILEPATH")}

        create_cms_estimates args are-
        cms_system: {self.cms_system}
        bundle_id: {self.bundle_id}
        estimate_id: {self.estimate_id}
        force_age_group_set_id: {self.force_age_group_set_id}
        run_id: {self.run_id}
        deliverable: {self.deliverable_name}
        noise_reduce: {self.noise_reduce}
        write_intermediate_data: {self.write_intermediate_data}
        write_final_data: {self.write_final_data}
        release_id: {self.release_id}
        get_type: {self.get_type}
        denom_source: {self.denom_source}
        Files will be saved as {self.ff}

        Begin Logging...
        """
        self.log.info(header)

    def _check_args(self):
        if self.write_final_data:
            if self.run_id is None:
                msg = "Error: Writing final data requires a run_id for versioning"
                raise RuntimeError(msg)

    def create_inst_dir(self):
        """Creates the directory named with bundle/cms system and uuid to store relevant info"""
        base = filepath_parser(
            ini="pipeline.cms", section="base_paths", section_key="estimates_versions"
        )

        # use this path to store all logs/plots/intermediate data
        self.inst_write_path = "FILEPATH"
        # create uuid dir
        Path(self.inst_write_path).mkdir(parents=True, exist_ok=True)

        if self.run_id:
            uuid_path = "FILEPATH"
            Path(uuid_path).touch()

    def assign_max_denom_type(self):
        """Update any new max denom types here"""
        if self.is_maternal_bundle:
            self.max_denom_type = "full_with_pregnancy"
        else:
            # default val, assigned to mdcr but shouldn't be used
            self.max_denom_type = "full_count"

    def get_processing_cols(self):
        """need a list of columns to use when querying the database"""

        self.cols = readers.icd_mart_proc_cols(
            cms_system=self.cms_system, deliverable=self.deliverable
        )

    def set_age_group_set(self):
        """Use the default clinical age group set or attempt to force use of another"""
        if self.force_age_group_set_id:
            if not isinstance(self.force_age_group_set_id, int):
                raise ValueError("clinical_age_group_set_id needs to be an integer.")
            self.clinical_age_group_set_id = self.force_age_group_set_id
        else:
            msg = "We are currently HARDCODING age group set 2 (GBD2020+) for both medicare and medicaid"
            self.log.info(msg)
            self.clinical_age_group_set_id = 2

    def dtype_converter(self):

        """Convert self.df column data types"""

        int_cols = ["file_source_id", "location_id_gbd", "rti_race_cd"]
        str_cols = ["location_id_cnty"]
        bool_cols = []
        date_cols = ["service_start", "service_end", "dob", "dod"]

        for ic in int_cols:
            self.df[ic] = pd.to_numeric(self.df[ic], downcast="integer")
        for sc in str_cols:
            self.df[sc] = self.df[sc].astype(str)
        for bc in bool_cols:
            self.df.loc[self.df[bc] == "True", bc] = True
            self.df.loc[self.df[bc] == "False", bc] = False
        for dt in date_cols:
            self.df[dt] = pd.to_datetime(self.df[dt], format="%Y-%m-%d")

    def get_bundle_name(self):
        """Pull bundle_name from bundle.bundle"""
        # Connect Clinial DB to query DATABASE
        db = Database()
        db.load_odbc("SERVER")

        # Query for the bundle
        name = db.query(
            "QUERY"
        )

        # Set attribute
        self.bundle_name = name.bundle_name[0]

    def get_data(self, drop_unused_cols: bool = True):
        """Get the bundle-mart data using the 'get_type' attribute.

        Args:
            drop_unused_cols (bool): To remove extra columns. Defaults to True.
        """

        self.get_processing_cols()

        if self.get_type == "spark_parquet":
            self.df = readers.build_bundle_table(
                cms_system=self.cms_system,
                run_id=self.run_id,
                estimate_id=self.estimate_id,
                bundle_id=self.bundle_id,
            )
        else:
            raise NotImplementedError("Data read method not implemented")

        self.dtype_converter()

        self.df.rename(columns=self.deliverable.rename_loc_dict, inplace=True)
        if "cms_facility_id" in self.df.columns:
            self.df["facility_id"] = self.df["cms_facility_id"].fillna(99999)

        if drop_unused_cols:
            drops = [
                "cms_facility_id",
                "clm_srvc_fac_zip_cd",
                "service_end",
                "eligibility_reason",
                "managed_care",
                "dod",
                "part_c",
            ]
            drops = [d for d in drops if d in self.df.columns]
            self.df.drop(drops, axis=1, inplace=True)

        # mark as identifiable, eg don't move this data off LU_CMS
        self.df["is_identifiable"] = 1

    def get_map_version(self) -> int:
        """Identify the map version used in the Claims run."""

        qu = "QUERY"

        map_version = query(
            qu,
            conn_def="SERVER",
        ).iloc[0]
        map_version = int(map_version)
        return map_version

    def inter_writer(self, write_path: str):
        file_storer.write_wrapper(self.df, write_path, self.ff)

    def make_plot_df(self, plot_step) -> pd.DataFrame:
        """To ensure the pipeline data won't be modified we will make a copy of the df, sometimes a subset
        of the df depending on plot step/data size"""
        plot_cols = constants.plotting_cols[plot_step]

        if plot_cols:
            df = self.df[plot_cols].copy()
        else:
            df = self.df.copy()
        return df

    def make_plots(self, plot_step):
        """Wrapped up plotting factory"""
        factory = PlotFactory.get_step_plotter(plot_step)
        factory.create_plots(
            df=self.make_plot_df(plot_step),
            out_dir=self.inst_write_path,
            deliverable=self.deliverable,
            cms_system=self.cms_system,
            bundle_id=self.bundle_id,
            estimate_id=self.estimate_id,
            bundle_name=self.bundle_name,
        )

    def create_deliverable_obj(self):
        """From the input string deliverable_name create a dataclass for use in processing"""
        if self.deliverable_name == "gbd":
            self.deliverable = process_types.GbdDeliverable
        elif self.deliverable_name == "ushd":
            self.deliverable = process_types.UshdDeliverable
        elif self.deliverable_name == "correction_factors":
            self.deliverable = process_types.CfDeliverable
        else:
            raise NotImplementedError(
                f"Deliverable type {self.deliverable_name} is not supported"
            )

    def token_writer(self, err: Any) -> None:
        validate_bundle_estimate.write_token(
            pipeline=f"cms_{self.cms_system}",
            tokendir=self.record_path,
            run_id=self.run_id,
            bundle_id=self.bundle_id,
            estimate_id=self.estimate_id,
            err=err,
        )

    def main(self):
        """
        processing steps
        1) Read numerator ICD-mart data from input source defined in self.get_type
        2) Run the bundle mart manager to filter and de-dup
        3) Assign age in years, age bin and aggregate numerator data
        4) Read and attach denominators. denom input source defined in self.denom_source
            4.1) Apply the maternal adjustment for maternal bundles
        5) Create estimate rates from encounter counts / sample size
        6) Optionally noise reduce
        """

        self.create_deliverable_obj()
        self.get_bundle_name()
        self.get_data()  # pull in bundle level data from ICD-mart
        initmsg = f"There are {len(self.df):,} rows of data in this bundle-table."
        self.log.info(initmsg)
        if self.run_id:
            self.run_dir = "FILEPATH"
            self.record_path = "FILEPATH"

        if self.df.shape[0] == 0:
            self.token_writer(err="No data present for this bundle.")
            return

        failures = []
        if self.write_intermediate_data:
            pre = self.df.shape
            self.make_plots(0)
            post = self.df.shape
            if pre != post:
                failures.append(f"The step 0 plots changed df shape {pre} to {post}")

        self.set_age_group_set()  # determine which clinical age set to use
        # get the code systems with this bundle present in the map
        self.code_system_ids = clinical_mapping_db.get_code_sys_by_bundle(
            bundle_id=self.bundle_id, map_version=self.get_map_version()
        )

        # Tag the maternal status of the bundle being processed
        mat_bundles = maternal_functions.get_maternal_bundles(
            map_version=self.map_version, run_id=self.run_id
        )
        if self.bundle_id in mat_bundles:
            self.log.info("This is a maternal bundle. The maternal adjustment will be applied")
            self.is_maternal_bundle = True
        else:
            self.is_maternal_bundle = False

        self.assign_max_denom_type()  # include any restricted beneficiaries here

        # remove numerator data that falls outside of eligibility reqs
        self.df = apply_elig_filters.filter_elig_vals(
            df=self.df,
            cms_system=self.cms_system,
            estimate_id=self.estimate_id,
            max_denom_type=self.max_denom_type,
        )

        mbmd_obj = manage_icd_mart_data.EstimateIdController(
            df=self.df,
            estimate_id=self.estimate_id,
            cms_system=self.cms_system,
            logger=self.log,
            map_version=self.map_version,
        )
        self.df = mbmd_obj.df
        del mbmd_obj

        if self.df.shape[0] == 0:
            self.token_writer(err="All rows have been filtered from self.df")
            return

        self.log.info(f"There are {len(self.df):,} rows of data after dedup and filtering")
        if self.write_intermediate_data:
            pre = self.df.shape
            self.inter_writer("FILEPATH")
            self.make_plots(1)
            post = self.df.shape
            if pre != post:
                failures.append(f"The step 3 plots changed df shape {pre} to {post}")

        # assign age at service start and aggregate
        self.df = assign_age_and_agg.run(
            df=self.df,
            deliverable=self.deliverable,
            cms_system=self.cms_system,
            clinical_age_group_set_id=self.clinical_age_group_set_id,
            logger=self.log,
            map_version=self.map_version,
        )
        self.log.info(
            f"There are {len(self.df):,} rows of data after adding age and aggregating"
        )
        if self.write_intermediate_data:
            pre = self.df.shape
            self.inter_writer("FILEPATH")
            self.make_plots(2)
            post = self.df.shape
            if pre != post:
                failures.append(f"The step 2 plots changed df shape {pre} to {post}")

        # Output a CF data product before noise reduction is applied, similar to the current
        # Marketscan CF data
        if self.deliverable.name == "correction_factors":
            # Remove locations identified in deliverable class
            self.df = col_reqs.remove_locs(self.df, self.deliverable)
            # Copy the data and adjust some column names/values to match CF inputs
            cf_df = reshape_for_cfs.enforce_cf_schema(self.df.copy())
            if self.write_final_data:
                final_path = "FILEPATH"
                file_storer.write_wrapper(
                    df=col_reqs.clean_cols(cf_df, self.deliverable),
                    write_path=final_path,
                    file_format=self.ff,
                )
            self.log.info(
                "Correction Factor deliverable has finished processing. "
                "Noise Reduction CF data not currently supported."
            )
            return

        # pull the denominators from the `denom_source` (db or csvs on disk)
        self.denoms = pull_denoms.run(
            bundle_id=self.bundle_id,
            estimate_id=self.estimate_id,
            cms_system=self.cms_system,
            deliverable=self.deliverable,
            max_denom_type=self.max_denom_type,
            clinical_age_group_set_id=self.clinical_age_group_set_id,
            pull_source=self.denom_source,
            logger=self.log,
            map_version=self.map_version,
        )

        # Some locations are in our denominator table may not be valid for some deliverables.
        # Removes these pre-defined locations by deliverable datalcass.
        pre = len(self.df)
        post = len(col_reqs.remove_locs(df=self.df, deliverable=self.deliverable))
        self.log.info(
            f"{pre - post} rows will be removed due to invalid locations by deliverable"
        )

        self.df, self.denoms = merge_denoms.merge_denoms(
            df=self.df,
            denoms=self.denoms,
            deliverable=self.deliverable,
            code_system_ids=self.code_system_ids,
            logger=self.log,
            cms_system=self.cms_system,
        )
        self.log.info(
            f"There are {len(self.df):,} rows of data after merging on the denominators"
        )
        if self.write_intermediate_data:
            pre = self.df.shape
            self.inter_writer("FILEPATH")
            self.make_plots(3)
            post = self.df.shape
            if pre != post:
                failures.append(f"The step 3 plots changed df shape {pre} to {post}")

        self.df = demographic.age_binning(
            self.df,
            clinical_age_group_set_id=self.clinical_age_group_set_id,
            drop_age=True,
            terminal_age_in_data=False,
            under1_age_detail=False,
            break_if_not_contig=True,
        )

        # aggregate again into the age group ids
        agg_cols = ["sample_size", "val"]
        groupcols = self.df.drop(agg_cols, axis=1, inplace=False).columns.tolist()
        self.log.info(f"Doing another groupby along the columns {groupcols}")
        self.log.info(f"Null rates per column\n\n{self.df.isnull().sum() / len(self.df)}")
        self.df = (
            self.df.copy()
            .groupby(groupcols)
            .agg(dict(zip(agg_cols, ["sum"] * len(agg_cols))))
            .reset_index()
        )

        self.df = demographic.group_id_start_end_switcher(
            self.df, self.clinical_age_group_set_id, remove_cols=False
        )

        if self.is_maternal_bundle:
            if self.deliverable.apply_maternal_adjustment:
                self.log.info("Apply the maternal adjustment to this data")
                self.df = merge_denoms.apply_maternal_adjustment(
                    df=self.df, run_id=self.run_id
                )
            else:
                self.log.info(
                    f"This is data for {self.deliverable.name}, we will not apply "
                    "maternal adjustments to it at this point"
                )

        self.log.info(
            f"There are {len(self.df):,} rows of data after age-binning and re-aggregating"
        )
        if self.write_intermediate_data:
            pre = self.df.shape
            self.inter_writer("FILEPATH")
            self.make_plots(4)
            post = self.df.shape
            if pre != post:
                failures.append(f"The step 4 plots changed df shape {pre} to {post}")

        if failures:
            raise RuntimeError("\n".join(failures))
        # create rates
        self.df = create_rates.create_rates(self.df)

        if self.noise_reduce:
            try:
                self.log.info("Running noise reduction")
                pre_nr_cols = self.df.columns.tolist()
                self.df = noise_reduction.nr_cms(
                    df=self.df,
                    run_id=self.run_id,
                    bundle_id=self.bundle_id,
                    estimate_id=self.estimate_id,
                    group_location_id=102,
                )  # CMS will always be US data

                # mark as no longer identifiable
                self.df["is_identifiable"] = 0

                # write nr data to the run for review
                self.make_plots(5)
                if self.run_id:
                    nr_path = "FILEPATH"
                    file_storer.write_wrapper(
                        df=self.df, write_path=nr_path, file_format=self.ff
                    )
                # retain only pre_nr cols
                self.df = self.df[pre_nr_cols]

                # set final column order, remove unused cols and sort rows
                self.df = col_reqs.clean_cols(self.df, deliverable=self.deliverable)

                self.log.info(f"There are {len(self.df):,} rows of Final data")
                if self.write_final_data:
                    final_path = (
                        "FILEPATH"
                    )
                    file_storer.write_wrapper(
                        df=self.df, write_path=final_path, file_format=self.ff
                    )

                self.log.info(
                    f"""Finished, review class attrs like self.df and self.denoms if running interactively or intermediate and final data if they were written
                    \nRun_id: {self.run_id}
                    \nIntermediate data written?: {self.write_intermediate_data}
                    \nFinal data written?: {self.write_final_data}"""
                )

            except (PatsyError, FloorError, RakingError) as e:
                self.log.info(
                    f"Noise reduction failed for bundle={self.bundle_id} estimate={self.estimate_id} with error {e}"
                )
                self.token_writer(err=e)
