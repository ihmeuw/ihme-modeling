import inspect
import os

import pandas as pd
from crosscutting_functions.pipeline_constants import poland as constants
from crosscutting_functions.clinical_constants.pipeline.schema import pol_pipeline
from crosscutting_functions import maternal as maternal_funcs
from crosscutting_functions.deduplication import estimates
from crosscutting_functions.pipeline import get_map_version
from crosscutting_functions.validations import validate_bundle_estimate
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser
from loguru import logger

from pol_nhf.pipeline.lib import (
    aggregate,
    dedup_claims,
    denominators,
    io_manager,
    mapping,
    square_data,
)


class PolandClaimsEstimates:
    def __init__(
        self,
        run_id: int,
        bundle_id: int,
        estimate_id: int,
        deliverable: str,
        write_final_data: bool,
    ):
        """
        Class that converts ICD mart data to final estimates dependent on deliverable name.
        Final data contains all years for both national and subnational locations.

        Arugments:
            run_id: CI specific run_id
            bundle_id: Bundle id present in clinical table DATABASE
            estimate_id: CI specific estimate id
            map_version: Version of the CI map
            deliverable: Name of the desired output.
                        Currently accepts "gbd" or "correction_factor"
        """
        self.run_id = run_id
        self.bundle_id = bundle_id
        self.estimate_id = estimate_id
        self.convert_estimate_id_to_estimate()
        self.map_version = get_map_version(self.run_id)
        self.deliverable = deliverable
        self.write_final_data = write_final_data

    def __repr__(self):
        return f"bundle_{self.bundle_id}_estimate_{self.estimate_id}_{self.deliverable}"

    @property
    def base_dir(self):
        return "FILEPATH"

    def log_mapping_failures(self):
        """
        Log bundles that returned None types after mapping

        We've noticed that some bundles fail after mapping steps. This is most likely
        due to bundles that only contain 5-6 digit codes
        """
        path = "FILEPATH"
        if self.df.empty:
            self.df = pd.DataFrame()  # drop metadata
            self.df.to_parquet("FILEPATH")
            call_stack = inspect.stack()
            logger.info(
                "Mapping process resulted in an empty datafame."
                f"Review data during {call_stack[1][3]}"
            )
            os._exit(0)

    def create_logger(self):
        log_path = "FILEPATH"
        logger.add(sink=log_path)

    def get_data(self):
        """
        Pull data from the ICD mart
        """
        logger.info(f"Reading ICD mart schema year(s): {constants.YEARS}")
        self.df = io_manager.read_bundle_data(
            bundle_id=self.bundle_id,
            run_id=self.run_id,
            estimate=self.estimate,
            year_ids=constants.YEARS,
        )

    def convert_estimate_id_to_estimate(self):
        try:
            self.estimate = getattr(estimates, f"ESTIMATE_{self.estimate_id}")
        except AttributeError:
            print(
                "Non claims estimate id. Verify estimate id and estimate dataclass "
                "at deduplication/estimates.py"
            )

    def apply_age_sex_restrictions(self):
        """
        Apply bundle level age sex restrictions
        """
        self.df = mapping.apply_bundle_age_sex_restrictions(self.df, self.map_version)
        self.log_mapping_failures()

    def apply_deduplication(self):
        """
        Remove duplicate claims based on bundle level measure and duration
        """
        self.df = dedup_claims.run_deduplication(
            df=self.df, map_version=self.map_version, estimate_id=self.estimate_id
        )
        self.log_mapping_failures()
        # now that that the data is deduped we can drop unecessary cols
        cols = constants.BASE_GROUPBY_COLS + ["age", "primary_dx_only"]
        self.df = self.df[cols]

    def create_cf_estimates(self):
        """
        Adds val column and aggregate these vals, (e.g. counts) for correction factor inuputs.
        """
        self.df = aggregate.create_estimate_id_values(self.df, self.deliverable)
        self.df["estimate_type"] = self.estimate.name

    def write_output(self):
        """
        Save estimates in run directory
        """
        if not self.write_final_data:
            return
        outpath = filepath_parser(
            ini="pipeline.pol_nhf",
            section="pipeline",
            section_key=f"{self.deliverable}_output",
        )
        path = "FILEPATH"
        self.df.to_parquet("FILEPATH")

    def prep_for_denom(self):
        """
        Add age_start, age_end, age_group_id columns. Retains original age column
        """
        self.df = denominators.prep_for_denoms(self.df)

    def apply_denominator(self):
        """
        Add sample size column

        Appends cached population
        """
        self.df = denominators.create_denominator(self.df, self.run_id)

    def asfr_adjust(self):
        """Apply ASFR adjustment to maternal bundles based on deliverable."""
        maternal_bundles = maternal_funcs.get_maternal_bundles(
            map_version=self.map_version, run_id=self.run_id
        )
        proc_bund = f"Deliverable={self.deliverable}, Bundle={self.bundle_id}"
        if constants.DELIVERABLE[self.deliverable]["apply_maternal_adjustment"]:
            if self.bundle_id in maternal_bundles:
                self.df["year_start"] = self.df["year_id"]
                self.df = maternal_funcs.apply_asfr(
                    df=self.df, maternal_bundles=maternal_bundles, run_id=self.run_id
                )
                logger.info(f"Applying ASFR: {proc_bund}")
                self.df = self.df.drop("year_start", axis=1)
        else:
            logger.info(f"ASFR was NOT applied: {proc_bund}")

    def create_gbd_estimate_values(self):
        """
        Add val column

        Creates counts and aggregates data by demographics and estimate id
        requirments
        """
        pre_shape = self.df.shape[0]
        temp = aggregate.create_estimate_id_values(self.df, self.deliverable)
        if pre_shape != temp["val"].sum():
            raise RuntimeError("Lost rows when aggregating subnational data")
        self.df = aggregate.create_national_values(temp, self.deliverable)

    def create_rates(self):
        """
        Convert estimates from count to rate space
        """
        self.df["mean"] = self.df.val / self.df.sample_size
        self.df.drop("val", axis=1, inplace=True)

    def prep_final_data(self):
        """
        Create final shape for GBD data output
        """
        self.df["nid"] = self.df["year_id"].map(constants.NID_DICT)
        # drop human readable age values
        self.df.drop(["age_start", "age_end"], axis=1, inplace=True)

    def apply_squared_data(self):
        """
        Square data for age, sex, and locations
        """
        self.df = square_data.create_square_data(self.df, self.map_version)
        if self.df.isnull().sum().sum() > 0:
            raise RuntimeError("Squared rows are missing data")

    def main(self):
        if self.write_final_data:
            self.create_logger()
        logger.info(f"Class attribues: {self.__dict__}")
        self.get_data()
        if len(self.df) == 0:
            logger.info(
                f"No records present for bundle={self.bundle_id} estimate={self.estimate_id}"
            )
            record_path = "FILEPATH"
            validate_bundle_estimate.write_token(
                pipeline="poland",
                tokendir=record_path,
                run_id=self.run_id,
                bundle_id=self.bundle_id,
                estimate_id=self.estimate_id,
                err="No data available.",
            )
        else:
            self.apply_age_sex_restrictions()
            logger.info("Data is mapped to bundle and age sex restricions are applied.")
            self.apply_deduplication()
            pol_pipeline.DedupedSchema(self.df)
            if self.deliverable == "correction_factors":
                self.create_cf_estimates()
                pol_pipeline.CorrectionFactorFinalSchema(self.df)
                self.write_output()
                logger.info("Correction factor processing complete.")
                return
            self.prep_for_denom()
            self.create_gbd_estimate_values()
            logger.info("Aggregation for non correction factor processing is complete.")
            self.apply_squared_data()
            self.apply_denominator()
            self.create_rates()
            logger.info("Data is transformed to rate space.")
            self.prep_final_data()
            logger.info("Created final shape of outputs.")
            self.asfr_adjust()
            pol_pipeline.GBDFinalSchema(self.df)
            logger.info("Data passed final validation.")
            self.write_output()
            logger.info("Pipeline finished.")
