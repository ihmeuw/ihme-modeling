"""Processing class to produce bundle-level estimates meeting estimate_id requirements from
the Marketscan ICD-mart data for a bundle_id, estimate_id, and deliverable_name"""

import json
import os
import pickle
from typing import List, Optional

import pandas as pd
import pandera as pa
from crosscutting_functions.clinical_constants.pipeline import marketscan as constants
from crosscutting_functions.clinical_constants.pipeline.schema import marketscan_pipeline
from crosscutting_functions.clinical_metadata_utils.pipeline_wrappers import (
    ClaimsWrappers,
)
from crosscutting_functions import demographic
from crosscutting_functions import maternal as maternal_funcs
from crosscutting_functions import pipeline
from crosscutting_functions.deduplication import estimates
from crosscutting_functions.mapping import clinical_mapping_db
from gbd.constants import measures
from loguru import logger

from marketscan.pipeline.lib import (
    aggregators,
    apply_deduplication,
    apply_denominators,
    clinical_noise_reduction,
    config,
    filters,
    io,
    mapping,
    process_types,
)


class CreateMarketscanEstimates:
    """Marketscan interface"""

    def __init__(
        self,
        bundle_id: int,
        estimate_id: int,
        deliverable_name: str,
        run_id: int,
        clinical_age_group_set_id: int,
        write_final_data: bool,
        map_version: Optional[int] = None,
    ):
        """This class uses the Marketscan ICD-mart to create final data depending
        on 3 key arguments that determine which clinical processes will be applied.
        These are bundle_id, estimate_id and deliverable_name.

        Args:
            bundle_id (int): Bundle_id present in the clinical map.
            estimate_id (int): Standard clinical claims estimate_id.
            deliverable_name (str): Consumer of output data.
                Currently 'gbd' or 'correction factors'.
            run_id (int): Standard clinical run_id. The map_version will be pulled from
                the run_metadata table the input run_id.
            map_version (int): Standard clinical map version. Must be provided if
                run_id is None.
            clinical_age_group_set_id (int): The set of age groups to include in non-CF
                final data. NOTE: these must be derivable from the data itself and
                Marketscan has no way to determine exact age under 1. Only age sets 1
                and 3 can currently be used.
            write_final_data (bool]): Requires that run_id is not None. Final data is
                written to a directory which has the run_id.
        """

        self.bundle_id = bundle_id
        self._create_estimate(estimate_id)
        self.deliverable = process_types.create_deliverable(deliverable_name)
        self.run_id = run_id
        self.map_version = map_version
        self.clinical_age_group_set_id = clinical_age_group_set_id
        self.write_final_data = write_final_data

        self.file_format = config.get_settings().file_format
        self._validate_map_run()
        self._assign_name()
        self._add_logger()
        if self.run_id:
            self.map_version = _assign_map_version_by_run(run_id)

    def _add_logger(self):
        if self.run_id:
            settings = config.get_settings()
            log_path = (
                "FILEPATH"
            )
            logger.add(log_path)
        logger.info(f"Logger up and running for {self.name}")

    def _validate_map_run(self):
        if self.map_version is not None and self.run_id is not None:
            raise RuntimeError(
                "Cannot provide both a map_version and run_id, one or the other"
                " must be selected (map_version is pulled for a run using the"
                " run_metadata table)"
            )
        if self.map_version is None and self.run_id is None:
            raise RuntimeError(
                "Must provide either a clinical map_version or a run_id in "
                "order for the pipeline to pull the correct map version"
            )
        if self.run_id is None and self.write_final_data:
            raise RuntimeError("run_id is required for final data outputs")

    def _assign_name(self):
        self.name = (
            f"bundle_{self.bundle_id}_estimate_{self.estimate.estimate_id}"
            f"_for_{self.deliverable.name}"
        )

    def _create_estimate(self, estimate_id) -> None:
        """Create the estimate class from the deduplication package for a given estimate_id"""

        self.estimate = getattr(estimates, f"ESTIMATE_{estimate_id}")

    def append_shape(self, msg):
        (rows, columns) = self.df.shape
        return f"{msg} Data has {rows:,} rows and {columns} columns".format(msg, rows, columns)

    def append_case_sum(self, msg):
        if "val" in self.df.columns:
            cases = self.df["val"].sum()
        else:
            cases = sum(self.df["sample_size"] * self.df["mean"])

        msg += " There are {:,} cases in the data".format(round(cases, 2))
        return msg

    def _manage_missing_claims(self):
        """Sometimes the underlying data is unable to meet requirements for a certain
        estimate_id. This happens with primary-diagnosis-only injuries data because
        e-coding in claims records is quite uncommon. These failures are expected, so we don't
        want the entire workflow to fail, but we would like some way to review these 'silent'
        failures. This method will write a JSON file with identifying attributes to the run.
        """

        settings = config.get_settings()
        if len(self.df) == 0 and self.run_id:
            # write a small notification file to allow the team to manually review these
            # failures
            note_path = (
                "FILEPATH"
            )
            with open(note_path, "w") as json_path:
                json.dump(self.name, json_path)
            logger.info(
                "This bundle-estimate cannot be calculated. There is no underlying data "
                "available which meets bundle and estimate requirements"
            )
            os._exit(0)

    def _assign_code_systems(self):
        """get list of ICD code systems by bundle"""
        self.code_system_ids = clinical_mapping_db.get_code_sys_by_bundle(
            bundle_id=self.bundle_id, map_version=self.map_version
        )
        logger.info(f"This bundle contains code system IDs {self.code_system_ids}")

    def _read(self):
        """read ICD-mart data"""
        settings = config.get_settings()
        self.df = io.pull_bundle_data(
            filepath=settings.input_dir,
            bundle_id=self.bundle_id,
            estimate=self.estimate,
            map_version=self.map_version,
            reader="ms_wrapper",
            columns=list(marketscan_pipeline.input_schema.columns.keys()),
        )
        logger.info(self.append_shape("Raw input data ingested."))

    def _map_to_bundle(self):
        """append on a bundle_id column and remove any non-bundle of interest data"""
        self.df = mapping.map_icd_mart(
            icd_mart_df=self.df, map_version=self.map_version, bundle_id=self.bundle_id
        )
        self.measure_id = self.df["measure_id"].iloc[0]
        logger.info(self.append_shape("Data mapped to bundle."))

    def _remove_ineligible_facilities(self):
        """Remove certain facility_ids for incidence outpatient data"""
        if self.measure_id == measures.INCIDENCE and self.estimate.apply_deduplication:
            self.df = filters.remove_inc_facilities(self.df)
            msg = "Certain facilities have been removed for this incidence bundle."
            logger.info(self.append_shape(msg))

        self._manage_missing_claims()

    def _apply_age_sex_restrictions(self, break_if_not_contig: bool):
        """apply age-sex restrictions"""
        self.df = filters.apply_clinical_age_sex_restrictions(
            df=self.df,
            map_version=self.map_version,
            clinical_age_group_set_id=self.clinical_age_group_set_id,
            break_if_not_contig=break_if_not_contig,
        )
        self._manage_missing_claims()
        logger.info(self.append_shape("Age and sex restrictions have been applied."))

    def _deduplicate(self):
        """Filter and deduplicate to meet requirements for input estimate_id"""
        self.df = apply_deduplication.deduplicate_data(
            df=self.df, estimate_id=self.estimate.estimate_id, map_version=self.map_version
        )
        self._manage_missing_claims()
        msg = f"Data now meets requirements of estimate {self.estimate.name}."
        logger.info(self.append_shape(msg))

    def _aggregate(self, agg_cols: List[str], sum_cols: List[str]):
        """Aggregate data out of claim or individual observations"""
        self.df = aggregators.sum_marketscan(df=self.df, agg_cols=agg_cols, sum_cols=sum_cols)
        msg = "Data has been aggregated."
        logger.info(self.append_case_sum(self.append_shape(msg)))

    def _write_correction_factors(self):
        """Output a CF data product before noise reduction is applied, similar to the current
        Marketscan CF data"""
        settings = config.get_settings()
        # Copy the data and adjust some column names/values to match CF inputs
        cf_df = pipeline.enforce_cf_schema(self.df.copy(deep=True))
        if self.write_final_data:
            final_path = (
                "FILEPATH"
            )
            io.write_wrapper(
                df=cf_df,
                write_path=final_path,
                file_format=self.file_format,
                overwrite=settings.overwrite_final_data,
            )
            msg = f"Final pipeline data for {self.deliverable.name} has been written."
            logger.info(self.append_case_sum(self.append_shape(msg)))

    def _incorporate_denominators(self):
        """Read, merge and validate denominators (aka sample size)
        Re-apply restrictions after data is squared by the denominator merge
        Fill missing encounters (NaN) with 0
        Remove years of data if the bundle does not map to a full code system for that year
        """
        settings = config.get_settings()
        self.denominator_df = io.pull_denominator_data(settings.denominator_dir)
        self.df = apply_denominators.merge_denominators(self.df, self.denominator_df)
        # re-apply age-sex restrictions after adding all demographics with denominator_df
        self.df = filters.apply_clinical_age_sex_restrictions(
            df=self.df,
            map_version=self.map_version,
            clinical_age_group_set_id=self.clinical_age_group_set_id,
            break_if_not_contig=True,
        )
        self.df = apply_denominators.fill_missing_encounters(self.df)
        self.df = apply_denominators.handle_code_system_year_missingness(
            self.df, self.code_system_ids
        )
        msg = "Denominators merged onto data."
        logger.info(self.append_case_sum(self.append_shape(msg)))

    def _bin_age(self):
        """age-bins into clinical age group set id"""
        self.df = demographic.age_binning(
            self.df,
            clinical_age_group_set_id=self.clinical_age_group_set_id,
            drop_age=True,
            terminal_age_in_data=False,
            under1_age_detail=False,
            break_if_not_contig=True,
        )
        msg = f"Age-binning complete with clinical group set {self.clinical_age_group_set_id}"
        logger.info(self.append_shape(msg))

    def _create_rates(self):
        self.df = apply_denominators.create_rates(self.df, self.measure_id)
        self.df = self.df.drop("val", axis=1)

    def _retain_us_child_locations(self):
        """Retain US child location_ids, currently 50 US states and DC"""
        settings = config.get_settings()
        self.df = filters.retain_us_child_locations(self.df, settings.release_id_for_us_states)
        msg = "Locations in data outside of US children have been removed"
        logger.info(self.append_shape(msg))

    def _convert_age_to_group_id(self):
        """attach age group id"""
        self.df = demographic.group_id_start_end_switcher(
            self.df, clinical_age_group_set_id=self.clinical_age_group_set_id, remove_cols=True
        )
        logger.info(self.append_shape("Age group ID attached and age start/end removed"))

    def _write_noise_reduced_pickle(self, noise_reduction):
        settings = config.get_settings()
        if self.write_final_data:
            pickle_path = (
                "FILEPATH"
            )
            pickle.dump(noise_reduction, open(f"{pickle_path}.p", "wb"))

    def _noise_reduce(self):
        """Loops over each unique sex_id in the data and applies the noise reduction algorithm
        to it. Appends results to a list and replaces the existing self.df object once finished
        """
        self.pre_noise_reduction_df = self.df.copy(deep=True)

        # sex_id is modeled separately in noise reduction, so loop over each unique value and
        # perform the smoothing step
        self.noise_reduction_obj_list = []
        for sex_id in self.pre_noise_reduction_df["sex_id"].unique():
            tmp = self.pre_noise_reduction_df.query(f"sex_id == {sex_id}").copy(deep=True)

            model_group = (
                f"{self.bundle_id}_{self.estimate.estimate_id}_"
                f"{sex_id}_{constants.PARENT_LOCATION_ID}"
            )

            tmp = tmp.rename(columns={"mean": constants.COLUMN_TO_NOISE_REDUCE})

            noise_reduced = clinical_noise_reduction.run_noise_reduction(
                df_to_noise_reduce=tmp,
                model_group=model_group,
                run_id=self.run_id,
                model_type="Poisson",
                cols_to_nr=[constants.COLUMN_TO_NOISE_REDUCE],
                name=self.name,
            )
            self._write_noise_reduced_pickle(noise_reduced)
            self.noise_reduction_obj_list.append(noise_reduced)

        self.df = pd.concat(
            [noise_reduction.df for noise_reduction in self.noise_reduction_obj_list],
            sort=False,
            ignore_index=False,
        )
        logger.info(self.append_shape("Noise Reduction is complete"))

    def _apply_asfr(self):
        maternal_bundles = maternal_funcs.get_maternal_bundles(
            map_version=self.map_version, run_id=self.run_id
        )
        proc_bund = f"Deliverable={self.deliverable.name}, Bundle={self.bundle_id}"
        if self.deliverable.apply_maternal_adjustment and self.bundle_id in maternal_bundles:
            self.df["year_start"] = self.df["year_id"]
            self.df = maternal_funcs.apply_asfr(
                df=self.df, maternal_bundles=maternal_bundles, run_id=self.run_id
            )
            logger.info(f"Applying ASFR: {proc_bund}")
            self.df = self.df.drop("year_start", axis=1)
        else:
            logger.info(f"ASFR was NOT applied: {proc_bund}")

    def _format_final_data_df(self):
        """Rename the *_final, eg noise reduced column and then filter all of the unneeded
        columns related to modeling and noise reduction"""
        self.df = self.df.rename(columns={f"{constants.COLUMN_TO_NOISE_REDUCE}_final": "mean"})
        self.df = self.df[marketscan_pipeline.final_schema.columns.keys()]
        logger.info(
            self.append_case_sum(self.append_shape("Final data formatting is complete"))
        )

    def _validate_schema(self, df: pd.DataFrame, schema: pa.DataFrameSchema) -> pd.DataFrame:
        """Enforce a defined schema on a dataset.

        Args:
            df (pd.DataFrame): Table to validate schema.
            schema (str): Name for the schema found with:
                IMPORT_PATH

        Raises:
            RuntimeError: The tested df is empty.
        """

        if df.empty:
            raise RuntimeError("There is an error, the table is empty.")
        else:
            schema.validate(df)

        return df

    def _write_final_data(self):
        """Writes a file to the run id. 1 file per bundle-estimate-deliverable"""
        settings = config.get_settings()
        if self.write_final_data:
            final_path = (
                "FILEPATH"
            )
            io.write_wrapper(
                df=self.df,
                write_path=final_path,
                file_format=self.file_format,
                overwrite=settings.overwrite_final_data,
            )
            msg = f"Final pipeline data for {self.deliverable.name} has been written."
            logger.info(self.append_case_sum(self.append_shape(msg)))

    def main(self) -> None:
        """Perform all Marketscan pipeline processing steps. The data can be quite large
        so it usually updates the self.df object in place.
        Optionally outputs final data to a clinical run_id
        """
        self._assign_code_systems()
        self._read()
        self.df = self._validate_schema(df=self.df, schema=marketscan_pipeline.input_schema)
        self._map_to_bundle()
        self.df = self._validate_schema(df=self.df, schema=marketscan_pipeline.input_schema)
        self._remove_ineligible_facilities()
        self._apply_age_sex_restrictions(break_if_not_contig=False)
        self._deduplicate()
        self._aggregate(
            agg_cols=constants.AGE_GROUPBY_COLS + self.deliverable.groupby_cols,
            sum_cols=["val"],
        )
        self.df = self._validate_schema(
            df=self.df, schema=marketscan_pipeline.intermediate_schema
        )
        if self.deliverable.name == "correction_factors":
            self._write_correction_factors()
            return
        self._incorporate_denominators()
        self._bin_age()
        self._aggregate(
            agg_cols=constants.DENOMINATOR_GROUPBY_COLS, sum_cols=["val", "sample_size"]
        )
        self.df = self._validate_schema(
            df=self.df, schema=marketscan_pipeline.denominator_schema
        )
        self._create_rates()
        self._retain_us_child_locations()
        self.df = self._validate_schema(
            df=self.df, schema=marketscan_pipeline.pre_noise_reduction_schema
        )
        self._noise_reduce()
        self._convert_age_to_group_id()
        self._format_final_data_df()
        self._apply_asfr()
        self.df = self._validate_schema(df=self.df, schema=marketscan_pipeline.final_schema)
        self._write_final_data()


def _assign_map_version_by_run(run_id: int) -> int:
    """Gets a map_version linked to a run_id from run_metadata."""

    settings = config.get_settings()
    map_version = int(
        ClaimsWrappers(run_id=run_id, odbc_profile=settings.odbc_profile)
        .pull_run_metadata()
        .map_version.iloc[0]
    )
    return map_version
