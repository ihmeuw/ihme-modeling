"""Paths used in ST-GPR.

The model output directory looks like this (before intermediate files are deleted):
<output_root_directory>/
    parameters.json -- parameters passed in by user, not yet prepped
    metadata.json -- annotated parameters, after prep
    prepped.feather -- prepped data
    covariates.feather -- GBD and/or custom covariates
    holdouts.feather -- holdout information
    custom_stage_1.feather -- custom stage 1 estimates
    temp_0.h5 -- stage 1 estimates
    stage1_summary.csv -- stage 1 statistics
    location_hierarchy.feather -- location hierarchy
    population.feather -- population estimates
    infile_tmp.csv -- temporary file used for infiling
    output_0_{parameter_set_number}.h5 -- final outputs for holdout 0 (nothing held out)
    st_temp_0/ -- Spacetime estimates at holdout 0 (nothing held out)
        {location_group_number}.csv -- Spacetime estimates for a location group
    gpr_temp_{holdout_number}/{parameter_set_number} -- Temp GPR estimates, saved by holdout
                                                        and param set
        {location_group_number}.csv
    rake_means_temp_0/ -- Final (raked) estimates, which only exist for holdout 0
    draws_temp_{holdout_number}/ -- Draws for given holdout. Only written for runs with draws.
                                    Final draws have holdout_number=0
        {location_id}.csv
"""

import os
from typing import Final

import stgpr_schema


class StgprPaths:
    """Sets directory structure constants given an ST-GPR version ID."""

    def __init__(self, stgpr_version_id: int) -> None:
        """Initializes constants for output directory structure."""
        settings = stgpr_schema.get_settings()

        # Root directories for output and parameters.
        self.OUTPUT_ROOT: Final[str] = settings.output_root_format.format(
            stgpr_version_id=stgpr_version_id
        )
        self.MNT_CC_OUTPUT_ROOT: Final[str] = settings.mnt_cc_output_root_format.format(
            stgpr_version_id=stgpr_version_id
        )

        # Parameters.
        self.PARAMETERS: Final[str] = os.path.join(self.MNT_CC_OUTPUT_ROOT, "parameters.json")
        self.METADATA: Final[str] = os.path.join(self.OUTPUT_ROOT, "metadata.json")

        # Data, covariates, and holdouts.
        self.PREPPED: Final[str] = os.path.join(self.OUTPUT_ROOT, "prepped.feather")
        self.COVARIATES: Final[str] = os.path.join(self.OUTPUT_ROOT, "covariates.feather")
        self.HOLDOUTS: Final[str] = os.path.join(self.OUTPUT_ROOT, "holdouts.feather")

        # Estimates.
        self.CUSTOM_STAGE_1: Final[str] = os.path.join(
            self.OUTPUT_ROOT, "custom_stage_1.feather"
        )
        self.STAGE_1_ESTIMATES: Final[str] = os.path.join(self.OUTPUT_ROOT, "temp_0.h5")
        self.SPACETIME_ESTIMATES_DIR_FORMAT: Final[str] = os.path.join(
            self.OUTPUT_ROOT, "st_temp_0", "{parameter_set_number}"
        )
        self.GPR_ESTIMATES_DIR_FORMAT: Final[str] = os.path.join(
            self.OUTPUT_ROOT, "gpr_temp_{holdout_number}", "{parameter_set_number}"
        )
        self.FINAL_ESTIMATES_DIR: Final[str] = os.path.join(
            self.OUTPUT_ROOT, "rake_means_temp_0"
        )

        # Final output.
        self.OUTPUT_FORMAT: Final[str] = os.path.join(
            self.OUTPUT_ROOT, "output_0_{parameter_set_number}.h5"
        )

        # Draws
        self.DRAWS_DIR_FORMAT: Final[str] = "draws_temp_{holdout_number}"
        self.DRAWS_FILE_FORMAT: Final[str] = "{location_id}.csv"
        self.DRAWS_DIR: Final[str] = os.path.join(self.OUTPUT_ROOT, self.DRAWS_DIR_FORMAT)
        self.DRAWS_FORMAT: Final[str] = os.path.join(
            self.DRAWS_DIR.format(holdout_number=0), self.DRAWS_FILE_FORMAT
        )

        # Statistics.
        self.STAGE_1_STATISTICS: Final[str] = os.path.join(
            self.OUTPUT_ROOT, "stage1_summary.csv"
        )
        self.FIT_STATISTICS: Final[str] = os.path.join(self.OUTPUT_ROOT, "fit_stats.csv")

        # Demographics.
        self.LOCATION_HIERARCHY: Final[str] = os.path.join(
            self.OUTPUT_ROOT, "location_hierarchy.feather"
        )
        self.AGGREGATION_LOCATION_HIERARCHY: Final[str] = os.path.join(
            self.OUTPUT_ROOT, "aggregation_location_hierarchy.feather"
        )
        self.POPULATION: Final[str] = os.path.join(self.OUTPUT_ROOT, "population.feather")

        # Infiling.
        self.INFILE_TMP: Final[str] = os.path.join(self.OUTPUT_ROOT, "infile_tmp.csv")

        # Logs.
        self.ERROR_LOG_PATH: Final[str] = os.path.join(self.OUTPUT_ROOT, "logs", "errors")
        self.OUTPUT_LOG_PATH: Final[str] = os.path.join(self.OUTPUT_ROOT, "logs", "output")
