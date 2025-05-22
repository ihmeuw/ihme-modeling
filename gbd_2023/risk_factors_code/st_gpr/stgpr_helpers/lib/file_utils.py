"""Helpers for caching intermediate file data and writing final outputs."""

import json
import os
from typing import Any, Dict, Optional

import pandas as pd

from db_stgpr import columns
from ihme_cc_gbd_schema.common import ModelStorageMetadata

from stgpr_helpers.lib.constants import paths


class StgprFileUtility:
    """Used to store output path and propagate it to file reading/writing functions.

    For covenience, separate functions are provided for each cached model input.
    """

    def __init__(self, stgpr_version_id: int) -> None:
        """Initialzes path constants for use in reading/writing functions."""
        self._stgpr_paths: paths.StgprPaths = paths.StgprPaths(stgpr_version_id)

    def make_root_directory(self) -> None:
        """Creates the output directory, if it doesn't exist yet."""
        os.makedirs(self._stgpr_paths.OUTPUT_ROOT, exist_ok=True)
        os.chmod(self._stgpr_paths.OUTPUT_ROOT, 0o775)

    def make_logs_directory(self, log_path: Optional[str] = None) -> None:
        """Creates the logs directory, if it doesn't exist yet.

        Creates the directory at log_path if given. Otherwise, the default error and output
        log paths are created.
        """
        if log_path is None:
            os.makedirs(self._stgpr_paths.ERROR_LOG_PATH, exist_ok=True)
            os.makedirs(self._stgpr_paths.OUTPUT_LOG_PATH, exist_ok=True)
            os.chmod(self._stgpr_paths.ERROR_LOG_PATH, 0o775)
            os.chmod(self._stgpr_paths.ERROR_LOG_PATH, 0o775)
        else:
            os.makedirs(log_path, exist_ok=True)
            os.chmod(log_path, 0o775)

    def make_root_parameters_directory(self) -> None:
        """Creates the parameters directory, if it doesn't exist yet."""
        os.makedirs(self._stgpr_paths.MNT_CC_OUTPUT_ROOT, exist_ok=True)
        os.chmod(self._stgpr_paths.MNT_CC_OUTPUT_ROOT, 0o775)

    def cache_parameters(self, params: Dict[str, Any]) -> None:
        """Caches user input parameters as JSON."""
        with open(self._stgpr_paths.PARAMETERS, "w") as f:
            json.dump(params, f)
        os.chmod(self._stgpr_paths.PARAMETERS, 0o775)

    def read_parameters(self) -> Dict[str, Any]:
        """Reads cached parameters."""
        with open(self._stgpr_paths.PARAMETERS) as f:
            return json.load(f)

    def cache_metadata(self, metadata: Dict[str, Any]) -> None:
        """Caches model metadata (annotated parameter dictionary) as JSON."""
        with open(self._stgpr_paths.METADATA, "w") as f:
            json.dump(metadata, f)
        os.chmod(self._stgpr_paths.METADATA, 0o775)

    def read_metadata(self) -> Dict[str, Any]:
        """Reads cached metadata."""
        with open(self._stgpr_paths.METADATA) as f:
            return json.load(f)

    def cache_prepped_data(self, prepped: pd.DataFrame) -> None:
        """Caches prepped data needed for a model run."""
        self._cache_dataframe(self._stgpr_paths.PREPPED, prepped)

    def read_prepped_data(self) -> pd.DataFrame:
        """Reads cached prepped data."""
        return self._read_cached_dataframe(self._stgpr_paths.PREPPED)

    def cache_holdouts(self, holdouts: pd.DataFrame) -> None:
        """Caches holdouts needed for a model run."""
        self._cache_dataframe(self._stgpr_paths.HOLDOUTS, holdouts)

    def read_holdouts(self) -> pd.DataFrame:
        """Reads cached holdouts."""
        return self._read_cached_dataframe(self._stgpr_paths.HOLDOUTS)

    def cache_custom_stage_1(self, custom_stage_1: pd.DataFrame) -> None:
        """Caches custom stage 1 needed for a model run."""
        self._cache_dataframe(self._stgpr_paths.CUSTOM_STAGE_1, custom_stage_1)

    def read_covariates(self) -> pd.DataFrame:
        """Reads cached custom stage 1."""
        return self._read_cached_dataframe(self._stgpr_paths.COVARIATES)

    def cache_covariates(self, covariates: pd.DataFrame) -> None:
        """Caches custom stage 1 needed for a model run."""
        self._cache_dataframe(self._stgpr_paths.COVARIATES, covariates)

    def read_custom_stage_1(self) -> pd.DataFrame:
        """Reads cached custom stage 1."""
        return self._read_cached_dataframe(self._stgpr_paths.CUSTOM_STAGE_1)

    def read_stage_1_estimates(self) -> pd.DataFrame:
        """Reads cached stage 1 estimates."""
        return pd.read_hdf(self._stgpr_paths.STAGE_1_ESTIMATES, "stage1")

    def read_stage_1_statistics(self) -> pd.DataFrame:
        """Reads cached stage 1 statistics."""
        return pd.read_csv(self._stgpr_paths.STAGE_1_STATISTICS)

    def read_spacetime_estimates(self, parameter_set_number: int) -> pd.DataFrame:
        """Reads cached spacetime estimates for a parameter set."""
        estimates_dir = self._stgpr_paths.SPACETIME_ESTIMATES_DIR_FORMAT.format(
            parameter_set_number=parameter_set_number
        )
        estimate_files = [
            os.path.join(estimates_dir, f)
            for f in os.listdir(estimates_dir)
            if f.endswith(".csv")
        ]
        return pd.concat([pd.read_csv(f) for f in estimate_files])

    def read_gpr_estimates(self, parameter_set_number: int) -> pd.DataFrame:
        """Reads cached spacetime estimates for a parameter set."""
        estimates_file = self._stgpr_paths.OUTPUT_FORMAT.format(
            parameter_set_number=parameter_set_number
        )
        return pd.read_hdf(estimates_file, "gpr")

    def read_gpr_estimates_temp(
        self, holdout_number: int, parameter_set_number: int
    ) -> pd.DataFrame:
        """Read cached GPR estimates for a holdout and parameter set."""
        estimates_dir = self._stgpr_paths.GPR_ESTIMATES_DIR_FORMAT.format(
            holdout_number=holdout_number, parameter_set_number=parameter_set_number
        )
        estimate_files = [
            os.path.join(estimates_dir, f)
            for f in os.listdir(estimates_dir)
            if f.endswith(".csv")
        ]
        return pd.concat([pd.read_csv(f) for f in estimate_files])

    def read_final_estimates(self, parameter_set_number: int) -> pd.DataFrame:
        """Reads cached final estimates.

        Only the best parameter set will have raked results.

        Final estimates in this context refer to summaries of post-raked results.
        """
        estimates_file = self._stgpr_paths.OUTPUT_FORMAT.format(
            parameter_set_number=parameter_set_number
        )
        return pd.read_hdf(estimates_file, "raked")

    def read_final_estimates_temp(self, parameter_set_number: int) -> pd.DataFrame:
        """Read temporary final estimates.

        Final estimates in this context refer to summaries of post-raked results.
        Temp GPR estimates are read rather than final GPR estimates as the former
        do not contain location aggregates.

        Args:
            parameter_set_number: must be the best parameter set or won't read in
                estimates for raked locations
        """
        non_raked_estimates = self.read_gpr_estimates_temp(
            holdout_number=0, parameter_set_number=parameter_set_number
        )

        raked_estimates_dir = self._stgpr_paths.FINAL_ESTIMATES_DIR
        raked_estimate_files = [
            os.path.join(raked_estimates_dir, f)
            for f in os.listdir(raked_estimates_dir)
            if f.endswith(".csv")
        ]
        raked_estimates = pd.concat([pd.read_csv(f) for f in raked_estimate_files])

        # Drop raked locations (national locations with subnationals) from GPR estimates
        non_raked_estimates = non_raked_estimates[
            ~non_raked_estimates[columns.LOCATION_ID].isin(
                raked_estimates[columns.LOCATION_ID].unique()
            )
        ]

        return pd.concat([raked_estimates, non_raked_estimates], sort=True)

    def cache_model_storage_metadata(self, gpr_draws: int) -> None:
        """Cache draws storage pattern to final draws dir."""
        draws_file_format = (
            os.path.join(
                self._stgpr_paths.DRAWS_DIR_FORMAT.format(holdout_number=0),
                self._stgpr_paths.DRAWS_FILE_FORMAT,
            )
            if gpr_draws > 0
            else "no_draws"
        )
        storage_metadata = ModelStorageMetadata.from_dict(
            {"storage_pattern": draws_file_format}
        )
        storage_metadata.to_file(self._stgpr_paths.OUTPUT_ROOT)

    def cache_final_draws(self, draws: pd.DataFrame) -> None:
        """Cache final draws by location in a single place.

        Always saves draws for holdout 0.
        """
        location_ids = draws[columns.LOCATION_ID].unique().tolist()

        for location_id in location_ids:
            draws[draws[columns.LOCATION_ID] == location_id].to_csv(
                self._stgpr_paths.DRAWS_FORMAT.format(location_id=location_id), index=False
            )

    def read_draws(
        self, holdout_number: int = 0, parameter_set_number: Optional[int] = None
    ) -> pd.DataFrame:
        """Read draws.

        holdout_number should be 0 and parameter_set_number should be None
        to read final draws (after run has completed).

        Args:
            holdout_number: holdout number of the draws to read. Defaults to 0 (no holdouts)
            parameter_set_number: If provided, must be the best parameter set or draws from
                raked locations will not be retrieved. Should only be provided within an
                ST-GPR run to access temporary files. For official model draws, do not
                provide a parameter set.
        """
        draws_dir = (
            os.path.join(
                self._stgpr_paths.DRAWS_DIR.format(holdout_number=holdout_number),
                str(parameter_set_number),
            )
            if parameter_set_number is not None
            else self._stgpr_paths.DRAWS_DIR.format(holdout_number=holdout_number)
        )

        draws_files = [
            os.path.join(draws_dir, f) for f in os.listdir(draws_dir) if f.endswith(".csv")
        ]
        return pd.concat([pd.read_csv(f) for f in draws_files])

    def read_all_fit_statistics(self) -> pd.DataFrame:
        """Read fit statistics from all parameter sets."""
        return pd.read_csv(self._stgpr_paths.FIT_STATISTICS)

    def read_fit_statistics(self, parameter_set_number: int) -> pd.DataFrame:
        """Reads cached fit statistics for a parameter set specifically for upload."""
        return (
            self.read_all_fit_statistics()
            .query("parameter_set == @parameter_set_number")
            .drop_duplicates(["var", "parameter_set", "is_mean", "oos_mean"])[
                ["var", "is_mean", "oos_mean"]
            ]
            .rename(
                columns={
                    "is_mean": columns.IN_SAMPLE_RMSE,
                    "oos_mean": columns.OUT_OF_SAMPLE_RMSE,
                }
            )
            .assign(
                **{
                    columns.MODEL_STAGE_ID: lambda df: df["var"].map(
                        {"stage1": 1, "st": 2, "gpr_mean": 3}
                    )
                }
            )
            .drop(columns="var")
        )

    def read_amplitude(self, parameter_set_number: int) -> pd.DataFrame:
        """Reads cached amplitude for a parameter set."""
        amplitude_file = self._stgpr_paths.OUTPUT_FORMAT.format(
            parameter_set_number=parameter_set_number
        )
        return pd.read_hdf(amplitude_file, "amp_nsv").rename(
            columns={"st_amp": columns.AMPLITUDE}
        )

    def read_nsv(self, parameter_set_number: int) -> pd.DataFrame:
        """Reads cached NSV."""
        nsv_file = self._stgpr_paths.OUTPUT_FORMAT.format(
            parameter_set_number=parameter_set_number
        )
        return (
            pd.read_hdf(nsv_file, "adj_data")
            .rename(columns={"nsv": columns.NON_SAMPLING_VARIANCE})[
                [columns.LOCATION_ID, columns.SEX_ID, columns.NON_SAMPLING_VARIANCE]
            ]
            .dropna(subset=[columns.NON_SAMPLING_VARIANCE])
            .drop_duplicates([columns.LOCATION_ID, columns.SEX_ID])
        )

    def cache_location_hierarchy(self, location_hierarchy: pd.DataFrame) -> None:
        """Caches location hierarchy as feather."""
        self._cache_dataframe(self._stgpr_paths.LOCATION_HIERARCHY, location_hierarchy)

    def read_location_hierarchy(self) -> pd.DataFrame:
        """Reads cached location hierarchy."""
        return self._read_cached_dataframe(self._stgpr_paths.LOCATION_HIERARCHY)

    def cache_aggregation_location_hierarchy(
        self, aggregation_location_hierarchy: pd.DataFrame
    ) -> None:
        """Caches aggregation location hierarchy as feather"""
        self._cache_dataframe(
            self._stgpr_paths.AGGREGATION_LOCATION_HIERARCHY, aggregation_location_hierarchy
        )

    def read_aggregation_location_hierarchy(self) -> pd.DataFrame:
        """Reads cached location hierarchy used for aggregation."""
        return self._read_cached_dataframe(self._stgpr_paths.AGGREGATION_LOCATION_HIERARCHY)

    def cache_population(self, population: pd.DataFrame) -> None:
        """Caches population estimates as feather."""
        self._cache_dataframe(self._stgpr_paths.POPULATION, population)

    def read_population(self) -> pd.DataFrame:
        """Reads cached population estimates."""
        return self._read_cached_dataframe(self._stgpr_paths.POPULATION)

    def _cache_dataframe(self, path: str, df: pd.DataFrame) -> None:
        """Caches DataFrame to model output directory.

        Feather only supports basic indices, so reset the index before saving.

        Args:
            path: Path where the DataFrame will be cached.
            df: DataFrame to cache.
        """
        df.reset_index(drop=True).to_feather(path)
        os.chmod(path, 0o775)

    def _read_cached_dataframe(self, path: str) -> pd.DataFrame:
        """Reads cached DataFrame.

        Args:
            path: Path to the cached DataFrame.

        Returns:
            Cached DataFrame.
        """
        return pd.read_feather(path)
