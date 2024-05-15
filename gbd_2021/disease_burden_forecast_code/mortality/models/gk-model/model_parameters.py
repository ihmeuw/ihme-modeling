"""This module contains a class that encapsulates logic/info related GK model parameters.
"""

from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import xarray as xr
from fhs_lib_database_interface.lib.constants import DimensionConstants
from tiny_structured_logger.lib.fhs_logging import get_logger

from fhs_lib_model.lib.constants import GKModelConstants

logger = get_logger()


class ModelParameterEffects:
    """Creates the initial random and fixed effects for GK Model."""

    def __init__(
        self,
        fixed_effects: Dict[str, List[Tuple[str, float]]],
        random_effects: Dict[str, List[str]],
        constants: Optional[List[str]] = None,
    ) -> None:
        """Initializer for fixed and random effects container.

        Args:
            fixed_effects (Dict[str, List[Tuple[str, float]]]): The fixed effects of the model.
                Maps the parameter names to their associated covariates, and the restrictions
                of those covariates.
                e.g.,
                .. code:: python
                    {
                        "beta_age": [("vehicles_2_plus_4wheels_pc", 0), ("sdi", 0)],
                        "beta_global": [("hiv", 1), ("sdi", 0), ("intercept", 0)],
                    }
            random_effects (Dict[str, List[str]]): The random effects of the model. Maps the
                parameter names to their associated covariates. e.g.,
                .. code:: python
                    {
                        "gamma_location_age": ["intercept"],
                        "gamma_age": ["time_var"],
                    }
            constants (Optional[List[str]]): The names of the constants to be added to the
                model. Defaults to ``None``, i.e., no constants.

        Raises:
            ValueError: If the fixed or random effects are improperly named.
        """
        self.fixed_effect_names = [
            "{}_{}".format("beta", t) for t in GKModelConstants.STANDARD_PARAMS
        ]
        missing_fixed = set(fixed_effects.keys()) - set(self.fixed_effect_names)
        if len(missing_fixed):
            err_msg = f"Missing fixed effects: {missing_fixed}"
            logger.error(err_msg)
            raise ValueError(err_msg)

        self.fixed_effects = {
            k: (fixed_effects[k] if k in fixed_effects.keys() else [])
            for k in self.fixed_effect_names
        }

        self.random_effect_names = [
            "{}_{}".format("gamma", t) for t in GKModelConstants.STANDARD_PARAMS
        ]
        missing_random = set(random_effects.keys()) - set(self.random_effect_names)
        if len(missing_random):
            err_msg = f"Missing random effects: {missing_random}"
            logger.error(err_msg)
            raise ValueError(err_msg)

        self.random_effects = {
            k: (random_effects[k] if k in list(random_effects.keys()) else [])
            for k in self.random_effect_names
        }

        self.constants = constants if constants is not None else []

        self.init = dict()
        self.data = dict()

    def extract_param_data(
        self,
        dataset: xr.Dataset,
        final_array: xr.DataArray,
        final_array_col: xr.DataArray,
    ) -> None:
        """Parse out the covariate information from the appropriate dictionaries.

        Args:
            dataset (xr.Dataset): The dataset to extract parameter information from.
            final_array (xr.DataArray): An array containing same data as in ``dataset``, except
                in array form such that each data variable from ``dataset`` exists as a slice
                of the array.
            final_array_col (xr.DataArray): Only the dims from
                ``GKModelConstants.MODELABLE_DIMS`` are expected, and in that order.

        Raises:
            ValueError: If any constraints are invalid.
        """
        if DimensionConstants.REGION_ID in list(dataset.data_vars.keys()):
            region_size = len(np.unique(dataset.region_id.values))
            logger.debug("Nonzero region size", bindings=dict(region_size=region_size))
        else:
            region_size = 0
            logger.debug("Region size is zero")

        if DimensionConstants.SUPER_REGION_ID in list(dataset.data_vars.keys()):
            super_region_size = len(np.unique(dataset.super_region_id.values))
            logger.debug(
                "Nonzero super region size",
                bindings=dict(super_region_size=super_region_size),
            )
        else:
            super_region_size = 0
            logger.debug("Super Region size is zero")

        for cov in self.fixed_effect_names:
            cov_type = "_".join(cov.split("_")[1:])

            beta_const_name = f"beta_{cov_type}_constraint"
            beta_raw_name = f"beta_{cov_type}_raw"
            beta_mean_name = f"beta_{cov_type}_mean"

            X = f"X_{cov_type}"
            X_draw = f"X_{cov_type}_draw"

            self.data[cov], self.data[beta_const_name] = self._parse_covariates(
                self.fixed_effects[cov]
            )
            self.data[X] = final_array_col.sel(cov=self.data[cov]).values
            self.data[X_draw] = final_array.sel(cov=self.data[cov])
            param_shape = self._name_to_shape(
                cov_type,
                self.data[X].shape,
                super_region_size,
                region_size,
            )
            self.init[beta_raw_name] = np.zeros(param_shape)
            self.init[beta_mean_name] = np.ones(param_shape)

        for random_effect_name in self.random_effect_names:
            random_effect_type = "_".join(random_effect_name.split("_")[1:])

            self.data[random_effect_name], _ = self._parse_covariates(
                self.random_effects[random_effect_name]
            )

            gamma_name = f"gamma_{random_effect_type}"
            Z = f"Z_{random_effect_type}"
            Z_draw = f"Z_{random_effect_type}_draw"
            tau_name = f"log_tau_{random_effect_type}"

            self.data[Z] = final_array_col.sel(cov=self.data[random_effect_name]).values
            self.data[Z_draw] = final_array.sel(cov=self.data[random_effect_name])

            param_shape = self._name_to_shape(
                random_effect_type,
                self.data[Z].shape,
                super_region_size,
                region_size,
            )
            self.init[gamma_name] = np.zeros(param_shape)
            self.init[tau_name] = np.zeros(
                (1, 1, 1, self.data[Z].shape[-1])
            )

        self.data["constant"] = final_array_col.sel(cov=self.constants).values
        self.data["constant_draw"] = final_array.sel(cov=self.constants)
        self.data["constant_mult"] = np.ones(
            (1, 1, 1, self.data["constant"].shape[3])
        )

    @staticmethod
    def _name_to_shape(
        covariate: str,
        dim_sizes: List[int],
        super_region_size: int,
        region_size: int,
    ) -> Tuple[int, int, int, int]:
        """Converts a covariate name to a proper shape.

        Args:
            covariate (str): The name of the covariate.
            dim_sizes (List[int, int, int, int]): Contains the generic size of each model-able
                dimension, in the order location, age, time, and covariate.
            super_region_size (int):
                The size of the super region dimension.
            region_size (int):
                The size of the region dimension.

        Returns:
            Tuple[int, int, int, int]: The dimension sizes expected for the given covariate.
        """
        location_size, age_size, _, covariate_size = dim_sizes

        expected_time_size: int = 1
        expected_covariate_size: int = covariate_size

        expected_age_size: int
        if GKModelConstants.AGE_PARAM in covariate:
            expected_age_size = age_size
        else:
            expected_age_size = 1

        expected_location_size: int
        if GKModelConstants.SUPER_REGION_PARAM in covariate:
            expected_location_size = super_region_size
        elif GKModelConstants.REGION_PARAM in covariate:
            expected_location_size = region_size
        elif GKModelConstants.LOCATION_PARAM in covariate:
            expected_location_size = location_size
        else:
            expected_location_size = 1

        return (
            expected_location_size,
            expected_age_size,
            expected_time_size,
            expected_covariate_size,
        )

    @staticmethod
    def _parse_covariates(
        covariates: Union[List[str], List[Tuple[str, float]]],
    ) -> Tuple[List[str], np.ndarray]:
        """Parse covariates from a list of tuples or strings.

        Args:
            covariates (Union[List[str], List[Tuple[str, float]]]): List of covariate names
                corresponding to model parameter. If for a fixed-effect parameter, then a list
                of tuples is given where each tuple has a covariate name and the restriction,
                i.e., constraint, associated with that covariate.

        Returns:
            Tuple[List[int], np.array]: The list of covariate names and a 1D array of with
                constraints.

        Raises:
            RuntimeError: If any constraints are invalid.
        """
        # Parameter has no covariates.
        if len(covariates) == 0:
            return [], np.array([])
        # Parameter is a **random** effect.
        elif isinstance(covariates[0], str):
            covariate_names = [c for c in covariates]
            constraints = [0] * len(covariates)  # Random effects don't have constraints.
        # Parameter is a **fixed** effect.
        else:
            # Separate out the covariate names from their constraints.
            covariate_names = [x for c in covariates for x in c if isinstance(x, str)]
            constraints = [x for c in covariates for x in c if not isinstance(x, str)]

        ModelParameterEffects._check_constraints(constraints)
        return covariate_names, np.array(constraints)

    @staticmethod
    def _check_constraints(constraints: List[int]) -> None:
        """Make sure that all constraints are either -1, 0, or 1.

        Args:
            constraints (List[int]): List of integers to use as GK TMB constraints.

        Raises:
            ValueError: If any constraints are invalid.
        """
        bad_constraints = set(constraints) - set(GKModelConstants.VALID_CONSTRAINTS)

        if len(bad_constraints):
            err_msg = "Constraints must be either 1, -1, or 0."
            logger.error(err_msg)
            raise ValueError(err_msg)
