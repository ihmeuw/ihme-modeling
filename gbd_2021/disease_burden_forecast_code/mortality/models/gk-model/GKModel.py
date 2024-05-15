"""This module contains an implementation of the Girosi-King Model, i.e., the "GK" Model.
"""

import site
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import xarray as xr
from fhs_lib_database_interface.lib.constants import DimensionConstants
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib.fhs_logging import get_logger

from fhs_lib_model.lib.constants import GKModelConstants, ModelConstants, PyMBConstants
from fhs_lib_model.lib.gk_model.model_parameters import ModelParameterEffects
from fhs_lib_model.lib.gk_model.omega import omega_prior_vectors, omega_translate
from fhs_lib_model.lib.gk_model.post_process import (
    np_to_xr,
    region_to_location,
    transform_parameter_array,
)
from fhs_lib_model.lib.gk_model.pre_process import data_var_merge
from fhs_lib_model.lib.pymb.pymb import PyMB

logger = get_logger()


class ConvergenceError(Exception):
    """Error alerting non convergence of models."""


class GKModel(PyMB):
    """This class is an implementation of the Girosi-King Model, i.e., the "GK" Model.

    The ``GKModel`` class is a subclass of the ``PyMB`` model class, meaning that it relies
    heavily on the Template Model Builder (TMB) R package. The ``GKModel``, allows for ease of
    data preparation as long as the expected variables are included in a DataFrame which is
    passed in at the initialization step of the class.
    """

    def __init__(
        self,
        dataset: Union[xr.DataArray, xr.Dataset],
        years: YearRange,
        fixed_effects: Dict[str, List[Tuple[str, float]]],
        random_effects: Dict[str, List[str]],
        draws: int,
        constants: Optional[List[str]] = None,
        y: Optional[str] = None,
        omega_amp: Union[float, Dict[str, float]] = 0,
        weight_decay: float = 0,
        seed: Optional[int] = None,
    ) -> None:
        """Initializer for the GKModel.

        Args:
            dataset: (xr.Dataset | xr.DataArray): Data to forecast. If ``xr.Dataset`` then
                ``y`` paramter must be specified as the variable to forecast.
            years (YearRange): The forecasting time series.
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
            draws (int): Number of draws to make for predictions.
            constants (Optional[List[str]]): The names of the constants to be added to the
                model. Defaults to ``None``, i.e., no constants.
            y (str):
                The name of the dependent, i.e., response, variable. **NOTE:** this should be a
                data variable on ``dataset``, if dataset is an ``xr.Dataset``. Defaults to "y".
            omega_amp (Union[float, Dict[str, float]]):
                How much to amplify the omega priors by. Defaults to ``0``.
            weight_decay (float):
                How much to decay the weight of the likelihood on predictions as they get
                farther away from the last year in sample. Defaults to ``0``.
            seed (Optional[int]):
                an optional seed for the C++ and numpy random number generators

        Raises:
            ValueError: If invalid arguments are given for any of the following reasons:

                * If ``y`` not a variable on ``dataset``, i.e.,
                  ``list(dataset.data_vars.keys())``.
                * If the fixed or random effects are improperly named.
                * If any constraints are invalid.
        """
        super().__init__(name=self.__class__.__name__)

        self._init_model()
        self._init_dataset(dataset, y)

        self.years = years
        self.num_of_draws = draws

        self.random_effect_names = [
            g for g in random_effects.keys() if len(random_effects[g]) > 0
        ]

        self.constants = constants if constants is not None else []
        param_effects = ModelParameterEffects(fixed_effects, random_effects, constants)

        # Create mapping of data (dependent and independent) variables to their values for this
        # model.
        self._init_data_map(param_effects, weight_decay, seed)

        # Initialize omega priors.
        self._init_omega_priors(omega_amp)

        # Separate the data elements with draws into there own collection, ``data_draw``.
        self._init_draw_data()

        # Create mapping of initial parameter values for this model.
        self._init_param_map(param_effects)

        # Create an instance variable that will be the xarray dataset containing the optimized
        # model parameter coefficients -- after the model-fitting process.
        self.coefficients: Optional[xr.Dataset] = None

        # Create an instance variable that will be the xarray dataarray containing the model
        # predictions for the all years from the first forecast year to the last forecast year.
        self.predictions: Optional[xr.DataArray] = None

        # Set the seed for PyMB's numpy use as well.
        self.rng = np.random.default_rng(seed=seed)

    def fit(self) -> xr.Dataset:
        """Optimize and fit the coefficients of the model parameters.

        Produces a dataset containing the coefficients for the fixed and random effects fit in
        the model. There is one variable per effect type (e.g. ``gamma_location_age``), and
        a covariate dimension specifying which covariate is associated with which coefficients.
        All covariate/effect-type pairs that were not fit (e.g., ``gamma_age`` and ``sdi``, if
        ``sdi`` was included as a global fixed effect) contain ``NaN`` values.

        Returns:
            xr.Dataset: A dataset containing the coefficients for the optimized model.

        Raises:
            ConvergenceError: If the model cannot converge, i.e., the convergence value is
                nonzero.
        """
        self._optimize()

        # Convert fit parameters coefficients into xarray format use to keep track of parameter
        # order.
        demog_coords = dict(
            location_id=self.data[GKModelConstants.LOCATION_PARAM],
            year_id=self.data[GKModelConstants.YEAR_PARAM],
            age_group_id=self.data[GKModelConstants.AGE_PARAM],
        )

        # Get the names of the fixed effects (e.g. ``"beta_global_raw"``).
        fixed_effects = [
            f + "_raw" for f in self.fixed_names if f + "_raw" in list(self.parameters.keys())
        ]

        # Get the names of the random effects.
        random_effects = [r for r in self.random_names if r in list(self.parameters.keys())]

        # Get the draws of the fixed effects and the random effect parameters.
        # **NOTE:** This does does not involve covariate values.
        fixed_effect_arrays = [
            transform_parameter_array(self.draws(f), self.data[f.replace("raw", "constraint")])
            for f in fixed_effects
        ]
        random_effect_arrays = [self.draws(k) for k in random_effects]

        # Get names of fixed effect covariates (e.g., the ``"intercept"`` and ``"sdi"``
        # covariates for the ``"beta_global" parameter``).
        fixed_cov_names = [self.data[k.replace("_raw", "")] for k in fixed_effects]
        # Get the names for the random effect covariate names in the same way.
        random_cov_names = [self.data[k] for k in random_effects]

        # If region or super region are part of the effects structure, then copy it out so the
        # param exists for every location.
        fixed_effect_arrays = [
            (
                region_to_location(
                    fixed_effect_arrays[i], self.data[GKModelConstants.REGION_PARAM]
                )
                if f"beta_{GKModelConstants.REGION_PARAM}" in fixed_effects[i]
                else fixed_effect_arrays[i]
            )
            for i in range(len(fixed_effect_arrays))
        ]
        fixed_effect_arrays = [
            (
                region_to_location(
                    fixed_effect_arrays[i], self.data[GKModelConstants.SUPER_REGION_PARAM]
                )
                if GKModelConstants.SUPER_REGION_PARAM in fixed_effects[i]
                else fixed_effect_arrays[i]
            )
            for i in range(len(fixed_effect_arrays))
        ]
        random_effect_arrays = [
            (
                region_to_location(
                    random_effect_arrays[i], self.data[GKModelConstants.REGION_PARAM]
                )
                if f"gamma_{GKModelConstants.REGION_PARAM}" in random_effects[i]
                else random_effect_arrays[i]
            )
            for i in range(len(random_effect_arrays))
        ]
        random_effect_arrays = [
            (
                region_to_location(
                    random_effect_arrays[i], self.data[GKModelConstants.SUPER_REGION_PARAM]
                )
                if GKModelConstants.SUPER_REGION_PARAM in random_effects[i]
                else random_effect_arrays[i]
            )
            for i in range(len(random_effect_arrays))
        ]

        # Convert the coefficient arrays to xarray.
        fixed_effect_arrays = [
            np_to_xr(r, demog_coords, fixed_cov_names[i])
            for i, r in enumerate(fixed_effect_arrays)
        ]
        random_effect_arrays = [
            np_to_xr(r, demog_coords, random_cov_names[i])
            for i, r in enumerate(random_effect_arrays)
        ]

        # Put the random and fixed effect parameter coefficients into one xarray.Dataset.
        additive_params = dict()
        for i, k in enumerate(fixed_effects):
            additive_params[k.replace("_raw", "")] = fixed_effect_arrays[i]

        for i, k in enumerate(random_effects):
            additive_params[k] = random_effect_arrays[i]
        self.coefficients = xr.Dataset(additive_params)

        return self.coefficients

    def predict(self) -> xr.DataArray:
        """Generate predictions for future years from the optimized model fit.

        Generate predictions from the year ``years.past_start`` up through the year
        ``years.forecast_end`` for input covariates using the fit_params. Variables with a
        coefficient defined to be 1 (like scalars), specified in constant_vars, are added on.
        Currently there is no intercept_shift option

        Returns:
            xr.DataArray: data array containing the predictions from the input covariates and
                fit parameters.
        """
        # Convert fit_params to an array in order to sum more easily
        fit_params = self.coefficients.to_array().fillna(0)

        # loop through covariates and add on their contribution to the total
        contributions = [
            (self.dataset[cov] * fit_params.sel(cov=cov, drop=True)).sum("variable")
            for cov in fit_params.cov.values
        ]
        pred_ds = sum(contributions)

        # Add on the data from the constant variables.
        for var in self.constants:
            pred_ds = pred_ds + self.dataset[var]

        self.predictions = pred_ds.sel(year_id=self.years.years)
        return self.predictions

    def _init_model(self) -> None:
        """Configure model and create the DLL for C++ extensions."""
        model: Optional[str] = None
        for package_dir in site.getsitepackages():
            so_file = Path(package_dir) / f"{self.name}.so"
            if so_file.is_file():
                model = str(so_file)
        if model is None:
            err_msg = f"No shared object library file found. Expecting ``{self.name}.so``"
            logger.error(err_msg)
            raise EnvironmentError(err_msg)
        self.load_model(model)

    def _init_dataset(
        self, dataset: Union[xr.DataArray, xr.Dataset], y: Optional[str]
    ) -> None:
        """Initialize, validate, and prepare input dataset."""
        if isinstance(dataset, xr.DataArray):
            logger.debug("DataArray given, converting to Dataset")
            dataset = xr.Dataset({y: dataset})

        self.y = y if y is not None else ModelConstants.DEFAULT_DEPENDENT_VAR
        if self.y not in list(dataset.data_vars.keys()):
            err_msg = "``y`` was not found in the input dataset"
            logger.error(err_msg)
            raise ValueError(err_msg)

        self.dataset = dataset

    def _init_omega_priors(self, omega_amp: Union[Dict[str, float]]) -> None:
        """Initialize Bayesian Omega Priors."""
        if omega_amp != 0:
            omega_amp_map = omega_translate(omega_amp)
            self.data.update(
                omega_prior_vectors(
                    self.data[f"y_{GKModelConstants.OmegaLevels.U}"],
                    self.data[GKModelConstants.HOLDOUT_START_PARAM],
                    level=GKModelConstants.OmegaLevels.U,
                    omega_amp_map=omega_amp_map,
                )
            )
            self.data.update(
                omega_prior_vectors(
                    self.data[f"y_{GKModelConstants.OmegaLevels.T}"],
                    self.data[GKModelConstants.HOLDOUT_START_PARAM],
                    level=GKModelConstants.OmegaLevels.T,
                    omega_amp_map=omega_amp_map,
                )
            )
        else:
            self.data.update({k: 0 for k in GKModelConstants.OMEGA_PRIORS})

    def _init_data_map(
        self,
        param_effects: ModelParameterEffects,
        weight_decay: float,
        seed: Optional[int],
    ) -> None:
        """Create mapping of data variables to their values for this model.

        Args:
            param_effects: fixed and random effects to be added to the data.
            weight_decay: weight decay value to be added to the data.
            seed: A seed for the GK Random number generator
        """
        self.data = dict()
        self.data["mean_adjust"] = int(0)
        self.data["testing_prints"] = int(0)
        self.data[GKModelConstants.WEIGHT_DECAY_PARAM] = weight_decay
        self.data[GKModelConstants.LOCATION_PARAM] = self.dataset.location_id.values
        self.data[GKModelConstants.AGE_PARAM] = self.dataset.age_group_id.values
        self.data[GKModelConstants.YEAR_PARAM] = self.dataset.year_id.values
        self.data[GKModelConstants.HOLDOUT_START_PARAM] = np.where(
            self.data[GKModelConstants.YEAR_PARAM] == self.years.past_end
        )[0][0]
        self.data["covariates2"] = []
        self.data["beta2_constraint"] = np.array([])

        final_array = data_var_merge(self.dataset)
        final_array_col = data_var_merge(self.dataset, collapse=True)
        param_effects.extract_param_data(self.dataset, final_array, final_array_col)
        self.data["X2"] = np.zeros((0, 0, 0, 0))
        self.data["X2_draw"] = np.zeros((0, 0, 0, 0))
        self.data.update(param_effects.data)
        self.fixed_names = param_effects.fixed_effect_names
        self.random_names = param_effects.random_effect_names
        self.data[f"y_{GKModelConstants.OmegaLevels.U}"] = final_array_col.loc[
            dict(cov=self.y)
        ].values
        self.data[f"y_{GKModelConstants.OmegaLevels.T}"] = final_array_col.loc[
            dict(cov=self.y)
        ].values
        self.data[f"y_{GKModelConstants.OmegaLevels.T}_draws"] = final_array.loc[
            dict(cov=self.y)
        ]
        # Get region mappings.
        self.data[GKModelConstants.REGION_PARAM] = np.repeat(
            0, len(self.data[GKModelConstants.LOCATION_PARAM])
        )
        self.data[GKModelConstants.SUPER_REGION_PARAM] = np.repeat(
            0, len(self.data[GKModelConstants.LOCATION_PARAM])
        )
        if DimensionConstants.REGION_ID in list(self.dataset.data_vars.keys()):
            self.data[GKModelConstants.REGION_PARAM] = pd.factorize(
                self.dataset.region_id.values
            )[0]
        if DimensionConstants.SUPER_REGION_ID in list(self.dataset.data_vars.keys()):
            self.data[GKModelConstants.SUPER_REGION_PARAM] = pd.factorize(
                self.dataset.super_region_id.values
            )[0]

        self.data["has_risks"] = np.zeros_like(self.data["age"], dtype=int)

        if seed is None:
            self.data["set_seed"] = 0
            self.data["seed"] = 0
        else:
            self.data["set_seed"] = 1
            self.data["seed"] = seed

    def _init_draw_data(self) -> None:
        """Separate the data elements that have draws into their own collection."""
        self.data_draw = dict()
        for k in list(self.data.keys()):
            if k.endswith("draw") or k.endswith("draws"):
                self.data_draw[k] = self.data.pop(k)

    def _init_param_map(self, param_effects: ModelParameterEffects) -> None:
        """Create mapping of initial parameter values for this model."""
        self.init = dict()
        self.init.update(
            {
                "log_age_sigma_pi": np.array([]),
                "log_location_sigma_pi": np.array([]),
                "logit_rho": np.array([]),
                "pi": np.zeros((0, 0, 0)),
            }
        )
        self.init.update(param_effects.init)
        _, _, _, k2 = self.data[
            "X2"
        ].shape
        r = np.sum(self.data["has_risks"])
        self.init[f"log_sigma_{GKModelConstants.OmegaLevels.U}"] = np.zeros(0 if r == 0 else 1)
        self.init[f"log_sigma_{GKModelConstants.OmegaLevels.T}"] = 0
        self.init[f"log_zeta_{GKModelConstants.OmegaLevels.D}"] = np.zeros(r)
        self.init["beta2_raw"] = np.zeros((1, r, 1, k2))

    def _optimize(
        self,
        opt_fun: str = PyMBConstants.DEFAULT_OPT_FUNC,
        method: str = PyMBConstants.DEFAULT_OPT_METHOD,
    ) -> None:
        """Optimize the model and store results in ``TMB_Model.TMB.fit``.

        Optimize the model and store results in ``TMB_Model.TMB.fit`` using the PyMP
        ``optimize``.

        Args:
            opt_fun: (str):  the R optimization function to use (e.g. ``'nlminb'`` or
                ``'optim'``). Defaults to ``'nlminb'``
            method (str): Method to use for optimization. Defaults to ``'L-BGFS-B'``.

        Raises:
            ConvergenceError: If the model cannot converge, i.e., the convergence value is
                nonzero.
        """
        super().optimize(
            opt_fun=opt_fun,
            method=method,
            draws=self.num_of_draws,
            random_effects=self.random_effect_names,
        )
        if self.convergence != 0:
            err_msg = "The model could not converge"
            logger.error(err_msg, bindings=dict(convergence=self.convergence))
            raise ConvergenceError(err_msg)
