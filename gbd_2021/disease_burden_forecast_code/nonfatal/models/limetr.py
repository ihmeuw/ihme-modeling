"""This module provides an FHS interface to the LimeTr model."""

import itertools
from collections import namedtuple
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import xarray as xr
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_database_interface.lib.constants import DimensionConstants
from fhs_lib_file_interface.lib import xarray_wrapper
from fhs_lib_file_interface.lib.file_system_manager import FileSystemManager
from fhs_lib_file_interface.lib.version_metadata import FHSDirSpec
from fhs_lib_year_range_manager.lib.year_range import YearRange
from flme import LME
from tiny_structured_logger.lib.fhs_logging import get_logger

from fhs_lib_model.lib.constants import ModelConstants
from fhs_lib_model.lib.model_protocol import ModelProtocol
from fhs_lib_model.lib.utils import assert_covariate_coords, mean_of_draw

logger = get_logger()

RandomEffect = namedtuple("RandomEffect", "dims, prior_value")


class LimeTr(ModelProtocol):
    """Instances of this class represent a LimeTr model.

    Can be it and used for predicting future esimates.
    """

    def __init__(
        self,
        past_data: xr.DataArray,
        years: YearRange,
        draws: Optional[int],
        covariate_data: Optional[List[xr.DataArray]] = None,
        fixed_effects: Optional[Dict] = None,
        fixed_intercept: Optional[str] = None,
        random_effects: Optional[Dict] = None,
        indicators: Optional[Dict] = None,
        seed: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        """Creates a new ``LimeTr`` model instance.

        Pre-conditions:
        ===============
        * All given ``xr.DataArray``s must have dimensions with at least 2
          coordinates. This applies for covariates and the dependent variable.
        * There must be at least one fixed effect (counting indicator as a
          fixed effect) or random effect in the model.
        * Effects (fixed or random) that are on covariates must have exactly
          the same name as the covariate they are associated with. e.g. you
          can't have a random effect called ``haq_age``.
        * Cannot have both global intercept be True and indicators.
        * Cannot have fixed effects if ``covariate_data`` is ``None``.

        For covariates:
        ---------------
        * The dimensions within each covariate are also dimensions of the
          dependent variable (i.e. ``dimensions_order_list``).
        * For the dimensions shared between each covariate and the dependent
          variable, the coordinates should be the same.
        * Each covariate's dataarray should have a ``scenario``, with at least
          one coord ``scenario=0``.
        * All covariates should have the same scenario coordinates --
          covariates that don't have actual scenarios should have their
          reference scenario broadcast out to all the expected scenarios by
          this point.

        For random effects:
        -------------------
        * Random effects have at least one shared dimension.
        * Random effects are mapped to a non-empty list of dimensions.
        * The dimensions within each random effect are also dimensions of
          the dependent variable (i.e. ``dimensions_order_list``).

        For indicators
        --------------
        * Indicator are mapped to a non-empty list of dimensions.
        * The dimensions within each indicator are also dimensions of the
          dependent variable (i.e. ``dimensions_order_list``).

        There should be at least one effect that is specific to a covariate.

        Args:
            past_data (xr.DataArray): Past data for dependent variable being forecasted. Only
                mean-of-draw-level past data is used, so if draws are given the
                mean will be taken.
            covariate_data (list[xr.DataArray] | None, optional): Past and forecast data for
                each covariate (i.e. independent variable). Each individual covariate
                dataarray must be named with the stage as is defined in the FHS file system.
                Only mean-of-draw-level past data is used, so if draws are given the
                mean will be taken.
            years (YearRange): forecasting timeseries
            draws (Optional[int]): Number of draws to generate for the betas (and thus the
                predictions) during the fitting.
            fixed_effects (Optional[Dict]): Dict of covariates to have their
                corresponding coefficients estimated and bounded by the given list.
                e.g.: {"haq": [0, float('inf')], "edu": [-float('inf'), float('inf')]}
            fixed_intercept (str | None, optional): To restrict the fixed intercept to be
                positive or negative, pass "positive" or "negative", respectively.
                "unestricted" says to estimate a fixed effect intercept that is not restricted
                to positive or negative. If ``None`` then no fixed intercept is estimated.
                Currently all of the strings get converted to unrestricted.
            indicators (Dict | None, optional): A dictionary mapping indicators to the
                dimensions that they are indicators on.
                e.g.: {"ind_age_sex": ["age_group_id", "sex_id"], "ind_loc": ["location_id"]}
            kwargs (Any): Unused additional keyword arguments
            random_effects (Dict | None, optional): A dictionary mapping covariates to the
                dimensions that their random slopes will be estimated for and the standard
                deviation of the gaussian prior on their variance.
                of the form {covariate: RandomEffect(list[dimension], std)...}
                Any key that is not in covariate_data is assumed to be an
                intercept.
                The std float represents the value of the gaussian prior on
                random effects variance. None means no prior.
                e.g.::
                    {"haq": RandomEffect(["location_id", "age_group_id"], None),
                    "education": RandomEffect(["location_id"], 3)}
            seed (Optional[int]): an optional seed to set for reproducibility.

        Raises:
            ValueError: Conditions
                * If fixed effects are non-empty but ``covariate_data`` is ``None``, or If
                ``fixed_effects``, ``indicators``, and ``random_effects`` are all ``None``/
                empty
                * If a given random effect, indicator, or covariate has 1 or more dimensions
                not included in ``self.dimensions_order_list``, i.e. those of the dependent
                variable
                * If there are no shared dims among all of the random effects, while random
                effects do actually exist * If global intercept is True **and** indicators are
                non-empty
                * If any dimension shared between any covariate and the dependent variable
                does not have the same coordinates on the dataarray each.
        """
        if not fixed_effects and not random_effects and not indicators:
            err_msg = (
                "`fixed_effects`, `indicators`, and `random_effects` are all " "`None`/empty."
            )
            logger.error(err_msg)
            raise ValueError(err_msg)
        elif fixed_intercept and indicators:
            err_msg = "Cannot have both global intercept be True and indicators."
            logger.error(err_msg)
            raise ValueError(err_msg)
        elif fixed_effects and not covariate_data:
            err_msg = "Cannot have fixed effects if `covariate_data` is `None`."
            logger.error(err_msg)
            raise ValueError(err_msg)

        self.seed = seed
        self._orig_past_data = past_data.copy()
        self._orig_random_effects = self._make_dims_conform(
            self._orig_past_data, random_effects, ModelConstants.ParamType.RANDOM
        )
        self._orig_fixed_effects = fixed_effects or dict()
        self._orig_indicators = self._make_dims_conform(
            self._orig_past_data, indicators, ModelConstants.ParamType.INDICATOR
        )

        needed_dims = [dim for dim in past_data.dims if dim != DimensionConstants.DRAW]
        self.dimensions_order_list, self.n_grouping_dims = self._find_random_shared_dims(
            self._orig_random_effects, needed_dims
        )
        self.years = years
        self.draws = draws
        self.past_data = self._convert_xarray_to_numpy(
            self._orig_past_data, self.dimensions_order_list
        )
        self._orig_covariate_data = (
            dict() if not covariate_data else {cov.name: cov.copy() for cov in covariate_data}
        )
        self.covariate_data = self._convert_covariates(
            list(self._orig_covariate_data.values()),
            self.dimensions_order_list,
            self._orig_past_data,
            self.years,
        )
        self.fixed_effects = self._convert_fixed_effects(self._orig_fixed_effects)
        self.fixed_intercept = self._convert_fixed_intercept(fixed_intercept)
        self.random_effects = self._convert_random_effects(
            self._orig_random_effects, self.dimensions_order_list
        )

        self.indicators = self._convert_indicators(
            self._orig_indicators, self.dimensions_order_list
        )

        LimeTr._assert_covariate_params(
            self._orig_fixed_effects,
            self._orig_random_effects,
            list(self._orig_covariate_data.values()),
        )

        ordered_dim_counts = self._get_dim_counts(
            self._orig_past_data, self.dimensions_order_list
        )

        self.model_instance = LME(
            dimensions=ordered_dim_counts,
            n_grouping_dims=self.n_grouping_dims,
            y=self.past_data,
            covariates=self.covariate_data,
            indicators=self.indicators,
            global_effects_names=self.fixed_effects,
            global_intercept=self.fixed_intercept,
            random_effects=self.random_effects,
        )

    def fit(self) -> xr.Dataset:
        """Fits the model and then returns the draws of the coefficients.

        Returns:
            xr.Dataset: the fit coefficient draws or means
        """
        self.model_instance.optimize(
            trim_percentage=0.0,
            inner_max_iter=1000,
            inner_tol=1e-5,
            outer_max_iter=1,
            outer_step_size=1.0,
            outer_tol=1e-6,
            share_obs_std=True,
            random_seed=self.seed,
        )
        if self.fixed_effects or self.indicators:
            self.model_instance.postVarGlobal()

        if self.random_effects:
            self.model_instance.postVarRandom()

        # mean random effect estimates
        coef_means = self._get_coefficient_means()

        # get covariance
        covariance_dict = {}
        if self.fixed_effects or self.indicators:
            fixed_covariance = self.model_instance.var_beta
            covariance_dict.update({ModelConstants.ParamType.FIXED: fixed_covariance})
        if self.random_effects:
            random_covariance = self.model_instance.var_u
            covariance_dict.update({ModelConstants.ParamType.RANDOM: random_covariance})
        self.posterior_cov_dict = np.array(covariance_dict)

        if self.draws:
            coef_draws = self._generate_coefficient_draws()
            return coef_draws
        else:
            return coef_means

    def predict(self) -> xr.DataArray:
        """Apply the draws of coefficients to obtain draws of forecasted ratio or indicator."""
        self.predictions = 0
        if self.draws:
            for data_var in self.coef_draws_ds.data_vars:
                self._apply_coefficients(data_var)
        else:
            for data_var in self.coef_mean_ds.data_vars:
                self._apply_coefficients(data_var)

        # expand dims other than scenario and year in case covariates don't
        # include all dims.
        self.predictions = expand_dimensions(self.predictions, **self._orig_past_data.coords)
        return self.predictions

    def save_coefficients(self, output_dir: FHSDirSpec, entity: str) -> None:
        """Saves model coefficients and posterior variance.

        Coefficients are saved in a xr.DataSet, while the posterior variance is saved directly
        as a dict containing fixed and random variance objects from LimeTr
        (var_u and var_beta).

        Args:
            output_dir (Path): the output directory to save coefficients in
            entity (str): the name of the entity to save. This will become the filename.
        """
        fs = FileSystemManager.get_file_system()

        # Save draw-level coefficients.
        draw_filespec = output_dir.append_sub_path(("coefficients",)).file(f"{entity}.nc")
        xarray_wrapper.save_xr_scenario(
            xr_obj=self.coef_mean_ds,
            file_spec=draw_filespec,
            metric="rate",
            space="identity",
        )

        # Save covariance matrix of coefficients.
        variance_dir = output_dir.append_sub_path(("variance",))
        fs.makedirs(variance_dir)

        variance_file = variance_dir.file(f"{entity}.npy")

        np.save(variance_file.data_path(), self.posterior_cov_dict)
        try:
            fs.chmod(variance_file, 0o775)
        except PermissionError:
            logger.warning(
                f"Could not set group-writable permissions on {variance_file}. "
                "Please set permissions manually."
            )

    def _apply_coefficients(self, data_var: str) -> None:
        """Apply the fixed-effect, random-effect, or indicator coefficients to the predictions.

        Fixed effects and random effects will either be associated with a
        covariate, meaning the coefficient is multiplied by covariate data and
        then added to the predictions, or it is an intercept and should simply
        be added to the predictions. Indicators will never have covariate
        names, so will always just be added to the predictions.

        Args:
            data_var (str): the name of the data variable to apply coefficients to
        """
        if self.draws:
            dataset_to_apply = self.coef_draws_ds
        else:
            dataset_to_apply = self.coef_mean_ds

        covariate_names = list(self.covariate_data.keys())
        if data_var == ModelConstants.ParamType.FIXED:
            param_names = dataset_to_apply[data_var]["parameter"].values
        elif LimeTr._is_random_effect(data_var, covariate_names):
            param_names = [data_var[len(ModelConstants.RANDOM_PREFIX) :]]
        else:  # Assume that coefficient is a random intercept or indicator
            param_names = ["not_covariate"]

        for param_name in param_names:
            if "parameter" in dataset_to_apply[data_var].dims:
                coef = dataset_to_apply[data_var].sel(parameter=param_name, drop=True)
            else:
                coef = dataset_to_apply[data_var]

            if param_name in list(self.covariate_data.keys()):
                # Since the name of the parameter appears in the list of
                # covariates, we apply its coefficient to the respective
                # covariate data.
                if self.draws:
                    self.predictions = self.predictions + (
                        self._orig_covariate_data[param_name] * coef
                    )
                else:
                    self.predictions = self.predictions + mean_of_draw(
                        self._orig_covariate_data[param_name] * coef
                    )

            else:  # Assume that coefficient is an intercept or indicator
                self.predictions = self.predictions + coef

    def _get_coefficient_means(self) -> xr.Dataset:
        """Extract coefficent mean estimates from LimeTr and combine them into xr.Dataset."""
        mean_feffect_da_list = self._get_fixed_effect_means()
        mean_reffect_da_list = self._get_random_effect_means()
        mean_indicator_da_list = self._get_indicator_means()

        self.coef_mean_ds = xr.merge(
            mean_feffect_da_list + mean_reffect_da_list + mean_indicator_da_list
        )

        return self.coef_mean_ds

    def _get_fixed_effect_means(self) -> List[xr.DataArray]:
        mean_feffect_da_list = []
        if self.fixed_intercept:
            feffects = ["global_intercept"] + list(self.fixed_effects.keys())
        else:
            feffects = list(self.fixed_effects.keys())
        for i, feffect_name in enumerate(feffects):
            mean_fixed_da = xr.DataArray(
                np.array([self.model_instance.beta_soln[i]]),
                dims=("parameter"),
                name=ModelConstants.ParamType.FIXED,
                coords={"parameter": [feffect_name]},
            )
            mean_feffect_da_list.append(mean_fixed_da)
        return mean_feffect_da_list

    def _get_random_effect_means(self) -> List[xr.DataArray]:
        mean_reffect_da_list = []
        column_index = 0
        for reffect_name in self.random_effects.keys():
            random_name = ModelConstants.RANDOM_PREFIX + reffect_name
            random_dims = self._orig_random_effects[reffect_name].dims
            ordered_random_dims = [
                dim for dim in self.dimensions_order_list if dim in random_dims
            ]
            coords_dict = {
                dim: list(self._orig_past_data[dim].values) for dim in ordered_random_dims
            }
            grouping_dims = self.dimensions_order_list[0 : self.n_grouping_dims]
            len_list = [len(coords_dict[grouping_dim]) for grouping_dim in grouping_dims]
            non_grouping_dims = sorted(list(set(coords_dict.keys()) - set(grouping_dims)))
            coord_combos = _coord_combinations(coords_dict, non_grouping_dims)
            if non_grouping_dims:
                # loop through all coord combinations from right to left, i.e.
                # if non_grouping_dims was ["age", "sex"] then it would loop
                # through with age 1 sex 1, age 1 sex 2, age 2 sex 1, etc.
                combo_ds_list = []
                for combo in coord_combos:
                    effect = []
                    for row_index in range(0, np.prod(len_list)):
                        effect.append(self.model_instance.u_soln[row_index][column_index])
                    reshape_list = len_list + [int(bool(i)) for i in non_grouping_dims]
                    combo_np = np.array(effect).reshape(reshape_list)
                    combo_ds = xr.DataArray(
                        combo_np, dims=list(combo.keys()), name=random_name, coords=combo
                    ).to_dataset()
                    combo_ds_list.append(combo_ds)
                    column_index = column_index + 1
                mean_random_da = xr.combine_by_coords(combo_ds_list)[random_name]
            else:
                effect = []
                for row_index in range(0, np.prod(len_list)):
                    effect.append(self.model_instance.u_soln[row_index][column_index])
                effect_np = np.array(effect).reshape(len_list)
                mean_random_da = xr.DataArray(
                    effect_np,
                    dims=list(coords_dict.keys()),
                    name=random_name,
                    coords=coords_dict,
                )
                column_index = column_index + 1

            mean_reffect_da_list.append(mean_random_da)

        return mean_reffect_da_list

    def _get_indicator_means(self) -> List[xr.DataArray]:
        mean_indicator_da_list = []
        for i, indicator_name in enumerate(self.indicators.keys()):
            if i == 0:
                start_value = len(self.fixed_effects.keys())
            indicator_dims = self._orig_indicators[indicator_name]
            ordered_indicator_dims = [
                dim for dim in self.dimensions_order_list if dim in indicator_dims
            ]
            coords_dict = {
                dim: list(self._orig_past_data[dim].values) for dim in ordered_indicator_dims
            }
            num_values = np.prod([len(dim) for dim in coords_dict.values()])
            end_value = start_value + num_values
            indicator_vals_list = self.model_instance.beta_soln.tolist()[start_value:end_value]
            len_list = [len(val) for val in coords_dict.values()]

            mean_indicator_da = xr.DataArray(
                np.array(indicator_vals_list).reshape(len_list),
                dims=list(coords_dict.keys()),
                name=indicator_name,
                coords=coords_dict,
            )
            mean_indicator_da_list.append(mean_indicator_da)

        return mean_indicator_da_list

    def _generate_coefficient_draws(self) -> xr.Dataset:
        """Utilize the LimeTr wrapper `outputDraws` function to generate draws of coefficients.

        Using `np.random.multivariate_normal` and convert the results to a ``xr.DataSet``.

        Returns:
            xr.Dataset: the coefficient draws dataset
        """
        (fixed_samples, indicator_samples, random_samples) = self.model_instance.outputDraws(
            n_draws=self.draws, by_type=True, combine_cov=True
        )

        coef_da_list = []
        if fixed_samples:
            fixed_da = xr.DataArray(
                fixed_samples.array,
                dims=("parameter", DimensionConstants.DRAW),
                name=ModelConstants.ParamType.FIXED,
                coords={
                    "parameter": fixed_samples.names,
                    DimensionConstants.DRAW: range(self.draws),
                },
            )
            coef_da_list.append(fixed_da)

        for random_index, random_sample in enumerate(random_samples):
            random_name = ModelConstants.RANDOM_PREFIX + random_sample.name
            param_name = random_sample.name
            random_dims = self._orig_random_effects[param_name].dims
            ordered_random_dims = [
                dim for dim in self.dimensions_order_list if dim in random_dims
            ]
            coords_dict = {
                dim: list(self._orig_past_data[dim].values) for dim in ordered_random_dims
            }
            coords_dict.update({DimensionConstants.DRAW: range(self.draws)})

            random_da = xr.DataArray(
                random_sample.array,
                dims=list(coords_dict.keys()),
                name=random_name,
                coords=coords_dict,
            )
            coef_da_list.append(random_da)

        for indicator_index, indicator_sample in enumerate(indicator_samples):
            param_name = indicator_sample.name
            indicator_dims = self._orig_indicators[param_name]
            ordered_indicator_dims = [
                dim for dim in self.dimensions_order_list if dim in indicator_dims
            ]

            coords_dict = {
                dim: list(self._orig_past_data[dim].values) for dim in ordered_indicator_dims
            }
            coords_dict.update({DimensionConstants.DRAW: range(self.draws)})

            indicator_da = xr.DataArray(
                indicator_sample.array,
                dims=list(coords_dict.keys()),
                name=param_name,
                coords=coords_dict,
            )
            coef_da_list.append(indicator_da)

        self.coef_draws_ds = xr.merge(coef_da_list)

        return self.coef_draws_ds

    @staticmethod
    def _is_random_effect(effect_name: str, covariate_names: List[str]) -> bool:
        """Checks if the effect is a random effect, excluding intercepts."""
        if not effect_name.startswith(ModelConstants.RANDOM_PREFIX):
            return False
        return effect_name[len(ModelConstants.RANDOM_PREFIX) :] in covariate_names

    @staticmethod
    def _get_dim_counts(data: xr.DataArray, dimensions_order_list: List[str]) -> List[int]:
        """The lengths of all the dims on the dependent var that are relevant to fitting.

        Args:
            data (xr.DataArray): Initial/unchanged past data of dependent variable
            dimensions_order_list (list[str]): An ordered list of dimensions needed by y with
                the shared grouping dimensions at the front of the list

        Returns:
            list[int]: The lengths of each dimension where the dims ordered relative
                to ``dimensions_order_list``.
        """
        dim_counts = [len(data[dim].values) for dim in dimensions_order_list]

        return dim_counts

    @staticmethod
    def _find_random_shared_dims(
        random_effects: Optional[Dict], needed_dims: List[str]
    ) -> Tuple[List[str], int]:
        """Determine the order that dims will go in and assign the number of grouping dims.

        Args:
            needed_dims (list[str]):
                Dimensions that are present in the y data and must be accounted
                for in the ordered list of dimensions.
            random_effects (Dict | None, optional):
                A dictionary mapping covariates to the dimensions that their
                random slopes will be estimated for and the standard deviation
                of the gaussian prior on their variance.
                of the form {covariate: (list[dimension], std)...}
                Any key that is not in covariate_data is assumed to be an
                intercept.
                The std float represents the value of the gaussian prior on
                random effects variance. None means no prior.
                e.g.::

                    {"haq": (["location_id", "age_group_id"], None),
                     "education": (["location_id"], 3)}

        Returns:
            Tuple[List[str], int]: dimensions_order_list, an ordered list of dimensions needed
                by y with the shared grouping dimensions at the front of the list; and
                n_grouping_dims, the number of dimensions to group by for optimization in
                LimeTr. **NOTE:** If there are random effects, then the grouping dims
                and the non-grouping dims will be sorted alphabetically
                relative to their respective sets.

        Raises:
            ValueError: If there are no shared dims among all of the random effects,
                while random effects do actually exist, or if a given random effect has 1 or
                more dimensions not included in ``self.dimensions_order_list``, i.e. those of
                the dependent variable.
        """
        if not random_effects:
            # There are *no* random effects, so there are no dims shared among
            # all of the random effects. Skip to the end.
            sorted_dims_order_list = needed_dims
            n_grouping_dims = 0
        else:
            # Build list of all random effect dimensions
            all_random_effect_dims = [
                reffect.dims for reffect in list(random_effects.values())
            ]

            # Find shared dimensions
            shared_dims = list(set.intersection(*map(set, all_random_effect_dims)))

            # If there are no shared dims among all of the random effects,
            # while random effects do actually exist, then LimeTr cannot run.
            msg = "There are no shared dimensions!"
            if not shared_dims:
                logger.error(msg)
                raise ValueError(msg)

            # Append the rest of the dims to shared_dims
            dimensions_order_list = deepcopy(shared_dims)
            for dim in needed_dims:
                if dim not in dimensions_order_list:
                    dimensions_order_list.append(dim)

            n_grouping_dims = len(shared_dims)

            # The grouping dims and the non-grouping dims will be sorted
            # alphabetically relative to their respective sets.
            sorted_dims_order_list = sorted(dimensions_order_list[:n_grouping_dims]) + sorted(
                dimensions_order_list[n_grouping_dims:]
            )

            LimeTr._assert_param_dims(
                random_effects, needed_dims, param_type=ModelConstants.ParamType.RANDOM
            )
            if set(needed_dims) != set(sorted_dims_order_list):
                raise ValueError(
                    f"The set of `needed_dims` [{needed_dims}] does not match the set of "
                    f"`sorted_dims_order_list [{sorted_dims_order_list}]."
                )

        return sorted_dims_order_list, n_grouping_dims

    @staticmethod
    def _convert_fixed_effects(fixed_effects: Dict) -> Dict:
        """Converts dict of fixed effects to list of names required by LimeTr.

        Ignores effect restrictions for the moment until implemented by LimeTr API.

        Args:
            fixed_effects (Dict): Dict of covariates to have their
                corresponding coefficients estimated and bounded by the given list.
                e.g.::

                    {"haq": [0, float('inf')],
                     "edu": [-float('inf'), float('inf')]
                     }

        Returns:
            limetr_fixed_effects (Dict): A dict of covariate names that will be used
                as fixed effects
        """
        limetr_fixed_effects = fixed_effects

        return limetr_fixed_effects

    @staticmethod
    def _convert_fixed_intercept(fixed_intercept: Optional[str]) -> bool:
        """Converts fixed intercept str/bool to bool for LimeTr API.

        Args:
            fixed_intercept (str | None, optional): To restrict the fixed intercept to be
                positive or negative, pass "positive" or "negative", respectively.
                "unestricted" says to estimate a fixed effect intercept that is not restricted
                to positive or negative. If ``None`` then no fixed intercept is
                estimated. Currently all of the strings get converted to
                unrestricted.

        Returns:
            (bool): Whether or not to have a fixed intercept
        """
        if isinstance(fixed_intercept, str):
            return True

        return False

    @staticmethod
    def _convert_random_effects(
        random_effects: Dict, dimensions_order_list: List[str]
    ) -> Dict:
        """Converts dict of random effects to format required by LimeTr API.

        **NOTE:** This should be used after ``LimeTr._find_random_shared_dims``
        to ensure that the random effects have been validated.

        **pre-conditions:**
        * Random effects have at least one shared dimension.
        * Random effects are mapped to a non-empty list of dimensions.
        * The dimensions within each random effect are also dimensions of the
          dependent variable (i.e. ``dimensions_order_list``).

        Args:
            dimensions_order_list (list[str]): An ordered list of dimensions needed by y with
                the shared grouping dimensions at the front of the list
            random_effects (Dict): A dictionary mapping covariates to the
                dimensions that their random slopes will be estimated for and the standard
                deviation of the gaussian prior on their variance.
                of the form {covariate: (list[dimension], std)...}
                Any key that is not in covariate_data is assumed to be an
                intercept.
                The std float represents the value of the gaussian prior on
                random effects variance. None means no prior.
                e.g.::

                    {"haq": (["location_id", "age_group_id"], None),
                     "education": (["location_id"], 3)}

        Returns:
            limetr_random_effects (Dict): A dictionary where key is the name of the covariate
                or intercept and value is a tuple with a boolean list specifying
                dimensions to impose random effects on and the standard
                deviation of the gaussian prior on their variance. e.g.

                    {'haq': ([True, False, False, False], None),
                     'intercept_location': ([True, False, False, False], 3)}
        """
        LimeTr._assert_param_dims(
            random_effects,
            dimensions_order_list,
            param_type=ModelConstants.ParamType.RANDOM,
        )

        limetr_random_effects = {}
        for effect_name, effect_tuple in random_effects.items():
            effect_bool_dims = LimeTr._get_existing_dims(
                avail_dims=effect_tuple.dims,
                dimensions_order_list=dimensions_order_list,
                param_type=ModelConstants.ParamType.RANDOM,
                param_name=effect_name,
            )
            limetr_random_effects.update(
                {effect_name: (effect_bool_dims, effect_tuple.prior_value)}
            )

        return limetr_random_effects

    @staticmethod
    def _convert_covariates(
        covariates: List[xr.DataArray],
        dimensions_order_list: List[str],
        dep_var_da: xr.DataArray,
        years: YearRange,
    ) -> Dict:
        """Converts list of covariate dataarrays into 1D numpy arrays to match LimeTr API.

        We fit covariate coefficients at mean-of-draw, and reference-scenario
        level, but we apply the coefficients to the covariate data that
        includes draws and scenarios.

        **pre-conditions:**
        * The dimensions within each covariate are also dimensions of the
          dependent variable (i.e. ``dimensions_order_list``).
        * Each covariate's dataarray should have a ``scenario``, with at least
          one coord ``scenario=0``.
        * All covariates should have the same scenario coordinates --
          covariates that don't have actual scenarios should have their
          reference scenario broadcast out to all the expected scenarios by
          this point.

        Args:
            covariates (list[xr.DataArray]): Past and forecast data for each covariate (i.e.
                independent variable). Each individual covariate dataarray should be named
                with the stage as is defined in the FHS file system.
            dimensions_order_list (list[str]): The dimensions of the dependent variable, where
                they are ordered with the random-effect grouping dims first.
            dep_var_da (xr.DataArray): Past data for dependent variable being forecasted. Will
                be used to infer expected coordinates on each dimension of the
                covariate data.
            years (YearRange): FHS year range

        Returns:
            Dict: A dictionary where the key is the covariate name, value is a
                tuple of (1D np.array, order_bool_list_dimensions_of_cov)
                with the ordered_bool_list based on the dimensions order of the
                y data.
        """
        mean_ref_covariates = [
            (
                mean_of_draw(cov.sel(year_id=years.past_years).sel(scenario=0, drop=True))
                if "scenario" in cov.coords
                else mean_of_draw(cov.sel(year_id=years.past_years))
            )
            for cov in covariates
        ]

        cov_dim_dict = {cov.name: cov.dims for cov in mean_ref_covariates}
        LimeTr._assert_param_dims(cov_dim_dict, dimensions_order_list)
        assert_covariate_coords(mean_ref_covariates, dep_var_da)

        limetr_covariates = {}
        for cov_da in mean_ref_covariates:
            cov_name = cov_da.name
            ordered_cov_dims = [dim for dim in dimensions_order_list if dim in cov_da.dims]
            cov_array = LimeTr._convert_xarray_to_numpy(cov_da, ordered_cov_dims)

            cov_bool_dims = LimeTr._get_existing_dims(
                avail_dims=cov_da.dims,
                dimensions_order_list=dimensions_order_list,
                param_type=ModelConstants.ParamType.COVARIATE,
                param_name=cov_name,
            )
            limetr_covariates.update({cov_name: (cov_array, cov_bool_dims)})

        return limetr_covariates

    @staticmethod
    def _make_dims_conform(
        dep_var: xr.DataArray, params: Optional[Dict], param_type: str
    ) -> Dict:
        """Make indicators or random effects dims consistent with those of the dependent var.

        For example, we might try applying an age-sex indicator to a cause that
        only affects females, so by this point the dependent variable dataarray
        has no ``sex_id`` dim at all. Therefore, the indicator should be
        reduced to just an age-indicator. This should not be used for other
        things, such as covariates.

        Args:
            dep_var (xr.DataArray): Dependent variable being forecasted -- used here to infer
                expected dims.
            params (Dict | None): A dictionary mapping covariates to the dimensions that their
                random slopes will be estimated for and the standard deviation
                of the gaussian prior on their variance of the form
                {covariate: (list[dimension], std)...} for random effects or
                {param: list[dimension]} for indicators
            param_type (str): What the params are referring to for the purposes of printing
                warning messages about dimensions being dropped (e.g.
                "indicator".)

        Returns:
            Dict: Updated dict that is consistent with the dependent variable.
        """
        if not params:
            return dict()

        expected_dims = set(dep_var.dims)
        updated_params = {}
        for name, info in params.items():
            if param_type == ModelConstants.ParamType.RANDOM:
                dims = info.dims
            else:
                dims = info

            new_dims = sorted(expected_dims & set(dims))

            if param_type == ModelConstants.ParamType.RANDOM:
                new_info = RandomEffect(new_dims, info.prior_value)
            else:
                new_info = new_dims

            if new_dims and set(dims).issubset(expected_dims):
                updated_params.update({name: info})
            elif new_dims:
                updated_params.update({name: new_info})
                warn_msg = (
                    f"The {param_type} {name}, originally across "
                    f"{dims}, had to be reduced to a(n) {param_type} "
                    f"only across {new_dims}"
                )
                logger.warning(
                    warn_msg,
                    bindings=dict(
                        param_name=name,
                        param_type=param_type,
                        old_dims=dims,
                        new_dims=new_dims,
                    ),
                )
            else:
                warn_msg = (
                    f"The {param_type} {name}, originally across "
                    f"{dims} had to be dropped completely"
                )
                logger.warning(
                    warn_msg,
                    bindings=dict(
                        param_name=name,
                        param_type=param_type,
                        dropped_dims=dims,
                    ),
                )

        return updated_params

    @staticmethod
    def _convert_indicators(indicators: Dict, dimensions_order_list: List[str]) -> Dict:
        """Converts list of dims to create indicator variables.

        For dictionary to format required by LimeTr API.

        **pre-conditions:**
        * Indicator are mapped to a non-empty list of dimensions.
        * The dimensions within each indicator are also dimensions of the
          dependent variable (i.e. ``dimensions_order_list``).

        Args:
            dimensions_order_list (list[str]): The dimensions of the dependent variable, where
                they are ordered with the random-effect grouping dims first.
            indicators (Dict): A dictionary mapping indicators to the
                dimensions that they are indicators on. e.g.::
                    {"ind_age_sex": ["age_group_id", "sex_id"],
                     "ind_loc": ["location_id"]}

        Returns:
            Dict: A dictionary where the key is the indicator name and value is a
                boolean list specifying dimensions on which to use indicator.
        """
        LimeTr._assert_param_dims(indicators, dimensions_order_list)

        limetr_indicators = {}
        for indicator, indicator_dims in indicators.items():
            ind_bool_dims = LimeTr._get_existing_dims(
                avail_dims=indicator_dims,
                dimensions_order_list=dimensions_order_list,
                param_type=ModelConstants.ParamType.INDICATOR,
                param_name=indicator,
            )
            limetr_indicators.update({indicator: ind_bool_dims})

        return limetr_indicators

    @staticmethod
    def _convert_xarray_to_numpy(
        dataarray: xr.DataArray, dimensions_order_list: List[str]
    ) -> np.ndarray:
        """Converts multi-dimensional dataarray into 1-D numpy array.

        Transposes the array so that the dimensions are in the order that is
        expected for computation, before flattening it to be 1-D.

        Args:
            dataarray (xr.DataArray): An xarray DataArray to convert into a 1D numpy array
            dimensions_order_list (list[str]): The dimensions of the dependent variable, where
                they are ordered with the random-effect grouping dims first.

        Returns:
            numpy_data (np.ndarray): A 1D numpy array containing the transposed and reshaped
                data
        """
        return dataarray.transpose(*dimensions_order_list).values.flatten()

    @staticmethod
    def _get_existing_dims(
        avail_dims: List[str],
        dimensions_order_list: List[str],
        param_type: str,
        param_name: str,
    ) -> List[bool]:
        """Convert a list of dimension names into a list of booleans.

        Ordered based on the dimensions order list of the dependent-variable
        data for the model.

        For example,::

            >>> avail_dims
            ["age_group_id", "sex_id"]
            >>> dimensions_order_list
            ["sex_id", "location_id", "age_group_id"]
            >>> LimeTr._get_existing_dims(
                avail_dims, dimensions_order_list, "haq", "random_effect")
            [True, False, True]

        Args:
            avail_dims (list[str]): The dimensions to convert into the bool list
            dimensions_order_list (list[str]): An ordered list of dimensions needed by y with
                the shared grouping dimensions at the front of the list
            param_type (str): The type of parameter, e.g. covariate, random-effect, or
                indicator.
            param_name (str): The name of the parameter e.g. "haq", "intercept", or
                "age_indicator".

        Returns:
            list(bool): Boolean list of whether dimension in avail dims, where order is
                based on the order of ``dimensions_order_list``.

        Raises:
            ValueError: If there are dims in the covariates, random-effects, or
                indicators that are NOT in the dependent variable (i.e. in
                ``needed_dims``).
        """
        missing_dims = set(avail_dims) - set(dimensions_order_list)
        if missing_dims:
            err_msg = (
                f"The {param_name} {param_type} has extra dims={missing_dims} "
                f"that ``past_data`` is missing"
            )
            logger.error(
                err_msg,
                bindings=dict(
                    model=__class__.__name__,
                    param_name=param_name,
                    param_type=param_type,
                    missing_dims=missing_dims,
                ),
            )
            raise ValueError(err_msg)

        avail_dims = [dim in avail_dims for dim in dimensions_order_list]

        return avail_dims

    @staticmethod
    def _assert_param_dims(
        params: Dict,
        needed_dims: List[str],
        param_type: str = ModelConstants.ParamType.NONRANDOM,
    ) -> None:
        """Check that all dimensions of random effects are within ``needed_dims``.

        This is intended to be used on dicts that map random effects and
        indicators to their respective dimensions, but NOT for fixed effects.

        Args:
            params (Dict): A dictionary mapping covariates to the dimensions that their
                random slopes will be estimated for.
                of the form ``{covariate: list[dimension], ...}``
            needed_dims (list[str]): Dimensions that are present in the y data and must be
                accounted for in the ordered list of dimensions.
            param_type (str): the param type name to validate against

        Raises:
            ValueError: If a given random effect has 1 or more dimensions not included
                in ``needed_dims``, i.e. those of the dependent variable.
        """
        err_msg = ""
        err_log_bindings = dict(
            param_type=param_type,
            needed_dims=needed_dims,
            extra_dims=dict(),
        )
        for param_name, param_dims in params.items():
            if param_type == ModelConstants.ParamType.RANDOM:
                param_dims = param_dims.dims
            diff = set(param_dims) - set(needed_dims)
            if diff:
                err_msg += (
                    f"`{param_name}` has dims={tuple(diff)} that don't "
                    f"exist in dependent variable data. "
                )
                err_log_bindings["extra_dims"].update({param_name: diff})

        if err_msg:
            logger.error(err_msg, bindings=err_log_bindings)
            raise ValueError(err_msg)

    @staticmethod
    def _assert_covariate_params(
        fixed_effects: Dict, random_effects: Dict, covariates: Iterable[xr.DataArray]
    ) -> None:
        """Throws a warning if there's no effects associated with one of the covariates."""
        covariate_names = [cov.name for cov in covariates]
        effects = list(set(fixed_effects.keys()) & set(random_effects.keys()))
        for effect in effects:
            if effect in covariate_names:
                return
        warn_msg = (
            "There must be at least one effect that is associated with one of the given"
            " covariates"
        )
        logger.warning(warn_msg)


def _coord_combinations(
    coords_dict: Dict[str, List[np.ndarray]], non_grouping_dims: Optional[List[str]]
) -> List[Dict[str, List[np.ndarray]]]:
    """Return list of coord dictionaries in order based on non_grouping_dim coordinate values.

    E.g. if non_grouping_dims is ['age', 'sex'] and coords
    dict has {"loc": [6, 8], "age": [28, 29, 30],  "sex": [1, 2]} then it would
    return a list that looks like this:
        [
            {"loc": [6, 8], "age": [28], "sex": [1]},
            {"loc": [6, 8], "age": [28], "sex": [2]},
            {"loc": [6, 8], "age": [29], "sex": [1]},
            ... ,
            {"loc": [6, 8], "age": [30], "sex": [2]}
        ]

    Args:
        coords_dict (Dict[str, List[np.ndarray]]): Dictionary containing coordinates observed
            in some xarray
        non_grouping_dims (Optional[List[str]]): Optional list of dimensions to be iterated
            over

    Returns:
        List[Dict[str, List[np.ndarray]]]: if ``non_grouping_dims`` is None a copy of
            `coords_dict`, otherwise a new list of coordinate dictionaries
    """
    if non_grouping_dims:
        non_grouping_coords_list = [coords_dict[dim] for dim in non_grouping_dims]
        non_grouping_coord_combos = list(itertools.product(*non_grouping_coords_list))
        grouping_coords_dict = coords_dict.copy()
        for dim in non_grouping_dims:
            del grouping_coords_dict[dim]
        coord_combo_list = []
        for combo in non_grouping_coord_combos:
            combo_dict = grouping_coords_dict.copy()
            for i, non_grouping_dim in enumerate(non_grouping_dims):
                combo_dict.update({non_grouping_dim: [combo[i]]})
            coord_combo_list.append(combo_dict)
    else:
        return [coords_dict.copy()]

    return coord_combo_list
