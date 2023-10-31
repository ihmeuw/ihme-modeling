from typing import Dict, List, NamedTuple, Tuple, Union

import numpy as np
import pandas as pd
from scipy.linalg import block_diag
import scipy.optimize as sciopt


class GroupInfo(NamedTuple):
    labels: np.ndarray
    sizes: np.ndarray
    indices: List[np.ndarray]


NO_GROUP = 'no_group'
INTERCEPT = 'intercept'
DEFAULT_RESPONSE_SE_COLUMN = 'response_se'


class MRData:
    """Data and group structure for model fit.

    Parameters
    ----------
    data
        DataFrame containing response variable, an optional standard error
        for the response variable, and the predictors.

    """

    def __init__(
            self,
            data: pd.DataFrame,
            response_column: str,
            response_se_column: str = '',
            predictors: List[str] = None,
            group_columns: List[str] = None,
    ):
        predictors = predictors if predictors else []
        group_columns = group_columns if group_columns else []
        self.validate_inputs(
            data,
            response_column,
            response_se_column,
            predictors,
            group_columns,
        )

        self._original_data = data.copy()

        self.response_column = response_column
        # If no observed standard error, use 1.0 so math is easy.
        if not response_se_column:
            response_se_column = DEFAULT_RESPONSE_SE_COLUMN
            data[response_se_column] = 1.0
        self.response_se_column = response_se_column

        # Add intercept as default predictor.
        if INTERCEPT not in predictors:
            predictors.append(INTERCEPT)
        data[INTERCEPT] = 1.0
        self.predictors = predictors

        self.group_info = {
            group_column: unpack_levels(data, group_column)
            for group_column in group_columns
        }
        self.data = data[
            [self.response_column, self.response_se_column]
            + self.predictors + list(self.group_info)
        ]
        self.all_groups = self.data[list(self.group_info)].drop_duplicates()
        self.group_info[NO_GROUP] = unpack_levels(self.data, NO_GROUP)

        self.response = self.data[self.response_column].values
        self.response_se = self.data[self.response_se_column].values

    @staticmethod
    def validate_inputs(
            data: pd.DataFrame,
            response_column: str,
            response_se_column: str,
            predictors: List[str],
            group_columns: List[str],
    ) -> None:
        if data.empty:
            raise ValueError('No model data.')
        if response_column not in data:
            raise ValueError(
                f'Response variable {response_column} not in data columns {data.columns}.'
            )
        if response_se_column and response_se_column not in data:
            raise ValueError(
                f'Response SE variable {response_se_column} not in data columns {data.columns}.'
            )
        for predictor in predictors:
            if predictor not in data:
                raise ValueError(
                    f'Predictor variable {predictor} not in data columns {data.columns}.'
                )
        for group_column in group_columns:
            if group_column not in data:
                raise ValueError(
                    f'Grouping variable {group_column} not in data columns {data.columns}.'
                )
            test_df = pd.DataFrame({group_column: data[group_column],
                                    'idx': np.arange(len(data))})
            max_diff = test_df.groupby(group_column).idx.diff().max()
            if not (np.isnan(max_diff) or max_diff == 1.0):
                raise ValueError(f'Data is discontiguous by group level {group_column}.')

    def __repr__(self):
        return (f'MRData(\n'
                f'    {self.response_column} ~ {" + ".join(self.predictors)},\n'
                f'    observations={len(self.data)}, group_columns={list(self.group_info)}\n'
                f')')


class MRModel:
    """Linear MetaRegression Model.
    """

    def __init__(
            self,
            data: MRData,
            predictors: 'PredictorModelSet',
    ):
        self.data = data
        predictors.attach_data(data)
        self.predictors = predictors

    def objective(
            self,
            coefficients: np.ndarray,
    ) -> float:
        """Objective function for minimization."""
        prediction = self.predictors.predict(coefficients)
        val = (
            0.5*np.sum(((self.data.response - prediction)/self.data.response_se)**2)
            + self.predictors.prior_objective(coefficients)
        )
        return val

    def gradient(
            self,
            coefficents: np.ndarray,
    ) -> np.ndarray:
        """Gradient of the objective function."""
        prediction = self.predictors.predict(coefficents)
        residual = self.data.response - prediction
        return self.predictors.gradient(coefficents, residual, self.data.response_se)

    def fit_model(
            self,
            initial_coefficents: np.ndarray = None,
            options: Dict = None,
    ) -> pd.DataFrame:
        """Fit the model, including initial condition and parameter."""
        if initial_coefficents is None:
            initial_coefficents = np.zeros(self.predictors.var_size)
        optimization_result = sciopt.minimize(
            fun=self.objective,
            x0=initial_coefficents,
            jac=self.gradient,
            method='L-BFGS-B',
            bounds=self.predictors.bounds,
            options=options,
        )

        return self.predictors.process_result(optimization_result.x, self.data.all_groups)

    def __repr__(self):
        data_repr = ("\n" + " "*9).join(repr(self.data).split("\n"))
        predictor_repr = ("\n" + " "*15).join(repr(self.predictors).split("\n"))
        return (f'MRModel(\n' 
                f'    data={data_repr},\n'
                f'    predictors={predictor_repr}\n'
                f')')


class PredictorModel:

    def __init__(
            self,
            predictor_name: str,
            group_level: str = NO_GROUP,
            bounds: Tuple[float, float] = (-np.inf, np.inf),
            gaussian_prior_params: Union[pd.DataFrame, Tuple[float, float]] = (0.0, np.inf),
    ):
        self.name = predictor_name
        self.group_level = group_level

        self.original_bounds = bounds
        self.original_prior = gaussian_prior_params
        self.bounds = None
        self.prior_level = None
        self.prior_mean = None
        self.prior_sd = None

        self.var_size = None
        self.group_labels = None
        self.group_sizes = None
        self.group_indices = None

        self.predictor = None
        self.matrix = None
        self.scale = None

    def attach_data(self, mr_data: MRData) -> None:
        assert self.name in mr_data.predictors
        assert self.group_level in mr_data.group_info

        group_info = mr_data.group_info[self.group_level]
        self.group_labels, self.group_sizes, self.group_indices = group_info
        self.var_size = len(self.group_labels)

        predictor = mr_data.data[self.name]
        if np.linalg.norm(predictor) > 0.0:
            self.scale = np.linalg.norm(predictor)
        else:
            self.scale = 1.0
        self.predictor = predictor.values / self.scale
        self.matrix = block_diag(*[
            self.predictor[group_idx].reshape((-1, 1))
            for group_idx in self.group_indices
        ])

        self.bounds = np.repeat(
            (np.array(self.original_bounds) * self.scale).reshape((1, -1)),
            self.var_size,
            axis=0,
        )

        self.prior_level = 'group' if isinstance(self.original_prior, pd.DataFrame) else 'shared'
        prior = reshape_prior(
            self.original_prior,
            self.group_level,
            self.group_labels,
        )
        self.prior_mean = prior[0]
        self.prior_sd = prior[1]

    def broadcast_coefficient(self, coefficient: np.ndarray) -> np.ndarray:
        return np.repeat(coefficient, self.group_sizes)

    def predict(self, coefficient: np.ndarray) -> np.ndarray:
        return self.predictor * self.broadcast_coefficient(coefficient)

    def prior_objective(self, coefficient: np.ndarray) -> float:
        scaled_coefficient = coefficient / self.scale
        return 0.5 * np.sum(((scaled_coefficient - self.prior_mean)/self.prior_sd)**2)

    def gradient(self, coefficient: np.ndarray, residual: np.ndarray, obs_se: np.ndarray) -> np.ndarray:
        gradient = -(self.matrix.T/obs_se).dot(residual/obs_se)
        scaled_coefficient = coefficient / self.scale
        gradient += ((scaled_coefficient - self.prior_mean)/self.prior_sd**2) / self.scale
        return gradient

    def process_result(self, coefficient: np.ndarray, all_groups: pd.DataFrame) -> pd.Series:
        if self.group_level is NO_GROUP:
            result = pd.Series(
                coefficient[0] / self.scale,
                index=all_groups.set_index(all_groups.columns.tolist()).index,
                name=self.name
            )
        else:
            result = pd.DataFrame({
                self.name: coefficient / self.scale,
                self.group_level: self.group_labels
            }).merge(all_groups).set_index(all_groups.columns.tolist())[self.name]
        return result

    @classmethod
    def from_specification(cls, covariate):
        return cls(
            predictor_name=covariate.name,
            group_level=covariate.group_level if covariate.group_level else NO_GROUP,
            bounds=covariate.bounds,
            gaussian_prior_params=covariate.gprior,
        )

    def __repr__(self) -> str:
        level = f', group={self.group_level}' if self.group_level else ''
        return f'{self.__class__.__name__}({self.name}{level}, prior_level={self.prior_level})'


class PredictorModelSet:

    def __init__(self, predictors: List[PredictorModel]):
        self.predictors = predictors
        self.var_sizes = None
        self.var_size = None
        self.var_idx = None
        self.bounds = None

    def attach_data(self, mr_data: MRData) -> None:
        var_sizes = []
        for predictor in self.predictors:
            predictor.attach_data(mr_data)
            var_sizes.append(predictor.var_size)

        self.var_sizes = np.array(var_sizes)
        self.var_size = np.sum(self.var_sizes)
        self.var_idx = sizes_to_indices(self.var_sizes)
        self.bounds = np.vstack([
            predictor.bounds for predictor in self.predictors
        ])

    def predict(self, coefficients: np.ndarray) -> np.ndarray:
        return np.sum([
            predictor.predict(coefficients[idx])
            for predictor, idx in zip(self.predictors, self.var_idx)
        ], axis=0)

    def prior_objective(self, coefficients: np.ndarray) -> float:
        # noinspection PyTypeChecker
        return np.sum([
            predictor.prior_objective(coefficients[idx])
            for predictor, idx in zip(self.predictors, self.var_idx)
        ])

    def gradient(
        self,
        coefficients: np.ndarray,
        residual: np.ndarray,
        obs_se: np.ndarray,
    ) -> np.ndarray:
        return np.hstack([
            predictor.gradient(coefficients[idx], residual, obs_se)
            for predictor, idx in zip(self.predictors, self.var_idx)
        ])

    def process_result(
        self,
        coefficients: np.ndarray,
        all_groups: pd.DataFrame,
    ) -> pd.DataFrame:
        return pd.concat([
            predictor.process_result(coefficients[idx], all_groups)
            for predictor, idx in zip(self.predictors, self.var_idx)
        ], axis=1)

    def __len__(self) -> int:
        return len(self.predictors)

    def __iter__(self):
        return iter(self.predictors)

    def __repr__(self) -> str:
        predictors = ',\n    '.join([repr(predictor) for predictor in self.predictors])
        return (f'PredictorModelSet(\n'
                f'    {predictors},\n'
                f')')


def unpack_levels(
    data: pd.DataFrame,
    group_level: str,
) -> GroupInfo:
    if group_level is not NO_GROUP:
        assert group_level in data
        labels, sizes = get_labels_and_counts(data[group_level])
        indices = sizes_to_indices(sizes)
    else:
        labels, sizes = np.array([1]), np.array([len(data)])
        indices = [np.arange(len(data))]
    return GroupInfo(labels, sizes, indices)


def get_labels_and_counts(items: pd.Series):
    item_counts = {}  # Dicts stay ordered, this is important.
    for item in items.tolist():
        if item in item_counts:
            item_counts[item] += 1
        else:
            item_counts[item] = 1
    labels, counts = zip(*item_counts.items())
    return np.array(labels), np.array(counts)


def reshape_prior(
    effect_prior: Union[pd.DataFrame, Tuple[float, float]],
    group_level: str,
    group_labels: np.ndarray
) -> Tuple[np.ndarray, np.ndarray]:
    if isinstance(effect_prior, pd.DataFrame):
        assert group_level is not NO_GROUP
        assert set(effect_prior) == {'mean', 'sd'}
        assert len(effect_prior) == len(group_labels)
        effect_prior = effect_prior.loc[group_labels.astype(effect_prior.index.dtype)]
        effect_prior_df = effect_prior.reset_index()
        assert set(effect_prior_df).difference({'mean', 'sd'}) == {group_level}
        assert set(effect_prior_df[group_level]) == set(group_labels)
        effect_prior_mean = effect_prior['mean'].values
        effect_prior_sd = effect_prior['sd'].values
    else:
        effect_prior = np.repeat(
            np.array(effect_prior).reshape((1, -1)),
            len(group_labels),
            axis=0,
        )
        effect_prior_mean = effect_prior[:, 0]
        effect_prior_sd = effect_prior[:, 1]

    return effect_prior_mean, effect_prior_sd


def sizes_to_indices(
    sizes: np.ndarray,
) -> List[np.ndarray]:
    """Converting sizes to corresponding indices.
    """
    u_id = np.cumsum(sizes)
    l_id = np.insert(u_id[:-1], 0, 0)

    return [
        np.arange(l, u) for l, u in zip(l_id, u_id)
    ]
