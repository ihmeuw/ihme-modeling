# -*- coding: utf-8 -*-
"""
    data
    ~~~~

    `data` module for `mrtool` package.
"""
from typing import Dict, List, Union, Any
import warnings
from dataclasses import dataclass, field
import numpy as np
import pandas as pd
from .utils import empty_array, to_list, is_numeric_array, expand_array


@dataclass
class MRData:
    """Data for simple linear mixed effects model.
    """
    obs: np.ndarray = field(default_factory=empty_array)
    obs_se: np.ndarray = field(default_factory=empty_array)
    covs: Dict[str, np.ndarray] = field(default_factory=dict)
    study_id: np.ndarray = field(default_factory=empty_array)
    data_id: np.ndarray = field(default_factory=empty_array)
    cov_scales: Dict[str, float] = field(init=False, default_factory=dict)

    def __post_init__(self):
        self._check_attr_type()

        self.obs = expand_array(self.obs, (self.num_points,), np.nan, 'obs')
        self.obs_se = expand_array(self.obs_se, (self.num_points,), 1.0, 'obs_se')
        self.study_id = expand_array(self.study_id, (self.num_points,), 'Unknown', 'study_id')
        self.data_id = expand_array(self.data_id, (self.num_points,), np.arange(self.num_points), 'data_id')
        assert len(np.unique(self.data_id)) == self.num_points, "data_id must be unique for each data point."
        self.covs.update({'intercept': np.ones(self.num_points)})
        for cov_name, cov in self.covs.items():
            assert len(cov) == self.num_points, f"covs[{cov_name}], inconsistent shape."

        self._remove_nan_in_covs()
        self._get_study_structure()
        self._get_cov_scales()

    @property
    def num_points(self):
        """Number of data points.
        """
        return max([len(self.obs), len(self.obs_se), len(self.study_id)] +
                   [len(cov) for cov in self.covs.values()])

    @property
    def num_obs(self):
        """Number of observations.
        """
        return len(self.obs)

    @property
    def num_covs(self):
        """Number of covariates.
        """
        return len(self.covs)

    @property
    def num_studies(self):
        """Number of studies.
        """
        return len(self.studies)

    def _check_attr_type(self):
        """Check the type of the attributes.
        """
        assert isinstance(self.obs, np.ndarray)
        assert is_numeric_array(self.obs)
        assert isinstance(self.obs_se, np.ndarray)
        assert is_numeric_array(self.obs_se)
        assert isinstance(self.study_id, np.ndarray)
        assert isinstance(self.data_id, np.ndarray)
        assert isinstance(self.covs, dict)
        for cov in self.covs.values():
            assert isinstance(cov, np.ndarray)
            assert is_numeric_array(cov)

    def _get_cov_scales(self):
        """Compute the covariate scale.
        """
        if self.is_empty():
            self.cov_scales = {cov_name: np.nan for cov_name in self.covs.keys()}
        else:
            self.cov_scales = {cov_name: np.max(np.abs(cov)) for cov_name, cov in self.covs.items()}
            zero_covs = [cov_name for cov_name, cov_scale in self.cov_scales.items() if cov_scale == 0.0]
            for cov_name in zero_covs:
                warnings.warn(f"numbers in covariate[{cov_name}] are all zero. "
                              f"Please use this in spline range exposure or when preidct.")

    def _get_study_structure(self):
        """Get the study structure.
        """
        self.studies, self.study_sizes = np.unique(self.study_id,
                                                   return_counts=True)
        self._sort_by_study_id()

    def _sort_data(self, index: np.ndarray):
        """Sort the object.

        Args:
            index (np.ndarray): Sorting index.
        """
        index = np.array(index)
        assert (np.sort(index) == np.arange(self.num_obs)).all(), "Sorting index must go from 0 to num_obs - 1."
        self.obs = self.obs[index]
        self.obs_se = self.obs_se[index]
        for cov_name, cov in self.covs.items():
            self.covs[cov_name] = cov[index]
        self.study_id = self.study_id[index]
        self.data_id = self.data_id[index]

    def _sort_by_study_id(self):
        """Sort data by study_id.
        """
        if not self.is_empty() and self.num_studies != 1:
            sort_index = np.argsort(self.study_id)
            self._sort_data(sort_index)

    def _sort_by_data_id(self):
        """Sort data by data_id.
        """
        if not self.is_empty():
            sort_index = np.argsort(self.data_id)
            self._sort_data(sort_index)

    def _remove_nan_in_covs(self):
        """Remove potential nans in covaraites.
        """
        if not self.is_empty():
            index = np.full(self.num_obs, False)
            for cov_name, cov in self.covs.items():
                cov_index = np.isnan(cov)
                if cov_index.any():
                    warnings.warn(f"There are {cov_index.sum()} nans in covaraite {cov_name}.")
                index = index | cov_index
            self._remove_data(index)

    def _remove_data(self, index: np.ndarray):
        """Remove the data point by index.

        Args:
            index (np.ndarray): Bool array, when ``True`` delete corresponding data.
        """
        assert len(index) == self.num_obs
        assert all([isinstance(i, (bool, np.bool_)) for i in index])

        keep_index = ~index
        self.obs = self.obs[keep_index]
        self.obs_se = self.obs_se[keep_index]
        for cov_name, cov in self.covs.items():
            self.covs[cov_name] = cov[keep_index]
        self.study_id = self.study_id[keep_index]
        self.data_id = self.data_id[keep_index]

    def _get_data(self, index: np.ndarray) -> 'MRData':
        """Get the data point by index.

        Args:
            index (np.ndarray): Indices of the data we want to get.

        Returns:
            MRData: data object contains the data from indices.
        """
        obs = self.obs[index].copy()
        obs_se = self.obs_se[index].copy()
        covs = {}
        for cov_name, cov in self.covs.items():
            covs[cov_name] = cov[index].copy()
        study_id = self.study_id[index].copy()
        data_id = self.data_id[index].copy()

        return MRData(obs, obs_se, covs, study_id, data_id)

    def is_empty(self) -> bool:
        """Return true when object contain data.
        """
        return self.num_points == 0

    def _assert_not_empty(self):
        """Raise ValueError when object is empty.
        """
        if self.is_empty():
            raise ValueError("MRData object is empty.")

    def is_cov_normalized(self, covs: Union[List[str], str, None] = None) -> bool:
        """Return true when covariates are normalized.
        """
        if covs is None:
            covs = list(self.covs.keys())
        else:
            covs = to_list(covs)
            assert self.has_covs(covs)
        ok = not self.is_empty()
        for cov_name in covs:
            ok = ok and ((not is_numeric_array(self.covs[cov_name])) or
                         (np.max(np.abs(self.covs[cov_name])) == 1.0))
        return ok

    def reset(self):
        """Reset all the attributes to default values.
        """
        self.obs = empty_array()
        self.obs_se = empty_array()
        self.covs = dict()
        self.covs['intercept'] = np.ones(0)
        self.study_id = empty_array()
        self.data_id = empty_array()

    def load_df(self, data: pd.DataFrame,
                col_obs: Union[str, None] = None,
                col_obs_se: Union[str, None] = None,
                col_covs: Union[List[str], None] = None,
                col_study_id: Union[str, None] = None,
                col_data_id: Union[str, None] = None):
        """Load data from data frame.
        """
        self.reset()

        self.obs = empty_array() if col_obs is None else data[col_obs].to_numpy()
        self.obs_se = empty_array() if col_obs_se is None else data[col_obs_se].to_numpy()
        self.study_id = empty_array() if col_study_id is None else data[col_study_id].to_numpy()
        self.data_id = empty_array() if col_data_id is None else data[col_data_id].to_numpy()
        self.covs = dict() if col_covs is None else {cov_name: data[cov_name].to_numpy()
                                                     for cov_name in col_covs}

        self.__post_init__()

    def load_xr(self, data,
                var_obs: Union[str, None] = None,
                var_obs_se: Union[str, None] = None,
                var_covs: Union[List[str], None] = None,
                coord_study_id: Union[str, None] = None):
        """Load data from xarray.
        """
        self.reset()

        self.obs = empty_array() if var_obs is None else data[var_obs].data.flatten()
        self.obs_se = empty_array() if var_obs_se is None else data[var_obs_se].data.flatten()
        if coord_study_id is None:
            self.study_id = empty_array()
        else:
            index = data.coords.to_index().to_frame(index=False)
            self.study_id = index[coord_study_id].to_numpy()
        self.covs = dict() if var_covs is None else {cov_name: data[cov_name].data.flatten()
                                                     for cov_name in var_covs}

        self.__post_init__()

    def to_df(self) -> pd.DataFrame:
        """Convert data object to data frame.
        """
        df = pd.DataFrame({
            'obs': self.obs,
            'obs_se': self.obs_se,
            'study_id': self.study_id
        })
        for cov_name in self.covs:
            df[cov_name] = self.covs[cov_name]

        return df

    def has_covs(self, covs: Union[List[str], str]) -> bool:
        """If the data has the provided covariates.

        Args:
            covs (Union[List[str], str]):
                List of covariate names or one covariate name.

        Returns:
            bool: If has covariates return `True`.
        """
        covs = to_list(covs)
        if len(covs) == 0:
            return True
        else:
            return all([cov in self.covs for cov in covs])

    def has_studies(self, studies: Union[List[Any], Any]) -> bool:
        """If the data has provided study_id

        Args:
            studies Union[List[Any], Any]:
                List of studies or one study.

        Returns:
            bool: If has studies return `True`.
        """
        studies = to_list(studies)
        if len(studies) == 0:
            return True
        else:
            return all([study in self.studies for study in studies])

    def _assert_has_covs(self, covs: Union[List[str], str]):
        """Assert has covariates otherwise raise ValueError.
        """
        if not self.has_covs(covs):
            covs = to_list(covs)
            missing_covs = [cov for cov in covs if cov not in self.covs]
            raise ValueError(f"MRData object do not contain covariates: {missing_covs}.")

    def _assert_has_studies(self, studies: Union[List[Any], Any]):
        """Assert has studies otherwise raise ValueError.
        """
        if not self.has_studies(studies):
            studies = to_list(studies)
            missing_studies = [study for study in studies if study not in self.studies]
            raise ValueError(f"MRData object do not contain studies: {missing_studies}.")

    def get_covs(self, covs: Union[List[str], str]) -> np.ndarray:
        """Get covariate matrix.

        Args:
            covs (Union[List[str], str]):
                List of covariate names or one covariate name.

        Returns:
            np.ndarray: Covariates matrix, in the column fashion.
        """
        covs = to_list(covs)
        self._assert_has_covs(covs)
        if len(covs) == 0:
            return np.array([]).reshape(self.num_obs, 0)
        else:
            return np.hstack([self.covs[cov_names][:, None] for cov_names in covs])

    def get_study_data(self, studies: Union[List[Any], Any]) -> 'MRData':
        """Get study specific data.

        Args:
            studies (Union[List[Any], Any]): List of studies or  one study.

        Returns
            MRData: Data object contains the study specific data.
        """
        self._assert_has_studies(studies)
        studies = to_list(studies)
        index = np.array([study in studies for study in self.study_id])
        return self._get_data(index)

    def normalize_covs(self, covs: Union[List[str], str, None] = None):
        """Normalize covariates by the largest absolute value for each covariate.
        """
        if covs is None:
            covs = list(self.covs.keys())
        else:
            covs = to_list(covs)
            self._assert_has_covs(covs)
        if not self.is_empty():
            for cov_name in covs:
                if is_numeric_array(self.covs[cov_name]):
                    self.covs[cov_name] = self.covs[cov_name]/self.cov_scales[cov_name]

    def __repr__(self):
        """Summary of the object.
        """
        dimension_summary = [
            "number of observations: %i" % self.num_obs,
            "number of covariates  : %i" % self.num_covs,
            "number of studies     : %i" % self.num_studies,
        ]
        return "\n".join(dimension_summary)
