# -*- coding: utf-8 -*-
"""
    Cov Finder
    ~~~~~~~~~~
"""
from typing import List, Dict, Tuple, Union
import warnings
from copy import deepcopy
import numpy as np
from mrtool import MRData, LinearCovModel, MRBRT
from mrtool.core.other_sampling import sample_simple_lme_beta


class CovFinder:
    """Class in charge of the covariate selection.
    """
    zero_gamma_uprior = np.array([0.0, 0.0])
    loose_gamma_uprior = np.array([1.0, 1.0])

    def __init__(self,
                 data: MRData,
                 covs: List[str],
                 pre_selected_covs: Union[List[str], None] = None,
                 normalized_covs: bool = True,
                 num_samples: int = 1000,
                 laplace_threshold: float = 1e-5,
                 power_range: Tuple[float, float] = (-8, 8),
                 power_step_size: float = 0.5,
                 inlier_pct: float = 1.0,
                 alpha: float = 0.05,
                 beta_gprior: Dict[str, np.ndarray] = None,
                 beta_gprior_std: float = 1.0,
                 bias_zero: bool = False,
                 use_re: Union[Dict, None] = None):
        """Covariate Finder.

        Args:
            data (MRData): Data object used for variable selection.
            covs (List[str]): Candidate covariates.
            normalized_covs (bool): If true, will normalize the covariates.
            pre_selected_covs (List[str] | None, optional):
                Pre-selected covaraites, will always be in the selected list.
            num_samples (int, optional):
                Number of samples used for judging if a variable is significance.
            laplace_threshold (float, optional):
                When coefficients from the Laplace regression is above this value,
                we consider it as the potential useful covariate.
            power_range (Tuple[float, float], optional):
                Power range for the Laplace prior standard deviation.
                Laplace prior standard deviation will go from `10**power_range[0]`
                to `10**power_range[1]`.
            power_step_size (float, optional): Step size of the swiping across the power range.
            inlier_pct (float, optional): Trimming option inlier percentage. Default to 1.
            alpha (float, optional): Significance threshold. Default to 0.05.
            beta_gprior_std (float, optional): Loose beta Gaussian prior standard deviation. Default to 1.
            bias_zero (bool, optional):
                If `True`, fit when specify the Gaussian prior it will be mean zero. Default to `False`.
            use_re (Union[Dict, None], optional):
                A dictionary of use_re for each covariate. When `None` we have an uninformative prior
                for the random effects variance. Default to `None`.
        """

        self.data = data
        self.covs = covs
        self.pre_selected_covs = [] if pre_selected_covs is None else pre_selected_covs
        assert len(set(self.pre_selected_covs) & set(self.covs)) == 0, \
            "covs and pre_selected_covs should be mutually exclusive."
        self.normalize_covs = normalized_covs
        self.inlier_pct = inlier_pct
        self.beta_gprior_std = beta_gprior_std
        assert self.beta_gprior_std > 0.0, f"beta_gprior_std={self.beta_gprior_std} has to be positive."
        self.bias_zero = bias_zero
        self.alpha = alpha
        if self.normalize_covs:
            self.data = deepcopy(data)
            self.data.normalize_covs(self.covs)
        self.selected_covs = self.pre_selected_covs.copy()
        self.beta_gprior = {} if beta_gprior is None else beta_gprior
        self.all_covs = self.pre_selected_covs + self.covs
        self.stop = False
        self.use_re = {} if use_re is None else use_re
        for cov in self.all_covs:
            if cov not in self.use_re:
                self.use_re[cov] = False

        self.num_samples = num_samples
        self.laplace_threshold = laplace_threshold
        self.power_range = power_range
        self.power_step_size = power_step_size
        self.powers = np.arange(*self.power_range, self.power_step_size)

        self.num_covs = len(pre_selected_covs) + len(covs)
        if len(covs) == 0:
            warnings.warn("There is no covariates to select, will return the pre-selected covariates.")
            self.stop = True

    def create_model(self,
                     covs: List[str],
                     prior_type: str = 'Laplace',
                     laplace_std: float = None) -> MRBRT:
        """Create Gaussian or Laplace model.

        Args:
            covs (List[str]): A list of covariates need to be included in the model.
            prior_type (str): Indicate if use ``Gaussian`` or ``Laplace`` model.
            laplace_std (float): Standard deviation of the Laplace prior. Default to None.

        Return:
            MRBRT: Created model object.
        """
        assert prior_type in ['Laplace', 'Gaussian'], "Prior type can only 'Laplace' or 'Gaussian'."
        if prior_type == 'Laplace':
            assert laplace_std is not None, "Use Laplace prior must provide standard deviation."

        if prior_type == 'Laplace':
            cov_models = [
                LinearCovModel(cov, use_re=True,
                               prior_beta_laplace=np.array([0.0, laplace_std])
                               if cov not in self.selected_covs else None,
                               prior_beta_gaussian=None
                               if cov not in self.selected_covs else self.beta_gprior[cov],
                               prior_gamma_uniform=self.loose_gamma_uprior if self.use_re[cov] else
                               self.zero_gamma_uprior)
                for cov in covs
            ]
        else:
            cov_models = [
                LinearCovModel(cov, use_re=True,
                               prior_beta_gaussian=None
                               if cov not in self.beta_gprior else self.beta_gprior[cov],
                               prior_gamma_uniform=self.loose_gamma_uprior if self.use_re[cov] else
                               self.zero_gamma_uprior)
                for cov in covs
            ]
        model = MRBRT(self.data, cov_models=cov_models, inlier_pct=self.inlier_pct)
        return model

    def fit_gaussian_model(self, covs: List[str]) -> MRBRT:
        """Fit Gaussian model.

        Args:
            covs (List[str]): A list of covariates need to be included in the model.

        Returns:
            MRBRT: the fitted model object.
        """
        gaussian_model = self.create_model(covs, prior_type='Gaussian')
        gaussian_model.fit_model(x0=np.zeros(gaussian_model.num_vars),
                                 inner_print_level=5, inner_max_iter=1000)
        empirical_gamma = np.var(gaussian_model.u_soln, axis=0)
        gaussian_model.gamma_soln = empirical_gamma
        gaussian_model.lt.gamma = empirical_gamma
        return gaussian_model

    def fit_laplace_model(self, covs: List[str], laplace_std: float) -> MRBRT:
        """Fit Laplace model.

        Args:
            covs (List[str]): A list of covariates need to be included in the model.
            laplace_std (float): The Laplace prior std.

        Returns:
            MRBRT: the fitted model object.
        """
        laplace_model = self.create_model(covs, prior_type='Laplace', laplace_std=laplace_std)
        lprior = laplace_model.create_lprior()
        scale = 1 if np.isinf(lprior[1]).all() else 2
        laplace_model.fit_model(x0=np.zeros(scale*laplace_model.num_vars),
                                inner_print_level=5, inner_max_iter=1000)
        return laplace_model

    def summary_gaussian_model(self, gaussian_model: MRBRT) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Summary the gaussian model.
        Return the mean standard deviation and the significance indicator of beta.

        Args:
            gaussian_model (MRBRT): Gaussian model object.

        Returns:
            Tuple[np.ndarray, np.ndarray, np.ndarray]:
                Mean, standard deviation and indicator of the significance of beta solution.
        """
        beta_samples = sample_simple_lme_beta(sample_size=self.num_samples,
                                              model=gaussian_model)
        beta_mean = gaussian_model.beta_soln
        beta_std = np.std(beta_samples, axis=0)
        beta_sig = self.is_significance(beta_samples, var_type='beta', alpha=self.alpha)
        return beta_mean, beta_std, beta_sig

    def fit_pre_selected_covs(self):
        """Fit the pre-selected covariates.
        """
        if len(self.pre_selected_covs) != 0:
            gaussian_model = self.fit_gaussian_model(self.pre_selected_covs)
            beta_mean, beta_std, _ = self.summary_gaussian_model(gaussian_model)
            beta_std.fill(self.beta_gprior_std)
            self.update_beta_gprior(self.pre_selected_covs, beta_mean, beta_std)

    def select_covs_by_laplace(self, laplace_std: float, verbose: bool = False):
        # fit laplace model and select the potential additional covariates
        laplace_model = self.fit_laplace_model(self.all_covs, laplace_std)
        additional_covs = []
        for i, cov in enumerate(self.all_covs):
            if np.abs(laplace_model.beta_soln[i]) > self.laplace_threshold and cov not in self.selected_covs:
                additional_covs.append(cov)

        if verbose:
            print(f'Laplace std: {laplace_std}')
            print('    potential additional covariates', additional_covs)

        if len(additional_covs) > 0:
            candidate_covs = self.selected_covs + additional_covs
            gaussian_model = self.fit_gaussian_model(candidate_covs)
            beta_soln_mean, beta_soln_std, beta_soln_sig = self.summary_gaussian_model(gaussian_model)
            if verbose:
                print('    Gaussian model fe_mean:', beta_soln_mean)
                print('    Gaussiam model fe_std: ', beta_soln_std)
                print('    Gaussian model re_var: ', gaussian_model.gamma_soln)
                print('    significance:', beta_soln_sig)
            # update the selected covs
            i_start = len(self.selected_covs)
            i_end = len(candidate_covs)
            for i in range(i_start, i_end):
                if beta_soln_sig[i]:
                    self.selected_covs.append(candidate_covs[i])
                    self.update_beta_gprior([candidate_covs[i]],
                                            np.array([0.0 if self.bias_zero else beta_soln_mean[i]]),
                                            np.array([self.beta_gprior_std]))
            if verbose:
                print('    selected covariates:', self.selected_covs)
            # update the stop
            self.stop = not all(beta_soln_sig)

        # other stop criterion
        if len(self.selected_covs) == self.num_covs:
            self.stop = True

    def update_beta_gprior(self, covs: List[str], mean: np.ndarray, std: np.ndarray):
        """Update the beta Gaussian prior.

        Args:
            covs (List[str]): Name of the covariates.
            mean (np.ndarray): Mean of the priors.
            std (np.ndarray): Standard deviation of the priors.
        """
        for i, cov in enumerate(covs):
            if cov not in self.all_covs:
                raise ValueError(f"Unrecognized covaraite {cov} for beta Gaussian prior update.")
            if cov not in self.beta_gprior:
                self.beta_gprior[cov] = np.array([mean[i], std[i]])

    def select_covs(self, verbose: bool = False):
        if len(self.covs) != 0:
            self.fit_pre_selected_covs()
            for power in self.powers:
                if not self.stop:
                    laplace_std = 10**power
                    self.select_covs_by_laplace(laplace_std, verbose=verbose)
            self.stop = True

    @staticmethod
    def is_significance(var_samples: np.ndarray,
                        var_type: str = 'beta',
                        alpha: float = 0.05) -> np.ndarray:
        assert var_type == 'beta', "Only support variable type beta."
        assert 0.0 < alpha < 1.0, "Significance threshold has to be between 0 and 1."
        var_uis = np.quantile(var_samples, (0.5*alpha, 1 - 0.5*alpha), axis=0)
        var_sig = var_uis.prod(axis=0) > 0

        return var_sig
