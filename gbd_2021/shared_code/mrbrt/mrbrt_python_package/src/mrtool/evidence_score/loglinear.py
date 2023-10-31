"""
Log linear scorelator
"""
import os
from pathlib import Path
from typing import List, Tuple, Union
import numpy as np
from scipy.stats import norm
import matplotlib.pyplot as plt
from mrtool import MRBRT
from mrtool.core.other_sampling import extract_simple_lme_specs, extract_simple_lme_hessian


class LoglinearScorelator:
    def __init__(self,
                 model: MRBRT,
                 alt_cov_names: List[str],
                 ref_cov_names: List[str] = None,
                 exposure_quantiles: Tuple[float] = (0.15, 0.85),
                 exposure_bounds: Tuple[float] = None,
                 draw_quantiles: Tuple[float] = (0.05, 0.95),
                 pred_exposure_bounds: Tuple[float] = None,
                 num_samples: int = 1000,
                 num_points: int = 100,
                 name: str = 'unknown'):
        self.model = model
        self.alt_cov_names = alt_cov_names
        self.ref_cov_names = [] if ref_cov_names is None else ref_cov_names
        self.exposure_quantiles = exposure_quantiles
        self.exposure_bounds = exposure_bounds
        self.pred_exposure_bounds = pred_exposure_bounds
        self.draw_quantiles = draw_quantiles
        self.num_samples = num_samples
        self.num_points = num_points
        self.name = name

        # create exposure information
        exposures = self.model.data.get_covs(self.alt_cov_names + self.ref_cov_names)
        if self.pred_exposure_bounds:
            self.exposure_lend = self.pred_exposure_bounds[0]
            self.exposure_uend = self.pred_exposure_bounds[1]
        else:
            self.exposure_lend = np.min(exposures)
            self.exposure_uend = np.max(exposures)
        alt_exposures = self.model.data.get_covs(self.alt_cov_names).mean(axis=1)
        if self.ref_cov_names:
            ref_exposures = self.model.data.get_covs(self.ref_cov_names).mean(axis=1)
        else:
            ref_exposures = np.zeros(self.model.data.num_obs)
        self.data_exposures = alt_exposures - ref_exposures
        self.pred_exposures = np.linspace(self.exposure_lend, self.exposure_uend, self.num_points)

        # compute the range of exposures
        self.exposure_lb = np.quantile(self.data_exposures, self.exposure_quantiles[0])
        self.exposure_ub = np.quantile(self.data_exposures, self.exposure_quantiles[1])
        if self.exposure_bounds is not None:
            self.exposure_lb = self.exposure_bounds[0]
            self.exposure_ub = self.exposure_bounds[1]
        self.effective_index = (self.pred_exposures >= self.exposure_lb) & (self.pred_exposures <= self.exposure_ub)

        # extract the cov model information
        self.cov_model_name = None
        for cov_model_name in self.model.cov_model_names:
            cov_model = self.model.get_cov_model(cov_model_name)
            if set(cov_model.alt_cov) == set(self.alt_cov_names):
                self.cov_model_name = cov_model_name
                break
        if self.cov_model_name is None:
            raise ValueError(f"Cannot find the CovModel with alt_cov={self.alt_cov_names}.")
        self.cov_model_index = self.model.get_cov_model_index(self.cov_model_name)

        beta_index = self.model.x_vars_indices[self.cov_model_index]
        gamma_index = self.model.z_vars_indices[self.cov_model_index]
        self.beta = self.model.beta_soln[beta_index][0]
        model_specs = extract_simple_lme_specs(self.model)
        beta_var = np.linalg.inv(extract_simple_lme_hessian(model_specs))
        self.beta_var = beta_var[np.ix_(beta_index, beta_index)][0, 0]
        self.gamma = self.model.gamma_soln[gamma_index][0]
        gamma_fisher = self.model.lt.get_gamma_fisher(self.model.gamma_soln)
        self.gamma_sd = np.sqrt(
            np.diag(np.linalg.inv(gamma_fisher))[gamma_index]
        )[0]
        self.gamma_ub = self.gamma + 2.0*self.gamma_sd

        # compute draws
        self.beta_lb = self.beta + norm.ppf(self.draw_quantiles[0], scale=np.sqrt(self.beta_var + self.gamma))
        self.beta_ub = self.beta + norm.ppf(self.draw_quantiles[1], scale=np.sqrt(self.beta_var + self.gamma))
        self.wider_beta_lb = self.beta + norm.ppf(self.draw_quantiles[0], scale=np.sqrt(self.beta_var + self.gamma_ub))
        self.wider_beta_ub = self.beta + norm.ppf(self.draw_quantiles[1], scale=np.sqrt(self.beta_var + self.gamma_ub))

        self.draw_median = self.beta*self.pred_exposures
        self.draw_lb = self.beta_lb*self.pred_exposures
        self.draw_ub = self.beta_ub*self.pred_exposures
        self.wider_draw_lb = self.wider_beta_lb*self.pred_exposures
        self.wider_draw_ub = self.wider_beta_ub*self.pred_exposures

    def get_score(self, use_gamma_ub: bool = False) -> float:
        if self.is_harmful():
            draw = self.wider_draw_lb if use_gamma_ub else self.draw_lb
            score = draw[self.effective_index].mean()
        else:
            draw = self.wider_draw_ub if use_gamma_ub else self.draw_ub
            score = -draw[self.effective_index].mean()
        return score

    def is_harmful(self) -> bool:
        return self.beta > 0.0

    def plot_data(self, ax=None):
        if ax is None:
            fig = plt.figure()
            ax = fig.add_subplot()
        data = self.model.data
        w = self.model.w_soln
        trim_index = w <= 0.1
        ax.scatter(self.data_exposures, data.obs, c='gray', s=5.0/data.obs_se, alpha=0.5)
        ax.scatter(self.data_exposures[trim_index], data.obs[trim_index],
                   c='red', marker='x', s=5.0/data.obs_se[trim_index])

    def plot_model(self,
                   ax=None,
                   title: str = None,
                   xlabel: str = 'exposure',
                   ylabel: str = 'ln relative risk',
                   xlim: tuple = None,
                   ylim: tuple = None,
                   xscale: str = None,
                   yscale: str = None,
                   folder: Union[str, Path] = None):

        if ax is None:
            fig = plt.figure()
            ax = fig.add_subplot()

        ax.plot(self.pred_exposures, self.draw_median, color='#69b3a2', linewidth=1)
        ax.fill_between(self.pred_exposures, self.draw_lb, self.draw_ub, color='#69b3a2', alpha=0.2)
        ax.fill_between(self.pred_exposures, self.wider_draw_lb, self.wider_draw_ub, color='#69b3a2', alpha=0.2)
        ax.axvline(self.exposure_lb, linestyle='--', color='k', linewidth=1)
        ax.axvline(self.exposure_ub, linestyle='--', color='k', linewidth=1)
        ax.axhline(0.0, linestyle='--', color='k', linewidth=1)

        title = self.name if title is None else title
        score = self.get_score()
        low_score = self.get_score(use_gamma_ub=True)
        title = f"{title}: score = ({low_score: .3f}, {score: .3f})"

        ax.set_title(title, loc='left')
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        if xlim is not None:
            ax.set_xlim(*xlim)
        if ylim is not None:
            ax.set_ylim(*ylim)
        if xscale is not None:
            ax.set_xscale(xscale)
        if yscale is not None:
            ax.set_yscale(yscale)

        self.plot_data(ax=ax)

        if folder is not None:
            folder = Path(folder)
            if not folder.exists():
                os.mkdir(folder)
            plt.savefig(folder/f"{self.name}.pdf", bbox_inches='tight')

        return ax

