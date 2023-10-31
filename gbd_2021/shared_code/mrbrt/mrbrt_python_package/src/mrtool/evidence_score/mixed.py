"""
Mixed scorelator
"""
import os
from pathlib import Path
from typing import Tuple, Union
import numpy as np
import matplotlib.pyplot as plt
from mrtool import MRData, MRBRT
from mrtool.core.other_sampling import sample_simple_lme_beta

class MixedScorelator:
    def __init__(self,
                 model: MRBRT,
                 cont_cov_name: str,
                 cont_quantiles: Tuple[float] = (0.15, 0.85),
                 cont_bounds: Tuple[float] = None,
                 draw_quantiles: Tuple[float] = (0.05, 0.95),
                 num_points: int = 100,
                 num_samples: int = 1000,
                 name: str = 'unknown'):
        self.model = model
        self.cont_cov_name = cont_cov_name
        self.cont_quantiles = cont_quantiles
        self.cont_bounds = cont_bounds
        self.draw_quantiles = draw_quantiles
        self.num_points = num_points
        self.num_samples = num_samples
        self.name = name

        # get continuous variable
        self.data_cont = self.model.data.covs[self.cont_cov_name]
        self.cont_lend = np.min(self.data_cont)
        self.cont_uend = np.max(self.data_cont)
        self.pred_cont = np.linspace(self.cont_lend, self.cont_uend, self.num_points)

        # get gamma
        self.gamma = self.model.gamma_soln[0]
        self.gamma_sd = 1.0/np.sqrt(self.model.lt.get_gamma_fisher(self.model.gamma_soln)[0, 0])

        # compute the range of exposures
        self.cont_lb = np.quantile(self.data_cont, self.cont_quantiles[0])
        self.cont_ub = np.quantile(self.data_cont, self.cont_quantiles[1])
        if self.cont_bounds is not None:
            self.cont_lb = self.cont_bounds[0]
            self.cont_ub = self.cont_bounds[1]
        self.effective_index = (self.pred_cont >= self.cont_lb) & (self.pred_cont <= self.cont_ub)

        self.draws = self.get_draws()
        self.wider_draws = self.get_draws(use_gamma_ub=True)
        self.draw_lb = np.quantile(self.draws, self.draw_quantiles[0], axis=0)
        self.draw_ub = np.quantile(self.draws, self.draw_quantiles[1], axis=0)
        self.wider_draw_lb = np.quantile(self.wider_draws, self.draw_quantiles[0], axis=0)
        self.wider_draw_ub = np.quantile(self.wider_draws, self.draw_quantiles[1], axis=0)


    def get_pred_data(self) -> MRData:
        zero_cov = np.zeros(self.num_points)
        other_covs = {
            cov_name: zero_cov
            for cov_name in self.model.data.covs
            if cov_name not in [self.cont_cov_name, 'intercept']
        }
        covs = {self.cont_cov_name: self.pred_cont, **other_covs}
        return MRData(covs=covs)

    def get_draws(self,
                  num_samples: int = None,
                  use_gamma_ub: bool = False) -> np.ndarray:
        gamma = self.gamma + 2.0*self.gamma_sd if use_gamma_ub else self.gamma
        num_samples = self.num_samples if num_samples is None else num_samples
        beta_samples = sample_simple_lme_beta(num_samples, self.model)
        gamma_samples = np.repeat(np.array([[gamma]]), num_samples, axis=0)
        pred_data = self.get_pred_data()
        return self.model.create_draws(pred_data, beta_samples=beta_samples, gamma_samples=gamma_samples).T

    def is_harmful(self) -> bool:
        median = np.median(self.draws, axis=0)
        return np.sum(median[self.effective_index] >= 0) > 0.5*np.sum(self.effective_index)

    def get_score(self, use_gamma_ub: bool = False) -> float:
        if self.is_harmful():
            draw = self.wider_draw_lb if use_gamma_ub else self.draw_lb
            score = draw[self.effective_index].mean()
        else:
            draw = self.wider_draw_ub if use_gamma_ub else self.draw_ub
            score = -draw[self.effective_index].mean()
        return score

    def plot_data(self, ax=None):
        if ax is None:
            fig = plt.figure()
            ax = fig.add_subplot()
        data = self.model.data
        w = self.model.w_soln
        trim_index = w <= 0.1
        ax.scatter(self.data_cont, data.obs, c='gray', s=5.0/data.obs_se, alpha=0.5)
        ax.scatter(self.data_cont[trim_index], data.obs[trim_index],
                   c='red', marker='x', s=5.0/data.obs_se[trim_index])

    def plot_model(self,
                   ax=None,
                   title: str = None,
                   xlabel: str = None,
                   ylabel: str = 'ln relative risk',
                   xlim: tuple = None,
                   ylim: tuple = None,
                   xscale: str = None,
                   yscale: str = None,
                   folder: Union[str, Path] = None):

        if ax is None:
            fig = plt.figure()
            ax = fig.add_subplot()
        draws_median = np.median(self.draws, axis=0)

        ax.plot(self.pred_cont, draws_median, color='#69b3a2', linewidth=1)
        ax.fill_between(self.pred_cont, self.draw_lb, self.draw_ub, color='#69b3a2', alpha=0.2)
        ax.fill_between(self.pred_cont, self.wider_draw_lb, self.wider_draw_ub, color='#69b3a2', alpha=0.2)
        ax.axvline(self.cont_lb, linestyle='--', color='k', linewidth=1)
        ax.axvline(self.cont_ub, linestyle='--', color='k', linewidth=1)
        ax.axhline(0.0, linestyle='--', color='k', linewidth=1)

        title = self.name if title is None else title
        score = self.get_score()
        low_score = self.get_score(use_gamma_ub=True)
        title = f"{title}: score = ({low_score: .3f}, {score: .3f})"

        ax.set_title(title, loc='left')
        xlabel = self.cont_cov_name if xlabel is None else xlabel
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
