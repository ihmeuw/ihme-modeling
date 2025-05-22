"""
Dichotomous scorelator
"""
import os
from pathlib import Path
from typing import Tuple, Union
import numpy as np
from scipy.stats import norm
import matplotlib.pyplot as plt
from mrtool import MRBRT
from mrtool.core.other_sampling import extract_simple_lme_specs, extract_simple_lme_hessian


class DichotomousScorelator:
    def __init__(self,
                 model: MRBRT,
                 cov_name: str = 'intercept',
                 draw_bounds: Tuple[float, float] = (0.05, 0.95),
                 name: str = 'unknown'):
        self.model = model
        self.cov_name = cov_name
        self.draw_bounds = draw_bounds
        self.cov_index = self.model.get_cov_model_index(self.cov_name)
        self.name = name

        x_ids = self.model.x_vars_indices[self.cov_index]
        z_ids = self.model.z_vars_indices[self.cov_index]
        self.beta = self.model.beta_soln[x_ids][0]
        self.gamma = self.model.gamma_soln[z_ids][0]

        # compute the fixed effects uncertainty
        model_specs = extract_simple_lme_specs(self.model)
        beta_var = np.linalg.inv(extract_simple_lme_hessian(model_specs))
        self.beta_var = beta_var[np.ix_(x_ids, x_ids)][0, 0]

        # compute the random effects uncertainty
        lt = self.model.lt
        gamma_fisher = lt.get_gamma_fisher(lt.gamma)
        gamma_var = np.linalg.inv(gamma_fisher)
        self.gamma_var = gamma_var[np.ix_(z_ids, z_ids)][0, 0]

        # compute score
        gamma_ub = self.gamma + 2.0*np.sqrt(self.gamma_var)
        self.draw_lb = self.beta + norm.ppf(self.draw_bounds[0], scale=np.sqrt(self.gamma + self.beta_var))
        self.draw_ub = self.beta + norm.ppf(self.draw_bounds[1], scale=np.sqrt(self.gamma + self.beta_var))
        self.wider_draw_lb = self.beta + norm.ppf(self.draw_bounds[0], scale=np.sqrt(gamma_ub + self.beta_var))
        self.wider_draw_ub = self.beta + norm.ppf(self.draw_bounds[1], scale=np.sqrt(gamma_ub + self.beta_var))

    def is_harmful(self) -> bool:
        return self.beta > 0.0

    def get_score(self, use_gamma_ub: bool = False) -> float:
        if use_gamma_ub:
            score = self.wider_draw_lb if self.is_harmful() else -self.wider_draw_ub
        else:
            score = self.draw_lb if self.is_harmful() else -self.draw_ub
        return score

    def plot_model(self,
                   ax=None,
                   title: str = None,
                   xlabel: str = 'ln relative risk',
                   ylabel: str = 'ln relative risk se',
                   xlim: tuple = None,
                   ylim: tuple = None,
                   xscale: str = None,
                   yscale: str = None,
                   folder: Union[str, Path] = None):
        if ax is None:
            fig = plt.figure()
            ax = fig.add_subplot()
        data = self.model.data
        trim_index = self.model.w_soln <= 0.1
        max_obs_se = np.max(data.obs_se)*1.1
        ax.set_ylim(max_obs_se, 0.0)
        ax.fill_betweenx([0.0, max_obs_se],
                         [self.beta, self.beta - 1.96*max_obs_se],
                         [self.beta, self.beta + 1.96*max_obs_se], color='#B0E0E6', alpha=0.4)
        obs = data.obs.copy()
        for i, cov_name in enumerate(self.model.cov_names):
            if cov_name == 'intercept':
                continue
            obs -= data.covs[cov_name]*self.model.beta_soln[i]
        ax.scatter(obs, data.obs_se, color='gray', alpha=0.4)
        ax.scatter(obs[trim_index],
                   data.obs_se[trim_index], color='red', marker='x', alpha=0.4)
        ax.plot([self.beta, self.beta - 1.96*max_obs_se], [0.0, max_obs_se],
                linewidth=1, color='#87CEFA')
        ax.plot([self.beta, self.beta + 1.96*max_obs_se], [0.0, max_obs_se],
                linewidth=1, color='#87CEFA')

        ax.axvline(0.0, color='r', linewidth=1, linestyle='--')
        ax.axvline(self.beta, color='k', linewidth=1, linestyle='--')
        ax.axvline(self.draw_lb, color='#69b3a2', linewidth=1)
        ax.axvline(self.draw_ub, color='#69b3a2', linewidth=1)
        ax.axvline(self.wider_draw_lb, color='#256b5f', linewidth=1)
        ax.axvline(self.wider_draw_ub, color='#256b5f', linewidth=1)

        title = self.name if title is None else title
        score = self.get_score()
        low_score = self.get_score(use_gamma_ub=True)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.set_title(f"{title}: score = ({low_score: .3f}, {score: .3f})", loc='left')

        if xlim is not None:
            ax.set_xlim(*xlim)
        if ylim is not None:
            ax.set_ylim(*ylim)
        if xscale is not None:
            ax.set_xscale(xscale)
        if yscale is not None:
            ax.set_yscale(yscale)

        if folder is not None:
            folder = Path(folder)
            if not folder.exists():
                os.mkdir(folder)
            plt.savefig(folder/f"{self.name}.pdf", bbox_inches='tight')

        return ax
