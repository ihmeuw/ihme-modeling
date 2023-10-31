"""
Scorelator
"""
import os
from pathlib import Path
from typing import Tuple, Union
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import norm
from .model import CWModel


class Scorelator:
    def __init__(self, model: CWModel,
                 draw_bounds: Tuple[float, float] = (0.05, 0.95),
                 type: str = 'harmful',
                 name: str = 'unknown'):
        self.model = model
        self.draw_bounds = draw_bounds
        self.type = type
        self.name = name

        self.cov_names = model.get_cov_names()
        self.intercept_index = self.cov_names.index("intercept") + \
                               np.arange(model.cwdata.num_dorms - 1)*model.num_vars_per_dorm
        beta = np.delete(model.beta, model.var_idx[model.gold_dorm])
        beta_sd = np.delete(model.beta_sd, model.var_idx[model.gold_dorm])
        gamma_fisher = model.lt.get_gamma_fisher(model.gamma)[0, 0]
        self.beta = beta[self.intercept_index]
        self.beta_sd = beta_sd[self.intercept_index]
        self.gamma = model.gamma[0]
        self.gamma_sd = 1.0/np.sqrt(gamma_fisher)

        gamma_ub = self.gamma + 2.0*self.gamma_sd

        self.draw_lb = self.beta + norm.ppf(self.draw_bounds[0], scale=np.sqrt(self.gamma + self.beta_sd**2))
        self.draw_ub = self.beta + norm.ppf(self.draw_bounds[1], scale=np.sqrt(self.gamma + self.beta_sd**2))
        self.wider_draw_lb = self.beta + norm.ppf(self.draw_bounds[0], scale=np.sqrt(gamma_ub + self.beta_sd**2))
        self.wider_draw_ub = self.beta + norm.ppf(self.draw_bounds[1], scale=np.sqrt(gamma_ub + self.beta_sd**2))

    def get_score(self, use_gamma_ub: bool = False) -> float:
        if use_gamma_ub:
            score = self.wider_draw_lb if self.type == 'harmful' else -self.wider_draw_ub
        else:
            score = self.draw_lb if self.type == 'harmful' else -self.draw_ub
        return score

    def plot_model(self,
                   ax=None,
                   title: str = None,
                   xlabel: str = 'definitions or methods',
                   ylabel: str = 'ln relative risk',
                   xlim: tuple = None,
                   ylim: tuple = None,
                   xscale: str = None,
                   yscale: str = None,
                   folder: Union[str, Path] = None):
        if ax is None:
            fig = plt.figure(figsize=(4*(self.model.cwdata.num_dorms - 1), 5))
            ax = fig.add_subplot()
        data = self.model.cwdata
        alt_dorms = np.array([dorm for i in range(data.num_obs) for dorm in data.alt_dorms[i]])
        obs_ref = np.array([
            np.sum([self.model.beta[self.model.var_idx[dorm]]
                    for dorm in data.ref_dorms[i]])
            for i in range(data.num_obs)
        ])
        obs = data.obs + obs_ref

        dorms = np.delete(data.unique_dorms, list(data.unique_dorms).index(self.model.gold_dorm))
        beta_sort_id = np.argsort(self.beta)
        if self.type == 'protective':
            beta_sort_id = beta_sort_id[::-1]
        dorms = dorms[beta_sort_id]

        for i, dorm in enumerate(dorms):
            index = alt_dorms == dorm
            beta_index = beta_sort_id[i]
            lb = i - 0.49
            ub = i + 0.49
            x = np.random.uniform(low=lb, high=ub, size=np.sum(index))
            ax.scatter(x, obs[index], s=2.0/data.obs_se[index], color='gray', alpha=0.5)
            ax.fill_between([lb, ub],
                            [self.draw_lb[beta_index], self.draw_lb[beta_index]],
                            [self.draw_ub[beta_index], self.draw_ub[beta_index]], color='#69b3a2', alpha=0.2)
            ax.fill_between([lb, ub],
                            [self.wider_draw_lb[beta_index], self.wider_draw_lb[beta_index]],
                            [self.wider_draw_ub[beta_index], self.wider_draw_ub[beta_index]],
                            color='#69b3a2', alpha=0.2)

        ax.axhline(0.0, color='red', linestyle='--', linewidth=1.0)
        title = self.name if title is None else title
        score = np.round(self.get_score(), 3)
        low_score = np.round(self.get_score(use_gamma_ub=True), 3)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.set_title(f"{title}: ref = {self.model.gold_dorm}\n"
                     f"categories: {dorms}\n"
                     f"low scores: {list(low_score[beta_sort_id])}\n"
                     f"scores: {list(score[beta_sort_id])}", loc='left')
        ax.set_xticks(np.arange(data.num_dorms - 1))
        ax.set_xticklabels(dorms)

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

