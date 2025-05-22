"""
Continous scorelator
"""
import os
from typing import List, Tuple, Union
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
from mrtool import MRData, MRBRT, MRBeRT
from mrtool.core.other_sampling import sample_simple_lme_beta


class ContinuousScorelator:
    def __init__(self,
                 signal_model: Union[MRBRT, MRBeRT],
                 final_model: Union[MRBRT],
                 alt_cov_names: List[str],
                 ref_cov_names: List[str],
                 exposure_quantiles: Tuple[float] = (0.15, 0.85),
                 exposure_bounds: Tuple[float] = None,
                 draw_quantiles: Tuple[float] = (0.05, 0.95),
                 num_samples: int = 1000,
                 num_points: int = 100,
                 ref_exposure: Union[str, float] = None,
                 j_shaped: bool = False,
                 name: str = 'unknown'):
        self.signal_model = signal_model
        self.final_model = final_model
        self.alt_cov_names = alt_cov_names
        self.ref_cov_names = ref_cov_names
        self.exposure_quantiles = exposure_quantiles
        self.exposure_bounds = exposure_bounds
        self.draw_quantiles = draw_quantiles
        self.num_samples = num_samples
        self.num_points = num_points
        self.ref_exposure = ref_exposure
        self.j_shaped = j_shaped
        self.name = name

        exposures = self.signal_model.data.get_covs(self.alt_cov_names + self.ref_cov_names)
        self.exposure_lend = np.min(exposures)
        self.exposure_uend = np.max(exposures)
        self.alt_exposures = self.signal_model.data.get_covs(self.alt_cov_names).mean(axis=1)
        self.ref_exposures = self.signal_model.data.get_covs(self.ref_cov_names).mean(axis=1)
        self.draws = self.get_draws(num_samples=self.num_samples, num_points=self.num_points)
        self.wider_draws = self.get_draws(num_samples=self.num_samples, num_points=self.num_points,
                                          use_gamma_ub=True)
        self.pred_exposures = self.get_pred_exposures()
        self.pred = self.get_pred()

        if self.ref_exposure is not None:
            if self.ref_exposure == 'min':
                self.pred_ref_index = np.argmin(self.pred)
            else:
                self.pred_ref_index = np.argmin(np.abs(self.pred_exposures - self.ref_exposure))
            self.draws -= self.draws[:, self.pred_ref_index, None]
            self.wider_draws -= self.wider_draws[:, self.pred_ref_index, None]

        # compute the range of exposures
        self.exposure_lb = np.quantile(self.ref_exposures, self.exposure_quantiles[0])
        self.exposure_ub = np.quantile(self.alt_exposures, self.exposure_quantiles[1])
        if self.exposure_bounds is not None:
            self.exposure_lb = self.exposure_bounds[0]
            self.exposure_ub = self.exposure_bounds[1]
        self.effective_index = (self.pred_exposures >= self.exposure_lb) & (self.pred_exposures <= self.exposure_ub)

        # compute the range of the draws
        self.draw_lb = np.quantile(self.draws, self.draw_quantiles[0], axis=0)
        self.draw_ub = np.quantile(self.draws, self.draw_quantiles[1], axis=0)
        self.wider_draw_lb = np.quantile(self.wider_draws, self.draw_quantiles[0], axis=0)
        self.wider_draw_ub = np.quantile(self.wider_draws, self.draw_quantiles[1], axis=0)

    def get_signal(self,
                   alt_cov: List[np.ndarray],
                   ref_cov: List[np.ndarray]) -> np.ndarray:
        covs = {}
        for i, cov_name in enumerate(self.alt_cov_names):
            covs[cov_name] = alt_cov[i]
        for i, cov_name in enumerate(self.ref_cov_names):
            covs[cov_name] = ref_cov[i]
        data = MRData(covs=covs)
        return self.signal_model.predict(data)

    def get_pred_exposures(self, num_points: int = 100):
        return np.linspace(self.exposure_lend, self.exposure_uend, num_points)

    def get_pred_data(self, num_points: int = 100) -> MRData:
        if num_points == -1:
            alt_cov = self.ref_exposures
        else:
            alt_cov = self.get_pred_exposures(num_points=num_points)
        ref_cov = np.repeat(self.exposure_lend, alt_cov.size)
        zero_cov = np.zeros(alt_cov.size)
        signal = self.get_signal(
            alt_cov=[alt_cov for _ in self.alt_cov_names],
            ref_cov=[ref_cov for _ in self.ref_cov_names]
        )
        other_covs = {
            cov_name: zero_cov
            for cov_name in self.final_model.data.covs
            if cov_name not in ('signal', 'linear')
        }
        if not self.j_shaped:
            covs = {'signal': signal, **other_covs}
        else:
            covs = {'signal': signal, 'linear': alt_cov - ref_cov, **other_covs}
        return MRData(covs=covs)

    def get_beta_samples(self, num_samples: int) -> np.ndarray:
        return sample_simple_lme_beta(num_samples, self.final_model)

    def get_gamma_samples(self, num_samples: int) -> np.ndarray:
        return np.repeat(self.final_model.gamma_soln.reshape(1, 1),
                         num_samples, axis=0)

    def get_samples(self, num_samples: int) -> Tuple[np.ndarray, np.ndarray]:
        return self.get_beta_samples(num_samples), self.get_gamma_samples(num_samples)

    def get_gamma_sd(self) -> float:
        lt = self.final_model.lt
        gamma_fisher = lt.get_gamma_fisher(lt.gamma)
        return 1.0/np.sqrt(gamma_fisher[0, 0])

    def get_draws(self,
                  num_samples: int = 1000,
                  num_points: int = 100,
                  use_gamma_ub: bool = False) -> np.ndarray:
        data = self.get_pred_data(num_points=num_points)
        beta_samples, gamma_samples = self.get_samples(num_samples=num_samples)
        if use_gamma_ub:
            gamma_samples += 2.0*self.get_gamma_sd()
        return self.final_model.create_draws(data,
                                             beta_samples=beta_samples,
                                             gamma_samples=gamma_samples,
                                             random_study=True).T

    def is_harmful(self) -> bool:
        median = np.median(self.draws, axis=0)
        return np.sum(median[self.effective_index] >= 0) > 0.5*np.sum(self.effective_index)

    def get_pred(self, num_points: int = 100) -> np.ndarray:
        data = self.get_pred_data(num_points=num_points)
        return self.final_model.predict(data)

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
        data = self.signal_model.data
        prediction = self.get_pred(num_points=-1)
        if self.ref_exposure is not None:
            prediction -= self.pred[self.pred_ref_index]
        if isinstance(self.signal_model, MRBRT):
            w = self.signal_model.w_soln
        else:
            w = np.vstack([model.w_soln for model in self.signal_model.sub_models]).T.dot(self.signal_model.weights)
        trim_index = w <= 0.1
        ax.scatter(self.alt_exposures, prediction + data.obs,
                   c='gray', s=5.0/data.obs_se, alpha=0.5)
        ax.scatter(self.alt_exposures[trim_index], prediction[trim_index] + data.obs[trim_index],
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
                   plot_wider_draws: bool = True,
                   folder: Union[str, Path] = None):

        if ax is None:
            fig = plt.figure()
            ax = fig.add_subplot()
        draws_median = np.median(self.draws, axis=0)

        ax.plot(self.pred_exposures, draws_median, color='#69b3a2', linewidth=1)
        ax.fill_between(self.pred_exposures, self.draw_lb, self.draw_ub, color='#69b3a2', alpha=0.2)
        if plot_wider_draws:
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
