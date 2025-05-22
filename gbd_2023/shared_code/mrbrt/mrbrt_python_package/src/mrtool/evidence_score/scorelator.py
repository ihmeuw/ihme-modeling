"""
    scorelator
    ~~~~~~~~~~
"""
from typing import List, Union
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from scipy.integrate import trapz
from .continuous import ContinuousScorelator
from .dichotomous import DichotomousScorelator
from .mixed import MixedScorelator
from .loglinear import LoglinearScorelator


class Scorelator:
    """Evaluate the score of the result.
    Warning: This is specifically designed for the relative risk application.
    Haven't been tested for others.
    """
    def __init__(self,
                 ln_rr_draws: np.ndarray,
                 exposures: np.ndarray,
                 exposure_domain: Union[List[float], None] = None,
                 ref_exposure: Union[float, None] = None,
                 score_type: str = 'area'):
        """Constructor of Scorelator.

        Args:
            ln_rr_draws (np.ndarray):
                Draws for log relative risk, each row corresponds to one draw.
            exposures (np.ndarray):
                Exposures pair with the `ln_rr_draws`, its size should match
                the number of columns of `ln_rr_draws`.
            exposure_domain (Union[List[float], None], optional):
                The exposure domain that will be considered for the score evaluation.
                If `None`, consider the whole domain defined by `exposure`.
                Default to None.
            ref_exposure (Union[float, None], optional):
                Reference exposure, the `ln_rr_draws` will be normalized to this point.
                All draws in `ln_rr_draws` will have value 0 at `ref_exposure`.
                If `None`, `ref_exposure` will be set to the minimum value of `exposures`.
                Default to None.
            score_type (str, optional):
                If ``'area'``, use area between curves to determine the score, if ``'point'``,
                use the ratio between line segments at point exposure to determine the score.
        """
        self.ln_rr_draws = np.array(ln_rr_draws)
        self.exposures = np.array(exposures)
        self.exposure_domain = [np.min(self.exposures),
                                np.max(self.exposures)] if exposure_domain is None else exposure_domain
        self.ref_exposure = np.min(self.exposures) if ref_exposure is None else ref_exposure

        self.num_draws = self.ln_rr_draws.shape[0]
        self.num_exposures = self.exposures.size

        # normalize the ln_rr_draws
        if self.ln_rr_draws.shape[1] > 1:
            self.ln_rr_draws = self.normalize_ln_rr_draws()
        self.rr_draws = np.exp(self.ln_rr_draws)
        self.rr_type = 'harmful' if self.rr_draws[:, -1].mean() >= 1.0 else 'protective'

        self.score_type = score_type
        self._check_inputs()

    def _check_inputs(self):
        """Check the inputs type and value.
        """
        if self.exposures.size != self.ln_rr_draws.shape[1]:
            raise ValueError(f"Size of the exposures ({self.exposures.size}) should be consistent"
                             f"with number of columns of ln_rr_draws ({self.ln_rr_draws.shape[1]})")

        if self.exposure_domain[0] > self.exposure_domain[1]:
            raise ValueError(f"Lower bound of exposure_domain ({self.exposure_domain[0]}) should be"
                             f"less or equal than upper bound ({self.exposure_domain[1]})")

        if not any((self.exposures >= self.exposure_domain[0]) &
                   (self.exposures <= self.exposure_domain[1])):
            raise ValueError("No exposures in the exposure domain.")

        if self.ref_exposure < np.min(self.exposures) or self.ref_exposure > np.max(self.exposures):
            raise ValueError("reference exposure should be within the range of exposures.")

        if not self.score_type in ['area', 'point']:
            raise ValueError("score_type has to been chosen from 'area' or 'point'.")

    def normalize_ln_rr_draws(self):
        """Normalize log relative risk draws.
        """
        shift = np.array([
            np.interp(self.ref_exposure, self.exposures, ln_rr_draw)
            for ln_rr_draw in self.ln_rr_draws
        ])
        return self.ln_rr_draws - shift[:, None]

    def get_evidence_score(self,
                           lower_draw_quantile: float = 0.025,
                           upper_draw_quantile: float = 0.975,
                           path_to_diagnostic: Union[str, Path, None] = None) -> float:
        """Get evidence score.

        Args:
            lower_draw_quantile (float, optional): Lower quantile of the draw for the score.
            upper_draw_quantile (float, optioanl): Upper quantile of the draw for the score.
            path_to_diagnostic (Union[str, Path, None], optional):
                Path of where the picture is saved, if None the plot will not be saved.
                Default to None.

        Returns:
            float: Evidence score.
        """
        rr_mean = np.median(self.rr_draws, axis=0)
        rr_lower = np.quantile(self.rr_draws, lower_draw_quantile, axis=0)
        rr_upper = np.quantile(self.rr_draws, upper_draw_quantile, axis=0)

        valid_index = (self.exposures >= self.exposure_domain[0]) & (self.exposures <= self.exposure_domain[1])

        if self.score_type == 'area':
            ab = seq_area_between_curves(rr_lower, rr_upper)
        else:
            ab = rr_upper - rr_lower
        if self.rr_type == 'protective':
            if self.score_type == 'area':
                abc = seq_area_between_curves(rr_lower, np.ones(self.num_exposures))
            else:
                abc = 1.0 - rr_lower
        elif self.rr_type == 'harmful':
            if self.score_type == 'area':
                abc = seq_area_between_curves(np.ones(self.num_exposures), rr_upper)
            else:
                abc = rr_upper - 1.0
        else:
            raise ValueError('Unknown relative risk type.')
        score = np.round((ab/abc)[valid_index].min(), 2)

        # plot diagnostic
        if path_to_diagnostic is not None:
            fig, ax = plt.subplots(1, 2, figsize=(22, 8.5))
            # plot rr uncertainty
            if self.exposures.size == 1:
                ax[0].boxplot(self.rr_draws,
                              meanline=True,
                              whis=(lower_draw_quantile*100, upper_draw_quantile*100),
                              positions=self.exposures)
            else:
                ax[0].fill_between(self.exposures, rr_lower, rr_upper, color='#69b3a2', alpha=0.3)
                ax[0].plot(self.exposures, rr_mean, color='#69b3a2')
                ax[0].set_xlim(np.min(self.exposures), np.max(self.exposures))
                ax[0].set_ylim(np.min(rr_lower) - rr_mean.ptp()*0.1, np.max(rr_upper) + rr_mean.ptp()*0.1)
            ax[0].axhline(1, color='#003333', alpha=0.5)
            # compute coordinate of annotation of A, B and C
            if self.rr_type == 'protective':
                self.annotate_between_curve('A', self.exposures, rr_lower, rr_mean, ax[0])
                self.annotate_between_curve('B', self.exposures, rr_mean, rr_upper, ax[0])
                self.annotate_between_curve('C', self.exposures, rr_upper, np.ones(self.num_exposures),
                                            ax[0], mark_area=True)
            else:
                self.annotate_between_curve('A', self.exposures, rr_mean, rr_upper, ax[0])
                self.annotate_between_curve('B', self.exposures, rr_lower, rr_mean, ax[0])
                self.annotate_between_curve('C', self.exposures, np.ones(self.num_exposures), rr_lower,
                                            ax[0], mark_area=True)

            # plot the score as function of exposure
            if self.exposures.size == 1:
                ax[1].scatter(self.exposures, ab/abc,
                              color='dodgerblue',
                              label=f'A+B / A+B+C: {score}')
            else:
                ax[1].plot(self.exposures, ab/abc,
                           color='dodgerblue',
                           label=f'A+B / A+B+C: {score}')
                ax[1].set_xlim(np.min(self.exposures), np.max(self.exposures))
            ax[1].legend()
            ax[1].legend(loc=1, fontsize='x-large')

            # plot the exposure domain
            ax[0].axvline(self.exposure_domain[0], linestyle='--', color='#003333')
            ax[0].axvline(self.exposure_domain[1], linestyle='--', color='#003333')
            ax[1].axvline(self.exposure_domain[0], linestyle='--', color='#003333')
            ax[1].axvline(self.exposure_domain[1], linestyle='--', color='#003333')

            plt.savefig(path_to_diagnostic, bbox_inches='tight')

        return score

    @staticmethod
    def annotate_between_curve(annotation: str,
                               x: np.ndarray, y_lower: np.ndarray, y_upper: np.ndarray, ax: Axes,
                               mark_area: bool = False):
        """Annotate between the curve.

        Args:
            annotation (str): the annotation between the curve.
            x (np.ndarray): independent variable.
            y_lower (np.ndarray): lower bound of the curve.
            y_upper (np.ndarray): upper bound of the curve.
            ax (Axes): axis of the plot.
            mark_area (bool, optional): If True mark the area. Default to False.
        """
        y_diff = y_upper - y_lower
        label_index = np.argmax(y_diff)
        if label_index > 0.95*len(x) or label_index < 0.05*len(x):
            label_index = int(0.4*len(x))
        label_x = x[label_index]
        label_y = 0.5*(y_lower[label_index] + y_upper[label_index])
        if label_y < y_upper[label_index]:
            ax.text(label_x, label_y, annotation, color='dodgerblue',
                    horizontalalignment='center', verticalalignment='center', size=20)

        if mark_area:
            ax.fill_between(x, y_lower, y_upper, linestyle='--', where=y_lower < y_upper,
                            edgecolor='black', facecolor="none", alpha=0.5)



def area_between_curves(lower: np.ndarray,
                        upper: np.ndarray,
                        ind_var: Union[np.ndarray, None] = None,
                        normalize_domain: bool = True) -> float:
    """Compute area between curves.

    Args:
        lower (np.ndarray): Lower bound curve.
        upper (np.ndarray): Upper bound curve.
        ind_var (Union[np.ndarray, None], optional):
            Independent variable, if `None`, it will assume sample points are
            evenly spaced. Default to None.
        normalize_domain (bool, optional):
            If `True`, when `ind_var` is `None`, will normalize domain to 0 and 1.
            Default to True.

    Returns:
        float: Area between curves.
    """
    assert upper.size == lower.size, "Vectors for lower and upper curve should have same size."
    if ind_var is not None:
        assert ind_var.size == lower.size, "Independent variable size should be consistent with the curve vector."
    assert lower.size >= 2, "At least need to have two points to compute interval."

    diff = upper - lower
    if ind_var is not None:
        return trapz(diff, x=ind_var)
    else:
        if normalize_domain:
            dx = 1.0/(diff.size - 1)
        else:
            dx = 1.0
        return trapz(diff, dx=dx)


def seq_area_between_curves(lower: np.ndarray,
                            upper: np.ndarray,
                            ind_var: Union[np.ndarray, None] = None,
                            normalize_domain: bool = True) -> np.ndarray:
    """Sequence areas_between_curves.

    Args: Please check the inputs for area_between_curves.

    Returns:
        np.ndarray:
            Return areas defined from the first two points of the curve
            to the whole curve.
    """
    if ind_var is None:
        area = np.array([area_between_curves(
            lower[:(i+1)],
            upper[:(i+1)],
            normalize_domain=normalize_domain)
            for i in range(1, lower.size)
        ])
    else:
        area = np.array([area_between_curves(
            lower[:(i + 1)],
            upper[:(i + 1)],
            ind_var[:(i + 1)],
            normalize_domain=normalize_domain)
            for i in range(1, lower.size)
        ])
    return np.hstack((area[0], area))
