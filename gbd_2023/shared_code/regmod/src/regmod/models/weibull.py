"""
Weibull Model
"""

import numpy as np
from scipy.stats import weibull_min
from regmod.data import Data
from regmod._typing import NDArray

from .model import Model


class WeibullModel(Model):
    param_names = ("b", "k")
    default_param_specs = {"b": {"inv_link": "exp"}, "k": {"inv_link": "exp"}}

    def __init__(self, data: Data, **kwargs):
        if not all(data.obs > 0):
            raise ValueError("Weibull model requires observations to be positive.")
        super().__init__(data, **kwargs)

    def nll(self, params: list[NDArray]) -> NDArray:
        t = self.data.obs
        ln_t = np.log(t)
        return (
            params[0] * (t ** params[1])
            - (params[1] - 1) * ln_t
            - np.log(params[0])
            - np.log(params[1])
        )

    def dnll(self, params: list[NDArray]) -> list[NDArray]:
        t = self.data.obs
        ln_t = np.log(t)
        return [
            t ** params[1] - 1 / params[0],
            ln_t * params[0] * (t ** params[1]) - ln_t - 1 / params[1],
        ]

    def d2nll(self, params: list[NDArray]) -> list[list[NDArray]]:
        t = self.data.obs
        ln_t = np.log(t)
        return [
            [1 / params[0] ** 2, ln_t * (t ** params[1])],
            [
                ln_t * (t ** params[1]),
                1 / params[1] ** 2 + params[0] * (ln_t**2) * (t ** params[1]),
            ],
        ]

    def get_ui(self, params: list[NDArray], bounds: tuple[float, float]) -> NDArray:
        scale = 1 / params[0] ** (1 / params[1])
        return [
            weibull_min.ppf(bounds[0], c=params[1], scale=scale),
            weibull_min.ppf(bounds[1], c=params[1], scale=scale),
        ]
