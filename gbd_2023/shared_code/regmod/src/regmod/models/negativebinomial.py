"""
Negative Binomial Model
"""

# pylint: disable=no-name-in-module

import numpy as np
from scipy.stats import nbinom
from scipy.special import digamma, loggamma, polygamma
from regmod.data import Data
from regmod._typing import NDArray

from .model import Model


class NegativeBinomialModel(Model):
    param_names: list[str] = ("n", "p")
    default_param_specs = {"n": {"inv_link": "exp"}, "p": {"inv_link": "expit"}}

    def __init__(self, data: Data, **kwargs):
        if not np.all(data.obs >= 0):
            raise ValueError(
                "Negative-Binomial model requires observations to be non-negative."
            )
        super().__init__(data, **kwargs)

    def nll(self, params: list[NDArray]) -> NDArray:
        return -(
            loggamma(params[0] + self.data.obs)
            - loggamma(params[0])
            + self.data.obs * np.log(1 - params[1])
            + params[0] * np.log(params[1])
        )

    def dnll(self, params: list[NDArray]) -> list[NDArray]:
        return [
            -(
                digamma(params[0] + self.data.obs)
                - digamma(params[0])
                + np.log(params[1])
            ),
            self.data.obs / (1 - params[1]) - params[0] / params[1],
        ]

    def d2nll(self, params: list[NDArray]) -> list[list[NDArray]]:
        return [
            [
                polygamma(1, params[0]) - polygamma(1, params[0] + self.data.obs),
                -1 / params[1],
            ],
            [
                -1 / params[1],
                params[0] / params[1] ** 2 + self.data.obs / (1 - params[1]) ** 2,
            ],
        ]

    def get_ui(self, params: list[NDArray], bounds: tuple[float, float]) -> NDArray:
        n = params[0]
        p = params[1]
        return [nbinom.ppf(bounds[0], n=n, p=p), nbinom.ppf(bounds[1], n=n, p=p)]
