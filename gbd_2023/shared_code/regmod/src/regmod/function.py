"""
Function module
"""

from dataclasses import dataclass, field

import numpy as np
from regmod._typing import Callable


@dataclass
class SmoothFunction:
    """Smooth function class bundle function and its derivative information
    together.

    Parameters
    ----------
    name : str
        Name of the function.
    fun : Callable
        The fuction require to accept numpy array as the input and output should
        have the same shape with the input.
    inv_fun : Callable
        Inverse of the function.
    dfun : Callable
        Derivative of the function.
    d2fun : Callable
        Second derivative of the function.
    """

    name: str
    fun: Callable = field(repr=False)
    inv_fun: Callable = field(repr=False)
    dfun: Callable = field(repr=False)
    d2fun: Callable = field(default=None, repr=False)


def identity_fun(x):
    return x


def identity_dfun(x):
    return 1.0 if np.isscalar(x) else np.ones(len(x))


def identity_d2fun(x):
    return 0.0 if np.isscalar(x) else np.zeros(len(x))


def exp_fun(x):
    return np.exp(x)


def exp_dfun(x):
    return np.exp(x)


def exp_d2fun(x):
    return np.exp(x)


def expit_fun(x):
    neg_indices = x < 0
    z = np.exp(-np.sqrt(x * x))
    y = 1 / (1 + z)
    if np.isscalar(x):
        if neg_indices:
            y = 1 - y
    else:
        y[neg_indices] = 1 - y[neg_indices]
    return y


def expit_dfun(x):
    z = np.exp(-np.sqrt(x * x))
    y = z / (1 + z) ** 2
    return y


def expit_d2fun(x):
    neg_indices = x < 0
    z = np.exp(-np.sqrt(x * x))
    y = z * (z - 1) / (z + 1) ** 3
    if np.isscalar(x):
        if neg_indices:
            y = -y
    else:
        y[neg_indices] = -y[neg_indices]
    return y


def log_fun(x):
    return np.log(x)


def log_dfun(x):
    return 1 / x


def log_d2fun(x):
    return -1 / x**2


def logit_fun(x):
    return np.log(x / (1 - x))


def logit_dfun(x):
    return 1 / (x * (1 - x))


def logit_d2fun(x):
    return (2 * x - 1) / (x * (1 - x)) ** 2


fun_list = [
    SmoothFunction(
        name="identity",
        fun=identity_fun,
        inv_fun=identity_fun,
        dfun=identity_dfun,
        d2fun=identity_d2fun,
    ),
    SmoothFunction(
        name="exp", fun=exp_fun, inv_fun=log_fun, dfun=exp_dfun, d2fun=exp_d2fun
    ),
    SmoothFunction(
        name="expit",
        fun=expit_fun,
        inv_fun=logit_fun,
        dfun=expit_dfun,
        d2fun=expit_d2fun,
    ),
    SmoothFunction(
        name="log", fun=log_fun, inv_fun=exp_fun, dfun=log_dfun, d2fun=log_d2fun
    ),
    SmoothFunction(
        name="logit",
        fun=logit_fun,
        inv_fun=expit_fun,
        dfun=logit_dfun,
        d2fun=logit_d2fun,
    ),
]


fun_dict = {fun.name: fun for fun in fun_list}
