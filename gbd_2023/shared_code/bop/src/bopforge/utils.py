"""
Ultility functions
"""
import argparse
from typing import Any, Dict, Tuple, Optional

import numpy as np
from mrtool import MRBRT, MRBeRT, MRData


def fill_dict(des_dict: Dict, default_dict: Dict) -> Dict:
    """Fill given dictionary by the default dictionary.

    Parameters
    ----------
    des_dict : Dict
        Given dictionary that needs to be filled.
    default_dict : Dict
        Default dictionary

    Returns
    -------
    Dict
        Updated dictionary.
    """
    for key, value in default_dict.items():
        if key not in des_dict:
            des_dict[key] = value
        elif isinstance(value, dict):
            des_dict[key] = fill_dict(des_dict[key], value)
    return des_dict


def get_beta_info(model: MRBRT, cov_name: str = "signal") -> Tuple[float, float]:
    """Get the posterior information of beta.

    Parameters
    ----------
    model : MRBRT
        MRBRT model, preferably is a simple linear mixed effects model.
    cov_name : str, optional
        Name of the interested covariates, default to be "signal".

    Returns
    -------
    Tuple[float, float]
        Return the mean and standard deviation of the corresponding beta.
    """
    lt = model.lt
    index = model.cov_names.index(cov_name)
    beta = model.beta_soln[index]
    beta_hessian = lt.hessian(lt.soln)[: lt.k_beta, : lt.k_beta]
    beta_sd = 1.0 / np.sqrt(beta_hessian[index, index])
    return (beta, beta_sd)


def get_gamma_info(model: MRBRT) -> Tuple[float, float]:
    """Get the posterior information of gamma.

    Parameters
    ----------
    model : MRBRT
        MRBRT model, preferably is a simple linear mixed effects model. Requires
        only have one random effect.

    Returns
    -------
    Tuple[float, float]
        Return the mean and standard deviation of the corresponding gamma.
    """
    lt = model.lt
    gamma = model.gamma_soln[0]
    gamma_fisher = lt.get_gamma_fisher(lt.gamma)
    gamma_sd = 1.0 / np.sqrt(gamma_fisher[0, 0])
    return (gamma, gamma_sd)


def get_signal(signal_model: MRBeRT, risk: np.ndarray) -> np.ndarray:
    """Get signal from signal_model

    Parameters
    ----------
    signal_model : MRBeRT
        Signal model object.
    risk : np.ndarray
        Risk exposures that we want to create signal on.
    """
    return signal_model.predict(
        MRData(
            covs={
                "ref_risk_lower": np.repeat(risk.min(), len(risk)),
                "ref_risk_upper": np.repeat(risk.min(), len(risk)),
                "alt_risk_lower": risk,
                "alt_risk_upper": risk,
            }
        )
    )


class ParseKwargs(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: list[str],
        option_string: Optional[str] = None,
    ):
        """Parse keyword arguments into a dictionary. Be sure to set `nargs='*'`
        in the ArgumentParser to parse the input as a list of strings, otherwise
        this function will break. If provided string does not the form of
        '{key}={value}', an error will be raised.

        Example
        -------

        .. code-block:: python

            parser = ArgumentParser()
            parser.add_argument(
                "-m",
                "--metadata",
                nargs="*",
                required=False,
                default={},
                action=ParseKwargs,
            )

        """
        data = dict()
        for value in values:
            data_key, _, data_value = value.partition("=")
            if (not data_key) or (not data_value):
                raise ValueError(
                    "please provide kwargs in the form of {key}={value}, "
                    f"current input is '{value}'"
                )
            data[data_key] = data_value
        setattr(namespace, self.dest, data)
