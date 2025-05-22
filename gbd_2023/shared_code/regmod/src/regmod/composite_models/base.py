"""
Base Model
"""
import logging
from copy import deepcopy
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from pandas import DataFrame
from regmod.composite_models.interface import NodeModel
from regmod.data import Data
from regmod.function import fun_dict
from regmod.models import BinomialModel, GaussianModel, PoissonModel
from regmod.prior import GaussianPrior
from regmod.utils import sizes_to_slices
from regmod.variable import Variable

logger = logging.getLogger(__name__)

link_funs = {
    "gaussian": fun_dict[
        GaussianModel.default_param_specs["mu"]["inv_link"]
    ].inv_fun,
    "poisson": fun_dict[
        PoissonModel.default_param_specs["lam"]["inv_link"]
    ].inv_fun,
    "binomial": fun_dict[
        BinomialModel.default_param_specs["p"]["inv_link"]
    ].inv_fun,
}

model_constructors = {
    "gaussian": lambda data, param_specs: GaussianModel(
        data, param_specs={"mu": param_specs}
    ),
    "poisson": lambda data, param_specs: PoissonModel(
        data, param_specs={"lam": param_specs}
    ),
    "binomial": lambda data, param_specs: BinomialModel(
        data, param_specs={"p": param_specs}
    ),
}


class BaseModel(NodeModel):
    """Base Model, a simple wrapper around the stats model.

    Parameters
    ----------
    name : str
        Name of the model
    data : Data
        The data object for the model.
    variables : List[Variable]
        A list of variables used in the model.
    mtype : str, optional
        Type of the model, please see model constructors for options. Default
        to be `"gaussian"`.
    prior_mask : Optional[Dict], optional
        Masks for the priors. Default to be None. When it is None prior won't be
        modified.

    Attributes
    ----------
    name : str
        Name of the model
    data : Data
        The data object for the model.
    variables : List[Variable]
        A list of variables used in the model.
    mtype : str
        Type of the model.
    param_specs : Dict
        Specifications of the parameter.
    model : Model
        `regmod.models.Model` instance with the given specification.
    prior_mask : Dict
        Masks for the priors.

    Methods
    -------
    add_offset(df, copy=False)
        Add offset to the given data frame.
    append(node, rank=0)
        Overwrite the append function in NodeModel.
    """

    def __init__(self,
                 name: str,
                 data: Data,
                 variables: List[Variable],
                 mtype: str = "gaussian",
                 prior_mask: Optional[Dict] = None,
                 **param_specs):

        super().__init__(name)
        if any(mtype not in model_config
               for model_config in (link_funs, model_constructors)):
            raise ValueError(f"Not supported model type {mtype}")
        data = deepcopy(data)
        variables = list(deepcopy(variables))

        self.mtype = mtype
        self.data = data
        self.variables = {v.name: v for v in variables}
        self.param_specs = {"variables": variables,
                            "use_offset": True,
                            **param_specs}
        self.model = None
        self.prior_mask = {} if prior_mask is None else prior_mask

    def add_offset(self, df: DataFrame, copy: bool = False) -> DataFrame:
        """Add offset to the given data frame.

        Parameters
        ----------
        df : DataFrame
            Given data frame.
        copy : bool, optional
            If True return a copy of the given data frame, by default False.

        Returns
        -------
        DataFrame
            Data frame with the offset.
        """
        df = df.copy() if copy else df
        if self.col_value in df:
            df[self.data.col_offset] = link_funs[self.mtype](df[self.col_value])
        return df

    def get_data(self) -> DataFrame:
        return self.data.df.copy()

    def set_data(self, df: DataFrame):
        if df.shape[0] == 0:
            raise ValueError("Attempt to use empty dataframe.")
        self.data.attach_df(self.add_offset(df, copy=True))

    def fit(self, **fit_options):
        logger.info(f"fit_node;start;{self.level};{self.name}")
        if self.model is None:
            model_constructor = model_constructors[self.mtype]
            self.model = model_constructor(self.data, self.param_specs)
        self.model.fit(**fit_options)
        message = f"fit_node;finish;{self.level};{self.name};"
        # message += f"{self.model.opt_result.success};"
        # message += f"{self.model.opt_result.niter}"
        logger.info(message)

    def predict(self, df: DataFrame = None):
        df = self.get_data() if df is None else df.copy()
        df = self.add_offset(df)

        pred_data = self.model.data.copy()
        pred_data.attach_df(df)

        df[self.col_value] = self.model.params[0].get_param(
            self.model.opt_coefs, pred_data
        )
        return df

    def get_draws(self, df: DataFrame = None, size: int = 1000) -> DataFrame:
        df = self.get_data() if df is None else df.copy()
        df = self.add_offset(df)

        pred_data = self.model.data.copy()
        pred_data.attach_df(df)

        coefs_draws = np.random.multivariate_normal(self.model.opt_coefs,
                                                    self.model.opt_vcov,
                                                    size=size)
        draws = np.vstack([
            self.model.params[0].get_param(coefs_draw, pred_data)
            for coefs_draw in coefs_draws
        ])
        df_draws = pd.DataFrame(
            draws.T,
            columns=[f"{self.col_value}_{i}" for i in range(size)],
            index=df.index
        )

        return pd.concat([df, df_draws], axis=1)

    def set_prior(self, priors: Dict[str, List]):
        priors = deepcopy(priors)
        for name, prior in priors.items():
            if name in self.prior_mask:
                prior.sd *= self.prior_mask[name]
            self.variables[name].add_priors(prior)
        self.model = model_constructors[self.mtype](self.data, self.param_specs)

    def set_prior_mask(self, masks: Dict):
        for name, mask in masks.items():
            if name in self.variables:
                if mask.size != self.variables[name]:
                    raise ValueError("Prior mask size not compatible.")
                self.prior_mask[name] = mask

    def get_posterior(self) -> Dict:
        if self.model.opt_coefs is None:
            raise AttributeError("Please fit the model first.")
        mean = self.model.opt_coefs
        # use minimum standard deviation of the posterior distribution
        sd = np.maximum(0.1, np.sqrt(np.diag(self.model.opt_vcov)))
        vnames = [v.name for v in self.param_specs["variables"]]
        slices = sizes_to_slices([self.variables[name].size
                                  for name in vnames])
        return {
            name: GaussianPrior(mean=mean[slices[i]], sd=sd[slices[i]])
            for i, name in enumerate(vnames)
        }

    def append(self, node: NodeModel, rank: int = 0):
        """Overwrite the append function in NodeModel.

        Parameters
        ----------
        node : NodeModel
            The node model to be appended node.
        rank : int, optional
            Indicate which compartment to append to, by default 0.

        Raises
        ------
        ValueError
            Raised if rank is greater than or equals to 1. BaseModel can only
            primary children.
        """
        if rank >= 1:
            raise ValueError(f"{type(self).__name__} can only have primary "
                             "link.")
        super().append(node, rank=rank)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={self.name}, mtype={self.mtype})"
