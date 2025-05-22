"""
Composite Model
"""
from typing import Dict, Iterable, List, Optional
from itertools import chain

import pandas as pd
from pandas import DataFrame
from regmod.composite_models.interface import NodeModel
from regmod.composite_models.node import Node


class CompositeModel(NodeModel):
    """Composite Model, abstract behavior of group model.

    Parameters
    ----------
    name : str
        Name of the model.
    models : List[NodeModel], optional
        A list of sub models. Default to be None. When it is None, models will
        be an empty list.
    masks : Optional[Dict], optional
        Prior masks, default to be None. When it is None, masks will be an empty
        dictionary.

    Methods
    -------
    get_computational_nodes()
        Get the computational nodes.
    """

    def __init__(self,
                 name: str,
                 models: Optional[List[NodeModel]] = None,
                 masks: Optional[Dict] = None):
        super().__init__(name)
        models = [] if models is None else models
        if not all(isinstance(model, NodeModel) for model in models):
            raise TypeError("Models must be instances of NodeModel.")
        if not all(model.isroot for model in models):
            raise ValueError("Models must be root.")
        self.extend(models, rank=1)
        masks = {} if masks is None else masks
        if not isinstance(masks, dict):
            raise TypeError("Masks must be dictionary.")
        self.set_prior_mask(masks)

    def get_computational_nodes(self) -> Iterable[NodeModel]:
        """Get the computational nodes.

        Returns
        -------
        Iterable[NodeModel]
            Computational nodes.
        """
        return chain.from_iterable(
            model.get_leafs(1) for model in self.children.named_lists[1]
        )

    def get_data(self) -> DataFrame:
        dfs = []
        for model in self.get_computational_nodes():
            df = model.get_data()
            df[self.col_label] = model.get_name(self.level + 1)
            dfs.append(df)
        return pd.concat(dfs, ignore_index=True)

    def set_data(self, df: DataFrame):
        for name in df[self.col_label].unique():
            self[name].set_data(df[df[self.col_label] == name])

    def get_posterior(self) -> Dict:
        return {
            model.get_name(self.level + 1): model.get_posterior()
            for model in self.get_computational_nodes()
        }

    def set_prior(self, priors: Dict):
        for name, prior in priors.items():
            self[name].set_prior(prior)

    def set_prior_mask(self, masks: Dict):
        for name, mask in masks.items():
            self[name].set_prior_mask(mask)

    def fit(self, **fit_options):
        for model in self.children.named_lists[1]:
            model.fit(**fit_options)

    def predict(self, df: DataFrame = None) -> DataFrame:
        df = self.get_data() if df is None else df
        return pd.concat([
            self[name].predict(df[df[self.col_label] == name])
            for name in df[self.col_label].unique()
        ], ignore_index=True)

    def get_draws(self,
                  df: DataFrame = None,
                  size: int = 1000) -> DataFrame:
        df = self.get_data() if df is None else df
        return pd.concat([
            self[name].get_draws(df[df[self.col_label] == name], size)
            for name in df[self.col_label].unique()
        ], ignore_index=True)
