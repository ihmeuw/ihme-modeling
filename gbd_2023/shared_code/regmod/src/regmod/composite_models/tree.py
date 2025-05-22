"""
Tree Model
"""
from typing import Dict, List

import numpy as np
from pandas import DataFrame

from regmod.composite_models.base import BaseModel
from regmod.composite_models.composite import CompositeModel
from regmod.composite_models.interface import NodeModel


class TreeModel(CompositeModel):
    """Tree Model with hierarchy structure. This model is also called Cascade.

    Methods
    -------
    fit()
        Overwrite the fit function in CompositeModel.
    """

    def _fit(self, model: NodeModel, **fit_options):
        model.fit(**fit_options)
        prior = model.get_posterior()
        for sub_model in model.children.named_lists[0]:
            sub_model.set_prior(prior)
            self._fit(sub_model, **fit_options)

    def fit(self, **fit_options):
        """Overwrite the fit function in CompositeModel.

        Raises
        ------
        ValueError
            Raised when model have more than one computational tree.
        """
        if len(self.children.named_lists[1]) != 1:
            raise ValueError(f"{type(self).__name__} must only have one "
                             "computational tree.")
        self._fit(self.children.named_lists[1][0], **fit_options)

    @classmethod
    def get_simple_tree(cls, name: str, *args, **kwargs) -> "TreeModel":
        """Get the simple TreeModel from given specification.

        Parameters
        ----------
        name : str
            Name of the model.

        Returns
        -------
        TreeModel
            A simple tree model.
        """
        return cls(name, models=[get_simple_basetree(*args, **kwargs)])


def get_simple_basetree(df: DataFrame,
                        col_label: List[str],
                        model_specs: Dict,
                        var_masks: Dict[str, float] = None,
                        lvl_masks: List[float] = None) -> BaseModel:
    """Get simple base model with tree structure from given specification.

    Parameters
    ----------
    df : DataFrame
        Given data frame.
    col_label : List[str]
        The name of the label column.
    model_specs : Dict
        Model specification including data and variables.
    var_masks : Dict[str, float], optional
        Prior masks for variables., by default None.
    lvl_masks : List[float], optional
        Prior masks for each level, by default None.

    Returns
    -------
    BaseModel
        Root node of the computational tree model.
    """
    # check data before create model
    data = model_specs["data"]
    variables = model_specs["variables"]
    data.attach_df(df)
    for v in variables:
        v.check_data(data)

    # create model
    model = BaseModel(**model_specs)
    data.detach_df()
    if len(col_label) == 0:
        return model

    # process masks
    final_var_masks = {v.name: np.full(v.size, np.inf) for v in variables}
    final_lvl_masks = [np.inf]*len(col_label)
    if var_masks is not None:
        final_var_masks.update(var_masks)
    if lvl_masks is not None:
        final_lvl_masks = list(lvl_masks) + final_lvl_masks[len(lvl_masks):]

    mask = {name: prior*final_lvl_masks[0]
            for name, prior in final_var_masks.items()}

    # create children model
    df_group = df.groupby(col_label[0])
    for name in df_group.groups.keys():
        model_specs = model_specs.copy()
        model_specs["name"] = name
        model_specs["prior_mask"] = mask
        model.append(get_simple_basetree(df_group.get_group(name),
                                         col_label[1:],
                                         model_specs,
                                         var_masks=var_masks,
                                         lvl_masks=final_lvl_masks[1:]))

    return model
