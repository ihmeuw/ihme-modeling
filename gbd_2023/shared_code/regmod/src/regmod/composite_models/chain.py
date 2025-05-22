"""
Chain Model
"""
from typing import Dict, List

from pandas import DataFrame
from regmod.composite_models.base import BaseModel

from regmod.composite_models.composite import CompositeModel
from regmod.composite_models.interface import NodeModel


class ChainModel(CompositeModel):
    """Chain Model with sequential model fitting.

    Methods
    -------
    fit()
        Overwrite the fit function in CompositeModel.
    """

    def _fit(self, model: NodeModel, **fit_options):
        model.fit(**fit_options)
        if len(model.children.named_lists[0]) > 0:
            sub_model = model.children.named_lists[0][0]
            sub_model.set_data(model.predict(sub_model.get_data()))
            self._fit(sub_model, **fit_options)

    def fit(self, **fit_options):
        """Overwrite the fit function in CompositeModel.

        Raises
        ------
        ValueError
            Raised when model have more than one computational tree.
        ValueError
            Raised when the computational tree is not a chain.
        """
        if len(self.children.named_lists[1]) != 1:
            raise ValueError(f"{type(self).__name__} must only have one "
                             "computational tree.")
        if len(self.children.named_lists[1][0].get_leafs(0)) != 1:
            raise ValueError(f"{type(self).__name__} computational tree must be"
                             " chain.")
        self._fit(self.children.named_lists[1][0], **fit_options)

    @classmethod
    def get_simple_chain(cls, name: str, *args, **kwargs) -> "ChainModel":
        """Get the simple ChainModel from given specification.

        Parameters
        ----------
        name : str
            Name of the model.

        Returns
        -------
        ChainModel
            A simple chain model.
        """
        return cls(name, models=[get_simple_basechain(*args, **kwargs)])


def get_simple_basechain(df: DataFrame,
                         model_specs: List[Dict]) -> BaseModel:
    """Get simple base model with chain structure from given specification.

    Parameters
    ----------
    df : DataFrame
        Given data frame.
    model_specs : List[Dict]
        Model specification including data and variables. It will be a list for
        each model in the chain.

    Returns
    -------
    BaseModel
        Root node of the computational chain model.

    Raises
    ------
    ValueError
        Raised when there is no model specification.
    """
    if len(model_specs) == 0:
        raise ValueError("Must provide specifications of BaseModel.")

    model = BaseModel(**model_specs[0])
    model.set_data(df)

    if len(model_specs) > 1:
        model.append(get_simple_basechain(df, model_specs[1:]))

    return model
