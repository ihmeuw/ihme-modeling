"""
Node Model
"""
from abc import ABC, abstractmethod
from typing import Dict

from pandas import DataFrame

from regmod.composite_models.node import Node


class ModelInterface(ABC):
    """Abstract class that encode the behavior of the model interface.

    Attributes
    ----------
    col_label : str
        Name of the label column, used in `predict` and `get_draws`.
    col_value : str
        Name of the value column, used in `predict` and `get_draws`.

    Methods
    -------
    set_data(df)
        Attach the data frame.
    get_data(df)
        Get the data frame.
    fit(**fit_options)
        Fit the model.
    predict(df=None)
        Predict model with given data frame.
    get_draws(df=None, size=1000)
        Get the draws of the prediction with given data frame.
    set_prior(priors)
        Attach priors.
    set_prior_mask(masks)
        Set the prior masks.
    get_posterior()
        Get the posterior of the fitted model.
    """

    col_label = "label"
    col_value = "value"

    @abstractmethod
    def set_data(self, df: DataFrame):
        """Attach the data frame.

        Parameters
        ----------
        df : DataFrame
            Input data frame.
        """
        pass

    @abstractmethod
    def get_data(self) -> DataFrame:
        """Get the data frame.

        Returns
        -------
        DataFrame
            Returns current data frame.
        """
        pass

    @abstractmethod
    def fit(self, **fit_options):
        """Fit the model."""
        pass

    @abstractmethod
    def predict(self, df: DataFrame = None) -> DataFrame:
        """Predict model with given data frame.

        Parameters
        ----------
        df : DataFrame, optional
            Given data frame, by default None. If `None` use the current data
            frame.

        Returns
        -------
        DataFrame
            Predicted data frame.
        """
        pass

    @abstractmethod
    def get_draws(self, df: DataFrame = None, size: int = 1000) -> DataFrame:
        """Get the draws of the prediction with given data frame.

        Parameters
        ----------
        df : DataFrame, optional
            Given data frame, by default None. If `None` use the current data
            frame.
        size : int, optional
            Number of draws, by default 1000.

        Returns
        -------
        DataFrame
            Predicted data frame.
        """
        pass

    @abstractmethod
    def set_prior(self, priors: Dict):
        """Attach priors.

        Parameters
        ----------
        priors : Dict
            Given priors.
        """
        pass

    @abstractmethod
    def set_prior_mask(self, masks: Dict):
        """Set the prior masks.

        Parameters
        ----------
        masks : Dict
            Given masks.
        """
        pass

    @abstractmethod
    def get_posterior(self) -> Dict:
        """Get the posterior of the fitted model.

        Returns
        -------
        Dict
            The posterior information.
        """
        pass


class NodeModel(Node, ModelInterface):
    """Node model is a class with structure of Node and ModelInterface.

    Methods
    -------
    append(node, rank=0)
        Over write the append in the Node class, check the type of the appended
        node.
    """

    def append(self, node: "NodeModel", rank: int = 0):
        """Over write the append in the Node class, check the type of the
        appended node.

        Parameters
        ----------
        node : NodeModel
            The node model to be appended node.
        rank : int, optional
            Indicate which compartment to append to, by default 0.

        Raises
        ------
        TypeError
            Rasied when appended node is not an instance of NodeModel.
        """
        if not isinstance(node, NodeModel):
            raise TypeError("Can only connect to instance of NodeModel.")
        super().append(node, rank=rank)
