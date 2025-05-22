"""
Data Module
"""

from dataclasses import dataclass, field

import numpy as np
from regmod._typing import NDArray, DataFrame


@dataclass
class Data:
    """Data class used to validate and access data in `pd.DataFrame`.

    Parameters
    ----------
    col_obs : str, optional
        Column name(s) for observation. Default is `None`.
    col_covs : list[str], optional
        Column names for covariates. Default is an empty list.
    col_weights : str, default="weights"
        Column name for weights. Default is `'weights'`. If `col_weights` is
        not in the data frame, a column with name `col_weights` will be added to
        the data frame filled with 1.
    col_offset : str, default="offset"
        Column name for weights. Default is `'offset'`. If `col_offset`
        is not in the data frame, a column with name `col_offset` will be added
        to the data frame filled with 0.
    df : pd.DataFrame, optional
        Data frame for the object. Default is an empty data frame.
    subset_cols : bool, optional
        If True only keep the enssential columns. Default to False.

    Attributes
    ----------
    obs
    covs
    weights
    offset
    trim_weights
    num_obs
    col_obs : str, optional
        Column name for observation, can be a single string, a list of string or
        `None`. When it is `None` you cannot access property `obs`.
    col_covs : list[str]
        A list of column names for covariates.
    col_weights : str
        Column name for weights. `weights` can be used in the likelihood
        computation. Values of `weights` are required to be between 0 and 1.
        Default for `col_weights` is `'weights'`. If `col_weights` is not in the
        data frame, a column with name `col_weights` will be added to the data
        frame filled with 1.
    col_offset : str
        Column name for offset. Same as `weights`, `offset` can be used in
        computing likelihood. `offset` needs to be pre-transformed according to
        link function of the parameters. Default for `col_offset` is `'offset'`.
        If `col_offset` is not in the data frame, a column with name
        `col_offset` will be added to the data frame filled with 0.
    df : pd.DataFrame
        Data frame for the object. Default is an empty data frame.
    cols : list[str]
        All the relevant columns, including, `col_obs` (if not `None`),
        `col_covs`, `col_weights`, `col_offset` and `'trim_weights'`.

    Methods
    -------
    is_empty()
        Indicator of empty data frame.
    check_cols()
        Validate if all `self.cols` are in `self.df`.
    parse_df(df=None)
        Subset `df` with `self.cols`.
    fill_df()
        Automatically add columns `'intercept'`, `col_weights`, `col_offset` and
        `'trim_weights'`, if they are not present in the `self.df`.
    detach_df()
        Set `self.df` to an empty data frame.
    attach_df(df)
        Validate `df` and set `self.df=df`.
    copy(with_df=False)
        Copy `self` to a new instance of the class.
    get_cols(cols)
        Access columns in `self.df`.
    get_covs(col_covs)
        Access covariates in `self.df`.

    Notes
    -----
    * This class should be replaced by a subclass of a more general dataclass
    * `get_covs` seems very redundant should only keep `get_cols`.

    """

    col_obs: str | None = None
    col_covs: list[str] = field(default_factory=list)
    col_weights: str = "weights"
    col_offset: str = "offset"
    df: DataFrame = field(default_factory=DataFrame)
    subset_cols: bool = False

    def __post_init__(self):
        self.col_covs = list(set(self.col_covs).union({"intercept"}))
        self.cols = self.col_covs + [self.col_weights, self.col_offset, "trim_weights"]
        if self.col_obs is not None:
            if isinstance(self.col_obs, str):
                self.cols.insert(0, self.col_obs)
            else:
                self.cols = self.col_obs + self.cols

        if self.is_empty():
            self.df = DataFrame(columns=self.cols)
        else:
            self.parse_df()
            self.fill_df()
            self.check_cols()

    def is_empty(self) -> bool:
        """Indicator of empty data frame.

        Returns
        -------
        bool
            Return `True` when `self.df` is empty.

        """
        return self.num_obs == 0

    def check_cols(self) -> None:
        """Validate if all `self.cols` are in `self.df`.

        Raises
        ------
        ValueError
            Raised if any col in `self.cols` is not in `self.df`.

        """
        for col in self.cols:
            if col not in self.df.columns:
                raise ValueError(f"Missing columnn {col}.")

    def parse_df(self, df: DataFrame | None = None) -> DataFrame:
        """Subset `df` with `self.cols`.

        Parameters
        ----------
        df : DataFrame, optional
            Data Frame used to create subset. When it is `None`, it will use
            `self.df`.

        Returns
        -------
        DataFrame
            Copy of input data frame with given subset columns.

        """
        if df is not None:
            if self.subset_cols:
                self.df = df.loc[:, df.columns.isin(self.cols)].copy()
            else:
                self.df = df.copy()

    def fill_df(self) -> None:
        """Automatically add columns `'intercept'`, `col_weights`, `col_offset`
        and `'trim_weights'`, if they are not present in the `self.df`.

        """
        if "intercept" not in self.df.columns:
            self.df["intercept"] = 1.0
        if self.col_weights not in self.df.columns:
            self.df[self.col_weights] = 1.0
        if self.col_offset not in self.df.columns:
            self.df[self.col_offset] = 0.0
        if self.col_obs is not None:
            cols = self.col_obs if isinstance(self.col_obs, list) else [self.col_obs]
            for col in cols:
                if col not in self.df.columns:
                    self.df[col] = np.nan
        self.df["trim_weights"] = 1.0

    def detach_df(self):
        """Set `self.df` to an empty data frame."""
        self.df = DataFrame(columns=self.cols)

    def attach_df(self, df: DataFrame):
        """Validate `df` and set `self.df=df`.

        Parameters
        ----------
        df : DataFrame
            Data frame to be attached.

        """
        self.parse_df(df)
        self.fill_df()
        self.check_cols()

    def copy(self, with_df=False) -> "Data":
        """Copy `self` to a new instance of the class.

        Parameters
        ----------
        with_df : bool, default=False
            If `True`, copy with attached data frame, else only copy the
            structure with empty data frame. Default is `False`.

        Returns
        -------
        Data
            Copied instance of `Data` class.

        """
        df = self.df.copy() if with_df else DataFrame(columns=self.cols)
        return Data(self.col_obs, self.col_covs, self.col_weights, self.col_offset, df)

    def get_cols(self, cols: str | list[str]) -> NDArray:
        """Access columns in `self.df`.

        Parameters
        ----------
        cols : str | list[str]
            Column name(s) need to accessed.

        Returns
        -------
        NDArray
            Numpy array corresponding to the column(s).

        """
        return self.df[cols].to_numpy()

    @property
    def num_obs(self) -> int:
        """Number of the observations/rows of data frame."""
        return self.df.shape[0]

    @property
    def obs(self) -> NDArray:
        """Observation column(s)."""
        if self.col_obs is None:
            raise ValueError("This data object does not contain observations.")
        return self.get_cols(self.col_obs)

    @property
    def covs(self) -> dict[str, NDArray]:
        """Covariates dictionary with column names as keys and corresponding
        numpy array as the column.
        """
        return self.df[self.col_covs].to_dict(orient="list")

    @property
    def weights(self) -> NDArray:
        """Weights column."""
        return self.get_cols(self.col_weights)

    @property
    def offset(self) -> NDArray:
        """Offset column."""
        return self.get_cols(self.col_offset)

    @property
    def trim_weights(self) -> NDArray:
        """Trimming weights column."""
        return self.get_cols("trim_weights")

    @trim_weights.setter
    def trim_weights(self, weights: float | NDArray):
        if np.any(weights < 0.0) or np.any(weights > 1.0):
            raise ValueError("trim_weights has to be between 0 and 1.")
        self.df["trim_weights"] = weights

    def get_covs(self, col_covs: str | list[str]) -> NDArray:
        """Access covariates in `self.df`.

        Parameters
        ----------
        col_covs : str | list[str]
            Column name(s) of the covariates.

        Returns
        -------
        NDArray
            Return the corresponding column(s) in the data frame. Always return
            matrix even if `col_covs` is a single string.

        """
        if not isinstance(col_covs, list):
            col_covs = [col_covs]
        return self.get_cols(col_covs)

    def __repr__(self):
        return f"Data(num_obs={self.num_obs})"
