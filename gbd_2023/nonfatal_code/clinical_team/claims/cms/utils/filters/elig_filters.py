from functools import reduce
from itertools import chain

import numpy as np
import pandas as pd
from crosscutting_functions.clinical_constants.pipeline.cms import max_rst_bnft_vals
from crosscutting_functions.db_connector.database import Database


class Elig_Filters:
    def __init__(self, df=None, filter_dict=None):
        self.filters = filter_dict
        self.df = df

    def filter(self):

        base_cols = [e for e in self.df.columns if e not in self.filters.keys()]

        df_list = []
        for k, v in self.filters.items():
            if k not in self.df.columns:
                continue
            cols = base_cols + [k]
            temp = self.df[cols]
            df_list.append(temp[~temp[k].isin(v)])

        return reduce(lambda left, right: pd.merge(left, right, on=base_cols), df_list)


class MDCR_Filter(Elig_Filters):
    def __init__(self, df, filter_dict=None):
        super().__init__(df, filter_dict)
        self.mdcr_filters()

    def mdcr_filters(self):
        if not self.filters:
            self.filters = {"part_c": [True]}


class MAX_Filter(Elig_Filters):
    def __init__(self, df, rst_bnft_vals=None, filter_dict=None):
        super().__init__(df, filter_dict)

        # sanity check
        self.df["restricted_benefit"] = self.df.restricted_benefit.astype(str)
        self.rst_bnft_vals = rst_bnft_vals

        if not rst_bnft_vals:
            # pulls in all values from max_rst_bnft_vals.
            self.rst_bnft_vals = list(chain.from_iterable(max_rst_bnft_vals.values()))

        self.max_filters()
        self.remove_rst_bnfts()

    def preg_age_sex_rst(self, map_version: int):
        """
        Please note: this does not appear to be used in the repo/CMS pipeline.

        Apply age sex restrictions on benes who received restricted
        care due to pregenancy.
        """
        db = Database()
        db.load_odbc("SERVER")

        dql = "QUERY"
        db_r = db.query(dql)

        temp = self.df[
            (self.df.restricted_benefit == "4")
            & (
                (self.df.sex_id == 1)
                | (self.df.age < float(db_r.yld_age_start))
                | (self.df.age > float(db_r.yld_age_end))
            )
        ]
        temp = self.df.merge(temp, on=temp.columns.tolist(), indicator=True, how="outer")
        self.df = (
            temp[temp._merge == "left_only"].drop("_merge", axis=1).reset_index(drop=True)
        )

    def max_filters(self):
        """
        Create a dict of rst bnft flags that should
        be removed from the data
        """
        if not self.filters:
            rst_flags = [
                e
                for e in self.df.restricted_benefit.unique().tolist()
                if e not in self.rst_bnft_vals
            ]
            self.filters = {"restricted_benefit": rst_flags}

    def remove_rst_bnfts(self):
        """
        Drop rst_bnft flags from the data
        """
        self.df = self.df[~self.df.restricted_benefit.isin(self.filters["restricted_benefit"])]

    def mask_full_bnft(self):
        """
        Create a single value for rst bnft flags where we believe the
        bene / month has full access to max benefits
        """
        self.df["restricted_benefit"] = np.where(
            self.df["restricted_benefit"].isin(max_rst_bnft_vals["full_bnfts"]),
            "1",
            self.df.restricted_benefit,
        )
