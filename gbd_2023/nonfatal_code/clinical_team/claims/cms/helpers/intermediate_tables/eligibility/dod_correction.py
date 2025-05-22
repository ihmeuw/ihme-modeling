import sys
from typing import Union

import pandas as pd
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser


class DOD_Correction:
    """
    Ad hoc class to correct monthly eligibility values that are past
    the dod month.

    For example, if the bene has a dod of 06/01/2014 and eligibilty in the months
    of July and August, for the same year, the class will remove the July and
    August eligibilty rows
    """

    def __init__(self, cms_system, year, group):
        self.cms_system = cms_system
        self.year = year
        self.group = group

        self.base = (
            "FILEPATH"
        )
        self.cols = None
        self.elig_df = None

    def reader(self):
        for e in ["demo", "elig"]:
            f = (
                "FILEPATH"
            )
            temp = pd.read_csv(f)

            if e == "elig":
                self.cols = temp.columns.tolist()
                self.elig_df = temp

            temp["gp"] = int(f.stem)
            yield temp

    def merge_dfs(self):
        df_list = [e for e in self.reader()]

        return (
            pd.merge(df_list[0], df_list[1], on=["bene_id", "year_id", "gp"], how="outer")
            .sort_values(by=["bene_id", "month_id"])
            .reset_index(drop=True)
        )

    def dod_df(self):
        df = self.merge_dfs()
        df["dod"] = pd.to_datetime(df["dod"])
        ids = df[df.dod.dt.month < df.month_id].bene_id.unique().tolist()
        df = df[df.bene_id.isin(ids)]

        df = df[df.dod.dt.month < df.month_id]
        return df

    def post_dod_months(self, df):
        self.elig_df = self.elig_df.merge(df, on=self.cols, indicator=True, how="outer")
        self.elig_df = self.elig_df[self.elig_df._merge == "left_only"].reset_index(drop=True)
        self.elig_df = self.elig_df[self.cols]
        self.elig_df.to_csv("FILEPATH", index=False)


if __name__ == "__main__":
    cms_system = sys.argv[1]
    year: Union[str, int] = sys.argv[2]
    group = sys.argv[3]

    year = int(year)

    dc = DOD_Correction(cms_system, year, group)
    df = dc.dod_df()
    dc.post_dod_months(df)
