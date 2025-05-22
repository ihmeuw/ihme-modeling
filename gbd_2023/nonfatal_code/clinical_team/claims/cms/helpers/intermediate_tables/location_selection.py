from os import pathconf_names

import numpy as np
import pandas as pd
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser


class Location_Select:
    def __init__(self, year, group, loc_sys):
        self.year = year
        self.group = group
        self.loc_sys = "mdcr" if loc_sys == "STATE_CNTY_FIPS" else "max"

    def run(self, df):
        if self.loc_sys == "mdcr":
            return self.select_location(df)
        else:
            return self.location_helper(df)

    def verify_bene(self, df, ids=False):
        """
        Verify that there is only one row for each
        bene in a given df
        """
        bene_ids = df[df.bene_id.duplicated()].bene_id.unique().tolist()

        if ids:
            return bene_ids, len(bene_ids) == 0
        else:
            return len(bene_ids) == 0

    def random_loc(self, df):
        """
        When a bene lives in more than one location for equal amount of time
        (eg. 6 mos in AZ and 6 mos in FL) randomly select a location
        """
        bene_ids, dups = self.verify_bene(df, ids=True)

        if not dups:
            coin = df[df.bene_id.isin(bene_ids)].reset_index(drop=True)
            # log this decision
            base = filepath_parser(
                ini="pipeline.cms", section="table_outputs", section_key="demo"
            )
            log_out = "FILEPATH"
            coin.to_csv("FILEPATH", index=False)

            coin = coin.sample(frac=1, random_state=1)

            return coin.drop_duplicates(subset=["bene_id"], keep="first")
        else:
            return

    def predom_loc(self, df):
        """
        When a bene has more than one location,
        assign the location where they spent
        the most time.
        """
        df["max_vc"] = df.groupby("bene_id")["location_vc"].transform("max")
        df["max_loc"] = df.loc[df.location_vc == df.max_vc, "location_id"]

        # keep only rows where max_vc and location_vc
        # are the same
        df = df.dropna(subset=["max_loc"])

        df_rand = self.random_loc(df)

        if df_rand is not None:

            cat = pd.concat([df_rand, df[~df.bene_id.isin(df_rand.bene_id.tolist())]])
            assert self.verify_bene(cat), "dup bene ids after random_loc() concat"

            return cat
        else:
            return df

    def multiple_bene_locs(self, df):
        """
        Identifies benes with more than one unique location
        in a given year
        """

        bene_ids = df.bene_id.value_counts() > 1
        df_bene = bene_ids.to_frame().reset_index()

        df_bene.rename({"bene_id": "multiple", "index": "bene_id"}, axis=1, inplace=True)

        # BENEs with more than one location
        return df[
            df.bene_id.isin(df_bene[df_bene.multiple == True].bene_id.tolist())  # noqa
        ].reset_index(drop=True)

    def location_helper(self, df):
        if self.loc_sys == "mdcr":
            df = self.multiple_bene_locs(df)
        temp = self.predom_loc(df)

        temp["location_id"] = temp["max_loc"]
        return temp.drop(["location_vc", "max_vc", "max_loc"], axis=1)

    def select_location(self, df):
        """
        Generate a single location for each bene_id

        Preconditions:
            * input must be wide format
        """
        df["location_vc"] = 0

        # Do not count placeholder values
        df.loc[~df.location_id.isin(["00000", "99999"]), "location_vc"] = 1

        gb = df.groupby(["bene_id", "location_id"]).agg({"location_vc": "sum"}).reset_index()
        missing = set(df.bene_id.tolist()) - set(gb.bene_id.tolist())

        # Edge case where a mdcr bene can have an entire
        # year of missing location_ids.
        #
        # We create our own null value (00999) to identify
        # this decision in the log files
        if len(missing) > 0:
            missing = list(missing)
            temp = pd.DataFrame(
                {
                    "bene_id": missing,
                    "location_id": ["00999" for _ in missing],
                    "location_vc": ["12" for _ in missing],
                }
            )
            gb = pd.concat([gb, temp], sort=False)

        df_m_locs = self.location_helper(gb)
        gb.drop("location_vc", axis=1, inplace=True)

        cat = pd.concat(
            [df_m_locs, gb[~gb.bene_id.isin(df_m_locs.bene_id.tolist())]], sort=False
        )

        assert self.verify_bene(cat), "dup benes after location_helper()"

        return cat
