import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from job_utils import draws


class ExAdjust(draws.SquareImport):

    def __init__(self, me_map, out_dir, **kwargs):
        super(ExAdjust, self).__init__(**kwargs)
        # set me_map
        self.me_map = me_map

        # set out_dir
        self.out_dir = out_dir

        # import draws
        self.draws = {}
        self.draws[self.me_map["env"]] = self.import_square(
            gopher_what={'modelable_entity_ids': [self.me_map["env"]]},
            source="dismod")
        for me_id in self.me_map["sub"].keys():
            self.draws[me_id] = self.import_square(
                gopher_what={'modelable_entity_ids': [me_id]},
                source="dismod")

        # compute the sum of the sub sequela
        self.draws["sigma_sub"] = self.calc_sigma_sub()

    def calc_sigma_sub(self):
        """calculate the sum of the sub sequela"""
        # concatenate all required frames
        sub_dfs = []
        for me_id in self.me_map["sub"].keys():
            sub_dfs.append(self.draws[me_id])
        sub_df = pd.concat(sub_dfs)

        # return the sum
        return sub_df.reset_index().groupby(self.idx_dmnsns.keys()).sum()

    def excess(self, sub_me):
        """calculate the excess proportions"""
        # get the needed data
        sub_me_df = self.draws[sub_me]
        env_df = self.draws[self.me_map["env"]]
        sigma_sub_df = self.draws["sigma_sub"]

        # create a boolean dataframe for our 2 cases
        more = (sigma_sub_df > env_df)

        # now calculate the excess values
        excess_df = (
            (sigma_sub_df[more] - env_df[more]) * sub_me_df[more] /
            sigma_sub_df[more]
        ).fillna(value=0)
        return excess_df

    def squeeze(self, sub_me):
        """calculate the squeezed proportions"""
        # get the needed data
        sub_me_df = self.draws[sub_me]
        env_df = self.draws[self.me_map["env"]]
        sigma_sub_df = self.draws["sigma_sub"]

        # create a boolean dataframe for our 2 cases
        more = (sigma_sub_df > env_df)

        # get the squeezed values when
        squeeze_more = env_df[more] * sub_me_df[more] / sigma_sub_df[more]
        squeeze_less = sub_me_df[~more]
        squeeze_df = squeeze_more.fillna(squeeze_less)
        return squeeze_df

    def resid(self):
        """calculate the residual numbers"""
        # get the needed data
        sigma_sub_df = self.draws["sigma_sub"]
        env_df = self.draws[self.me_map["env"]]

        # if it is a squeeze type then we use the absolute value of the diff
        resid_df = (env_df - sigma_sub_df)[(sigma_sub_df <= env_df)].fillna(0)
        return resid_df

    def adjust(self):
        """run exclusivity adjustment on all MEs"""
        self.draws[self.me_map["resid"]] = self.resid()
        for sub_me in self.me_map["sub"].keys():
            if "squeeze" in self.me_map["sub"][sub_me].keys():
                self.draws[self.me_map["sub"][sub_me]["squeeze"]] = (
                    self.squeeze(sub_me))
            if "excess" in self.me_map["sub"][sub_me].keys():
                self.draws[self.me_map["sub"][sub_me]["excess"]] = (
                    self.excess(sub_me))

    def export(self):
        """export all data"""
        fname = str(self.idx_dmnsns["year_id"][0]) + ".h5"

        # export residual
        me_id = self.me_map["resid"]
        resid_dir = os.path.join(self.out_dir, str(me_id))
        try:
            os.makedirs(resid_dir)
        except OSError:
            if not os.path.isdir(resid_dir):
                raise
        resid_df = self.draws[me_id]
        resid_df = resid_df.reset_index()
        resid_df.to_hdf(os.path.join(resid_dir, fname), key="draws",
                        format="table", data_columns=self.idx_dmnsns.keys())

        # export any subcause adjustments
        for sub_me in self.me_map["sub"].keys():
            if "squeeze" in self.me_map["sub"][sub_me].keys():
                me_id = self.me_map["sub"][sub_me]["squeeze"]
                sqz_dir = os.path.join(self.out_dir, str(me_id))
                try:
                    os.makedirs(sqz_dir)
                except OSError:
                    if not os.path.isdir(sqz_dir):
                        raise
                squeeze_df = self.draws[me_id]
                squeeze_df = squeeze_df.reset_index()
                squeeze_df.to_hdf(
                    os.path.join(sqz_dir, fname), key="draws", format="table",
                    data_columns=self.idx_dmnsns.keys())
            if "excess" in self.me_map["sub"][sub_me].keys():
                me_id = self.me_map["sub"][sub_me]["excess"]
                exs_dir = os.path.join(self.out_dir, str(me_id))
                try:
                    os.makedirs(exs_dir)
                except OSError:
                    if not os.path.isdir(exs_dir):
                        raise
                excess_df = self.draws[me_id]
                excess_df = excess_df.reset_index()
                excess_df.to_hdf(
                    os.path.join(exs_dir, fname), key="draws", format="table",
                    data_columns=self.idx_dmnsns.keys())
