import os
import argparse
import numpy as np
import pandas as pd
from job_utils import draws, parsers

os.chdir(os.path.dirname(os.path.realpath(__file__)))


##############################################################################
# Simple math for hardy weinberg
##############################################################################


class HardyWeinberg(draws.SquareImport):

    def duplicate_birth_age(self, df):
        idx = df.index.names
        df.reset_index(inplace=True)
        df_list = []
        most_detailed_ages = [{AGE GROUP IDS}]
        for age in most_detailed_ages:
            dup = df.copy(deep=True)
            dup['age_group_id'] = age
            df_list.append(dup)
        dup_df = pd.concat(df_list)
        dup_df.set_index(idx, inplace=True)
        return dup_df

    def calc_btt(self, btm_id):
        btm = self.import_square(
            gopher_what={"modelable_entity_ids": [btm_id]}, source="dismod")
        btm = self.duplicate_birth_age(btm)
        p = btm.apply(np.sqrt, axis=0)  # sqrt of the original
        btt = 2 * p * (1 - p)  # now the equation
        return btt

    def calc_ett(self, hbebt_id):
        hbebt = self.import_square(
            gopher_what={"modelable_entity_ids": [hbebt_id]}, source="dismod")
        hbebt = self.duplicate_birth_age(hbebt)
        p = hbebt.apply(np.sqrt, axis=0)  # sqrt of the original
        ett = p * (1 - p)  # now the equation
        return ett

    def calc_sct(self, hscbt_id, scbt_id):
        hscbt = self.import_square(
            gopher_what={"modelable_entity_ids": [hscbt_id]}, source="dismod")
        hscbt = self.duplicate_birth_age(hscbt)
        scbt = self.import_square(
            gopher_what={"modelable_entity_ids": [scbt_id]}, source="dismod")
        scbt = self.duplicate_birth_age(scbt)
        p = (hscbt + scbt).apply(np.sqrt, axis=0)  # add and sqrt
        sct = 2 * p * (1 - p)  # now the equation
        return sct

    def calc_hemi(self, cases_id):
        cases = self.import_square(
            gopher_what={"modelable_entity_ids": [cases_id]}, source="dismod")
        cases = self.duplicate_birth_age(cases)
        cases.ix[
            cases.index.isin([1], level="sex_id"),
            :] = 0  # set males to 0
        p = cases.apply(np.sqrt, axis=0)  # add and sqrt of the original
        hemi = 2 * p * (1 - p)  # now the equation
        return hemi


def hardy_weinberg(hardy_type, year_id, out_dir, **kwargs):

    # subset demographics
    dim = HardyWeinberg.dUSERt_idx_dmnsns
    dim["year_id"] = year_id
    dim["age_group_id"] = [{AGE GROUP ID FOR BIRTH}]
    hdwbg = HardyWeinberg(idx_dmnsns=dim)

    # calculate from hardy weinberg equations
    if hardy_type == "btt":
        btt = hdwbg.calc_btt(**kwargs)
        btt = btt.reset_index()
        btt.to_hdf(os.path.join(out_dir, str(year_id[0]) + ".h5"),
                   key="draws", format="table", data_columns=dim.keys())
    elif hardy_type == "ett":
        ett = hdwbg.calc_ett(**kwargs)
        ett = ett.reset_index()
        ett.to_hdf(os.path.join(out_dir, str(year_id[0]) + ".h5"),
                   key="draws", format="table", data_columns=dim.keys())
    elif hardy_type == "sct":
        sct = hdwbg.calc_sct(**kwargs)
        sct = sct.reset_index()
        sct.to_hdf(os.path.join(out_dir, str(year_id[0]) + ".h5"),
                   key="draws", format="table", data_columns=dim.keys())
    elif hardy_type == "hemi":
        hemi = hdwbg.calc_hemi(**kwargs)
        hemi = hemi.reset_index()
        hemi.to_hdf(os.path.join(out_dir, str(year_id[0]) + ".h5"),
                    key="draws", format="table", data_columns=dim.keys())
    else:
        raise Exception("incorrect hardy weinberg type, try another")

##############################################################################
# when called as a script
##############################################################################


if __name__ == "__main__":

    # parse command line args
    parser = argparse.ArgumentParser()
    parser.add_argument("--hardy_type", help="which type of calc to run",
                        required=True)
    parser.add_argument("--out_dir", help="root directory to save stuff",
                        required=True)
    parser.add_argument("--year_id", type=parsers.int_parser, nargs="*",
                        required=True, help="root directory to save stuff")
    parser.add_argument("--other_args", help="json style string map of ops",
                        required=True, type=parsers.json_parser)
    args = vars(parser.parse_args())

    # call exclusivity adjuster
    hardy_weinberg(
        hardy_type=args["hardy_type"], out_dir=args["out_dir"],
        year_id=args["year_id"], **args["other_args"])
