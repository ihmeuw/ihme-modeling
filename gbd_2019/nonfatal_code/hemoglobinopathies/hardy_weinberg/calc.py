import os
import argparse
import numpy as np
import pandas as pd
from job_utils import draws, parsers

"FILEPATH"


##############################################################################
# Simple math for hardy weinberg
##############################################################################


class HardyWeinberg(draws.SquareImport):

    def duplicate_birth_age(self, df):
        idx = df.index.names
        df.reset_index(inplace=True)
        df_list = []
        most_detailed_ages = [i for i in range(2, 21, 1)] + [30, 31, 32, 235, 164]
        for age in most_detailed_ages:
            dup = df.copy(deep=True)
            dup['age_group_id'] = age
            df_list.append(dup)
        dup_df = pd.concat(df_list)
        dup_df.set_index(idx, inplace=True)
        return dup_df

    def calc_btt(self, first_arg = 2085, second_arg=None):
        btm = self.import_square(
            first_arg, source="epi")
        btm = self.duplicate_birth_age(btm)
        p = btm.apply(np.sqrt, axis=0)  # sqrt of the original
        btt = 2 * p * (1 - p)  # now the equation
        return btt

    def calc_ett(self, first_arg = 2087, second_arg=None):
        hbebt = self.import_square(
            first_arg, source="epi")
        hbebt = self.duplicate_birth_age(hbebt)
        p = hbebt.apply(np.sqrt, axis=0)  # sqrt of the original
        ett = p * (1 - p)  # now the equation
        return ett

    def calc_sct(self, first_arg = 2097, second_arg = 2103):
        hscbt = self.import_square(
            first_arg, source="epi")
        hscbt = self.duplicate_birth_age(hscbt)
        scbt = self.import_square(
            second_arg, source="epi")
        scbt = self.duplicate_birth_age(scbt)
        p = (hscbt + scbt).apply(np.sqrt, axis=0)  # add and sqrt
        sct = 2 * p * (1 - p)  # now the equation
        return sct

    def calc_hemi(self, first_arg = 2112, second_arg=None):
        cases = self.import_square(
            first_arg, source="epi")
        cases = self.duplicate_birth_age(cases)
        cases.ix[
            cases.index.isin([1], level="sex_id"),
            :] = 0  # set males to 0
        p = cases.apply(np.sqrt, axis=0)  # add and sqrt of the original
        hemi = 2 * p * (1 - p)  # now the equation
        return hemi


def hardy_weinberg(hardy_type, out_dir, first_arg, second_arg, year_id):

    # subset demographics
    dim = HardyWeinberg.default_idx_dmnsns
    dim["year_id"] = year_id
    dim["age_group_id"] = [164]
    hdwbg = HardyWeinberg(idx_dmnsns=dim)

    # calculate from hardy weinberg equations
    if hardy_type == "btt":
        btt = hdwbg.calc_btt(first_arg, second_arg)
        btt = btt.reset_index()
        btt.to_hdf(os.path.join(out_dir, str(year_id[0]) + ".h5"),
                   key="draws", format="table", data_columns=dim.keys())
    elif hardy_type == "ett":
        ett = hdwbg.calc_ett(first_arg, second_arg)
        ett = ett.reset_index()
        ett.to_hdf(os.path.join(out_dir, str(year_id[0]) + ".h5"),
                   key="draws", format="table", data_columns=dim.keys())
    elif hardy_type == "sct":
        sct = hdwbg.calc_sct(first_arg, second_arg)
        sct = sct.reset_index()
        sct.to_hdf(os.path.join(out_dir, str(year_id[0]) + ".h5"),
                   key="draws", format="table", data_columns=dim.keys())
    elif hardy_type == "hemi":
        hemi = hdwbg.calc_hemi(first_arg, second_arg)
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
    parser.add_argument("hardy_type", help="which type of calc to run: btt, ett, sct, or hemi", type =str)
    parser.add_argument("out_dir", help="root directory to save stuff", type=str)
    parser.add_argument("first_arg", help="first input meid", type=int)
    parser.add_argument("second_arg", help="second input meid; only pertains to sct", type=int)
    parser.add_argument("year_id", nargs='*', type=int, help='year ids to include')

    args = parser.parse_args()

    # call exclusivity adjuster
    hardy_weinberg(args.hardy_type, args.out_dir, args.first_arg, args.second_arg, args.year_id)
