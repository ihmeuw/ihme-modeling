import os
import argparse
import pandas as pd
import json
import random
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from job_utils import draws, parsers


class AttributeFemale(draws.SquareImport):

    def __init__(self, me_map, decomp_step, **kwargs):
        # super init
        super(AttributeFemale, self).__init__(**kwargs)

        # store data by me in this dict key=me_id, val=dataframe
        self.me_map = me_map
        self.me_dict = {}

        # import every input
        for _, v in me_map.items():
            inputs = v.get("srcs", {})
            for me_id in inputs.values():
                self.me_dict[me_id] = self.import_square(
                    gopher_what={"modelable_entity_id": me_id},
                    source="epi",
                    decomp_step=decomp_step)

    def remove_negatives(self, df):
        positive = df.copy()
        positive[positive<0.] = 0.
        assert (positive < 0.0).any().any() == False
        return positive
    
    def subtract_primary_only(self):
        # get dataframes for Turner and congenital urogenital anomalies
        prim_key = self.me_map["env"]["srcs"]["prim"]
        turner_key = self.me_map["cong_uro"]["srcs"]["meid"]
        cong_uro_key = self.me_map["cong_uro"]["srcs"]["meid"]
        prim_env = self.me_dict[prim_key].copy()
        turner = self.me_dict[turner_key].copy()
        cong_uro = self.me_dict[cong_uro_key].copy()

        mean = 0.0909
        std_error = 0.0388

        proportion = random.gauss(mean, std_error)
        if proportion < 0:
            proportion = 0

        # subtract out turner and congenital urogenital prevalence from primary
        #calculate for each draw, meaning incorporate SE
        primary = prim_env - (turner) - (cong_uro * proportion)
        primary = self.remove_negatives(primary)
        self.me_dict[prim_key] = primary

    def calc_prim_sec_ratio(self):
        # get dataframes
        prim_key = self.me_map["env"]["srcs"]["prim"]
        sec_key = self.me_map["env"]["srcs"]["sec"]
        prim_env = self.me_dict[prim_key].copy()
        sec_env = self.me_dict[sec_key].copy()

        # scale the envelopes to figure out the split proportions
        sigma_prim_sec = prim_env + sec_env
        self.me_dict["prim_prop"] = prim_env / sigma_prim_sec
        self.me_dict["sec_prop"] = sec_env / sigma_prim_sec

    def split_subgroups(self):
        # get envelope props
        prim_prop = self.me_dict["prim_prop"].copy()
        sec_prop = self.me_dict["sec_prop"].copy()

        # loop through subgroups
        for sub_group, mapper in self.me_map.items():
            if mapper["type"] == "sub_group":
                sub_env = self.me_dict[mapper["srcs"]["tot"]]
                outputs = mapper.get("trgs", {})
                for imp, me_id in outputs.items():

                    # calculate num primary and num secondary
                    if imp == "prim":
                        self.me_dict[me_id] = sub_env * prim_prop
                    elif imp == "sec":
                        self.me_dict[me_id] = sub_env * sec_prop
                    else:
                        raise Exception("unknown target type during subgroup "
                                        "infertility severity split")

    def calc_residual(self):
        # compile subgroups
        sub_prims = pd.DataFrame()
        sub_secs = pd.DataFrame()
        for sub_group, mapper in self.me_map.items():
            if mapper["type"] == "sub_group":
                outputs = mapper.get("trgs", {})
                for imp, me_id in outputs.items():
                    if imp == "prim":
                        sub_prims = pd.concat([sub_prims, self.me_dict[me_id]])
                    elif imp == "sec":
                        sub_secs = pd.concat([sub_secs, self.me_dict[me_id]])

        # aggregate subgroups
        sigma_sub_prim = sub_prims.groupby(level=list(
            self.idx_dmnsns.keys())).sum()
        sigma_sub_sec = sub_secs.groupby(level=list(
            self.idx_dmnsns.keys())).sum()

        # get total envs
        prim_env = self.me_dict[self.me_map["env"]["srcs"]["prim"]].copy()
        sec_env = self.me_dict[self.me_map["env"]["srcs"]["sec"]].copy()

        # reserve 95% so some is left over for residual
        prim_95 = prim_env * .95
        sec_95 = sec_env * .95

        # squeeze any case where the total of cause attribution is bigger than
        # 95% of envelop so there is some left for residual
        prim_bool = (sigma_sub_prim > prim_95)
        sec_bool = (sigma_sub_sec > sec_95)
        for sub_group, mapper in self.me_map.items():
            if mapper["type"] == "sub_group":
                outputs = mapper.get("trgs", {})
                for imp, me_id in outputs.items():

                    # scale each ME to the 95% if it is bigger
                    to_scale = self.me_dict[me_id]
                    if imp == "prim":
                        scaled = (
                            to_scale[prim_bool] * prim_95[prim_bool] /
                            sigma_sub_prim[prim_bool])
                        scaled.fillna(to_scale[~prim_bool], inplace=True)
                    elif imp == "sec":
                        scaled = (
                            to_scale[sec_bool] * sec_95[sec_bool] /
                            sigma_sub_sec[sec_bool])
                        scaled.fillna(to_scale[~sec_bool], inplace=True)

                    # reassign scaled value
                    self.me_dict[me_id] = scaled

        # calculate the residuals
        prim_resid_key = self.me_map["resid"]["trgs"]["prim"]
        sec_resid_key = self.me_map["resid"]["trgs"]["sec"]
        sqzd_prim = sigma_sub_prim[~prim_bool].fillna(prim_95[prim_bool])
        sqzd_sec = sigma_sub_sec[~sec_bool].fillna(sec_95[sec_bool])
        prim_resid = prim_env - sqzd_prim
        prim_resid = self.remove_negatives(prim_resid)
        self.me_dict[prim_resid_key] = prim_resid
        sec_resid = sec_env - sqzd_sec
        sec_resid = self.remove_negatives(sec_resid)
        self.me_dict[sec_resid_key] = sec_resid

    def calc_excess_group(self, excess_group):
        # compile dataframes
        in_grp = pd.DataFrame()
        out_grp = pd.DataFrame()
        for sub_group, mapper in self.me_map.items():
            excess = mapper.get("excess")
            if excess == excess_group:
                # inputs
                inputs = mapper["srcs"]
                for me_id in inputs.values():
                    out_grp = pd.concat([out_grp, self.me_dict[me_id]])

                # ouputs
                outputs = mapper["trgs"]
                for me_id in outputs.values():
                    in_grp = pd.concat([in_grp, self.me_dict[me_id]])

        # calculate excess
        sigma_in_grp = in_grp.groupby(level=list(
            self.idx_dmnsns.keys())).sum()
        sigma_out_grp = out_grp.groupby(level=list(
            self.idx_dmnsns.keys())).sum()
        ex_bool = (sigma_in_grp > sigma_out_grp)
        excess = sigma_in_grp[ex_bool] - sigma_out_grp[ex_bool]
        excess.fillna(value=0, inplace=True)

        # save to dict
        self.me_dict[self.me_map["excess"]["trgs"][excess_group]] = excess

##############################################################################
# function to run attribution
##############################################################################


def female_attribution(me_map, location_id, out_dir, decomp_step):

    # declare calculation dimensions
    dim = AttributeFemale.default_idx_dmnsns
    dim["location_id"] = location_id
    #dim["age_group_id"] = [7,8,9,10,11,12,13,14,15]
    dim["age_group_id"] = [8,9,10,11,12,13,14]
    dim["measure_id"] = [5]
    dim["sex_id"] = [2]

    # run attribution
    attributer = AttributeFemale(me_map=me_map, decomp_step=decomp_step,
        idx_dmnsns=dim)
    attributer.subtract_primary_only()
    attributer.calc_prim_sec_ratio()
    attributer.split_subgroups()
    attributer.calc_residual()
    for grp in attributer.me_map["excess"]["trgs"].keys():
        attributer.calc_excess_group(excess_group=grp)

    # save resutls to disk
    for mapper in me_map.values():
        outputs = mapper.get("trgs", {})
        for me_id in outputs.values():
            fname = str(location_id[0]) + ".h5"
            out_df = attributer.me_dict[me_id].reset_index()
            out_df.to_hdf(os.path.join(out_dir, str(me_id), fname),
                key="draws", format="table", data_columns=list(dim.keys()))

##############################################################################
# when called as a script
##############################################################################

if __name__ == "__main__":

    # parse command line args
    parser = argparse.ArgumentParser()
    parser.add_argument("--map_name", help=("nested dictionary to pull out of"
                        " infertility map"), required=True, type=str)
    parser.add_argument("--out_dir", help="root directory to save stuff",
                        required=True)
    parser.add_argument("--location_id", help="which location to use",
                        type=parsers.int_parser, nargs="*", required=True)
    parser.add_argument("--decomp_step", required=True,
                        help="the decompostion step associated with this data",
                        type=str)
    args = vars(parser.parse_args())

    # import JSON file with me_id source and target information 
    code_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(code_dir,'infertility_map.json')
    with open(filepath, 'r') as f:
        imap = json.load(f)
    me_map = imap[args["map_name"]]

    # call function
    female_attribution(me_map=me_map, out_dir=args["out_dir"],
                       location_id=args["location_id"],
                       decomp_step=args["decomp_step"])
