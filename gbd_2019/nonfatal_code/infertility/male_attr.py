import os
import argparse
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from job_utils import draws, parsers


##############################################################################
# class to calculate attribution
##############################################################################

class AttributeMale(draws.SquareImport):

    def __init__(self, me_map, **kwargs):
        # super init
        super(AttributeMale, self).__init__(**kwargs)

        # store data by me in this dict 
        # key=me_id, val=dataframe
        self.me_map = me_map
        self.me_dict = {}

        # import every input
        for v in me_map.values():
            inputs = v.get("srcs", {})
            for me_id in inputs.values():
                self.me_dict[me_id] = self.import_square(
                    gopher_what={"modelable_entity_id": me_id},
                    source="epi")

    def remove_negatives(self, df):
        positive = df.copy()
        positive[positive<0.] = 0.
        assert (positive < 0.0).any().any() == False
        return positive

    def calc_residual(self):

        # compile keys
        env_prim_key = self.me_map["env"]["srcs"]["prim"]
        env_sec_key = self.me_map["env"]["srcs"]["sec"]
        kline_bord_key = self.me_map["kline"]["srcs"]["bord"]
        kline_mild_key = self.me_map["kline"]["srcs"]["mild"]
        kline_asym_key = self.me_map["kline"]["srcs"]["asym"]
        idio_prim_key = self.me_map["idio"]["trgs"]["prim"]
        idio_sec_key = self.me_map["idio"]["trgs"]["sec"]
        cong_uro_inf_only_key = self.me_map["cong_uro"]["srcs"]["inf_only"]
        cong_uro_ag_key = self.me_map["cong_uro"]["srcs"]["ag"]
        cong_uro_uti_key = self.me_map["cong_uro"]["srcs"]["uti"]
        cong_uro_imp_key = self.me_map["cong_uro"]["srcs"]["imp"]
        cong_uro_ag_uti_key = self.me_map["cong_uro"]["srcs"]["ag_uti"]
        cong_uro_ag_imp_key = self.me_map["cong_uro"]["srcs"]["ag_imp"]
        cong_uro_imp_uti_key = self.me_map["cong_uro"]["srcs"]["imp_uti"]
        cong_uro_ag_imp_uti_key = self.me_map["cong_uro"]["srcs"]["ag_imp_uti"]

        # sum up klinefelter
        sigma_kline = (
            self.me_dict[kline_bord_key] + self.me_dict[kline_mild_key] +
            self.me_dict[kline_asym_key])

        #sum up congenital urogenital
        sigma_cong_uro = (
            self.me_dict[cong_uro_inf_only_key] + self.me_dict[cong_uro_ag_key] +
            self.me_dict[cong_uro_uti_key] + self.me_dict[cong_uro_imp_key] +
            self.me_dict[cong_uro_ag_uti_key] + self.me_dict[cong_uro_ag_imp_key] +
            self.me_dict[cong_uro_imp_uti_key] + self.me_dict[cong_uro_ag_imp_uti_key]
        )

        # subtract klinefleter and congenital urogenital from total primary
        idio_prim = self.me_dict[env_prim_key] - sigma_kline - sigma_cong_uro
        idio_prim = self.remove_negatives(idio_prim)
        self.me_dict[idio_prim_key] = idio_prim
        self.me_dict[idio_sec_key] = self.me_dict[env_sec_key]

##############################################################################
# function to run attribution
##############################################################################


def male_attribution(me_map, location_id, out_dir):

    # declare calculation dimensions
    dim = AttributeMale.default_idx_dmnsns
    dim["location_id"] = location_id
    dim["measure_id"] = [5]
    dim["sex_id"] = [1]

    # run attribution
    attributer = AttributeMale(me_map=me_map, idx_dmnsns=dim)
    attributer.calc_residual()

    # save results to disk
    for mapper in me_map.values():
        outputs = mapper.get("trgs", {})
        for me_id in outputs.values():
            fname = str(location_id[0]) + ".h5"
            out_df = attributer.me_dict[me_id].reset_index()
            out_df.to_hdf(os.path.join(out_dir, str(me_id), fname), 
                key="draws", format="table", data_columns=dim.keys())

##############################################################################
# when called as a script
##############################################################################

if __name__ == "__main__":

    # parse command line args
    parser = argparse.ArgumentParser()
    parser.add_argument("--me_map", help="json style string map of ops",
                        required=True, type=parsers.json_parser)
    parser.add_argument("--out_dir", help="root directory to save stuff",
                        required=True)
    parser.add_argument("--location_id", help="which year to use",
                        type=parsers.int_parser, nargs="*", required=True)
    args = vars(parser.parse_args())

    # call function
    male_attribution(me_map=args["me_map"], out_dir=args["out_dir"],
                     location_id=args["location_id"])
