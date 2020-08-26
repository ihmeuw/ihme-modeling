import os
import argparse
import pandas as pd
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from job_utils import draws, parsers

##############################################################################
# Simple math for envelope calculation
##############################################################################


class SexEnvelopeMkr(object):

    def __init__(self, male_prop_id, female_prop_id, exp_id, env_id, year_id):

        # base dimensions importer
        dim = draws.SquareImport.default_idx_dmnsns
        dim["year_id"] = year_id
        dim["measure_id"] = [5]
        self.base_importer = draws.SquareImport(idx_dmnsns=dim)

        # import and scale sex proportions
        self.male_prop = self.import_sex_prop(male_prop_id, 1)
        self.female_prop = self.import_sex_prop(female_prop_id, 2)
        self.scale_props()

        # import exposure and env
        self.exposure = self.import_exposure(exp_id)
        self.envelope = self.import_couples(env_id)

    def import_sex_prop(self, modelable_entity_id, sex_id):

        # desired index shape (contains prev and incidence in index, no sex_id)
        broadcast_idx_dmnsns = self.base_importer.idx_dmnsns.copy()
        broadcast_idx_dmnsns.pop("sex_id")
        broadcast_idx = pd.MultiIndex.from_product(
            broadcast_idx_dmnsns.values(),
            names=broadcast_idx_dmnsns.keys())
        broadcast_df = pd.DataFrame(index=broadcast_idx)

        # get import shape
        gopher_idx_dmnsns = broadcast_idx_dmnsns.copy()
        gopher_idx_dmnsns["sex_id"] = [sex_id]
        gopher_idx_dmnsns["measure_id"] = [18]

        # import using specified dimensions
        importer = draws.SquareImport(idx_dmnsns=gopher_idx_dmnsns)
        tmp_df = importer.import_square(
            {"modelable_entity_id": modelable_entity_id},
            source="epi")

        # broadcast to preferred shape
        broadcast_over = broadcast_idx_dmnsns.keys()
        broadcast_over.remove("measure_id")
        tmp_df = tmp_df.reset_index()[broadcast_over + importer.draw_cols]
        df = pd.merge(broadcast_df.reset_index(), tmp_df, on=broadcast_over,
                      how="left").set_index(broadcast_idx_dmnsns.keys())
        df.fillna(value=0, inplace=True)
        return df

    def import_exposure(self, modelable_entity_id):

        # desired index shape (contains prev and incidence in index and male)
        broadcast_idx_dmnsns = self.base_importer.idx_dmnsns.copy()
        broadcast_df = self.base_importer.get_index_df()

        # get import shape
        gopher_idx_dmnsns = broadcast_idx_dmnsns.copy()
        gopher_idx_dmnsns["sex_id"] = [2]

        # import using specified dimensions
        importer = draws.SquareImport(idx_dmnsns=gopher_idx_dmnsns)
        tmp_df = importer.import_square(
            {"modelable_entity_id": modelable_entity_id},
            source="epi")

        # broadcast to preferred shape
        broadcast_over = broadcast_idx_dmnsns.keys()
        broadcast_over.remove("sex_id")
        tmp_df = tmp_df.reset_index()[broadcast_over + importer.draw_cols]
        df = pd.merge(broadcast_df.reset_index(), tmp_df, on=broadcast_over,
                      how="left").set_index(broadcast_idx_dmnsns.keys())
        df.fillna(value=0, inplace=True)
        return df

    def import_couples(self, modelable_entity_id):

        # desired index shape (contains prev and incidence in index and male)
        broadcast_idx_dmnsns = self.base_importer.idx_dmnsns.copy()
        broadcast_df = self.base_importer.get_index_df()

        # get import shape
        gopher_idx_dmnsns = broadcast_idx_dmnsns.copy()
        gopher_idx_dmnsns["sex_id"] = [2]

        # import using specified dimensions
        importer = draws.SquareImport(idx_dmnsns=gopher_idx_dmnsns)
        tmp_df = importer.import_square(
            {"modelable_entity_id": modelable_entity_id},
            source="epi")

        # broadcast to preferred shape
        broadcast_over = broadcast_idx_dmnsns.keys()
        broadcast_over.remove("sex_id")
        tmp_df = tmp_df.reset_index()[broadcast_over + importer.draw_cols]
        df = pd.merge(broadcast_df.reset_index(), tmp_df, on=broadcast_over,
                      how="left").set_index(broadcast_idx_dmnsns.keys())
        df.fillna(value=0, inplace=True)
        return df

    def scale_props(self):
        # get final shape of scaled props
        concat_df = self.base_importer.get_index_df()

        # find which draws don't equal at least 1
        total_prop = self.male_prop + self.female_prop
        under_1 = (total_prop < 1)

        # scale the ones that don't equal at least 1
        scaled_male = self.male_prop[under_1] / total_prop[under_1]
        scaled_female = self.female_prop[under_1] / total_prop[under_1]

        # combined the newly scaled with the stuff not in need of scaling
        new_male = scaled_male.fillna(self.male_prop[~under_1])
        new_female = scaled_female.fillna(self.female_prop[~under_1])

        # reshape to have all demographics
        new_male = new_male.reset_index()
        new_male["sex_id"] = 1
        new_male = new_male.set_index(self.base_importer.idx_dmnsns.keys())
        new_male = pd.concat([concat_df, new_male], axis=1)
        new_male.fillna(value=0, inplace=True)

        new_female = new_female.reset_index()
        new_female["sex_id"] = 2
        new_female = new_female.set_index(self.base_importer.idx_dmnsns.keys())
        new_female = pd.concat([concat_df, new_female], axis=1)
        new_female.fillna(value=0, inplace=True)

        # reassign the new values to self
        self.male_prop = new_male
        self.female_prop = new_female

    def sex_env(self, sex):
        if sex == "male":
            return self.male_prop * self.exposure * self.envelope
        elif sex == "female":
            return self.female_prop * self.exposure * self.envelope
        else:
            raise Exception("option: " + sex + " is not allowed")


def calculate_envs(male_prop_id, female_prop_id, exp_id, env_id, male_env_id,
                   female_env_id, year_id, out_dir):

    # initialize env maker class
    sex_env_mkr = SexEnvelopeMkr(
        male_prop_id=male_prop_id, exp_id=exp_id, env_id=env_id,
        female_prop_id=female_prop_id, year_id=year_id)

    # make sex specific envs
    male_sex_env = sex_env_mkr.sex_env(sex="male")
    female_sex_env = sex_env_mkr.sex_env(sex="female")

    # export male
    male_keys = male_sex_env.index.names
    male_sex_env = male_sex_env.reset_index()
    male_sex_env.to_hdf(
        os.path.join(out_dir, str(male_env_id), str(year_id[0]) + ".h5"),
        key="draws", format="table", data_columns=male_keys)

    # export female
    female_keys = female_sex_env.index.names
    female_sex_env = female_sex_env.reset_index()
    female_sex_env.to_hdf(
        os.path.join(out_dir, str(female_env_id), str(year_id[0]) + ".h5"),
        key="draws", format="table", data_columns=female_keys)

##############################################################################
# when called as a script
##############################################################################

if __name__ == "__main__":

    # parse command line args
    parser = argparse.ArgumentParser()
    parser.add_argument("--male_prop_id", required=True,
                        help="me_id, proportion of infertility due to males",
                        type=parsers.int_parser)
    parser.add_argument("--female_prop_id", required=True,
                        help="me_id, proportion of infertility due to females",
                        type=parsers.int_parser)
    parser.add_argument("--exp_id", required=True,
                        help="me_id, proportion of couples exposed to infert",
                        type=parsers.int_parser)
    parser.add_argument("--env_id", required=True,
                        help="me_id, total infertiltiy envelope",
                        type=parsers.int_parser)
    parser.add_argument("--male_env_id", required=True,
                        help="me_id, where to save the computed male env",
                        type=parsers.int_parser)
    parser.add_argument("--female_env_id", required=True,
                        help="me_id, where to save the computed female env",
                        type=parsers.int_parser)
    parser.add_argument("--year_id", type=parsers.int_parser, nargs="*",
                        required=True, help="which year to calculate for")
    parser.add_argument("--out_dir", help="root directory to save stuff",
                        required=True)
    args = vars(parser.parse_args())

    # run function
    calculate_envs(
        male_prop_id=args["male_prop_id"],
        female_prop_id=args["female_prop_id"],
        exp_id=args["exp_id"],
        env_id=args["env_id"],
        male_env_id=args["male_env_id"],
        female_env_id=args["female_env_id"],
        year_id=args["year_id"],
        out_dir=args["out_dir"])
