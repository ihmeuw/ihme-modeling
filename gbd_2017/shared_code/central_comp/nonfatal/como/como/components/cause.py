
from hierarchies import dbtrees

from como import residuals
from como import common


class CauseResultComputer(object):

    def __init__(self, como_version):
        # inputs
        self.como_version = como_version

        # set up the dimensions we are using
        self.dimensions = self.como_version.nonfatal_dimensions

    @property
    def index_cols(self):
        # measure/birth dimension doesn't matter for columns
        return self.dimensions.get_cause_dimensions(5, False).index_names

    @property
    def draw_cols(self):
        # measure/birth dimension doesn't matter for columns
        return self.dimensions.get_cause_dimensions(5, False).data_list()

    def aggregate_cause(self, df, cause_set_version_id):
        ct = dbtrees.causetree(
            cause_set_version_id=cause_set_version_id)
        df = common.agg_hierarchy(
            tree=ct,
            df=df,
            index_cols=self.index_cols,
            data_cols=self.draw_cols,
            dimension="cause_id")
        df = df[self.index_cols + self.draw_cols]
        for col in self.index_cols:
            df[col] = df[col].astype(int)
        return df

    def other_drug(self, yld_df):
        """
        Use proportion of other drug users, exclusive of those who also use
        cocaine or amphetamines.
        """
        other_drug_prop = .0024216
        other_drug_se = .00023581
        other_drug_draws = common.draw_from_beta(
            other_drug_prop, other_drug_se, len(self.draw_cols))

        amph_coc_prop = .00375522
        amph_coc_se = .00029279
        amph_coc_draws = common.draw_from_beta(
            amph_coc_prop, amph_coc_se, len(self.draw_cols))

        ratio_draws = other_drug_draws / amph_coc_draws

        other_drug_ylds = yld_df[yld_df.cause_id.isin([563, 564])]
        other_drug_ylds = other_drug_ylds.groupby(
            ["location_id", "year_id", "sex_id", "age_group_id"]).sum()
        other_drug_ylds = other_drug_ylds.reset_index()
        dcs = other_drug_ylds.filter(like="draw").columns
        other_drug_ylds.loc[:, dcs] = other_drug_ylds.loc[
            :, dcs].values * ratio_draws

        other_drug_ylds["cause_id"] = 566
        other_drug_ylds["measure_id"] = 3
        other_drug_ylds = other_drug_ylds[self.index_cols + self.draw_cols]
        for col in self.index_cols:
            other_drug_ylds[col] = other_drug_ylds[col].astype(int)
        return other_drug_ylds

    def residuals(self, cause_ylds, simulated_ylds):
        location_id = cause_ylds["location_id"].unique().item()
        ratio_df = residuals.calc(
            location_id=location_id,
            ratio_df=self.como_version.global_ratios,
            output_type="cause_id",
            drawcols=self.draw_cols,
            seq_ylds=simulated_ylds,
            cause_ylds=cause_ylds)
        ratio_df = ratio_df[self.index_cols + self.draw_cols]
        for col in self.index_cols:
            ratio_df[col] = ratio_df[col].astype(int)
        return ratio_df
