import pandas as pd

from db_queries.core.cause import view_cause_hierarchy_history
from gbd.constants import measures
from hierarchies import dbtrees, tree

from como import residuals
from como import common


class CauseResultComputer:

    def __init__(self, como_version):
        # inputs
        self.como_version = como_version

        # set up the dimensions we are using
        self.dimensions = self.como_version.nonfatal_dimensions

    @property
    def index_cols(self):
        return self.dimensions.get_cause_dimensions(
            measure_id=measures.PREVALENCE, at_birth=False).index_names

    @property
    def draw_cols(self):
        return self.dimensions.get_cause_dimensions(
            measure_id=measures.PREVALENCE, at_birth=False).data_list()

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
        all_done = df.cause_id.unique().tolist()

        cflat = view_cause_hierarchy_history(
            self.como_version.reporting_cause_set_version_id)
        cflat = cflat[['cause_id', 'path_to_top_parent', 'parent_id',
                       'cause_name', 'acause', 'cause_outline']]
        roots = cflat[cflat.cause_id == cflat.parent_id].parent_id
        cts = []
        for root in roots:
            thisflat = cflat[
                cflat.path_to_top_parent.str.startswith(str(root))]
            ct = tree.parent_child_to_tree(
                thisflat,
                'parent_id',
                'cause_id',
                info_cols=['cause_name', 'acause', 'cause_outline'])
            cts.append(ct)

        reporting_dfs = []
        for ct in cts:
            rep_only = common.agg_hierarchy(
                tree=ct,
                df=df.copy(deep=True),
                index_cols=self.index_cols,
                data_cols=self.draw_cols,
                dimension="cause_id")
            reporting_dfs.append(rep_only)
        reporting_df = pd.concat(reporting_dfs)

        reporting_df = reporting_df[~reporting_df["cause_id"].isin(all_done)]
        reporting_df = reporting_df[self.index_cols + self.draw_cols]
        for col in self.index_cols:
            reporting_df[col] = reporting_df[col].astype(int)

        return pd.concat([df, reporting_df]).reset_index(drop=True)

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

        # cause_id 563 - Cocaine use disorders
        # cause_id 564 - Amphetamine use disorders
        other_drug_ylds = yld_df[yld_df.cause_id.isin([563, 564])]
        other_drug_ylds = other_drug_ylds.groupby(
            ["location_id", "year_id", "sex_id", "age_group_id"]).sum()
        other_drug_ylds = other_drug_ylds.reset_index()
        dcs = other_drug_ylds.filter(like="draw").columns
        other_drug_ylds.loc[:, dcs] = other_drug_ylds.loc[
            :, dcs].values * ratio_draws

        # cause_id 566 - Other drug use disorders
        other_drug_ylds["cause_id"] = 566
        other_drug_ylds["measure_id"] = measures.YLD
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
