import pandas as pd
import numpy as np

from gbd.constants import measures
from hierarchies import dbtrees

from como.legacy import residuals
from como.legacy import common


class CauseResultComputer:
    def __init__(self, como_version):

        self.como_version = como_version

        self.dimensions = self.como_version.nonfatal_dimensions

    @property
    def index_cols(self):
        return self.dimensions.get_cause_dimensions(
            measure_id=measures.PREVALENCE, at_birth=False
        ).index_names

    @property
    def draw_cols(self):
        return self.dimensions.get_cause_dimensions(
            measure_id=measures.PREVALENCE, at_birth=False
        ).data_list()

    def aggregate_cause(self, df, cause_set_version_id):
        ct = dbtrees.causetree(
            cause_set_version_id=cause_set_version_id,
            gbd_round_id=self.como_version.gbd_round_id,
        )
        df = common.agg_hierarchy(
            tree=ct,
            df=df,
            index_cols=self.index_cols,
            data_cols=self.draw_cols,
            dimension="cause_id",
        )
        df = df[self.index_cols + self.draw_cols]
        for col in self.index_cols:
            df[col] = df[col].astype(int)
        all_done = df.cause_id.unique().tolist()

        return df.reset_index(drop=True)

    def other_drug(self, yld_df):
        """
        Use proportion of other drug users, exclusive of those who also use
        cocaine or amphetamines.
        """
        other_drug_prop = 0.0024216
        other_drug_se = 0.00023581
        other_drug_draws = common.draw_from_beta(
            other_drug_prop, other_drug_se, len(self.draw_cols)
        )
        amph_coc_prop = 0.00375522
        amph_coc_se = 0.00029279
        amph_coc_draws = common.draw_from_beta(
            amph_coc_prop, amph_coc_se, len(self.draw_cols)
        )

        ratio_draws = other_drug_draws / amph_coc_draws

        other_drug_ylds = yld_df[yld_df.cause_id.isin([563, 564])]
        other_drug_ylds = other_drug_ylds.groupby(
            ["location_id", "year_id", "sex_id", "age_group_id"]
        ).sum()
        other_drug_ylds = other_drug_ylds.reset_index()
        dcs = other_drug_ylds.filter(like="draw").columns
        other_drug_ylds.loc[:, dcs] = other_drug_ylds.loc[:, dcs].values * ratio_draws

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
            cause_ylds=cause_ylds,
        )
        ratio_df = ratio_df[self.index_cols + self.draw_cols]
        for col in self.index_cols:
            ratio_df[col] = ratio_df[col].astype(int)
        return ratio_df

    def aggregate_cause_prevalence(
        self, prev_df: pd.DataFrame, cause_set_version_id: int
    ) -> pd.DataFrame:
        """Calculate non-most-detailed parent cause aggregate prevalence.
        Combines each cause aggregate parents child cause draws as 1 - (the
        product of (1 - combined most-detailed cause prevalence)).
        """
        cause_prev = []
        ct = dbtrees.causetree(
            cause_set_version_id=cause_set_version_id,
            gbd_round_id=self.como_version.gbd_round_id,
        )
        lvl = ct.max_depth() - 1
        agg_cause_exceptions = (
            self.como_version.agg_cause_exceptions.parent_id.unique().tolist()
        )

        def combine_draws(df: pd.DataFrame) -> pd.DataFrame:
            return pd.Series(
                1 - np.prod(1 - df[self.draw_cols].values, axis=0),
                index=self.draw_cols
            )

        while lvl >= 0:
            for node in ct.level_n_descendants(lvl):
                child_ids = [c.id for c in node.leaves()]
                cause_id = node.id

                if child_ids and cause_id not in agg_cause_exceptions:
                    df = prev_df.loc[(prev_df["cause_id"].isin(child_ids))]
                    df["cause_id"] = cause_id
                    df = df.groupby(self.index_cols)\
                           .apply(combine_draws)\
                           .reset_index()

                    cause_prev.append(df)
            lvl -= 1

        agg_cause_df = pd.concat(cause_prev)
        return agg_cause_df[self.index_cols + self.draw_cols]
