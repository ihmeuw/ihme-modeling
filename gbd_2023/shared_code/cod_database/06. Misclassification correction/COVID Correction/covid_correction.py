import os
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm

from cod_prep.claude.claude_io import get_claude_data
from cod_prep.claude.cod_process import CodProcess
from cod_prep.claude.configurator import Configurator
from cod_prep.downloaders.ages import get_cod_ages
from cod_prep.downloaders.causes import get_all_related_causes, get_current_cause_hierarchy, add_cause_metadata
from cod_prep.downloaders.locations import get_current_location_hierarchy, add_location_metadata
from cod_prep.downloaders.population import add_population
from cod_prep.utils import report_duplicates, report_if_merge_fail

CONF = Configurator("standard")

import warnings

warnings.filterwarnings("ignore")

def get_reference_ages(cause, cause_meta):
    ref_ages = [6, 7, 8]
    ihd = get_all_related_causes("cvd_ihd", cause_meta_df=cause_meta)
    valvu = get_all_related_causes('cvd_valvu', cause_meta_df=cause_meta)
    cvd = ihd + valvu + [501, 502, 498, 503]
    if cause in cvd:
        ref_ages = [999]

    if cause in ["515", "529"]:
        ref_ages = [999]

    if cause in ["998", "591", "976", "619"]:
        ref_ages = [999]

    upper_dig = get_all_related_causes('digest_upper', cause_meta_df=cause_meta)
    other_dig = get_all_related_causes('digest_other', cause_meta_df=cause_meta)
    digest = upper_dig + other_dig + [535]
    if cause in digest:
        ref_ages = [999]

    cancers = [444, 411, 414, 441, 453, 456, 423, 426, 459, 462, 432, 435, 465, 438, 474, 486, 1012]
    cancers = get_all_related_causes(cancers, cause_meta_df=cause_meta)
    if cause in cancers:
    	ref_ages = [999]

    return ref_ages

def agg_to_level_three(df, cause_meta):
    inc_cols = [x for x in df.columns if x != 'deaths']

    df = add_cause_metadata(df, ['level', 'path_to_top_parent'], cause_meta_df=cause_meta)
    df = df.loc[df.level >= 3]

    df['cause_id'] = df.path_to_top_parent.apply(lambda x: int(x.split(',')[3]))
    df = df.groupby(inc_cols, as_index=False).deaths.sum()
    return df

def apply_linear_regression(a, b, c, d, e, year=2020, w_uncertainty=True, alpha=0.1):
    year_vals = pd.Series([a, b, c, d, e]).replace({np.inf: np.nan, 0: np.nan})
    if year_vals.isin([np.nan]).all():
        if w_uncertainty:
            return np.nan, np.nan, np.nan
        else:
            return np.nan

    mean_vals = year_vals.dropna()
    year_vals = year_vals.fillna(mean_vals.mean())

    x = np.array([2015, 2016, 2017, 2018, 2019]).reshape(-1, 1)
    y = np.array(year_vals).reshape(-1, 1)
    lr = sm.OLS(y, sm.add_constant(x)).fit()
    prediction = lr.params[1]*year + lr.params[0] 

    if prediction < 0:
        prediction = e / 2

    if w_uncertainty:
        conf_interval = lr.conf_int(alpha)
        upper = conf_interval[1][1]*year + conf_interval[0][0]
        lower = conf_interval[1][0]*year + conf_interval[0][1]

        upper = max(upper, prediction)
        lower = max(lower, 0)

        return prediction, upper, lower
    else:
        return prediction


def gen_relative_rates_global(df, ref_df=False):
    df = df.groupby(["year_id", "cause_id", "age_group_id", "sex_id"], as_index=False)[
        "deaths", "population"
    ].sum()
    df["rate"] = df.deaths / df.population

    if ref_df:
        df["reference_rate"] = df.groupby(["year_id", "cause_id", "sex_id"]).rate.transform(np.mean)
        df = df[["year_id", "cause_id", "sex_id", "reference_rate"]].drop_duplicates()
    else:
        df = df[["year_id", "cause_id", "age_group_id", "sex_id", "rate"]]
        report_duplicates(df, ["year_id", "cause_id", "age_group_id", "sex_id"])

    return df


def gen_relative_rates_country(df, ref_df=False):
    df["rate"] = df.deaths / df.population

    if ref_df:
        df["reference_rate"] = df.groupby(
            ["location_id", "site_id", "year_id", "sex_id", "cause_id"]
        ).rate.transform(np.mean)
        df = df[
            ["location_id", "site_id", "year_id", "sex_id", "cause_id", "reference_rate"]
        ].drop_duplicates()
    else:
        df = df[[
            'location_id', 'year_id', 'site_id', 'age_group_id', 'sex_id',
            'cause_id', 'deaths', 'population', 'rate'
        ]]
        report_duplicates(
            df, ["location_id", "year_id", "site_id", "age_group_id", "sex_id", "cause_id"]
        )

    return df


def run_regressions(df, metric=None, year=None):
    assert metric in ["reference_rate", "relative_dr"]
    if metric == "reference_rate":
        indices = ["sex_id", "cause_id"]
        df = df[["year_id", "sex_id", "cause_id", "reference_rate"]].drop_duplicates()
    else:
        indices = ["age_group_id", "sex_id", "cause_id"]
    df = df.pivot_table(index=indices, columns="year_id", values=metric, fill_value=0).reset_index()
    for col in list(range(2015, 2020)):
        if col not in df.columns:
            df[col] = np.nan
        df = df.rename(columns={col: str(col)})

    df[metric] = df.apply(
        lambda row: apply_linear_regression(
            row["2015"], row["2016"], row["2017"], row["2018"], row["2019"],
            year=year, w_uncertainty=False
        ),
        axis=1,
    )

    return df


class CovidCorrector(CodProcess):
    def __init__(
        self,
        nid,
        extract_type_id,
        iso3,
        year_id,
        code_system_id,
        as_launch_set_id=None,
    ):
        self.conf = CONF
        self.nid = nid
        self.extract_type_id = extract_type_id
        self.iso3 = iso3
        self.year_id = year_id
        self.code_system_id = code_system_id
        self.as_launch_set_id = as_launch_set_id
        self.demo_cols = ["location_id", "year_id", "site_id", "age_group_id", "sex_id"]
        self.incoming_id_cols = [
            "nid", "extract_type_id", "site_id", "location_id", "year_id",
            "age_group_id", "sex_id", "cause_id"
        ]
        if self.as_launch_set_id == 0:
            self.as_launch_set_id = None

        self.covid_cause_id = 1048

        self.cause_meta = get_current_cause_hierarchy(
            cause_set_id=4,
            cause_set_version_id=self.conf.get_id("cause_set_version"),
            force_rerun=False,
            block_rerun=True,
        )
        self.loc_meta = get_current_location_hierarchy(
            location_set_id=35,
            location_set_version_id=self.conf.get_id("location_set_version"),
            force_rerun=False,
            block_rerun=True,
        )
        self.age_meta = get_cod_ages(force_rerun=False, block_rerun=True)

    def needs_a_level_three_prop(self):
        if self.iso3 == 'COL' and self.year_id in [2020, 2021]:
            return True
        elif self.iso3 == 'BIH' and self.year_id == 2021:
            return True
        elif self.iso3 == 'GTM' and self.year_id == 2021:
            return True
        elif self.iso3 == 'ECU' and self.year_id in [2020, 2021]:
            return True
        else:
            return False

    def assign_age_bands(self, df):
        age_df = self.age_meta[["age_group_id", "simple_age"]].drop_duplicates()
        df = df.merge(age_df, on=["age_group_id"], how="left")
        report_if_merge_fail(df, "simple_age", "age_group_id")

        df["age_band"] = np.nan
        df.loc[df.simple_age < 15, "age_band"] = "under_15"
        df.loc[(df.simple_age >= 15) & (df.simple_age < 55), "age_band"] = "15_54"
        df.loc[df.simple_age >= 55, "age_band"] = "55+"
        assert df.age_band.notnull().all()
        df = df.drop(["simple_age"], axis=1)
        return df

    def make_age_flag_df(self, df):
        df = df[["cause_id", "age_group", "sex_id", "coef"]].drop_duplicates()
        report_duplicates(df, ["cause_id", "age_group", "sex_id"])
        df = df.rename(columns={
            "age_group": "age_band", "coef": "coef_weight"}
        )

        dfs = []
        for c in df.cause_id.unique().tolist():
            children = get_all_related_causes(c, cause_meta_df=self.cause_meta)
            for child in children:
                temp = df.loc[df.cause_id == c]
                temp["cause_id"] = child
                dfs.append(temp)
        df = pd.concat(dfs)
        df = df.drop_duplicates(subset=["cause_id", "age_band", "sex_id"])
        df["correction_flag"] = 1
        return df

    def identify_correction_causes(self):
        orig = pd.read_csv(
            self.conf.get_resource("master_covid_selections")
        ).query("cause_id != 1048")

        master = orig.loc[
            (orig.covid_pearson > 0) & (orig.covid_p < 0.05) & (orig.prediction == 1)
        ]

        buffer_demo = master[['cause_id', 'age_group', 'sex_id', 'super_region_name']].drop_duplicates()
        buffer = orig.merge(buffer_demo, how='left', indicator=True)
        buffer = buffer.loc[
            (buffer.covid_pearson > 0) & (buffer.covid_p >= 0.05) & (buffer.covid_p < 0.1) &
            (buffer.prediction == 1) & (buffer._merge == 'both')
        ]
        buffer = buffer.drop('_merge', axis=1)
        master = pd.concat([master, buffer])
        master = master.loc[
            (master.iso3 == self.iso3) &
            (master.year_id == self.year_id)
        ]

        self.age_flag_df = self.make_age_flag_df(master)

        correction_causes = master.cause_id.unique().tolist()
        correction_causes = get_all_related_causes(
            [int(x) for x in correction_causes], cause_meta_df=self.cause_meta
        )

        inj = get_all_related_causes("_inj", cause_meta_df=self.cause_meta)
        alcohol = get_all_related_causes("mental_alcohol", cause_meta_df=self.cause_meta)
        drugs = get_all_related_causes("mental_drug", cause_meta_df=self.cause_meta)
        tb = get_all_related_causes("tb", cause_meta_df=self.cause_meta)
        drop_causes = inj + alcohol + drugs + tb + [375]
        correction_causes = list(set(correction_causes) - set(drop_causes))

        return correction_causes

    def make_cause_specific_rr(self, df, ref_ages, country=False):
        ref = df.loc[df.age_group_id.isin(ref_ages)]

        if country:
            df = gen_relative_rates_country(df, ref_df=False)
            assert set(ref_ages) == set(ref.age_group_id)
            ref = gen_relative_rates_country(ref, ref_df=True)
            df = df.merge(
                ref, on=["location_id", "site_id", "year_id", "sex_id", "cause_id"], how="left"
            )
            df["relative_dr"] = df.rate / df.reference_rate
            return df
        else:
            df = add_population(
                df, pop_run_id=CONF.get_id("pop_run"), force_rerun=False, block_rerun=True
            )
            ref = add_population(
                ref, pop_run_id=CONF.get_id("pop_run"), force_rerun=False, block_rerun=True
            )
            df = gen_relative_rates_global(df)
            ref = gen_relative_rates_global(ref, ref_df=True)
            df = df.merge(ref, on=["year_id", "sex_id", "cause_id"], how="left")
            df["relative_dr"] = df.rate / df.reference_rate

            ref_rate_df = run_regressions(ref, metric="reference_rate", year=self.year_id)
            ref_rate_df = ref_rate_df[["cause_id", "sex_id", "reference_rate"]].drop_duplicates()
            if df.relative_dr.isnull().all():
                return pd.DataFrame()
            df = run_regressions(df, metric="relative_dr", year=self.year_id)

            df = df[["cause_id", "age_group_id", "sex_id", "relative_dr"]].drop_duplicates()
            df = df.merge(ref_rate_df, on=["cause_id", "sex_id"], how="left")
            report_duplicates(df, ["cause_id", "age_group_id", "sex_id"])
            return df

    def generate_counterfactual_estimate(self, correction_ids, level_three=False):
        cfac = get_claude_data(
            "redistribution",
            iso3=self.iso3,
            is_active=True,
            data_type_id=9,
            year_id=range(2015, 2020),
            code_system_id=self.code_system_id,
            as_launch_set_id=self.as_launch_set_id,
            force_rerun=False,
            block_rerun=True,
        )
        if level_three:
            cfac = agg_to_level_three(cfac, self.cause_meta)
        cfac = cfac.loc[cfac.cause_id.isin(correction_ids)]

        cfac = add_location_metadata(cfac, 'ihme_loc_id', location_meta_df=self.loc_meta)
        cfac['ihme_loc_id'] = cfac.ihme_loc_id.str.slice(0, 3)
        cfac = cfac.drop('location_id', axis=1)
        cfac = add_location_metadata(cfac, 'location_id', merge_col='ihme_loc_id', location_meta_df=self.loc_meta)

        if len(cfac) == 0:
            return pd.DataFrame(columns=self.demo_cols + ["cause_id", "counterfactual"])
        cfac = cfac.groupby(self.demo_cols + ["cause_id"], as_index=False).deaths.sum()

        cfac["mean_fill"] = cfac.groupby(
            ["location_id", "site_id", "age_group_id", "sex_id", "cause_id"]
        ).deaths.transform(np.mean)
        cfac = cfac.pivot_table(
            index=["location_id", "site_id", "age_group_id", "sex_id", "cause_id", "mean_fill"],
            columns="year_id",
            values="deaths",
            fill_value=0,
        ).reset_index()
        for col in list(range(2015, 2020)):
            if col not in cfac.columns.tolist():
                cfac[str(col)] = 0
            cfac = cfac.rename(columns={col: str(col)})
            cfac.loc[cfac[str(col)] == 0, str(col)] = cfac["mean_fill"]

        pred_cols = []
        upper_ui_cols = []
        lower_ui_cols = []
        years = [2020, 2021, 2022]
        for year in years:
            lr_col = 'lr_' + str(year)
            pred_col = 'counterfactual_' + str(year)
            upper_col = 'upper_' + str(year)
            lower_col = 'lower_' + str(year)

            pred_cols.append(pred_col)
            upper_ui_cols.append(upper_col)
            lower_ui_cols.append(lower_col)

            cfac[lr_col]= cfac.apply(
                lambda row: apply_linear_regression(
                    row["2015"], row["2016"], row["2017"], row["2018"], row["2019"], year
                ),
                axis=1
            )
            cfac[pred_col] = cfac[lr_col].apply(lambda x: float(x[0]))
            cfac[upper_col] = cfac[lr_col].apply(lambda x: float(x[1]))
            cfac[lower_col] = cfac[lr_col].apply(lambda x: float(x[2]))

        pred = cfac[
            ["location_id", "site_id", "age_group_id", "sex_id", "cause_id"] + pred_cols 
        ]
        uppers = cfac[
            ["location_id", "site_id", "age_group_id", "sex_id", "cause_id"] + upper_ui_cols 
        ]
        lowers = cfac[
            ["location_id", "site_id", "age_group_id", "sex_id", "cause_id"] + lower_ui_cols 
        ]

        pred = pred.melt(id_vars=["location_id", "site_id", "age_group_id", "sex_id", "cause_id"],
            value_vars=pred_cols, var_name='year_id', value_name='counterfactual')
        pred['year_id'] = pred['year_id'].replace(dict(zip(pred_cols, years)))
        uppers = uppers.melt(id_vars=["location_id", "site_id", "age_group_id", "sex_id", "cause_id"],
            value_vars=upper_ui_cols, var_name='year_id', value_name='upper')
        uppers['year_id'] = uppers['year_id'].replace(dict(zip(upper_ui_cols, years)))
        lowers = lowers.melt(id_vars=["location_id", "site_id", "age_group_id", "sex_id", "cause_id"],
            value_vars=lower_ui_cols, var_name='year_id', value_name='lower')
        lowers['year_id'] = lowers['year_id'].replace(dict(zip(lower_ui_cols, years)))

        cfac = pred.merge(uppers, on=['location_id', 'year_id', 'site_id', 'age_group_id', 'sex_id', 'cause_id'])
        cfac = cfac.merge(lowers, on=['location_id', 'year_id', 'site_id', 'age_group_id', 'sex_id', 'cause_id'])

        assert cfac.year_id.isin([2020, 2021, 2022]).all()
        report_duplicates(cfac, self.demo_cols + ["cause_id"])
        return cfac

    def generate_global_rr(self, correction_ids, level_three=False):
        if self.iso3 in self.conf.get_id("subnational_modeled_iso3s"):
            iso_arg = self.iso3
            region_arg = None
            etid_arg = self.extract_type_id
        else:
            iso_arg = None
            region_arg = self.loc_meta.loc[self.loc_meta.ihme_loc_id == self.iso3].region_id.iloc[0]
            etid_arg = None

        gr = get_claude_data(
            "redistribution",
            iso3=iso_arg,
            region_id=region_arg,
            extract_type_id=etid_arg,
            is_active=True,
            year_id=range(2015, 2020),
            data_type_id=9,
            code_system_id=self.code_system_id,
            as_launch_set_id=self.as_launch_set_id,
            force_rerun=False,
            block_rerun=True,
        )
        if level_three:
            gr = agg_to_level_three(gr, self.cause_meta)

        gr = add_location_metadata(gr, 'ihme_loc_id', location_meta_df=self.loc_meta)
        gr['ihme_loc_id'] = gr.ihme_loc_id.str.slice(0, 3)
        gr = gr.drop('location_id', axis=1)
        gr = add_location_metadata(gr, 'location_id', merge_col='ihme_loc_id', location_meta_df=self.loc_meta)

        gr = gr.groupby(self.demo_cols + ["cause_id"], as_index=False).deaths.sum()
        gr = gr.loc[gr.cause_id.isin(correction_ids)]

        grs = []
        for cause in gr.cause_id.unique().tolist():
            ref_ages = get_reference_ages(cause, self.cause_meta)
            temp = gr.loc[gr.cause_id == cause]
            if not set(ref_ages).issubset(set(temp.age_group_id)):
                continue
            temp = self.make_cause_specific_rr(temp, ref_ages)
            if len(temp) > 0:
                grs.append(temp)

        if len(grs) == 0:
            return pd.DataFrame(
                columns=["cause_id", "age_group_id", "sex_id", "reference_rate", "relative_dr"]
            )
        gr = pd.concat(grs)
        return gr

    def generate_data_specific_relative_rates(self, prop_df, correction_ids):
        prop_df = add_location_metadata(prop_df, 'ihme_loc_id', location_meta_df=self.loc_meta)
        prop_df['ihme_loc_id'] = prop_df.ihme_loc_id.str.slice(0, 3)
        prop_df = prop_df.drop('location_id', axis=1)
        prop_df = add_location_metadata(prop_df, 'location_id', merge_col='ihme_loc_id', location_meta_df=self.loc_meta)

        prop_df = prop_df.groupby(self.demo_cols + ["cause_id"], as_index=False).deaths.sum()
        prop_df = prop_df.loc[prop_df.cause_id.isin(correction_ids)]
        prop_df = add_population(
            prop_df, pop_run_id=CONF.get_id("pop_run"), force_rerun=False, block_rerun=True
        )

        prop_dfs = []
        for cause in prop_df.cause_id.unique().tolist():
            ref_ages = get_reference_ages(cause, self.cause_meta)
            temp = prop_df.loc[prop_df.cause_id == cause]
            if not set(ref_ages).issubset(set(temp.age_group_id)):
                temp["reference_rate"] = np.nan
                temp["relative_dr"] = np.nan
                prop_dfs.append(temp)
                continue
            temp = self.make_cause_specific_rr(temp, ref_ages, country=True)
            prop_dfs.append(temp)
        prop_df = pd.concat(prop_dfs)
        return prop_df

    def apply_manual_overrides(self, df):
        if self.iso3 == 'BIH':
            cvd_ihd = get_all_related_causes('cvd_ihd', cause_meta_df=self.cause_meta)
            df.loc[df.cause_id.isin(cvd_ihd), 'expected_deaths'] = df['counterfactual']

        if self.iso3 == 'DEU' and self.year_id == 2022:
            cvd_ihd = get_all_related_causes('cvd_ihd', cause_meta_df=self.cause_meta)
            df.loc[df.cause_id.isin(cvd_ihd), 'expected_deaths'] = df['deaths']

        if self.iso3 == 'POL':
            cvd_afib = get_all_related_causes('cvd_afib', cause_meta_df=self.cause_meta)
            df.loc[df.cause_id.isin(cvd_afib), 'expected_deaths'] = df['deaths']

        if self.iso3 == 'GTM':
            cvd_ihd = get_all_related_causes('cvd_ihd', cause_meta_df=self.cause_meta)
            df.loc[df.cause_id.isin(cvd_ihd), 'expected_deaths'] = df['counterfactual']
        
        return df

    def replace_problem_causes_with_level_three(self, prop_df, prop_df3):
        inc_cols = prop_df.columns.tolist()
        if self.iso3 == 'COL' and self.year_id in [2020, 2021]:
            lri_causes = get_all_related_causes('lri', cause_meta_df=self.cause_meta)
            lri = prop_df3.query("cause_id == 322")[[
                'location_id', 'year_id', 'age_group_id', 'sex_id', 'site_id', 'scaling_prop'
            ]].drop_duplicates()
            prop_df = prop_df.merge(lri, on=['location_id', 'year_id', 'age_group_id', 'sex_id', 'site_id'], how='left', suffixes=("", "_l3"))
            prop_df.loc[prop_df.cause_id.isin(lri_causes), 'scaling_prop'] = prop_df['scaling_prop_l3']

        if self.iso3 == 'BIH' and self.year_id == 2021:
            ihd_causes = get_all_related_causes('cvd_ihd', cause_meta_df=self.cause_meta)
            ihd = prop_df3.query("cause_id == 493")[[
                'location_id', 'year_id', 'age_group_id', 'sex_id', 'site_id', 'scaling_prop'
            ]].drop_duplicates()
            prop_df = prop_df.merge(ihd, on=['location_id', 'year_id', 'age_group_id', 'sex_id', 'site_id'], how='left', suffixes=("", "_l3"))
            prop_df.loc[prop_df.cause_id.isin(ihd_causes), 'scaling_prop'] = prop_df['scaling_prop_l3']

        if self.iso3 == 'ECU' and self.year_id in [2020, 2021]:
            lri_causes = get_all_related_causes('lri', cause_meta_df=self.cause_meta)
            lri = prop_df3.query("cause_id == 322")[[
                'location_id', 'year_id', 'age_group_id', 'sex_id', 'site_id', 'scaling_prop'
            ]].drop_duplicates()
            prop_df = prop_df.merge(lri, on=['location_id', 'year_id', 'age_group_id', 'sex_id', 'site_id'], how='left', suffixes=("", "_l3"))
            prop_df.loc[prop_df.cause_id.isin(lri_causes), 'scaling_prop'] = prop_df['scaling_prop_l3']

        if self.iso3 == 'GTM' and self.year_id == 2021:
            lri_causes = get_all_related_causes('lri', cause_meta_df=self.cause_meta)
            lri = prop_df3.query("cause_id == 322")[[
                'location_id', 'year_id', 'age_group_id', 'sex_id', 'site_id', 'scaling_prop'
            ]].drop_duplicates()
            prop_df = prop_df.merge(lri, on=['location_id', 'year_id', 'age_group_id', 'sex_id', 'site_id'], how='left', suffixes=("", "_l3"))
            prop_df.loc[prop_df.cause_id.isin(lri_causes), 'scaling_prop'] = prop_df['scaling_prop_l3']

        prop_df = prop_df[inc_cols]
        assert prop_df.notnull().values.all()
        return prop_df


    def generate_correction_factors(self, prop_df, gr, cfac):
        prop_df = self.assign_age_bands(prop_df)
        prop_df = prop_df.merge(self.age_flag_df, on=["age_band", "cause_id", "sex_id"], how="left")
        prop_df["correction_flag"] = prop_df.correction_flag.fillna(0)
        prop_df["coef_weight"] = prop_df.coef_weight.fillna(0)

        prop_df.loc[prop_df.coef_weight > 1, "coef_weight"] = 1

        prop_df = prop_df.merge(
            gr, on=["cause_id", "age_group_id", "sex_id"], how="left", suffixes=("_data", "_global")
        )
        prop_df = prop_df.merge(cfac, on=self.demo_cols + ["cause_id"], how="left")
        prop_df.loc[prop_df.counterfactual.isnull(), "counterfactual"] = prop_df["deaths"]
        prop_df.loc[prop_df.upper.isnull(), "upper"] = prop_df["deaths"]
        prop_df.loc[prop_df.lower.isnull(), "lower"] = prop_df["deaths"]

        prop_df.loc[
            (prop_df.reference_rate_data.notnull()) & (prop_df.reference_rate_global.isnull()),
            "reference_rate_global",
        ] = prop_df["reference_rate_data"]
        prop_df.loc[
            (prop_df.relative_dr_data.notnull()) & (prop_df.relative_dr_global.isnull()),
            "relative_dr_global",
        ] = prop_df["relative_dr_data"]
        assert (
            len(
                prop_df.loc[
                    (prop_df.relative_dr_data.notnull()) & (prop_df.relative_dr_global.isnull())
                ]
            ) == 0
        )

        prop_df["exceeds_global"] = 0
        prop_df.loc[
            (prop_df.relative_dr_data.notnull())
            & (prop_df.relative_dr_data > prop_df.relative_dr_global),
            "exceeds_global",
        ] = 1
        prop_df["correction_deaths"] = prop_df.deaths
        prop_df.loc[prop_df.exceeds_global == 1, "correction_deaths"] = (
            prop_df.reference_rate_global * prop_df.population * prop_df.relative_dr_global
        )

        prop_df["needs_only_cfac"] = 0
        prop_df.loc[
            ((prop_df.relative_dr_data.isnull()) | (prop_df.exceeds_global == 0)) &
            (prop_df.deaths > prop_df.counterfactual),
            "needs_only_cfac"
        ] = 1

        prop_df["expected_deaths"] = prop_df.deaths

        prop_df.loc[
            (prop_df.exceeds_global == 1) & (prop_df.correction_flag == 1), "expected_deaths"
        ] = (prop_df.correction_deaths + prop_df.counterfactual) / 2

        prop_df.loc[
            (prop_df.needs_only_cfac == 1) & (prop_df.correction_flag == 1), "expected_deaths"
        ] = prop_df.counterfactual

        prop_df.loc[(prop_df.expected_deaths > prop_df.upper) & (prop_df.correction_flag == 1),
            'expected_deaths'] = prop_df['upper']
        prop_df.loc[(prop_df.expected_deaths < prop_df.lower) & (prop_df.correction_flag == 1),
            'expected_deaths'] = prop_df['lower']

        prop_df = self.apply_manual_overrides(prop_df)
        prop_df.loc[
            prop_df.expected_deaths >= prop_df.deaths,
            "expected_deaths",
        ] = prop_df['deaths']

        prop_df = prop_df.loc[prop_df.deaths > 0]
        prop_df["scaling_prop"] = (prop_df.deaths - prop_df.expected_deaths) / prop_df.deaths
        prop_df.loc[prop_df.correction_flag != 1, "scaling_prop"] = 0
        assert (prop_df.scaling_prop >= 0).all() & (prop_df.scaling_prop <= 1).all()
        report_duplicates(prop_df, self.demo_cols + ["cause_id"])
        prop_df = prop_df[self.demo_cols + ["cause_id", "scaling_prop", "coef_weight"]].drop_duplicates()

        prop_df['scaling_prop'] = prop_df.scaling_prop * prop_df.coef_weight
        prop_df = prop_df.drop(["coef_weight"], axis=1)
        return prop_df

    def scale_observations_and_create_covid(self, df, prop_df):
        df = add_location_metadata(df, 'iso3', location_meta_df=self.loc_meta)
        prop_df = add_location_metadata(prop_df, 'iso3', location_meta_df=self.loc_meta)
        prop_df = prop_df.drop('location_id', axis=1)
        merge_cols = [x for x in self.demo_cols if x != 'location_id'] + ['iso3']

        df = df.merge(prop_df, on=merge_cols + ["cause_id"], how="left")
        df["scaling_prop"] = df["scaling_prop"].fillna(0)
        df["covid_deaths"] = df.deaths * df.scaling_prop
        df["deaths"] = df.deaths * (1 - df.scaling_prop)

        covid_df = df.loc[df.covid_deaths > 0]
        covid_df["cause_id"] = self.covid_cause_id
        covid_df["deaths"] = covid_df["covid_deaths"]
        df = df.append(covid_df, ignore_index=True)

        df = df.groupby(self.incoming_id_cols, as_index=False).deaths.sum()
        return df

    def get_computed_dataframe(self, df):
        incoming_deaths = df.deaths.sum()

        correction_ids = self.identify_correction_causes()
        if len(correction_ids) == 0:
            return df

        gr = self.generate_global_rr(correction_ids)
        cfac = self.generate_counterfactual_estimate(correction_ids)

        prop_df = df.copy(deep=True)
        prop_df = self.generate_data_specific_relative_rates(prop_df, correction_ids)

        prop_df = self.generate_correction_factors(prop_df, gr, cfac)
        if self.needs_a_level_three_prop():
            gr3 = self.generate_global_rr(correction_ids, level_three=True)
            cfac3 = self.generate_counterfactual_estimate(correction_ids, level_three=True)
            prop_df3 = df.copy(deep=True)
            prop_df3 = agg_to_level_three(prop_df3, self.cause_meta)
            prop_df3 = self.generate_data_specific_relative_rates(prop_df3, correction_ids)
            prop_df3 = self.generate_correction_factors(prop_df3, gr3, cfac3)
            prop_df = self.replace_problem_causes_with_level_three(prop_df, prop_df3)

        df = self.scale_observations_and_create_covid(df, prop_df)
        assert np.allclose(incoming_deaths, df.deaths.sum())
        return df
