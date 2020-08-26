import pandas as pd
import numpy as np
import os

from cod_prep.claude.cod_process import CodProcess
from cod_prep.downloaders.causes import get_parent_and_childen_causes
from cod_prep.downloaders.locations import (
    get_country_level_location_id, add_location_metadata
)

from cod_prep.downloaders.ages import add_age_metadata
from cod_prep.claude.configurator import Configurator
from cod_prep.utils import report_if_merge_fail

pd.options.mode.chained_assignment = None

CONF = Configurator('standard')
N_DRAWS = CONF.get_resource('uncertainty_draws')


class RTIAdjuster(CodProcess):
    """
        All of the police data from the Various_RTI folder is RTI only.
        It needs an adjustment similar to the injury-fraction adjustment

        Implemented in run_phase_aggregation.py

    """

    death_cols = ['deaths', 'deaths_corr', 'deaths_raw', 'deaths_rd']
    rti_sources = ['Various_RTI']

    def __init__(self, df, cause_meta_df, age_meta_df, location_meta_df):
        self.df = df
        self.merge_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id']
        self.orig_cols = df.columns
        self.cmdf = cause_meta_df
        self.amdf = age_meta_df
        self.lmdf = location_meta_df

    def get_computed_dataframe(self):
        """Compute that dataframe."""
        # first, recompute the cause fractions without
        # any non-injuries to be sure that we're working with injury only

        # drop non inj_trans_x causes
        inj_trans_causes = list(self.cmdf[
            self.cmdf.acause.str.startswith('inj_trans')
        ].cause_id.unique())
        df = self.df[self.df.cause_id.isin(inj_trans_causes)]
        # remake cf proportion
        df = df.groupby(['location_id', 'year_id',
                         'sex_id', 'age_group_id',
                         'nid', 'extract_type_id']).apply(self.cf_without_cc_code)
        df = self.apply_rti_fractions(df)
        df = self.cleanup(df)
        # optional diagnostics
        self.diag_df = df.copy()
        return df

    def cf_without_cc_code(self, df):
        df['cf'] = df['cf'] / df.cf.sum()
        return df

    def apply_rti_fractions(self, df):
        rti_fraction_input = pd.read_csv(CONF.get_resource('rti_fractions_data'))
        df = df.merge(rti_fraction_input, how='left', on=self.merge_cols)
        df['cf'] = df['cf'] * df['rti_fractions']
        return df

    def cleanup(self, df):
        df = df.drop(['rti_fractions'], axis=1)
        return df

    def get_diagnostic_dataframe(self):
        """Return diagnostics."""
        try:
            return self.diag_df
        except AttributeError:
            print(
                "You requested the diag dataframe before it was ready, "
                "returning an empty dataframe."
            )
        return pd.DataFrame()


class MaternalHIVRemover(CodProcess):
    """Remove HIV from maternal sources.

    This is an extra step in addition to the SampleSizeCauseRemover
    specific to maternal data.

    During formatting, some maternal data has all cause death totals and
    maternal deaths while others only have maternal deaths. Data with
    only maternal deaths requires that an artifical total deaths be
    created using the HIV free envelope. These data do NOT need to be adjusted.
    Maternal data sources reporting all cause deaths likely include HIV and,
    therefore, need to be adjusted to remove HIV.

    Another way to think about it-- is this step adjusts cc_code to be HIV free
    """

    death_cols = ['deaths', 'deaths_corr', 'deaths_raw', 'deaths_rd']

    def __init__(self, df, env_meta_df, env_hiv_meta_df, source, nid):
        self.df = df
        # source/nids that use the envelope in formatting
        self.maternal_env_sources = CONF.get_resource('maternal_env_sources')
        # ages 10-50
        self.maternal_ages = range(7, 16)
        # hiv free envelope
        self.env_meta_df = env_meta_df
        # with HIV envelope
        self.env_hiv_meta_df = env_hiv_meta_df
        self.merge_cols = ['age_group_id', 'location_id', 'year_id', 'sex_id']
        self.orig_cols = df.columns
        self.source = source
        self.nid = nid
        self.maternal_cause_id = 366
        self.cc_code = 919

    def get_computed_dataframe(self):
        """Compute that dataframe."""
        df = self.flag_observations_to_adjust()
        env_df = self.calculate_hiv_envelope_ratio()
        df = self.adjust_deaths(df, env_df)

        # optional diagnostics
        self.diag_df = df.copy()

        # clean up final and process columns
        df = self.cleanup(df)
        return df

    def get_diagnostic_dataframe(self):
        """Return diagnostics."""
        try:
            return self.diag_df
        except AttributeError:
            print(
                "You requested the diag dataframe before it was ready, "
                "returning an empty dataframe."
            )
            return pd.DataFrame()

    def flag_observations_to_adjust(self):
        """Flag the rows where we need to adjust the sample size."""
        df = self.df.copy()
        # look for sources/nids in spreadsheet produced by formatting
        try:
            nid_df = pd.read_excel(
                self.maternal_env_sources.format(source=self.source)
            )
            if self.nid in nid_df.NID.unique():
                df['used_env'] = 1
            else:
                df['used_env'] = 0
        except IOError:
            df['used_env'] = 0

        # subset to maternal causes/ages/sex
        # if maternal causes are not in the cause list, then do not adjust
        if self.maternal_cause_id in df.cause_id.unique():
            df.loc[
                (df['used_env'] == 0) & (df['sex_id'] == 2) &
                (df['cause_id'] == self.cc_code) &
                (df['age_group_id'].isin(self.maternal_ages)), 'adjust'
            ] = 1
            df['adjust'] = df['adjust'].fillna(0)
        else:
            df['adjust'] = 0

        return df

    def calculate_hiv_envelope_ratio(self):
        """Calculate ratio of HIV free envelope / HIV envelope."""
        self.env_hiv_meta_df.rename(
            columns={'mean_env': 'mean_hiv_env'}, inplace=True
        )
        env_df = self.env_hiv_meta_df.merge(
            self.env_meta_df, on=self.merge_cols
        )
        env_df['hiv_ratio'] = env_df['mean_env'] / env_df['mean_hiv_env']
        env_df.drop(['lower_x', 'upper_x', 'lower_y',
                     'upper_y', 'run_id_x', 'run_id_y'], axis=1, inplace=True)

        # make sure the ratio is a reasonable value
        assert (
            (env_df['hiv_ratio'] <= 1.001) & (env_df['hiv_ratio'] > 0)
        ).values.all(), "HIV envelope ratio must be between 1 and 0"

        return env_df

    def adjust_deaths(self, df, env_df):
        """Adjust sample sizes using hiv free: hiv envelope ratio."""
        # merge on envelope with ratio
        df = df.merge(env_df, on=self.merge_cols, how='left')

        # don't remove more than 95% of deaths
        df.loc[df['hiv_ratio'] < .05, 'hiv_ratio'] = .05

        # adjust deaths
        for col in self.death_cols:
            df[col + '_adj'] = df[col].copy()
            df.loc[
                df['adjust'] == 1, col + '_adj'
            ] = df['hiv_ratio'] * df[col + '_adj']

        return df

    def cleanup(self, df):
        """Set adjusted deaths to final values."""
        for col in self.death_cols:
            df[col] = df[col + '_adj'].copy()
        df = df[self.orig_cols]
        return df


class SampleSizeCauseRemover(CodProcess):
    """Remove certain causes (hiv, shocks) from sample size.

    The all-cause mortality envelope that CoDEM uses does not include hiv
    or shocks. So to make cause fractions align with that envelope, hiv
    and shocks must be removed from the denominator. This preserves the
    simple relationship:
        cf (as uploaded to database) * all_deaths (in envelope) = cause_deaths

    This code expects a dataframe with death observations at each level of
    the cause hierarchy. So HIV/AIDs should be present in addition to its
    sub-causes. The sub-causes should roughly add up to HIV/AIDs but it is
    not enforced because we may not have data on all sub-causes, and often
    don't.

    We only want to subtract the number of deaths in the parent hiv cause and
    the parent shocks cause.shocks

    Cause fractions for related causes to the ones being removed from the
    denominator should not have themselves removed from the deonomiator. This
    is uninterpretable and unnecessary - what does it mean to have:
        number of hiv deaths / number of deaths not including hiv/shocks
    Answer: little, and that fraction could be over 1 in high-hiv
    country-year-ages. So we avoid adjusting the related causes.

    """

    # hiv, disaster, war/terrorism/execution
    adjust_causes = [298, 729, 945]
    cf_cols = ['cf', 'cf_rd', 'cf_corr', 'cf_raw']

    def __init__(self, cause_meta_df):
        self.cause_meta_df = cause_meta_df
        self.sample_size_cols = ['location_id', 'year_id', 'sex_id',
                                 'site_id', 'nid', 'extract_type_id',
                                 'age_group_id']
        self.affected_causes = get_parent_and_childen_causes(
            self.adjust_causes, self.cause_meta_df)

    def get_computed_dataframe(self, df):
        """Return computations."""
        df = self.set_adjustment_causes(df)
        df = self.set_affected_causes(df)
        df = self.adjust_sample_size(df)
        df = self.remake_cf(df)
        self.diag_df = df.copy()
        df = self.cleanup(df)
        return df

    def get_diagnostic_dataframe(self):
        """Return diagnostics."""
        try:
            return self.diag_df
        except AttributeError:
            print(
                "You requested the diag dataframe before it was ready, "
                "returning an empty dataframe."
            )
            return pd.DataFrame()

    def set_adjustment_causes(self, df):
        """Flag rows that contain deaths we want to remove from sample size."""
        df = df.copy()
        df['is_adjustment_cause'] = 0
        is_adjustment_cause = df['cause_id'].isin(self.adjust_causes)
        df.loc[is_adjustment_cause, "is_adjustment_cause"] = 1
        return df

    def set_affected_causes(self, df):
        """Mark rows that are for causes we do not want to adjust."""
        df = df.copy()
        df['is_affected_cause'] = 0
        df.loc[
            df['cause_id'].isin(self.affected_causes),
            "is_affected_cause"
        ] = 1
        return df

    def adjust_sample_size(self, df):
        """Remove adjustment cause deaths from sample size."""
        df = df.copy()
        is_adjust_cause = df['is_adjustment_cause'] == 1
        df.loc[is_adjust_cause, 'deaths_remove'] = df['cf'] * df['sample_size']
        df['deaths_remove'] = df['deaths_remove'].fillna(0)

        df['sample_size_remove'] = df.groupby(
            self.sample_size_cols, as_index=False
        )['deaths_remove'].transform('sum')

        df['sample_size_adj'] = df['sample_size']
        is_affected_cause = df['is_affected_cause'] == 1
        df.loc[
            ~is_affected_cause,
            'sample_size_adj'
        ] = df['sample_size_adj'] - df['sample_size_remove']

        return df

    def remake_cf(self, df):
        """Make new cause fractions with the adjusted sample sizes."""
        df = df.copy()
        for cf_col in self.cf_cols:
            df[cf_col] = (df[cf_col] * df['sample_size']) / \
                df['sample_size_adj']
            # a remnant from squaring, if the group was created completely
            # in squaring, the previous calculation does 0/0 which is undefined
            # it is 0/0 because when the group (age sex location year) is
            # created completely from squaring, it is given sample_size 0
            # and cf 0
            df[cf_col] = df[cf_col].fillna(0)
        df['deaths'] = df['sample_size_adj'] * df['cf']
        return df

    def cleanup(self, df):
        """Keep only adjusted sample size and drop process columns."""
        df = df.copy()
        df = df.drop(
            ['is_affected_cause', 'is_adjustment_cause', 'sample_size',
             'deaths_remove', 'sample_size_remove'],
            axis=1
        )
        df.rename(columns={'sample_size_adj': 'sample_size'}, inplace=True)
        return df


class Raker(CodProcess):
    """Sqeeze/Expand sub national deaths into national deaths.

    Adjusts deaths and therefore cause fractions at the subnational unit
    to take into account the ratio of subnational to national deaths
    """

    def __init__(self, df, source, double=False):
        """Initialize the object with some info on columns we need."""
        self.df = df
        self.source = source
        self.double = double
        self.merge_cols = ['sex_id', 'age_group_id',
                           'cause_id', 'year_id', 'iso3']
        self.cf_cols = ['cf_final']
        self.draw_cols = [x for x in self.df.columns if 'cf_draw_' in x]
        if len(self.draw_cols) > 0:
            self.cf_cols = self.draw_cols + ['cf_final']
        self.death_cols = ['deaths' + x.split('cf')[1] for x in self.cf_cols]
        self.death_prop_cols = [(x + '_prop') for x in self.death_cols]

    def get_computed_dataframe(self, location_hierarchy):
        # get raked data
        if self.double:
            df = self.double_rake(self.df, location_hierarchy)
        else:
            df = self.standard_rake(self.df, location_hierarchy)
        df.drop('is_nat', axis=1, inplace=True)
        return df

    def standard_rake(self, df, location_hierarchy):
        # grab the length of the incoming dataframe
        start = len(df)

        # prep dataframe
        df = self.add_iso3(df, location_hierarchy)
        df = self.flag_aggregates(df, location_hierarchy)

        # if there are no subnational locations, why rake?
        if 0 in df['is_nat'].unique():

            # create 'deaths' columns for easy aggregating
            df = self.make_deaths(df)

            aggregate_df = self.prep_aggregate_df(df)
            subnational_df = self.prep_subnational_df(df)
            sub_and_agg = subnational_df.merge(
                aggregate_df, on=self.merge_cols, how='left'
            )
            for death_col in self.death_cols:
                sub_and_agg.loc[
                    sub_and_agg['{}_agg'.format(death_col)].isnull(),
                    '{}_agg'.format(death_col)
                ] = sub_and_agg['{}_sub'.format(death_col)]
            sub_and_agg.loc[
                sub_and_agg['sample_size_agg'].isnull(), 'sample_size_agg'
            ] = sub_and_agg['sample_size_sub']
            df = df.merge(sub_and_agg, how='left', on=self.merge_cols)

            # do not want to add or drop observations
            end = len(df)
            assert start == end, "The number of rows have changed,"\
                                 " this really shouldn't happen."
            df = self.replace_metrics(df)
            df = self.cleanup(df)
        return df

    def cleanup(self, df):
        """Drop unnecessary columns."""
        sub_cols = [x for x in df.columns if 'sub' in x]
        agg_cols = [x for x in df.columns if 'agg' in x]
        prop_cols = [x for x in df.columns if 'prop' in x]
        df = df.drop(sub_cols + agg_cols + prop_cols +
                     self.death_cols, axis=1)
        return df

    def add_iso3(self, df, location_hierarchy):
        """Add iso3 to incoming dataframe."""
        df = add_location_metadata(df, 'ihme_loc_id',
                                   location_meta_df=location_hierarchy)
        df['iso3'] = df['ihme_loc_id'].str[0:3]
        df.drop(['ihme_loc_id'], axis=1, inplace=True)
        return df

    def prep_subnational_df(self, df):
        """Prep sub national dataframe.

        Set a temporary non-zero deaths floor (needed for division later)
        and create subnational sample_size and deaths columns.
        """
        df = df[df['is_nat'] == 0]
        sub_total = df.groupby(
            self.merge_cols, as_index=False
        )[self.death_cols + ['sample_size']].sum()

        # create _sub columns
        for death_col in self.death_cols:
            sub_total.loc[sub_total[death_col] == 0, death_col] = .0001
            sub_total.rename(
                columns={death_col: death_col + '_sub'}, inplace=True
            )
        sub_total.rename(columns={'sample_size': 'sample_size_sub'}, inplace=True)

        sub_total = sub_total[
            self.merge_cols + [x for x in sub_total.columns if 'sub' in x]
        ]

        return sub_total

    def flag_aggregates(self, df, location_hierarchy):
        """Flag if the location_id is a subnational unit or not."""
        country_locations = get_country_level_location_id(
            df.location_id.unique(), location_hierarchy)
        df = df.merge(country_locations, how='left', on='location_id')
        df.loc[df['location_id'] == df['country_location_id'], 'is_nat'] = 1
        df.loc[df['location_id'] != df['country_location_id'], 'is_nat'] = 0
        df = df.drop('country_location_id', axis=1)
        return df

    def fix_inf_death_props(self, row):
        """When deaths_sub col is very very low, deaths_prop col can become infinity. Currently
        this is only an issue for PHL subnationals (12/11/2018). The solution is to set
        the infinity proportion to the max of the non-inf draws."""
        prop_values = row[self.death_prop_cols].values.tolist()
        if np.inf in prop_values:
            maxfill = max(p for p in prop_values if p != np.inf)
            prop_values = [p if p != np.inf else maxfill for p in prop_values]
            row[self.death_prop_cols] = prop_values

        return row

    def replace_metrics(self, df):
        """Adjust deaths based on national: subnational deaths ratio."""
        if self.source == "Other_Maternal":
            df['prop_ss'] = df['sample_size_agg'] / df['sample_size_sub']
            df.loc[
                df['is_nat'] == 0, 'sample_size'
            ] = df['sample_size'] * df['prop_ss']

        for death_col in self.death_cols:
            # change deaths
            df['{}_prop'.format(death_col)] = \
                df['{}_agg'.format(death_col)] / df['{}_sub'.format(death_col)]

        # PHL special case
        if df['iso3'].tolist()[0] == 'PHL':
            df = df.apply(self.fix_inf_death_props, axis=1)

        for death_col in self.death_cols:
            df.loc[df['is_nat'] == 0, death_col] = \
                df[death_col] * df['{}_prop'.format(death_col)]

            # change cause fractions
            cf_col = 'cf' + death_col.split('deaths')[1]
            df.loc[df['is_nat'] == 0, cf_col] = df[death_col] / df['sample_size']
            df.loc[df[cf_col] > 1, cf_col] = 1

        return df

    def prep_aggregate_df(self, df):
        """Sum deaths at the national level."""
        df = df[df['is_nat'] == 1]

        for death_col in self.death_cols:
            df = df.rename(columns={death_col: death_col + '_agg'})

        # sample_size aggregates are only used for Other_Maternal source
        df = df.rename(columns={'sample_size': 'sample_size_agg'})

        df = df[
            self.merge_cols + [x for x in df.columns if '_agg' in x]
        ]
        df = df.groupby(self.merge_cols, as_index=False).sum()

        return df

    def make_deaths(self, df):
        """Create deaths from cause fractions."""
        for cf_col in self.cf_cols:
            df['deaths' + cf_col.split('cf')[1]] = df[cf_col] * df['sample_size']
        return df

    def rake_detail_to_intermediate(self, df, location_hierarchy, intermediate_locs):
        """Raking the detailed locations to their non-national parent. Have to do this
        individually by each intermediate location.
        """
        # add parent_id to the data
        df = add_location_metadata(df, ['parent_id'],
            location_meta_df=location_hierarchy
        )
        # loop through each intermediate loc and rake
        dfs = []
        for loc in intermediate_locs:
            temp = df.loc[(df.parent_id == loc) |
                        (df.location_id == loc)
            ]
            # treating the intermediate location as a national to align with
            # the logic that identifies aggregates in the raker
            temp.loc[temp.location_id == loc, 'location_id'] = temp['parent_id']
            temp.drop('parent_id', axis=1, inplace=True)
            temp = self.standard_rake(temp, location_hierarchy)
            dfs.append(temp)
        df = pd.concat(dfs, ignore_index=True)
        # subset to just the detail locs, we'll add the intermediate and national on later
        df = df.loc[df.is_nat == 0]
        return df

    def double_rake(self, df, location_hierarchy):
        """Method built to rake twice when one location level exists between
        most detailed and the national level. Perhaps in time this can be expanded
        to function for datasets where multiple locations levels exists between
        most detailed and the national level (GBR only?).
        """
        start = len(df)
        # flag location levels
        df = add_location_metadata(df, ['level'],
            location_meta_df=location_hierarchy
        )
        report_if_merge_fail(df, 'level', 'location_id')

        # isolate the different location levels in the dataframe, assumption that levels
        # in the data are 3(natl), 4(intermediate/state), and 5(most detailed)
        national = (df['level'] == 3)
        intermediate = (df['level'] == 4)
        detail = (df['level'] == 5)
        # some checks:
        assert len(df[detail]) > 0, "Double raking assumes there is subnational data in the" \
            " dataframe, however no subnational detail is present"
        assert len(df[intermediate]) > 0, "Attempting to double rake, but there are no" \
            " intermediate locations in the data to rake to first"
        intermediate_locs = df[intermediate].location_id.unique().tolist()
        df.drop('level', axis=1, inplace=True)

        # rake the detailed locs to their parent first (first rake of double raking)
        non_national = df.loc[detail | intermediate]
        single_raked_detail_locs = self.rake_detail_to_intermediate(non_national,
            location_hierarchy, intermediate_locs
        )
        # now take the subnationals that have been raked to the intermediate locations
        # and rake them to the national (the second rake of double raking)
        detail_and_national = pd.concat([single_raked_detail_locs, df.loc[national]],
            ignore_index=True
        )
        detail_df = self.standard_rake(detail_and_national, location_hierarchy)
        detail_df = detail_df.loc[detail_df.is_nat == 0]

        # rake the intermediate level to the national
        intermediate_and_national = df.loc[intermediate | national]
        intermediate_and_national = self.standard_rake(intermediate_and_national, location_hierarchy)

        # concat the national, single raked intermediate locs, and double raked detail locs
        df = pd.concat([detail_df, intermediate_and_national], ignore_index=True)
        assert start == len(df), "The number of rows have changed,"\
                             " this really shouldn't happen."
        return df

class AnemiaAdjuster(CodProcess):
    """Reduce anemia levels in verbal autopsy data.

    Iron-deficiency anemia is believed to be over-reported in
    Verbal Autopsy.

    This adjustment takes any instance of iron-deficiency anemia and
    redistributes it onto a set of pre-defined proportions onto a set
    of target causes. Should not change the sum total cause fractions.

    It is ok that anemia is left in the data after this method, it will
    just be less.

    This method is most likely to break due to the proportions not
    matching up with the data, perhaps because they are only extended
    to 2017, or perhaps they have a target cause that is no longer
    in the hierarchy.
    """

    cf_cols = ['cf', 'cf_raw', 'cf_corr', 'cf_rd']
    anemia_cause_id = 390

    def __init__(self):
        self.anemia_props_path = CONF.get_resource('va_anemia_proportions')
        self.location_set_version_id = CONF.get_id('location_set_version')

    def get_computed_dataframe(self, df):
        original_columns = list(df.columns)

        orig_deaths_sum = (df['cf'] * df['sample_size']).sum()

        # split the anemia in the data onto a set of target causes with
        # proportions that add to 1
        anemia_props = pd.read_csv(self.anemia_props_path)
        anemia_df = df.loc[df['cause_id'] == self.anemia_cause_id]
        anemia_df = add_location_metadata(
            anemia_df, 'ihme_loc_id',
            location_set_version_id=self.location_set_version_id,
            force_rerun=False
        )
        anemia_df['iso3'] = anemia_df['ihme_loc_id'].str.slice(0, 3)
        unique_iso3s = list(anemia_df['iso3'].unique())
        merge_props = anemia_props.loc[
            anemia_props['iso3'].isin(unique_iso3s)
        ]
        unique_years = list(anemia_df.year_id.unique())
        years_under_90 = [u for u in unique_years if u < 1990]
        if len(years_under_90) > 0:
            props_90 = merge_props.query('year_id == 1990')
            for copy_year in years_under_90:
                copy_props = props_90.copy()
                copy_props['year_id'] = copy_year
                merge_props = merge_props.append(
                    copy_props, ignore_index=True)
        anemia_df = anemia_df.merge(
            merge_props,
            on=['iso3', 'year_id', 'age_group_id', 'sex_id', 'cause_id'],
            how='left'
        )
        # use the unchanged anemia df as diag df
        self.diag_df = anemia_df

        sum_to_one_id_cols = list(set(original_columns) - set(self.cf_cols))
        assert np.allclose(
            anemia_df.groupby(
                sum_to_one_id_cols
            )['anemia_prop'].sum(),
            1
        )

        anemia_df['cause_id'] = anemia_df['target_cause_id']
        for cf_col in self.cf_cols:
            anemia_df[cf_col] = anemia_df[cf_col] * anemia_df['anemia_prop']

        # remove extra columns used in this adjustment
        anemia_df = anemia_df[original_columns]

        # replace the anemia data in the incoming df with the newly split
        # anemia data
        df = df.loc[df['cause_id'] != self.anemia_cause_id]
        df = df.append(anemia_df, ignore_index=True)

        # collapse together potential duplicate location-year-age-sex-causes
        # introduced from splitting anemia onto targets
        sum_cols = self.cf_cols
        group_cols = list(set(df.columns) - set(sum_cols))
        df = df.groupby(group_cols, as_index=False)[sum_cols].sum()

        new_deaths_sum = (df['cf'] * df['sample_size']).sum()

        assert np.allclose(orig_deaths_sum, new_deaths_sum)

        return df

    def get_diagnostic_dataframe(self):
        if self.diag_df is not None:
            return self.diag_df
        else:
            print("Run get computed dataframe first")
