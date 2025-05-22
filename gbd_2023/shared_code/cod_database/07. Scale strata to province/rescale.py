import pandas as pd
import numpy as np

from db_tools import ezfuncs

from cod_prep.utils import (
    report_if_merge_fail, expand_to_u5_age_detail
)
from cod_prep.claude.cod_process import CodProcess
from cod_prep.claude.configurator import Configurator

CONF = Configurator()


class ChinaHospitalUrbanicityRescaler(CodProcess):
    hospdead_available_year_start = 2008
    blank_site_id = 2
    missing_hospdead_id = 9
    in_out_hosp_prop_name = 'prop'
    # columns that have no place in calculations but shouldn't be lost
    extra_val_cols = ['deaths_raw', 'deaths_corr', 'deaths_rd']

    def __init__(self):
        pass

    def get_computed_dataframe(self, df):

        df['deaths_before_scaling'] = df['deaths']

        df = self.extract_strata_hospdead_from_site(df)

        props_df = self.get_in_out_hospital_proportions()
        df = self.add_in_out_hospital_proportions(df, props_df)

        df = self.collapse_to_hospital_weighted_deaths(df)

        df = self.scale_to_original_death_totals(df)

        self.diag_df = df.copy()

        df = self.collapse_and_clean(df)

        assert np.allclose(
            df['deaths'].sum(), df['deaths_before_scaling'].sum()), \
            "China scaling changed death totals and should not have"
        df['scale_pct_change'] = \
            abs((df['deaths'] - df['deaths_before_scaling'])) / df['deaths']
        if (df['year_id'] > self.hospdead_available_year_start).any():
            assert (
                df.loc[
                    df['year_id'] > self.hospdead_available_year_start,
                    'scale_pct_change'] > .01
            ).any(), "China scaling didn't change anything, and it should have"

        df = df.drop(['scale_pct_change', 'deaths_before_scaling'], axis=1)

        return df

    def get_diagnostic_dataframe(self):
        if self.diag_df is None:
            raise AssertionError("Run get_computed_dataframe first")
        else:
            return self.diag_df

    def get_in_out_hospital_proportions(self):
        props_df = pd.read_csv(CONF.get_resource('china_in_out_hospital_props'))
        props_df = expand_to_u5_age_detail(props_df)
        return props_df

    def extract_strata_hospdead_from_site(self, df):

        year_sites = df[['year_id', 'site_id']].drop_duplicates()

        sites = ezfuncs.query(
            "SELECT site_id, site_name FROM ADDRESS",
            conn_def='ADDRESS'
        )
        year_sites = year_sites.merge(sites, how='left')
        report_if_merge_fail(year_sites, 'site_name', 'site_id')

        base_site_pattern = "^Prov (?P<prov>[0-9]+) Strata (?P<strata>[1-2])"
        no_hosp_site_pattern = base_site_pattern + "$"
        with_hosp_site_pattern = base_site_pattern + \
            " Hosp (?P<hospdead>[0-1])$"

        no_hosp_year_sites = year_sites.loc[
            year_sites['year_id'] < self.hospdead_available_year_start]
        if len(no_hosp_year_sites) > 0:
            no_hosp_year_sites[['prov', 'strata']] = \
                no_hosp_year_sites['site_name'].str.extract(
                    no_hosp_site_pattern, expand=False)
            no_hosp_year_sites['hospdead'] = self.missing_hospdead_id

        hosp_year_sites = year_sites.loc[
            year_sites['year_id'] >= self.hospdead_available_year_start]
        if len(hosp_year_sites) > 0:
            hosp_year_sites[['prov', 'strata', 'hospdead']] = \
                hosp_year_sites['site_name'].str.extract(
                    with_hosp_site_pattern, expand=False)

        year_sites = no_hosp_year_sites.append(
            hosp_year_sites, ignore_index=True)

        # province is already in location_id, so don't need it
        year_sites = year_sites[['year_id', 'site_id', 'strata', 'hospdead']]
        year_sites['hospdead'] = year_sites['hospdead'].astype(int)
        year_sites['strata'] = year_sites['strata'].astype(int)

        df = df.merge(year_sites, on=['year_id', 'site_id'], how='left')
        report_if_merge_fail(df, 'hospdead', ['year_id', 'site_id'])
        report_if_merge_fail(df, 'strata', ['year_id', 'site_id'])

        # site can now be blank because we pulled relevant info out of it
        df['site_id'] = self.blank_site_id

        return df

    def add_in_out_hospital_proportions(self, df, props_df):

        df = df.merge(props_df, how='left')
        df.loc[
            df['hospdead'] == self.missing_hospdead_id,
            self.in_out_hosp_prop_name] = 1

        report_if_merge_fail(
            df,
            self.in_out_hosp_prop_name,
            ['location_id', 'age_group_id', 'sex_id', 'strata', 'hospdead']
        )
        return df

    def collapse_to_hospital_weighted_deaths(self, df):
        df['hosp_weighted_deaths'] = \
            df['deaths'] * df[self.in_out_hosp_prop_name]

        df['orig_sample_size'] = df.groupby(
            ['location_id', 'year_id', 'age_group_id', 'sex_id', 'strata']
        )['deaths'].transform(np.sum)

        val_cols = ['hosp_weighted_deaths', 'deaths_before_scaling'] + \
            self.extra_val_cols

        group_hosp_cols = list(
            set(df.columns) -
            set(val_cols + [self.in_out_hosp_prop_name, 'hospdead'])
        )
        df = df.groupby(group_hosp_cols, as_index=False)[val_cols].sum()

        return df

    def scale_to_original_death_totals(self, df):

        df['hosp_weighted_sample'] = df.groupby(
            ['location_id', 'year_id', 'age_group_id', 'sex_id', 'strata']
        )['hosp_weighted_deaths'].transform(np.sum)
        df['hosp_weighted_cf'] = \
            df['hosp_weighted_deaths'] / df['hosp_weighted_sample']
        df['deaths'] = df['hosp_weighted_cf'] * df['orig_sample_size']
        return df

    def collapse_and_clean(self, df):

        group_cols = [
            'nid', 'extract_type_id', 'location_id', 'year_id', 'age_group_id',
            'sex_id', 'site_id', 'cause_id'
        ]
        val_cols = ['deaths', 'deaths_before_scaling'] + self.extra_val_cols
        df = df.groupby(group_cols, as_index=False)[val_cols].sum()
        return df
