import os

import pandas as pd
import numpy as np

from cod_prep.claude.configurator import Configurator
from cod_prep.claude.cod_process import CodProcess
from cod_prep.downloaders import (
    add_location_metadata, get_current_location_hierarchy,
    get_env, get_country_level_location_id,
    get_cod_ages
)
from cod_prep.utils import (
    report_if_merge_fail, print_log_message, cod_timestamp
)
from cod_prep.claude.claude_io import get_claude_data
pd.options.mode.chained_assignment = None


class CalculateSourceMetadata(CodProcess):
    """Create various indicators for nid/extract_type_id pairs.

    Indicators include: representativeness, envelope coverage,
    percent garbage deaths, completeness, and majority child deaths
    """

    def __init__(self):
        self.cg = Configurator('standard')
        self.cache_dir = self.cg.get_directory('db_cache')
        self.test = False
        self.cache_options = {
            'force_rerun': True,
            'block_rerun': False,
            'cache_dir': self.cache_dir
        }
        self.dataset_filters = {
            'data_type_id': [8, 9, 10, 12],
            'location_set_id': 35,
            'is_active': True,
            'year_id': list(range(1980, 2050))
        }
        self.national_nids = self.cg.get_resource("nid_replacements")

        # resources
        self.completeness = self.cg.get_resource("completeness")
        self.env_meta_df = get_env(
            env_run_id=self.cg.get_id('env_run'),
            **self.cache_options
        )
        self.location_meta_df = get_current_location_hierarchy(
            location_set_version_id=self.cg.get_id('location_set_version'),
            **self.cache_options
        )
        self.cod_age_meta_df = get_cod_ages(**self.cache_options)
        self.cod_ages = list(self.cod_age_meta_df['age_group_id'].unique())

        # identifiers
        self.source_cols = ["source", "nid", "data_type_id"]
        self.geo_cols = ["location_id", "year_id"]
        self.meta_cols = ["nationally_representative", "detail_level_id"]
        self.value_cols = ['deaths']
        self.year_end = self.cg.get_id('year_end')
        self.full_time_series = "full_time_series"

        # drop dataset criteria
        self.gbg_threshold = 0.5
        self.comp_threshold = 0.5
        self.mccd_comp_threshold = 0.2

        # directories
        self.current_best_version = "FILEPATH"
        self.out_dir = "FILEPATH"
        self.arch_dir = "FILEPATH"
        self.timestamp = cod_timestamp()

    def create_sourcemetadata_outputs(self):
        """Return computations."""
        print_log_message("Compiling VA and VR source metadata")
        df = self.get_va_vr_sourcemetadata()

        print_log_message("Aggregating to country level")
        df = self.append_national_aggregates(df)

        print_log_message("Creating child study indicators")
        df_child = self.create_child_deaths_indicator(df)

        print_log_message("Calculating envelope coverage")
        df_cov = self.calculate_env_coverage(df)

        print_log_message("Calculating percent garbage")
        df_gc = self.calculate_garbage_percentage(df)

        # merge on child indicator
        if 'child5' in df.columns:
            df = df.drop(['child5', 'child15'], axis=1)
        df = df.merge(df_child, on=self.source_cols, how='left')

        # make sure there isn't conflicting metadata by source-location-year
        meta_cols = ["child5", "child15", "nationally_representative"]
        df = df.groupby(
            self.source_cols + self.geo_cols + meta_cols, as_index=False
        )['deaths'].sum()
        assert not df[self.source_cols + self.geo_cols].duplicated().any()
        df = df.drop('deaths', axis=1)

        merge_cols = self.source_cols + self.geo_cols

        # merge on envelope coverage
        df = df.merge(df_cov, on=merge_cols, how='left')
        report_if_merge_fail(df, 'pct_env_coverage', merge_cols)

        # merge on garbage percentage
        df = df.merge(df_gc, on=merge_cols, how='left')
        report_if_merge_fail(df, 'pctgarbage', merge_cols)

        # merge on completeness for VR
        print_log_message("Getting completeness")
        df_comp = self.get_completeness()
        # completeness merge is special because only VR gets completeness
        df = df.merge(df_comp, on=['location_id', 'year_id'], how='left')
        report_if_merge_fail(
            df[~(df['data_type_id'].isin([8, 12]))],
            'comp',
            ['location_id', 'year_id', 'data_type_id']
        )
        df.loc[df['data_type_id'].isin([8, 12]), 'comp'] = 1
        report_if_merge_fail(df, 'comp', ['location_id', 'year_id'])

        if len(df.query('year_id == 1986 & location_id == 140 & source == "ICD9_BTL"')) == 0:
            df = self.fix_bahrain_1986(df)

        vr_df = self.prep_vr_indicators(df)
        self.export_csv("FILEPATH", vr_df)

        print_log_message("Calculating percent well certified by time window")
        df = self.calculate_pct_certified_by_timewindow(df)

        # write all indicators for all vr / va source location years
        self.export_csv("FILEPATH", df)

        stars_df = self.prep_stars(df)
        self.export_csv("FILEPATH", stars_df)

        hierarchy_df = self.prep_star_level_location_hierarchy(stars_df)
        self.export_csv("FILEPATH", hierarchy_df)

        # output files used by various modeling teams based on stars
        print_log_message("Writing stars outputs used by modeling teams")
        self.prep_team_specific_stars_outputs(stars_df)

        print_log_message("Comparing star versions")
        self.make_stars_changelog()

        print_log_message("Done!")

        return df

    def get_va_vr_sourcemetadata(self):
        df = get_claude_data(phase="sourcemetadata", **self.dataset_filters)

        df = df.query(
            '~((location_id == 433 | location_id == 434) & year_id == 1980)'
        )

        # get rid of other maternal VR
        df = df.query('~(source == "Other_Maternal" & location_id == 38)')

        # collapse to remove extract type id
        group_cols = self.geo_cols + self.source_cols + \
            ['age_group_id', 'sex_id'] + self.meta_cols
        df = df.groupby(group_cols, as_index=False)[self.value_cols].sum()

        return df

    def append_national_aggregates(self, df):
        """Aggregate subnationals to national and append."""
        country_ids = get_country_level_location_id(
            df.location_id.unique(), self.location_meta_df
        ).set_index('location_id')['country_location_id'].to_dict()
        df["country_loc_id"] = df["location_id"].map(country_ids)
        report_if_merge_fail(df, 'country_loc_id', 'location_id')

        nat_vr_df = df.query(
            'country_loc_id != location_id & '
            '(data_type_id == 9 | data_type_id == 10)')
        nat_vr_df = self.aggregate_national_vr(nat_vr_df)

        nat_va_df = df.query(
            'country_loc_id != location_id & data_type_id in [8, 12]'
        )
        nat_va_df = self.aggregate_national_va(nat_va_df)

        df = pd.concat([nat_va_df, nat_vr_df, df], ignore_index=True)

        df = df.drop('country_loc_id', axis=1)

        return df

    def aggregate_national_vr(self, df):
        """Dedup VR sources and collapse to country year."""
        # make sure this is VR
        assert (df['data_type_id'].isin([9, 10])).all()
        # make sure this is subnational
        assert (df['country_loc_id'] != df['location_id']).values.all(), \
            "Found national data"

        # England (extract from main df and apppend to not conflict with GBR)
        eng_sources = ['England_UTLA_ICD9', 'England_UTLA_ICD10']
        eng_df = df.loc[df['source'].isin(eng_sources)]
        eng_df['country_loc_id'] = 4749
        eng_df['source'] = "ENG_aggregated_sources"
        # no need to change nids for england as they don't vary by utla
        df = df.append(eng_df, ignore_index=True)

        gbr_sources = ['ICD9_detail', 'UK_1981_2000', 'England_UTLA_ICD9',
                       'ICD10', 'UK_2001_2011', 'England_UTLA_ICD10',
                       'Scotland_VitalStats_ICD10']
        assert set(
            df.query('country_loc_id==95').source.unique()
        ) == set(gbr_sources)
        df.loc[df['country_loc_id'] == 95, 'source'] = "GBR_aggregated_sources"

        not_mainland_china = (
            df['country_loc_id'] == 6) & ~(
            df['source'].isin(['China_1991_2002', 'China_2004_2012',
                               'China_DSP_prov_ICD10'])
        )
        df = df[~not_mainland_china]

        df.loc[
            df['source'].str.startswith("India_MCCD"), 'source'
        ] = "India_MCCD"
        ind_df = df.loc[df['source'] == "India_MCCD"]
        ind_df = ind_df.merge(
            self.location_meta_df[['location_id', 'parent_id']],
            on='location_id', how='left'
        )
        ind_df['country_loc_id'] = ind_df['parent_id']
        ind_df = ind_df.drop('parent_id', axis=1)
        df = pd.concat([df, ind_df], ignore_index=True)

        df.loc[
            (df['country_loc_id'] == 63) &
            (df['year_id'].isin([2015, 2016])), 'source'
        ] = 'UKR_MoH_ICD10_tab'

        id_cols = ['data_type_id', 'country_loc_id', 'location_id',
                   'year_id', 'detail_level_id', 'age_group_id', 'sex_id']
        dups = df[df[id_cols].duplicated()]
        if len(dups) > 0:
            raise AssertionError("Duplicated: \n{}".format(dups))

        # Replace nid with aggregate for all subnational VR
        nid_dict = pd.read_csv(
            self.national_nids
        ).set_index('match_location_id')['NID'].to_dict()
        df['new_nid'] = df['country_loc_id'].map(nid_dict)
        df.loc[df['country_loc_id'] == 4749, 'new_nid'] = df['nid']
        report_if_merge_fail(df, 'new_nid', 'country_loc_id')
        df['nid'] = df['new_nid']

        # Keep all the columns except location id
        df = df.groupby(
            self.meta_cols + self.source_cols +
            ["year_id", "country_loc_id", "age_group_id", "sex_id"],
            as_index=False
        )[self.value_cols].sum()

        # check for duplicates
        id_cols.remove('location_id')
        dups = df[df[id_cols].duplicated()]
        if len(dups) > 0:
            raise AssertionError("Duplicated: \n{}".format(dups))

        # now the country location id is the location id
        df = df.rename(columns={'country_loc_id': 'location_id'})

        return df

    def aggregate_national_va(self, df):
        """Simpler method to aggregate VA to national."""
        # make sure this is VA
        assert (df['data_type_id'].isin([8, 12])).all()

        # make sure this is subnational
        assert (df['country_loc_id'] != df['location_id']).values.all(), \
            "Found national data"

        df = df.groupby(
            self.meta_cols + self.source_cols +
            ["year_id", "country_loc_id", "age_group_id", "sex_id"],
            as_index=False
        )[self.value_cols].sum()

        # for VA there can be many sources in one loc aggregate
        id_cols = ['country_loc_id', 'year_id', 'nid',
                   'source', 'detail_level_id', 'age_group_id', 'sex_id']
        dups = df[df[id_cols].duplicated()]
        if len(dups) > 0:
            raise AssertionError("Duplicated: \n{}".format(dups))

        # now the country location id is the location id
        df = df.rename(columns={'country_loc_id': 'location_id'})

        return df

    def create_child_deaths_indicator(self, df):
        """Collapse age after creating two age bins."""
        under5ages = self.cod_age_meta_df.query(
            "age_group_years_end <= 5")['age_group_id'].unique().tolist()
        under15ages = self.cod_age_meta_df.query(
            "age_group_years_end <= 15")['age_group_id'].unique().tolist()
        # flag deaths under 5
        df['under5deaths'] = 1 * \
            df['age_group_id'].isin(under5ages) * df['deaths']
        # flag deaths under 15
        df['under15deaths'] = 1 * \
            df['age_group_id'].isin(under15ages) * df['deaths']

        df = df.groupby(
            self.source_cols, as_index=False
        )['under5deaths', 'under15deaths', 'deaths'].sum()

        # create column to indicate if majority of deaths are from children
        df['child5'] = 1 * ((df['under5deaths'] / df['deaths']) >= .95)
        df['child15'] = 1 * ((df['under15deaths'] / df['deaths']) >= .95)

        df = df.drop(["under5deaths", "under15deaths", "deaths"], axis=1)
        return df

    def get_age_weight_df(self):
        """
        We have shifted to pulling age weights based on mortality information
        """
        df = get_env(
            env_run_id=self.cg.get_id('env_run'),
            force_rerun=False,
            block_rerun=True
        )
        df = df.query("location_id == 1 & sex_id == 3 & year_id >= 2010")
        # collapse out year
        df = df.groupby(['age_group_id', 'location_id', 'sex_id'], as_index=False).mean_env.sum()
        # total deaths for weights
        total = df.loc[df.age_group_id == 22]['mean_env'].iloc[0]
        # get the ages we care about (cod ages, under 1, and 80+)
        age_df = get_cod_ages()
        ages = age_df.age_group_id.unique().tolist()
        ages += [21, 28]
        # limit env df to relevant ages
        df = df.loc[df.age_group_id.isin(ages)]
        # group by age, and then make weights
        df = df.groupby('age_group_id', as_index=False).mean_env.sum()
        df['weight'] = df['mean_env'] / total
        # some renaming
        df.rename(columns={'weight': 'age_group_weight_value'}, inplace=True)

        check_val = abs((df.loc[~df.age_group_id.isin([21, 28])].mean_env.sum() / total) - 1)
        assert check_val < 0.01

        df = df[['age_group_id', 'age_group_weight_value']]
        return df

    def get_age_standardized_percents(self, df):
        df = df.groupby(self.source_cols + self.geo_cols + ['age_group_id'],
                        as_index=False)['g12_deaths', 'agr_deaths', 'deaths'].sum()

        age_weight_df = self.get_age_weight_df()
        age_weight_dict = age_weight_df.drop_duplicates(
            ['age_group_id', 'age_group_weight_value']
        ).set_index('age_group_id')['age_group_weight_value'].to_dict()

        df['weight'] = df['age_group_id'].map(age_weight_dict)
        report_if_merge_fail(df, 'weight', 'age_group_id')

        df['pctgarbage'] = (df['g12_deaths'] / df['deaths']) * df['weight']
        df['pctagr'] = (df['agr_deaths'] / df['deaths']) * df['weight']

        df.loc[(df.deaths == 0) & (df.pctgarbage.isnull()), 'pctgarbage'] = 0
        df.loc[(df.deaths == 0) & (df.pctagr.isnull()), 'pctagr'] = 0
        assert df.notnull().values.all()

        df = df.groupby(self.source_cols + self.geo_cols,
                        as_index=False)['pctgarbage', 'pctagr'].sum()
        return df

    def calculate_garbage_percentage(self, df):
        """Calculate percent garbage by detail level."""
        assert 'detail_level_id' in df.columns
        df['g12_deaths'] = 0
        df.loc[
            df['detail_level_id'].isin([11, 12]), 'g12_deaths'
        ] = df['deaths']
        df['agr_deaths'] = 0
        df.loc[
            df['detail_level_id'].isin([21, 22]), 'agr_deaths'
        ] = df['deaths']

        df = self.get_age_standardized_percents(df)

        df = df[self.source_cols + self.geo_cols +
                ['pctgarbage', 'pctagr']]
        return df

    def calculate_env_coverage(self, df):
        """Calculate the percentage of envelope covered."""
        # demographic variables
        dem_cols = self.geo_cols + ['age_group_id', 'sex_id']

        # prep envelope
        env_df = self.env_meta_df.loc[
            (self.env_meta_df['age_group_id'].isin(self.cod_ages)) &
            (self.env_meta_df['sex_id'].isin([1, 2]))
        ]
        env_df = env_df[dem_cols + ['mean_env']]
        env_df['total_env'] = env_df.groupby(
            self.geo_cols
        )['mean_env'].transform(sum)

        # only keep demographics represented in the data
        df = df[
            self.source_cols + dem_cols
        ]
        df = df.drop_duplicates()

        # merge on envelope df
        df = df.merge(env_df, on=dem_cols, how='left')
        report_if_merge_fail(df, 'mean_env', dem_cols)

        df['env_covered'] = df.groupby(
            self.source_cols + self.geo_cols
        )['mean_env'].transform(sum)
        assert not ((df['env_covered'] - df['total_env']) > .0001).any()

        df['pct_env_coverage'] = df['env_covered'] / df['total_env']
        # all VR should be 1
        df.loc[df['data_type_id'].isin([9, 10]), 'pct_env_coverage'] = 1

        df = df.drop(['env_covered', 'total_env'], axis=1)
        df = df[
            self.source_cols + self.geo_cols + ['pct_env_coverage']
        ].drop_duplicates()
        assert not df.duplicated(self.source_cols + self.geo_cols).values.any()

        return df

    def get_completeness(self):
        """Retrieve VR completeness.
        """
        df = pd.read_csv(self.completeness)
        df = add_location_metadata(df, 'ihme_loc_id',
                                   location_meta_df=self.location_meta_df)

        # the completeness csv has duplicates for some location/years within
        # mainland China
        is_mainland_china = (df['ihme_loc_id'].str.startswith("CHN")) & ~(
            df['ihme_loc_id'].isin(["CHN_354", "CHN_361"]))
        is_sample = df['denominator'] == "sample"
        df = df.loc[(is_mainland_china & is_sample) |
                    (~is_mainland_china & ~is_sample)]

        assert not df[['ihme_loc_id', 'year_id']].duplicated().any()

        # set to 1 if it's over 1
        df['comp'] = df['comp'].apply(lambda x: 1 if x > 1 else x)

        df = df[['location_id', 'year_id', 'comp']]

        return df

    def calculate_percent_well_certified(self, df):
        """Calculate percent well certified."""
        df['va_adjustment'] = 1
        df['subnat_va_adjustment'] = 1

        df.loc[df['data_type_id'] == 8, 'va_adjustment'] = .64
        df.loc[
            df['data_type_id'].isin([8, 12]), 'subnat_va_adjustment'
        ] = 1 - 0.9 * (df['nationally_representative'] == 0)

        base_equation = df['comp'] * (1 - df['pctgarbage'] - df['pctagr'])

        # All of these add to 1 for VR
        va_adj = df['va_adjustment'] * \
            df['subnat_va_adjustment'] * df['pct_env_coverage']
        assert np.allclose(va_adj[df['data_type_id'].isin([9, 10])], 1), \
            "Found values where va adjustment affects VR"

        df['pct_well_certified'] = base_equation * va_adj

        return df

    def assign_time_group(self, year):
        """Mapping from year to time window."""
        if year >= 2015:
            year_start = 2010
        else:
            year_start = 5 * (int(year) // 5)
        if year_start == 2010:
            year_end = self.year_end
        else:
            year_end = year_start + 4
        return "{}_{}".format(year_start, year_end)

    def assign_stars(self, x):
        """Mapping from percent well certified to stars."""
        if x >= .85:
            return 5
        elif x >= .65:
            return 4
        elif x >= .35:
            return 3
        elif x >= .10:
            return 2
        elif x > 0:
            return 1
        else:
            return 0

    def calculate_pct_certified_by_timewindow(self, df, method='max_all'):
        """Calculate percent by timewindow.

        Arguments:
            df: pandas DataFrame, the input data of percent well certified
            method: str, either 'mean_vr' or 'max_all'

        Returns:
            df: pandas DataFrame with
                [keys] : [values]
                [location_id, time_window] : [percent well certified]
        """
        df = self.calculate_percent_well_certified(df)

        assert method in ['mean_vr', 'max_all']
        assert 'pct_well_certified' in df.columns

        # add all location years to the dataframe, even if there is no data
        locs = self.location_meta_df.query(
            'level == 3 | is_estimate == 1 | location_id == 4749'
        )[['location_id']]
        locs['dummy'] = 1
        years = pd.DataFrame(list(range(1980, self.year_end + 1)), columns=['year_id'])
        years['dummy'] = 1
        template = years.merge(locs, how='outer', on='dummy')
        template = template[['location_id', 'year_id']]
        df = template.merge(df, how='left')
        df['pct_well_certified'] = df['pct_well_certified'].fillna(0)

        # Fill missings so that groupbys won't drop data that I want
        # fill integer columns with int
        int_cols = df.select_dtypes(include=['number'])
        str_cols = list(set(df.columns) - set(int_cols))
        for ic in int_cols:
            df[ic] = df[ic].fillna(-1)
        for sc in str_cols:
            df[sc] = df[sc].fillna('')

        # add time window
        years = list(set(df['year_id']))
        year_group_map = dict()
        for year in years:
            year_group_map[year] = self.assign_time_group(year)
        df['time_window'] = df['year_id'].map(year_group_map)

        if method == 'mean_vr':
            # calculate the mean vr pct in the time window
            vr_avg = df.query('data_type_id == 9 | data_type_id == 10')
            merge_vars = ['location_id', 'year_id',
                          'time_window', 'data_type_id']
            vr_avg['avg_vr'] = vr_avg.groupby(['location_id', 'time_window'])[
                'pct_well_certified'].transform(np.mean)
            vr_avg = vr_avg[merge_vars + ['avg_vr']]

            # merge that back on to the data, and make the avg vr 0 for va
            df = df.merge(vr_avg, on=merge_vars, how='left')
            assert len(df.loc[(df['data_type_id'].isin([9, 10])) &
                              (df['avg_vr'].isnull())]) == 0
            df['avg_vr'] = df['avg_vr'].fillna(0)

            # combine so value is either VA unmodified or mean VR
            df['pct_well_certified_primed'] = (
                ((1 * (df['data_type_id'].isin([9, 10]))) * df['avg_vr']) +
                ((1 * (df['data_type_id'].isin([8, 12]))) * df['pct_well_certified'])
            )
            pwc_col = 'pct_well_certified_primed'

        elif method == 'max_all':
            pwc_col = 'pct_well_certified'

        df['time_window_pct_wc'] = df.groupby(['location_id', 'time_window'])[
            pwc_col].transform(np.max)

        assert not (df['time_window_pct_wc'] > 1).any()

        return df

    def apply_star_buffer(self, df):
        """Some locations with pwc values right on the edge of a star rating cutoff can switch
        star ratings everytime we run smp due to slight changes in demographics or pct garbage.
        Add a buffer to prevent this."""
        incoming_cols = df.columns.tolist()
        old_stars = pd.read_csv(self.cg.get_resource('stars_by_iso3'))
        old_stars = old_stars[['location_id', 'time_window', 'stars']]
        df = df.merge(old_stars, on=['location_id', 'time_window'], how='left', suffixes=('', '_old'))

        buffer_dict = {
            5: 0.82,
            4: 0.62,
            3: 0.32,
            2: 0.07,
            1: 0
        }
        df['buffer_pwc'] = df.stars_old.map(buffer_dict)

        # if something decreased star rating, did it stay in the pwc buffer window? if so preserve star rating
        # only consider where stars >0, if something is zero stars it implies we have no data
        decreased_stars = (df.stars_old > df.stars)
        greater_than_zero = (df.stars > 0)
        df.loc[
            decreased_stars &
            greater_than_zero &
            (df.time_window_pct_wc >= df.buffer_pwc),
            'stars'
        ] = df['stars_old']
        df = df[incoming_cols]
        assert df.notnull().values.all()
        return df

    def calculate_stars(self, df):
        """Collapse time window percentages to stars."""
        # keep only location-time window and how well that time window was
        # certified
        df = df[['location_id', 'time_window', 'time_window_pct_wc']]
        df = df.drop_duplicates()

        # should only me one measurement of time window pct well certified
        assert not df[['location_id', 'time_window']].duplicated().any()
        # make sure that there are an equal number of time windows in each location
        # this was ensured by squaring to all location-years in an earlier step
        assert len(set(df.groupby('location_id')['time_window'].count())) == 1

        # add full time series as the mean of the time interval pct well certified
        ftdf = df.groupby('location_id', as_index=False)[
            'time_window_pct_wc'].mean()
        ftdf['time_window'] = self.full_time_series
        df = df.append(ftdf, ignore_index=True)

        # star thresholds defined in another mehotd
        df['stars'] = df['time_window_pct_wc'].apply(
            lambda x: self.assign_stars(x)
        )

        # apply buffer for location/years that are on the edge of star ratings
        df = self.apply_star_buffer(df)

        return df

    def get_env_by_time_window(self):
        """Get an envelope by time window."""
        # keep 1980 onwards
        env = self.env_meta_df.query('year_id >= 1980')

        # add time window
        env['time_window'] = env['year_id'].apply(
            lambda x: self.assign_time_group(x)
        )

        env = env.groupby(
            ['location_id', 'time_window'], as_index=False
        )['mean_env'].sum()

        # restrict to level 3
        env = add_location_metadata(
            env, "level", location_meta_df=self.location_meta_df
        )
        env = env.query('level == 3')

        # keep the right columns
        env = env[['location_id', 'time_window', 'mean_env']]

        # append 1980 onwards envelope
        env_all = env.groupby(
            ['location_id'], as_index=False
        )['mean_env'].sum()
        env_all['time_window'] = self.full_time_series
        env = pd.concat([env_all, env], ignore_index=True)

        return env

    def measure_the_world(self, df):
        """Collapse location level star estimates to global level.

        Measures the proportion of global deaths that are well certified
        over time.
        """
        # keep only countries to make envelope merge straight forward
        df = df.query('location_level == 3')
        df = df[['location_id', 'time_window', 'time_window_pct_wc']]

        env = self.get_env_by_time_window()
        df = df.merge(env, on=['location_id', 'time_window'], how='outer')
        assert not df['mean_env'].isnull().any()
        assert not df['time_window_pct_wc'].isnull().any()

        df['num_wc'] = df['time_window_pct_wc'] * df['mean_env']
        df = df.groupby(
            ['time_window'], as_index=False
        )['mean_env', 'num_wc'].sum()
        df['time_window_pct_wc'] = df['num_wc'] / df['mean_env']

        # assign location_id
        df['location_id'] = 1

        # add stars
        df['stars'] = df['time_window_pct_wc'].apply(
            lambda x: self.assign_stars(x)
        )

        # restrict columns and add location metadata
        df.drop(['num_wc', 'mean_env'], axis=1, inplace=True)
        df['ihme_loc_id'] = 'G'
        df['location_level'] = 0
        df['location_name'] = 'Global'
        df['location_sort_order'] = 1.0
        df['parent_id'] = 1

        assert df.notnull().values.any()

        return df

    def fix_full_time_series_value(self, df):
        """Change the value of the 'time_window' column.
        """
        df.loc[
            df['time_window'].isin(['1980_2016', '1980_2017']), 'time_window'
        ] = self.full_time_series
        return df

    def make_stars_changelog(self):
        """Make the changelog to see what changed between two versions.

        Arguments:
        old_version, str: The old version to compare the new one to.

        Returns:
            Nothing, but writes its output to a csv in the smp folder
        """
        # get the new stuff and the old stuff
        new = pd.read_csv("FILEPATH")
        new = self.fix_full_time_series_value(new)
        old = pd.read_csv("FILEPATH")
        old = self.fix_full_time_series_value(old)

        # merge them
        merge_vars = ['location_id', 'time_window', 'location_name',
                      'ihme_loc_id', 'location_level', 'location_sort_order',
                      'parent_ihme_loc_id']
        df = new.merge(old, on=merge_vars, how='outer',
                       suffixes=('_new', '_old'), indicator=True)

        # cleanup output
        keep_cols = ['location_name', 'ihme_loc_id', 'time_window',
                     'stars_new', 'stars_old', 'time_window_pct_wc_new',
                     'time_window_pct_wc_old']

        df = df.query('stars_new != stars_old')[keep_cols]

        # write
        self.export_csv("FILEPATH", df)

    def apply_vr_drop_buffers(self, df):
        incoming_cols = df.columns.tolist()
        old_drops = pd.read_csv(self.cg.get_resource('vr_indicators'))
        old_drops = old_drops[['location_id', 'year_id', 'data_type_id', 'drop_status']]
        df = df.merge(old_drops, on=['location_id', 'year_id', 'data_type_id'], how='left', suffixes=('', '_old'))
        sub_rep = df['nationally_representative'] == 0
        mccd = df['source'].str.startswith("India_MCCD")

        # set buffers, currently allowing 5% for garbage and completeness, 2% for mccd
        df['comp_buffer'] = self.comp_threshold - 0.05
        df['garbage_buffer'] = self.gbg_threshold + 0.05
        df.loc[mccd, 'comp_buffer'] = self.mccd_comp_threshold - 0.02
        df.loc[sub_rep, 'comp_buffer'] = 0

        # if the location/year entered drop status, did it remain within the buffers? if so, don't drop
        newly_dropped = (df.drop_status > df.drop_status_old)
        df.loc[
            newly_dropped &
            (df.comp > df.comp_buffer) &
            (df.pctgarbage < df.garbage_buffer),
            'drop_status'
        ] = 0
        df = df[incoming_cols]
        assert df.notnull().values.all()
        return df



    def prep_vr_indicators(self, df):
        """Prep outputs for VR by location/year.
        """
        is_vr = df['data_type_id'].isin([9, 10])
        keep_cols = [
            'location_id', 'year_id', 'data_type_id', 'nid', 'source',
            'nationally_representative', 'pctgarbage', 'pctagr', 'comp'
        ]
        vr_df = df.loc[is_vr, keep_cols]
        assert not vr_df[
            ['location_id', 'year_id', 'data_type_id']
        ].duplicated().any()
        vr_df = add_location_metadata(
            vr_df, 'ihme_loc_id', location_meta_df=self.location_meta_df
        )

        vr_df['pct_well_cert'] = vr_df['comp'] * (1 - vr_df['pctgarbage'] - vr_df['pctagr'])

        # set variable logic for clarity
        sub_rep = vr_df['nationally_representative'] == 0
        nigeria = vr_df['source'] == "Nigeria_VR"
        mccd = vr_df['source'].str.startswith("India_MCCD")
        too_much_garbage = vr_df['pctgarbage'] > self.gbg_threshold
        too_low_comp = vr_df['comp'] < self.comp_threshold
        valid_mccd_comp = vr_df['comp'] > self.mccd_comp_threshold

        vr_df['drop_status'] = 0
        vr_df.loc[too_much_garbage | too_low_comp, 'drop_status'] = 1
        vr_df.loc[mccd & valid_mccd_comp & ~too_much_garbage, 'drop_status'] = 0
        vr_df.loc[sub_rep & ~too_much_garbage, 'drop_status'] = 0
        vr_df.loc[nigeria, 'drop_status'] = 1
        exception_nids = [16516, 16517, 375235]
        vr_df.loc[vr_df.nid.isin(exception_nids), 'drop_status'] = 0

        # apply vr drop buffers
        vr_df = self.apply_vr_drop_buffers(vr_df)

        comp_under_70 = (vr_df['comp'] < .7)
        vr_df['comp_under_70'] = 0
        vr_df.loc[comp_under_70, 'comp_under_70'] = 1

        return vr_df

    def fix_bahrain_1986(self, df):
        bahrain_1986 = pd.DataFrame(
            {'location_id': 140, 'year_id': 1986,
             'source': 'ICD9_BTL', 'data_type_id': 9,
             'child5': 0, 'child15': 0, 'pct_env_coverage': 1,
             'pctgarbage': 0.234717099, 'pctagr': 0,
             'nationally_representative': 1,
             'comp': 1, 'nid': 149128}, index=[0])
        assert set(bahrain_1986.columns) == set(df.columns)
        df = pd.concat([df, bahrain_1986], ignore_index=True)
        return df

    def prep_stars(self, df):
        """Calculate stars related indicators."""
        print_log_message("Calculating stars")
        stars_df = self.calculate_stars(df)

        # formatting pieces, conforming to previous versions
        stars_df = add_location_metadata(
            stars_df, ["ihme_loc_id", "level", "location_ascii_name",
                       "sort_order", "parent_id"],
            location_meta_df=self.location_meta_df
        )
        stars_df = stars_df.rename(
            columns={"sort_order": "location_sort_order",
                     "level": "location_level",
                     "location_ascii_name": "location_name"}
        )

        print_log_message("Calculating global stars, because why not")
        world_stars = self.measure_the_world(stars_df)
        stars_df = pd.concat([stars_df, world_stars], ignore_index=True)

        # add parent ihme_loc_id instead of parent_id
        ihme_loc_id_dict = self.location_meta_df.set_index(
            "location_id")["ihme_loc_id"].to_dict()
        stars_df["parent_ihme_loc_id"] = \
            stars_df["parent_id"].map(ihme_loc_id_dict)
        stars_df.drop("parent_id", axis=1, inplace=True)

        assert stars_df.notnull().values.any()

        return stars_df

    def prep_team_specific_stars_outputs(self, df):
        """Output files used by various modeling teams."""
        india = (df['ihme_loc_id'].str.startswith("IND"))
        stars_over_4 = (df['stars'] >= 4)
        full_time = (df['time_window'] == self.full_time_series)
        countries = (df['location_level'] == 3)
        not_global = (df['location_id'] != 1)
        keep_cols = ['location_id', 'ihme_loc_id', 'time_window', 'stars']

        # four+ star countries used in data rich spectrum and hiv tb models
        four_plus = df.loc[
            not_global & ~india & stars_over_4 & full_time, keep_cols
        ]
        self.export_csv("FILEPATH", four_plus)

    def prep_star_level_location_hierarchy(self, df):
        # add information for most detailed
        df = add_location_metadata(df, 'most_detailed')
        full_time = (df['time_window'] == self.full_time_series)
        most_detailed = (df['most_detailed'] == 1)

        # limit to most detailed locs and the full time series
        df = df.loc[
            full_time &
            most_detailed
        ]
        # add the parent_ids
        df.loc[df.stars.isin([4, 5]), 'parent_id'] = 1
        df.loc[df.stars.isin([0, 1, 2, 3]), 'parent_id'] = 0
        assert df.parent_id.notnull().all()

        df = df[['ihme_loc_id', 'location_id', 'parent_id', 'time_window', 'stars']]
        return df

    def export_csv(self, filename, df):
        """Export csvs to working and archive directories."""
        if not self.test:
            if not os.path.exists(self.arch_dir):
                os.mkdir(self.arch_dir)
            arch_filename = "{}_{}".format(filename, self.timestamp)
            df.to_csv(
                "{dir}/{file}.csv".format(dir=self.arch_dir, file=arch_filename),
                index=False, encoding='utf-8')
            df.to_csv("{dir}/{file}.csv".format(dir=self.out_dir, file=filename),
                      index=False, encoding='utf-8')


def main():
    """Calculate indicators and export various csvs."""
    calculate_sourcemetadata = CalculateSourceMetadata()
    calculate_sourcemetadata.create_sourcemetadata_outputs()


if __name__ == '__main__':
    main()
