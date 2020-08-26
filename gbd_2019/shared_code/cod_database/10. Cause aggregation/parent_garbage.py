from cod_process import CodProcess
import numpy as np

from cod_prep.downloaders import add_code_metadata

from cod_prep.utils import print_log_message, report_duplicates

from metric_functions import calc_sample_size, convert_to_cause_fractions

from aggregators import CauseAggregator


AGGREGATION_IDX_COLS = [
    'nid', 'extract_type_id', 'location_id', 'year_id',
    'age_group_id', 'sex_id', 'cause_id', 'site_id'
]


class ParentMappedAggregatedGarbageAdder(CodProcess):

    def __init__(self, nid, extract_type_id, source, cause_package_hierarchy,
                 cause_hierarchy, package_map, code_map, remove_decimal,
                 disagg_df, misdc_df
                 ):
        self.nid = nid
        self.extract_type_id = extract_type_id
        self.source = source
        self.cause_hierarchy = cause_hierarchy
        self.cause_package_hierarchy = cause_package_hierarchy
        self.package_map = package_map
        self.code_map = code_map
        self.remove_decimal = remove_decimal
        self.disagg_df = disagg_df
        self.misdc_df = misdc_df

    def get_computed_dataframe(self, df):

        orig_cols = list(df.columns)
        # this is a meaningless number, but it is so precise that
        # testing the same sum is equal after this process will
        # ensure with a lot of confidence that the individual values
        # did not change
        sum_orig_cf_col = df['cf'].sum()
        assert df['cf'].notnull().values.all()

        gbg_df = self.assemble_parentmapped_garbage_for_agg_df()

        df = ParentMappedAggregatedGarbageAdder.add_parentmapped_garbage_to_agg_df(
            df, gbg_df
        )

        self.diag_df = df.copy()

        df = df[orig_cols]

        assert np.allclose(df['cf'].sum(), sum_orig_cf_col)
        assert df['cf'].notnull().values.all()
        return df

    def get_diagnostic_dataframe(self):
        if self.diag_df is None:
            raise ValueError("Need to run get_computed_dataframe first")
        return self.diag_df.copy()

    def get_parentmapped_garbage(self, df):

        assert 'code_id' in df.columns, \
            "Need a code_id to map to packages, but columns " \
            "were: {}".format(df.columns)

        package_id_to_parent_id = self.cause_package_hierarchy.set_index(
            'package_id', verify_integrity=True
        )['parent_id']

        value_to_package_id = self.package_map.set_index(
            'value', verify_integrity=True)['package_id']

        df = add_code_metadata(df, 'value', code_map=self.code_map)
        if self.remove_decimal:
            df['value'] = df['value'].str.replace(".", "")

        df['package_id'] = df['value'].map(value_to_package_id)
        df['parent_id'] = df['package_id'].map(package_id_to_parent_id)

        # still need to know number of deaths in non-garbage in some applications, to
        # get the sample size for example
        df.loc[df['package_id'].isnull(), 'parent_id'] = -1
        # keep only
        df['cause_id'] = df['parent_id']
        df = df.groupby(AGGREGATION_IDX_COLS, as_index=False)['deaths'].sum()
        return df

    def assemble_parentmapped_garbage_for_agg_df(self):

        print_log_message("Getting parent-mapped garbage from disaggregation")
        df_raw = self.get_parentmapped_garbage(self.disagg_df)
        df_raw = df_raw.rename(columns={'deaths': 'deaths_raw'})

        print_log_message("Getting parent-mapped garbage from misdiagnosiscorrection")
        df_corr = self.get_parentmapped_garbage(self.misdc_df)
        df_corr = df_corr.rename(columns={'deaths': 'deaths_corr'})

        # merge on full intersection of columns, whatever that is
        # after renaming the value columns something specific to the
        # dataframes
        print_log_message("Merging parent-mapped garbage")
        df = df_raw.merge(df_corr, how='outer')

        # within 10 deaths of each other (goal of this assertion
        # is that this should not fail unless the above code is
        # flawed, not trying to find errors in the raw/corr phases)
        assert np.allclose(df['deaths_corr'].sum(), df['deaths_raw'].sum(), atol=10)

        # make a full dataframe with all stages, like we would
        # see in the aggregation phase
        df['deaths_raw'] = df['deaths_raw'].fillna(0)
        df['deaths_corr'] = df['deaths_corr'].fillna(0)
        df['deaths_rd'] = 0
        df['deaths'] = 0

        print_log_message("Converting to cause fractions and aggregating up cause hierarchy")
        df = calc_sample_size(df, calc_deaths_col='deaths_raw')
        # remove the non-garbage that was coded to -1 in
        # get_parentmapped_garbage
        df = df.query('cause_id != -1')
        df = convert_to_cause_fractions(df, deaths_cols=['deaths_raw', 'deaths_corr', 'deaths_rd', 'deaths'])

        cause_aggregator = CauseAggregator(df, self.cause_hierarchy, self.source)
        df = cause_aggregator.get_computed_dataframe()
        df = df[AGGREGATION_IDX_COLS + ['cf_raw', 'cf_corr']].copy()
        report_duplicates(df, AGGREGATION_IDX_COLS)
        return df

    @staticmethod
    def add_parentmapped_garbage_to_agg_df(agg_df, gbg_df):
        report_duplicates(agg_df, AGGREGATION_IDX_COLS)
        report_duplicates(gbg_df, AGGREGATION_IDX_COLS)
        gbg_df = gbg_df[AGGREGATION_IDX_COLS + ['cf_raw', 'cf_corr']]
        df = agg_df.merge(gbg_df, on=AGGREGATION_IDX_COLS, how='left', suffixes=('', '_gbgparnt'))
        df.loc[
            df['cf_raw_gbgparnt'] > 0,
            'cf_raw'
        ] = df['cf_raw'] + df['cf_raw_gbgparnt']
        df.loc[
            df['cf_corr_gbgparnt'] > 0,
            'cf_corr'
        ] = df['cf_corr'] + df['cf_corr_gbgparnt']
        return df
