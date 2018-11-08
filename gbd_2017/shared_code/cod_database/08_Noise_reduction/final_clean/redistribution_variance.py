import os
import pandas as pd
import numpy as np
from scipy.special import logit, expit
import scipy.stats as st
import warnings

from cod_prep.utils import (
    report_if_merge_fail, report_duplicates, print_log_message
)
from cod_prep.downloaders import (
    get_all_related_causes, add_population,
    get_value_from_nid, get_package_targets,
    get_cause_package_map
)
from garviz_etl import assemble_garviz_upload
from configurator import Configurator
from claude_io import get_claude_data, delete_claude_output, write_phase_output
from cod_process import CodProcess
from cod_prep.claude.run_phase_misdiagnosiscorrection import (
    AVAILABLE_CODE_SYSTEMS as MISDC_CODE_SYSTEMS
)


CONF = Configurator('standard')
N_DRAWS = int(CONF.get_resource('uncertainty_draws'))

LOGIT_CF_VAR_COL = 'variance_rd_logit_cf'
LOG_DEATHRATE_VAR_COL = 'variance_rd_log_dr'
UPPER_RD_COL = 'cf_final_high_rd'
LOWER_RD_COL = 'cf_final_low_rd'
MEAN_RD_COL = 'cf_final'
RD_VAR_COL = 'rd_mad_capped'
MISDC_VAR_COL = 'deaths_variance'

MISDC_MERGE_COLS = [
    'nid', 'extract_type_id', 'location_id', 'year_id', 'age_group_id',
    'sex_id', 'cause_id'
]

MISDC_CAUSES = [500, 544, 543]


def dataset_has_redistribution_variance(data_type_id, source):
    return data_type_id in [9, 10] and source != "Other_Maternal"


def modelgroup_has_redistribution_variance(model_group):
    return model_group.startswith("VR-")


class RedistributionVarianceEstimator(CodProcess):

    draws = range(0, N_DRAWS)
    cf_draw_cols = ['cf_draw_{}'.format(draw) for draw in draws]

    def __init__(self, nid, extract_type_id, cause_meta_df, remove_decimal,
                 code_system_id, cause_map, package_map):
        self.cause_hierarchy = cause_meta_df
        self.remove_decimal = remove_decimal
        self.nid = nid
        self.extract_type_id = extract_type_id
        self.remove_decimal = remove_decimal
        self.code_system_id = code_system_id
        self.cause_map = cause_map
        self.package_map = package_map
        self.has_misdc = self.code_system_id in MISDC_CODE_SYSTEMS

    def get_computed_dataframe(self, df):

        orig_cols = list(df.columns)
        print_log_message("Starting")

        print_log_message("Reading data")
        agg_cause_ids = list(df['cause_id'].unique())

        print_log_message("Adding garbage envelope")
        garbage_deaths = self.get_redistribution_envelope(df, agg_cause_ids)
        if len(garbage_deaths) > 0:
            gbg_merge_cols = ['location_id', 'year_id',
                              'age_group_id', 'sex_id', 'site_id', 'cause_id']
            df = df.merge(garbage_deaths, how='left', on=gbg_merge_cols)
            report_if_merge_fail(df, 'garbage_targeting_cause', gbg_merge_cols)
            report_duplicates(df, gbg_merge_cols)
        else:
            df['garbage_targeting_cause'] = 0

        print_log_message("Adding residual variance")
        residual_variance = self.get_residual_variance()
        resid_merge_cols = ['cause_id', 'age_group_id', 'sex_id']
        df = df.merge(residual_variance, how='left', on=resid_merge_cols)

        df[RD_VAR_COL] = df[RD_VAR_COL]**2

        assert df[RD_VAR_COL].notnull().any()

        if self.has_misdc:

            print_log_message(
                "Getting dismod variance for midsiagnosis corrected causes"
            )
            misdc_variance = self.get_misdiagnosiscorrection_variance()
            df_misdc = df[df['cause_id'].isin(MISDC_CAUSES)]
            df_misdc = df_misdc.merge(
                misdc_variance, on=MISDC_MERGE_COLS, how='left'
            )

            df_misdc[RD_VAR_COL] = df_misdc[MISDC_VAR_COL]
            df_misdc = df_misdc.drop(MISDC_VAR_COL, axis=1)

            df = df.loc[~df['cause_id'].isin(MISDC_CAUSES)]
            df = df.append(df_misdc, ignore_index=True)

        df[RD_VAR_COL] = df[RD_VAR_COL].fillna(0)

        print_log_message("Measuring redistribution variance")
        df = df.apply(self.calculate_redistribution_variance_wrapper, axis=1)
        print_log_message("Done")

        self.diag_df = df.copy()

        draw_cols = ['draw_{}'.format(i) for i in range(0, N_DRAWS)]
        keep_cols = list(orig_cols) + list(draw_cols)
        df = df[keep_cols]
        return df

    def get_diagnostic_dataframe(self):
        if self.diag_df is None:
            print("Run get_computed_dataframe first")
        return self.diag_df

    def remove_package_1(self, df):
        """Remove Package 1 from a map of cause: packages."""
        df['code_system_id'] = self.code_system_id
        csid_p1_map = CONF.get_resource('code_system_id_package_1_map')
        csid_p1_map = pd.read_csv(csid_p1_map)
        df = df.merge(
            csid_p1_map, how='left', on=['code_system_id', 'package_id']
        )
        df = df[df['P-1'].isnull()]
        df = df.drop('P-1', axis=1)
        return df

    def get_package_1_deaths(self):
        """Pull Package 1 deaths by NID, extract type."""
        magic_table_data = assemble_garviz_upload(
            self.nid, self.extract_type_id, upload_folder=None, test=False
        )
        magic_table_data['code_system_id'] = self.code_system_id
        csid_p1_map = CONF.get_resource('code_system_id_package_1_map')
        csid_p1_map = pd.read_csv(csid_p1_map)
        magic_table_data = magic_table_data.merge(
            csid_p1_map, how='left', on=['code_system_id', 'package_id']
        )
        magic_table_data = magic_table_data[magic_table_data['P-1'] == 1]
        magic_table_data = magic_table_data.drop('P-1', axis=1)
        return magic_table_data

    def get_redistribution_envelope(self, agg_df, agg_cause_ids):

        print_log_message("Getting cause-package map")
        cause_package_map = get_cause_package_map(
            self.code_system_id, remove_decimal=self.remove_decimal,
            cause_map=self.cause_map, package_map=self.package_map
        )

        print_log_message("Getting claude data - disaggregation")
        raw_df = get_claude_data("disaggregation", nid=self.nid,
                                 extract_type_id=self.extract_type_id)

        # if there is no garbage in the raw data, then set to 0
        if 743 not in raw_df.cause_id.unique():
            return pd.DataFrame()

        print_log_message("Getting package-targets relation")
        package_targets = get_package_targets(
            self.code_system_id, recurse_garbage_targets=False,
            remove_decimal=self.remove_decimal,
            force_rerun=False, block_rerun=True
        )
        package_targets = self.remove_package_1(package_targets)
        id_to_package_map = \
            cause_package_map.query('map_type == "package_id"')[
                ['code_id', 'map_id']
            ]
        id_to_package_map.rename(columns={'map_id': 'package_id'}, inplace=True)

        print_log_message("Mapping raw data to package ids")
        raw_df = raw_df.merge(id_to_package_map, on='code_id', how='left')
        report_if_merge_fail(
            raw_df.query('cause_id == 743'), 'package_id', 'code_id'
        )

        dfs = []
        print_log_message("Looping over {} causes".format(len(agg_cause_ids)))
        group_cols = ['location_id', 'year_id', 'age_group_id',
                      'sex_id', 'site_id']
        square_df = agg_df[group_cols].drop_duplicates()
        bad_nid_extract_pairs = [(69913, 1), (69918, 1),
                                 (69922, 1), (93739, 1)]
        nid_extract_pair = (self.nid, self.extract_type_id)
        if nid_extract_pair not in bad_nid_extract_pairs:
            p1_df = self.get_package_1_deaths()

        for cause_id in agg_cause_ids:
            cause_ids = get_all_related_causes(cause_id, self.cause_hierarchy)
            package_ids = list(
                package_targets[
                    package_targets['cause_id'].isin(cause_ids)
                ]['package_id'].unique())

            df = raw_df[raw_df['package_id'].isin(package_ids)].copy()
            df = df.groupby(
                group_cols,
                as_index=False
            )['deaths'].sum()
            df = square_df.merge(df, how='left')
            df['deaths'] = df['deaths'].fillna(0)
            df = df.rename(columns={'deaths': 'garbage_targeting_cause'})
            df['cause_id'] = cause_id
            dfs.append(df)
            if nid_extract_pair not in bad_nid_extract_pairs:
                # Do the same for Package 1 deaths
                p1_cause_df = p1_df[p1_df.cause_id.isin(cause_ids)]
                p1_cause_df = p1_cause_df.groupby(['location_id', 'year_id',
                                                   'age_group_id', 'sex_id'],
                                                  as_index=False).freq.sum()
                p1_cause_df = square_df.merge(p1_cause_df, how='left')
                p1_cause_df['freq'] = p1_cause_df['freq'].fillna(0)
                p1_cause_df = p1_cause_df.rename(
                    columns={'freq': 'garbage_targeting_cause'}
                )
                p1_cause_df['cause_id'] = cause_id
                dfs.append(p1_cause_df)

        print_log_message("Concatenating")
        df = pd.concat(dfs, ignore_index=True)
        df = df.groupby(group_cols + ['cause_id'], as_index=False).sum()

        return df

    def get_residual_variance(self):
        """Pull residual variance from model results."""
        residuals_dir = CONF.get_directory('rd_residual_variance_dir')
        residuals_files = os.listdir(residuals_dir)
        dfs = []
        for residuals_file in residuals_files:
            df = pd.read_csv(os.path.join(residuals_dir, residuals_file))
            dfs.append(df)
        df = pd.concat(dfs, ignore_index=True)
        return df

    def get_misdiagnosiscorrection_variance(self):
        misdc_var_path = CONF.get_resource("misdiagnosis_variance")
        dfs = []
        for cause_id in MISDC_CAUSES:
            df = pd.read_csv(
                misdc_var_path.format(
                    nid=self.nid,
                    extract_type_id=self.extract_type_id,
                    cause_id=cause_id
                )
            )
            df['cause_id'] = cause_id
            dfs.append(df)
        df = pd.concat(dfs, ignore_index=True)
        df = df[MISDC_MERGE_COLS + [MISDC_VAR_COL]]
        return df

    def calculate_redistribution_variance_wrapper(self, row,
                                                  expit_threshold=.999,
                                                  offset=10**(-20)):
        try:
            draws_deaths = self.calculate_redistribution_variance(
                row, expit_threshold=.999, offset=10**(-5)
            )

        except Exception:
            raise
            draws_deaths = [row['deaths']] * N_DRAWS

        # Make deaths draw columns for this row
        death_draws_series = pd.Series(draws_deaths).transpose()
        death_draws_series.index = [
            'draw_{}'.format(col) for col in death_draws_series.index
        ]

        row = row.append(death_draws_series)
        return row

    def calculate_redistribution_variance(self, row, expit_threshold=.999,
                                          offset=10**(-5)):
        row_uses_misdc_variance = row['cause_id'] in MISDC_CAUSES and self.has_misdc

        deaths_before = row['cf_corr'] * row['sample_size']
        deaths = row['cf'] * row['sample_size']

        if not row_uses_misdc_variance:
            if (deaths - offset) <= deaths_before:
                return [deaths] * N_DRAWS

            if deaths_before <= offset:
                deaths_before += offset
                deaths += offset

            pct_garbage = (deaths - deaths_before) / deaths
            assert pct_garbage > 0 and pct_garbage < 1, \
                "percent garbage is outside of 0 to 1 range: ({b} - {a}) / "\
                "{b}={p}".format(a=deaths_before, b=deaths, p=pct_garbage)
            logit_pct_garbage = logit(pct_garbage)

            normal = np.random.normal(loc=0, scale=np.sqrt(row[RD_VAR_COL]),
                                      size=N_DRAWS)

            draws_logit_pct_garbage = normal + logit_pct_garbage

            draws_pct_garbage = np.array([min(expit(val), expit_threshold)
                                          for val in draws_logit_pct_garbage])
            draws_pct_garbage = np.array([expit(val)
                                          for val in draws_logit_pct_garbage])

            draws_deaths = deaths_before / (1 - draws_pct_garbage)
        else:
            draws_deaths = np.random.normal(loc=deaths, scale=np.sqrt(row[RD_VAR_COL]), size=N_DRAWS)

        if np.inf in draws_deaths:
            max_fill = max(d for d in draws_deaths if d != np.inf)
            draws_deaths = np.where(draws_deaths == np.inf, max_fill, draws_deaths)

        draws_deaths = np.array([max(val - offset, 0) for val in draws_deaths])


        max_draw = row['sample_size']
        if not row_uses_misdc_variance:
            max_draw = deaths_before + row['garbage_targeting_cause']

            if max_draw < deaths:
                max_draw = row['sample_size']
            if max_draw > row['sample_size']:
                max_draw = row['sample_size']

        draws_deaths = np.array([min(val, max_draw) for val in draws_deaths])

        return draws_deaths

    @staticmethod
    def calculate_codviz_bounds(row):
        std_dev = np.sqrt(row[LOGIT_CF_VAR_COL])
        z = st.norm.ppf(.975)
        ui = z * std_dev
        row[LOWER_RD_COL] = expit(logit(row[MEAN_RD_COL]) - ui)
        row[UPPER_RD_COL] = expit(logit(row[MEAN_RD_COL]) + ui)
        if not (row[MEAN_RD_COL] > 0 and row[MEAN_RD_COL] < 1):
            row[LOWER_RD_COL] = row[MEAN_RD_COL]
            row[UPPER_RD_COL] = row[MEAN_RD_COL]


        try:
            assert row[MEAN_RD_COL] <= row[UPPER_RD_COL]
        except AssertionError:
            if row[MEAN_RD_COL] > row[UPPER_RD_COL]:
                assert np.isclose(0, (row[MEAN_RD_COL] - row[UPPER_RD_COL]))

        try:
            assert row[MEAN_RD_COL] >= row[LOWER_RD_COL]
        except AssertionError:
            if row[MEAN_RD_COL] < row[LOWER_RD_COL]:
                assert np.isclose(0, (row[MEAN_RD_COL] - row[LOWER_RD_COL]))

        return row

    @staticmethod
    def calculate_codem_variances(row, cf_draw_cols, zero_one_buffer=.01):

        cf_draws = np.array(
            row[cf_draw_cols]
        ).flatten()
        cf_draws = cf_draws.astype(float)

        if any(cf_draws <= 0) or any(cf_draws >= 1):

            fill = np.median(cf_draws)
            if fill <= 0 or fill >= 1:
                valid_draws = cf_draws[
                    np.where(np.logical_and(cf_draws > 0, cf_draws < 1))
                ]
                if len(valid_draws) > 0:
                    fill = np.median(valid_draws)
                else:
                    warnings.warn("Row with all un-logitable draws, filling in both variances with 0")
                    row[LOGIT_CF_VAR_COL] = 0
                    row[LOG_DEATHRATE_VAR_COL] = 0
                    return row
            cf_draws = np.where(cf_draws <= 0, fill, cf_draws)
            cf_draws = np.where(cf_draws >= 1, fill, cf_draws)

        upper_cap = 1 - zero_one_buffer
        lower_floor = 0 + zero_one_buffer
        cf_draws = np.where(
            cf_draws >= upper_cap, upper_cap, cf_draws
        )
        cf_draws = np.where(
            cf_draws <= lower_floor, lower_floor, cf_draws
        )
        logit_cf_draws = logit(cf_draws)
        row[LOGIT_CF_VAR_COL] = np.var(logit_cf_draws)

        deaths_draws = cf_draws * row['sample_size']
        deaths_draws_rates = deaths_draws / row['population']
        if 0 in deaths_draws_rates:
            fill = deaths_draws_rates.mean()
            deaths_draws_rates = np.where(
                deaths_draws_rates == 0, fill, deaths_draws_rates
            )
        log_deaths_draws_rates = np.log(deaths_draws_rates)
        row[LOG_DEATHRATE_VAR_COL] = np.var(log_deaths_draws_rates)

        if all(cf_draws == 0):
            row[LOGIT_CF_VAR_COL] = 0

        if all(deaths_draws_rates == 0):
            row[LOG_DEATHRATE_VAR_COL] = 0

        if row['age_group_id'] == 27:
            row[LOG_DEATHRATE_VAR_COL] = 0

        if pd.isnull(row[LOG_DEATHRATE_VAR_COL]) or pd.isnull(row[LOGIT_CF_VAR_COL]):
            raise AssertionError("Null variances for row: {}".format(row))

        return row

    @staticmethod
    def make_codem_codviz_metrics(df, pop_df):
        """Use draws to calculate inputs for CODEm and CoDViz."""
        add_cols = [LOWER_RD_COL, UPPER_RD_COL, LOGIT_CF_VAR_COL,
                    LOG_DEATHRATE_VAR_COL]
        for col in add_cols:
            df[col] = np.nan

        if N_DRAWS > 0:
            cf_draw_cols = RedistributionVarianceEstimator.cf_draw_cols

            df = add_population(df, pop_df=pop_df)
            report_if_merge_fail(
                df.query('age_group_id != 27'), 'population',
                ['age_group_id', 'location_id', 'year_id', 'sex_id']
            )

            # get variance for CODEm
            df = df.apply(
                RedistributionVarianceEstimator.calculate_codem_variances,
                cf_draw_cols=cf_draw_cols, axis=1
            )

            # get the upper and lower bounds for CoDViz
            df = df.apply(
                RedistributionVarianceEstimator.calculate_codviz_bounds, axis=1
            )

            # drop draw/diagnostic/pop columns
            df = df.drop(cf_draw_cols + ['population'], axis=1)

        else:
            df[LOWER_RD_COL], df[UPPER_RD_COL] = df['cf_final'], df['cf_final']
            df[LOGIT_CF_VAR_COL], df[LOG_DEATHRATE_VAR_COL] = 0, 0

        # make sure there aren't any null values in the added columns
        check_no_nulls = [
            LOWER_RD_COL, UPPER_RD_COL, MEAN_RD_COL, LOGIT_CF_VAR_COL,
            LOG_DEATHRATE_VAR_COL
        ]
        null_vals = df.loc[
            df[check_no_nulls].isnull().any(axis=1),
            MISDC_MERGE_COLS + check_no_nulls
        ]
        if len(null_vals) > 0:
            raise AssertionError(
                'there are null values in redistribution uncertainty '
                'columns: \n{}'.format(null_vals)
            )

        return df
