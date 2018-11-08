import pandas as pd
import numpy as np
import re

from cod_process import CodProcess
from configurator import Configurator

from cod_prep.utils import print_log_message
from redistribution_variance import dataset_has_redistribution_variance


class NoiseReducer(CodProcess):
    conf = Configurator('standard')
    ndraws = conf.get_resource('uncertainty_draws')

    def __init__(self, data_type_id, source):
        self.data_type_id = data_type_id
        self.source = source
        self.maternal_exceptions = [
            "Other_Maternal", "Mexico_BIRMM",
            "Maternal_report", "SUSENAS"
        ]

        self.id_cols = ['nid', 'extract_type_id', 'location_id', 'site_id',
                        'year_id', 'age_group_id', 'sex_id', 'cause_id',
                        'sample_size']
        self.value_cols = ['cf', 'cf_raw', 'cf_rd', 'cf_corr']

        self.has_redistribution_variance = dataset_has_redistribution_variance(
            self.data_type_id, self.source
        )
        if self.has_redistribution_variance:
            self.value_cols += ['cf_draw_' + str(n)
                                for n in range(0, self.ndraws)]

    def get_computed_dataframe(self, df):

        df = df[df['sample_size'] > 0]

        if self.data_type_id != 8:
            if self.has_redistribution_variance:
                print_log_message(
                    "Noise reducing data with {} draws".format(self.ndraws)
                )
                for n in range(0, self.ndraws + 1):
                    if n == self.ndraws:
                        prior_deaths_col = 'pred_deaths'
                        prior_se_col = 'std_err_deaths'
                        cf_col = 'cf'
                        pre_nr_col = 'cf_pre_nr'
                    else:
                        prior_deaths_col = self.extract_col(
                            '^pred_draw_{}$'.format(n), df.columns
                        )
                        prior_se_col = self.extract_col(
                            '^std_err_draw_{}$'.format(n), df.columns
                        )
                        orig_deaths_col = self.extract_col(
                            '^draw_{}$'.format(n), df.columns
                        )
                        df['cf_draw_' + str(n)] = \
                            df[orig_deaths_col] / df['sample_size']
                        cf_col = self.extract_col(
                            '^cf_draw_{}$'.format(n), df.columns
                        )
                        pre_nr_col = 'cf_pre_nr_draw_' + str(n)

                    df = NoiseReducer.make_metrics_using_results(
                        df, prior_se_col, prior_deaths_col, cf_col
                    )
                    df = self.replace_data(df, cf_col, pre_nr_col)
            else:
                print_log_message("Noise reducing data with no draws.")
                prior_deaths_col = 'pred_deaths'
                prior_se_col = 'std_err_deaths'
                cf_col = 'cf'
                pre_nr_col = 'cf_pre_nr'
                df = NoiseReducer.make_metrics_using_results(
                    df, prior_se_col, prior_deaths_col, cf_col
                )
                df = self.replace_data(df, cf_col, pre_nr_col)

            # diagnostic df will only have intermediate steps for one draw
            self.diag_df = df
            df = self.cleanup(df, cf_col)

        return df

    def extract_col(self, pattern, col_list):
        """Get draw columns from incoming data."""
        match_list = [x for x in col_list if re.search(pattern, x)]
        if len(match_list) == 0:
            print("No columns found matching {}".format(pattern))
            return None
        elif len(match_list) > 1:
            print("Found more than one columns matching {} "
                  "expecting only one column".format(pattern))
            return None
        else:
            return match_list[0]

    @staticmethod
    def get_predicted_var(df, prior_se_col, prior_deaths_col, default_var=False):
        """Set the variance based on the cause fraction."""
        df['predicted_cf'] = df[prior_deaths_col] / df['sample_size']
        if default_var:
            df['predicted_std_err'] = 0
            df['predicted_var'] = ((1 / df['sample_size']) *
                                   (df['predicted_cf']) *
                                   (1 - df['predicted_cf']))
        else:
            # Normalize standard error
            df['predicted_std_err'] = np.exp(
                df[prior_se_col] - 1) * df['predicted_cf']
            df.predicted_std_err = np.nan_to_num(df.predicted_std_err)

            df['predicted_var'] = np.square(df['predicted_std_err'])
            df.loc[
                (df['predicted_var'].isnull()) |
                (df['predicted_var'] == np.inf),
                'predicted_var'] = 10**91 * df['predicted_cf']
        df['predicted_var'] = np.nan_to_num(df['predicted_var'])
        return df

    @staticmethod
    def make_metrics_using_results(df, prior_se_col, prior_deaths_col, cf_col):

        non_zero_std_err = df[df[prior_se_col] != 0]
        non_zero_std_err = NoiseReducer.get_predicted_var(
            non_zero_std_err, prior_se_col, prior_deaths_col
        )
        zero_std_err = df[df[prior_se_col] == 0]
        zero_std_err = NoiseReducer.get_predicted_var(
            zero_std_err, prior_se_col, prior_deaths_col, default_var=True
        )
        df = non_zero_std_err.append(zero_std_err).reset_index(drop=True)

        cf_component = df[cf_col] * (1 - df[cf_col])
        df['std_err_data'] = np.sqrt(
            (cf_component / df['sample_size']) +
            ((1.96**2) / (4 * df['sample_size'] ** 2))
        )
        df['variance_data'] = df['std_err_data'] ** 2
        df['mean_cf_data_component'] = df[cf_col] * \
            (df['predicted_var'] /
             (df['predicted_var'] + df['variance_data']))
        df['mean_cf_prediction_component'] = (
            df['predicted_cf'] *
            (df['variance_data'] /
                (df['predicted_var'] + df['variance_data']))
        )
        df['mean'] = (df['mean_cf_data_component'] +
                      df['mean_cf_prediction_component'])
        df['variance_numerator'] = (
            df['predicted_var'] * df['variance_data']
        )
        df['variance_denominator'] = (
            df['predicted_var'] + df['variance_data']
        )
        df['variance'] = (
            df['variance_numerator'] / df['variance_denominator']
        )
        assert df['mean'].notnull().all()
        return df

    def replace_data(self, df, cf_col, pre_nr_col):
        """Use model results to fill in values."""
        df[pre_nr_col] = df[cf_col]
        if self.source in self.maternal_exceptions:
            df[cf_col] = df['predicted_cf']
        else:
            df[cf_col] = df['mean']
        return df

    def cleanup(self, df, cf_col):
        """Keep only necessary columns and conform data types."""
        if 'orig_location_id' in df.columns:
            df['location_id'] = df['orig_location_id']
            df.drop('orig_location_id', axis=1, inplace=True)
        df[cf_col] = df[cf_col].astype(float)
        df = df[self.id_cols + self.value_cols]
        return df

    def get_diagnostic_dataframe(self):
        """Return diagnostic evaluation of how things went."""
        try:
            diag_df = self.diag_df
        except AttributeError:
            print(
                "Diagnostic dataframe is the result of get_computed_dataframe,"
                " if you are invoking this on the NoiseReducer object before "
                "calling get_computed_dataframe, it will return an empty "
                "dataframe"
            )
            diag_df = pd.Dataframe()
        return diag_df
