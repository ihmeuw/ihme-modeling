import logging

import pandas as pd
from core_maths.summarize import get_summary
from db_tools.ezfuncs import query
import gbd.constants as gbd

from dalynator.computation_element import ComputationElement
from dalynator.age_aggr import AgeAggregator

logger = logging.getLogger(__name__)


class MetricConverter(ComputationElement):
    """
    Takes a number-space DataFrame and converts it to rate and pct space
    """

    DEFAULT_DRAW_COLS = ['draw_{}'.format(x) for x in range(1000)]

    def __init__(self, df, to_rate=False, to_percent=False,
                 data_container=None, include_pre_df=True):
        self.df = df
        self.to_rate = to_rate
        self.to_percent = to_percent
        self.data_container = data_container
        self.include_pre_df = include_pre_df

    def convert_to_pct(self, df, index_cols=['location_id', 'year_id',
                                             'age_group_id', 'sex_id',
                                             'measure_id', 'metric_id'],
                       value_cols=DEFAULT_DRAW_COLS):
        """ Convert DataFrame in number-space to pct-of-total-cause space.
         TODO Should this ALWAYS merge on the intersection of the ID sets of
         the two data frames"""

        # Get the all-cause "envelope"
        env_df = df.query('cause_id == 294')
        # Merge on the envelope
        pct_df = df.merge(env_df, on=index_cols, suffixes=('', '_env'))
        # Transform to pct space
        env_vcs = ["{}_env".format(col) for col in value_cols]
        pct_df[value_cols] = (
            pct_df[value_cols].values / pct_df[env_vcs].values)
        # If there is a divide-by-zero issue, will fill in with 0s
        pct_df[value_cols] = pct_df[value_cols].fillna(0)
        # Change metric to pct space
        pct_df['metric_id'] = gbd.metrics.PERCENT
        # Final cleanup
        keepcols = index_cols + ['cause_id'] + value_cols
        return pct_df[keepcols]

    @staticmethod
    def get_detailed_ages(age_group_set_id=12):
        """
        Pull the most-detailed GBD age groups.
        """
        q = """
        SELECT
            age_group_id
        FROM
            shared.age_group_set_list
        WHERE
            age_group_set_id = {}
        ORDER BY sort_order;""".format(age_group_set_id)
        df = query(q, conn_def='CONN_DEF')
        return df['age_group_id'].unique().tolist()

    @staticmethod
    def aggregate_population(df):
        """ Aggregates population into """
        # Change name to avoid collisions in the function
        df = df.rename(columns={'pop_scaled': 'draw_0'})
        # Add in measure (choosing randomly) and metric ids
        df['measure_id'] = gbd.measures.DALY
        df['metric_id'] = gbd.metrics.NUMBER
        # Use
        index_cols = ['location_id', 'year_id', 'sex_id', 'age_group_id']
        df = AgeAggregator(df, ['draw_0'], index_cols,
                           age_groups={gbd.age.ALL_AGES: (0, 200)}).get_data_frame()
        # Remove ASR (AGE_STANDARDIZED rates, finding them by the fact they are RATE, not NUMBER)
        df = df.loc[df['metric_id'] == gbd.metrics.NUMBER]
        # Change pop name back
        df = df.rename(columns={'draw_0': 'pop_scaled'})
        return df[index_cols + ['pop_scaled']]

    def convert_to_rates(self, df, value_cols=DEFAULT_DRAW_COLS):
        """ Convert DataFrame in number-space to rate space
        """

        # Get population
        pop_df = self.data_container['pop']

        # Merge on population
        rate_df = df.merge(pop_df,
                           on=['location_id', 'year_id', 'sex_id',
                               'age_group_id'],
                           how='inner')
        if len(rate_df) != len(df):
            msg = (
                "Rows before and after pop merge do not match: "
                "before {}, after {}".format(len(df), len(rate_df)))
            raise AssertionError(msg)
        # Transform
        rate_df[value_cols] = (
            rate_df[value_cols].values / rate_df[['pop_scaled']].values)
        # Change metric to pct space
        rate_df['metric_id'] = gbd.metrics.RATE
        # Return with original columns
        return rate_df[df.columns.tolist()]

    def get_data_frame(self):
        df = self.df
        number_df = df.loc[df['metric_id'] == gbd.metrics.NUMBER
                           ].copy(deep=True)

        new_df = []
        if self.to_rate:
            # Convert data to rate space
            rate_df = self.convert_to_rates(
                number_df, value_cols=[c for c in df if c.startswith('draw_')])
            new_df.append(rate_df)
            # Remove rates from input DataFrame, and also remove
            # AGE_STANDARDIZED
            df = df.loc[(df['metric_id'] != gbd.metrics.RATE) |
                        ((df['age_group_id'] == gbd.age.AGE_STANDARDIZED) &
                         (df['metric_id'] == gbd.metrics.RATE))]
        if self.to_percent:
            # Convert data to pct space
            pct_df = self.convert_to_pct(
                number_df, value_cols=[c for c in df if c.startswith('draw_')])
            new_df.append(pct_df)
            # Remove pct ages from input DataFrame
            df = df.loc[(df['metric_id'] != gbd.metrics.PERCENT)]

        if self.include_pre_df:
            new_df.append(df)
        new_df = pd.concat(new_df, sort=True).reset_index(drop=True)

        return new_df


class ComputeSummaries(ComputationElement):
    """Compute the summaries (mean, median, lower, upper) of ALL draw columns.
    A draw column is a column whose name starts with draw_*

    This function does not save them as CSV files, see write_csv.py
    The data source must have exactly the following indexes:
        ['location_id', 'year_id', 'age_group_id', 'sex_id',
        'cause_id', 'measure_id', 'metric_id']
        Any other 'extra' non-draw columns (e.g. "pop") will be carried
        through unchanged.
    """

    END_MESSAGE = "END compute summaries"

    MINIMUM_INDEXES = ['location_id', 'year_id', 'age_group_id', 'sex_id',
                       'cause_id', 'measure_id', 'metric_id']
    """Must have at least these indexes"""

    def __init__(self,
                 in_df,
                 write_out_columns,
                 index_cols=MINIMUM_INDEXES,
                 ):
        self.in_df = in_df
        self.write_out_columns = write_out_columns
        self.index_cols = index_cols

    def get_data_frame(self):
        logger.info("BEGIN compute summaries")

        self.validate_measure_and_metric(self.in_df, "incoming dataframe")
        logger.debug("validated")

        sumdf = get_summary(self.in_df,
                            self.in_df.filter(like='draw_').columns)
        sumdf = sumdf.reset_index()
        del sumdf['index']
        del sumdf['median']

        if 'pct_change_means' in sumdf:
            logger.info("replacing mean of pct change distribution with pct "
                        "change of means")
            sumdf['mean'] = sumdf['pct_change_means']
        sumdf = sumdf[self.write_out_columns]

        return sumdf

    @staticmethod
    def log_and_raise(error_message):
        logger.error(error_message)
        raise ValueError(error_message)

    def validate_measure_and_metric(self, df, df_name):
        """Set the index
        TODO rename?"""

        non_draw_indexes = [x for x in df.columns if not x.startswith('draw_')]
        if non_draw_indexes:
            logger.debug(
                "dataframe {} has non-draw indexes {}, re-indexing".format(
                    df_name, non_draw_indexes))
            df.reset_index(inplace=True)
            # The set of index columns will now typically be longer
            index_cols = [x for x in df.columns if not x.startswith('draw_')]
            df.set_index(index_cols, inplace=True)
            self.index_cols = index_cols
