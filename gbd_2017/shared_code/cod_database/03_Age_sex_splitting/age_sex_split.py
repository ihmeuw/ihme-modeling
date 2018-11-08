import pandas as pd
import numpy as np

import db_queries
from cod_prep.downloaders import prep_child_to_available_parent_map
from cod_prep.claude.relative_rate_split import relative_rate_split
from cod_prep.downloaders import (
    get_cause_age_sex_distributions,
    get_current_cause_hierarchy,
    get_country_level_location_id,
    get_pop,
    getcache_age_aggregate_to_detail_map,
    add_location_metadata
)
from datetime import datetime
from cod_process import CodProcess
from configurator import Configurator
pd.options.mode.chained_assignment = None


class AgeSexSplitter(CodProcess):

    diag_df = None
    gbd_team_for_ages = "cod"

    id_cols = ['code_id', 'sex_id', 'site_id', 'nid', 'extract_type_id',
               'year_id', 'age_group_id', 'location_id']

    def __init__(self, cause_set_version_id, pop_run_id,
                 distribution_set_version_id,
                 verbose=False, collect_diagnostics=False, id_cols=None,
                 value_column='deaths'):

        self.cause_set_version_id = cause_set_version_id
        self.pop_run_id = pop_run_id
        self.distribution_set_version_id = distribution_set_version_id
        self.conf = Configurator('standard')
        self.verbose = verbose
        self.collect_diagnostics = collect_diagnostics
        self.value_column = value_column
        if id_cols is not None:
            self.id_cols = id_cols

    def get_computed_dataframe(self, df, location_meta_df):
        """Split value_column into detailed age and sex groups.

        Applies a relative rate splitting algorithm with a K-multiplier that
        adjusts for the specific population that the data to be split applies
        to.

        Arguments and Attributes:
            df (pandas.DataFrame): must contain all columns needed to merge on
                population:
                    ['location_id', 'age_group_id', 'sex_id', 'year_id'].
                Must be unique on id_cols.
            id_cols (list): list of columns that must exist in df and identify
                observations. Used to preserve df in every way except for
                splitting value_column, age_group_id, and sex_id.
            pop_run_id (int): which population version to use
            cause_set_version_id (int): which cause set version id to use
            value_column (str): must be a column in df that contains values
                to be split
            gbd_round_id (int): which gbd_round is it
            gbd_team_for_ages (str): what gbd team to use to call the shared
                function db_queries.get_demographics

        Returns:
            split_df (pandas.DataFrame): contains all the columns passed
                in df, but all age_group_id values will be detailed, all
                sex_ids will be detailed (1, 2), and val will be split
                into these detailed ids.
        """
        # set cache options
        standard_cache_options = {
            'force_rerun': False,
            'block_rerun': True,
            'cache_dir': "standard",
            'cache_results': False
        }
        verbose = self.verbose
        value_column = self.value_column
        pop_run_id = self.pop_run_id
        cause_set_version_id = self.cause_set_version_id
        gbd_round_id = self.conf.get_id('gbd_round')
        id_cols = self.id_cols
        gbd_team_for_ages = self.gbd_team_for_ages

        orig_val_sum = df[self.value_column].sum()

        # pull in populations
        # get relevant populations
        if verbose:
            print("[{}] Prepping population".format(str(datetime.now())))

        locations_in_data = list(set(df.location_id))
        mapping_to_country_location_id = get_country_level_location_id(
            locations_in_data, location_meta_df
        )
        # Map subnational to it's country
        df = df.merge(mapping_to_country_location_id,
                      how='left', on='location_id')
        df.rename(columns={'location_id': 'orig_location_id'}, inplace=True)

        df['location_id'] = df['country_location_id']
        df.drop('country_location_id', axis=1, inplace=True)
        country_locations_in_data = list(df['location_id'].unique())
        years_in_data = list(set(df.year_id))
        pop_df = get_pop(
            pop_run_id=pop_run_id,
            **standard_cache_options
        )
        pop_df = pop_df.loc[
            (pop_df['location_id'].isin(country_locations_in_data)) &
            (pop_df['year_id'].isin(years_in_data))
        ]

        # what columns identify population data
        pop_id_cols = ['location_id', 'age_group_id', 'sex_id', 'year_id']

        assert not pop_df[pop_id_cols].duplicated().any()
        # pull causes table
        if verbose:
            print("[{}] Prepping cause metadata".format(str(datetime.now())))
        cause_meta_df = get_current_cause_hierarchy(
            cause_set_version_id=cause_set_version_id,
            **standard_cache_options
        )

        # pull age sex weights
        if verbose:
            print("[{}] Prepping age sex weights".format(str(datetime.now())))
        dist_df = get_cause_age_sex_distributions(
            distribution_set_version_id=self.distribution_set_version_id,
            **standard_cache_options
        )
        keep_cols = ['cause_id', 'age_group_id', 'sex_id', 'weight']
        dist_df = dist_df[keep_cols]
        # pull age detail map
        if verbose:
            print("[{}] Prepping age agg to detail "
                  "map".format(str(datetime.now())))
        age_detail_map = getcache_age_aggregate_to_detail_map(
            gbd_round_id=gbd_round_id,
            **standard_cache_options
        )

        # create map from aggregate sex ids to detail sex ids
        if verbose:
            print("[{}] Prepping sex detail map".format(str(datetime.now())))
        sex_detail_map = AgeSexSplitter.prep_sex_aggregate_to_detail_map()

        detail_maps = {
            'age_group_id': age_detail_map,
            'sex_id': sex_detail_map
        }

        dist_causes = dist_df.cause_id.unique()

        if verbose:
            print("[{}] Prepping cause_id to weight cause "
                  "map".format(str(datetime.now())))
        cause_to_weight_cause_map = \
            AgeSexSplitter.prep_cause_to_weight_cause_map(
                cause_meta_df, dist_causes)

        val_to_dist_maps = {
            'cause_id': cause_to_weight_cause_map
        }
        # which columns are to be split
        split_cols = ['age_group_id', 'sex_id']

        split_inform_cols = ['cause_id']

        value_cols = [value_column]

        if verbose:
            print("[{}] Running RR splitting "
                  "algorithm".format(str(datetime.now())))
        split_df = relative_rate_split(
            df,
            pop_df,
            dist_df,
            detail_maps,
            split_cols,
            split_inform_cols,
            pop_id_cols,
            value_cols,
            pop_val_name='population',
            val_to_dist_map_dict=val_to_dist_maps,
            verbose=verbose
        )

        df.drop('location_id', axis=1, inplace=True)
        df.rename(columns={'orig_location_id': 'location_id'}, inplace=True)
        if self.collect_diagnostics:
            # making this optional because of memory usage
            self.diag_df = split_df.copy()

        group_columns = list(df.columns)
        group_columns.remove(value_column)
        if verbose:
            print("[{}] Collapsing result".format(str(datetime.now())))
        split_df = split_df.groupby(group_columns,
                                    as_index=False)[value_column].sum()


        if verbose:
            print("[{}] Asserting valid results".format(str(datetime.now())))
        val_diff = abs(split_df[value_column].sum() - orig_val_sum)
        if not np.allclose(split_df[value_column].sum(), orig_val_sum):
            text = "Difference of {} {} from age sex " \
                   "splitting".format(val_diff, value_column)
            raise AssertionError(text)

        # check that all age group ids are good
        good_age_group_ids = db_queries.get_demographics(
            gbd_team_for_ages,
            gbd_round_id=gbd_round_id
        )['age_group_id']
        bad = set(split_df.age_group_id) - set(good_age_group_ids)
        if len(bad) > 0:
            text = "Some age group ids still aggregate: {}".format(bad)
            raise AssertionError(text)

        # should be the same set of cause ids
        assert set(split_df.cause_id) == set(df.cause_id)

        return split_df

    def get_diagnostic_dataframe(self):

        if self.diag_df is None:
            self.collect_diagnostics = True
            # run computation with diagnostics 'on'
            self.get_computed_data_frame()
            assert self.diag_df is not None

        # make sure these are all integer values
        int_cols = ['agg_age_group_id', 'agg_sex_id', 'location_id']
        self.diag_df[int_cols] = \
            self.diag_df[int_cols].astype(int)

        # return diagnostic dataframe
        return self.diag_df.copy()

    @staticmethod
    def prep_sex_aggregate_to_detail_map():
 
        df = pd.DataFrame(
            columns=['agg_sex_id', 'sex_id'],
            data=[
                [3, 1],
                [3, 2],
                [9, 1],
                [9, 2],
                [1, 1],
                [2, 2]
            ]
        )
        return df

    @staticmethod
    def prep_cause_to_weight_cause_map(cause_meta_df, weight_causes):

        weight_cause_map = prep_child_to_available_parent_map(
            cause_meta_df,
            weight_causes
        )
        wgt_cause_col = 'dist_cause_id'
        weight_cause_map = weight_cause_map.rename(
            columns={'parent_cause_id': wgt_cause_col}
        )


        acauses = cause_meta_df[['cause_id', 'acause']].set_index(
            'cause_id').to_dict()['acause']
        paths = cause_meta_df[['cause_id', 'path_to_top_parent']].set_index(
            'cause_id').to_dict()['path_to_top_parent']
        weight_cause_map['path_to_top_parent'] = \
            weight_cause_map['cause_id'].map(paths)


        weight_cause_map.loc[
            weight_cause_map['cause_id'] == 843,
            wgt_cause_col] = 344

        weight_cause_map.loc[
            weight_cause_map['cause_id'].isin([919]),
            wgt_cause_col] = 294

        weight_cause_map.loc[
            weight_cause_map['cause_id'].isin([855, 851]),
            wgt_cause_col] = 855

        weight_cause_map.loc[
            weight_cause_map['cause_id'] == 940,
            wgt_cause_col] = 716

        weight_cause_map['acause'] = weight_cause_map['cause_id'].map(acauses)
        weight_cause_map['weight_acause'] = \
            weight_cause_map[wgt_cause_col].map(acauses)
        return weight_cause_map[['cause_id', wgt_cause_col]]
