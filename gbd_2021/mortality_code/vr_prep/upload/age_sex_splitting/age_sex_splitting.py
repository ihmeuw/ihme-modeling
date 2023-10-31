"""
Core functionality for age sex splitting. Functions from this file should be
imported.

There are three main classes:
    1) AgeSexInputData:
        Builds input data object
    2) AgeSexDistribution
        Builds input distribution object
    3) AgeSexDataSplit
        Contains the methods to perform splitting. input data and distribution
        objects are passed in.
"""

import pandas as pd
import numpy as np
import sys
import getpass
import gbd

from hierarchies.dbtrees import agetree

sys.path.append("".format(getpass.getuser()))
from cod_prep.utils import report_if_merge_fail


class AgeSexInputData(object):
    def __init__(self, input_data, sex_id_column='sex_id',
                 age_group_id_column='age_group_id', value_column='value'):
        self.input_data = input_data
        self.sex_id_column = sex_id_column
        self.age_group_id_column = age_group_id_column
        self.value_column = value_column

        self.check_index_unique()

    # Validation
    def check_na_values(self):
        pass

    def check_missing_columns(self):
        pass

    def check_extra_columns(self):
        pass

    def check_negative_values(self):
        pass

    def check_index_unique(self):
        unique_cols = self.index_columns + ['sex_id', 'age_group_id']
        duplicates = self.data.loc[self.data[unique_cols].duplicated(), unique_cols].copy(deep=True)
        duplicates = duplicates.drop_duplicates()
        if  len(duplicates) > 0:
            raise AssertionError("There were {} duplicate rows identified:\n{}".format(
                len(duplicates), duplicates.to_string()))

    # Calculate properties to be used in age-sex splitting
    @property
    def index_columns(self):
        index_columns = (
            list(set(self.input_data.columns) - set([
                self.sex_id_column, self.age_group_id_column,
                self.value_column]))
        )
        return [col for col in self.input_data.columns if col in index_columns]


    @property
    def data(self):
        data = self.input_data.copy(deep=True)
        new_columns = {
            self.sex_id_column: 'sex_id',
            self.age_group_id_column: 'age_group_id',
            self.value_column: 'value_aggregate'
        }
        data[self.value_column] = data[self.value_column].astype('float64')
        data = data.fillna("--filled-na--")
        return data.rename(columns=new_columns)



class AgeSexDistribution(object):
    def __init__(self, distribution_data, index_columns, weight_column='weight'):
        self.distribution_data = distribution_data
        self.index_columns = index_columns
        self.weight_column = weight_column

    # Validation
    def check_na_values(self):
        pass

    def check_extra_columns(self):
        pass

    def check_negative_values(self):
        pass

    @property
    def data(self):
        data = self.distribution_data.copy(deep=True)
        data = data[self.index_columns + [self.weight_column]]
        new_columns = {
            self.weight_column: 'weight'
        }
        return data.rename(columns=new_columns)



class AgeSexDataSplit(object):
    def __init__(self, input_data, distribution_data, pop_data, gbd_round_id, is_cod_vr, out_dir):
        # Input data
        self.input_data = input_data
        self.distribution_data = distribution_data
        self.pop_data = pop_data

        # Information to help fetch metadata
        self.gbd_round_id = gbd_round_id
        self.is_cod_vr = is_cod_vr
        self.out_dir = out_dir


    # Validation
    def check_valid_age_groups(self):
        pass

    def check_valid_sex_groups(self):
        pass

    def check_sum_total(self, data_before, data_after, index_columns):
        data_before = data_before.copy(deep=True)
        data_after = data_after.copy(deep=True)

        # Rename value columns
        data_before = data_before.rename(columns={'value_aggregate': '_value_'})
        data_after = data_after.rename(columns={'value_split': '_value_'})

        # Get totals by the input index columns
        data_before = data_before.groupby(index_columns)['_value_'].sum().reset_index()
        data_after = data_after.groupby(index_columns)['_value_'].sum().reset_index()

        # Sort
        data_before = data_before.sort_values(index_columns).reset_index(drop=True)
        data_after = data_after.sort_values(index_columns).reset_index(drop=True)

        # Merge together
        data = pd.merge(
            data_before, data_after, on=index_columns, how='outer',
            suffixes=['before', 'after'])

        # Calculate difference
        data.loc[:,'abs_diff'] = abs(data['_value_before'] - data['_value_after'])

        # Check
        over_threshold = data.loc[data['abs_diff'] > 0.0001].copy(deep=True)
        if len(over_threshold) > 0:
            raise AssertionError(
                "There are {} rows that are different before and after:\n{}".format(
                    len(over_threshold), over_threshold.to_string()))

    def check_merge_columns(self):
        pass

    def check_conflict_columns(self):
        pass

    # Determine merge columns
    @property
    def k_denominator_index_columns(self):
        return self.input_data.index_columns + ['sex_id_aggregate', 'age_group_id_aggregate']


    # Fetch inputs
    def get_sex_split_mapping(self):
        """
        Return a mapping from the aggregate sex_id to the most-detailed sex_id

        Because the mapping space for sex_ids is so small, the values are
        mannually coded below.

        Returns:
            df (pandas.DataFrame): ['sex_id_aggregate', 'sex_id']
        """
        data = pd.DataFrame(
            columns=['sex_id_aggregate', 'sex_id'],
            data=[
                [3, 1],
                [3, 2],
                [9, 1],
                [9, 2],
                [1, 1],
                [2, 2]
            ]
        )
        return data

    def get_age_split_mapping(self, age_group_list):
        """
        Look up each age group id to determine the underlying most-detailed ages
        """
        mapping = []
        for age_group_id in age_group_list:

            # We want GBD2019 age groups for all non-22 age groups. New age groups should only be split to from unknown and both sex.
            if (age_group_id == 22):
                age_tree = agetree(age_group_id, gbd_round_id=self.gbd_round_id).leaves()
            else:
                age_tree = agetree(age_group_id, gbd_round_id=6).leaves()

            for l in age_tree:
                mapping.append({
                    'age_group_id_aggregate': age_group_id,
                    'age_group_id': l.id})
        return pd.DataFrame(mapping)


    # Data prep
    def expand_age_groups(self, data):
        # Get age group mappings
        age_group_list = data['age_group_id'].drop_duplicates().tolist()
        age_group_id_mapping = self.get_age_split_mapping(age_group_list)

        # Merge on mappings to expand age groups
        data = data.rename(columns={'age_group_id': 'age_group_id_aggregate'})
        data = pd.merge(
            data, age_group_id_mapping, on='age_group_id_aggregate', how='left')
        report_if_merge_fail(df=data, check_col='age_group_id', merge_cols=['age_group_id_aggregate'])

        return data

    def expand_sex_groups(self, data):
        # Get sex mappings
        sex_id_mapping = self.get_sex_split_mapping()

        # Merge on mappings to expand age groups
        data = data.rename(columns={'sex_id': 'sex_id_aggregate'})
        data = pd.merge(data, sex_id_mapping, on='sex_id_aggregate', how='left')
        report_if_merge_fail(df=data, check_col="sex_id", merge_cols=['sex_id_aggregate'])

        return data

    def expand_age_sex_groups(self, data):
        # Expand sex groups
        data = self.expand_sex_groups(data)

        # Expand age groups
        data = self.expand_age_groups(data)

        return data

    def prep_data(self):
        # Expand age and sex of input data
        data = self.expand_age_sex_groups(self.input_data.data.copy(deep=True))
        
        if self.is_cod_vr == "TRUE":
            data_expanded_name = "cod_expanded_age_groups"
        else:
            data_expanded_name = "noncod_expanded_age_groups"
        data_expanded_out_file = "{}/{}.csv".format(self.out_dir, data_expanded_name)
        
        data.to_csv(data_expanded_out_file, index=False)

        # Drop duplicates in data
        data = data.drop_duplicates()

        # Create some fake age weights here
        to_split_weights = self.distribution_data.data.query("age_group_id == 4 | age_group_id == 5")

        # use NOR national weights for new subnationals
        for loc in range(60132, 60138):
            temp = to_split_weights.loc[to_split_weights.location_id==90]
            temp.location_id = loc
            to_split_weights = pd.concat([to_split_weights, temp])

        group_cols = ['location_id', 'year_id', 'sex_id', 'source']
        data_pnn = data.loc[data.age_group_id.isin([388,389]), group_cols + ['age_group_id', 'value_aggregate']].pivot_table(index = group_cols, columns='age_group_id', values = 'value_aggregate').reset_index()
        data_pnn.loc[:,'age_group_parent'] = 4

        data_ch = data.loc[data.age_group_id.isin([238,34]), group_cols + ['age_group_id', 'value_aggregate']].pivot_table(index = group_cols, columns='age_group_id', values = 'value_aggregate').reset_index()
        data_ch.loc[:,'age_group_parent'] = 5

        # Fill missing vals with 0
        data_pnn[388] = data_pnn[[388]].fillna(0)
        data_pnn[389] = data_pnn[[389]].fillna(0)
        data_ch[238] = data_ch[[238]].fillna(0)
        data_ch[34] = data_ch[[34]].fillna(0)

        # Calculate ratio and merge on
        data_pnn.loc[:,'ratio'] = data_pnn[388] / (data_pnn[388] + data_pnn[389])
        data_ch.loc[:,'ratio'] = data_ch[238] / (data_ch[238] + data_ch[34])

        # copy 2017 weights to use with 2018
        u1_wts = to_split_weights.query("year_id == 2017")
        u1_wts.year_id = 2018
        to_split_weights = pd.concat([to_split_weights, u1_wts], sort=True)

        # copy 2017 weights to use with 2019
        u2_wts = to_split_weights.query("year_id == 2017")
        u2_wts.year_id = 2019
        to_split_weights = pd.concat([to_split_weights, u2_wts], sort=True)

        # copy 2017 weights to use with 2020
        u3_wts = to_split_weights.query("year_id == 2017")
        u3_wts.year_id = 2020
        to_split_weights = pd.concat([to_split_weights, u3_wts], sort=True)
        
        # copy 2017 weights to use with 2021
        u4_wts = to_split_weights.query("year_id == 2017")
        u4_wts.year_id = 2021
        to_split_weights = pd.concat([to_split_weights, u4_wts], sort=True)

        data_pnn = data_pnn.merge(to_split_weights, left_on = ['location_id', 'year_id', 'sex_id', 'age_group_parent'],
            right_on = ['location_id', 'year_id', 'sex_id', 'age_group_id'], how='left')
        data_ch = data_ch.merge(to_split_weights, left_on = ['location_id', 'year_id', 'sex_id', 'age_group_parent'],
            right_on = ['location_id', 'year_id', 'sex_id', 'age_group_id'], how='left')

        # reshape long, calculate ratio, combine
        data_pnn = pd.melt(data_pnn, id_vars=['location_id','year_id','sex_id','source', 'ratio', 'weight'], value_vars=[388,389], var_name='age_group_id')
        data_ch = pd.melt(data_ch, id_vars=['location_id','year_id','sex_id','source', 'ratio', 'weight'], value_vars=[238,34], var_name='age_group_id')

        data_pnn.weight = data_pnn['weight'] * data_pnn['ratio']
        data_ch.weight = data_ch['weight'] * data_ch['ratio']

        data_pnn = data_pnn[['location_id', 'year_id', 'sex_id', 'age_group_id', 'weight']]
        data_ch = data_ch[['location_id', 'year_id', 'sex_id', 'age_group_id', 'weight']]

        # Finally combine
        final_weights = pd.concat([self.distribution_data.data, data_pnn, data_ch], sort=True)

        # for new Norway subnationals, apply Norway national weights
        for loc in range(60132, 60138):
            temp = final_weights.loc[final_weights.location_id==90]
            temp.location_id = loc
            final_weights = pd.concat([final_weights, temp])

        # Fill nas with 0 - no deaths in those age bins
        final_weights.weight = final_weights['weight'].fillna(0)

        # Add on old age groups to weights for 2018, use 2017
        new_wts = final_weights.loc[(final_weights.year_id==2017) & ~(final_weights.age_group_id.isin([388,389,34,238]))]
        new_wts.year_id=2018
        final_weights = pd.concat([final_weights, new_wts], sort=True)

        # Add on old age groups to weights for 2019, use 2017
        new_wts2 = final_weights.loc[(final_weights.year_id==2017) & ~(final_weights.age_group_id.isin([388,389,34,238]))]
        new_wts2.year_id=2019
        final_weights = pd.concat([final_weights, new_wts2], sort=True)

        # Add on old age groups to weights for 2020, use 2017
        new_wts3 = final_weights.loc[(final_weights.year_id==2017) & ~(final_weights.age_group_id.isin([388,389,34,238]))]
        new_wts3.year_id=2020
        final_weights = pd.concat([final_weights, new_wts3], sort=True)
        
        # Add on old age groups to weights for 2021, use 2017
        new_wts4 = final_weights.loc[(final_weights.year_id==2017) & ~(final_weights.age_group_id.isin([388,389,34,238]))]
        new_wts4.year_id=2021
        final_weights = pd.concat([final_weights, new_wts4], sort=True)

        # Merge on weights
        data = pd.merge(
            data, final_weights,
            on=self.distribution_data.index_columns, how='left')

        # If all
        report_if_merge_fail(df=data, check_col="weight", merge_cols=self.distribution_data.index_columns)

        # Merge on population
        data = pd.merge(
            data, self.pop_data.copy(deep=True),
            on=['location_id', 'year_id', 'sex_id', 'age_group_id'], how='left')
        report_if_merge_fail(df=data, check_col="population", merge_cols=['location_id', 'year_id', 'sex_id', 'age_group_id'])

        return data


    # Do math
    def calculate_k_denominator(self, data):
        data = data.copy(deep=True)
        data.loc[:,'k_denominator'] = data['weight'] * data['population']
        data = data.groupby(self.k_denominator_index_columns)['k_denominator'].sum()
        data = data.reset_index()
        return data


    def relative_rate_split(self, data):
        """
        expected_rate * national_pop *
        (aggregate_deaths / aggregate_sum_of_expected_rate_times_national_pop)
        """
        data = data.copy(deep=True)
        data.loc[:,'value_split'] = (
            data['weight'] * data['population'] *
            (data['value_aggregate'] / data['k_denominator']))
        return data


    def format_output(self, data):
        data = data.copy(deep=True)
        keep_columns = self.input_data.index_columns + ['sex_id', 'age_group_id', 'value_split', "age_group_id_aggregate", "sex_id_aggregate"]
        new_columns = {
            'sex_id': self.input_data.sex_id_column,
            'age_group_id': self.input_data.age_group_id_column,
            'value_split': self.input_data.value_column
        }
        data = data[keep_columns].rename(columns=new_columns)
        for c in data.columns:
            if len(data.loc[data[c].astype('str') == "--filled-na--", c]) > 0:
                data.loc[data[c].astype('str') == "--filled-na--", c] = np.nan
        return data


    # Functions to run
    def run_relative_rate_split(self):
        # Prep data
        data = self.prep_data()

        # Calculate k denominators
        k_denominator = self.calculate_k_denominator(data)
        data = pd.merge(
            data, k_denominator,
            on=self.k_denominator_index_columns,
            how='left')
        report_if_merge_fail(df=data, check_col="k_denominator", merge_cols=self.k_denominator_index_columns)

        # Run age-sex split
        data = self.relative_rate_split(data)
        # when no value calculated for value_split, use value_aggregate
        data.loc[(data.value_split.isna()), 'value_split'] = data.loc[(data.value_split.isna()), 'value_aggregate']

        # Check sum totals
        print("Deaths grand total before: {}".format(self.input_data.data["value_aggregate"].sum()))
        print("Deaths grand total after: {}".format(data["value_split"].sum()))
        self.check_sum_total(self.input_data.data, data, self.input_data.index_columns)

        # Reformat
        data = self.format_output(data)

        return data
