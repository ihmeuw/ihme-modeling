"""Correct the number of HIV deaths.

Notes:
This module has HIV specific corrections, but uses methods from the
positive_excess_corrections module, which is a general adjustment
for any causes, not necessarily HIV.
"""

import pandas as pd

from cod_process import CodProcess
from configurator import Configurator
from cod_prep.downloaders import (
    prep_child_to_available_parent_map,
    add_population,
    add_cause_metadata,
    get_cod_ages,
    add_code_metadata,
    get_garbage_from_package
)
from cod_prep.claude.positive_excess_correction import (
    identify_positive_excess,
    flag_correct_dem_groups,
    move_excess_to_target,
    assign_code_to_created_target_deaths
)
from cod_prep.utils.misc import print_log_message

# ignore SettingWithCopy warning
pd.options.mode.chained_assignment = None


class HIVCorrector(CodProcess):
    """Compare rates to reference rates and mark positive excess of HIV."""

    # Terminal age group in all datasets (this method is post age splitting)
    maxage = 95

    # Terminal age group TO CORRECT (past this age, we will just leave
    # the cause composition of deaths as-is)
    maxcorrect = 70

    # ## Reference age groups ##
    # For each cause, these age groups determine the ages to pool together
    # mortality rates to get a reference rate, against which relative rates
    # will be calculated for each age.
    # They are a critical lever to deal with cause-specific anomalous age
    # patterns that may inhibit this method from "catching" hidden hiv.
    # Ages selected should be those where we'd expect data to be rich (large
    # sample size) and stable for the given cause
    # Some of these actually refer to groups (e.g. _all, tb [tb & sub-causes])
    reference_ages = {
        294: [18, 19, 20],
        297: [16, 17, 18],
        545: [17, 18],
        380: [4, 5],
        366: [12, 13, 14],
    }

    # Set age restrictions for each country in which we move garbage
    # ZAF: 15-55, inclusive
    # ZWE: 10-45, inclusive
    # MOZ: 15-45, inclusive
    move_gc_age_restrictions = {
        196: [8, 9, 10, 11, 12, 13, 14, 15, 16],
        482: [8, 9, 10, 11, 12, 13, 14, 15, 16],
        483: [8, 9, 10, 11, 12, 13, 14, 15, 16],
        484: [8, 9, 10, 11, 12, 13, 14, 15, 16],
        485: [8, 9, 10, 11, 12, 13, 14, 15, 16],
        486: [8, 9, 10, 11, 12, 13, 14, 15, 16],
        487: [8, 9, 10, 11, 12, 13, 14, 15, 16],
        488: [8, 9, 10, 11, 12, 13, 14, 15, 16],
        489: [8, 9, 10, 11, 12, 13, 14, 15, 16],
        490: [8, 9, 10, 11, 12, 13, 14, 15, 16],
        198: [7, 8, 9, 10, 11, 12, 13, 14],
        184: [8, 9, 10, 11, 12, 13, 14]
    }

    # set the cause_id for sepsis, injury, and hiv-redistribution garbage codes
    sepsis_cause_id = 860
    injury_cause_id = 9991
    hivrd_cause_id = 9992

    def __init__(self, df, iso3, code_system_id, pop_df,
                 cause_meta_df, loc_meta_df, age_meta_df,
                 correct_garbage=False):
        self.df = df
        # set filepaths
        self.conf = Configurator('standard')
        self.cache_dir = self.conf.get_directory('db_cache')
        self.cause_selections_path = self.conf.get_resource('cause_selections')
        # grab metadata
        self.iso3 = iso3
        self.code_system_id = code_system_id
        self.value_cols = ['deaths']
        self.pop_col = 'population'
        self.cause_meta_df = cause_meta_df
        self.pop_df = pop_df
        self.loc_meta_df = loc_meta_df
        self.age_meta_df = age_meta_df
        self.correct_garbage = correct_garbage

    def get_computed_dataframe(self):
        """Main method to execute computations and return result.

        Notes:
        UNDECIDED HOW TO DO THIS WITHOUT ALL YEARS IN MEMORY LIKE STATA HAD

        Potential solutions:
        1. Don't do this at all, just correct ANY cause-age-sex-location-year
            that exceeds the global reference rate
              - this would potentially change results slightly, but does not
                seem unreasonable, and in fact seems more correct

        2. Prime HIV correction by assembling the list ahead of time
              - might take a long time and need to be rerun every time, which
                would essentially double the required time for this step
              - advantage is that it mimics last years results without needing
                any additional years of data
              - could eliminate some of the problems with this method by
                running it very infrequently instead of every time
                the data changes

        3. Take a 'source' argument in the class and pull the other data that
            we pulled last year to pool years necessary to generate this list

        4. Run HIV correction with all the data for a 'source' altogether, like
            the stata code did, but still update versions based on nid-year

        FOR NOW: Follow method 1 and expect to test the similarity later
        """
        keep_cols = self.df.columns

        if not self.country_needs_correction():
            print_log_message("Country doesn't need hiv correction")
            self.diag_df = None
            return self.df

        print_log_message("Getting rates df")
        rates_df = self.get_rates_df(self.cause_meta_df)
        if self.correct_garbage:
            df = add_code_metadata(self.df, add_cols=['value'],
                                   code_system_id=self.code_system_id,
                                   force_rerun=False,
                                   block_rerun=True,
                                   cache_dir=self.cache_dir)
            df = self.identify_sepsis_gc(df, self.code_system_id)
            df = self.identify_injury_gc(df, self.code_system_id)
            df = self.identify_hivrd_gc(df, self.code_system_id)
            # do a groupby to collapse down to cause_id level for next steps
            group_cols = [
                x for x in keep_cols if x not in ['code_id', 'deaths']
            ]
            df_by_code = df.copy()
            df_by_cause = df.groupby(
                group_cols, as_index=False)['deaths'].sum()
        else:
            df_by_cause = self.df
        df = add_population(df_by_cause, pop_df=self.pop_df)
        print_log_message("Flagging correct dem groups for "
                          "{0} rows of data".format(len(df)))
        df = flag_correct_dem_groups(df,
                                     self.code_system_id,
                                     self.cause_meta_df,
                                     self.loc_meta_df,
                                     self.age_meta_df,
                                     rates_df,
                                     self.reference_ages,
                                     self.move_gc_age_restrictions,
                                     self.value_cols,
                                     self.pop_col,
                                     self.cause_selections_path,
                                     correct_garbage=self.correct_garbage)
        cause_to_targets_map = self.get_cause_to_targets_map(
            self.cause_meta_df
        )
        print_log_message("Identifying positive excess")
        df = identify_positive_excess(df,
                                      rates_df,
                                      cause_to_targets_map,
                                      self.reference_ages,
                                      self.loc_meta_df,
                                      self.cause_meta_df,
                                      self.value_cols,
                                      self.pop_col,
                                      self.correct_garbage)
        if self.correct_garbage:
            df = self.calculate_garbage_positive_excess(df, df_by_code,
                                                        group_cols)
            print_log_message("Moving excess to target")
            df = move_excess_to_target(df, self.value_cols,
                                       cause_to_targets_map,
                                       self.correct_garbage)
            computed_df = assign_code_to_created_target_deaths(
                df, self.code_system_id, self.cause_meta_df)
        else:
            print_log_message("Moving excess to target")
            computed_df = move_excess_to_target(df, self.value_cols,
                                                cause_to_targets_map,
                                                self.correct_garbage)
        self.diag_df = computed_df
        return computed_df[keep_cols]

    def country_needs_correction(self):
        """Does master cause selections ever target this country?"""
        country_id = self.loc_meta_df.loc[
            self.loc_meta_df['ihme_loc_id'] == self.iso3,
            'location_id'].iloc[0]
        master_cause_selections = pd.read_csv(self.cause_selections_path)
        return country_id in set(master_cause_selections.location_id)

    def identify_sepsis_gc(self, df, code_system_id):
        """Mark garbage sepsis related deaths in ICD10 and ICD9_detail."""
        if code_system_id == 1:
            package = 'FIX-P-58'
        elif code_system_id == 6:
            package = 'FIX-P-6'
        else:
            return df
        sepsis_df = get_garbage_from_package(code_system_id, package)
        # may need some more things for formatting, but basic idea:
        df = df.merge(sepsis_df, on=['value'],
                      how='left', indicator=True)
        # change the cause_id to septicemia
        df.loc[df['_merge'] == 'both', 'cause_id'] = self.sepsis_cause_id
        df.drop(['_merge', 'package_description', 'package_id',
                 'package_name'], axis=1, inplace=True)
        return df

    def identify_hivrd_gc(self, df, code_system_id):
        hivrd_df = get_garbage_from_package(
            code_system_id, "HIV-CORR", match_prefix=True
        )
        df = df.merge(hivrd_df, on=['value'], how='left', indicator=True)
        # change cause id to hivcorr
        df.loc[df['_merge'] == 'both', 'cause_id'] = self.hivrd_cause_id
        df.drop(['_merge', 'package_description', 'package_id',
                 'package_name'], axis=1, inplace=True)
        return df

    def identify_injury_gc(self, df, code_system_id):
        """Unflag garbage for injury related deaths."""
        is_gc = df['cause_id'] == 743
        if code_system_id == 1:
            is_injury_code = df['value'].str.contains('^[V-Y]')
        elif code_system_id == 6:
            is_injury_code = df['value'].str.contains('^E')
        else:
            # always false
            is_injury_code = df['value'] != df['value']
        # mark it
        df.loc[is_injury_code & is_gc, 'cause_id'] = self.injury_cause_id
        return df

    def calculate_garbage_positive_excess(self, df, df_by_code,
                                          group_cols):
        """Calculate positive excess for pre redistribution garbage.

        Essentially do some back calculations so that the positive
        excess calculated at the cause_id level can be applied at the
        more detailed code_id level. Excess at the cause_id level
        needs to be split up appropriately for the code_id level.
        """
        df['prop'] = df['deaths_post'] / df['deaths']
        keep_cols = group_cols + ['flagged'] + ['prop']
        merge_cols = [x for x in keep_cols if x not in ['prop', 'flagged']]
        df = df[keep_cols]
        df = df_by_code.merge(df, how='left', on=merge_cols)

        # now make deaths_post and positive excess at the code_id detail level
        df.loc[
            df['flagged'] == 1,
            'deaths_post'] = df['prop'] * df['deaths']
        df.loc[
            df['flagged'] == 1,
            'positive_excess'] = df['deaths'] - df['deaths_post']
        # where positive excess is negative, or flagged is 0, val_post = val
        no_pe = (df['positive_excess'] < 0) | (df['positive_excess'].isnull())
        df.loc[no_pe, 'deaths_post'] = df['deaths']
        df.loc[no_pe, 'positive_excess'] = 0
        # change sepsis cause_id back to garbage now that excess is calculated
        df.loc[df['cause_id'] == self.sepsis_cause_id, 'cause_id'] = 743
        df.loc[df['cause_id'] == self.injury_cause_id, 'cause_id'] = 743
        df.loc[df['cause_id'] == self.hivrd_cause_id, 'cause_id'] = 743
        # groupby to consolidate changing the sepsis cause_id
        group_cols = [x for x in df.columns if x not in
                      ['positive_excess', 'deaths', 'deaths_post',
                       'flagged', 'prop']]
        df.drop(['prop', 'flagged'], axis=1, inplace=True)
        df = df.groupby(group_cols, as_index=False).sum()
        return df

    def get_diagnostic_dataframe(self):
        """Return evaluation of HIV correction results.

        Idea: return df with age, rate_before_hivc, rate_after_hivc
        """
        return self.diag_df

    def visualize_positive_excess_moved(self):
        """Show what happened."""
        raise NotImplementedError

    def get_hiv_epidemic_start_years(self):
        """Get years by country that HIV epidemic began."""
        raise NotImplementedError

    def get_global_relative_rates(self, location_id_list, step):
        """Get global relative mortality rates by sex for correction causes.

        These rates are calculated ahead of time, usually by combining all
        the high quality vital registration together, and saved at some
        location which is read int.

        This method fetches those rates for a particular set of locations. The
        rates can either be calculated before or after redistribution, and
        the option is provided as to which type to pull.

        Args:
            location_id_list: list of location_ids
            step: "PRE" (before redistribution) or "POST" (after)

        Returns:
            Appended relative rates for the location list
        """
        # for each location:
        #   determine filepath
        #   read in data
        # append all together
        # transform to bear bones style
        raise NotImplementedError
        # return
        return pd.DataFrame()

    def _adjust_former_soviet_countries(self, df):
        """Make a special adjustment for former soviet countries."""
        raise NotImplementedError

    def get_rates_df(self, cause_meta_df):
        """Write a nice description here."""

        if self.correct_garbage:
            filepath_infix = 'PRE'
        else:
            filepath_infix = 'POST'

        rates_path = self.conf.get_resource(
            'hivcorr_global_causespecific_relrates'
        ).format(pre_post=filepath_infix, iso=self.iso3)
        rates = pd.read_stata(rates_path)
        # convert acause to cause_id
        rates = add_cause_metadata(
            rates,
            ['cause_id'],
            merge_col='acause',
            cause_meta_df=cause_meta_df
        )
        rates.loc[
            rates['acause'] == "_sepsis_gc", 'cause_id'] = self.sepsis_cause_id
        # convert age to age_group_id
        # TODO THIS NEEDS TO BE CHANGED B/C RIGHT NOW IT QUERIES DB
        # change get_demographics to have caching option too?
        age_df = get_cod_ages()
        age_df = age_df.loc[~age_df['age_group_id'].isin([2, 3])]
        age_df['agecat'] = age_df['age_group_years_start']
        age_df.loc[age_df['age_group_id'] == 4, 'agecat'] = 0
        age_df = age_df[['agecat', 'age_group_id']]
        # merge on ages to rates data
        rates = rates.merge(age_df, on='agecat', how='left')
        assert not rates['age_group_id'].isnull().any()
        # clean up columns
        rates = rates.rename(columns={'sex': 'sex_id'})
        rates = rates.drop(['acause', 'agecat'], axis=1)
        return rates

    def get_cause_to_targets_map(self, cause_meta_df):
        """Determine how to map child causes."""
        cause_to_targets_map = {
            294: 300,
            297: 948,
            743: 298
        }
        cause_to_target_cause_map = prep_child_to_available_parent_map(
            cause_meta_df, cause_to_targets_map.keys())
        cause_to_target_cause_map['target_cause_id'] = \
            cause_to_target_cause_map['parent_cause_id'].map(
                cause_to_targets_map
        )
        cause_to_target_cause_map = \
            cause_to_target_cause_map.set_index('cause_id')[
                'target_cause_id'].to_dict()
        return cause_to_target_cause_map
