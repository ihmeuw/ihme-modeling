import pandas as pd

from cod_prep.claude.cod_process import CodProcess
from cod_prep.claude.configurator import Configurator
from cod_prep.downloaders import (
    prep_child_to_available_parent_map,
    add_population,
    add_cause_metadata,
    get_cod_ages,
    add_code_metadata,
    get_package_list
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

    maxcorrect = 70

    reference_ages = {
        294: [18, 19, 20],
        297: [16, 17, 18],
        545: [17, 18],
        366: [12, 13, 14],
    }

    move_gc_age_limits = {
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
                                     self.move_gc_age_limits,
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
                                      self.reference_ages,
                                      self.loc_meta_df,
                                      self.age_meta_df,
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
        if code_system_id in [1, 6]:
            # Find the sepsis package
            pkg_list = get_package_list(
                code_system_id, include_garbage_codes=True,
                force_rerun=False, block_rerun=True, cache_results=False,
                cache_dir=self.cache_dir)
            sepsis_df = pkg_list.loc[pkg_list.package_description.str.contains('[Ss]epsis')]
            assert len(sepsis_df.package_id.unique()) == 1, f'Need exactly 1 sepsis package'
            sepsis_df = sepsis_df.rename(columns={'garbage_code': 'value'})[['value']]
            # may need some more things for formatting, but basic idea:
            df = df.merge(sepsis_df, on=['value'],
                          how='left', indicator=True)
            # change the cause_id to septicemia
            df.loc[df['_merge'] == 'both', 'cause_id'] = self.sepsis_cause_id
            df.drop(['_merge'], axis=1, inplace=True)
        return df

    def identify_hivrd_gc(self, df, code_system_id):
        pkg_list = get_package_list(
            code_system_id, include_garbage_codes=True,
            force_rerun=False, block_rerun=True, cache_results=False,
            cache_dir=self.cache_dir)
        hivrd_df = pkg_list.loc[pkg_list.package_name.str.startswith('HIV-CORR')]
        hivrd_df = hivrd_df.rename(columns={'garbage_code': 'value'})[['value']]
        df = df.merge(hivrd_df, on=['value'], how='left', indicator=True)
        df.loc[df['_merge'] == 'both', 'cause_id'] = self.hivrd_cause_id
        df.drop(['_merge'], axis=1, inplace=True)
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
        no_pe = (df['positive_excess'] < 0) | (df['positive_excess'].isnull())
        df.loc[no_pe, 'deaths_post'] = df['deaths']
        df.loc[no_pe, 'positive_excess'] = 0
        df.loc[df['cause_id'] == self.sepsis_cause_id, 'cause_id'] = 743
        df.loc[df['cause_id'] == self.injury_cause_id, 'cause_id'] = 743
        df.loc[df['cause_id'] == self.hivrd_cause_id, 'cause_id'] = 743
        group_cols = [x for x in df.columns if x not in
                      ['positive_excess', 'deaths', 'deaths_post',
                       'flagged', 'prop']]
        df.drop(['prop', 'flagged'], axis=1, inplace=True)
        df = df.groupby(group_cols, as_index=False).sum()
        return df

    def get_diagnostic_dataframe(self):
        return self.diag_df

    def get_rates_df(self, cause_meta_df):

        pre_or_post = "pre" if self.correct_garbage else "post"
        rates_path = self.conf.get_resource(
            f'hiv_correction_relative_rates_{pre_or_post}'
        ).format(iso=self.iso3)
        rates = pd.read_stata(rates_path)
        # Use relative rates of tb_other for all its children
        cause_duplication = (
            cause_meta_df.query('acause_parent in ["tb_other", "digest_ibd"]')
            .rename(columns={'acause_parent': 'acause', 'acause': 'acause_child'})
            .loc[:, ['acause', 'acause_child']]
        )
        rates = rates.append(
            rates.merge(cause_duplication)
            .assign(acause=lambda d: d['acause_child'])
            .drop(columns='acause_child'),
            sort=False,
        )
        rates.replace({"acause": {"digest_gastrititis": "digest_gastritis"}}, inplace=True)
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
        age_df = get_cod_ages(force_rerun=False, block_rerun=True, cache_results=False)
        age_df = age_df.loc[~age_df['age_group_id'].isin([2, 3, 388])]
        age_df['agecat'] = age_df['age_group_years_start']
        age_df['agecat'].update(age_df['age_group_id'].map({389: 0, 238: 1, 34: 1}))
        # merge on ages to rates data
        age_df = age_df[['agecat', 'age_group_id']]
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
            cause_meta_df, list(cause_to_targets_map.keys()))
        cause_to_target_cause_map['target_cause_id'] = \
            cause_to_target_cause_map['parent_cause_id'].map(
                cause_to_targets_map
        )
        cause_to_target_cause_map = \
            cause_to_target_cause_map.set_index('cause_id')[
                'target_cause_id'].to_dict()
        return cause_to_target_cause_map
