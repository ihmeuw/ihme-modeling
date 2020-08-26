import pandas as pd
import numpy as np
from cod_process import CodProcess
from cod_prep.downloaders.locations import add_location_metadata
from cod_prep.downloaders.causes import add_cause_metadata, get_all_related_causes
from cod_prep.claude.configurator import Configurator
from cod_prep.utils.misc import print_log_message
from cod_prep.utils import report_if_merge_fail


class EnvelopeLocationSplitter(CodProcess):
    """Used to create Telangana from Andhra Pradesh."""

    def __init__(self, df, env_meta_df, source):
        self.df = df
        self.env_meta_df = env_meta_df
        self.source = source
        self.orig_cols = df.columns
        # dictionary of split_type: [original_id, new_id]
        # e.g. urban: [ap_urban_id, telangana_urban_id]
        self.split_ids = {"UKR": [63, 50559, 44934, 44939],
                          "urban": [43872, 43902],
                          "rural": [43908, 43938]}

        # last year of Ukraine data requiring a split
        self.ukr_year = 2014

        # sources needing to be split for India
        self.ind_sources = [
            "India_SCD_states_rural", "India_MCCD_states_ICD9",
            "India_MCCD_states_ICD10", "India_SRS_states_report"
        ]

    def needs_splitting(self):
        """Check if the data needs to be adjusted."""
        data_loc_ids = list(self.df.location_id.unique())

        # Ukraine data in ICD9_USSR_Tab
        if (self.split_ids["UKR"][0] in data_loc_ids) and \
                (self.df.year_id.max() <= self.ukr_year) and \
                self.source != "Cancer_Registry":
            return "UKR"
        elif self.source in (self.ind_sources):
            return "IND"
        else:
            return None

    def prep_envelope(self, split_type):
        """Prepare envelope dataframe."""
        env_df = self.env_meta_df.loc[
            self.env_meta_df['location_id'].isin(self.split_ids[split_type])
        ]
        env_wide = env_df.pivot_table(
            columns='location_id',
            index=['age_group_id', 'year_id', 'sex_id'],
            values='mean_env',
        )
        env_wide.columns.name = None
        env_wide.reset_index(inplace=True)
        return env_wide

    def adjust_ap_telangana(self, orig_id, new_id, env_df, df_ap):
        """Adjust sample size for andhra pradesh/telangana split."""
        env_df['prop_ap'] = env_df[orig_id] / (env_df[new_id] + env_df[orig_id])
        env_df['prop_tg'] = 1 - env_df['prop_ap']
        df_ap = df_ap.merge(
            env_df, on=['age_group_id', 'year_id', 'sex_id'], how='left'
        )

        # copy cfs and rename location to telangana
        df_tg = df_ap.copy()
        df_tg['location_id'] = new_id

        # adjust sample sizes
        df_tg['sample_size'] = df_tg['sample_size'] * df_tg['prop_tg']
        df_ap['sample_size'] = df_ap['sample_size'] * df_ap['prop_ap']

        df = pd.concat([df_ap, df_tg], ignore_index=True)

        return df

    def adjust_ukr(self, env_df, split_type):
        """Adjust sample size for Ukraine without Crimea and Sevastopol."""
        orig_id = self.split_ids[split_type][0]
        no_cs = self.split_ids[split_type][1]
        crimea = self.split_ids[split_type][2]
        sev = self.split_ids[split_type][3]

        env_df['prop_no_cs'] = env_df[no_cs] / env_df[orig_id]
        env_df['prop_crimea'] = env_df[crimea] / env_df[orig_id]
        env_df['prop_sev'] = env_df[sev] / env_df[orig_id]
        df = self.df.merge(
            env_df, on=['age_group_id', 'year_id', 'sex_id'], how='left'
        )
        report_if_merge_fail(df, 'prop_no_cs', ['age_group_id', 'year_id', 'sex_id'])

        # resulting dataset will have Ukraine without Crimea and Sevastopol,
        # Crimea, and Sevastopol location_ids
        no_cs_df = df.copy()
        no_cs_df['sample_size'] = no_cs_df['prop_no_cs'] * no_cs_df['sample_size']
        no_cs_df['location_id'] = no_cs

        crimea_df = df.copy()
        crimea_df['sample_size'] = crimea_df['prop_crimea'] * crimea_df['sample_size']
        crimea_df['location_id'] = crimea

        sev_df = df.copy()
        sev_df['sample_size'] = sev_df['prop_crimea'] * sev_df['sample_size']
        sev_df['location_id'] = sev

        df = pd.concat([no_cs_df, crimea_df, sev_df], ignore_index=True)

        return df

    def get_computed_dataframe(self):
        """Return computations."""
        split_type = self.needs_splitting()
        if not split_type:
            print_log_message("No location splitting required.")
            self.diag_df = None
            return self.df

        # quick check that sample size has been created
        assert 'sample_size' in self.df.columns

        # grab total deaths of incoming data
        start_deaths = (self.df.sample_size * self.df.cf).sum()

        if split_type == "UKR":
            # prep envelope df
            env_wide = self.prep_envelope(split_type)

            # split data
            df = self.adjust_ukr(env_wide, split_type)

            # append ukraine w/o crimea/sevastopol to incoming data
            # national data not modeled, but needed for CodViz
            df = pd.concat([df, self.df], ignore_index=True)

            # expected result is national data + national w/o crimea/sevastopol
            # plus crimea, plus sevastopol
            end_deaths = (df.sample_size * df.cf).sum()
            assert np.isclose(
                (end_deaths / start_deaths), 2, atol=0.05
            )
        else:
            df_list = []
            for split_type in ["urban", "rural"]:
                # prep envelope df
                env_wide = self.prep_envelope(split_type)

                # get location_ids to split
                orig_id = self.split_ids[split_type][0]
                new_id = self.split_ids[split_type][1]

                # separate out andhra pradesh data
                df_ap = self.df.loc[self.df['location_id'] == orig_id]

                # do location splittling
                df = self.adjust_ap_telangana(orig_id, new_id, env_wide, df_ap)

                df_list.append(df)

            # remove ap from incoming data
            ap_ids = [self.split_ids["urban"][0]] + [self.split_ids["rural"][0]]
            df_no_ap = self.df.loc[~(self.df['location_id'].isin(ap_ids))]
            df_list.append(df_no_ap)

            df = pd.concat(df_list, ignore_index=True)

            # compare start and end deaths
            end_deaths = (df.sample_size * df.cf).sum()
            assert np.isclose(start_deaths, end_deaths, rtol=0.001)

        # make sure not to add any new columns
        df = df[self.orig_cols]

        return df

    def get_diagnostic_dataframe(self):
        """Return diagnostics."""
        pass


class FractionFixer(CodProcess):
    """Used for crazy RTI adjustment that should probably be removed"""

    def __init__(self, df):
        self.df = df

    def get_computed_data_frame(self):
        """Return computations"""
        raise NotImplementedError

    def get_diagnostic_dataframe(self):
        """Return diagnostics"""
        pass


class InjuryRedistributor(CodProcess):
    """Used to redistribute injuries in South Africa."""

    def __init__(self, df, loc_meta_df, cause_meta_df):
        self.df = df
        self.start_deaths = self.df['deaths'].sum()
        self.loc_meta_df = loc_meta_df
        self.cause_meta_df = cause_meta_df
        self.conf = Configurator("standard")

    def get_computed_dataframe(self):
        self.set_injury_cause_list()
        self.set_iso3_on_data()
        inj_df = self.get_injury_df(self.df)
        sans_inj_df = self.df[~self.df['cause_id'].isin(self.injury_cause_list)]

        # We have to prepare two datasets to make proportions
        inj_by_iso_sex_year = self.prep_deaths_by_iso_sex_year(inj_df)
        inj_props = self.prep_injury_proportions_file()
        props_with_deaths = inj_props.merge(inj_by_iso_sex_year,
                                            on='sex_id', how='left')
        props_with_deaths[
            'deaths'
        ] = props_with_deaths['deaths'] * props_with_deaths['prop']
        props_with_deaths = props_with_deaths.drop('prop', axis=1)
        inj_env = props_with_deaths.rename(columns={'deaths': 'inj_env'})

        # Replace poisoning and sucidie childs with parent
        inj_df = self.replace_poisoning_and_suicide(inj_df)
        # Get age pattern
        age_pattern_df = self.get_age_pattern_df(inj_df)
        inj_df = age_pattern_df.merge(inj_env, on=['year_id', 'iso3',
                                                   'sex_id', 'cause_id'],
                                      how='left', indicator=True)
        # If the merge failed, then there is no envelope and we do not want to
        # redistribute any deaths onto this cause
        inj_df.loc[inj_df.inj_env.isnull(), 'inj_env'] = 0
        inj_df['deaths'] = inj_df['prop'] * inj_df['inj_env']
        result = sans_inj_df.append(inj_df, ignore_index=True)
        assert np.isclose(self.start_deaths, result.deaths.sum())
        result.drop(['_merge', 'inj_env', 'prop', 'iso3'], axis='columns', inplace=True)
        assert result.notnull().values.all()
        return result

    def replace_poisoning_and_suicide(self, df):
        df = add_cause_metadata(df, 'acause',
                                    cause_meta_df=self.cause_meta_df)
        inj_poison = self.cause_meta_df[
            self.cause_meta_df.acause == "inj_poisoning"
        ]['cause_id'].unique()[0]
        inj_suicide = self.cause_meta_df[
            self.cause_meta_df.acause == "inj_suicide"
        ]['cause_id'].unique()[0]
        df.loc[
            df['acause'].str.startswith("inj_poison"), 'cause_id'
        ] = inj_poison
        df.loc[
            df['acause'].str.startswith("inj_suicide"), 'cause_id'
        ] = inj_suicide
        df = df[~df['acause'].isin(['inj_homicide', 'inj_trans_road'])]
        df = df.drop("acause", axis=1)
        return df

    def get_age_pattern_df(self, df):
        df = df.groupby([col for col in df.columns if col not in ['deaths']],
                        as_index=False)['deaths'].sum()
        df['all_age_total'] = df.groupby(['sex_id', 'year_id', 'cause_id', 'location_id'])['deaths'].transform(sum)
        df['prov_total'] = df.groupby(['sex_id', 'year_id', 'cause_id'])['deaths'].transform(sum)
        df['prov_split'] = df['all_age_total'] / df['prov_total']
        df['prop'] = (df['deaths'] / df['all_age_total']) * df['prov_split']
        df = df.drop(['prov_split', 'prov_total', 'all_age_total', 'deaths'], axis=1)
        return df

    def prep_injury_proportions_file(self):
        filepath = self.conf.get_resource('injury_proportions')
        inj_props = pd.read_csv(filepath)
        inj_props = inj_props[inj_props['most_detailed'] == 1]
        inj_props = inj_props[['acause', 'rdp2', 'rdp1']]
        inj_props = add_cause_metadata(inj_props,
                                       'cause_id',
                                       merge_col='acause',
                                       cause_meta_df=self.cause_meta_df)
        inj_props = inj_props.loc[inj_props['cause_id'].notnull()]
        inj_props = inj_props.drop('acause', axis=1)
        inj_props = pd.melt(inj_props, id_vars=['cause_id'],
                            var_name='sex_id', value_name='prop')
        inj_props['sex_id'] = inj_props['sex_id'].apply(lambda x: x[3]).astype(int)
        inj_props['total_prop'] = inj_props.groupby('sex_id')['prop'].transform(sum)
        inj_props['prop'] = inj_props['prop'] / inj_props['total_prop']
        inj_props = inj_props.drop('total_prop', axis=1)
        return inj_props

    def set_iso3_on_data(self):
        self.df = add_location_metadata(self.df, 'ihme_loc_id',
                                        location_meta_df=self.loc_meta_df)
        self.df['iso3'] = self.df['ihme_loc_id'].apply(lambda x: x[0:3])
        self.df = self.df.drop('ihme_loc_id', axis=1)

    def set_injury_cause_list(self):
        inj_causes = self.cause_meta_df[
            self.cause_meta_df['acause'].str.startswith("inj")
        ]
        self.injury_cause_list = list(inj_causes['cause_id'].unique())

    def get_injury_df(self, df):
        df = df[df.cause_id.isin(self.injury_cause_list)]
        return df

    def prep_deaths_by_iso_sex_year(self, df):
        df = df[['iso3', 'year_id', 'sex_id', 'deaths']]
        df = df.groupby(['iso3', 'year_id',
                         'sex_id'], as_index=False)['deaths'].sum()
        return df


class LRIRedistributor(CodProcess):
    """Used to redistribute a proportion of LRI deaths to tb_other"""
    lri_ages = [4, 5, 6, 7]
    id_cols = ['nid', 'extract_type_id', 'location_id', 'year_id',
                'age_group_id', 'sex_id', 'site_id', 'cause_id']

    def __init__(self, df, cause_meta_df):
        self.df = df
        self.start_deaths = self.df.deaths.sum()
        self.conf = Configurator('standard')
        self.cause_meta_df = cause_meta_df

    def get_computed_dataframe(self):
        # standardize the input data - anything that went through the injury corrector
        # has extra columns we don't need
        df = self.df[self.id_cols + ['deaths']]

        # use the lri deaths in the data and the proportions to create adjustment df
        adjust_df = self.get_adjust_df(df)
        # if no lri to move, exit
        if not len(adjust_df) > 0:
            return df
        moved_deaths = adjust_df.death_adjustment.sum()

        # scale down the existing lri deaths
        df = self.adjust_lri(df, adjust_df)

        # use the deaths we took from lri to scale up tb_other
        df = self.adjust_tb(df, adjust_df)

        # assert deaths unchanged and cleanup
        df = df[self.id_cols + ['deaths']]
        assert df.notnull().values.all()
        df = df.groupby(self.id_cols, as_index=False).deaths.sum()
        assert np.allclose(df.deaths.sum(), self.start_deaths), \
            'Total deaths have changed during LRI redistribution'
        print_log_message("Moved " + str(moved_deaths) + " from LRI to tb_other")
        return df

    def get_adjust_df(self, df):
        lri_parent = self.cause_meta_df.loc[
            self.cause_meta_df.acause == 'lri'
        ]['cause_id'].unique()[0]
        lri_causes = get_all_related_causes(lri_parent, cause_meta_df=self.cause_meta_df)
        df = df.loc[
            (df.cause_id.isin(lri_causes)) &
            (df.age_group_id.isin(self.lri_ages))
        ]
        # if no lri to move, exit
        if not len(df) > 0:
            return df
        # read in lri/tb redistribution proportions, proportions
        # are location/year specific and apply only to under 15, excluding 0-27 days
        prop_df = pd.read_csv(self.conf.get_resource('lri_tb_proportions'))
        prop_df = prop_df[['location_id', 'year_id', 'tb_prop']]
        df = df.merge(prop_df,
            on=['location_id', 'year_id'],
            how='left'
        )
        report_if_merge_fail(df, 'tb_prop', ['location_id', 'year_id'])
        df['death_adjustment'] = df['deaths'] * df['tb_prop']
        df = df[self.id_cols + ['death_adjustment']]
        return df

    def adjust_lri(self, df, adjust_df):
        df = df.merge(adjust_df,
            on=self.id_cols,
            how='left'
        )
        df['death_adjustment'] = df['death_adjustment'].fillna(0)
        df['deaths'] = df['deaths'] - df['death_adjustment']
        df.drop('death_adjustment', axis=1, inplace=True)
        return df

    def adjust_tb(self, df, adjust_df):
        # set adjust df cause id to 934, tb_other, and collapse deaths
        # we will add all the lri deaths to tb_other in the main df
        tb_other = self.cause_meta_df.loc[
            self.cause_meta_df.acause == 'tb_other'
        ]['cause_id'].unique()[0]
        adjust_df['cause_id'] = tb_other
        adjust_df = adjust_df.groupby(self.id_cols,
                as_index=False).death_adjustment.sum()
        # merge on to the data, posibility of creating new demographics for tb_other
        # that were not yet present in the data (the right only merges)
        df = df.merge(adjust_df,
            on=self.id_cols,
            how='outer',
            indicator=True
        )
        df.loc[df._merge == 'both', 'deaths'] = df['deaths'] + df['death_adjustment']
        df.loc[df._merge == 'right_only', 'deaths'] = df['death_adjustment']
        return df

