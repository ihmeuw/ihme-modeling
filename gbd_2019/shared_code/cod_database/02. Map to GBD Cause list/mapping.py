"""Map data causes to GBD causes."""

import pandas as pd
import numpy as np
from pathlib2 import Path

from cod_prep.claude.cod_process import CodProcess
from cod_prep.downloaders import (
    add_nid_metadata,
    add_code_metadata,
    add_cause_metadata,
    get_garbage_from_package,
    get_all_related_causes,
)
from cod_prep.claude.configurator import Configurator
from cod_prep.utils import report_if_merge_fail, print_log_message, distribute


class BridgeMapper(CodProcess):
    """Replace acauses with those in the bridge map.

    Arguments:
        source (str)
        cause_set_version_id (int)
        code_system (str)
    Returns:
        df, pandas DataFrame: only change is replacing some cause_ids
        diag_df, pandas DataFrame: shows which cause_ids have been changed
    """

    id_cols = ['nid', 'extract_type_id', 'location_id', 'year_id',
               'age_group_id', 'sex_id', 'cause_id',
               'site_id']
    val_cols = ['deaths', 'deaths_rd', 'deaths_corr', 'deaths_raw']

    # data type id for verbal autopsy
    VA = 8

    def __init__(self, source, cause_meta_df, code_system):
        self.source = source
        self.code_system = code_system
        self.conf = Configurator("standard")
        self.bridge_map_path = Path(self.conf.get_directory('bridge_maps'))
        self.cause_meta_df = cause_meta_df
        self.cache_options = {
            'force_rerun': False,
            'block_rerun': True,
            'cache_results': False,
            'cache_dir': 'standard'
        }

    def get_computed_dataframe(self, df):
        """Replace acauses with those in the bridge map."""
        # VA sources are the only ones where this may not work
        df = add_nid_metadata(df, ['data_type_id'], **self.cache_options)
        has_verbal_autopsy = self.VA in df['data_type_id'].unique()
        df.drop(columns='data_type_id', inplace=True)

        if self.needs_bridging(has_verbal_autopsy):
            file_name = self.get_file_name(has_verbal_autopsy)
            map_df = pd.read_csv(self.bridge_map_path / file_name)
            map_df = map_df[['acause', 'bridge_code']]

            # add acause column to deaths data
            bridge_mapped = add_cause_metadata(
                df,
                ['acause'],
                merge_col='cause_id',
                cause_meta_df=self.cause_meta_df
            )
            # hack, this cause_id snuck in somehow...
            bridge_mapped.loc[
                bridge_mapped['cause_id'] == 606, 'acause'
            ] = 'gyne_femaleinfert'
            report_if_merge_fail(bridge_mapped, 'acause', 'cause_id')
            bridge_mapped.drop(['cause_id'], axis=1, inplace=True)

            # perform zz bridge code redistribution before other bridge mapping
            bridge_mapped = self.redistribute_zz_bridge_codes(bridge_mapped, map_df)

            bridge_mapped = bridge_mapped.merge(
                map_df, how='left', on='acause'
            )
            bridge_mapped = self.acause_to_bridge_code(bridge_mapped)
            # bring cause_id back
            bridge_mapped = add_cause_metadata(
                bridge_mapped,
                ['cause_id'],
                merge_col='acause',
                cause_meta_df=self.cause_meta_df
            )

            # hack, this cause_id snuck in
            bridge_mapped.loc[
                bridge_mapped['acause'] == 'gyne_femaleinfert', 'cause_id'
            ] = 606
            report_if_merge_fail(bridge_mapped, 'cause_id', 'acause')
            # output diagnostic dataframe
            self.diag_df = bridge_mapped
            # drop unnecessary columns
            bridge_mapped = self.clean_up(bridge_mapped)
            return bridge_mapped
        else:
            self.diag_df = df
            df = self.clean_up(df)
            return df

    def needs_bridging(self, has_verbal_autopsy):
        """
        Check data type and code_system to see if the bridge map is needed.
        """
        code_systems_to_bridge_map = [
            "ICD9_detail", "ICD9_BTL", "ICD10_tabulated",
            "ICD8_detail", "ICD8A",
            "China_1991_2002", "India_SCD_states_rural", "India_MCCD_states_ICD10",
            "India_MCCD_states_ICD9", "India_SRS_states_report",
            "Russia_FMD_1989_1998", "ICD9_USSR_Tabulation", "INDEPTH_ICD10_VA",
            "India_Maharashtra_SCD", "India_CRS", "PHL_VSR_1999_2005"
        ]
        special_sources_to_bridge_map = [
            "Russia_FMD_ICD9",
            "India_SRS_states_report", "India_MCCD_Orissa_ICD10"
        ]
        # not all VA sources use a bridge map... something to think about
        # in the future, but not necessary right now
        if has_verbal_autopsy | \
            (self.code_system in code_systems_to_bridge_map) | \
            (self.source in special_sources_to_bridge_map):
            # we need to use the bridge map!
            return True
        else:
            # we do not need to use the bridge map
            return False

    def get_file_name(self, has_verbal_autopsy):
        """Determine the file name needed based on the source or code system.

        Note: The default file name will be the name of the code system,
        with some exceptions. For some sources we have specified specific
        files to bridge map with, all other sources will use the file
        that matches its code_system.
        """
        source_to_sheet = {
            "India_MCCD_Orissa_ICD10": "India_MCCD_states_ICD10",
            "India_MCCD_Delhi_ICD10": "India_MCCD_states_ICD10",
            "Thailand_Public_Health_Statistics": "ICD10_tabulated",
            "India_SRS_states_report": "India_SRS_states_report",
            "UKR_databank_ICD10_tab": "ICD10_tabulated",
            "Russia_FMD_ICD9": "Russia_FMD_1989_1998",
        }
        if has_verbal_autopsy and (self.source != 'India_SRS_states_report'):
            file_name = 'INDEPTH_ICD10_VA'
        else:
            file_name = source_to_sheet.get(self.source, self.code_system)
        return file_name + '.csv'

    def redistribute_zz_bridge_codes(self, df, map_df):
        """
        A mini-redistribution, but only redistributes causes bridge mapped to zz codes
        """
        grouping_cols = list(set(self.id_cols) - {'cause_id'})
        start_deaths = {col: df.groupby(grouping_cols)[col].sum() for col in self.val_cols}

        zz_code_idxs = map_df['bridge_code'].str.startswith('ZZ-')
        # get the order to do the zz code redistribution in:
        # start on lowest level of hierarchy and work our way up
        zz_code_targets = (map_df
                           .loc[zz_code_idxs, ['bridge_code']]
                           .drop_duplicates()
                           .assign(acause=lambda d: d['bridge_code'].str.replace('ZZ-', '_'))
                           .merge(self.cause_meta_df, on='acause')
                           .sort_values(['level', 'acause'], ascending=False)
                           .loc[:, 'bridge_code']
                           .tolist()
                           )
        # don't distribute onto anything that maps to a zz code
        all_causes_to_zz_codes = set(map_df.loc[zz_code_idxs, 'acause'])

        for zz_code in zz_code_targets:
            child_cause_ids = get_all_related_causes(zz_code.strip().replace('ZZ-', '_'),
                                                     self.cause_meta_df)
            child_causes = self.cause_meta_df.loc[
                self.cause_meta_df['cause_id'].isin(child_cause_ids),
                'acause'].tolist()

            acauses_to_redistribute = map_df.loc[map_df['bridge_code'] == zz_code, 'acause']
            to_redistribute = df['acause'].isin(acauses_to_redistribute)
            valid_child_causes = set(child_causes) - all_causes_to_zz_codes

            print_log_message('Found ZZ code: {}, deaths: {}'
                              .format(zz_code, df.loc[to_redistribute, 'deaths'].sum()))

            # distribute onto at least all combinations of these
            # this is to ensure everything in df[to_redistribute]
            # get weights
            values_to_include = {
                'acause': valid_child_causes,
            }
            for col in grouping_cols:
                values_to_include[col] = df.loc[to_redistribute, col].unique()
            distributed = distribute(df[to_redistribute],
                                     based_on=df[df['acause'].isin(valid_child_causes)],
                                     distribute_over='acause',
                                     within=grouping_cols,
                                     value_col='deaths',
                                     values_to_include=values_to_include,
                                     base_value=0.001,  # this is mostly arbitrary
                                     )
            report_if_merge_fail(distributed, check_col='acause', merge_cols=grouping_cols)

            # what follows is an unfortunate side effect of having multiple value columns
            # in the data -- it makes the merging somewhat more involved than simply
            # appending distributed data to existing data
            # TODO: refactor this into a generic method in redistribution_utils
            df = df.merge(distributed[grouping_cols + ['acause', 'deaths']],
                          how='outer',
                          on=grouping_cols + ['acause'],
                          suffixes=('', '_new'),
                          )
            # default to 0 deaths in all values where new variables / IDs (i.e. new causes)
            # are in the distributed data (right only)
            # and where distributed does not have data (i.e. other causes in original
            # data that weren't distributed onto) (left only)
            df[self.val_cols + ['deaths_new']] = df[self.val_cols + ['deaths_new']].fillna(0)
            # Set values that were distributed away from their cause to 0.
            # This has the effect of moving deaths away from one cause to another.
            df.loc[df['acause'].isin(acauses_to_redistribute), 'deaths'] = 0
            # now add distributed data to old
            df['deaths'] += df['deaths_new']
            df.drop(columns='deaths_new', inplace=True)

            # make sure deaths didn't move out of a nid-etid-site-location-year-sex-age group
            for col in self.val_cols:
                end_deaths = df.groupby(grouping_cols)[col].sum()
                assert np.allclose(start_deaths[col], end_deaths), \
                    "Dropped/added deaths during ZZ code redistribution: " + \
                    "start {}: {}, end {}: {}".format(col, start_deaths[col], col, end_deaths)
        return df

    def acause_to_bridge_code(self, df):
        """Replace the acause with the bridge code."""
        # there might still be zz codes in the data because we aren't
        # performing zz code redistribution on the other value columns,
        # so if something is coded to i.e. _neo in the raw data, then
        # we keep it as _neo.
        df['acause'].update(df['bridge_code'].str.replace('ZZ-', '_'))
        return df

    def get_diagnostic_dataframe(self):
        """Return a diagnostic dataframe.

        Diagnostic dataframe shows all changes made due to bridge mapping.
        Maybe change this later to there is some sort of output.
        """
        if self.diag_df is None:
            print("No run of get computed dataframe yet")
        else:
            return self.diag_df

    def clean_up(self, df):
        """Group rogue duplicates."""
        df = df.groupby(self.id_cols, as_index=False)[self.val_cols].sum()
        return df


class GBDCauseMapper(CodProcess):
    """Convert cause codes into cause_ids.

    Arguments:
        id_cols (list):
        data_col (list):
        unique_cols (list):
    Returns:
        df, a pandas DataFrame with addition of cause_id
        diag_df, a pandas DataFrame: assesses the difference
        between different mapping versions
    """

    id_cols = ['nid', 'extract_type_id', 'location_id', 'year_id',
               'age_group_id', 'sex_id', 'cause_id', 'code_id',
               'site_id']
    data_col = ['deaths']
    unique_cols = ['nid', 'extract_type_id', 'location_id', 'year_id',
                   'age_group_id', 'sex_id',
                   'cause_id', 'code_id', 'site_id']
    # These are acauses 'sub_total', and '_sb'
    unnecessary_causes = [920, 744]
    cache_dir = str()

    def __init__(self, cause_set_version_id, code_map):
        self.cg = Configurator("standard")
        self.cache_dir = self.cg.get_directory('db_cache')
        self.cause_set_version_id = cause_set_version_id
        self.code_map = code_map

    def get_computed_dataframe(self, df, code_system_id):

        # make special cause adjustments
        df = self.special_cause_reassignment(df, code_system_id)

        """Map code id to cause id."""
        print_log_message("Merging with cause map")
        # get code metadata from a file already cached
        df = add_code_metadata(
            df, ['cause_id'], code_system_id,
            code_map=self.code_map
        )
        report_if_merge_fail(df, 'cause_id', 'code_id')

        # Make sure the mappings are good!
        print("Asserting it's all good")
        self.assert_valid_mappings(df, code_system_id)
        df = self.drop_unnecessary_causes(df, self.unnecessary_causes)
        print("Collapsing")
        df = self.collapse_and_sum_by_deaths(df)
        return df

    def drop_unnecessary_causes(self, df, unnecessary_causes):
        # Drops causes set as unnecessary, subtotal and stillbirth
        df = df.copy()
        df = df[~df['cause_id'].isin(unnecessary_causes)]
        return df

    def special_cause_reassignment(self, df, code_system_id):
        """Replace the actual data cause under certain conditions.

        There are instances where a PI has good reason to
        believe that a certain group of deaths were assigned
        to the wrong cause, and it is known what cause to re-assign
        those deaths to. Implement here.

        This essentially allows mapping based on not just the cause
        and code system but based on other information like
        the location, NID, year, etc.

        It can also be used (sparingly) for hotfixes like
        changing all codes with values 'acause_digest_gastrititis'
        to be named 'acause_digest_gastritis'.

        Args:
            df (DataFrame): data with cause

        Returns:
            DataFrame: with any modifications
        """

        cache_args = {
            'force_rerun': False,
            'block_rerun': True,
            'cache_dir': 'standard',
            'cache_results': False
        }
        # Some SRS codes get redistributed differently than
        # other ICD10 datasets
        df = add_nid_metadata(
            df, 'source', **cache_args
        )

        if (df['source'] == "India_SRS_states_report").any():
            print_log_message("Changing SRS codes to custom garbage groups")
            assert (df['source'] == "India_SRS_states_report").all()

            df = add_code_metadata(
                df, 'value', code_system_id=code_system_id,
                **cache_args
            )

            custom_grbg = pd.read_csv(
                self.cg.get_resource("srs_custom_garbage_groups")
            )
            custom_grbg = custom_grbg.query('active == 1')
            custom_grbg['value'] = custom_grbg['srs_custom_garbage_group']
            custom_grbg = add_code_metadata(
                custom_grbg, 'code_id', code_system_id=code_system_id,
                merge_col='value', **cache_args
            )
            custom_grbg = custom_grbg.rename(
                columns={'code_id': 'new_code_id'})
            custom_grbg = custom_grbg[['package_id', 'new_code_id']]

            gp_dfs = []
            for package_id in custom_grbg.package_id.unique():
                # THIS QUERIES THE DATABASE - BUT THERE SHOULD NEVER BE A TON
                # OF SRS JOBS HAPPENING AT ONCE SO IT SHOULD BE OK
                gp_df = get_garbage_from_package(
                    code_system_id, package_id, package_arg_type="package_id"
                )
                assert len(gp_df) != 0, \
                    "Found 0 codes for package {}".format(package_id)
                gp_dfs.append(gp_df)
            gp_df = pd.concat(gp_dfs, ignore_index=True)

            gp_df = gp_df.merge(custom_grbg, how='left')
            report_if_merge_fail(gp_df, 'new_code_id', 'package_id')
            gp_df = gp_df[['value', 'new_code_id']]
            gp_df['value'] = gp_df['value'].str.strip()

            df = df.merge(gp_df, how='left', on='value')
            df.loc[df['new_code_id'].notnull(), 'code_id'] = df['new_code_id']
            df['code_id'] = df['code_id'].astype(int)
            df = df.drop(['new_code_id', 'value'], axis=1)

        df = df.drop('source', axis=1)

        china_cdc_2008 = (df['nid'] == 270005) & (df['extract_type_id'] == 2)
        # J96.00 - move five to four digit J96.0 (this should be a rule in formatting, only keep 4 digit detail)
        five_dig_code = df['code_id'] == 13243
        df.loc[
            china_cdc_2008 & five_dig_code,
            'code_id'
        ] = 13242

        return df

    def collapse_and_sum_by_deaths(self, df):
        """Group by final columns, summing across deaths.

        Directly modifies the dataframe, keeping only the columns needed
        to move on to the next Claude step. Also includes an assertion
        that there are no duplicates.
        """
        df = df.groupby(self.id_cols, as_index=False)[self.data_col].sum()
        self.assert_unique_cols_unique(df)
        return df

    def assert_valid_mappings(self, df, code_system_id):
        """Test that the mapping worked.

        Runs a suite of assertions to make sure that mapping was successful.
        Args:
            df (DataFrame): with at least code_id and cause_id
        Returns:
            None
        Raises:
            AssertionError: Any condition fails
        """
        # add code value from cached code map
        print("Adding value")
        df = add_code_metadata(
            df, ['value'], code_system_id,
            force_rerun=False,
            block_rerun=True,
            cache_dir=self.cache_dir
        )
        report_if_merge_fail(df, 'value', 'code_id')
        # get acause from cached cause hierarchy
        print("Adding acause")
        df = add_cause_metadata(
            df, ['acause'],
            cause_set_version_id=self.cause_set_version_id,
            force_rerun=False,
            block_rerun=True,
            cache_dir=self.cache_dir
        )
        report_if_merge_fail(df, 'acause', 'cause_id')

        # Test that all causes starting with 'acause_' are mapped correctly.
        # acause_cvd, for example, should be mapped to 'cvd' (not 'cvd_ihd').
        # 'acause__gc_X59' should be mapped to '_gc', etc.
        print("Checking implied acauses")
        check_df = df.loc[df['value'].str.startswith('acause_')]
        check_df['implied_acause'] = \
            check_df['value'].str.replace('acause_', '', 1)

        check_df.loc[
            check_df['value'].str.contains("acause__gc"),
            'implied_acause'
        ] = "_gc"
        bad_df = check_df.loc[
            check_df['acause'] != check_df['implied_acause']
        ]
        if len(bad_df) > 0:
            bad_stuff = bad_df[['value', 'acause']].drop_duplicates()
            raise AssertionError(
                "These code values do not match their acause: "
                "\n{}".format(bad_stuff)
            )

        print("Checking for bad values")
        # assert incorrect acauses are gone
        bad_acauses = ['acause_digest_gastrititis',
                       'acause_hiv_tb',
                       'acause_tb_drug']

        bad_df = df.loc[df['value'].isin(bad_acauses)].value.unique()
        if len(bad_df) > 0:
            raise AssertionError(
                "Found these bad code values in the data: {}".format(bad_stuff)
            )

    def assert_unique_cols_unique(self, df):
        """Test that columns that should uniquely identify the dataframe do."""
        assert not df.duplicated(self.unique_cols).any()