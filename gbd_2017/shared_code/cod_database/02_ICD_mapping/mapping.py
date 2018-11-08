"""Map data causes to GBD causes."""

import pandas as pd
from cod_prep.claude.cod_process import CodProcess
from cod_prep.downloaders import (
    add_nid_metadata,
    add_code_metadata,
    add_cause_metadata,
    get_garbage_from_package
)
from cod_prep.claude.configurator import Configurator
from cod_prep.utils import report_if_merge_fail, print_log_message


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
        self.bridge_map_path = self.conf.get_resource('bridge_map')
        self.cause_meta_df = cause_meta_df
        self.cache_options = {
            'force_rerun': False,
            'block_rerun': True,
            'cache_results': False,
            'cache_dir': 'standard'
        }

    def get_computed_dataframe(self, df):
        """Replace acauses with those in the bridge map."""
        df = add_nid_metadata(df, ['data_type_id'], **self.cache_options)
        has_verbal_autopsy = self.VA in df['data_type_id'].unique()

        if self.needs_bridging(has_verbal_autopsy):
            sheet_name = self.get_sheet_name(has_verbal_autopsy)
            map_df = pd.read_excel(self.bridge_map_path, sheetname=sheet_name)
            map_df = map_df[['acause', 'bridge_code']]

            # add acause column to deaths data
            bridge_mapped = add_cause_metadata(
                df,
                ['acause'],
                merge_col='cause_id',
                cause_meta_df=self.cause_meta_df
            )

            bridge_mapped.loc[
                bridge_mapped['cause_id'] == 606, 'acause'
            ] = 'gyne_femaleinfert'
            report_if_merge_fail(bridge_mapped, 'acause', 'cause_id')
            bridge_mapped.drop(['cause_id'], axis=1, inplace=True)
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
        """Check data type and source to see if the bridge map is needed."""
        sources_to_bridge_map = [
            "India_SCD_states_rural", "India_CRS",
            "India_MCCD_states_ICD9", "India_MCCD_states_ICD10",
            "India_Maharashtra_SCD", "India_MCCD_Orissa_ICD10",
            "India_MCCD_Delhi_ICD10", "ICD9_BTL", "Russia_FMD_1989_1998",
            "China_1991_2002", "ICD9_USSR_Tabulation", "ICD10_tabulated",
            "Thailand_Public_Health_Statistics", "India_SRS_states_report",
            "ICD8A", "UKR_databank_ICD10_tab", "Russia_FMD_ICD9",       
        ]

        if has_verbal_autopsy | (self.source in sources_to_bridge_map):
            return True
        else:
            return False

    def get_sheet_name(self, has_verbal_autopsy):
        """Determine the sheet name needed based on the source.
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
            sheet_name = 'INDEPTH_ICD10_VA'
        elif self.source in source_to_sheet.keys():
            sheet_name = source_to_sheet[self.source]
        else:
            sheet_name = self.code_system
        return sheet_name

    def acause_to_bridge_code(self, df):
        """Replace the acause with the bridge code."""
        df['swap'] = 0
        df.loc[
            (df['acause'] != df['bridge_code']) &
            (df['bridge_code'].notnull()),
            'swap'
        ] = 1
        df.loc[df['swap'] == 1, 'acause'] = df['bridge_code']
        self.causes_not_in_bridge_map(df)
        return df

    def causes_not_in_bridge_map(self, df):
        """Print causes that aren't in the bridge map, but are in the data."""
        check = set(df.loc[df['bridge_code'].isnull(), 'acause'])
        if len(check) > 0:
            print("These acauses are not in the bridge map: {}".format(check))

    def get_diagnostic_dataframe(self):
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
        self.cache_dir = self.cg.get_directory('FILEPATH')
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

        This essentially allows mapping based on not just the cause
        and code system but based on other information like
        the location, NID, year, etc.

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
        
        five_dig_code = df['code_id'] == 13243
        df.loc[
            china_cdc_2008 & five_dig_code,
            'code_id'
        ] = 13242

        return df

    def collapse_and_sum_by_deaths(self, df):
        """Group by final columns, summing across deaths.
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
