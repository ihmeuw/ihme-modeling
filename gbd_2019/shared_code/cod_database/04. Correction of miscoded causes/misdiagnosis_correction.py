import os

import pandas as pd
import numpy as np
from scipy.special import logit

from cod_process import CodProcess
from configurator import Configurator
from cod_prep.downloaders import (
    add_code_metadata, get_clean_package_map_for_misdc, get_cause_map, add_location_metadata,
    get_package_map, remove_five_plus_digit_icd_codes
)
from cod_prep.utils import clean_icd_codes, print_log_message, report_if_merge_fail


class MisdiagnosisCorrector(CodProcess):
    conf = Configurator()
    block_rerun = {'force_rerun': False, 'block_rerun': True}
    process_dir = conf.get_directory('mc_process_data')
    mcod_prob_path = conf.get_resource('misdiagnosis_prob_path')
    cc_code = 919

    # The MCoD proportions and empirical limits were produced in GBD 2017 - remap deleted packages
    # to their closest GBD 2019 equivalents
    # For deleted packages whose codes were split into multiple other existing packages, allow
    # these codes to use the proportions for their new packages
    # NOTE: there are proportions for several packages that are not used due to
    # code system/package mismatches or overlaps in garbage codes between packages
    package_remap = {
        '_p_118': '_p_4133',
        '_p_119': '_p_4133',
        '_p_97': '_p_4133',
        '_p_98': '_p_4133',
        '_p_3148': '_p_4138',
        '_p_3149': '_p_4138',
        '_p_3209': '_p_4138',
        '_p_192': '_p_4139',
        '_p_204': '_p_3715',
        '_p_230': '_p_4134',
        '_p_251': '_p_4134',
        '_p_3673': '_p_4153',
        '_p_63': '_p_4153',
        '_p_64': '_p_4153',
        '_p_65': '_p_4153',
        '_p_66': '_p_4153',
        '_p_67': '_p_4153',
        '_p_84': '_p_4158',
        '_p_85': '_p_4158',
        '_p_91': '_p_4158',
        '_p_195': '_p_190',
        '_p_200': '_p_190',
        '_p_223': '_p_222',
        '_p_4038': '_p_4035',
        '_p_3822': '_p_2119'
    }

    # renal failure in ICD10 was formerly one package, but now is split into two
    # <old package with existing props/limits>: <new package that needs mcod props/limits>
    package_split = {
        '_p_106': '_p_4143'
    }

    def __init__(self, nid, extract_type_id, code_system_id, code_map_version_id,
                 adjust_id, remove_decimal):
        self.nid = nid
        self.extract_type_id = extract_type_id
        self.code_system_id = code_system_id
        self.code_map_version_id = code_map_version_id
        self.adjust_id = adjust_id
        self.remove_decimal = remove_decimal
        self.dismod_dir = self.conf.get_directory('misdc_dismod_dir').format(
            adjust_id=self.adjust_id)
        self.dem_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id',
                         'site_id', 'nid', 'extract_type_id']
        if adjust_id == 543:
            self.misdiagnosis_version_id = 4
        else:
            self.misdiagnosis_version_id = 3
        self.misdiagnosis_path = self.mcod_prob_path.format(
            adjust_id=self.adjust_id,
            version_id=self.misdiagnosis_version_id,
            code_system_id=self.code_system_id
        )

    def store_intermediate_data(self, df, move_df, orig_df):
        '''Write intermediate files for tracking deaths moved and for uncertainty.'''
        if len(move_df) > 0:
            id_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id']
            # do some aggregation
            df = df.groupby(['nid', 'extract_type_id', 'site_id', 'cause_id'] + id_cols,
                            as_index=False).deaths.sum()
            ss_df = self.get_ss(df)

            # get draws
            draws_df = self.load_dismod(location_id=list(df.location_id.unique()),
                                        year_id=list(df.year_id.unique()), file_name='best_draws')
            draw_cols = [d for d in list(draws_df) if 'draw_' in d]

            # scale draws to match mean of deaths moved
            draws_df = draws_df[
                id_cols + draw_cols + ['est_frac', 'population', 'est_mx']
            ].merge(move_df.loc[move_df.map_id == str(self.adjust_id)], how='right')
            draws_df = draws_df.merge(ss_df)
            draws_df['added_scalar'] = draws_df['misdiagnosed_scaled'] / \
                (draws_df['est_frac'] * draws_df['sample_size'])
            draws_df['completeness_scalar'] = draws_df['sample_size'] / \
                (draws_df['est_mx'] * draws_df['population'])
            draws_df = pd.concat(
                [
                    draws_df[id_cols + ['site_id']],
                    draws_df[draw_cols].apply(
                        lambda x:
                            x * draws_df['population'] *
                            draws_df['completeness_scalar'] * draws_df['added_scalar']
                    ),
                    draws_df[['misdiagnosed_scaled']]
                ],
                axis=1
            )

            # get original deaths
            draws_df = draws_df.merge(df.loc[df.cause_id == self.adjust_id], how='right')
            draws_df['deaths'] = draws_df['deaths'] - draws_df['misdiagnosed_scaled']

            # 1) get variance of total deaths -> original + draws of deaths moved
            draws_df['deaths_variance'] = draws_df[draw_cols].apply(
                lambda x: x + draws_df['deaths']
            ).var(axis=1)
            draws_df['deaths_mean'] = draws_df[draw_cols].apply(
                lambda x: x + draws_df['deaths']
            ).mean(axis=1)

            # 2) get variance in logit percent moved ->
            #     logit(draws of deaths moved / (original + draws of deaths moved))
            draws_df[draw_cols] = draws_df[draw_cols].apply(
                lambda x: logit((x + 1e-5) / (x + 1e-5 + draws_df['deaths']))
            )
            draws_df.replace([np.inf, -np.inf], np.nan, inplace=True)
            draws_df[draw_cols] = draws_df[draw_cols].fillna(logit(1 - 1e-5))
            draws_df['logit_frac_variance'] = draws_df[draw_cols].var(axis=1)
            draws_df['logit_frac_mean'] = draws_df[draw_cols].mean(axis=1)

            # store
            draws_df = draws_df[
                id_cols + ['nid', 'extract_type_id', 'site_id',
                           'deaths_mean', 'deaths_variance',
                           'logit_frac_mean', 'logit_frac_variance']
            ]
            draws_df.to_csv(
                '{mcdir}/{aid}/ui/{nid}_{etid}.csv'.format(
                    mcdir=self.process_dir, aid=self.adjust_id, nid=self.nid,
                    etid=self.extract_type_id
                ),
                index=False
            )

        # store deaths moved
        # WARNING: the "orig_deaths" and "pct_moved" columns are NOT always accurate
        # because of how we create move_df
        move_df['pct_moved'] = move_df['misdiagnosed_scaled'] / \
            move_df['orig_deaths']
        move_df.to_csv(
            '{mcdir}/{aid}/pct/{nid}_{etid}.csv'.format(
                mcdir=self.process_dir, aid=self.adjust_id, nid=self.nid, etid=self.extract_type_id
            ),
            index=False
        )

        # store original data
        orig_df.to_csv(
            '{mcdir}/{aid}/orig/{nid}_{etid}.csv'.format(
                mcdir=self.process_dir, aid=self.adjust_id, nid=self.nid, etid=self.extract_type_id
            ),
            index=False
        )

    def assign_packages(self, df):
        '''Load packages from redistribution location.'''
        package_df = get_clean_package_map_for_misdc(self.code_system_id,
                                                     remove_decimal=self.remove_decimal)
        df = df.merge(package_df, how='left')
        df.loc[df.map_id.astype(str) == 'nan', 'map_id'] = df['cause_id']
        df['map_id'] = df['map_id'].astype(str)
        return df

    def add_map_ids(self, df):
        '''Assing map value to garbage based on package id.'''
        df = add_code_metadata(df, ['value'], code_map_version_id=self.code_map_version_id,
                               **self.block_rerun)
        df['value'] = clean_icd_codes(df['value'], self.remove_decimal)
        # we do this extra step in downloading packages for ICD10, ICD9_detail
        if self.code_system_id in [1, 6]:
            df = remove_five_plus_digit_icd_codes(df, code_system_id=self.code_system_id, trim=True)
        df = self.assign_packages(df)
        # some checks
        garbage_cause_id = (df.cause_id == 743)
        garbage_map_id = (df.map_id.str.contains('_p_', na=False))
        bad_codes = df.loc[
            ~garbage_cause_id & garbage_map_id, ['value', 'map_id', 'cause_id']].drop_duplicates()
        assert len(bad_codes) == 0, \
            'Code(s) mapped to both a cause and a package: {}'.format(bad_codes)
        bad_garbage = df.loc[
            garbage_cause_id & ~garbage_map_id, ['value', 'map_id']].drop_duplicates()
        assert len(bad_garbage) == 0, \
            'Code(s) mapped to garbage but not a package: {}'.format(bad_garbage)
        df.drop('value', axis=1, inplace=True)
        return df

    def get_ss(self, df):
        '''Get total deaths in dataset (by demographic group).'''
        df = df.groupby(self.dem_cols, as_index=False).deaths.sum()
        df.rename(index=str, inplace=True, columns={'deaths': 'sample_size'})
        return df

    def decomp_one_remap_location_id(self, loc_ids):
        """For decomp step one, there is no dismod data for new locations
        or new subnationals. For new subnationals, we will use dismod data for
        the subnational's parent. For new locations we will use dismod data
        from a neighboring region country. This function/step should be removed
        in step two (12/17/2018)"""
        df = pd.DataFrame({'location_id': loc_ids})
        df = add_location_metadata(
            df, 'parent_id', location_set_version_id=self.conf.get_id('location_set_version'),
            **self.block_rerun
        )
        report_if_merge_fail(df, 'parent_id', 'location_id')
        # make subnational location the parent
        df.loc[df.parent_id.isin([16, 51, 86, 165, 214]), 'location_id'] = df['parent_id']
        # remap Saint Kitts to Dominican Repuclic
        df.loc[df.location_id == 393, 'location_id'] = 111
        # remap Monaco and San Marino to Italy
        df.loc[df.location_id.isin([367, 396]), 'location_id'] = 86
        # remap Palau and Cook Islands to Papua New Guinea
        df.loc[df.location_id.isin([320, 380]), 'location_id'] = 26
        return df.location_id.unique().tolist()

    def expand_original_locations(self, df, orig_loc_ids):
        """Part two of fix for decomp step 1. load_dismod needs to
        return a dataframe with the location ids that were originally
        passed in, so that later merges will succeed."""

        dfs = []
        for loc in orig_loc_ids:
            temp = df.copy()
            temp['location_id'] = loc
            dfs.append(temp)
        df = pd.concat(dfs, ignore_index=True)

        assert set(df.location_id) == set(orig_loc_ids)
        return df

    def load_dismod(self, location_id, year_id, file_name='best'):
        '''Retreive interpolated mortality data from DisMod model.
        '''
        orig_loc_ids = location_id
        if self.adjust_id != 543:
            location_id = self.decomp_one_remap_location_id(location_id)
        # No dismod inputs for Iceland, 2018 - use Iceland 2017
        if (location_id == [83]) and (year_id == [2018]) and (self.adjust_id != 543):
            iceland_2018 = True
            year_id = [2017]
        else:
            iceland_2018 = False

        dm_files = sorted(os.listdir(self.dismod_dir))
        assert file_name + '.h5' in dm_files, 'file_name {}.h5 not found. '\
            'files found in {}: {}'.format(file_name, self.dismod_dir, dm_files)
        path = '{}/{}.h5'.format(self.dismod_dir, file_name)
        read_hdf_kwargs = {'path_or_buf': path}
        if len(location_id) <= 30 and len(year_id) <= 30:
            read_hdf_kwargs.update(
                {'where': 'location_id={} and year_id={}'.format(location_id, year_id)}
            )
            df = pd.read_hdf(**read_hdf_kwargs)
        elif len(year_id) <= 30:
            read_hdf_kwargs.update({'where': 'year_id={}'.format(year_id)})
            df = pd.read_hdf(**read_hdf_kwargs)
            df = df.loc[df['location_id'].isin(location_id)]
        else:
            df = pd.read_hdf(**read_hdf_kwargs)
            df = df.loc[(df['location_id'].isin(location_id)) & (df['year_id'].isin(year_id))]

        if iceland_2018:
            df['year_id'] = 2018

        if not set(df.location_id) == set(orig_loc_ids):
            df = self.expand_original_locations(df, orig_loc_ids)
        return df

    def get_deficit(self, df):
        '''Identify missing deaths in VR based on CSMR from DisMod.'''
        df = df.copy()
        est_df = self.load_dismod(list(df.location_id.unique()), list(df.year_id.unique()))
        # get envelope of incoming data deaths
        ss_df = self.get_ss(df)
        est_df = est_df.merge(ss_df)
        df = df.loc[df.cause_id == self.adjust_id].groupby(
            self.dem_cols, as_index=False).deaths.sum()
        # will create rows for every demographic with deaths for the 'adjust_id'
        df = df.merge(est_df[self.dem_cols + ['est_frac', 'sample_size']], how='right')
        df['deaths'].fillna(0, inplace=True)
        # scale the cause fractions from model to the deaths in the incoming data
        df['deficit'] = (df['est_frac'] * df['sample_size']) - df['deaths']
        # for dementia the deficit can be negative, we'll move deaths away from dementia
        # deficit cannot be negative if there were no dementia deaths to begin with, though
        if self.adjust_id == 543:
            df.loc[(df['deaths'] == 0) & (df['deficit'] < 0), 'deficit'] = 0
        else:
            df.loc[df.deficit < 0, 'deficit'] = 0
        return df[self.dem_cols + ['deficit']]

    def duplicate_values(self, df, col=None, splits=None):
        """For a given df, create duplicate rows with a different map_id"""
        assert not df[col].isin(splits.values()).any()
        dupes = df.loc[df[col].isin(splits.keys())].copy()
        if len(dupes > 0):
            dupes[col].update(dupes[col].map(splits))
            df = pd.concat([dupes, df], sort=True, ignore_index=True)
        assert df[col].notnull().all()
        return df

    def add_mcod_props(self, df):
        '''
        Add misdiagnosis probabilities calculated from multiple cause data and subset to
        map_ids that have misdiagnosis probability > 0.
        '''
        prop_df = pd.read_hdf(self.misdiagnosis_path, key='data')
        prop_df['cause_mapped'] = prop_df['cause_mapped'].astype(str)
        prop_df = prop_df.loc[prop_df.recode_deaths > 0]
        prop_df.cause_mapped.update(prop_df.cause_mapped.map(self.package_remap))
        prop_df = self.duplicate_values(prop_df, col='cause_mapped', splits=self.package_split)

        prop_df = prop_df.groupby(['cause_mapped', 'age_group_id', 'sex_id'], as_index=False)[
            ['deaths', 'recode_deaths']
        ].sum()
        prop_df['ratio'] = prop_df['recode_deaths'] / (prop_df['deaths'] + prop_df['recode_deaths'])
        assert prop_df.notnull().values.all()
        df = df.merge(prop_df[['cause_mapped', 'age_group_id', 'sex_id', 'ratio']],
                      left_on=['map_id', 'age_group_id', 'sex_id'],
                      right_on=['cause_mapped', 'age_group_id', 'sex_id'],
                      how='inner')
        df.drop('cause_mapped', axis=1, inplace=True)
        return df

    def get_dementia_map_id(self, df):
        """Return the package_id with the '_p_' prefix for dementia.

        We created special garbage packages for the purpose of moving deaths from AD
        to garbage. The package name for all of them is 'Dementia'.
        """
        package_map = get_package_map(self.code_system_id, **self.block_rerun)
        dementia = package_map.loc[package_map['package_name'] == 'Dementia']
        assert len(dementia) > 0, "{} is missing the Dementia package".format(self.code_system_id)
        package_ids = dementia['package_id'].unique()
        package_ids = ['_p_' + str(x) for x in package_ids]
        assert len(package_ids) == 1, \
            "More than one dementia package in {}".format(self.code_system_id)
        return package_ids[0]

    def add_adjust_id_rows(self, df):
        '''Add row of deaths to be added to adjust cause.'''
        adjust_df = df.groupby(
            self.dem_cols, as_index=False)['misdiagnosed_scaled', 'deaths'].sum()
        adjust_df['map_id'] = str(self.adjust_id)
        df = pd.concat([df, adjust_df], ignore_index=True, sort=True)
        return df

    def apply_empirical_caps(self, df):
        '''Based on 5-star VR countries, enforce caps.'''
        lim_df = pd.read_csv(
            self.conf.get_resource('misdc_limits').format(
                adjust_id=self.adjust_id, code_system_id=self.code_system_id)
        )
        lim_df['map_id'] = lim_df['map_id'].astype(str)
        lim_df.map_id.update(lim_df.map_id.map(self.package_remap))
        lim_df = self.duplicate_values(lim_df, col='map_id', splits=self.package_split)
        # For packages that we merged together, we would ideally recalculate percent limit
        # based on the recoded deaths, but we did not save them
        # Instead we take the 95th percentile of the percent limits to be the new percent limit
        # Erring on the side of moving more deaths rather than less
        lim_df = lim_df.groupby(['age_group_id', 'sex_id', 'map_id'])[
            'pct_limit'].quantile(0.95).reset_index()
        assert lim_df.notnull().values.all()

        df = df.merge(lim_df, how='left')
        df.loc[df.pct_limit.isnull(), 'pct_limit'] = 0.95
        df['deaths_limit'] = df['deaths'] * df['pct_limit']
        df.loc[df.misdiagnosed_scaled > df.deaths_limit, 'misdiagnosed_scaled'] = df['deaths_limit']
        df.drop(['pct_limit', 'deaths_limit'], axis=1, inplace=True)
        return df

    def get_deaths_to_move(self, df):
        '''Determine how many deaths to move from each code.'''
        df = df.groupby(self.dem_cols + ['cause_id', 'map_id'], as_index=False).deaths.sum()
        def_df = self.get_deficit(df)
        # subset to non adjust_id deaths + causes deemed likely to be miscoded (inner merge)
        df = self.add_mcod_props(df)
        df['misdiagnosed'] = df['deaths'] * df['ratio']
        df['potential_misdiagnosed'] = df.groupby(self.dem_cols).misdiagnosed.transform('sum')
        # merge on the level of adjust_id deaths we believe is correct, based on model results
        df = df.merge(def_df)
        # remove rows where we need to take away deaths from the adjust_id, handle these separately
        if self.adjust_id == 543:
            df = df[df['deficit'] >= 0]
        df['misdiagnosed_scaled'] = df['misdiagnosed'] * \
            (df['deficit'] / df['potential_misdiagnosed'])
        df = self.apply_empirical_caps(df)
        # add back in the rows for the adjust_id
        df = self.add_adjust_id_rows(df)
        df = df[self.dem_cols + ['map_id', 'misdiagnosed_scaled', 'deaths']]
        df = df.rename(columns={'deaths': 'orig_deaths'})
        return df

    def get_deaths_to_move_away(self, df, senility_map_id):
        """Determine how many deaths to move away from alzheimer's/dementia.

        As of GBD 2019, we are changing the level of AD, and now in some cases we will be
        moving AD deaths to the senility garbage package to be redistributed proportionally.
        """
        deficit_df = self.get_deficit(df).query('deficit < 0')
        df = df.groupby(self.dem_cols + ['cause_id', 'map_id'], as_index=False).deaths.sum()
        df = df.loc[df['cause_id'] == self.adjust_id]
        df['misdiagnosed'] = df['deaths']
        df['potential_misdiagnosed'] = df.groupby(self.dem_cols).misdiagnosed.transform('sum')
        df = df.merge(deficit_df)
        df['misdiagnosed_scaled'] = df['misdiagnosed'] * \
            (df['deficit'] / df['potential_misdiagnosed'])

        # add rows where we'll be moving the adjust_id deaths to
        senility_df = df.copy()
        senility_df['map_id'] = senility_map_id
        # senility_df['misdiagnosed_scaled'] = -1 * senility_df['misdiagnosed_scaled']

        # append the dataframes back together
        df = pd.concat([df, senility_df], ignore_index=True, sort=True)
        df = df[self.dem_cols + ['map_id', 'misdiagnosed_scaled', 'deaths']]
        df = df.rename(columns={'deaths': 'orig_deaths'})

        # should all be negative, we add this value for adjust_id and subtract for other causes
        assert ((df['misdiagnosed_scaled'] < 0).all())

        return df

    def get_code_ids_from_map_ids(self, map_id):
        cs_map = get_cause_map(code_map_version_id=self.code_map_version_id, **self.block_rerun)
        pkg_map = get_clean_package_map_for_misdc(self.code_system_id,
                                                  remove_decimal=self.remove_decimal)
        assert type(map_id) == str
        if map_id.startswith('_p_'):
            values = pkg_map.loc[pkg_map['map_id'] == map_id, 'value'].values
            codes = cs_map.loc[cs_map.value.isin(values), 'code_id'].values
            cause_id = 743
            assert len(codes) > 0, "No code_ids matching {} in the cause map".format(map_id)
        else:
            codes = cs_map.loc[cs_map.cause_id == int(map_id), 'code_id'].values
            cause_id = int(map_id)
            if len(codes) == 0:
                codes = cs_map.loc[cs_map.cause_id == self.cc_code, 'code_id'].values
                cause_id = self.cc_code
        code_id = codes[0]
        code_dict = {map_id: code_id}
        cause_dict = {map_id: cause_id}
        return code_dict, cause_dict

    def merge_on_scaled(self, df, move_df, senility_map_id=None):
        '''Attach scaled misdiagnosed deaths.

        There could be multiple codes for a single adjust_id, but eventually they'll
        all be going to the same cause_id. If adjust_id is not present in dataset,
        look in map and add to first code.

        If adjust_id is not in map, move to cc_code (aka denominator).
        '''
        keep_cols = self.dem_cols + ['map_id', 'misdiagnosed_scaled']
        df = df.merge(move_df[keep_cols], how='outer')
        if df.cause_id.isnull().values.any():
            permitted_null_map_ids = [str(self.adjust_id)]
            if self.adjust_id == 543:
                permitted_null_map_ids.append(senility_map_id)
            null_map_ids = df.loc[df.cause_id.isnull(), 'map_id'].unique()
            assert set(null_map_ids).issubset(set(permitted_null_map_ids))
            # fix code_ids and cause_ids for newly added rows
            for map_id in permitted_null_map_ids:
                code_dict, cause_dict = self.get_code_ids_from_map_ids(map_id)
                df.loc[df['code_id'].isnull(), 'code_id'] = df['map_id'].map(code_dict)
                df.loc[df['cause_id'].isnull(), 'cause_id'] = df['map_id'].map(cause_dict)
            df['deaths'].fillna(0, inplace=True)
            for idvar in [i for i in list(df) if i.endswith('_id')] + ['nid']:
                if df[idvar].dtype == 'float64':
                    df[idvar] = df[idvar].astype(int)

        return df

    def death_jumble(self, df, move_df, senility_map_id=None):
        '''Use values we've calculated to actually move deaths in main dataframe.'''
        df = self.merge_on_scaled(df, move_df, senility_map_id)
        df['cause_total'] = df.groupby(self.dem_cols + ['map_id']).deaths.transform('sum')
        df['misdiagnosed_scalar'] = df.apply(
            lambda x:
            (x['cause_total'] + x['misdiagnosed_scaled']) / x['cause_total']
            if x['map_id'] == str(self.adjust_id) and x['cause_total'] > 0
            else
            ((x['cause_total'] - x['misdiagnosed_scaled']) / x['cause_total']
                if x['map_id'] != str(self.adjust_id) and x['cause_total'] > 0
                else 0), axis=1
        )
        df['misdiagnosed_scalar'].fillna(1, inplace=True)
        df['deaths'] = df['deaths'] * df['misdiagnosed_scalar']
        df.loc[
            (df.map_id == str(self.adjust_id)) & (df.cause_total == 0), 'deaths'
        ] = df['misdiagnosed_scaled']
        if self.adjust_id == 543:
            # where we added new rows, set new deaths
            df.loc[
                (df.map_id == senility_map_id) & (df.cause_total == 0), 'deaths'
            ] = df['misdiagnosed_scaled'] * -1
        df = df.groupby(self.dem_cols + ['code_id', 'cause_id'], as_index=False).deaths.sum()
        return df

    def get_computed_dataframe(self, df):
        '''For specified cause, use independent miscode probabilities from multiple
        cause data and mortality rates from DisMod to adjust miscoded deaths.'''
        start_deaths = df['deaths'].sum()
        start_deaths_target = df.loc[df.cause_id == self.adjust_id, 'deaths'].sum()
        start_deaths_cc = df.loc[df.cause_id == self.cc_code, 'deaths'].sum()

        df = df.loc[df.deaths > 0]

        print_log_message("Adding map_id column for package_ids")
        df = self.add_map_ids(df)
        # Save a copy of the mapped data before any correction
        orig_df = df.copy()

        print_log_message("Getting deaths to move")
        move_df = self.get_deaths_to_move(df)
        if self.adjust_id == 543:
            senility_map_id = self.get_dementia_map_id(df)
            take_df = self.get_deaths_to_move_away(df, senility_map_id)
            # append them together to get the total deaths being moved
            move_df = pd.concat([move_df, take_df], ignore_index=True, sort=True)
        else:
            senility_map_id = None
        print_log_message("Jumbling up deaths")
        df = self.death_jumble(df, move_df, senility_map_id)

        print_log_message("Checking deaths jumbled well")
        end_deaths = df['deaths'].sum()
        end_deaths_target = df.loc[df.cause_id == self.adjust_id, 'deaths'].sum()
        end_deaths_cc = df.loc[df.cause_id == self.cc_code, 'deaths'].sum()

        assert abs(int(start_deaths) - int(end_deaths)) <= 5, \
            'Bad jumble - added/lost deaths ' \
            '(started: {}, ended: {})'.format(str(int(start_deaths)),
                                              str(int(end_deaths)))
        assert ((df['deaths'] > 0).all()), "There are negative deaths!"

        print_log_message("Storing intermediate data")
        self.store_intermediate_data(df, move_df, orig_df)

        deaths_moved = int((end_deaths_target + end_deaths_cc) -
                           (start_deaths_target + start_deaths_cc))
        print_log_message('Deaths moved: {}'.format(deaths_moved))
        return df
