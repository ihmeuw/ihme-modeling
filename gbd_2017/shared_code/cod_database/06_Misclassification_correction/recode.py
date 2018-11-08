
import pandas as pd
import numpy as np

from cod_prep.claude.cod_process import CodProcess
from cod_prep.downloaders.causes import (
    add_cause_metadata,
    get_all_related_causes
)
from cod_prep.downloaders.nids import add_nid_metadata
from cod_prep.claude.configurator import Configurator
import warnings

class Recoder(CodProcess):

    id_cols = ['nid', 'extract_type_id', 'location_id', 'year_id',
               'age_group_id', 'sex_id', 'cause_id',
               'site_id']
    val_cols = ['deaths', 'deaths_rd', 'deaths_corr', 'deaths_raw']

    def __init__(self, cause_meta_df, source, code_system_id, data_type_id):
        self.source = source
        self.code_system_id = code_system_id
        self.data_type_id = data_type_id
        self.cause_meta_df = cause_meta_df
        self.conf = Configurator("standard")
        self.vr_indicators_path = self.conf.get_resource('vr_indicators')
        self.cache_options = {
            'force_rerun': False,
            'block_rerun': True,
            'cache_results': False,
            'cache_dir': self.conf.get_directory('db_cache')
        }

    def get_computed_dataframe(self, df):

        if 'data_type_id' not in df.columns:
            df = add_nid_metadata(df, "data_type_id", **self.cache_options)
        df = self.recode(df)
        df = self.conform_secret_causes(df)
        df = self.clean_up(df)

        return df

    def get_diagnostic_dataframe(self):
        """Return diagnostics."""
        pass

    def recode_sids(self, df):
        path_to_4_stars_sheet = self.conf.get_resource("four_star_locations")
        four_five_star_locs = pd.read_csv(path_to_4_stars_sheet)
        four_five_star_locs = four_five_star_locs[['location_id']]
        four_five_star_locs = four_five_star_locs.location_id.unique()
        less_than_four_star = ~df['location_id'].isin(four_five_star_locs)
        is_sids = df['cause_id'] == 686
        df.loc[is_sids & less_than_four_star, 'cause_id'] = 380
        return df

    def clean_up(self, df):
        """Group rogue duplicates."""
        df = df.groupby(self.id_cols, as_index=False)[self.val_cols].sum()
        return df

    def conform_secret_causes(self, df):

        df = add_cause_metadata(
            df, add_cols=['secret_cause', 'parent_id'],
            cause_meta_df=self.cause_meta_df,
            **self.cache_options
        )
        injuries_replace_parents = [722, 720, 719]
        replaced_injuries = df['cause_id'].isin(injuries_replace_parents)
        df.loc[replaced_injuries, 'parent_id'] = 723
        secret_causes = df['secret_cause'] == 1
        not_cc_code = df['cause_id'] != 919
        len_before = len(df)
        if df['parent_id'].isnull().values.any():
            raise AssertionError(
                'There are missing parent cause_ids'
            )
        df.loc[secret_causes & not_cc_code, 'cause_id'] = df['parent_id']
        len_after = len(df)
        if len_before != len_after:
            raise AssertionError(
                'The length of the dataframe has changed from {} to {}'.format(
                    len_before, len_after
                )
            )
        df.drop(['parent_id', 'secret_cause'], axis=1, inplace=True)
        return df

    def drop_leukemia_subtypes(self, df):

        leuk_subtypes = get_all_related_causes('neo_leukemia', self.cause_meta_df)

        leuk_subtypes.remove(487)

        df.loc[
            (df['cause_id'].isin(leuk_subtypes)) & (df['deaths_rd'] > 0) &
            (df['deaths_raw'] <= 0), 'cause_id'
        ] = 487

        return df

    
    def recode(self, df):
 
        cause_metadata_df = self.cause_meta_df
        cause_metadata_df = cause_metadata_df[["cause_id",
                                               "path_to_top_parent",
                                               "acause"]]
        ckd_cause_ids = get_all_related_causes('ckd', cause_metadata_df)
        ckd_cause_ids.remove(593)
        ckd_less_other = df['cause_id'].isin(ckd_cause_ids)
        neonate = df['age_group_id'].isin([2, 3])
        df.loc[ckd_less_other & neonate, 'cause_id'] = 652

        resp_ids = [509, 515, 516, 520]
        is_cert_resp_causes = df['cause_id'].isin(resp_ids)

        df.loc[is_cert_resp_causes & neonate, 'cause_id'] = 322

        is_asthma = df['cause_id'] == 515
        df.loc[is_asthma & (df['age_group_id'] == 4), 'cause_id'] = 322

        maternal_cause_ids = get_all_related_causes(366, cause_metadata_df)
        maternal_cause_ids = df['cause_id'].isin(maternal_cause_ids)

        non_maternal_ages = np.logical_not(
            df['age_group_id'].isin([7, 8, 9, 10, 11, 12, 13, 14, 15, 22])
        )
        df.loc[maternal_cause_ids & non_maternal_ages, 'cause_id'] = 919

        alzheimers = df['cause_id'] == 543
        under_40 = df['age_group_id'].isin(range(1, 13, 1))
        df.loc[alzheimers & under_40, 'cause_id'] = 919

        cong_causes = get_all_related_causes('cong', cause_metadata_df)
        congenital = df['cause_id'].isin(cong_causes)
        over_70 = df['age_group_id'].isin([19, 20, 30, 31, 32, 235])
        df.loc[congenital & over_70, "cause_id"] = 919

        hepatitis = get_all_related_causes(400, cause_metadata_df)
        hepatitis = df['cause_id'].isin(hepatitis)
        if self.code_system_id in [7, 9]:
            df.loc[hepatitis & neonate, "cause_id"] = 380
        else:
            df.loc[hepatitis & neonate, "cause_id"] = 384

        inj_disaster_light = df['cause_id'] == 984
        df.loc[inj_disaster_light, 'cause_id'] = 716

        if self.code_system_id not in [1, 6]:
            ckd_diabetes = df['cause_id'].isin([997, 998])
            df.loc[ckd_diabetes, 'cause_id'] = 589

        if self.code_system_id not in [1, 6, 9]:
            diabetes_subtypes = df['cause_id'].isin([975, 976])
            df.loc[diabetes_subtypes, 'cause_id'] = 587

        diabetes_type_2 = df['cause_id'] == 976
        under_15 = df['age_group_id'] < 8
        df.loc[diabetes_type_2 & under_15, 'cause_id'] = 975

        iron_or_iodine = df['cause_id'].isin([388, 390])
        df.loc[iron_or_iodine, 'cause_id'] = 919

        under_1 = df['age_group_id'] < 5
        cvd_ihd = df['cause_id'] == 493
        df.loc[cvd_ihd & under_1, 'cause_id'] = 643

        if 686 in df.cause_id.unique():
            df = self.recode_sids(df)

        df.loc[df.cause_id.isin([344, 409, 410,
                                 542, 558, 669,
                                 680, 961]), 'cause_id'] = 919

        if self.data_type_id not in [6, 7, 8]:
            df.loc[df['cause_id'] == 687, 'cause_id'] = 919

        one_to_14 = df['age_group_id'].isin([5, 6, 7])
        cvd_ihd = df['cause_id'] == 493
        df.loc[cvd_ihd & one_to_14, 'cause_id'] = 507

        cancer_recodes = get_all_related_causes([411, 414, 423, 426, 429, 432,
                                                 435, 438, 441, 444, 450, 453,
                                                 456, 459, 462, 465, 468, 474,
                                                 486, 483], cause_metadata_df)
        cancer_recodes = df['cause_id'].isin(cancer_recodes)
        cancer_ages = df['age_group_id'].isin(range(2, 8, 1))
        df.loc[cancer_recodes & cancer_ages, "cause_id"] = 489

        not_icd10 = self.code_system_id != 1
        neo_meso = df['cause_id'] == 483
        df.loc[neo_meso & not_icd10, "cause_id"] = 489

        if self.source.endswith("AAMSP"):
            digest_hernia = df['cause_id'].isin([531])
            df.loc[digest_hernia, "cause_id"] = 919


        if self.source == "":
            homicide_and_suicide = df['cause_id'].isin([724, 725, 726, 727, 941,
                                                        718, 719, 720, 721, 722, 723])
            bad_years = df['year_id'].isin(range(2007, 2015))
            # _unintent
            df.loc[bad_years & homicide_and_suicide, "cause_id"] = 919


        inj_war = get_all_related_causes(945, cause_metadata_df)
        is_inj_war = df['cause_id'].isin(inj_war)
        jamaica = df['location_id'] == 115
        year_2005 = df['year_id'] == 2005
        vr = df['data_type_id'] == 9
        df.loc[is_inj_war & jamaica & year_2005 & vr, 'cause_id'] = 724

        inj_mech_gun = df['cause_id'] == 705
        year_2006 = df['year_id'] == 2006
        df.loc[inj_mech_gun & year_2006 & jamaica & vr, 'cause_id'] = 724

        if self.source == "ICD10":
            digest_ibd = df['cause_id'] == 532
            suriname = df['location_id'] == 118
            year_1995_2012 = df['year_id'].isin(range(1995, 2013, 1))
            df.loc[digest_ibd & suriname & year_1995_2012, 'cause_id'] = 526

        endo_prodcedural = df['cause_id'] == 624
        df.loc[endo_prodcedural, 'cause_id'] = 708


        schizo = df['cause_id'] == 559
        tibet = df['location_id'] == 518
        df.loc[schizo & tibet, 'cause_id'] = 919

        hiv = get_all_related_causes(298, cause_metadata_df)
        hiv = df['cause_id'].isin(hiv)
        pre_1980 = df['year_id'] < 1980
        df.loc[hiv & pre_1980, 'cause_id'] = 919

        diabetes_causes = get_all_related_causes(587, cause_metadata_df)
        diabetes = df['cause_id'].isin(diabetes_causes)
        df.loc[neonate & diabetes, 'cause_id'] = 380

        under_20 = df['age_group_id'].isin(range(0, 8, 1))
        stroke = get_all_related_causes(
            'cvd_stroke', cause_metadata_df
        )
        stroke_deaths = df['cause_id'].isin(stroke)
        va = df['data_type_id'] == 8
        
        df.loc[under_20 & stroke_deaths & va, 'cause_id'] = 491

        over_95 = df['age_group_id'] == 235
        inj_trans_road_pedal = df['cause_id'] == 691
        df.loc[over_95 & inj_trans_road_pedal, 'cause_id'] = 919

        df.loc[schizo, 'cause_id'] = 919

        if self.source == "Russia_FMD_1999_2011":
            cvd_pvd = df['cause_id'] == 502
            df.loc[cvd_pvd, 'cause_id'] = 491

        if self.source == "":
            sui_homi_causes = [717, 718, 719, 720, 721, 722, 723,
                               724, 725, 726, 727, 941]
            sui_homi = df['cause_id'].isin(sui_homi_causes)
            bad_years = df['year_id'].isin(range(2007, 2015))
            df.loc[sui_homi & bad_years, 'cause_id'] = 919

        if "India_MCCD" in self.source:
            non_neonates = np.logical_not(df['age_group_id'].isin([2, 3]))
            neonatal_sepsis = df['cause_id'].isin([])
            df.loc[non_neonates & neonatal_sepsis, 'cause_id'] = 380

        if self.source == "India_SCD_states_rural":
            warnings.warn("Implement SCD rd artifact recode")


        inj_war_execution = df['cause_id'] == 854

        if self.source == "ICD9_BTL":
            ecuador = df['location_id'] == 122
            year_1980_1990 = df['year_id'].isin(range(1980, 1991, 1))
            df.loc[inj_war_execution & ecuador & year_1980_1990,
                   'cause_id'] = 855

            bih = df['location_id'] == 44
            year_1985_1991 = df['year_id'].isin([1985, 1986, 1987, 1988,
                                                 1989, 1990, 1991])
            df.loc[inj_war_execution & bih &
                   year_1985_1991, 'cause_id'] = 855

            warnings.warn("BTL cancer recode needed")

        if self.source == "ICD10":
            irq = df['location_id'] == 143
            year_2008 = df['year_id'] == 2008
            df.loc[inj_war_execution & year_2008 & irq, 'cause_id'] = 855

        if self.source == "ICD9_detail":
            if ((df['location_id'] == 43) & (df['year_id'] == 1997)).any():
                warnings.warn("Albania homicide recode needed")

        if self.source == "ICD9_USSR_Tabulated":
            warnings.warn("Missing some homicide fixes for TJK, ARM here.")

        df = self.drop_leukemia_subtypes(df)

        if self.data_type_id in [1, 3, 5, 7]:
            maternal_causes = get_all_related_causes('maternal', cause_metadata_df)
            injury_causes = get_all_related_causes('_inj', cause_metadata_df)
            maternal = df['cause_id'].isin(maternal_causes)
            inj = df['cause_id'].isin(injury_causes)
            df.loc[~(maternal | inj), 'cause_id'] = 919

            if self.data_type_id == 5:
                df.loc[~maternal, 'cause_id'] = 919

        return df
