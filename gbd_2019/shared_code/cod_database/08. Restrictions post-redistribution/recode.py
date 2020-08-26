"""
atch-all fixes to results after redistribution.
"""

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
# from cod_prep.utils import report_if_merge_fail


class Recoder(CodProcess):
    """Move deaths from one thing to another based on expert opinon."""

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
        """Return computations."""

        # this method is de-activated until we establish how data drops
        # will be executed (new preference is through not uploading them or
        # running them through noise reduction)
        # df = self.drop_low_quality_data(self.df)
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
        # SIDS in under 4 star locations needs to be recoded to neonatal 02/26/18
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
        """Remove secret causes and conform to reporting cause hierarchy."""
        # replace parent_id = 723 if cause is "inj_suicide_pesti",
        # "inj_suicide_fire", "inj_suicide_hang")
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
        """Remove leukemia subtypes deaths created by redistribution.

        Deaths that are created in redistribution for leukemia subtypes should
        be recoded to the parent leukemia.
        """
        leuk_subtypes = get_all_related_causes('neo_leukemia', self.cause_meta_df)

        # remove parent leukemia cause_id
        leuk_subtypes.remove(487)

        df.loc[
            (df['cause_id'].isin(leuk_subtypes)) & (df['deaths_rd'] > 0) &
            (df['deaths_raw'] <= 0), 'cause_id'
        ] = 487

        return df

    def recode(self, df):
        """Recode based on expert judgement.
        """
        cause_metadata_df = self.cause_meta_df
        cause_metadata_df = cause_metadata_df[["cause_id",
                                               "path_to_top_parent",
                                               "acause"]]
        # recode ckd except for ckd_other to cong_other in neonates
        ckd_cause_ids = get_all_related_causes('ckd', cause_metadata_df)
        ckd_cause_ids.remove(593)
        ckd_less_other = df['cause_id'].isin(ckd_cause_ids)
        neonate = df['age_group_id'].isin([2, 3])
        df.loc[ckd_less_other & neonate, 'cause_id'] = 652

        # recode resp_copd, resp_asthma, resp_other, resp_interstitial to lri
        # in neonates
        resp_ids = [509, 515, 516, 520]
        is_cert_resp_causes = df['cause_id'].isin(resp_ids)
        # neonate already defined
        df.loc[is_cert_resp_causes & neonate, 'cause_id'] = 322

        # recode resp_asthma to lri in perinates
        is_asthma = df['cause_id'] == 515
        df.loc[is_asthma & (df['age_group_id'] == 4), 'cause_id'] = 322

        # Drop any maternal cause below age 10 and above age 55
        # (recode to cc_code)
        maternal_cause_ids = get_all_related_causes(366, cause_metadata_df)
        maternal_cause_ids = df['cause_id'].isin(maternal_cause_ids)
        # ages not in the maternal age range
        non_maternal_ages = np.logical_not(
            df['age_group_id'].isin([7, 8, 9, 10, 11, 12, 13, 14, 15, 22])
        )
        df.loc[maternal_cause_ids & non_maternal_ages, 'cause_id'] = 919

        # Drop alzheimers below age 40 to (recode to cc_code)
        # dementia cause_id = 543
        alzheimers = df['cause_id'] == 543
        under_40 = df['age_group_id'].isin(range(1, 13, 1))
        df.loc[alzheimers & under_40, 'cause_id'] = 919

        # Recode congenital causes to cc_code in ages over 70
        # (stata: substr(acause, 1, 4) == "cong")
        cong_causes = get_all_related_causes('cong', cause_metadata_df)
        congenital = df['cause_id'].isin(cong_causes)
        over_70 = df['age_group_id'].isin([19, 20, 30, 31, 32, 235])
        df.loc[congenital & over_70, "cause_id"] = 919

        # Recode neonatal-aged hepatitis
        # (and all sub-causes) to neonatal_hemolytic
        # except ICD9_USSR_Tabulated and ICD10_tabulated
        # Recode neonatal-aged hepatitis (and all sub-causes) to neonatal
        # if source is ICD9_USSR_Tabulated or ICD10_tabulated
        hepatitis = get_all_related_causes(400, cause_metadata_df)
        hepatitis = df['cause_id'].isin(hepatitis)
        if self.code_system_id in [7, 9]:
            df.loc[hepatitis & neonate, "cause_id"] = 380
        else:
            df.loc[hepatitis & neonate, "cause_id"] = 384

        # inj_disaster_light to inj_othunintent 2/07/18
        inj_disaster_light = df['cause_id'] == 984
        df.loc[inj_disaster_light, 'cause_id'] = 716

        # ckd diabetes type to ckd all but icd10 2/07/18
        # added ICD9_detail to exception 5/15/18
        if self.code_system_id not in [1, 6]:
            ckd_diabetes = df['cause_id'].isin([997, 998])
            df.loc[ckd_diabetes, 'cause_id'] = 589

        # Removing diabetes remap 7/2/2019 - want to use the results of the new
        # unspecified diabetes regression for everything
        # # diabetes subtypes to parent all but icd10 2/07/18
        # # added ICD9_detail, ICD10_tab to exception 5/15/18
        # if self.code_system_id not in [1, 6, 9]:
        #     diabetes_subtypes = df['cause_id'].isin([975, 976])
        #     df.loc[diabetes_subtypes, 'cause_id'] = 587

        # diabetes to type 1 under 15 everywhere 2/07/18
        diabetes_type_2 = df['cause_id'] == 976
        under_15 = df['age_group_id'] < 8
        df.loc[diabetes_type_2 & under_15, 'cause_id'] = 975

        # nutrition iron and iodine to zz every data 2/07/18
        iron_or_iodine = df['cause_id'].isin([388, 390])
        df.loc[iron_or_iodine, 'cause_id'] = 919

        # cvd_ihd move to cong_heart  in under one year 2/07/18
        under_1 = df['age_group_id'] < 5
        cvd_ihd = df['cause_id'] == 493
        df.loc[cvd_ihd & under_1, 'cause_id'] = 643

        if 686 in df.cause_id.unique():
            df = self.recode_sids(df)

        # Need to map _neo, _mental, _infect
        # etc to cc code 2/07/18
        df.loc[df.cause_id.isin([344, 409, 410,
                                 542, 558, 669,
                                 680, 961]), 'cause_id'] = 919
        # usually we also have to map _inj to cc_code, but in some VA we have
        # other sources for splitting _inj we do not move to cc_code 3/26/2018
        if self.data_type_id not in [6, 7, 8]:
            df.loc[df['cause_id'] == 687, 'cause_id'] = 919

        # cvd_ihd to cvd_other in under age one to 14 years 2/07/18 bridge map
        one_to_14 = df['age_group_id'].isin([5, 6, 7])
        cvd_ihd = df['cause_id'] == 493
        df.loc[cvd_ihd & one_to_14, 'cause_id'] = 507
        # TODO test if the distinction between this and the above is necessary,
        # e.g. would the bridge map already map neonatal_hemolytic to neonatal?

        # Do shared cancer recodes (previously in cancer_recodes.do)
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

        # Recode digest_hernia to cc_code if source is Ethiopia_AAMSP
        # added Ethiopia_subnational_AAMSP in GBD2017
        if self.source.endswith("AAMSP"):
            digest_hernia = df['cause_id'].isin([531])
            df.loc[digest_hernia, "cause_id"] = 919

        # in these years we split a garbage of homicide/suicide to
        # their causes proportionally, now we want to recode the years
        # that we don't want to use in the homicide/suicide model.
        if self.source == "Iran_Mohsen_special_ICD10":
            homicide_and_suicide = df['cause_id'].isin([724, 725, 726, 727, 941,
                                                        718, 719, 720, 721, 722, 723])
            bad_years = df['year_id'].isin(range(2007, 2015))
            # _unintent
            df.loc[bad_years & homicide_and_suicide, "cause_id"] = 919

        # Recode war subcauses to inj_homicide in Jamaica 2005 VR
        inj_war = get_all_related_causes(945, cause_metadata_df)
        is_inj_war = df['cause_id'].isin(inj_war)
        jamaica = df['location_id'] == 115
        year_2005 = df['year_id'] == 2005
        vr = df['data_type_id'] == 9
        df.loc[is_inj_war & jamaica & year_2005 & vr, 'cause_id'] = 724

        # Recode inj_mech_gun to inj_homicide for Jamaica 2006 VR
        # "In ICD10 2005 there a large number of deaths due to
        # homicides, but in 2006 many of these deaths have moved to
        # unintentional firearms.
        # 2006 is missing homicides deaths. USERNAME wants to move deaths from
        # unintentional firearms to homicides."
        inj_mech_gun = df['cause_id'] == 705
        year_2006 = df['year_id'] == 2006
        df.loc[inj_mech_gun & year_2006 & jamaica & vr, 'cause_id'] = 724

        # Recode digest_ibd to digest for Suriname 2005-2012 ICD10
        # "Because NR has a very bad effect on IBD in Surinam please recode all
        # of data from  1995-2012 (ICD10 ) for "digest_ibd" to "digest"  in
        # Suriname and keep them in recoding list for every upload"
        # TODO should this be more years than just 2012? like all of ICD10?
        if self.source == "ICD10":
            digest_ibd = df['cause_id'] == 532
            suriname = df['location_id'] == 118
            year_1995_2012 = df['year_id'].isin(range(1995, 2013, 1))
            df.loc[digest_ibd & suriname & year_1995_2012, 'cause_id'] = 526

        # Recode endo_procedural to inj_homicide, writ-large
        # "GBD2013 HACK: USERNAME and USERNAME want Endo-procedural
        # to go to inj_medical just for this round.
        # In GBD2014 it will go to endo"
        endo_prodcedural = df['cause_id'] == 624
        df.loc[endo_prodcedural, 'cause_id'] = 708

        # Recode Schizophrenia to cc_code in Tibet - USERNAME's reason:
        # "Because have very bad effect in Noise Reduction"
        schizo = df['cause_id'] == 559
        tibet = df['location_id'] == 518
        df.loc[schizo & tibet, 'cause_id'] = 919

        # Recode HIV and all sub-causes before 1980 to cc_code, writ-large
        hiv = get_all_related_causes(298, cause_metadata_df)
        hiv = df['cause_id'].isin(hiv)
        pre_1980 = df['year_id'] < 1980
        df.loc[hiv & pre_1980, 'cause_id'] = 919

        # Recode diabetes and all sub-causes to neonatal, if age is neonatal
        # "2-Any death assigned to Diabetes in neonatal period (age 0-28 days)
        # in all data format (Except ICD9 and ICD10 detail) including all MCCD,
        # DSP , Russia format, VA have to recode to the neonatal death" -USERNAME
        # TODO this should be an age restriction for GBD not a recode
        # TODO implement
        diabetes_causes = get_all_related_causes(587, cause_metadata_df)
        diabetes = df['cause_id'].isin(diabetes_causes)
        df.loc[neonate & diabetes, 'cause_id'] = 380

        # Recode cvd_stroke and all subcauses to cvd
        # in Verbal Autopsy under 20 years
        # "Any death in VA and SCD that assigned to the Stroke
        # in under age 20 years have to recode to all CVD"
        # Not done in bridge map; stata code does this for all VA
        # despite SCD comment.
        under_20 = df['age_group_id'].isin(range(0, 8, 1))
        stroke = get_all_related_causes(
            'cvd_stroke', cause_metadata_df
        )
        stroke_deaths = df['cause_id'].isin(stroke)
        va = df['data_type_id'] == 8
        # cvd cause_id is 491
        df.loc[under_20 & stroke_deaths & va, 'cause_id'] = 491

        # Recode inj_trans_road_pedal to cc_code if age over 95, for everything
        # USERNAME request 1/20/2017 "remove inj_trans_road_pedal for over
        # 95 in all countries and years"
        # TODO should this be an age restriction? questionable...
        over_95 = df['age_group_id'] == 235
        inj_trans_road_pedal = df['cause_id'] == 691
        df.loc[over_95 & inj_trans_road_pedal, 'cause_id'] = 919

        # Recode mental_schizo to _mental everywhere
        # "USERNAME request 1/31/2017 to get rid of all mental_schizo as a cause
        # of death and map to _mental"
        # TODO implement
        # TODO should this be yld_only, then? questionable...
        # TODO if maintaining this, don't need restriction restricting
        # mental_schizo to cc_code in Tibet
        df.loc[schizo, 'cause_id'] = 919

        # Recode msk and all sub-causes to cc_code in all VA
        # "USERNAME and USERNAME request 2/14/2017 "msk recode to cc_code for all
        # VA and SRS"
        # this is in the bridge map already

        # Recode cvd_pvd to cvd in Russia_FMD_1999_2011
        # Russia 1999 2011 has a weird outlier for pvd, should be cvd according
        # to USERNAME 02/13/2017
        # TODO implement
        if self.source == "Russia_FMD_1999_2011":
            cvd_pvd = df['cause_id'] == 502
            df.loc[cvd_pvd, 'cause_id'] = 491

        # USERNAME said to remove this following recode 2/26/2018
        # # In all VR USERNAME wants to move mental_drug deaths in under 15
        # # to unintentional poisoning. -USERNAME 7/8/2015
        # # cause_id 562 (mental_drug_opioids) has different age restrictions,
        # # so recode it separately
        # mental_causes_no_op = df['cause_id'].isin(
        #     [560, 561, 563, 564, 565, 566]
        # )
        # mental_no_op_ages = df['age_group_id'].isin(range(2, 8, 1))
        # df.loc[mental_causes_no_op & mental_no_op_ages & vr, 'cause_id'] = 700

        # mental_op = df['cause_id'] == 562
        # mental_op_ages = df['age_group_id'].isin([4, 5, 6, 7])
        # df.loc[mental_op & mental_op_ages & vr, 'cause_id'] = 700

        # Temp fix for self imposed redistribution error
        # move suicide and homicide in these years to cc_code
        if self.source == "Iran_Mohsen_special_ICD10":
            sui_homi_causes = [717, 718, 719, 720, 721, 722, 723,
                               724, 725, 726, 727, 941]
            sui_homi = df['cause_id'].isin(sui_homi_causes)
            bad_years = df['year_id'].isin(range(2007, 2015))
            df.loc[sui_homi & bad_years, 'cause_id'] = 919

        # In India MCCD neonatal sepsis should only be in under 1 month
        if "India_MCCD" in self.source:
            non_neonates = np.logical_not(df['age_group_id'].isin([2, 3]))
            neonatal_sepsis = df['cause_id'].isin([])
            df.loc[non_neonates & neonatal_sepsis, 'cause_id'] = 380

        # In India_SCD_states_rural we are trying to get rid of all the
        # redistribution artifacts
        if self.source == "India_SCD_states_rural":
            warnings.warn("Implement SCD rd artifact recode")

        # Recoding state actor violence to war for proper schocks tracking
        # in ICD9btl & icd10 inj_war_execution > inj_war_war in Ecuador '80-'90
        inj_war_execution = df['cause_id'] == 854

        if self.source == "ICD9_BTL":
            ecuador = df['location_id'] == 122
            year_1980_1990 = df['year_id'].isin(range(1980, 1991, 1))
            df.loc[inj_war_execution & ecuador & year_1980_1990,
                   'cause_id'] = 855

            # inj_war_execution > inj_war_war for BIH from 1985-91
            bih = df['location_id'] == 44
            year_1985_1991 = df['year_id'].isin([1985, 1986, 1987, 1988,
                                                 1989, 1990, 1991])
            df.loc[inj_war_execution & bih &
                   year_1985_1991, 'cause_id'] = 855
            # in icd9_btl there are cancer recodes to be implemented here
            warnings.warn("BTL cancer recode needed")

        if self.source == "ICD10":
            irq = df['location_id'] == 143
            year_2008 = df['year_id'] == 2008
            df.loc[inj_war_execution & year_2008 & irq, 'cause_id'] = 855

        # USERNAME said cirrhosis and hepatitis in India SRS did not go very well (5/26/19)
        # "Move any death from SRS in the final stage due to cirrhosis to hepatitis in under 15
        # Move 30% death from SRS in the final stage due to cirrhosis to hepatitis in between 15-24"
        if self.source == "India_SRS_states_report":
            # There should be no cirrhosis subtypes in SRS, but include them in case things change
            cirrhosis_ids = [521, 522, 523, 524, 971, 525]
            hepatitis_id = 400

            # Under 15
            under_15 = df['age_group_id'] < 8
            cirrhosis = df['cause_id'].isin(cirrhosis_ids)
            df.loc[under_15 & cirrhosis, 'cause_id'] = hepatitis_id

            # 15-24
            start_deaths = df[self.val_cols].sum(axis=0)
            # Create proportions to split
            split_df = pd.DataFrame()
            for age_group_id in [8, 9]:
                for cirrhosis_id in cirrhosis_ids:
                    small_df = pd.DataFrame({
                        'new_cause_id': [cirrhosis_id, hepatitis_id],
                        'pct': [0.70, 0.30]
                    })
                    small_df['cause_id'] = cirrhosis_id
                    small_df['age_group_id'] = age_group_id
                    split_df = split_df.append(small_df, sort=True)
            # Merge in the proportions and split
            # Do not apply the split retroactively - can't take away deaths from
            # cirrhosis in earlier phases if they aren't there yet
            df = df.merge(split_df, how='left', on=['age_group_id', 'cause_id'])
            matches = df.new_cause_id.notnull()
            df.loc[matches, 'cause_id'] = df['new_cause_id']
            df.loc[matches, 'deaths'] = df['deaths'] * df['pct']
            for col in ['deaths_raw', 'deaths_corr', 'deaths_rd']:
                df.loc[matches & (df['new_cause_id'] == hepatitis_id), col] = 0
            df.drop(["new_cause_id", "pct"], axis='columns', inplace=True)
            assert np.allclose(start_deaths, df[self.val_cols].sum(axis=0))
            assert df.notnull().values.all()

        # USERNAMEFm says we should not have congenital in older age groups
        # in this study. USERNAME says that since congenital is created by the
        # redistribution of sepsis for this study: "Result of redistrbution on sepsis
        # have to be very low, if the problem is just this one drop result of redistribution
        # due to sepsis"
        # The larger question is if/when we should create causes in VA
        malawi_va_study = df['nid'] == 413649
        congenital = df.cause_id.isin(get_all_related_causes('cong', cause_metadata_df))
        df.loc[malawi_va_study & congenital, 'cause_id'] = 919

        if self.source == "ICD9_detail":
            if ((df['location_id'] == 43) & (df['year_id'] == 1997)).any():
                warnings.warn("Albania homicide recode needed")

        if self.source == "ICD9_USSR_Tabulated":
            warnings.warn("Missing some homicide fixes for TJK, ARM here.")

        df = self.drop_leukemia_subtypes(df)

        # mortuary, burial, self-reported COD, census/survey,
        # and tabulated hospital data should be reduced down to just
        # injuries, maternal, and cc_code
        if self.data_type_id in [1, 3, 5, 7]:
            maternal_causes = get_all_related_causes('maternal', cause_metadata_df)
            injury_causes = get_all_related_causes('_inj', cause_metadata_df)
            maternal = df['cause_id'].isin(maternal_causes)
            inj = df['cause_id'].isin(injury_causes)
            df.loc[~(maternal | inj), 'cause_id'] = 919

            # for sibling history, we only want maternal and cc_code
            if self.data_type_id == 5:
                df.loc[~maternal, 'cause_id'] = 919

        return df
