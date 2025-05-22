"""
Run network meta-analysis to model the distribution of pathogens
causing a given infectious syndrome.
"""
import pandas as pd
import numpy as np
import argparse
from pathlib import Path
import sys
import getpass
import yaml
import warnings
from scipy.stats import t
import pickle
from cod_prep.utils import (
    report_duplicates, report_if_merge_fail, create_square_df,
    wait_for_job_ids
)
import itertools
from cod_prep.claude.claude_io import Configurator, makedirs_safely
from mcod_prep.utils.covariates import merge_covariate
from cod_prep.downloaders import (
    add_age_metadata, get_current_location_hierarchy,
    add_location_metadata, get_cod_ages, pretty_print,
    get_pop, add_population, getcache_age_aggregate_to_detail_map,
    get_country_level_location_id, get_ages,
    prep_age_aggregate_to_detail_map
)
from mcod_prep.utils.causes import get_infsyn_hierarchy, get_all_related_syndromes
from cod_prep.utils import print_log_message, warn_if_merge_fail
from amr_prep.utils.pathogen_formatting import PathogenFormatter
from mcod_prep.utils.mcod_cluster_tools import submit_mcod
from multiprocessing import Pool
from functools import partial
from pathogen_model_utils import (
    add_country_location_id, add_model_ages, aggregate_data, PathogenCFR
)
sys.path.append('/ihme/homes/{}/netprop/src'.format(getpass.getuser()))
from netprop.data import Data
from netprop.dorm_model import DormModel
from netprop.model import Model


CONF = Configurator()


class PathogenNetwork(object):
    """Model a pathogen network"""
    blank_pathogens = ['none', 'unknown']
    out_dir = Path(CONF.get_directory("process_data").format(model_step='03a_pathogen'))
    id_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id', 'hosp']

    def __init__(self, model_version, infectious_syndrome, keep_pathogens,
                 covariates, agg_age_group_ids, cfr_use, age_weights_use,
                 year_ids, cfr_ready=None, factor_covs=None, ref_pathogen=None,
                 aggregate_cols=None, unknown=0.5, age_start=None,
                 age_end=None, study_weights=None, gprior_sd=None):
        self.model_version = model_version
        self.infectious_syndrome = infectious_syndrome
        self.agg_age_group_ids = agg_age_group_ids
        self.keep_pathogens = keep_pathogens
        self.ref_pathogen = ref_pathogen
        self.covariates = covariates + ['intercept']
        self.cfr_use = cfr_use or {}
        self.age_weights_use = age_weights_use
        self.year_ids = year_ids
        self.factor_covs = factor_covs or []
        self.cfr_ready = cfr_ready
        self.aggregate_cols = aggregate_cols or []
        self.unknown = unknown
        self.age_start = age_start
        self.age_end = age_end
        self.study_weights = study_weights
        self.gprior_sd = gprior_sd

        self.conf = Configurator()
        self.cache_kwargs = {'force_rerun': True, 'block_rerun': False, 'cache_results': True}
        self.validate_inputs()
        self.model_dir = PathogenNetwork.out_dir / self.infectious_syndrome / self.model_version
        makedirs_safely(str(self.model_dir))

        self.misc_configs = pd.read_csv(
            "~/amr/modelling/pathogen/misc_configs.csv"
        ).set_index('entity', verify_integrity=True)['value'].to_dict()

    def validate_inputs(self):
        """Validate inputs"""
        assert self.conf.config_type == 'amr'

        self.infsyn = get_infsyn_hierarchy()
        assert self.infectious_syndrome in self.infsyn.infectious_syndrome.unique()
        self.target_syndromes = get_all_related_syndromes(self.infectious_syndrome, self.infsyn)

        pathogens = pd.read_csv(
            f"{self.conf.get_directory('amr_repo')}/maps/pathogen_metadata.csv"
        )
        assert set(self.keep_pathogens) <= set(pathogens.pathogen)
        if self.ref_pathogen is not None:
            assert self.ref_pathogen in pathogens.pathogen.unique()
            assert self.ref_pathogen in self.keep_pathogens

        ages = get_ages(**self.cache_kwargs)
        assert set(self.agg_age_group_ids) <= set(ages.age_group_id)

    def drop_data(self, df):
        # DATA DROPS/OUTLIERS
        if self.infectious_syndrome == 'respiratory_infectious':
            # If this resp but includes neonatal, only drop strep pneumo
            # for microbiology and lit - we believe this will be underrepresented
            # compared to ICD
            df = self.add_covariates(
                df, covariates=['microbiology'],
                add_model_age=True)
            df = df.loc[~(
                (df.pathogen == 'streptococcus_pneumoniae') & (
                    (df.microbiology == 1) | (df.source.str.endswith("_lit"))
                )
            )]
            # for respiratory, remove any pathogen/age/hosp that has < 10 cases total
            df['total_cases'] = df.groupby(
                ['pathogen', 'agg_age_group_id', 'hosp'])['cases'].transform(sum)
            df = df.loc[df['total_cases'] >= 10]
            df = df.drop(['microbiology', 'total_cases'], axis='columns')
            # Remove special lit sources formatted for CW
            df = df.loc[~df.source.isin(['lri_lit_flu_only', 'lri_lit_rsv_only'])]
            # also remove general lit data with rsv and flu rows
            df = df.loc[(df.source != 'lri_lit') | (~df.pathogen.isin(['flu', 'rsv']))]
            # drop all 'unknown' flu and rsv data
            df = df.loc[(~df.pathogen.isin(['flu', 'rsv'])) | (df.hosp != 'unknown')]
            df = df.drop(['agg_age_group_id', 'data_type'], axis = 'columns')
        elif self.infectious_syndrome == 'cns_infectious':
            df = df.loc[~(
                (df.source == 'champs') & (df.pathogen == 'other')
            )]
            # Legacy from original modelling iteration - non-neonatal
            # meningitis models don't include the GBS meningitis literature
            if 'non_neonatal' in self.model_version:
                df = df.loc[df.source != 'gbs_meningitis_lit']
        return df

    def subset_ages(self, df):
        age_group_table = get_ages()
        good_age_group_ids = df.age_group_id.unique().tolist()
        age_detail_map = prep_age_aggregate_to_detail_map(age_group_table, good_age_group_ids)\
            .query("agg_age_group_id in @self.agg_age_group_ids")
        df = df.loc[df['age_group_id'].isin(age_detail_map['age_group_id'].unique()),:]
        return df

    def calculate_se(self, df):
        # Calculate "props" for the purposes of getting uncertainty
        df = df.groupby(
            self.id_cols + ['source', 'nid', 'pathogen', 'data_type'],
            as_index=False
        )['cases'].sum()
        df['total_all_paths'] = df.groupby(
            [c for c in self.id_cols if c != 'pathogen'] + ['nid']
        )['cases'].transform(sum)
        df = df.loc[df.total_all_paths != 0]
        df = df.loc[df.cases != 0]
        df['prop'] = df['cases'] / df['total_all_paths']
        # From binomial distribution
        df['se'] = np.sqrt(df['prop'] * (1 - df['prop']) * df['total_all_paths'])
        assert df.notnull().values.all()
        df = df.drop(['total_all_paths', 'prop'], axis='columns')
        return df

    def set_composites(self, df):
        """
        Set composite pathogens for specific studies.

        This is a very manual process
        """
        # Set composite pathogen for gbs_meningitis_lit
        if self.infectious_syndrome == 'cns_infectious':
            # Hmwe's team extracts bacterial meningitis only
            df.loc[
                (df.source == 'gbs_meningitis_lit') & (df.pathogen == 'other'),
                'pathogen'
            ] = '-'.join(set(self.keep_pathogens).union({'other'}) - {'group_b_strep', 'virus'})
        return df

    def get_matches(self, df, ref, case_def, match_cols):
        """
        Match observations for the crosswalk

        Based on Kareha's CW code, copied from police conflict code
        """
        assert ref in df[case_def].unique()
        flu_rsv_paths = [
            'flu', 'rsv'
        ]
        assert ref not in flu_rsv_paths
        alts = [x for x in df[case_def].unique() if x != ref]
        assert df[match_cols].notnull().values.all()
        # First match ref to alts
        matches = []
        # Don't compare flu/RSV to a non-flu/RSV pathogen -
        # see more details below
        if self.infectious_syndrome == 'respiratory_infectious':
            alts_pair_to_match = list(set(alts) - set(flu_rsv_paths))
        else:
            alts_pair_to_match = alts
        for alt in alts_pair_to_match:
            match = df.query(f"{case_def} == '{ref}'").merge(
                df.query(f"{case_def} == '{alt}'"),
                on=match_cols, validate='one_to_one'
            )
            matches.append(match)

        combos = list(itertools.combinations(alts, 2))
        
        for combo in combos:
            match = df.query(f"{case_def} == '{combo[0]}'").merge(
                df.query(f"{case_def} == '{combo[1]}'"),
                on=match_cols, validate='one_to_one'
            )
            matches.append(match)
        df_matched = pd.concat(matches, sort=True)

        # Calculate log ratios and standard errors using delta method
        df_matched['log_ratio'] = np.log(df_matched['cases_y'] / df_matched['cases_x'])
        df_matched['log_ratio_se'] = np.sqrt(
            (df_matched['se_x'] / df_matched['cases_x'])**2
            + (df_matched['se_y'] / df_matched['cases_y'])**2
        )
        return df_matched

    def get_flu_rsv_crosswalks(self, target_cols):
        cw_dir = self.out_dir / "flu_rsv_crosswalk"
        df = pd.read_csv(
            cw_dir / f"flu/{self.misc_configs['flu_cw_version']}/data_orig.csv"
        ).append(
            pd.read_csv(cw_dir / f"rsv/{self.misc_configs['rsv_cw_version']}/data_orig.csv")
        )

        # Calculate ratios
        df['log_ratio'] = np.log((1 - df['frac']) / df['frac'])

        # Set pathogens
        flu_paths = ['flu']#, 'poly_flu_rsv']
        rsv_paths = ['rsv']#, 'poly_flu_rsv']
        non_flu_paths = (set(self.keep_pathogens) - set(flu_paths)).union({'other'})
        non_rsv_paths = (set(self.keep_pathogens) - set(rsv_paths)).union({'other'})
        df.loc[df.pathogen == 'flu', 'pathogen_x'] = 'flu'
        df.loc[df.pathogen == 'rsv', 'pathogen_x'] = 'rsv'
        df.loc[df.pathogen == 'flu', 'pathogen_y'] = '-'.join(non_flu_paths)
        df.loc[df.pathogen == 'rsv', 'pathogen_y'] = '-'.join(non_rsv_paths)

        # Set hosp to community - the crosswalk adjusts everything
        # to CAI
        df['hosp'] = 'community'

        # Calculate log_ratio_se using delta method
        df['log_ratio_se'] = np.sqrt(
            ((df['se'] / df['frac'])**2) * 2
        )

        df['cases_x'] = df['frac']
        df['cases_y'] = 1 - df['frac']
        df['se_x'] = df['se']
        df['se_y'] = df['se']
        df = df[target_cols]
        assert df.notnull().values.all()
        return df

    def add_covariates(self, df, covariates=None, add_model_age=True):
        if not covariates:
            covariates = [cov for cov in self.covariates if cov != 'intercept']
        if add_model_age:
            df = add_model_ages(df, self.agg_age_group_ids)
        for cov in covariates:
            if cov not in df:
                if cov in ['europe', 'south_se_asia']:
                    lh = get_current_location_hierarchy(
                        location_set_version_id=self.conf.get_id('location_set_version'),
                        **self.cache_kwargs
                    )
                    region_map = lh.set_index('location_id')['region_name'].to_dict()
                    cov_to_string_match = {
                        'europe': ["Central Europe", "Eastern Europe", "Western Europe"],
                        'south_se_asia': ["South Asia", "Southeast Asia"]
                    }
                    df[cov] = df['location_id'].map(region_map).isin(
                        cov_to_string_match[cov]).astype(int)
                elif cov == 'hosp_continuous':
                    print_log_message(f"Using unknown = {self.unknown}")
                    df['hosp_continuous'] = df['hosp'].map({
                        'community': 0,
                        'unknown': self.unknown,
                        'hospital': 1
                    })
                elif cov == 'microbiology':
                    nid_metadata = pd.read_csv('~/amr/maps/nid_metadata.csv')
                    if 'source' in df.columns:
                        df.loc[df['source'].isin(nid_metadata['source']), 'microbiology'] = 1
                        df.loc[~df['source'].isin(nid_metadata['source']), 'microbiology'] = 0
                        df.loc[df.source.str.endswith("_lit"), 'microbiology'] = 0
                    else:
                        df['microbiology'] = 0
                elif cov == 'recalc_Hib3_coverage_prop':
                	recalc_hib3 = pd.read_csv('/mnt/team/amr/priv/intermediate_files/03a_pathogen/'
                		'hib_pcv_vaccine_coverage/reestimated_hib_coverage.csv')
                	df = df.merge(recalc_hib3, on = ['year_id', 'location_id', 'age_group_id'])
                elif cov == 'recalc_PCV3_coverage_prop':
                	recalc_pcv3 = pd.read_csv('/mnt/team/amr/priv/intermediate_files/03a_pathogen/'
                		'hib_pcv_vaccine_coverage/reestimated_pcv_coverage.csv')
                	df = df.merge(recalc_pcv3, on = ['year_id', 'location_id', 'age_group_id'])
                elif cov == 'cumulative_PCV':
                    cumulo_pcv = pd.read_csv('/mnt/team/amr/priv/intermediate_files/03a_pathogen/'
                        'hib_pcv_vaccine_coverage/cumulative_indirect_pcv_coverage.csv')
                    df = df.merge(cumulo_pcv, on = ['year_id', 'location_id'], validate = 'many_to_one')
                elif cov == 'cumulative_hib':
                    cumulo_hib = pd.read_csv('/mnt/team/amr/priv/intermediate_files/03a_pathogen/'
                        'hib_pcv_vaccine_coverage/cumulative_indirect_hib_coverage.csv')
                    df = df.merge(cumulo_hib, on = ['year_id', 'location_id'], validate = 'many_to_one')
                else:
                    df = merge_covariate(
                        df, cov, decomp_step=self.conf.get_id("decomp_step"),
                        gbd_round_id=self.conf.get_id("gbd_round")
                    )
                if cov not in ['recalc_Hib3_coverage_prop', 'recalc_PCV3_coverage_prop']:
                    assert df[cov].notnull().all()
                elif cov == 'recalc_Hib3_coverage_prop':
                    assert df['Hib3_coverage_prop'].notnull().all()
                elif cov == 'recalc_PCV3_coverage_prop':
                    assert df['PCV3_coverage_prop'].notnull().all()
        return df

    def create_predictions_template(self, pathogens, ages, years):
        """
        Create a predictions template. Should always have all of the id_cols
        """
        lh = get_current_location_hierarchy(
            location_set_version_id=self.conf.get_id('location_set_version'))
        locs = lh.query("level == 3").location_id.unique().tolist()
        if 'sex_id' in self.covariates:
            sexes = [1, 2]
        else:
            sexes = [3]
        if 'hosp' in self.covariates or 'hosp_continuous' in self.covariates:
            hosp = ['community', 'hospital', 'unknown']
        else:
            hosp = ['all']
        index = pd.MultiIndex.from_product(
            [locs, ages, pathogens, years, sexes, hosp],
            names=[
                'location_id', 'age_group_id', 'pathogen',
                'year_id', 'sex_id', 'hosp'
            ]
        )
        square_df = pd.DataFrame(index=index).reset_index()
        square_df = self.add_covariates(square_df, add_model_age=True)
        square_df = square_df.drop('age_group_id', axis='columns')

        # Add CFR
        merge_cols = [c for c in self.cfr_obj.cfr_cols if c != "age_group_id"] + ['agg_age_group_id']
        cfr = self.cfr_obj.cfr.rename(columns={'age_group_id': 'agg_age_group_id'})
        square_df = square_df.merge(
            cfr, how='left', validate='many_to_one', on=merge_cols
        )
        print_log_message(
            f"Filling {square_df.loc[square_df.cfr.isnull(), merge_cols].drop_duplicates()}")
        square_df = square_df.merge(
            cfr.loc[cfr.pathogen == 'all'].drop('pathogen', axis='columns')[
                [c for c in merge_cols if c != 'pathogen'] + ['cfr']
            ], how='left', validate='many_to_one',
            on=[c for c in merge_cols if c != 'pathogen']
        )
        square_df['cfr'] = square_df['cfr_x'].fillna(square_df['cfr_y'])
        square_df = square_df.drop(['cfr_x', 'cfr_y'], axis='columns')
        if self.cfr_ready:
            report_if_merge_fail(square_df, 'cfr', merge_cols)
        else:
            square_df['cfr'] = square_df['cfr'].fillna(1)

        # Add a bit of metadata for easy plotting
        square_df = add_location_metadata(
            square_df, ['location_name', 'super_region_id'])
        square_df['super_region_name'] = square_df['super_region_id'].map(
            lh.set_index('location_id')['location_name_short'].to_dict()
        )
        return square_df

    def one_hot_encode(self, df):
        # Drop all of the factor covs from the list of covariates
        self.covariates = [cov for cov in self.covariates if cov not in self.factor_covs]
        new_factor_covs = []
        for col in self.factor_covs:
            if df[col].dtype == 'float64':
                df[col] = df[col].astype('int')
            add_cols = pd.get_dummies(
                df[col], prefix=col, prefix_sep='', drop_first=True)
            self.covariates = list(set(self.covariates + add_cols.columns.tolist()))
            new_factor_covs = list(set(new_factor_covs + add_cols.columns.tolist()))
            df = pd.concat([df, add_cols], axis=1)
        self.new_factor_covs = new_factor_covs
        return df

    def set_gaussian_priors(self):
        # Set 0 Gaussian priors on all covariates for every bug
        # axes for priors, 0 - mean coefficient estimate, 1 - prior SD
        if self.gprior_sd is not None:
            self.gpriors = {
                pathogen: {cov: [0, self.gprior_sd] for cov in self.covariates if cov != 'intercept'}
                for pathogen in self.keep_pathogens + ['other']
            }
        else:
            self.gpriors = {
                pathogen: None for pathogen in self.keep_pathogens + ['other']
            }
        # Exceptions
        if self.infectious_syndrome == 'respiratory_infectious':
            non_HAI_bugs = ['flu', 'rsv']
            for pathogen in non_HAI_bugs:
                self.gpriors[pathogen]['hosp_continuous'][0] = -10
                self.gpriors[pathogen]['hosp_continuous'][1] = 0.01

            if len(self.agg_age_group_ids) > 1:
                self.gpriors['group_b_strep']['haqi'][1] = 0.02
                self.gpriors['legionella_spp']['haqi'][1] = 0.02

            if self.agg_age_group_ids == [42]:
                self.gpriors['rsv']['haqi'][1] = 0.015
                self.gpriors['chlamydia_spp']['haqi'][1] = 0.1

            if len(self.agg_age_group_ids) > 1:
                self.gpriors['chlamydia_spp']['intercept'] = [0, 0.02]
                self.gpriors['other']['intercept'] = [0, 0.02]

            non_strep = [bug for bug in self.keep_pathogens + ['other'] if bug != 'streptococcus_pneumoniae']
            non_hib = [bug for bug in self.keep_pathogens + ['other'] if bug != 'haemophilus_influenzae']

            if 'PCV3_coverage_prop' in self.covariates:
                for pathogen in non_strep:
                    self.gpriors[pathogen]['PCV3_coverage_prop'][1] = 0.01
            if 'cumulative_PCV' in self.covariates:
                for pathogen in non_strep:
                    if (pathogen == 'flu') & (len(self.agg_age_group_ids) > 1):
                        self.gpriors[pathogen]['cumulative_PCV'][1] = 0.002
                    else:
                        self.gpriors[pathogen]['cumulative_PCV'][1] = 0.01
            if 'Hib3_coverage_prop' in self.covariates:
                for pathogen in non_hib:
                    self.gpriors[pathogen]['Hib3_coverage_prop'][1] = 0.01
            if 'cumulative_hib' in self.covariates:
                for pathogen in non_hib:
                    self.gpriors[pathogen]['cumulative_hib'][1] = 0.01
        
        elif self.infectious_syndrome == 'cns_infectious':
            non_strep = [bug for bug in self.keep_pathogens + ['other'] if bug != 'streptococcus_pneumoniae']
            non_hib = [bug for bug in self.keep_pathogens + ['other'] if bug != 'haemophilus_influenzae']
            non_meningo = [bug for bug in self.keep_pathogens + ['other'] if bug != 'neisseria_meningitidis']

            if 'PCV3_coverage_prop' in self.covariates:
                for pathogen in non_strep:
                    if len(self.agg_age_group_ids) > 1:
                        self.gpriors[pathogen]['PCV3_coverage_prop'][1] = 0.02
                    else:
                        self.gpriors[pathogen]['PCV3_coverage_prop'][1] = 0.1
            if 'cumulative_PCV' in self.covariates:
                for pathogen in non_strep:
                    if len(self.agg_age_group_ids) > 1:
                        self.gpriors[pathogen]['cumulative_PCV'][1] = 0.02
                    else:
                        self.gpriors[pathogen]['cumulative_PCV'][1] = 0.1
            if 'Hib3_coverage_prop' in self.covariates:
                for pathogen in non_hib:
                    if len(self.agg_age_group_ids) > 1:
                        self.gpriors[pathogen]['Hib3_coverage_prop'][1] = 0.02
                    else:
                        self.gpriors[pathogen]['Hib3_coverage_prop'][1] = 0.1
            if 'cumulative_hib' in self.covariates:
                for pathogen in non_hib:
                    if len(self.agg_age_group_ids) > 1:
                        self.gpriors[pathogen]['cumulative_hib'][1] = 0.02
                    else:
                        self.gpriors[pathogen]['cumulative_hib'][1] = 0.1
            if 'cv_menafrivac' in self.covariates:
                for pathogen in non_meningo:
                    if len(self.agg_age_group_ids) > 1:
                        self.gpriors[pathogen]['cv_menafrivac'][1] = 0.02
                    else:
                        self.gpriors[pathogen]['cv_menafrivac'][1] = 0.1
            

    def run_models(self, df_matched, read_model_cache, oosv):
        """Run models with optional leave one country out (LOCO) cross validation"""
        if not read_model_cache:
            dorm_models = [
                DormModel(
                    name=pathogen, covs=self.covariates,
                    gprior=gprior)
                for pathogen, gprior in self.gpriors.items()
            ]
            with open(self.model_dir / "dorm_models.pkl", 'wb') as file:
                pickle.dump(dorm_models, file)
            df_matched.to_csv(self.model_dir / "input_data.csv", index=False)
            worker = f"{self.conf.get_directory('amr_repo')}/"\
                f"modelling/pathogen/model_worker.py"

        if oosv:
            df_matched = df_matched.reset_index(drop=True)
            df_matched = add_location_metadata(df_matched, 'iso3', **self.cache_kwargs)
            assert df_matched.iso3.notnull().all()
            iso3s = sorted(df_matched.iso3.unique().tolist())
            print(f"Running out-of-sample validation with {len(iso3s)} holdouts")
            data_splits = [
                df_matched.assign(
                   train=lambda d: d['iso3'] != iso3,
                   test=lambda d: d['iso3'] == iso3
                ) for iso3 in iso3s
            ] + [df_matched.assign(train=True, test=True)]
            (self.model_dir / "out_of_sample").mkdir(exist_ok=True)
        else:
            data_splits = df_matched.assign(train=True, test=True, holdout='no_holdout')

        if not read_model_cache:
            print_log_message("Launching workers for modelling...")
            jobs = []

            if oosv:
                for holdout in iso3s + ['no_holdout']:
                    jobname = f"modelworker_{self.model_version}_"\
                        f"{self.infectious_syndrome}_{holdout}"
                    params = [
                        self.model_version, self.infectious_syndrome,
                        holdout, self.ref_pathogen
                    ]
                    jid = submit_mcod(
                        jobname, language='python', worker=worker,
                        cores=10, memory="10G", params=params,
                        runtime="10:00:00", logging=True, queue="long.q",
                        log_base_dir=self.model_dir
                    )
                    jobs.append(jid)
                print_log_message("Waiting...")
                wait_for_job_ids(jobs)
                print_log_message("Jobs complete!")
            else:
                for holdout in ['no_holdout']:
                    jobname = f"modelworker_{self.model_version}_"\
                        f"{self.infectious_syndrome}_{holdout}"
                    params = [
                        self.model_version, self.infectious_syndrome,
                        holdout, self.ref_pathogen
                    ]
                    jid = submit_mcod(
                        jobname, language='python', worker=worker,
                        cores=10, memory="10G", params=params,
                        runtime="10:00:00", logging=True, queue="long.q",
                        log_base_dir=self.model_dir
                    )
                    jobs.append(jid)
                print_log_message("Waiting...")
                wait_for_job_ids(jobs)
                print_log_message("Jobs complete!")

        print_log_message("Reading cached models...")
        models = {}

        if oosv:
            for holdout in iso3s + ['no_holdout']:
                out_file = self.model_dir / {'no_holdout': ''}.get(
                    holdout, "out_of_sample") / f"model_{holdout}.pkl"
                with open(out_file, 'rb') as file:
                    models[holdout] = pickle.load(file)
            data_splits = dict(zip(iso3s + ['no_holdout'], data_splits))
        else:
            for holdout in ['no_holdout']:
                out_file = self.model_dir / {'no_holdout': ''}.get(
                    holdout, "out_of_sample") / f"model_{holdout}.pkl"
                with open(out_file, 'rb') as file:
                    models[holdout] = pickle.load(file)
        return data_splits, models

    def create_beta_df(self, model):
        buglist = []
        cov_name = []
        betas = []
        for bug in model.dorms:
            betas.extend(model.beta[model.dorm_model_index[bug]].tolist())
            cov_name.extend(model.dorm_models[bug].covs)
            buglist.extend([bug] * len(model.dorm_models[bug].covs))

        betas = pd.DataFrame({'pathogen': buglist, 'cov_names': cov_name, 'beta': betas})

        betas['beta_sd'] = np.sqrt(np.diagonal(model.beta_vcov))

        betas['beta_pval'] = 2 * t.pdf(
            -1 * np.abs(betas['beta'] / betas['beta_sd']),
            model.data.shape[0] - model.beta.shape[0]
        )

        betas.loc[betas['beta_pval'] <= 0.05, 'signif'] = True
        betas.loc[betas['beta_pval'] > 0.05, 'signif'] = False
        return betas

    def get_residuals(self, model, newdata, oosv):
        """
        Tricky way of pulling out the guts of model.get_residual
        to work with any new data
        """
        # Load new data into a new data object
        if oosv:
            newdata = newdata.query("test").reset_index().assign(intercept=1)

        newdata_obj = Data.load(
            newdata,
            obs="log_ratio",
            obs_se="log_ratio_se",
            ref_dorm="pathogen_x",
            alt_dorm="pathogen_y",
            dorm_separator="-"
        )
        # Step 1 - construct dorm_model_mats
        # Model matrices (X) for each pathogen
        dorm_model_mats = {
            name: model.dorm_models[name].get_mat(newdata)
            for name in model.dorms
        }
        # X * beta for each pathogen
        dorm_values = model.get_dorm_values(
            beta=model.beta, dorm_model_mats=dorm_model_mats)
        # Weights w for each pathogen
        ref_dorm_weights = model.get_dorm_weights(newdata_obj.ref_dorm)
        alt_dorm_weights = model.get_dorm_weights(newdata_obj.alt_dorm)
        ref_pred = np.log(np.sum(ref_dorm_weights * dorm_values, axis=1))
        alt_pred = np.log(np.sum(alt_dorm_weights * dorm_values, axis=1))
        newdata['resid'] = newdata_obj.obs.values - (alt_pred - ref_pred)
        return newdata

    def get_prop_residuals(self, df, models):
        # Some studies have no matches and therefore can't
        # provide useful proportion info
        df = df.loc[df.iso3.isin(models.keys())]
        df = df.assign(
            pathogen_x=self.ref_pathogen,
            pathogen_y=lambda d: d['pathogen'],
            log_ratio=lambda d: np.log(d['cases']),
            log_ratio_se=1
        )
        prop_resids = pd.concat([
            self.get_residuals(
                models[holdout],
                df.assign(
                    test=lambda d: d['iso3'] == holdout if holdout != 'no_holdout' else True
                )
            ).assign(holdout=holdout)
            for holdout in models.keys()
        ])
        prop_resids['prop'] = prop_resids['cases'] / prop_resids.groupby(
            self.id_cols + ['nid', 'holdout']
        ).cases.transform(sum)
        prop_resids['cases_pred'] = np.exp(
            prop_resids['log_ratio'] - prop_resids['resid']
        )
        prop_resids['prop_pred'] = prop_resids['cases_pred'] / prop_resids.groupby(
            self.id_cols + ['nid', 'holdout']
        ).cases_pred.transform(sum)
        return prop_resids

    def generate_point_predictions(self, model, preds):
        print_log_message('Generating point predictions...')
        preds = self.one_hot_encode(preds)
        preds['intercept'] = 1

        # generate case point predictions
        # reset index for safety before concatenating columns
        preds = preds.reset_index(drop=True)
        # Model.predict returns columns for each pathogen, sorted
        # alphabetically
        pathogens = sorted(self.keep_pathogens + ['other'])
        preds = pd.concat([
            preds, pd.DataFrame(model.predict(preds), columns=pathogens)
        ], axis=1)
        assert preds.notnull().values.all()
        # Now select the correct column for each row
        preds['prop_cases'] = preds.apply(lambda x: x[x['pathogen']], axis=1)
        preds = preds.drop(pathogens, axis='columns')

        # Calculate prop_deaths
        dem_cols = ['location_id', 'agg_age_group_id', 'sex_id', 'year_id', 'hosp']
        preds['prop_deaths'] = preds['prop_cases'] * preds['cfr']
        preds['prop_deaths'] = preds['prop_deaths'] / preds.groupby(
            dem_cols)['prop_deaths'].transform(sum)

        # Apply any post-processing
        if self.infectious_syndrome == 'respiratory_infectious':
            poly = (preds.agg_age_group_id == 42) & (preds.hosp == 'community') & (
                preds.pathogen == 'polymicrobial'
            )
            flu_rsv = (preds.hosp == 'hospital') & (preds.pathogen.isin([
                'flu', 'rsv'
            ]))
            zero_out = poly | flu_rsv
            preds.loc[zero_out, 'prop_cases'] = 0
            preds.loc[zero_out, 'prop_deaths'] = 0
            preds['prop_cases'] = preds['prop_cases'] / preds.groupby(
                dem_cols)['prop_cases'].transform(sum)
            preds['prop_deaths'] = preds['prop_deaths'] / preds.groupby(
                dem_cols)['prop_deaths'].transform(sum)
        return preds

    def make_plots(self):
        worker = f"{self.conf.get_directory('amr_repo')}/modelling/pathogen/"\
            f"make_plots.R"
        params = [
            self.model_dir, self.infectious_syndrome,
            '--cov_cols'
        ] + self.covariates
        if len(self.factor_covs) > 0:
            params += ['--encode_cols'] + self.new_factor_covs

        # Launch a job
        print_log_message("Launching job for making plots...")
        jobname = f"make_plots_{self.model_version}_{self.infectious_syndrome}"
        jid = submit_mcod(
            jobname, language='r', worker=worker,
            cores=1, memory="10G", params=params,
            runtime="00:30:00", logging=True,
            log_base_dir=self.model_dir
        )
        print_log_message("Waiting...")
        wait_for_job_ids([jid])
        print_log_message("Job complete")

    def run(self, read_data_cache=True, read_model_cache=False, oosv=False):
        df = pd.read_csv(self.model_dir / "raw_data_cache.csv")
        df['nid'] = df['nid'].apply(lambda x: int(x) if type(x) in [int, float] else x)

        print_log_message("Subsetting ages...")
        df = self.subset_ages(df)

        print_log_message("Getting CFRs...")
        self.cfr_obj = PathogenCFR(
            self.infectious_syndrome, self.agg_age_group_ids, self.cfr_use,
            cfr_ready=self.cfr_ready)
        self.cfr_obj.get_cfrs()
        print_log_message("Applying CFRs...")
        df = self.cfr_obj.apply_cfrs(df)
        df = df.groupby(
            self.id_cols + ['source', 'nid', 'pathogen', 'data_type'],
            as_index=False
        )['cases'].sum()
        print_log_message("Aggregating data...")
        df = aggregate_data(
            df, cols=self.aggregate_cols,
            agg_age_group_ids=self.agg_age_group_ids,
            id_cols=self.id_cols + ['source', 'nid', 'pathogen', 'data_type'])
        print_log_message("Calculating standard errors...")
        df = self.calculate_se(df)

        print_log_message("Applying data drops...")
        df = self.drop_data(df)

        print_log_message("Getting matches...")
        if self.ref_pathogen is None:
            # The reference pathogen should have no effect on the final
            # proportions, but does influence the interpretation of the
            # the coefficient - typically seems to be easiest to understand
            # in relation to the most common pathogen
            self.ref_pathogen = df.groupby(['pathogen'])['cases'].sum().idxmax()
            print_log_message(
                f"No reference pathogen specified, setting to {self.ref_pathogen}"
            )
        df = self.set_composites(df)
        df_matched = self.get_matches(
            df, self.ref_pathogen, 'pathogen',
            self.id_cols + ['nid', 'source']
        )

        if self.infectious_syndrome == 'respiratory_infectious':
            cw = self.get_flu_rsv_crosswalks(df_matched.columns.tolist())
            cw = cw.loc[cw['age_group_id'].isin(df_matched['age_group_id'].unique()), :]
            df_matched = df_matched.append(cw)

        print_log_message("Adding covariates...")
        df = self.add_covariates(df)
        df_matched = self.add_covariates(df_matched)

        if self.infectious_syndrome == 'respiratory_infectious':
            vedata = pd.read_csv(
                '/mnt/team/amr/priv/intermediate_files/03a_pathogen/'
                'respiratory_infectious/streppneumo_VE_PAFs_4_22.csv'
            )
            vedata['pathogen_y'] = '-'.join(
                (set(self.keep_pathogens) - {'streptococcus_pneumoniae'}).union({'other'})
            )
            vedata['source'] = vedata['study_type']
            vedata['log_ratio_se'] = vedata['mod_se']

            vedata = vedata.loc[vedata['agg_age_group_id'].isin(df_matched['age_group_id'].unique()), :]

            if 'recalc_PCV3_coverage_prop' in self.covariates:
                vedata = vedata.drop('PCV3_coverage_prop', axis = 'columns')
                recalc_pcv3 = pd.read_csv('/mnt/team/amr/priv/intermediate_files/03a_pathogen/'
                    		'hib_pcv_vaccine_coverage/reestimated_pcv_coverage.csv')
                vedata = vedata.merge(recalc_pcv3, left_on = ['year_id', 'location_id', 'agg_age_group_id'],
                    right_on = ['year_id', 'location_id', 'age_group_id'])

            if 'recalc_Hib3_coverage_prop' in self.covariates:
                vedata = vedata.drop('Hib3_coverage_prop', axis = 'columns')
                recalc_hib3 = pd.read_csv('/mnt/team/amr/priv/intermediate_files/03a_pathogen/'
                            'hib_pcv_vaccine_coverage/reestimated_hib_coverage.csv')
                vedata = vedata.merge(recalc_hib3, left_on = ['year_id', 'location_id', 'agg_age_group_id'],
                	right_on = ['year_id', 'location_id', 'age_group_id'])

            if 'cumulative_PCV' in self.covariates:
                cumulo_pcv = pd.read_csv('/mnt/team/amr/priv/intermediate_files/03a_pathogen/'
                    'hib_pcv_vaccine_coverage/cumulative_indirect_pcv_coverage.csv')
                vedata = vedata.merge(cumulo_pcv, on = ['year_id', 'location_id'], validate = 'many_to_one')

            if 'cumulative_hib' in self.covariates:
                cumulo_hib = pd.read_csv('/mnt/team/amr/priv/intermediate_files/03a_pathogen/'
                    'hib_pcv_vaccine_coverage/cumulative_indirect_hib_coverage.csv')
                vedata = vedata.merge(cumulo_hib, on = ['year_id', 'location_id'], validate = 'many_to_one')


            df_matched = df_matched.append(vedata, sort=False)

            
        print_log_message("Applying study weights...")
        if self.study_weights is not None:
            df_matched['log_ratio_se'] /= df_matched['source'].map(
                self.study_weights).fillna(1)

        print_log_message("Saving input data...")
        df = pretty_print(df, exclude=['nid'])
        df['iso3'] = df['ihme_loc_id'].str[0:3]
        df.to_csv(self.model_dir / "data_orig.csv", index=False)

        print_log_message("Creating predictions template...")
        preds = self.create_predictions_template(
            self.keep_pathogens + ['other'],
            df.agg_age_group_id.unique().tolist(),
            self.year_ids
        )

        self.covariates = [c.replace('recalc_', '') for c in self.covariates]

        print_log_message("One-hot encoding factor variables...")
        df_matched = self.one_hot_encode(df_matched)
        df = self.one_hot_encode(df)

        print_log_message("Setting Gaussian priors...")
        self.set_gaussian_priors()

        if oosv:
            print_log_message("Running model in-sample and with LOCO CV")
        else:
            print_log_message("Running model in-sample")
        data_splits, models = self.run_models(df_matched, read_model_cache, oosv)

        print_log_message("Getting betas and residuals...")
        betas = pd.concat([
            self.create_beta_df(model).assign(holdout=holdout)
            for holdout, model in models.items()
        ])
        # Ratio-space residuals
        if oosv:
            resids = pd.concat([
            self.get_residuals(models[holdout], data_splits[holdout], oosv).assign(
                holdout=holdout
            ) for holdout in models.keys()
            ])
        else:
            resids = self.get_residuals(models['no_holdout'], data_splits, oosv)
        # Proportion-space residuals
        #prop_resids = self.get_prop_residuals(df, models)

        print_log_message("Making predictions...")
        preds = self.generate_point_predictions(models['no_holdout'], preds)

        print_log_message("Saving results")
        resids.to_csv(self.model_dir / "resids.csv", index=False)
        betas.to_csv(self.model_dir / "betas.csv", index=False)
        preds.to_csv(self.model_dir / "predictions.csv", index=False)

        self.make_plots()
        print_log_message("Done!")


def parse_config(model_version, infectious_syndrome):
    config_file = pd.read_excel(
        "~/amr/modelling/pathogen/config.xlsx",
        sheet_name='run'
    )
    config = config_file.query(
        f"model_version == '{model_version}' & "
        f"infectious_syndrome == '{infectious_syndrome}'"
    )
    assert len(config) == 1
    config = config.iloc[0].to_dict()
    # Parse lists and dictionaries
    for param in {
        'covariates', 'agg_age_group_ids', 'keep_pathogens',
        'cfr_use', 'age_weights_use', 'year_ids', 'factor_covs',
        'aggregate_cols', 'study_weights'
    }.intersection(set(config.keys())):
        if not config[param] == 'None':
            config[param] = str(config[param]).split(',')
            if param in ['agg_age_group_ids', 'year_ids']:
                if ":" in config[param][0]: 
                    years = str(config[param][0]).split(":")
                    year_start = int(years[0])
                    year_end = int(years[1])
                    years = list(range(year_start,year_end+1))
                    config[param] = [int(x) for x in years]
                else: 
                    config[param] = [int(x) for x in config[param]]
            if param in ['cfr_use', 'age_weights_use', 'study_weights']:
                param_to_value_type = {
                    'cfr_use': str, 'age_weights_use': str,
                    'study_weights': float
                }
                value_type = param_to_value_type[param]
                config[param] = {
                    x.split(':')[0]: value_type(x.split(':')[1]) for x in config[param]
                }
        else:
            config[param] = None
    for param in ['ref_pathogen', 'age_start', 'age_end', 'unknown', 'gprior_sd']:
        if config[param] == 'None':
            config[param] = None
    for param in ['cfr_ready']:
        config[param] = bool(config[param])

    # Deduce keep_pathogens
    if 'keep_pathogens' not in config.keys() or config['keep_pathogens'] is None:
        syn_path = pd.read_csv("~/amr/modelling/pathogens_assessed_by_syndrome.csv")
        syn_path = syn_path.query(
            f"infectious_syndrome == '{config['infectious_syndrome']}'"
        )
        config['keep_pathogens'] = syn_path['pathogens'].iloc[0].split(', ')
    return config


def save_config(out_dir, config):
    with open(f'{out_dir}/config.yml', 'w') as outfile:
        yaml.dump(config, outfile)


if __name__ == '__main__':
    # Input args
    parser = argparse.ArgumentParser(description='Launch pathogen network model')
    parser.add_argument('model_version', type=str)
    parser.add_argument('infectious_syndrome', type=str)
    parser.add_argument('--oosv', action='store_true')
    parser.add_argument('--read_data_cache', action='store_true')
    parser.add_argument('--read_model_cache', action='store_true')
    args = parser.parse_args()
    config = parse_config(args.model_version, args.infectious_syndrome)
    print_log_message(
        f"You submitted the following config: {config}"
    )
    network = PathogenNetwork(**config)
    save_config(network.model_dir, config)
    network.run(read_data_cache=args.read_data_cache, read_model_cache=args.read_model_cache, oosv=args.oosv)
