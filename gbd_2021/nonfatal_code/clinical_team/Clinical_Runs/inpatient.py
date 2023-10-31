"""
Class that runs inpatient pipeline
"""

import pandas as pd
import numpy as np
import time
import sys
import glob
import warnings
import os
import pickle
import subprocess
import re
import ipdb
from db_tools.ezfuncs import query

from clinical_info.Functions.Clinical_functions import clinical_funcs
from clinical_info.Functions import hosp_prep, gbd_hosp_prep, bundle_check, data_structure_utils as dsu, demographic_utils as du
from clinical_info.Mapping import clinical_mapping
from clinical_info.Formatting.all_sources import fmt_sources_master as fmt_master
from clinical_info.Mapping import submit_icd_mapping, clinical_mapping as cm
from clinical_info.Envelope.prep_for_env import prep_for_env_main
from clinical_info.Envelope.apply_env_only import apply_envelope_only
from clinical_info.AgeSexSplitting.compute_weights import compute_weights
from clinical_info.AgeSexSplitting.run_age_sex_splitting import run_age_sex_splitting
from clinical_info.Envelope.create_population_estimates.submit_pop_ests import create_icg_single_year_est
from clinical_info.Envelope import submit_bundle_draw_estimates as sb  # import main_bundle_draw_submit
from clinical_info.Envelope.create_population_estimates.icg_prep_maternal_data import write_maternal_denom
from clinical_info.Functions.clinfo_stdout_logger import ClinfoLogger
from clinical_info.Clinical_Runs import clean_final_bundle
from clinical_info.Testing import confirm_square_data
from clinical_info.Odd_jobs.adhoc_pipeline_fixes import emr_replacement
from clinical_info.Corrections import haqi_prep
from clinical_info.Upload.tools.io.database import Database
from clinical_info.Envelope import re_aggregate_under1
from clinical_info.Database.bundle_relationships import relationship_methods as br


class Inpatient(object):
    """The Inpatient class is used to run the Inpatient pipeline via the main
    method.

    Parameters
    ----------
    run_id : int
        The id for the run.
    write_results : bool
        Should results be writen? Default is True.
    map_version : str
        The map version. Default is "current".

    Attributes
    ----------
    map : str
        see paramter "map_version"
    base : str
        The base path for the specific inpatient clinical run.
    gbd_round_id : int
        The GBD round ID. Default is None.
    decomp_step : str
        The decomp step. Default is "".
    env_path : str
        The filepath for the envelope to use when going from count to rate
        space. Default is "NULL"
    no_draws_env : str
        The filepath for the no draws envelope. Default is "NULL"
    env_stgpr_id : int
        Default is -1.
    env_me_id : int
        Default is -1.
    cf_version_id : int
        Default is -1.
    run_tmp_unc : bool
        Default is True
    begin_with : str
        A plain english string indicating which step in the process the main()
        function should begin with. Default is a passive aggressive message.
    set_map_int_version : type
        The map version to use. Default is generated from
        clinical_mapping.test_and_return_map_version.
    run_id
    write_results
    bundle_level_cfs : bool
        Indicating whether CF data should be re-run at the end of the pipeline using
        Correction Factors created at the bundle level. This is probably the most
        methodologically correct version when set to True b/c it creates ICG level
        draws for mean0 and then retains draws through bundle aggregation, 5 year agg
        and CF application

    """
    def __init__(
        self, run_id, clinical_age_group_set_id, remove_live_births,
        write_results=True, map_version="current"):

        self.map = map_version
        self.run_id = run_id
        self.clinical_age_group_set_id = clinical_age_group_set_id
        self.base = FILEPATH
        self.write_results = write_results
        self.gbd_round_id = None
        self.decomp_step = ""
        self.clinical_decomp_step = None
        self.env_path = "NULL"
        self.no_draws_env = "NULL"
        self.env_stgpr_id = -1
        self.env_me_id = -1
        self.cf_version_id = -1
        self.run_tmp_unc = True
        self.begin_with = "please run the automater method to find out"
        self.set_map_int_version = clinical_mapping.test_and_return_map_version(self.map, prod=True)
        self.cf_model_type = 'mr-brt'
        self.bundle_level_cfs = True
        self.make_age_sex_weights = True
        self._confirm_master_data = True
        self.weight_squaring_method = 'bundle_source_specific'
        self.set_sources
        self.remove_live_births = remove_live_births
        self.que = 'all.q'
        self.cf_agg_stat = 'mean'
        warnings.warn("At points this class will run code that uses multiprocessing with at most 15 pools. If ran on fair cluster then it needs to be ran with at least 15 threads.")

    def __str__(self):
        display = "GBD round {}, Central waterfall step {}".format(self.gbd_round_id, self.decomp_step)
        display += "\nThe clinical decomp step is {}".format(self.clinical_decomp_step)
        display += "\nThis run wil use clinical age group set id {}".format(self.clinical_age_group_set_id)
        display += "\nThe next step of the inpatient pipeline will be: {}".format(self.begin_with)
        display += "\nrun_id is {}".format(self.run_id)
        display += "\nIt is {} that bundle level CFs will be used".format(self.bundle_level_cfs)
        display += "\nwrite_results is {}".format(self.write_results)
        display += "\nfiles will be saved to {}".format(self.base)
        display += "\nusing map {}".format(self.map)
        display += "\nwhich currently is map version {}".format(self.set_map_int_version)
        display += "\nthe envelope with draws is versioned {}".format(self.env_path)
        display += "\nthe envelope without draws is versioned {}".format(self.no_draws_env)
        display += "\nthis run will use {} model type CFs".format(self.cf_model_type)
        display += "\nAge-sex weights will be created from inp data: {}".format(self.make_age_sex_weights)
        display += f"\nThe {self.weight_squaring_method} weight creation method will be used"
        display += f"\nWill live birth icd codes be removed? {self.remove_live_births}"
        return display

    def __repr__(self):
        display = "GBD round {}, Central waterfall step {}".format(self.gbd_round_id, self.decomp_step)
        display += "\nThe clinical decomp step is {}".format(self.clinical_decomp_step)
        display += "\nThis run wil use clinical age group set id {}".format(self.clinical_age_group_set_id)
        display += "\nThe next step of the inpatient pipeline will be: {}".format(self.begin_with)
        display += "\nrun_id is {}".format(self.run_id)
        display += "\nIt is {} that bundle level CFs will be used".format(self.bundle_level_cfs)
        display += "\nwrite_results is {}".format(self.write_results)
        display += "\nfiles will be saved to {}".format(self.base)
        display += "\nusing map {}".format(self.map)
        display += "\nwhich currently is map version {}".format(self.set_map_int_version)
        display += "\nthe envelope with draws is versioned {}".format(self.env_path)
        display += "\nthe envelope without draws is versioned {}".format(self.no_draws_env)
        display += "\nthis run will use {} model type CFs".format(self.cf_model_type)
        display += "\nAge-sex weights will be created from inp data: {}".format(self.make_age_sex_weights)
        display += f"\nThe {self.weight_squaring_method} weight creation method will be used"
        display += f"\nWill live birth icd codes be removed? {self.remove_live_births}"
        return display

    @property
    def set_sources(self):
        """Create lists for all of the sources that master data will use. Setting this in the
        Inpatient class rather than in master data allows us to run a subset of sources
        by manually overwriting the attributes
        "new" here means it was formatted in Pyton with an output in hdf
        "old" means it was formatted in Stata with a .dta output"""

        # need to create a function for this
        db = Database()
        db.load_odbc()

        dql = QUERY
        db_src = db.query(dql).source_name.tolist()
        db_src = [e.strip() for e in db_src] # this is annoying

        self._old_sources = ['USA_NHAMCS_92_10', 'NOR_NIPH_08_12',
                            'NZL_NMDS', 'EUR_HMDB', 'SWE_PATIENT_REGISTRY_98_12',
                            'MEX_SINAIS', 'USA_NAMCS', 'USA_HCUP_SID_03', 'USA_HCUP_SID_04',
                            'USA_HCUP_SID_05', 'USA_HCUP_SID_06', 'USA_HCUP_SID_07',
                            'USA_HCUP_SID_08', 'USA_HCUP_SID_09', 'USA_NHDS_79_10']
        
        # db does not store appended years to source names.
        old = {re.sub('[^A-Z, a-z]*$', '', e) for e in self._old_sources}
        self._new_sources = [e for e in db_src if e not in old ]
        self._new_sources.remove('GEO_Discharge') 
        self._new_sources.extend(['SWE_Patient_Register_15_16', 
                                'GEO_Discharge_16_17', 'MEX_SINAIS'])

    def log_stdout(self):
        """The inp class spits out a lot of useful info to stdout. Store it!"""

        write_path = FILEPATH
        sys.stdout = ClinfoLogger(write_path=write_path)

    def check_merged_nids(self, df, break_if_missing=True):
        """
        Checks if all sources are present from earlier steps.

        Args:
            df (Pandas DataFrame): contains mapped data.
            break_if_missing (Bool): If True, will throw error when an NID is
                missing

        Raises:
            Assertion Error
        """

        print("Running the validation to confirm merged NIDs are present")

        if 'year_id' in df.columns.tolist():
            df.rename(columns={'year_id': 'year_start'}, inplace=True)
        df = df[['source', 'nid', 'year_start']].drop_duplicates()

        df = hosp_prep.apply_merged_nids(df, assert_no_nulls=False,
                                         fillna=True)
        nulls = df['nid'].isnull().sum()
        missing = (df['nid'] <= 0).sum()
        null_msg = "There are null merged NIDs"
        missing_msg = "There are incorrectly coded NIDs"

        if break_if_missing:
            assert nulls == 0, "{}".format(null_msg)
            assert missing == 0, "{}".format(missing_msg)
        else:
            if nulls != 0:
                warnings.warn(null_msg)
            if missing != 0:
                warnings.warn(missing_msg)

        return

    def master_data(self):
        """Aggergates all inpatient hospital sources into a single source.

        Files are written to FILEPATH
        Parameters
        ----------


        Returns
        -------
        None

        """

        if self.gbd_round_id is None or self.decomp_step == '':
            raise ValueError("The population caching tool requires a gbd round id and decomp step to pull pop from")

        # set true only if run_id is 5, otherwise false
        if self.run_id == 5:
            gbd2019_decomp4 = True
            print("Running with only a subset of data sources for gbd2019 decomp4")
        else:
            gbd2019_decomp4 = False
        fmt_master.main(self.run_id,
                        clinical_age_group_set_id=self.clinical_age_group_set_id,
                        gbd_round_id=self.gbd_round_id,
                        decomp_step=self.decomp_step,
                        gbd2019_decomp4=gbd2019_decomp4,
                        new_sources=self._new_sources,
                        old_sources=self._old_sources,
                        remove_live_births=self.remove_live_births)

        status = "wait"
        time.sleep(15)
        while status == "wait":

            p = subprocess.Popen("qstat", stdout=subprocess.PIPE)
            qstat_txt = p.communicate()[0]
            qstat_txt = qstat_txt.decode('utf-8')
            pattern = 'fmt_+'
            found = re.search(pattern, qstat_txt)
            try:
                found.group(0)
                status = "wait"
                time.sleep(40)
            except:
                status = "go"
        print(status)
        return

    def map_to_icg(self):
        """Map source icd codes to icg.

        Parameters
        ----------


        Returns
        -------
        pd.DataFrame
            The master data in icg space via a pandas dataframe.

        """
        df = submit_icd_mapping.icd_mapping(en_proportions=True, create_en_matrix_data=True,
                                            save_results=self.write_results, write_log=True, deaths='non', extra_name="",
                                            run_id=self.run_id, map_version=self.map)

        return df
    
    def get_source_round(self):
        '''
        Return source name and age sex round
        '''
        db = Database()
        db.load_odbc()

        dql = (QUERY)
        
        src_rnd = db.query(dql)

        return src_rnd

    def age_sex_weights(self, back, env_path, make_weights,
                        round_2_only, overwrite_weights=True, rounds=2):
        """


        Params:
            back (pandas DataFrame):
                Inpatient clinical data
            env_path (str):
                filepath for the envelope to use when going from count to rate
                space
            round_2_only (bool):
                skip the first round of making weights, and use all the 2nd
                round sources
            make_weights (bool):
                If True run the entirety of the function
                If False we check for an existing weight file and then return
            overwrite_weights (bool):
                The weights exist in a csv in each specific run in
                FILEPATH. This option may be misleading as
                well. I think we'll always want to overwrite weights.
            rounds (int):
                How many rounds of weights to make, we're currently making 2,
                although there has been some talk that the 2nd round is
                unnecessary/has little effect
        Returns
            Nothing
        """
        if not make_weights:
            weight_path = FILEPATH.format(self.base)
            if os.path.exists(weight_path):
                return
            else:
                assert False, "The weights don't appear to be present at {}".format(weight_path)
        if round_2_only and rounds < 2:
            raise ValueError("Think about what these two args mean together")

        source_round_df = self.get_source_round()

        # check that all sources have a designated round
        source_diff = set(back['source'].unique()) - set(source_round_df['source'].unique())
        assert source_diff == set(),\
            "Please add {} to the list of sources and which round to use it in".format(source_diff)

        round_1_newdrops = source_round_df.loc[source_round_df.use_in_round > 1, 'source']
        round_2_newdrops = source_round_df.loc[source_round_df.use_in_round > 2, 'source']

        weight_cols = ['age_group_id', 'sex_id', 'location_id', 'year_start', 'year_end',
                       'source', 'icg_id', 'product']

        if (not round_2_only) and (make_weights):
            df = back.copy()

            df = df[~df.source.isin(round_1_newdrops)].copy()
            r1_sources = df.source.unique().tolist()

            exp_round1_sources = source_round_df.query("use_in_round <= 1")['source']
            if set(r1_sources).symmetric_difference(exp_round1_sources):
                raise ValueError("Expected sources don't match what's in the data")

            print(f"Begin Round One weights with {r1_sources}")
            df = du.retain_age_sex_split_age_groups(df, run_id=self.run_id, round_id=1,
                                                    clinical_age_group_set_id=self.clinical_age_group_set_id)

            df = apply_envelope_only(df, run_id=self.run_id,
                                     env_path=env_path,
                                     apply_age_sex_restrictions=True,
                                     want_to_drop_data=True,
                                     create_hosp_denom=False,
                                     gbd_round_id=self.gbd_round_id,
                                     decomp_step=self.decomp_step,
                                     clinical_age_group_set_id=self.clinical_age_group_set_id,
                                     use_cached_pop=True)

            df = df[weight_cols]
            compute_weights(df, run_id=self.run_id, round_id=1,
                            clinical_age_group_set_id=self.clinical_age_group_set_id,
                            gbd_round_id=self.gbd_round_id, decomp_step=self.decomp_step,
                            overwrite_weights=overwrite_weights,
                            squaring_method=self.weight_squaring_method)

        if (round_2_only and make_weights) or (make_weights and rounds == 2):
            df = back.copy()
            del back

            df = df[~df.source.isin(round_2_newdrops)].copy()
            r2_sources = df.source.unique().tolist()

            exp_round2_sources = source_round_df.query("use_in_round <= 2")['source']
            if set(r2_sources).symmetric_difference(exp_round2_sources):
                raise ValueError("Expected sources don't match what's in the data")

            print(f"Begin Round Two weights with {r2_sources}")

            df = run_age_sex_splitting(df, run_id=self.run_id, gbd_round_id=self.gbd_round_id,
                                       clinical_age_group_set_id=self.clinical_age_group_set_id,
                                       decomp_step=self.decomp_step, verbose=True, round_id=2,
                                       write_viz_data=False)

            df = du.retain_age_sex_split_age_groups(df, run_id=self.run_id, round_id=2,
                                                    clinical_age_group_set_id=self.clinical_age_group_set_id)

            df = apply_envelope_only(df, run_id=self.run_id,
                                     env_path=env_path,
                                     apply_age_sex_restrictions=True,
                                     want_to_drop_data=True,
                                     create_hosp_denom=False,
                                     gbd_round_id=self.gbd_round_id,
                                     decomp_step=self.decomp_step,
                                     clinical_age_group_set_id=self.clinical_age_group_set_id,
                                     use_cached_pop=True)
            df = df[weight_cols]

            # compute the actual weights
            compute_weights(df, run_id=self.run_id, gbd_round_id=self.gbd_round_id,
                            clinical_age_group_set_id=self.clinical_age_group_set_id,
                            decomp_step=self.decomp_step, round_id=2,
                            overwrite_weights=overwrite_weights,
                            squaring_method=self.weight_squaring_method)
        return

    def convert_obsolete_special_map_icgs(self, df):
        """
        Parameters
        ----------
        df : pd.DataFrame
            A DataFrame of inpatient data

        Returns
        -------
        pd.DataFrame
            The `df` after it has had old icgs removed
        """
        fix_dict = {

            'e-code, (inj_poisoning_other)': 'poisoning_other',
            'lower respiratory infection(all)': 'lower respiratory infection(unspecified)',
            'neoplasm non invasive other': 'other benign and in situ neoplasms',

            'e-code, (inj_trans)': 'z-code, (inj_trans)',

            'digest, gastritis and duodenitis': '_none',

            'resp, copd, bronchiectasis': 'resp, other'
            }

        chk_df = clinical_mapping.get_clinical_process_data("icg_durations", map_version=self.map)
        chk_df = chk_df[['icg_name', 'icg_id']]

        chk_df = chk_df[chk_df['icg_name'].isin(list(fix_dict.values()))]


        exp_id_dict = {1267: 864, 1266: 1116, 1265: 159, 1264: 1241, 1263: 1, 1262: 434}

        sdiff = set(chk_df['icg_id']).symmetric_difference(set(exp_id_dict.values()))
        assert not sdiff, "The icg_ids don't match our hardcoding. This is unexpected"


        for key, value in list(fix_dict.items()):
            # get new icg_id
            an_id = chk_df.query("icg_name == @value")
            assert len(an_id) == 1
            an_id = an_id.icg_id.iloc[0]
            df.loc[df['icg_name'] == key, ['icg_name', 'icg_id']] = (value, an_id)

        return df

    def age_sex_split(self, df, rounds=2):
        """Run age sex splitting on the given dataframe.

        Parameters
        ----------
        df : pd.DataFrame
            A DataFrame to split.
        rounds : int
            Round id. Default is 2

        Returns
        -------
        pd.DataFrame
            The `df` after it has been age sex split.

        """

        # remove measure for now
        df.drop('icg_measure', axis=1, inplace=True)

        df = self.convert_obsolete_special_map_icgs(df)

        df = df.groupby(df.columns.drop('val').tolist()).agg({'val': 'sum'}).reset_index()
        df = run_age_sex_splitting(df, run_id=self.run_id, gbd_round_id=self.gbd_round_id,
                                   clinical_age_group_set_id=self.clinical_age_group_set_id,
                                   decomp_step=self.decomp_step, verbose=True, round_id=rounds,
                                   write_viz_data=False)
        wpath = FILEPATH.format(rid=self.run_id, r=rounds)

        hosp_prep.write_hosp_file(df, wpath, backup=self.write_results)

        return df

    def convert_to_int(self, df):
        """
        Parameters
        ----------
        df : pd.DataFrame
            The dataframe to convert.

        Returns
        -------
        pd.DataFrame
            The dataframe after the columns have been converted.

        """
        dfcols = df.columns
        int_cols = ['age_group_id', 'age_start', 'age_end', 'location_id',
                    'sex_id', 'year_id', 'nid', 'estimate_id', 'icg_id', 'metric_id',
                    'diagnosis_id', 'representative_id']
        int_cols = [c for c in int_cols if c in dfcols]
        for col in int_cols:
            df[col] = df[col].astype(int)
        return df

    def chk_data_for_upload(self, df, estimate_table):
        """Confirm data has the exact column names and data types to match what's
        already in the database

        Parameters
        ----------
        df : pd.DataFrame
            Description of parameter `df`.
        estimate_table : str
            Either "intermediate_estimates" or "final_estimates_icg".

        Returns
        -------
        None

        """
        if estimate_table == 'intermediate_estimates':
            exp_cols = ['age_group_id', 'sex_id', 'location_id', 'year_id', 'representative_id',
                        'estimate_id', 'source_type_id', 'icg_id', 'nid', 'run_id',
                        'diagnosis_id', 'val']
            assert df.isnull().sum().sum() == 0, "Nulls in these columns are not expected {}".format(df.isnull().sum())
        elif estimate_table == 'final_estimates_icg':
            exp_cols = ['age_group_id', 'sex_id', 'location_id', 'year_id', 'representative_id', 'estimate_id',
                        'source_type_id', 'diagnosis_id', 'icg_id', 'nid', 'run_id', 'mean', 'lower', 'upper',
                        'sample_size', 'cases']
        else:
            print("What are the expected columns?")

        diff = set(df.columns).symmetric_difference(set(exp_cols))
        assert not diff, "Cols don't match {}".format(diff)

        if estimate_table == 'intermediate_estimates':
            for col in exp_cols:
                if col != 'val':
                    assert df[col].dtype == int, '{} is the wrong data type'.format(col)
                else:
                    assert df[col].dtype == float, '{} is the wrong data type'.format(col)
        elif estimate_table == 'final_estimates_icg':
            for col in exp_cols:
                if col in ['mean', 'upper', 'lower', 'sample_size', 'cases']:
                    assert df[col].dtype == float, '{} is the wrong data type'.format(col)
                else:
                    assert df[col].dtype == int, '{} is the wrong data type'.format(col)
                    assert df[col].isnull().sum() == 0, 'There are null values for some reason'

        print("ready to write {} results for upload to the db".format(estimate_table))
        return

    def write_intermediate(self, df, overwrite=True):
        """Write hospital data post age-sex splitting
        in a clinical run.

        Parameters
        ----------
        df : pd.DataFrame
            The dataframe to write.
        overwrite : bool
            Should data be overwritten? Default is True.

        Returns
        -------
        None

        """

        df.drop(['outcome_id', 'source', 'age_group_unit', 'icg_name',
                 'facility_id', 'metric_id'],
                axis=1, inplace=True)

        df['estimate_id'] = 1  # unadj inp data
        df['source_type_id'] = 10  # inpatient source
        df['run_id'] = self.run_id

        df = self.convert_to_int(df)
        self.chk_data_for_upload(df, estimate_table='intermediate_estimates')

        fpath = FILEPATH.format(rid=self.run_id)
        if not os.path.exists(fpath) or overwrite:
            df.to_csv(fpath, index=False, na_rep='NULL')

        return

    def prep_for_env(self, df):
        """Prepares the dataframe for application of the inpatient envelope.

        Parameters
        ----------
        df : pd.DataFrame
            A dataframe to prep.

        Returns
        -------
        (pd.DataFrame, pd.DataFrame)
            The prepped dataframe and a full coverage dataframe.

        """

        print("Prepping the data for application of the inpatient envelope...")
        df, full_coverage_df = prep_for_env_main(df, run_id=self.run_id,
                                                 env_path=self.env_path,
                                                 gbd_round_id=self.gbd_round_id,
                                                 decomp_step=self.decomp_step,
                                                 clinical_age_group_set_id=self.clinical_age_group_set_id,
                                                 new_env_or_data=True,
                                                 write=self.write_results,
                                                 drop_data=True,
                                                 create_hosp_denom=True,
                                                 fix_norway_subnat=False)

        return df, full_coverage_df

    def pop_estimates(self, df):
        """Transforms our estimates from counts of hospital admissions to
        GBD population level rates

        Parameters
        ----------
        df : pd.DataFrame
            The dataframe.

        Returns
        -------
        pd.DataFrame
            Single year ICG level estimates of inpatient clinical data

        """
        print("Creating Population level estimates: {}".format(self.no_draws_env))
        df = create_icg_single_year_est(df=df,
                                        run_id=self.run_id,
                                        gbd_round_id=self.gbd_round_id,
                                        decomp_step=self.decomp_step,
                                        run_tmp_unc=self.run_tmp_unc,
                                        write=self.write_results,
                                        cf_model_type=self.cf_model_type,
                                        env_path=self.env_path)
        return df

    def write_final_icg(self, df):
        """
        Parameters
        ----------
        df : pd.DataFrame
            Data to write.

        Returns
        -------
        pd.DataFrame
            The passed in `df` after it has been formatted and saved.

        """

        df = self.convert_to_int(df)


        write_path = FILEPATH.format(self.run_id)

        hosp_prep.write_hosp_file(df=df, write_path=write_path, backup=False)

        return df

    def create_bundles(self, df):
        """Creates bundle estimates using the given data.

        Parameters
        ----------
        df : pd.DataFrame
            Data to create estimates with.

        Returns
        -------
        pd.DataFrame
            A dataframe with bundle estimates.

        """
        if self.bundle_level_cfs:
            print("We're sending out ~250 jobs to process the bundle level CF"
                          " draws by age, sex and year bin.")
            df = sb.main_bundle_draw_submit(run_id=self.run_id,
                                    draws=1000,
                                    gbd_round_id=self.gbd_round_id,
                                    decomp_step=self.decomp_step,
                                    clinical_age_group_set_id=self.clinical_age_group_set_id)
        else:
            raise ValueError((f"The inpatient pipeline curently only works with"
                              f" bundle level correction factors. Setting "
                              f"bundle_level_cfs to {self.bundle_level_cfs} "
                              f"needs additional work before running"))


        return df

    def mat_ratio_bundle_validation(self, df, ratio_input_bundles):
        '''
        maternal team wants ratio estimates for existing bundles. They will be
        created in the Uploader but let's add a test here to confirm the data
        they need is present
        '''
        ratio_ests = cm.get_active_bundles(cols=['bundle_id', 'estimate_id'],
                                          bundle_id=ratio_input_bundles,
                                          estimate_id=[7,8,9])
        ratio_ests['rows_present'] = 0

        for b in ratio_input_bundles:
            for est in [7, 8, 9]:
                best_rows = len(df.query(f"bundle_id == {b} and estimate_id == {est}"))
                mask = f"(ratio_ests['bundle_id'] == {b}) & (ratio_ests['estimate_id'] == {est})"
                ratio_ests.loc[eval(mask), 'rows_present'] = best_rows
                del best_rows
        failures = ratio_ests.query("rows_present == 0")
        if len(failures) > 0:
            raise ValueError(f"There are mat ratio inputs missing {failures}")
        else:
            pass

    def confirm_master_data_run(self):
        """Ensure that every master data worker job completed. More specifically we'll
        check for all the NIDs we expect to be present using the is_active column
        from the nid table in our clinical db

        Parameters
        ----------
        run_id : int
            The run ID.

        Returns
        -------
        None

        """
        print("Reviewing active NIDs present in master data ... ")
        # get nids from the run
        base = FILEPATH.format(self.run_id)
        files = glob.glob(base + FILEPATH)

        nids = []
        for f in files:
            tmp = pd.read_hdf(f, columns=['nid'])
            nids += tmp['nid'].unique().tolist()

        engine = clinical_funcs.get_engine()
        db_table = TABLE
        if self.run_id == 5:
            print("GBD2019 decomp 4 is only using partial data")
            d4_nids = (409529, 409530, 283865, 369564, 369579, 333649, 333650, 126516, 126518,
                       126517, 319414, 354240, 150449, 220205, 281773, 354896)
            query = QUERY
        else:
            query = QUERY
        active_niddf = pd.read_sql(con=engine, sql=query)

        diffs = set(active_niddf['nid'].unique()).symmetric_difference(set(nids))
        if diffs:
            print(active_niddf[active_niddf['nid'].isin(diffs)])
            msg = "The nids present in master data don't match the NIDs we expect from the NID db table {}: {}".format(db_table, diffs)
            assert False , msg
        return

    def confirm_env_versions(self):
        """Confirm the environment version.

        Parameters
        ----------


        Returns
        -------
        None

        """
        env = re.sub('_draws', '', os.path.basename(self.env_path)[:-3])  # index to remove .H5
        no_draws = os.path.basename(self.no_draws_env)[:-4]  # index to remove .csv
        assert env == no_draws, "Why aren't the envelopes the same?"
        return

    def check_icd_indv_files(self, pre_mapping):
        """Check individual icd files.

        Parameters
        ----------
        run_id : int
            Run ID.

        Returns
        -------
        None

        """
        base = FILEPATH.format(self.run_id)
        md = glob.glob(base + FILEPATH)
        icd = glob.glob(base + FILEPATH)

        if pre_mapping:
            if icd:
                msg = "There are icd files present in FILEPATH "\
                    "confirm these have been overwritten by your re-run or manually delete "\
                    "otherwise an outdated map could be used"
                warnings.warn(msg)
        else:
            src_diff = (set([os.path.basename(f) for f in  md]).symmetric_difference(
                [os.path.basename(f2) for f2 in icd]))
            if src_diff:
                error_msg = ("There is a set difference between the files output to "
                            "/master_data and those output to FILEPATH. These "
                            f"different file(s) are {src_diff}")
                raise ValueError(error_msg)
        return

    def final_tests(self, df):
        """run some tests on the df and the clinical.active_bundle_metadata table"""
        print("beginning final tests on the inpatient dataframe")

        # Catch duplicated data
        id_cols = ['age_group_id', 'sex_id', 'location_id', 'year_start',
                   'year_end', 'nid', 'bundle_id', 'estimate_id']

        assert len(df) == len(df[id_cols].drop_duplicates()), ("There is "
            "duplicated data. Review the agg to five file for dupes "
           f"in the following columns {id_cols}. Historical, duplication "
            "has been caused by the CFs and is therefore only present in "
            "certain bundle_ids. Maybe check that")


        bundle_check.test_clinical_bundle_est()
        miss = bundle_check.test_refresh_bundle_ests(df=df, pipeline='inp', prod=False)
        exp_miss = [28, 207, 208,
                    6113, 6116, 6119, 6122, 6125,
                    6941, 6944]

        warnings.warn(miss_warn)
        real_miss = miss.copy()
        for key in list(miss.keys()):
            if key in exp_miss:
                del real_miss[key]
        if len(real_miss) > 0:
            print(real_miss)
            assert False, (f"We're breaking because the following "
                           f"bundles/estimates are missing {real_miss}")

        confirm_square_data.validate_test_df(df=df,
                                             clinical_age_group_set_id=self.clinical_age_group_set_id,
                                             run_id=self.run_id)

        print("bundle_id and square data tests have passed")

    def create_bundle_loc_df(self, df, map_version, run_id):
        """
        Parameters
        ---------
        df : Pandas DataFrame
            data right after envelope has been applied and right before
            expanding to bundle
        map_version : str
            The map version.
        run_id : int
            Run ID.

        Returns
        -------
        None
        """

        source_locations = df[['source', 'location_id']].drop_duplicates()

        ndf = df[['source', 'icg_id',
                  'icg_name']].drop_duplicates().copy()
        ndf = clinical_mapping.map_to_gbd_cause(df=ndf, input_type='icg',
                                                output_type='bundle',
                                                write_unmapped=False,
                                                map_version=map_version)
        ndf.drop('bundle_measure', axis=1, inplace=True)
        ndf = ndf.drop_duplicates()

        ndf = ndf.merge(source_locations, how='outer', on=['source'])

        write_path = FILEPATH.format(run_id)
        ndf.to_csv(write_path, index=False)

        return


    def create_eti_est_df(self, df, map_version, run_id, fill_missing_estimates=True):
        """

        Parameters
        ---------
        df : Pandas DataFrame
            data right after envelope has been applied and right before
            expanding to bundle
        map_version : str
            The map version.
        run_id : int
            Run ID.

        Returns
        -------
        None
        """

        eti_est_df = df[['icg_id', 'icg_name', 'estimate_id']].drop_duplicates()
        eti_est_df['keep'] = 1

        eti_est_df = clinical_mapping.expand_bundles(df=eti_est_df, prod=True,
                                                     drop_null_bundles=True,
                                                     map_version=map_version,
                                                     test_merge=True)

        eti_est_df = eti_est_df[['bundle_id', 'estimate_id', 'keep']].drop_duplicates()

        if fill_missing_estimates:

            df_list = [eti_est_df]

            est_dict = {1: [2, 3, 4], 6: [7, 8, 9]}
            for base_est in est_dict:
                for cf_est in est_dict[base_est]:
                    tmp = eti_est_df.query("estimate_id == @base_est").copy()
                    tmp['estimate_id'] = cf_est
                    df_list.append(tmp)
            # now add in injury estimates mainly
            estimates = query(QUERY)
            estimates = tuple(estimates.loc[estimates.estimate_name.str.contains("inp-"), 'estimate_id'].tolist())

            bundles = cm.get_active_bundles(cols=['bundle_id'],
                                            estimate_id=list(estimates))
            bundles['keep'] = 1
            df_list.append(bundles)

            eti_est_df = pd.concat(df_list, sort=False, ignore_index=True)
            eti_est_df.drop_duplicates(inplace=True)
            print("{} shape of eti est df. Should be OK if it's larger than 1208".format(eti_est_df.shape))

        eti_est_df.to_csv(FILEPATH.format(run_id), index=False)

        return

    def validate_age_sex_sources(self, df):
        """Run a set of validations on the process source table and the data"""
        failures = []
        src_rounds = self.get_source_round()
        bad_rounds = set([1, 2, 3]).symmetric_difference(src_rounds['use_in_round'])
        if bad_rounds:
            failures.append(f"The following use_in_round values are bad {bad_rounds}")
        dupe_sources = src_rounds[src_rounds.duplicated(subset='source', keep=False)]
        if len(dupe_sources) > 0:
            failures.append(f"There are duplicated source rounds. {dupe_sources}")
        source_diffs = set(df['source'].unique()).symmetric_difference(src_rounds['source'].unique())
        if source_diffs:
            failures.append(f"There is a misalignment of source names. {source_diffs}")
        if failures:
            m = ("\nThe following tests have failed. The process source table and "
                 "the inpatient data are not aligned.\n")
            m = m + ' -- '.join(failures)
            raise ValueError(m)
        return

    def clone_bundles(self):

        print("Cloning any bundles outlined in the bundle_relationship table")
        out_dir = FILEPATH.format(self.run_id)
        df = pd.read_csv(f"{out_dir}/FILEPATH")
        df.to_csv(f"{out_dir}/FILEPATH", index=False)
        bc = br.BundleClone(run_id=self.run_id)
        df = bc.clone_and_return(df=df)
        df.to_csv(f"{out_dir}/FILEPATH", index=False)

    def automater(self):
        """An inelegant method of skipping code that has already been run depending on
        which HDF files have been created in the process.

        For example, if ICD mapping
        has been run, then this function will tell the Inpatient.main() method to read
        in that hdf and begin processing at age-sex splitting.

        Note: if you want to manually begin the process from some specific place you
              can set the inp.begin_with attribute to the appropriate string, eg
              if you want to re-run prep envelope then it's "inp.begin_with = 'prepare_envelope'"

        Parameters
        ----------


        Returns
        -------
        (list, list, list, list, list, str, bool)
            master_data, icd, age_sex, prep_env, apply_env: (list)
                lists of hdf filepaths to read in if needed
            begin_with: (str)
                plain english string indicating which step in the process the main()
                function should begin with
            read_data: (bool)
                if any of the lists of data above exist, this is set to True
                and the data is read in from HDF

        """

        base = FILEPATH.format(self.run_id)
        master_data = glob.glob(base + FILEPATH)
        icd = glob.glob(base + FILEPATH)
        age_sex = glob.glob(base + FILEPATH)
        prep_env = glob.glob(base + FILEPATH)
        apply_env = glob.glob(base + FILEPATH)
        create_bundle_est = glob.glob(base + FILEPATH)

        read_data = True

        if create_bundle_est:
            self.begin_with = "all done.. ?"
        elif apply_env:
            self.begin_with = 'expand_to_bundle'
        elif prep_env:
            self.begin_with = 'apply_envelope'
        elif age_sex:
            self.begin_with = 'prepare_envelope'
        elif icd:
            self.begin_with = 'age_sex_splitting'
        elif master_data:
            self.begin_with = 'icd_mapping'
            read_data = False
        else:
            self.begin_with = 'master_data'
            read_data = False

        return master_data, icd, age_sex, prep_env, apply_env, create_bundle_est, read_data

    def main(self, control_begin_with=False):
        """Run the inpatient pipeline.

        Parameters
        ----------
        control_begin_with : bool
            Force run from a specific point in the pipeline. Should be used with
            care.

        Returns
        -------
        None
            Description of returned object.

        """

        self.log_stdout()

        possible_begins = ['master_data', 'icd_mapping', 'age_sex_splitting', 'prepare_envelope',
                           'apply_envelope', 'expand_to_bundle']

        pre_begin_with = self.begin_with

        master_data, icd, age_sex, prep_env, apply_env, create_bundle_est, read_data = self.automater()

        if control_begin_with:
            if pre_begin_with not in possible_begins:
                print("The step {} is not understood, please correct to one of these {}".\
                    format(pre_begin_with, possible_begins))
            self.begin_with = pre_begin_with

        if self.begin_with == 'master_data':
            pickle.dump(self, open("{}/FILEPATH".format(self.base), "wb"))

            print("Caching the 5 year HAQi CFs and input data to /../FILEPATH")
            haqi_prep.set_5_year_haqi_cf(
                gbd_round_id=self.gbd_round_id, decomp_step=self.decomp_step,
                run_id=self.run_id, min_treat=0.1, max_treat=0.75)

            print("Initiating master data subroutine...")
            self.master_data()
            self.begin_with = 'icd_mapping'

        if self.begin_with == 'icd_mapping':
            pickle.dump(self, open("{}/FILEPATH".format(self.base), "wb"))
            if self._confirm_master_data:
                self.confirm_master_data_run()
            self.check_icd_indv_files(pre_mapping=True)
            print("Initiating icd mapping subroutine...")
            mapped_df = self.map_to_icg()
            self.check_icd_indv_files(pre_mapping=False)

            # validate age-sex data sources
            self.validate_age_sex_sources(mapped_df)
            self.begin_with = 'age_sex_splitting'

        if self.begin_with == 'age_sex_splitting':
            pickle.dump(self, open("{}/FILEPATH".format(self.base), "wb"))

            print("Initiating age sex splitting subroutine...")
            if read_data:
                assert len(icd) == 1, "Why more than 1 ICD mapping file??"
                mapped_df = pd.read_hdf(icd[0])
                read_data = False

            if "GEO_COL_00_13" in mapped_df.source.unique():
                mapped_df = mapped_df[mapped_df.source != "GEO_COL_00_13"]



            self.age_sex_weights(env_path=self.no_draws_env, back=mapped_df,
                                 make_weights=self.make_age_sex_weights,
                                 round_2_only=False)
            # Run age sex splitting
            df = self.age_sex_split(mapped_df)

            self.begin_with = 'prepare_envelope'
            del mapped_df

        if self.begin_with == 'prepare_envelope':
            pickle.dump(self, open("{}/FILEPATH".format(self.base), "wb"))
            print("Initiating prepare envelope subroutine...")
            self.confirm_env_versions()
            if read_data:
                assert len(age_sex) == 1, "Too many files in age-sex split folder !!!"
                print("reading in age-sex split data")
                df = pd.read_hdf(age_sex[0])
                print("done")
                read_data = False

  
            df.rename(columns={'year_start': 'year_id'}, inplace=True)
            df.drop(['year_end'], axis=1, inplace=True)

            # write data for intermediate use in db
            self.write_intermediate(df.copy(), overwrite=False)  # only needs to be done once

            # prep env
            df, full_coverage_df = self.prep_for_env(df.copy())
            self.begin_with = 'apply_envelope'

        if self.begin_with == 'apply_envelope':
            pickle.dump(self, open("{}/FILEPATH".format(self.base), "wb"))
            print("Initiating apply envelope subroutine...")

            # create maternal denominators
            write_maternal_denom(denom_type='ifd_asfr',
                                 run_id=self.run_id,
                                 gbd_round_id=self.gbd_round_id,
                                 decomp_step=self.decomp_step,
                                 clinical_age_group_set_id=self.clinical_age_group_set_id)

            if read_data:
                assert len(prep_env) == 2, "too many prep env files"
                df = pd.read_hdf(sorted(prep_env)[0])
                if self.run_id == 5:
                    full_coverage_df = pd.DataFrame()
                else:
                    full_coverage_df = pd.read_hdf(sorted(prep_env)[1])
                read_data = False

            df = pd.concat([df, full_coverage_df], sort=False, ignore_index=True)
            del full_coverage_df

            du.confirm_age_group_set(age_col=df['age_group_id'].unique(),
                                     clinical_age_group_set_id=\
                                         self.clinical_age_group_set_id,
                                     full_match=True)

            # apply correction factors, adjust maternal denoms
            df = self.pop_estimates(df)
            # Checked that all merged NIDs are present before agg to 5
            chk_cols = ['source', 'nid', 'year_id']
            self.check_merged_nids(df[chk_cols].drop_duplicates().copy())

            # if nid check succeeds then drop the cols it needed
            df = df.drop(['nid', 'year_id'], axis=1).drop_duplicates()

            df = self.write_final_icg(df)
            print(df.columns)
            assert 'source' in df.columns, "'source' must be in the dataframe"

            self.create_bundle_loc_df(df=df, map_version=self.map,
                                      run_id=self.run_id)

            self.create_eti_est_df(df=df, map_version=self.map,
                                   run_id=self.run_id)

            self.begin_with = 'expand_to_bundle'
            self.read_data = False


        if self.begin_with == 'expand_to_bundle':
            pickle.dump(self, open("{}/FILEPATH".format(self.base), "wb"))
            print((f"Initiating expand to bundle subroutine. The CF aggregation "
                   f"method will be {self.cf_agg_stat}"))
            if self.bundle_level_cfs:
                # check inputs real quick
                bcf_files = glob.glob(self.base + "/FILEPATH")

                age_len = len(hosp_prep.get_hospital_age_groups(clinical_age_group_set_id=self.clinical_age_group_set_id))
                bcfexp_file_count = 2 * age_len * 2
                if len(bcf_files) != bcfexp_file_count:
                    raise ValueError(f"We expect {bcfexp_file_count} split ICG files but there are only {len(bcf_files)}")

                a, s, y = sb.get_demos(all_years=True,
                                       clinical_age_group_set_id=self.clinical_age_group_set_id)
                exp_files = len(a) * len(s) * len(y)
                prep_files, ob_final_files = sb.ob_file_getter(self.run_id)
                if len(prep_files) == len(ob_final_files) == exp_files:

                    final_files = sb.ob_file_getter(run_id=self.run_id,
                                                    for_reading=True)
                    df = pd.concat([pd.read_csv(f) for f in final_files],
                                    sort=False,
                                    ignore_index=True)
                else:  # send out the parallel jobs
                    df = self.create_bundles(df=pd.DataFrame())


                if self.cf_agg_stat == 'median':

                    df.loc[df['lower'].notnull(), 'mean'] =\
                        df.loc[df['lower'].notnull(), 'median_CI_team_only']
                else:
                    pass
                drops = ['median_CI_team_only', 'use_draws']
                drops = [d for d in drops if d in df.columns]
                df.drop(drops, axis=1, inplace=True)

            else:
                if read_data:
                    assert len(apply_env) == 1, "Too many files produced by apply env"
                    df = pd.concat([pd.read_hdf(f) for f in apply_env])
                    read_data = False

                df = self.create_bundles(df=df)              

            # make the ratio maternal bundles
            self.mat_ratio_bundle_validation(df=df,
                                             ratio_input_bundles=[75, 76, 825, 6107, 6110])

            # Ensure that ALL age group IDs made it
            du.confirm_age_group_set(age_col=df['age_group_id'].unique(),
                                     clinical_age_group_set_id=\
                                         self.clinical_age_group_set_id,
                                     full_match=True)


            # write the bundle level CFs to /share
            if self.bundle_level_cfs:
                bundle_cf_path = FILEPATH.format(self.run_id)
                hosp_prep.write_hosp_file(df=df, write_path=bundle_cf_path, backup=True)

            # run our final bundle tests
            self.final_tests(df)

            # aggregate the U1 data to 0-1 and live birth rates
            years = df.year_start.sort_values().unique().tolist()
            re_aggregate_under1.create_u1_and_lb_aggs(run_id=self.run_id,
                                                      gbd_round_id=self.gbd_round_id,
                                                      decomp_step=self.decomp_step,
                                                      input_age_set=self.clinical_age_group_set_id,
                                                      year_start_list=years,
                                                      cf_agg_stat=self.cf_agg_stat,
                                                      full_coverage_sources=hosp_prep.full_coverage_sources(),
                                                      lb_agid=164)

            clean_final_bundle.inp_main(gbd_round_id=self.gbd_round_id,
                                         clinical_decomp_step=self.clinical_decomp_step,
                                         run_id=self.run_id)

            self.clone_bundles()

            self.begin_with = 'all done.. ?'

        if self.begin_with == "all done.. ?":
            pickle.dump(self, open("{}/FILEPATH".format(self.base), "wb"))

            bundle_path = FILEPATH.format(self.run_id)
            icg_path = FILEPATH.format(self.run_id)

            print("The inpatient process has finished running.\n"\
                  "Single year ICG level estimates are present in:\n{i}\n"\
                  "Five year bundle level estimates are present in:\n{b}".\
                  format(i=icg_path, b=bundle_path))

        return
