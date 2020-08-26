"""
Class that runs inpatient pipeline
"""

import pandas as pd
import numpy as np
import time
import sys
import getpass
import glob
import warnings
import ipdb
import os
import sqlalchemy
import functools
import multiprocessing
import pickle
import warnings
from db_tools.ezfuncs import query

user = getpass.getuser()
repo_dir = ['Functions', "FILEPATH", "FILEPATH",
            'Envelope', 'Mapping', 'AgeSexSplitting']
codebase = r"FILEPATH".format(user)
list(map(sys.path.append, [codebase + e for e in repo_dir]))

import clinical_funcs
import hosp_prep
import gbd_hosp_prep
import clinical_mapping
import subprocess
import re
import standardize_format as stnd_fmt
import fmt_sources_master as fmt_master
import submit_icd_mapping
from prep_for_env import prep_for_env_main
from apply_env_only import apply_envelope_only
from compute_weights import compute_weights
from run_age_sex_splitting import run_age_sex_splitting
from icg_apply_env import create_icg_single_year_est, pooled_writer
from create_bundle_estimates import create_bundle_estimates
from submit_bundle_draw_estimates import main_bundle_draw_submit
import bundle_check


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
        The GBD round ID. Default is 6.
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
    def __init__(self, run_id, write_results=True, map_version="current"):
        self.map = map_version
        self.run_id = run_id
        self.base = "FILEPATH".format(self.run_id)
        self.write_results = write_results
        self.gbd_round_id = 6
        self.decomp_step = ""
        self.env_path = "NULL"
        self.no_draws_env = "NULL"
        self.env_stgpr_id = -1
        self.env_me_id = -1
        self.cf_version_id = -1
        self.run_tmp_unc = True
        self.begin_with = "Thanks for initalizing me!"
        self.set_map_int_version = clinical_mapping.test_and_return_map_version(self.map, prod=True)
        self.cf_model_type = 'mr-brt'
        self.bundle_level_cfs = True
        self.make_age_sex_weights = True
        self.weight_squaring_method = 'bundle_source_specific'
        warnings.warn("At points this class will run code that uses multiprocessing with at most 15 pools. If ran on fair cluster then it needs to be ran with at least 15 threads.")

    def __str__(self):
        display = "The next step of the inpatient pipeline will be {}".format(self.begin_with)
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
        return display

    def __repr__(self):
        display = "The next step of the inpatient pipeline will be {}".format(self.begin_with)
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
        return display

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

        print("Mapped data is ready. Checking merged NIDs")


        df = df[df.year_start > 1989].copy()
        df = df[(df.diagnosis_id == 1) & (df.facility_id.isin(['hospital', 'inpatient unknown']))]

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

        Parameters
        ----------


        Returns
        -------
        None

        """
        warnings.warn("Ensure that the worker scripts to compare master data are updated to adjust for neonatal age groups!!")

        if self.run_id == 5:
            gbd2019_decomp4 = True
            print("Running with only a subset of data sources for gbd2019 decomp4")
        else:
            gbd2019_decomp4 = False
        fmt_master.main(self.run_id, gbd2019_decomp4=gbd2019_decomp4)

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

        Returns
        -------
        pd.DataFrame
            The master data in icg space via a pandas dataframe.

        """
        df = submit_icd_mapping.icd_mapping(en_proportions=True, create_en_matrix_data=False,
                                            save_results=self.write_results, write_log=True, deaths='non', extra_name="",
                                            run_id=self.run_id, map_version=self.map)

        return df

    def age_sex_weights(self, back, env_path, make_weights,
                        round_2_only=False, overwrite_weights=True, rounds=2):
        """
        Function to create the age-sex weights using 1 or 2 rounds


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
            weight_path = "FILEPATH".format(self.base)
            if os.path.exists(weight_path):
                return
            else:
                assert False, "The weights don't appear to be present at {}".format(weight_path)


        source_round_df = pd.read_csv(self.base + "FILEPATH"\
                                    "FILEPATH")


        source_diff = set(back['source'].unique()) - set(source_round_df['source'].unique())
        assert source_diff == set(),\
            "Please add {} to the list of sources and which round to use it in".format(source_diff)

        round_1_newdrops = source_round_df.loc[source_round_df.use_in_round > 1, 'source']
        round_2_newdrops = source_round_df.loc[source_round_df.use_in_round > 2, 'source']

        if (not round_2_only) or (make_weights):
            print('Beginning the first round of creating age-sex weights')
            df = back.copy()

            df = df[~df.source.isin(round_1_newdrops)].copy()
            df = apply_envelope_only(df, run_id=self.run_id, env_path=env_path,
                                     apply_age_sex_restrictions=True,
                                     want_to_drop_data=True, create_hosp_denom=False,
                                     gbd_round_id=self.gbd_round_id, decomp_step=self.decomp_step)
            compute_weights(df, run_id=self.run_id, round_id=1, fill_gaps=False,
                            gbd_round_id=self.gbd_round_id, decomp_step=self.decomp_step,
                            overwrite_weights=overwrite_weights,
                            squaring_method=self.weight_squaring_method)

        if (round_2_only) or (make_weights and rounds == 2):
            print('Beginning the second round of creating age-sex weights')
            df = back.copy()

            df = df[~df.source.isin(round_2_newdrops)].copy()


            df = run_age_sex_splitting(df, run_id=self.run_id, gbd_round_id=self.gbd_round_id,
                                       decomp_step=self.decomp_step, verbose=True, round_id=2,
                                       write_viz_data=False)

            df = apply_envelope_only(df, run_id=self.run_id,
                                     env_path=env_path,
                                     apply_age_sex_restrictions=True,
                                     want_to_drop_data=True,
                                     create_hosp_denom=False,
                                     gbd_round_id=self.gbd_round_id,
                                     decomp_step=self.decomp_step)


            compute_weights(df, run_id=self.run_id, gbd_round_id=self.gbd_round_id,
                            decomp_step=self.decomp_step, round_id=2,
                            fill_gaps=False, overwrite_weights=overwrite_weights,
                            squaring_method=self.weight_squaring_method)
        return

    def convert_obsolete_special_map_icgs(self, df):
        """Converts the out of use ICGs from special maps to ones currently in ICD 9/10 mapping

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


        df.drop('icg_measure', axis=1, inplace=True)

        df = self.convert_obsolete_special_map_icgs(df)

        df = df.groupby(df.columns.drop('val').tolist()).agg({'val': 'sum'}).reset_index()
        df = run_age_sex_splitting(df, run_id=self.run_id, gbd_round_id=self.gbd_round_id,
                                   decomp_step=self.decomp_step, verbose=True, round_id=rounds,
                                   write_viz_data=False)
        wpath = "FILEPATH"\
            "FILEPATH".format(rid=self.run_id, r=rounds)

        hosp_prep.write_hosp_file(df, wpath, backup=self.write_results)

        return df

    def convert_to_int(self, df):
        """Convert int expected cols to int

        Pandas can't store missing values as int cols, so it's constantly
        converting ints to floats. This is an issue with the db.

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
        """Write hospital data post age-sex splitting to the for_upload folder
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

        df['estimate_id'] = 1
        df['source_type_id'] = 10
        df['run_id'] = self.run_id

        df = self.convert_to_int(df)
        self.chk_data_for_upload(df, estimate_table='intermediate_estimates')

        fpath = "FILEPATH"\
            "FILEPATH".format(rid=self.run_id)
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
                                                 new_env_or_data=True,
                                                 write=self.write_results,
                                                 drop_data=True,
                                                 create_hosp_denom=True,
                                                 fix_norway_subnat=False)

        return df, full_coverage_df

    def apply_env(self, df, full_coverage_df):
        """Applies the inpatient envelope using the passed in dataframes.

        Parameters
        ----------
        df : pd.DataFrame
            The dataframe.
        full_coverage_df : pd.DataFrame
            A full coverage dataframe.

        Returns
        -------
        pd.DataFrame
            Single year ICG level estimates of inpatient clinical data

        """
        print("Applying the inpatient envelope: {}".format(self.no_draws_env))
        df_env = create_icg_single_year_est(df, full_coverage_df,
                                            no_draws_env_path=self.no_draws_env,
                                            run_id=self.run_id,
                                            gbd_round_id=self.gbd_round_id,
                                            decomp_step=self.decomp_step,
                                            run_tmp_unc=self.run_tmp_unc,
                                            write=self.write_results,
                                            cf_model_type=self.cf_model_type,
                                            bundle_level_cfs=self.bundle_level_cfs)
        return df_env

    def write_final_icg_splits(self, df, run_id, verbose=False):
        """Split the final ICG estimates up by age/sex and 5 year start value

        Parameters
        ----------
        df : pd.DataFrame
                Data to split and write
        run_id : int
            The run ID.

        Returns
        -------
        None
        """
        base = "FILEPATH".format(run_id)
        df['year_start'] = df['year_id']
        df['year_end'] = df['year_start']
        df = hosp_prep.year_binner(df)
        for age_group in df.age_group_id.unique():
            for sex in df.sex_id.unique():
                for year in df.year_start.unique():
                    tmp = df.query("age_group_id == @age_group & sex_id == @sex & year_start == @year").copy()
                    tmp.drop(['year_start', 'year_end'], axis=1, inplace=True)
                    write_path = "FILEPATH".format(base, age_group, sex, year)
                    hosp_prep.write_hosp_file(tmp, write_path, backup=False)
                    if verbose:
                        print("Done writing {}_{}_{}".format(age_group, sex, year))
        df.drop(['year_start', 'year_end'], axis=1, inplace=True)
        return

    def write_final_icg(self, df):
        """Writes the final icg.

        Parameters
        ----------
        df : pd.DataFrame
            Data to write.

        Returns
        -------
        pd.DataFrame
            The passed in `df` after it has been formatted and saved.

        """

        """apply the icg_estimate formatting we need here-
        do the same thing you did before, compare the database to the flat file!!
        """

        drops = [d for d in ['age_group_unit', 'facility_id', 'estimate_type',
                 'metric_id'] if d in df.columns]
        df.drop(drops, axis=1, inplace=True)
        df['run_id'] = self.run_id
        df['source_type_id'] = 10
        df['diagnosis_id'] = 1
        df['cases'] = np.nan

        df = self.convert_to_int(df)

        self.chk_data_for_upload(df.drop(['source', 'icg_name'], axis=1), estimate_table='final_estimates_icg')
        write_path = "FILEPATH".format(self.run_id)
        print("Writing the H5 file for the clinical team's use to \n{}".format(write_path))
        hosp_prep.write_hosp_file(df=df, write_path=write_path, backup=False)


        to_return = df.copy()

        self.write_final_icg_splits(df, self.run_id)

        df.drop(['source', 'icg_name'], axis=1, inplace=True)
        self.chk_data_for_upload(df, estimate_table='final_estimates_icg')


        print("Creating a list of DFs to write CSVs using multiproc")
        write_list = []
        df = df.groupby(['age_group_id', 'sex_id'])
        for x, y in df:
            write_list.append(y)

        print("Beginning to write CSV data")
        warnings.warn("The function write_final_icg uses multiprocessing with 10 pools. If ran on fair cluster then it needs to be ran with at least 10 threads.")
        p = multiprocessing.Pool(10)
        partial_writer = functools.partial(pooled_writer, run_id=self.run_id)
        p.map(partial_writer, write_list)
        del write_list
        return to_return

    def create_bundles(self, df, run_id, bundle_level_cfs):
        """Creates bundle estimates using the given data.

        Parameters
        ----------
        df : pd.DataFrame
            Data to create estimates with.
        run_id : int
            The run ID.
        bundle_level_cfs : Bool
            re-create bundle envelope uncertainty draws to apply to the
            correction factor draws

        Returns
        -------
        pd.DataFrame
            A dataframe with bundle estimates.

        """
        if bundle_level_cfs:
            print("We're sending out ~250 jobs to process the bundle level CF"
                          " draws by age, sex and year bin.")
            df = main_bundle_draw_submit(run_id=self.run_id,
                                    draws=1000,
                                    gbd_round_id=self.gbd_round_id,
                                    decomp_step=self.decomp_step)
        else:
            df = create_bundle_estimates(df=df,
                                         run_id=self.run_id,
                                         gbd_round_id=self.gbd_round_id,
                                         decomp_step=self.decomp_step)

        return df

    def mat_ratio_bundles(self, df):
        '''
        maternal ratio estimates for existing bundles.
        '''

        hyper = df[df.bundle_id == 75]
        eclampsia = df[df.bundle_id == 76]
        severe_pre = df[df.bundle_id == 667]
        pre = df[df.bundle_id == 6107]
        hellp = df[df.bundle_id == 6110]

        df_list = []
        for e in range(7, 10, 1):
            df_1 = self.mat_ratio_bundles_worker(num=eclampsia, denom=severe_pre,
                                                 bundle_id=6113, estimate=e)
            df_list.append(df_1)

            df_2 = self.mat_ratio_bundles_worker(num=eclampsia, denom=pre,
                                                 bundle_id=6116, estimate=e)
            df_list.append(df_2)

            df_3 = self.mat_ratio_bundles_worker(num=pre, denom=hyper,
                                                 bundle_id=6119, estimate=e)
            df_list.append(df_3)

            df_4 = self.mat_ratio_bundles_worker(num=hellp, denom=pre,
                                                 bundle_id=6122, estimate=e)
            df_list.append(df_4)

            df_5 = self.mat_ratio_bundles_worker(num=severe_pre, denom=pre,
                                                 bundle_id=6125, estimate=e)
            df_list.append(df_5)

        df_mat = pd.concat(df_list, sort=True)

        msg = 'There should be no null estimates'
        assert df_mat['mean'].isnull().sum() == 0, msg

        final = pd.concat([df, df_mat], sort=True)
        return final

    def mat_ratio_bundles_worker(self, denom, num, bundle_id, estimate):
        denom = denom[denom.estimate_id == estimate]
        num = num[num.estimate_id == estimate]


        cols = ['location_id', 'year_start', 'year_end', 'age_group_id', 'sex_id', 'nid',
                'representative_id', 'estimate_id', 'haqi_cf', 'source', 'measure']
        suffix = ['_denom', '_num']
        mat_ss = pd.merge(denom, num, on = cols, how='inner', suffixes=suffix)

        msg = 'We expect the sample size to be the same across age/sex/loc/year/source'
        assert mat_ss[mat_ss.sample_size_denom != mat_ss.sample_size_num].shape[0] == 0, msg
        del mat_ss


        m = pd.merge(denom, num, how='inner', on=cols, suffixes=suffix)


        m = m[m.mean_denom > 0]
        m = m[m.mean_num > 0]

        bid_num = num.bundle_id.unique().item()
        bid_denom = denom.bundle_id.unique().item()


        m['ratio'] = m.loc[(m['bundle_id_num'] == bid_num) & (m.estimate_id == estimate),'mean_num'] /\
        m.loc[(m.bundle_id_denom== bid_denom) & (m.estimate_id == estimate), 'mean_denom']


        drop = {e if e.endswith('_denom') else 'no' for e in m.columns}
        drop.remove('no')
        drop = list(drop) + ['mean_num']

        m.drop(columns=drop, axis=1, inplace=True)
        m.rename({'ratio': 'mean', 'upper_num': 'upper', 'lower_num': 'lower', 'sample_size_num': 'sample_size',
                    'bundle_id_num' : 'bundle_id', 'correction_factor_num': 'correction_factor'},
                    axis=1, inplace=True)
        m['upper'] = np.nan
        m['lower'] = np.nan
        m['correction_factor'] = np.nan
        m['bundle_id'] = bundle_id
        return m

    def confirm_master_data_run(self, run_id):
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

        base = "FILENAME".format(self.run_id)
        files = glob.glob(base + "FILEPATH")

        nids = []
        for f in files:
            tmp = pd.read_hdf(f, columns=['nid'])
            nids += tmp['nid'].unique().tolist()
            del tmp

        engine = clinical_funcs.get_engine()
        db_table = "SQL"
        if run_id == 5:
            print("GBD2019 decomp 4 is only using partial data")
            d4_nids = (409529, 409530, 283865, 369564, 369579, 333649, 333650, 126516, 126518,
                       126517, 319414, 354240, 150449, 220205, 281773, 354896)
            query = "SQL".format(db_table, d4_nids)
        else:
            query = "SQL".format(db_table)
        active_niddf = pd.read_sql("DB QUERY")


        active_exclusion = ['NOR_ICPC']
        active_niddf = active_niddf[~active_niddf['source_name'].isin(active_exclusion)]

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
        env = re.sub('_draws', '', os.path.basename(self.env_path)[:-3])
        no_draws = os.path.basename(self.no_draws_env)[:-4]
        assert env == no_draws, "Why aren't the envelopes the same?"
        return

    def check_icd_indv_files(self, run_id):
        """Check individual icd files.

        Parameters
        ----------
        run_id : int
            Run ID.

        Returns
        -------
        None

        """
        base = "FILENAME".format(self.run_id)
        icd = glob.glob(base + "FILEPATH")
        if icd:
            msg = "There are icd files present in FILEPATH "
                  "confirm these have been overwritten by your re-run or manually delete "\
                  "otherwise an outdated map could be used"
            warnings.warn(msg)
        return

    def final_tests(self, df):
        """run some tests on the df and the clinical.bundle table"""
        print("beginning final tests on the inpatient dataframe")


        bundle_check.test_clinical_bundle_est()

        miss = bundle_check.test_refresh_bundle_ests(df=df, pipeline='inp', prod=False)
        exp_miss = [53, 61, 93, 181, 195, 196, 198, 208, 213, 762, 763, 764, 765, 766]
        real_miss = miss.copy()
        for key in list(miss.keys()):
            if key in exp_miss:
                del real_miss[key]
        if len(real_miss) > 0:
            print(real_miss)
            assert False, "We're breaking because the above bundles/estimates are missing"

        print("bundle_id and square data tests have passed")

    def create_bundle_loc_df(self, df, map_version, run_id):
        """Creates a file that allows us to properly square the data
        in the expand bundle step.

        We parallelize that step a lot. Each parallel job doesn't have access
        to the other jobs, so can't know all locations/bundle there are data
        for.  This file is made prior to splitting up the data into parallel
        jobs, and each job reads this file in so that it can know what data
        exists.

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

        ndf = df[['source', 'icg_id',
                  'icg_name', 'location_id']].drop_duplicates().copy()
        ndf = clinical_mapping.map_to_gbd_cause(df=ndf, input_type='icg',
                                                output_type='bundle',
                                                write_unmapped=False,
                                                map_version=map_version)
        ndf.drop('bundle_measure', axis=1, inplace=True)
        ndf = ndf.drop_duplicates()
        write_path = "FILEPATH"\
                     "FILEPATH".format(run_id)
        ndf.to_csv(write_path, index=False)

        return


    def create_eti_est_df(self, df, map_version, run_id, fill_missing_estimates=True):
        """Creates a file that allows us to properly square the data
        in the expand bundle step.

        We parallelize that step a lot. Each parallel job doesn't have access
        to the other jobs, so can't know all estimate_ids/bundle there are data
        for.  This file is made prior to splitting up the data into parallel
        jobs, and each job reads this file in so that it can know what data
        exists.

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
            """There are still issues with parent injuries and bundle ratios that are present
               here but not in the eti_est df from older runs"""
            df_list = [eti_est_df]


            est_dict = {1: [2, 3, 4], 6: [7, 8, 9]}
            for base_est in est_dict:
                for cf_est in est_dict[base_est]:
                    tmp = eti_est_df.query("estimate_id == @base_est").copy()
                    tmp['estimate_id'] = cf_est
                    df_list.append(tmp)

            estimates = query("SQL", conn_def='epi')
            estimates = tuple(estimates.loc[estimates.estimate_name.str.contains("inp-"), 'estimate_id'].tolist())

            bundles = query(f"SQL", conn_def='epi')
            bundles['keep'] = 1
            df_list.append(bundles)

            eti_est_df = pd.concat(df_list, sort=False, ignore_index=True)
            eti_est_df.drop_duplicates(inplace=True)
            print("{} shape of eti est df. Should be OK if it's larger than 1208".format(eti_est_df.shape))

        eti_est_df.to_csv("FILENAME"
                          "FILEPATH"
                          "FILEPATH".format(run_id), index=False)

        return


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

        base = "FILENAME".format(self.run_id)
        master_data = glob.glob(base + "FILEPATH")
        icd = glob.glob(base + "FILEPATH")
        age_sex = glob.glob(base + "FILEPATH")
        prep_env = glob.glob(base + "FILEPATH")
        apply_env = glob.glob(base + "FILEPATH")
        create_bundle_est = glob.glob(base + "FILEPATH")

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
            pickle.dump(self, open("FILEPATH".format(self.base), "wb"))
            print("Initiating master data subroutine...")
            self.master_data()
            self.begin_with = 'icd_mapping'

        if self.begin_with == 'icd_mapping':
            pickle.dump(self, open("FILEPATH".format(self.base), "wb"))
            self.confirm_master_data_run(run_id=self.run_id)
            self.check_icd_indv_files(run_id=self.run_id)
            print("Initiating icd mapping subroutine...")
            mapped_df = self.map_to_icg()
            self.begin_with = 'age_sex_splitting'

        if self.begin_with == 'age_sex_splitting':
            pickle.dump(self, open("FILEPATH".format(self.base), "wb"))

            print("Initiating age sex splitting subroutine...")
            if read_data:
                assert len(icd) == 1, "Why more than 1 ICD mapping file??"
                mapped_df = pd.read_hdf(icd[0])
                read_data = False


            self.check_merged_nids(mapped_df.copy())


            if "GEO_COL_00_13" in mapped_df.source.unique():
                mapped_df = mapped_df[mapped_df.source != "GEO_COL_00_13"]


            self.age_sex_weights(env_path=self.no_draws_env, back=mapped_df, make_weights=self.make_age_sex_weights)

            df = self.age_sex_split(mapped_df)

            self.begin_with = 'prepare_envelope'
            del mapped_df

        if self.begin_with == 'prepare_envelope':
            pickle.dump(self, open("FILEPATH".format(self.base), "wb"))
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


            self.write_intermediate(df.copy(), overwrite=False)


            df, full_coverage_df = self.prep_for_env(df.copy())
            self.begin_with = 'apply_envelope'

        if self.begin_with == 'apply_envelope':
            pickle.dump(self, open("FILEPATH".format(self.base), "wb"))
            print("Initiating apply envelope subroutine...")
            if read_data:
                assert len(prep_env) == 2, "too many prep env files"
                df = pd.read_hdf(sorted(prep_env)[0])
                if self.run_id == 5:
                    full_coverage_df = pd.DataFrame()
                else:
                    full_coverage_df = pd.read_hdf(sorted(prep_env)[1])
                read_data = False

            if self.run_id != 5:

                assert 'UK_HOSPITAL_STATISTICS' not in df['source'].unique(), "UTLA data should not be present"
                assert 'UK_HOSPITAL_STATISTICS' in full_coverage_df['source'].unique(), "UTLA data should be present"


            df = self.apply_env(df, full_coverage_df)
            del full_coverage_df
            df = self.write_final_icg(df)
            print(df.columns)
            assert 'source' in df.columns, "'source' must be in the dataframe"





            self.create_bundle_loc_df(df=df, map_version=self.map,
                                      run_id=self.run_id)



            self.create_eti_est_df(df=df, map_version=self.map,
                                   run_id=self.run_id)

            self.begin_with = 'expand_to_bundle'



        if self.begin_with == 'expand_to_bundle':
            pickle.dump(self, open("FILEPATH".format(self.base), "wb"))
            print("Initiating expand to bundle subroutine...")
            if self.bundle_level_cfs:

                bcf_files = glob.glob(self.base + "FILEPATH")
                if self.run_id != 5:
                    assert len(bcf_files) > 250, "Why are there less than 250 split ICG files?"

                df = self.create_bundles(df=pd.DataFrame(),
                                         run_id=self.run_id,
                                         bundle_level_cfs=self.bundle_level_cfs)

                drops = ['median_CI_team_only', 'use_draws']
                drops = [d for d in drops if d in df.columns]
                df.drop(drops, axis=1, inplace=True)

            else:
                if read_data:
                    assert len(apply_env) == 1, "Too many files produced by apply env"
                    df = pd.concat([pd.read_hdf(f) for f in apply_env])
                    read_data = False

                df = self.create_bundles(df=df,
                                         run_id=self.run_id,
                                         bundle_level_cfs=self.bundle_level_cfs)


            df = self.mat_ratio_bundles(df=df)


            if self.bundle_level_cfs:
                bundle_cf_path = "FILEPATH"\
                                 "FILEPATH".format(self.run_id)
                hosp_prep.write_hosp_file(df=df, write_path=bundle_cf_path, backup=True)


            self.final_tests(df)

            self.begin_with = 'all done.. ?'

        if self.begin_with == "all done.. ?":
            pickle.dump(self, open("FILEPATH".format(self.base), "wb"))

            bundle_path = "FILEPATH"\
                          "FILENAME".format(self.run_id)
            icg_path = "FILEPATH"\
                       "FILENAME".format(self.run_id)

            print("The inpatient process has finished running.\n"\
                  "Single year ICG level estimates are present in:\n{i}\n"\
                  "Five year bundle level estimates are present in:\n{b}".\
                  format(i=icg_path, b=bundle_path))

        return
