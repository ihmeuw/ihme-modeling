"""
Class that runs inpatient pipeline
"""

import getpass
import glob
import os
import pickle
import re
import subprocess
import sys
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from crosscutting_functions.clinical_metadata_utils.pipeline_wrappers import (
    InpatientWrappers,
)
from crosscutting_functions.db_connector.database import Database
from crosscutting_functions import demographic, general_purpose, legacy_pipeline
from crosscutting_functions.pipeline import get_release_id
from crosscutting_functions.mapping import clinical_mapping, clinical_mapping_db
from db_tools.ezfuncs import query

from inpatient.AgeSexSplitting.compute_weights import compute_weights
from inpatient.AgeSexSplitting.run_age_sex_splitting import run_age_sex_splitting
from inpatient.Clinical_Runs import clean_final_bundle
from inpatient.Clinical_Runs.utils.cache_inpatient_envelope import (
    write_inp_envelope_to_run,
)
from inpatient.Clinical_Runs.utils.constants import InpRunConstants, RunDBSettings
from inpatient.Clinical_Runs.utils.create_splitting_report import send_report
from inpatient.Clinical_Runs.utils.inp_gbd15_sources import gbd_15_sources
from inpatient.Clinical_Runs.utils.prep_inpatient_maternal_data import (
    adjust_mat_denom,
    write_maternal_denom,
)
from CorrectionsFactors import haqi_prep
from inpatient.Envelope import apply_bundle_cfs, re_aggregate_under1
from inpatient.Envelope.apply_env_only import apply_envelope_only
from inpatient.Envelope.bundle_estimates import (
    submit_bundle_draw_estimates as sb,  
)
from inpatient.Envelope.create_population_estimates.submit_pop_ests import (
    create_icg_single_year_est,
)
from inpatient.Envelope.prep_for_env import prep_for_env_main
from inpatient.Formatting.all_sources import fmt_sources_master as fmt_master

from crosscutting_functions import bundle_check
from crosscutting_functions.clinfo_stdout_logger import ClinfoLogger
from inpatient.Mapping import submit_icd_mapping
from inpatient.Envelope import confirm_square_data


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
    env_path : str
        The filepath for the envelope to use when going from count to rate
        space.
    no_draws_env_path : str
        The filepath for the no draws envelope.
    env_stgpr_id : int
        Default is pulled from database metadata.
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
        clinical_mapping_db.test_and_return_map_version.
    run_id
    write_results
    bundle_level_cfs : bool
        Indicating whether CF data should be re-run at the end of the pipeline using
        Correction Factors created at the bundle level. This is probably the most
        methodologically correct version when set to True b/c it creates ICG level
        draws for mean0 and then retains draws through bundle aggregation, 5 year agg
        and CF application

    """

    def __init__(self, run_id, clinical_age_group_set_id, map_version):
        iw = InpatientWrappers(run_id, RunDBSettings.iw_profile)
        self.env_stgpr_id = iw.pull_envelope_model_id()

        self.map = map_version
        self.run_id = run_id
        self.clinical_age_group_set_id = clinical_age_group_set_id
        self.base = FILEPATH.format(self.run_id)
        self.age_sex_weights_version_id = None
        self.env_path = (FILEPATH
        )
        self.no_draws_env_path = (FILEPATH
        )
        self.begin_with = "please run the automater method to find out"
        self.set_map_int_version = clinical_mapping_db.test_and_return_map_version(
            self.map, prod=True
        )
        self.make_weights = InpRunConstants.make_weights
        self.remove_live_births = InpRunConstants.remove_live_births
        self.bin_years = InpRunConstants.bin_years
        self.que = InpRunConstants.que
        self.cf_agg_stat = InpRunConstants.cf_agg_stat
        self.write_results = InpRunConstants.write_results
        self.bundle_level_cfs = InpRunConstants.bundle_level_cfs
        self.draws = InpRunConstants.draws
        self.run_tmp_unc = InpRunConstants.run_tmp_unc
        self.weight_squaring_method = InpRunConstants.weight_squaring_method

    def __str__(self):
        display = "GBD release id {}".format(get_release_id(run_id=self.run_id))
        display += "\nThis run wil use clinical age group set id {}".format(
            self.clinical_age_group_set_id
        )
        display += "\nThe next step of the inpatient pipeline will be: {}".format(
            self.begin_with
        )
        display += "\nrun_id is {}".format(self.run_id)
        display += "\nfiles will be saved to {}".format(self.base)
        display += "\nusing map {}".format(self.map)
        display += "\nwhich currently is map version {}".format(self.set_map_int_version)
        display += "\nthe envelope with draws is versioned {}".format(self.env_path)
        display += "\nthe envelope without draws is versioned {}".format(
            self.no_draws_env_path
        )
        display += f"\n Inpatient Run Constants {InpRunConstants()}"
        return display

    def __repr__(self):
        display = "GBD release id {}".format(get_release_id(run_id=self.run_id))
        display += "\nThis run wil use clinical age group set id {}".format(
            self.clinical_age_group_set_id
        )
        display += "\nThe next step of the inpatient pipeline will be: {}".format(
            self.begin_with
        )
        display += "\nrun_id is {}".format(self.run_id)

        display += "\nfiles will be saved to {}".format(self.base)
        display += "\nusing map {}".format(self.map)
        display += "\nwhich currently is map version {}".format(
            clinical_mapping_db.test_and_return_map_version(self.map, prod=True)
        )
        display += "\nthe envelope with draws is versioned {}".format(self.env_path)
        display += "\nthe envelope without draws is versioned {}".format(
            self.no_draws_env_path
        )
        display += f"\n Inpatient Run Constants {InpRunConstants()}"
        return display

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

        if "year_id" in df.columns.tolist():
            df = df.rename(columns={"year_id": "year_start"})
        df = df[["source", "nid", "year_start"]].drop_duplicates()

        df = legacy_pipeline.apply_merged_nids(df, assert_no_nulls=False, fillna=True)
        nulls = df["nid"].isnull().sum()
        missing = (df["nid"] <= 0).sum()
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

    def query_clinical_db(self, dql, odbc_profile=RunDBSettings.db_profile):
        db = Database()
        db.load_odbc(odbc_profile)
        return db.query(dql)

    def set_sources(self):
        """
        Identify 'old' and 'new' sources for post format processing.
        'Old' sources are ones that were first formatted in GBD 2015.
        'New' sources are ones with an .H5 extension
        """

        source_years = self.pull_run_source()
        source_names = set(source_years.source_name.tolist())
        gbd_15_sources_names = set(gbd_15_sources.db_source_name.tolist())
        new_sources = list(source_names - gbd_15_sources_names)
        old_sources = list(source_names.intersection(gbd_15_sources_names))

        if old_sources:
            temp = source_years.merge(
                gbd_15_sources,
                left_on=["source_name", "year_id"],
                right_on=["db_source_name", "year_id"],
            )
            temp_sources = temp.file_source_name.unique().tolist()
            possible_new_sources = set(old_sources) - set(temp_sources)

            if possible_new_sources:
                print(
                    "  Sources that may need to be updated in the future with new data: "
                    f"{possible_new_sources}"
                )
                # new_sources.extend(list(possible_new_sources))
            old_sources = temp.file_source_name.unique().tolist()

        print("  These are the Stata-formatted sources to be run: " f"{old_sources}")
        print("  These are the Python-formatted sources to be run: " f"{new_sources}")
        return new_sources, old_sources

    def pull_run_source(self):
        """
        Read from DB and subset by given run_id
        """
        dql = (QUERY
        )
        return self.query_clinical_db(dql)

    def master_data(self):
        """Aggergates all inpatient hospital sources into a single source.

        Files are written to FILEPATH
        """
        # set true only if run_id is 5, otherwise false
        if self.run_id == 5:
            gbd2019_decomp4 = True
            print("Running with only a subset of data sources for gbd2019 decomp4")
        else:
            gbd2019_decomp4 = False

        new_sources, old_sources = self.set_sources()
        fmt_master.main(
            self.run_id,
            clinical_age_group_set_id=self.clinical_age_group_set_id,
            gbd2019_decomp4=gbd2019_decomp4,
            new_sources=new_sources,
            old_sources=old_sources,
            remove_live_births=self.remove_live_births,
        )

        status = "wait"
        time.sleep(15)
        while status == "wait":
            USER = getpass.getuser()
            p = subprocess.Popen(["squeue", "-u", f"{USER}"], stdout=subprocess.PIPE)
            squeue_txt = p.communicate()[0]
            squeue_txt = squeue_txt.decode("utf-8")
            # print("waiting...")
            pattern = "fmt_+"
            found = re.search(pattern, squeue_txt)
            try:
                found.group(0)  # if the unc jobs are out this should work
                status = "wait"
                time.sleep(40)  # wait 40 seconds before checking again
            except:
                status = "go"
        print(status)

    def map_to_icg(self):
        """Map source icd codes to icg.

        Parameters
        ----------


        Returns
        -------
        pd.DataFrame
            The master data in icg space via a pandas dataframe.
        """
        df = submit_icd_mapping.icd_mapping(
            en_proportions=True,
            create_en_matrix_data=True,
            save_results=self.write_results,
            write_log=True,
            deaths="non",
            extra_name="",
            run_id=self.run_id,
            map_version=self.map,
        )

        return df

    def get_universe(self):
        """
        Return the universe id that the run is on.
        """
        dql = (QUERY
        )
        universe = self.query_clinical_db(dql)["clinical_data_universe_id"].tolist()

        if len(universe) == 1:
            universe_id = universe[0]
        else:
            msg = (
                f"There are {len(universe)} unique universe_id with the given run_id."
                "This should not happen in DB - please update accordingly."
            )
            raise ValueError(msg)

        return int(universe_id)

    def get_source_round(self):
        """
        Return source name and age sex round
        """
        dql = (QUERY
        )

        return self.query_clinical_db(dql)

    def age_sex_weights(
        self,
        back,
        env_path,
        round_2_only,
        overwrite_weights=True,
        rounds=2,
    ):
        """
        GBD2019 and earlier function to create the age-sex weights using 1 or 2 rounds

        We may want to review a few of these parameters at some point. The
        interaction between 'round_2_only', 'rounds' and 'make_weights' is not
        intuitive.

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
        iw = InpatientWrappers(self.run_id, RunDBSettings.iw_profile)
        run_metadata = iw.pull_run_metadata()
        self.age_sex_weights_version_id = run_metadata.age_sex_weights_version_id[0]

        if self.make_weights == bool(self.age_sex_weights_version_id):
            raise RuntimeError(
                "age_sex_weights_version_id evaluates to the same bool as "
                "self.make_weights. These two arguments must be different"
            )

        if not self.make_weights:
            base = FILEPATH
            weight_path = FILEPATH.format(
                base, self.age_sex_weights_version_id
            )
            if os.path.exists(weight_path):
                return
            else:
                assert False, "The weights don't appear to be present at {}".format(
                    weight_path
                )
        if round_2_only and rounds < 2:
            raise ValueError("Think about what these two args mean together")

        # validate age-sex data sources
        self.validate_age_sex_sources(back)

        source_round_df = self.get_source_round()

        # check that all sources have a designated round
        source_diff = set(back["source"].unique()) - set(source_round_df["source"].unique())
        assert (
            source_diff == set()
        ), "Please add {} to the list of sources and which round to use it in".format(
            source_diff
        )

        round_1_newdrops = source_round_df.loc[source_round_df.use_in_round > 1, "source"]
        round_2_newdrops = source_round_df.loc[source_round_df.use_in_round > 2, "source"]

        # we don't need the full data to create weights!
        weight_cols = [
            "age_group_id",
            "sex_id",
            "location_id",
            "year_start",
            "year_end",
            "source",
            "icg_id",
            "product",
        ]

        # Identify the age_sex_weights_version_id which will be created
        next_aswv_id = pd.read_sql(QUERY
            iw.ddl.connection,
        ).AUTO_INCREMENT[0]

        # Make directory
        os.makedirs(FILEPATH)

        if (not round_2_only) and (self.make_weights):
            df = back.copy()

            # drop sources you don't want to use for splitting
            df = df[~df.source.isin(round_1_newdrops)].copy()
            r1_sources = df.source.unique().tolist()

            # make sure ALL expected round 1 sources are in the data
            exp_round1_sources = source_round_df.query("use_in_round <= 1")["source"]
            if set(r1_sources).symmetric_difference(exp_round1_sources):
                raise ValueError("Expected sources don't match what's in the data")

            print(f"Begin Round One weights with {r1_sources}")
            df = demographic.retain_age_sex_split_age_groups(
                df,
                run_id=self.run_id,
                round_id=1,
                clinical_age_group_set_id=self.clinical_age_group_set_id,
            )

            df = apply_envelope_only(
                df,
                run_id=self.run_id,
                env_path=env_path,
                apply_age_sex_restrictions=True,
                want_to_drop_data=True,
                clinical_age_group_set_id=self.clinical_age_group_set_id,
                use_cached_pop=True,
                map_version=self.map,
            )

            df = df[weight_cols]  # don't need all the columns
            compute_weights(
                df,
                run_id=self.run_id,
                round_id=1,
                clinical_age_group_set_id=self.clinical_age_group_set_id,
                overwrite_weights=overwrite_weights,
                squaring_method=self.weight_squaring_method,
                map_version=self.map,
                next_age_sex_weights_version_id=next_aswv_id,
            )

        if (round_2_only and self.make_weights) or (self.make_weights and rounds == 2):
            df = back.copy()  
            # drop sources you don't want to use for splitting
            df = df[~df.source.isin(round_2_newdrops)].copy()
            r2_sources = df.source.unique().tolist()

            # make sure ALL expected round 1 sources are in the data
            exp_round2_sources = source_round_df.query("use_in_round <= 2")["source"]
            if set(r2_sources).symmetric_difference(exp_round2_sources):
                raise ValueError("Expected sources don't match what's in the data")

            print(f"Begin Round Two weights with {r2_sources}")
            # split the data
            df = run_age_sex_splitting(
                df,
                run_id=self.run_id,
                clinical_age_group_set_id=self.clinical_age_group_set_id,
                verbose=True,
                round_id=2,
                write_viz_data=False,
                map_version=self.map,
                age_sex_weights_version_id=next_aswv_id,
            )  

            df = demographic.retain_age_sex_split_age_groups(
                df,
                run_id=self.run_id,
                round_id=2,
                clinical_age_group_set_id=self.clinical_age_group_set_id,
            )

            # go to rate space
            df = apply_envelope_only(
                df,
                run_id=self.run_id,
                env_path=env_path,
                apply_age_sex_restrictions=True,
                want_to_drop_data=True,
                clinical_age_group_set_id=self.clinical_age_group_set_id,
                use_cached_pop=True,
                map_version=self.map,
            )
            df = df[weight_cols]  # don't need all the columns

            # compute the actual weights
            compute_weights(
                df,
                run_id=self.run_id,
                clinical_age_group_set_id=self.clinical_age_group_set_id,
                round_id=2,
                overwrite_weights=overwrite_weights,
                squaring_method=self.weight_squaring_method,
                map_version=self.map,
                next_age_sex_weights_version_id=next_aswv_id,
            )

        # Pull NIDs and source_names present
        iw = InpatientWrappers(self.run_id, RunDBSettings.iw_profile)  # Re-instantiate
        nids = back.nid.unique().tolist()
        del back
        source_names = pd.read_sql(QUERY,
            iw.ddl._create_connection(),
        ).source_name.tolist()

        # Make new age_sex_weights_version
        iw.insert_into_age_sex_weights_version(
            age_sex_weights_version_name=(f"Weights generated by run_id {self.run_id}"),
            age_sex_weights_version_description=(
                f"Sources included: {', '.join(src for src in source_names)}"
            ),
        )
        self.age_sex_weights_version_id = next_aswv_id

        # Update run_metadata
        iw.update_run_metadata(age_sex_weights_version_id=self.age_sex_weights_version_id)

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
            "e-code, (inj_poisoning_other)": "poisoning_other",
            "lower respiratory infection(all)": "lower respiratory infection(unspecified)",
            "neoplasm non invasive other": "other benign and in situ neoplasms",
            "e-code, (inj_trans)": "z-code, (inj_trans)",
            "digest, gastritis and duodenitis": "_none",
            "resp, copd, bronchiectasis": "resp, other",
        }

        # Cast as string
        df["icg_name"] = df["icg_name"].astype(str)

        chk_df = clinical_mapping_db.get_clinical_process_data("icg", map_version=self.map)
        chk_df = chk_df[["icg_name", "icg_id"]]
        # retain only 'good' icg names
        chk_df = chk_df[chk_df["icg_name"].isin(list(fix_dict.values()))]

        exp_id_dict = {1267: 864, 1266: 1116, 1265: 159, 1264: 1241, 1263: 1, 1262: 434}

        sdiff = set(chk_df["icg_id"]).symmetric_difference(set(exp_id_dict.values()))
        assert not sdiff, "The icg_ids don't match our hardcoding. This is unexpected"

        # loop over dict and replace key (old icg name/id) with value (new icg name/id)
        for key, value in list(fix_dict.items()):
            # get new icg_id
            an_id = chk_df.query("icg_name == @value")
            assert len(an_id) == 1
            an_id = an_id.icg_id.iloc[0]
            df.loc[df["icg_name"] == key, ["icg_name", "icg_id"]] = (value, an_id)

        # Casting back
        df["icg_name"] = df["icg_name"].astype("category")

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

        print("Converting special maps")
        df = self.convert_obsolete_special_map_icgs(df)

        # cast categories as str for a sec
        df["source"] = df["source"].astype(str)
        df["facility_id"] = df["facility_id"].astype(str)
        df["outcome_id"] = df["outcome_id"].astype(str)
        df["icg_name"] = df["icg_name"].astype(str)

        df = df.groupby(df.columns.drop("val").tolist()).agg({"val": "sum"}).reset_index()

        # Cast back
        df["source"] = df["source"].astype("category")
        df["facility_id"] = df["facility_id"].astype("category")
        df["outcome_id"] = df["outcome_id"].astype("category")
        df["icg_name"] = df["icg_name"].astype("category")

        print("Starting age sex splitting")

        if not self.make_weights:
            iw = InpatientWrappers(self.run_id, RunDBSettings.iw_profile)
            run_metadata = iw.pull_run_metadata()
            self.age_sex_weights_version_id = run_metadata.age_sex_weights_version_id[0]
        df = run_age_sex_splitting(
            df,
            run_id=self.run_id,
            clinical_age_group_set_id=self.clinical_age_group_set_id,
            verbose=True,
            round_id=rounds,
            write_viz_data=False,
            map_version=self.map,
            age_sex_weights_version_id=self.age_sex_weights_version_id,
        )
        wpath = (FILEPATH.format(rid=self.run_id, r=rounds)
        )

        general_purpose.write_hosp_file(df, wpath, backup=self.write_results)

        return df

    def convert_to_int(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert int expected cols to int

        Pandas can't store missing values as int cols, so it's constantly
        converting ints to floats. This is an issue with the db.

        Parameters
        ----------
        df : pd.DataFrame
            The dataframe to convert.

        Returns
        -------
            The dataframe after the columns have been converted.

        """
        dfcols = df.columns
        int_cols = [
            "age_group_id",
            "age_start",
            "age_end",
            "location_id",
            "sex_id",
            "year_id",
            "nid",
            "estimate_id",
            "icg_id",
            "metric_id",
            "diagnosis_id",
            "representative_id",
        ]
        int_cols = [c for c in int_cols if c in dfcols]
        for col in int_cols:
            df[col] = df[col].astype(int)
        return df

    def chk_data_for_upload(self, df: pd.DataFrame, estimate_table: str) -> None:
        """Confirm data has the exact column names and data types to match what's
        already in the database

        Parameters
        ----------
        df : pd.DataFrame
            Description of parameter `df`.
        estimate_table : str
            Either "intermediate_estimates" or "final_estimates_icg".
        """
        if estimate_table == "intermediate_estimates":
            exp_cols = [
                "age_group_id",
                "sex_id",
                "location_id",
                "year_id",
                "representative_id",
                "estimate_id",
                "source_type_id",
                "icg_id",
                "nid",
                "run_id",
                "diagnosis_id",
                "val",
            ]
            assert (
                df.isnull().sum().sum() == 0
            ), "Nulls in these columns are not expected {}".format(df.isnull().sum())
        elif estimate_table == "final_estimates_icg":
            exp_cols = [
                "age_group_id",
                "sex_id",
                "location_id",
                "year_id",
                "representative_id",
                "estimate_id",
                "source_type_id",
                "diagnosis_id",
                "icg_id",
                "nid",
                "run_id",
                "mean",
                "lower",
                "upper",
                "sample_size",
                "cases",
            ]
        else:
            print("What are the expected columns?")

        diff = set(df.columns).symmetric_difference(set(exp_cols))
        assert not diff, "Cols don't match {}".format(diff)

        if estimate_table == "intermediate_estimates":
            for col in exp_cols:
                if col != "val":
                    assert df[col].dtype == int, "{} is the wrong data type".format(col)
                else:
                    assert df[col].dtype == float, "{} is the wrong data type".format(col)
        elif estimate_table == "final_estimates_icg":
            for col in exp_cols:
                if col in ["mean", "upper", "lower", "sample_size", "cases"]:
                    assert df[col].dtype == float, "{} is the wrong data type".format(col)
                else:
                    assert df[col].dtype == int, "{} is the wrong data type".format(col)
                    assert df[col].isnull().sum() == 0, "There are null values for some reason"

        print("ready to write {} results for upload to the db".format(estimate_table))

    def write_intermediate(self, df: pd.DataFrame, overwrite: bool = True) -> None:
        """Write hospital data post age-sex splitting to the for_upload folder
        in a clinical run.

        Parameters
        ----------
        df : pd.DataFrame
            The dataframe to write.
        overwrite : bool
            Should data be overwritten? Default is True.
        """
        # drop a lot of stuff to save space
        df = df.drop(
            [
                "outcome_id",
                "source",
                "age_group_unit",
                "icg_name",
                "facility_id",
                "metric_id",
            ],
            axis=1,
        )

        df["estimate_id"] = 1  # unadj inp data
        df["source_type_id"] = 10  # inpatient source
        df["run_id"] = self.run_id

        df = self.convert_to_int(df)
        self.chk_data_for_upload(df, estimate_table="intermediate_estimates")

        fpath = (FILEPATH.format(rid=self.run_id)
        )
        if not os.path.exists(fpath) or overwrite:
            df.to_csv(fpath, index=False, na_rep="NULL")

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
        df, full_coverage_df = prep_for_env_main(
            df,
            run_id=self.run_id,
            env_path=self.env_path,
            clinical_age_group_set_id=self.clinical_age_group_set_id,
            new_env_or_data=True,
            write=self.write_results,
            drop_data=True,
            fix_norway_subnat=False,
            map_version=self.map,
        )

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
        print("Creating Population level estimates: {}".format(self.no_draws_env_path))
        df = create_icg_single_year_est(
            df=df,
            run_id=self.run_id,
            run_tmp_unc=self.run_tmp_unc,
            write=self.write_results,
            env_path=self.env_path,
            draws=self.draws,
        )
        return df

    def write_final_icg(self, df):
        """ICG data contains draws now, so we can't concat it all together (too big)
        We're not currently doing ICG level CFs so our "final" data here is just
        a list of ICGs, estimates, locations that we we'll use to make the data
        square in parallel.

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
        write_path = (FILEPATH
        )
        print("Writing the H5 file for the clinical team's use to \n{}".format(write_path))
        general_purpose.write_hosp_file(df=df, write_path=write_path, backup=False)

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
            print(
                "We're sending out ~250 jobs to process the bundle level CF"
                " draws by age, sex and year bin."
            )
            df = sb.main_bundle_draw_submit(
                run_id=self.run_id,
                draws=self.draws,
                clinical_age_group_set_id=self.clinical_age_group_set_id,
                bin_years=self.bin_years,
            )
        else:
            raise ValueError(
                (
                    f"The inpatient pipeline curently only works with"
                    f" bundle level correction factors. Setting "
                    f"bundle_level_cfs to {self.bundle_level_cfs} "
                    f"needs additional work before running"
                )
            )

        return df

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
        base = FILEPATH
        files = glob.glob(FILEPATH)

        nids = []
        for f in files:
            tmp = pd.read_hdf(f, columns=["nid"])
            nids += tmp["nid"].unique().tolist()

        db = Database()
        db.load_odbc("clinical")
        db_table = DB

        query = f"""QUERY ;"""

        active_niddf = db.query(query)

        diffs = set(active_niddf["nid"].unique()).symmetric_difference(set(nids))
        if diffs:
            print(active_niddf[active_niddf["nid"].isin(diffs)])
            msg = (
                "The nids present in master data don't match "
                "the NIDs we expect from the NID db table {}: {}".format(db_table, diffs)
            )
            assert False, msg
        return

    def confirm_env_versions(self):
        """Confirm the environment version."""
        env = re.sub("_draws", "", os.path.basename(self.env_path)[:-3])  # index to remove .H5
        no_draws = os.path.basename(self.no_draws_env_path)[:-4]  # index to remove .csv
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
        md = glob.glob(FILEPATH)
        icd = glob.glob(FILEPATH)

        if pre_mapping:
            if icd:
                msg = (
                    "There are icd files present in FILEPATH "
                    "confirm these have been overwritten by your re-run or manually delete "
                    "otherwise an outdated map could be used"
                )
                warnings.warn(msg)
        else:
            src_diff = set([os.path.basename(f) for f in md]).symmetric_difference(
                [os.path.basename(f2) for f2 in icd]
            )
            if src_diff:
                error_msg = (
                    "There is a set difference between the files output to "
                    "/master_data and those output to /icd_mapping. These "
                    f"different file(s) are {src_diff}"
                )
                raise ValueError(error_msg)
        return

    def verify_bundle_cf(self, df):
        """
        Post verification that the expected cf was appended /
        created during apply_bundle_cfs process.
        Does not check for all age groups / sex. See check_files() for this test
        """
        iw = InpatientWrappers(self.run_id, RunDBSettings.iw_profile)
        run_metadata = iw.pull_run_metadata()
        dql = (QUERY
        )
        active_bids_est = self.query_clinical_db(dql)
        active_bids = active_bids_est.bundle_id.tolist()
        df = df[["bundle_id", "estimate_id"]].drop_duplicates()
        df = df[df.bundle_id.isin(active_bids)]

        m = df.merge(active_bids_est, on=["bundle_id", "estimate_id"])
        missing = set(df.bundle_id.tolist()) - set(m.bundle_id.tolist())
        if missing:
            missing_df = active_bids_est.loc[
                active_bids_est["bundle_id"].isin(missing), ["bundle_id", "estimate_id"]
            ]
            raise RuntimeError(
                f"CF models were not applied to following bundles: {missing}"
                "If the following table contains a maternal estimate (ID 6-9)"
                "It may be worth reviewing the behavior of the function which"
                f"returns a list of maternal bundle_ids. {missing_df}"
            )

    def final_tests(self, df):
        """run some tests on the df and the DB table"""
        print("beginning final tests on the inpatient dataframe")
        self.verify_bundle_cf(df=df)

        # Catch duplicated data
        id_cols = [
            "age_group_id",
            "sex_id",
            "location_id",
            "year_start",
            "year_end",
            "nid",
            "bundle_id",
            "estimate_id",
        ]

        assert len(df) == len(df[id_cols].drop_duplicates()), (
            "There is "
            "duplicated data. Review the agg to five file for dupes "
            f"in the following columns {id_cols}. Historical, duplication "
            "has been caused by the CFs and is therefore only present in "
            "certain bundle_ids. Maybe check that"
        )

        # make sure DB has coherent mappings
        bundle_check.test_clinical_bundle_est(map_version=self.map)
        # confirm expected bundle ids are present
        miss = bundle_check.test_refresh_bundle_ests(
            df=df, pipeline="inp", map_version=self.map, prod=False
        )
        exp_miss = [
            28,
            207,
            208,
            6113,
            6116,
            6119,
            6122,
            6125,  # 5 maternal ratio bundles
            8276,
        ]
        miss_warn = (
            f"\n\nTEMPORARY DROPS IN HERE!!!!\n\n\n"
            f"For various reasons we expect the following bundles "
            f"to fail this test {exp_miss}"
        )

        warnings.warn(miss_warn)
        if miss != "This df seems to have all estimates present":
            real_miss = miss.copy()
            for key in list(miss.keys()):
                if key in exp_miss:
                    del real_miss[key]
            if len(real_miss) > 0:
                raise ValueError(
                    "We're breaking because the following "
                    f"bundles/estimates are missing {real_miss}"
                )


        confirm_square_data.validate_test_df(
            df=df,
            clinical_age_group_set_id=self.clinical_age_group_set_id,
            run_id=self.run_id,
            map_version=self.map,
            bin_years=self.bin_years,
        )

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

        # get all locations for each source
        source_locations = df[["source", "location_id"]].drop_duplicates()

        # get all bundles for each source
        ndf = df[["source", "icg_id", "icg_name"]].drop_duplicates().copy()
        ndf = clinical_mapping.map_to_gbd_cause(
            df=ndf,
            input_type="icg",
            output_type="bundle",
            write_unmapped=False,
            map_version=map_version,
            retain_active_bundles=True,
        )
        ndf = ndf.drop("bundle_measure", axis=1)
        ndf = ndf.drop_duplicates()

        # combine bundles and locations by source
        ndf = ndf.merge(source_locations, how="outer", on=["source"])

        write_path = (FILEPATH.format(run_id)
        )
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

        eti_est_df = df[["icg_id", "icg_name", "estimate_id"]].drop_duplicates()
        eti_est_df["keep"] = 1

        eti_est_df = clinical_mapping.expand_bundles(
            df=eti_est_df,
            prod=True,
            drop_null_bundles=True,
            map_version=map_version,
            test_merge=True,
        )

        eti_est_df = eti_est_df[["bundle_id", "estimate_id", "keep"]].drop_duplicates()

        if fill_missing_estimates:
            """There are still issues with parent injuries and bundle ratios that are present
            here but not in the eti_est df from older runs"""
            df_list = [eti_est_df]
            # add in the 3 CF estimate_ids for mat and non-mat data
            # {base estimate: [cfs, added, on]}
            est_dict = {1: [2, 3, 4]}
            for base_est in est_dict:
                for cf_est in est_dict[base_est]:
                    tmp = eti_est_df.query("estimate_id == @base_est").copy()
                    tmp["estimate_id"] = cf_est
                    df_list.append(tmp)
            # now add in injury estimates mainly
            estimates = query(QUERY, conn_def="epi")
            estimates = estimates.loc[
                estimates.estimate_name.str.contains("inp-"), "estimate_id"
            ].tolist()

            bundles = clinical_mapping_db.get_active_bundles(
                cols=["bundle_id"], estimate_id=estimates, map_version=map_version
            )
            bundles["keep"] = 1
            df_list.append(bundles)

            eti_est_df = pd.concat(df_list, sort=False, ignore_index=True)
            eti_est_df = eti_est_df.drop_duplicates()
            print(
                "{} shape of eti est df. Should be OK if it's larger than 1208".format(
                    eti_est_df.shape
                )
            )

        eti_est_df.to_csv(FILEPATH
            "eti_est_df.csv".format(run_id),
            index=False,
        )

        return

    def validate_age_sex_sources(self, df):
        """Run a set of validations on the process source table and the data"""
        failures = []
        src_rounds = self.get_source_round()
        bad_rounds = set([1, 2, 3]).symmetric_difference(src_rounds["use_in_round"])
        if bad_rounds:
            failures.append(f"The following use_in_round values are bad {bad_rounds}")
        dupe_sources = src_rounds[src_rounds.duplicated(subset="source", keep=False)]
        if len(dupe_sources) > 0:
            failures.append(f"There are duplicated source rounds. {dupe_sources}")
        source_diffs = set(df["source"].unique()).symmetric_difference(
            src_rounds["source"].unique()
        )
        if source_diffs:
            failures.append(f"There is a misalignment of source names. {source_diffs}")
        if failures:
            m = (
                "\nThe following tests have failed. The process source table and "
                "the inpatient data are not aligned.\n"
            )
            m = m + " -- ".join(failures)
            raise ValueError(m)
        return

    def automater(self):
        """An inelegant method of skipping code that has already been run depending on
        which HDF files have been created in the process.

        For example, if ICD mapping
        has been run, then this function will tell the Inpatient.main() method to read
        in that hdf and begin processing at age-sex splitting.

        Note: if you want to manually begin the process from some specific place you
              can set the inp.begin_with attribute to the appropriate string, eg
              if you want to re-run prep envelope then
              it's "inp.begin_with = 'prepare_envelope'"

        Returns
        -------
        (list, list, list, list, list, str, bool)
            cache_central_inputs, master_data, icd, age_sex, prep_env, apply_env: (list)
                lists of hdf filepaths to read in if needed
            begin_with: (str)
                plain english string indicating which step in the process the main()
                function should begin with
            read_data: (bool)
                if any of the lists of data above exist, this is set to True
                and the data is read in from HDF
        """

        base = FILEPATH.format(self.run_id)
        cache_central_inputs = glob.glob(FILEPATH)
        master_data = glob.glob(FILEPATH)
        icd = glob.glob(FILEPATH)
        age_sex = glob.glob(FILEPATH)
        prep_env = glob.glob(FILEPATH)
        apply_env = glob.glob(FILEPATH)
        create_bundle_est = glob.glob(FILEPATH)
        maternal_adjustment = glob.glob(FILEPATH
        )

        read_data = True
        if maternal_adjustment:
            self.begin_with = "all done.. ?"
        elif create_bundle_est:
            self.begin_with = "maternal_adjustment"
        elif apply_env:
            self.begin_with = "expand_to_bundle"
        elif prep_env:
            self.begin_with = "apply_envelope"
        elif age_sex:
            self.begin_with = "prepare_envelope"
        elif icd:
            self.begin_with = "age_sex_splitting"
        elif master_data:
            self.begin_with = "icd_mapping"
            read_data = False  # icd mapping just reads automatically
        elif cache_central_inputs:
            self.begin_with = "master_data"
            read_data = False
        else:
            self.begin_with = "cache_central_inputs"
            read_data = False

        return (
            cache_central_inputs,
            master_data,
            icd,
            age_sex,
            prep_env,
            apply_env,
            create_bundle_est,
            maternal_adjustment,
            read_data,
        )

    def main(self, control_begin_with: bool = False) -> None:
        """Run the inpatient pipeline.

        Parameters
        ----------
        control_begin_with : bool
            Force run from a specific point in the pipeline. Should be used with
            care.
        """

        self.log_stdout()

        possible_begins = [
            "cache_central_inputs",
            "master_data",
            "icd_mapping",
            "age_sex_splitting",
            "prepare_envelope",
            "apply_envelope",
            "expand_to_bundle",
            "maternal_adjustment",
        ]

        pre_begin_with = self.begin_with

        (
            cache_central_inputs,
            master_data,
            icd,
            age_sex,
            prep_env,
            apply_env,
            create_bundle_est,
            maternal_adjustment,
            read_data,
        ) = self.automater()

        if control_begin_with:
            if pre_begin_with not in possible_begins:
                print(
                    "The step {} is not understood, please correct to one of these {}".format(
                        pre_begin_with, possible_begins
                    )
                )
            self.begin_with = pre_begin_with

        if self.begin_with == "cache_central_inputs":
            pickle.dump(self, open(FILEPATH.format(self.base), "wb"))

            haqi_prep.set_haqi_cf(
                run_id=self.run_id,
                bin_years=self.bin_years,
                min_treat=0.1,
                max_treat=0.75,
            )

            write_inp_envelope_to_run(
                run_id=self.run_id,
                version_id=self.env_stgpr_id,
                draws=self.draws,
                env_path=self.env_path,
                no_draws_env_path=self.no_draws_env_path,
            )
            self.begin_with = "master_data"

        if self.begin_with == "master_data":
            pickle.dump(self, open(FILEPATH.format(self.base), "wb"))

            print("Initiating master data subroutine...")
            self.master_data()
            self.begin_with = "icd_mapping"

        if self.begin_with == "icd_mapping":
            pickle.dump(self, open(FILEPATH.format(self.base), "wb"))

            self.confirm_master_data_run()
            self.check_icd_indv_files(pre_mapping=True)
            print("Initiating icd mapping subroutine...")
            mapped_df = self.map_to_icg()
            self.check_icd_indv_files(pre_mapping=False)

            self.begin_with = "age_sex_splitting"

        if self.begin_with == "age_sex_splitting":
            pickle.dump(self, open(FILEPATH.format(self.base), "wb"))

            print("Initiating age sex splitting subroutine...")
            if read_data:
                assert len(icd) == 1, "Why more than 1 ICD mapping file??"
                mapped_df = pd.read_hdf(icd[0])
                read_data = False

            pre_split_nids = set(mapped_df["nid"].unique())

            # replace this with retain active sources func
            if "GEO_COL_00_13" in mapped_df.source.unique():
                mapped_df = mapped_df[mapped_df.source != "GEO_COL_00_13"]

            # Make weights
            self.age_sex_weights(
                env_path=self.no_draws_env_path,
                back=mapped_df,
                round_2_only=False,
            )
            # Run age sex splitting
            df = self.age_sex_split(mapped_df)

            nid_diff = pre_split_nids.symmetric_difference(df["nid"].unique())
            if nid_diff:
                raise ValueError(
                    "The age-sex splitting process has lost data. The following "
                    "NIDs should be present in the data but were lost post-split"
                    f"\n{nid_diff}"
                )
            send_report(run_id=self.run_id, squeue=self.que)
            self.begin_with = "prepare_envelope"
            del mapped_df

        if self.begin_with == "prepare_envelope":
            pickle.dump(self, open(FILEPATH.format(self.base), "wb"))
            print("Initiating prepare envelope subroutine...")
            self.confirm_env_versions()
            if read_data:
                assert len(age_sex) == 1, "Too many files in age-sex split folder !!!"
                print("reading in age-sex split data")
                df = pd.read_hdf(age_sex[0])
                print("done")
                read_data = False

            # clean_up unecessary cols
            df = df.rename(columns={"year_start": "year_id"})
            df = df.drop(["year_end"], axis=1)

            # write the denominator file for adding zeros and validation
            legacy_pipeline.create_hosp_denom(df=df, denom_col="val", run_id=self.run_id)

            # write data for intermediate use in db
            self.write_intermediate(df.copy(), overwrite=False)  # only needs to be done once

            # prep env
            df, full_coverage_df = self.prep_for_env(df.copy())
            self.begin_with = "apply_envelope"

        if self.begin_with == "apply_envelope":
            pickle.dump(self, open(FILEPATH.format(self.base), "wb"))
            print("Initiating apply envelope subroutine...")

            if read_data:
                assert len(prep_env) in [1, 2], "too many prep env files"
                df = pd.concat(
                    [pd.read_hdf(f) for f in prep_env],
                    sort=False,
                    ignore_index=True,
                )
                read_data = False
            else:
                df = pd.concat([df, full_coverage_df], sort=False, ignore_index=True)
                del full_coverage_df

            cause_fraction_cols = ["numerator", "denominator", "cause_fraction"]
            if not any(c in df.columns for c in cause_fraction_cols):
                df[cause_fraction_cols] = np.nan, np.nan, np.nan

            print("Confirming age group set")
            demographic.confirm_age_group_set(
                age_col=df["age_group_id"].unique(),
                clinical_age_group_set_id=self.clinical_age_group_set_id,
                full_match=True,
            )

            # Convert rates from admission counts to population-level
            # ICG rates
            df = self.pop_estimates(df)  # A select dataset with reduced cols is returned

            if self.bin_years:
                # Checked that all merged NIDs are present before agg to 5
                chk_cols = ["source", "nid", "year_id"]
                self.check_merged_nids(df[chk_cols].drop_duplicates().copy())

            # if nid check succeeds then drop the cols it needed
            df = df.drop(["nid", "year_id"], axis=1).drop_duplicates()

            # final icg is used by the automater method to get pipeline progress
            df = self.write_final_icg(df)

            print(df.columns)
            assert "source" in df.columns, "'source' must be in the dataframe"

            # make bundle location file. This file allows us to square the data
            # within the bundles/locations that we have data for, while
            # parallalizing such that each parallel job wouldn't otherwise have
            # access to this information
            self.create_bundle_loc_df(df=df, map_version=self.map, run_id=self.run_id)

            # make estimate_id-bundle_id file. Helps us square data during
            # parallelized runs
            self.create_eti_est_df(df=df, map_version=self.map, run_id=self.run_id)

            self.begin_with = "expand_to_bundle"
            self.read_data = False

        # aggregate to bundle level, agg to 5 years, create parent injuries,
        # apply inj (en proportion) CFs, write data to /share
        if self.begin_with == "expand_to_bundle":
            pickle.dump(
                self,
                open(FILEPATH.format(self.base), "wb"),
            )
            print(
                (
                    f"Initiating expand to bundle subroutine. The CF aggregation "
                    f"method will be {self.cf_agg_stat}"
                )
            )
            if self.bundle_level_cfs:
                bcf_files = Path(FILEPATH
                )
                bcf_files = len({e.stem.split("_df")[0] for e in bcf_files.glob("*.H5")})
                # create a file for each sex*age_group*denom_type(2)
                # Currently {sexes}*{ages}*2
                age_len = len(
                    demographic.get_hospital_age_groups(
                        clinical_age_group_set_id=self.clinical_age_group_set_id
                    )
                )
                bcfexp_file_count = 2 * age_len
                if bcf_files != bcfexp_file_count:
                    raise ValueError(
                        f"We expect {bcfexp_file_count} split ICG files but there "
                        f"are only {len(bcf_files)}"
                    )

                # pull in submit bundle methods to check output files if the
                # final files match the prep(input) files then don't run in
                # parallel again
                a, s, y = sb.get_demos(
                    bin_years=self.bin_years,
                    clinical_age_group_set_id=self.clinical_age_group_set_id,
                    run_id=self.run_id,
                )
                exp_files = len(a) * len(s) * len(y)
                prep_files, ob_final_files = sb.ob_file_getter(self.run_id, self.bin_years)
                if len(prep_files) == len(ob_final_files) == exp_files:
                    warnings.warn(
                        (
                            "\n\n\nREADING IN EXISTING BUNDLE FINAL FILES"
                            " FROM FILEPATH"
                            "DELETE THE FILES IF YOU DON'T WANT "
                            "THEM USED OR NEED TO RE-RUN\n\n"
                        )
                    )
                    final_files = sb.ob_file_getter(
                        run_id=self.run_id, bin_years=self.bin_years, for_reading=True
                    )
                    df = pd.concat(
                        [pd.read_csv(f) for f in final_files],
                        sort=False,
                        ignore_index=True,
                    )
                else:  # send out the parallel jobs
                    print("Creating bundles")
                    df = self.create_bundles(df=pd.DataFrame())

                # drop some of the cols we used to process
                if self.cf_agg_stat == "median":
                    # for rows with uncertainty, replace mean with median
                    df.loc[df["lower"].notnull(), "mean"] = df.loc[
                        df["lower"].notnull(), "median_CI_team_only"
                    ]
                else:
                    pass
                drops = ["median_CI_team_only"]
                drops = [d for d in drops if d in df.columns]
                df = df.drop(drops, axis=1)

            else:
                if read_data:
                    assert len(apply_env) == 1, "Too many files produced by apply env"
                    df = pd.concat([pd.read_hdf(f) for f in apply_env])
                    read_data = False

                df = self.create_bundles(df=df)

            # Ensure that ALL age group IDs made it
            demographic.confirm_age_group_set(
                age_col=df["age_group_id"].unique(),
                clinical_age_group_set_id=self.clinical_age_group_set_id,
                full_match=True,
            )

            # write the bundle level CFs
            if self.bundle_level_cfs:
                bin_dir = "agg_5_years" if self.bin_years else "single_years"

                bundle_cf_path = (FILEPATH
                )
                general_purpose.write_hosp_file(df=df, write_path=bundle_cf_path, backup=True)

            # aggregate the U1 data to 0-1 and live birth rates
            years = df.year_start.sort_values().unique().tolist()
            re_aggregate_under1.create_u1_and_lb_aggs(
                run_id=self.run_id,
                input_age_set=self.clinical_age_group_set_id,
                year_start_list=years,
                cf_agg_stat=self.cf_agg_stat,
                map_version=self.map,
                full_coverage_sources=legacy_pipeline.full_coverage_sources(),
                bin_years=self.bin_years,
                lb_agid=164,
            )

            clean_final_bundle.inp_main(
                run_id=self.run_id,
                bin_years=self.bin_years,
            )

            self.begin_with = "maternal_adjustment"

        if self.begin_with == "maternal_adjustment":
            # create maternal denominator file
            write_maternal_denom(
                denom_type="ifd_asfr",
                run_id=self.run_id,
                clinical_age_group_set_id=self.clinical_age_group_set_id,
                bin_years=self.bin_years,
            )

            # apply maternal adjustment to each draw col and overwrite sample size with
            # a live births estimation for maternal bundles
            mat_df = adjust_mat_denom(
                run_id=self.run_id,
                draw_cols=[f"draw_{i}" for i in range(0, self.draws)],
                cf_agg_stat=self.cf_agg_stat,
            )

            # Read in compiled file and concat with output of adjust mat denom
            # run our final bundle tests
            df = pd.read_csv(FILEPATH
            )

            col_diff = set(df.columns).symmetric_difference(mat_df.columns)
            if col_diff:
                raise KeyError(
                    "The columns present in the maternal adjusted data "
                    f"and the input data do not match. {col_diff}"
                )

            df = pd.concat([df, mat_df], ignore_index=True, sort=False)
            # apply HAQi correction all at once after concat
            df = apply_bundle_cfs.apply_haqi_corrections(df, self.run_id)
            # drop haqi_cf col after
            if "haqi_cf" not in df.columns:
                raise KeyError("haqi_cf col not present after applying haqi correction")
            df = df.drop("haqi_cf", axis=1)

            self.final_tests(df)

            final_path = (FILEPATH
            )
            df.to_csv(final_path, index=False, na_rep="NULL")

            self.begin_with = "all done.. ?"

        if self.begin_with == "all done.. ?":
            pickle.dump(self, open(FILEPATH.format(self.base), "wb"))

            bundle_path = (FILEPATH.format(self.run_id)
            )
            icg_path = (FILEPATH.format(self.run_id)
            )

            print(
                "The inpatient process has finished running.\n"
                "Single year ICG level estimates are present in:\n{i}\n"
                "Five year bundle level estimates are present in:\n{b}".format(
                    i=icg_path, b=bundle_path
                )
            )

        return
