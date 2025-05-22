"""
This Module handles the removal of certain processing types of data from the
CMS numerators (aka the mdcr or max "ICD-marts")

We build the ICD-marts to contain as much data as we'd imagine we'd be using
so it's the starting off point for several unique estimate types. This module
controls the dropping of data for those estimates, the assignment of NID to them
and the estimate ID values.

The filter logic, which ties an estimate_id to a set of conditions to remove
rows actually exists in constants.py

The Clinical Info CMS schema uses a 'file_source_id' to link the the extracted/transformed
CMS data in our database back to the original source files.

# Example Use
mart = pd.read_csv(/path/to/mart/data)
df = EstimateIdController(df=mart.copy(),
                          estimate_id=21,
                          cms_system='mdcr',
                          logger=logger,
                          map_version=30)
"""
from typing import Dict, List, Tuple

import pandas as pd
from crosscutting_functions.clinical_constants.pipeline import cms as constants
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser
from crosscutting_functions.deduplication import dedup

from cms.utils import logger


class FsIDController:
    """Class to manage and extract various elements of file_source_id and
    file_source_type. Used to filter data that won't be used
    for certain estimates as well as attaching record-level NIDs
    """

    def __init__(self, cms_system: str, processing_type: List[str], logger: logger):
        """cms_system: (str)
            Must be either 'mdcr' or 'max'
        processing_type: (list)
            Must hold 1 or more of these four strings-
            'inpatient', 'oupatient', 'hospice', 'hha'
            When the prep_mart_df method is run any data outside
            of the provided types will be removed
        logger: CMS Logger from utils
        """

        self.cms_system = cms_system
        self.processing_type = processing_type
        self.logger = logger

        self.validate_args()
        self.get_file_ids()

    def prep_mart_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """MAIN CMS FILTER METHOD
        Input df is bundle level icd-mart data with all processing_types present

        Data will be filtered (removed) based on processing types arg from the
        Class instantiation. Then the is_otp column is attached and NIDs are attached
        """

        df = self.filter_by_processing_type(df)
        df = self.attach_is_otp(df)

        return df

    def validate_args(self):
        """simple checks on input arguments"""

        if self.cms_system not in ("max", "mdcr"):
            raise ValueError(
                f"Only 2 acceptable cms_system options, 'mdcr' or 'max', not {self.cms_system}"
            )
        if not isinstance(self.processing_type, list):
            if not isinstance(self.processing_type, str):
                raise TypeError(
                    f"processing type must be a list or string, found {type(self.processing_type)}"
                )
            self.processing_type = [self.processing_type]

    def filter_by_processing_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """Given a Pandas DataFrame of CMS bundle-mappable data from the icd-mart,
        this function will remove any file source IDs that are not in the specified
        processing type when the class is instantiated,
        ie ['inpatient', 'outpatient'] OR ['inpatient', 'outpatient', 'hha']
        """

        # count how many rows were dropped
        pre_rows = len(df)
        pre_ids = df.file_source_id.unique().tolist()
        self.logger.info(f"Retaining file source ids in this list {self.filter_ids}")
        df = df[df.file_source_id.isin(self.filter_ids)]
        post_rows = len(df)

        removed_ids = list(set(pre_ids) - set(df.file_source_id))
        if df.empty:
            raise RuntimeError("All rows of data have been removed")

        self.logger.info(
            f"The processing_type filter has removed {pre_rows-post_rows} rows "
            f"and {len(removed_ids)} unique file source IDs"
        )

        return df

    def attach_is_otp(self, df: pd.DataFrame) -> pd.DataFrame:
        """attach an is_otp column to the cms data by file source id"""

        otp_df = self.fid_df[["file_source_id", "is_otp"]].copy().drop_duplicates()

        df = df.merge(otp_df, how="left", validate="m:1")

        return df

    def get_file_source_type(self) -> Tuple[str, ...]:
        """create a tuple of file source types based on 'plain'-ish language input
        normalize the file_source_types values to match the way we expect to process
        the data. Should be easy to remove or include hha and/or hospice data. or to
        extract file source ids for say, outpatient only files. Note mdcr carrier is
        included in the 'outpatient' processing type here"""

        if self.cms_system in ["mdcr", "max"]:
            proc_dict: Dict[str, Tuple[str, ...]] = constants.fst_proc_dict[self.cms_system]
        else:
            raise ValueError("Only 2 acceptable cms_system options, 'mdcr' or 'max'")

        file_types = [proc_dict[t] for t in self.processing_type if t in proc_dict.keys()]
        file_types_tuple = tuple([str(i) for t in file_types for i in t])

        if len(file_types_tuple) < 1:
            raise RuntimeError("Expected file_types to exist.")

        return file_types_tuple

    def assign_otp_type(self):
        """Use file_source_types from constants to assign values to an is_otp
        column. If file source types are expanded this will break until updated"""

        self.is_otp_dict = constants.is_otp_dict
        self.fid_df["is_otp"] = None

        cond_out = "self.fid_df.file_source_type.isin(self.is_otp_dict['outpatient'])"
        self.fid_df.loc[eval(cond_out), "is_otp"] = 1

        cond_in = "self.fid_df.file_source_type.isin(self.is_otp_dict['inpatient'])"
        self.fid_df.loc[eval(cond_in), "is_otp"] = 0
        null_count = self.fid_df.isnull().sum().sum()
        if null_count != 0:
            msg = f"There are {null_count} unexpected Nulls"
            helper = "Did you expand/modify the file_source_types?"
            raise ValueError(f"{msg}. {helper}")

    def get_file_ids(self):
        """
        uses cms_system and processing_type to return a table of file_source_ids
        from the lookup table and assigns the result to a class attr.
        Result is a df containing file source id, file name, file type and file path
        """

        self.file_types = self.get_file_source_type()
        if len(self.file_types) > 1:
            comp = list(self.file_types)
        elif len(self.file_types) == 1:
            comp = [self.file_types[0]]
        else:
            raise ValueError(f"This looks wrong 'self.file_types'=\n{self.file_types}")

        base = filepath_parser(ini="pipeline.cms", section="base_paths", section_key="base")
        base = "FILEPATH"
        df_src = pd.read_csv("FILEPATH")
        df_type = pd.read_csv("FILEPATH")
        types = (
            df_type.loc[df_type["file_source_type_name"].isin(comp), "file_source_type"]
            .unique()
            .tolist()
        )
        self.fid_df = df_src.loc[df_src["file_source_type"].isin(types)].reset_index(drop=True)
        self.assign_otp_type()
        self.filter_ids = self.fid_df.file_source_id.unique().tolist()


class EstimateIdController:
    """Instantiate with an estimate_id then pass it a dataframe of bundle-mappable
    datafrom the icd-mart.
    Class will filter the appropriate data along 2 or 3 dimensions. All data is
    filtered for inpatient/outpatient, and diagnosis position. Medicare data is
    also filtered to remove out-of-sample enrollees
    """

    def __init__(
        self,
        df: pd.DataFrame,
        estimate_id: int,
        cms_system: str,
        logger: logger,
        map_version: int,
    ):
        self.df = df
        self.estimate_id = estimate_id
        self.cms_system = cms_system
        self.logger = logger
        self.map_version = map_version

        # run it
        self.prep_mart_df()

    def prep_mart_df(self):
        """MAIN CMS FILTER METHOD
        Runs the icd-mart related methods to pass data onto the rest of the
        cms pipeline"""
        self.convert_mart_to_estimate()
        self.validate_estimate()
        self.drop_diagnosis_id()

    def convert_mart_to_estimate(self):
        """In CMS data, Estimate IDs are defined along multiple criteria
        1) inpatient/outpatient
        2) diagnosis position
        3) de-duplicated or not
        4) in/out of five percent sample [for medicare only]
        5) entitlement based filtering [for medicare only]

        crit 1 is handled by the FsIDController using file source id
        crit 2 builds off of 1, then adds some logic to filter dx position
        crit 3 determines whether to use the dedup package
        crit 4 is a column in icd-mart data and used to filter rows
        crit 5 is another column in icd-mart data used to filter rows
        """

        system_dict = constants.est_id_dict[self.cms_system]
        if self.estimate_id in system_dict.keys():
            # create the FsID controller and filter file_ids outside of the proc_type
            processing_type = system_dict[self.estimate_id]["proc_type"]
            self.logger.info(f"Filtering data based on the processing types {processing_type}")
            fsc = FsIDController(
                cms_system=self.cms_system,
                processing_type=processing_type,
                logger=self.logger,
            )
            self.df = fsc.prep_mart_df(self.df)

            # filter out of sample data, medicare only
            if self.cms_system == "mdcr":
                sample_filter = system_dict[self.estimate_id]["sample_status"]
                if sample_filter == "basic_five_only":
                    pre = len(self.df)
                    self.df = self.df[self.df.five_percent_sample]
                    lost = pre - len(self.df)
                    self.logger.info(
                        f"Removing {lost} rows of out of sample Medicare data. "
                        f"Roughly {round(lost / pre, 2) * 100}% of rows removed."
                    )
                elif sample_filter == "in_and_out":
                    self.logger.info("Retaining both in and out of sample data")
                else:
                    raise ValueError(f"There is no filter method for {sample_filter}")
                entitlements = system_dict[self.estimate_id]["entitlements"]
                pre_entitle = len(self.df)
                self.df = self.df[self.df.entitlement.isin(entitlements)]
                entitle_diff = pre_entitle - len(self.df)
                self.logger.info(
                    f"{entitle_diff} rows have been removed due to claims outside of entitlements reqs"
                )

            # deduplicate data, or not and filter by estimate_id
            estimate_dedup = dedup.ClinicalDedup(
                enrollee_col="bene_id",
                service_start_col="service_start",
                year_col="year_id",
                estimate_id=self.estimate_id,
                map_version=self.map_version,
            )

            self.df["bundle_id"] = pd.to_numeric(self.df["bundle_id"], downcast="integer")

            # Dedup needs service start as np.datetime64
            self.df = estimate_dedup.main(df=self.df, create_backup=False)
            self.logger.info("Data modified to meet estimate requirements.")
            del estimate_dedup

        else:
            raise ValueError(
                f"There are no known filtering and deduplication "
                f"methods for estimate {self.estimate_id} currently"
            )

    def validate_estimate(self):
        """Need some additional validations here after vetting"""

        failures = []
        if self.estimate_id in (14, 15, 16, 28, 29):
            if self.df.is_otp.sum() != 0:
                failures.append(
                    f"\nThe estimate is {self.estimate_id} but there "
                    f"are {self.df.is_otp.sum()} rows of outpatient data!"
                )
        if self.estimate_id in (14, 15, 28):
            non_pri = len(self.df[self.df.diagnosis_id != 1])
            if non_pri != 0:
                failures.append(f"There are {non_pri} rows with non-primary diagnoses!")
        if failures:
            raise ValueError("\n".join(failures))

    def drop_diagnosis_id(self):
        """After filters are applied assigned remove the diagnosis id column"""

        # remove input to estimate cols
        self.df.drop("diagnosis_id", axis=1, inplace=True)
