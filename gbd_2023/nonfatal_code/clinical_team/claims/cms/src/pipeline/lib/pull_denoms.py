from typing import List

import pandas as pd
from crosscutting_functions.clinical_constants.pipeline import cms as constants
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser
from crosscutting_functions.mapping.clinical_mapping import apply_restrictions

from cms.src.pipeline.lib.apply_elig_filters import filter_elig_vals
from cms.utils import logger, readers
from cms.utils.process_types import Deliverable


class EstimateDenominator:
    """Class preps the denominator for a given estimate_id,
    cms source and reporting output. Bundle ID is used to apply age/sex restrictions
    """

    def __init__(
        self,
        bundle_id: int,
        estimate_id: int,
        cms_system: str,
        deliverable: Deliverable,
        max_denom_type: str,
        clinical_age_group_set_id: int,
        logger: logger,
        map_version: int,
    ):
        """
        Args:
            bundle_id (int): Clinical bundle_id
            estimate_id (int): Clinical estimate_id.
            cms_system (str): CMS reporting source aka 'cms_system'.
                Either 'mdcr' or 'max'
            deliverable (Deliverable):
                Deliverable specific processing object from process_types.
            max_denom_type (str): If processing max data, which type of
                denominator to use.
            clinical_age_group_set_id (int): Age group set used to apply the
                age/sex restrictions.
            logger (logger): CMS Logger from utils
            map_version (int): Clinical bundle map version.
        """
        self.bundle_id = bundle_id
        self.estimate_id = estimate_id
        self.cms_system = cms_system
        self.deliverable = deliverable
        self.max_denom_type = max_denom_type
        self.clinical_age_group_set_id = clinical_age_group_set_id
        self.logger = logger
        self.map_version = map_version
        self.denoms = None

    def pull_denom_data(self, pull_source: str) -> pd.DataFrame:
        """Pull the CMS denom data by deliverable and cms_system
        NOTE, both ushd and gbd deliverables are restricted.
        A filter is applied and defined in constants

        Args:
            pull_source (str): Identifies which denom method to use.
            Accepted values are: 'intermediate_parquet',

        Raises:
            ValueError: pull_source arg is not valid.

        Returns:
            pd.DataFrame: Deliverable specific denominator data.
        """

        min_age = constants.sys_deliverable_ages[self.cms_system][self.deliverable.name][
            "min_age"
        ]
        max_age = constants.sys_deliverable_ages[self.cms_system][self.deliverable.name][
            "max_age"
        ]
        self.logger.info(
            f"Pulling denominator data from {pull_source} between system limit ages of {min_age} and {max_age}"
        )

        filters = [("age", "<=", max_age), ("age", ">=", min_age)]

        if pull_source == "intermediate_parquet":

            base = filepath_parser(
                ini="pipeline.cms", section="table_outputs", section_key="denom"
            )
            path = (
                "FILEPATH"
            )
            cms_system_years = eval(f"constants.{self.cms_system}_years")

            denom_df_list = [
                readers.read_from_parquet(file_path="FILEPATH", filters=filters)
                for year in cms_system_years
            ]

            d_df = pd.concat(denom_df_list, sort=False, ignore_index=True)

            df_condition = self.deliverable.denom_filter_prefix["df_cond"]
            denom_df = d_df[eval(df_condition)]

        else:
            raise ValueError(f"The {pull_source} method is not recognized")

        # rename the location columns in the denoms
        denom_df = denom_df.rename(columns=self.deliverable.rename_loc_dict, inplace=False)
        self.logger.info(f"Column names are {denom_df.columns}")

        assert denom_df.age.max() <= max_age, "Max age in data is too large"
        assert denom_df.age.min() >= min_age, "Min age in data is too small"

        return denom_df

    def five_percent_correct(self) -> None:
        """Determines correct sample based on estimate_id."""
        est_sam = constants.est_id_dict["mdcr"][self.estimate_id]

        if est_sam["sample_status"] == "basic_five_only":
            self.df: pd.DataFrame = self.df[self.df.five_percent_sample]
            self.df.drop("eh_five_percent_sample", axis=1, inplace=True)

    def create_est_denom(self) -> pd.DataFrame:
        """Main method which creates a sample size based on the
        instantiated class attributes.

        Returns:
            pd.DataFrame: Sample size by age, sex_id, year.
        """

        gb_cols = self.get_gb_cols()

        # apply row filters to remove ineligible ppl
        self.df = filter_elig_vals(
            df=self.df,
            cms_system=self.cms_system,
            estimate_id=self.estimate_id,
            max_denom_type=self.max_denom_type,
        )
        if self.cms_system == "mdcr":
            self.five_percent_correct()

        pre_cols = self.df.columns
        self.df = self.df[gb_cols + ["sample_size"]]
        self.logger.info(f"The columns {set(pre_cols) - set(self.df.columns)} will be removed")

        # Avoid zero division
        self.df = self.df[self.df.sample_size > 0]

        assert self.df.isnull().sum().sum() == 0, "Null values present"

        # NOTE: a groupby is necessary in MAX and MDCR for not only
        # five_percent adjustments but also because apply_restrictions
        # (which are applied after fetching the data) creates a terminal age group
        # of 99.
        gb = self.df.groupby(gb_cols).agg({"sample_size": "sum"}).reset_index()

        assert all(gb.sample_size > 0), "0 sample size"
        return gb

    def get_gb_cols(self) -> List[str]:
        """Get a list of columns to aggregate with a pandas groupby."""

        gb_cols = (
            constants.cms_system_deliverable_gb_dict["demo"] + self.deliverable.groupby_cols
        )
        return gb_cols

    def sample_size_restrictions(self) -> pd.DataFrame:
        """Applies individual level restrictions to a bundle sample.

        Returns:
            pd.DataFrame:
        """
        self.df["bundle_id"] = self.bundle_id
        self.df = self.df[self.df.sex_id.isin([1, 2])]
        self.df = apply_restrictions(
            self.df,
            age_set="indv",
            cause_type="bundle",
            clinical_age_group_set_id=self.clinical_age_group_set_id,
            map_version=self.map_version,
        )

        return self.df.drop("bundle_id", axis=1)


def run(
    bundle_id: int,
    estimate_id: int,
    cms_system: str,
    deliverable: Deliverable,
    max_denom_type: str,
    clinical_age_group_set_id: int,
    logger: logger,
    map_version: int,
    pull_source: str = "intermediate_parquet",
) -> pd.DataFrame:
    """A wrapper around EstimateDenominator for the create_est_denom function.

    Returns:
        pd.DataFrame: Sample size by age, sex_id, year.
    """

    denom_class = EstimateDenominator(
        bundle_id=bundle_id,
        estimate_id=estimate_id,
        cms_system=cms_system,
        deliverable=deliverable,
        max_denom_type=max_denom_type,
        clinical_age_group_set_id=clinical_age_group_set_id,
        logger=logger,
        map_version=map_version,
    )
    denom_class.df = denom_class.pull_denom_data(pull_source)
    denom_class.sample_size_restrictions()
    setattr(denom_class, "denoms", denom_class.create_est_denom())

    return denom_class.denoms
