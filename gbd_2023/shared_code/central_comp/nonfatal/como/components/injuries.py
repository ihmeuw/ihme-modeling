from typing import Dict, List

import pandas as pd
from loguru import logger

from gbd import constants as gbd_constants
from hierarchies import dbtrees

from como.legacy import common as legacy_common
from como.lib import common, constants, draw_io, input_collection_utils, version
from como.lib.types import ModelSourceArgs, MultiInputCollectionArgs
from como.lib.utils import batched


class SexualViolenceInputCollector:
    """Collects filtered sexual violence inputs as dataframes and interpolates missing
    years.

    Args:
        como_verson (version.ComoVersion): ComoVersion object associated with the current
            COMO being run.
        location_id (int): location_id to filter inputs with. If no location is passed, uses
            ComoVersion nonfatal dimensions to determine locations to use.
        sex_id (int): sex_id to filter inputs with. If no sex is passed, uses ComoVersion
            nonfatal dimensions to determine sexes to use.
    """

    def __init__(
        self, como_version: version.ComoVersion, location_id: int, sex_id: int
    ) -> None:
        self.como_version = como_version
        self.location_id = location_id
        self.sex_id = sex_id

    def _get_sexual_violence_collection_args(
        self, chunk_size: int, measure_id: List[int]
    ) -> List[MultiInputCollectionArgs]:
        """Fetches and returns a list of MEs associated with the sexual violence
        sequela list tied to MVIDs.
        """
        memv_df = self.como_version.mvid_list.merge(
            self.como_version.sexual_violence_sequela,
            on=gbd_constants.columns.MODELABLE_ENTITY_ID,
        )
        chunked_args = []
        for indices in batched(range(memv_df.shape[0]), chunk_size):  # noqa: B911
            batch_df = memv_df.iloc[list(indices)]
            models = [
                ModelSourceArgs(
                    modelable_entity_id=itup.modelable_entity_id,
                    model_version_id=itup.model_version_id,
                )
                for itup in batch_df.itertuples()
            ]
            chunked_args.append(
                MultiInputCollectionArgs(
                    como_dir=self.como_version.como_dir,
                    location_id=self.location_id,
                    sex_id=self.sex_id,
                    measure_id=measure_id,
                    model_sources=models,
                    input_collector_type=constants.InputCollector.SEXUAL_VIOLENCE,
                )
            )

        return chunked_args

    def collect_sexual_violence_inputs(
        self, measure_id: List[int], n_processes: int = 20
    ) -> List[pd.DataFrame]:
        """Reads model inputs using multiprocess queues and returns a list of result
        DFs. Potentially exit with 137 exit code to propagate jobmon failures.
        """
        chunked_args = self._get_sexual_violence_collection_args(
            chunk_size=50, measure_id=measure_id
        )
        n_models = sum([len(i.model_sources) for i in chunked_args])
        message = (
            f"For COMO v{self.como_version.como_version_id}, location_id "
            f"{self.location_id}, sex_id {self.sex_id} "
        )
        logger.info(
            message
            + f"attempting to collect sexual violence input data from {n_models} model "
            + f"versions in {len(chunked_args)} chunks with {n_processes} workers."
        )

        result_list = input_collection_utils.collect_input_data(
            chunked_args=chunked_args, n_processes=n_processes
        )
        logger.info(message + "completed collecting sexual violence results.")
        output_dataframes = common.compile_collection_results(result_list=result_list)
        return output_dataframes


class ENInjuryInputCollector:
    """Collects filtered EN injury inputs as dataframes and interpolates missing years.

    Args:
        como_verson (version.ComoVersion): ComoVersion object associated with the current
            COMO being run.
        location_id (int): location_id to filter inputs with. If no location is passed, uses
            ComoVersion nonfatal dimensions to determine locations to use.
        sex_id (int): sex_id to filter inputs with. If no sex is passed, uses ComoVersion
            nonfatal dimensions to determine sexes to use.
    """

    def __init__(
        self, como_version: version.ComoVersion, location_id: int, sex_id: int
    ) -> None:
        self.como_version = como_version
        self.location_id = location_id
        self.sex_id = sex_id

    def _get_en_injury_collection_args(
        self, chunk_size: int, measure_id: List[int]
    ) -> List[MultiInputCollectionArgs]:
        """Fetches and returns a list of MEs associated with the injuries list tied to
        MVIDs.
        """
        memv_df = self.como_version.mvid_list.merge(
            self.como_version.injury_sequela, on=gbd_constants.columns.MODELABLE_ENTITY_ID
        )
        chunked_args = []
        for indices in batched(range(memv_df.shape[0]), chunk_size):  # noqa: B911
            batch_df = memv_df.iloc[list(indices)]
            models = [
                ModelSourceArgs(
                    modelable_entity_id=itup.modelable_entity_id,
                    model_version_id=itup.model_version_id,
                )
                for itup in batch_df.itertuples()
            ]
            chunked_args.append(
                MultiInputCollectionArgs(
                    como_dir=self.como_version.como_dir,
                    location_id=self.location_id,
                    sex_id=self.sex_id,
                    measure_id=measure_id,
                    model_sources=models,
                    input_collector_type=constants.InputCollector.EN_INJURY,
                )
            )

        return chunked_args

    def collect_injuries_inputs(
        self, measure_id: List[int], n_processes: int = 20
    ) -> List[pd.DataFrame]:
        """Reads model inputs using multiprocess queues and returns a list of result
        DFs. Potentially exit with 137 exit code to propagate jobmon failures.
        """
        chunked_args = self._get_en_injury_collection_args(
            chunk_size=50, measure_id=measure_id
        )
        n_models = sum([len(i.model_sources) for i in chunked_args])
        message = (
            f"For COMO v{self.como_version.como_version_id}, location_id "
            f"{self.location_id}, sex_id {self.sex_id} "
        )
        logger.info(
            message
            + f"attempting to collect EN injury input data from {n_models} model "
            + f"versions in {len(chunked_args)} chunks with {n_processes} workers."
        )

        result_list = input_collection_utils.collect_input_data(
            chunked_args=chunked_args, n_processes=n_processes
        )
        logger.info(message + "completed collecting EN injury results.")
        output_dataframes = common.compile_collection_results(result_list=result_list)
        return output_dataframes


class InjuryResultComputer:
    """Computes injury and ncode aggregates.

    Args:
        como_verson (version.ComoVersion): ComoVersion object associated with the current
            COMO being run.
    """

    def __init__(self, como_version: version.ComoVersion) -> None:
        # inputs
        self.como_version = como_version

        # set up draw source factory
        self._ss_factory = draw_io.SourceSinkFactory(como_version)

        # set up the dimensions we are using
        self.dimensions = self.como_version.nonfatal_dimensions

    def get_injuries_dimensions(self, measure_id: int) -> Dict[str, List[int]]:
        """Returns injury dimensions for a passed measure ID."""
        return self.dimensions.get_injuries_dimensions(measure_id=measure_id, at_birth=False)

    @property
    def index_cols(self) -> List[str]:
        """Returns the index column names for injury models."""
        # measure/birth dimension doesn't matter for columns
        return self.dimensions.get_injuries_dimensions(
            measure_id=gbd_constants.measures.PREVALENCE, at_birth=False
        ).index_names

    @property
    def draw_cols(self) -> List[str]:
        """Returns the draw column names for injury models."""
        # measure/birth dimension doesn't matter for columns
        return self.dimensions.get_injuries_dimensions(
            measure_id=gbd_constants.measures.PREVALENCE, at_birth=False
        ).data_list()

    def aggregate_injuries(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generates and returns injury cause aggregates."""
        ct = dbtrees.causetree(
            cause_set_version_id=self.como_version.cause_set_version_id,
            release_id=self.como_version.release_id,
        )
        df = legacy_common.agg_hierarchy(
            tree=ct,
            df=df,
            index_cols=self.index_cols,
            data_cols=self.draw_cols,
            dimension=gbd_constants.columns.CAUSE_ID,
        )
        df = df[self.index_cols + self.draw_cols]
        return df

    def aggregate_ncodes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generates and returns injury ncode aggregates using parent ID."""
        # aggregate ncodes
        df = df.merge(self.como_version.ncode_hierarchy)
        df_agg = df.copy()
        df_agg = (
            df_agg.groupby(
                [
                    gbd_constants.columns.YEAR_ID,
                    gbd_constants.columns.LOCATION_ID,
                    gbd_constants.columns.AGE_GROUP_ID,
                    gbd_constants.columns.SEX_ID,
                    gbd_constants.columns.MEASURE_ID,
                    gbd_constants.columns.CAUSE_ID,
                    "parent_id",
                ]
            )[self.draw_cols]
            .sum()
            .reset_index()
        )
        df_agg = df_agg.rename(columns={"parent_id": gbd_constants.columns.REI_ID})

        # set attribute
        df = pd.concat([df, df_agg])
        df = df[self.index_cols + self.draw_cols]
        for col in self.index_cols:
            df[col] = df[col].astype(int)
        return df
