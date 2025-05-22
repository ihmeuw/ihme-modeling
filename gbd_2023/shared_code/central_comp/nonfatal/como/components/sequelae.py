from typing import List, Optional

import pandas as pd
from loguru import logger

from gbd import constants as gbd_constants
from hierarchies import dbtrees

from como.legacy import common as legacy_common
from como.legacy import residuals
from como.lib import common, constants, draw_io, input_collection_utils, version
from como.lib.types import ModelSourceArgs, MultiInputCollectionArgs
from como.lib.utils import batched


class BirthPrevInputCollector:
    """Collects filtered birth prevalence inputs as dataframes and interpolates missing
    years.

    Args:
        como_verson (version.ComoVersion): ComoVersion object associated with the current
            COMO being run.
        location_id (int): location_id to filter inputs with. If no location is passed, uses
            ComoVersion nonfatal dimensions to determine locations to use.
        year_id (int): year_id to filter inputs with. If no year is passed, uses ComoVersion
            nonfatal dimensions to determine years to use. Interpolates missing years.
        sex_id (int): sex_id to filter inputs with. If no sex is passed, uses ComoVersion
            nonfatal dimensions to determine sexes to use.
    """

    def __init__(
        self,
        como_version: version.ComoVersion,
        location_id: Optional[int],
        sex_id: Optional[int],
    ) -> None:
        self.como_version = como_version
        self.location_id = location_id
        self.sex_id = sex_id

    def _get_birth_prev_collection_args(
        self, chunk_size: int
    ) -> List[MultiInputCollectionArgs]:
        """Fetches and returns a list of MEs associated with the BP list tied to
        MVIDs.
        """
        memv_df = self.como_version.mvid_list.merge(
            self.como_version.birth_prev, on=gbd_constants.columns.MODELABLE_ENTITY_ID
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
                    measure_id=gbd_constants.measures.PREVALENCE,
                    model_sources=models,
                    input_collector_type=constants.InputCollector.BIRTH_PREV,
                )
            )

        return chunked_args

    def collect_birth_prev_inputs(
        self, n_processes: int = 20, chunk_size: int = 40
    ) -> List[pd.DataFrame]:
        """Reads model inputs using multiprocess queues and returns a list of result
        DFs. Potentially exit with 137 exit code to propagate jobmon failures.
        """
        chunked_args = self._get_birth_prev_collection_args(chunk_size=50)
        n_models = sum([len(i.model_sources) for i in chunked_args])
        message = (
            f"For COMO v{self.como_version.como_version_id}, location_id "
            f"{self.location_id}, sex_id {self.sex_id} "
        )
        logger.info(
            message
            + f"attempting to collect birth prevalence input data from {n_models} model "
            + f"versions in {len(chunked_args)} chunks with {n_processes} workers."
        )

        result_list = input_collection_utils.collect_input_data(
            chunked_args=chunked_args, n_processes=n_processes
        )
        logger.info(message + "completed collecting birth prevalence results.")
        output_dataframes = common.compile_collection_results(result_list=result_list)
        return output_dataframes


class SequelaInputCollector:
    """Collects filtered sequelae inputs as dataframes and interpolates missing years.

    Args:
        como_verson (version.ComoVersion): ComoVersion object associated with the current
            COMO being run.
        location_id (int): location_id to filter inputs with. If no location is passed, uses
            ComoVersion nonfatal dimensions to determine locations to use.
        sex_id (int): sex_id to filter inputs with. If no sex is passed, uses ComoVersion
            nonfatal dimensions to determine sexes to use.
    """

    def __init__(
        self,
        como_version: version.ComoVersion,
        location_id: Optional[int],
        sex_id: Optional[int],
    ) -> None:
        self.como_version = como_version
        self.location_id = location_id
        self.sex_id = sex_id

    def _get_sequela_collection_args(
        self, chunk_size: int, measure_id: int
    ) -> List[MultiInputCollectionArgs]:
        """Fetches and returns a list of MEs associated with the sequela list tied to
        MVIDs.
        """
        memv_df = self.como_version.mvid_list.merge(
            self.como_version.sequela_list, on=gbd_constants.columns.MODELABLE_ENTITY_ID
        )
        memv_df = memv_df[memv_df.sequela_id > 0]
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
                    input_collector_type=constants.InputCollector.SEQUELA,
                )
            )

        return chunked_args

    def collect_sequela_inputs(
        self, measure_id: int, n_processes: int = 20, chunk_size: int = 40
    ) -> List[pd.DataFrame]:
        """Reads model inputs using multiprocess queues and returns a list of result
        DFs. Potentially exit with 137 exit code to propagate jobmon failures.
        """
        chunked_args = self._get_sequela_collection_args(chunk_size=50, measure_id=measure_id)
        n_models = sum([len(i.model_sources) for i in chunked_args])
        message = (
            f"For COMO v{self.como_version.como_version_id}, location_id "
            f"{self.location_id}, sex_id {self.sex_id} "
        )
        logger.info(
            message
            + f"attempting to collect sequela input data from {n_models} model "
            + f"versions in {len(chunked_args)} chunks with {n_processes} workers."
        )
        result_list = input_collection_utils.collect_input_data(
            chunked_args=chunked_args, n_processes=n_processes
        )
        logger.info(message + "completed collecting sequela results.")
        output_dataframes = common.compile_collection_results(result_list=result_list)
        return output_dataframes


class SequelaResultComputer:
    """Computes sequelae aggregates and residuals.

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

    @property
    def index_cols(self) -> List[str]:
        """Returns the index column names for sequelae models."""
        # measure/birth dimension doesn't matter for columns
        return self.dimensions.get_sequela_dimensions(
            measure_id=gbd_constants.measures.PREVALENCE, at_birth=False
        ).index_names

    @property
    def draw_cols(self) -> List[str]:
        """Returns the draw column names for sequelae models."""
        # measure/birth dimension doesn't matter for columns
        return self.dimensions.get_sequela_dimensions(
            measure_id=gbd_constants.measures.PREVALENCE, at_birth=False
        ).data_list()

    def aggregate_sequela(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generates and returns sequelae aggregates."""
        seq_tree = dbtrees.sequelatree(
            sequela_set_id=2,
            sequela_set_version_id=self.como_version.sequela_set_version_id,
            release_id=self.como_version.release_id,
        )
        df = df[self.index_cols + self.draw_cols]
        df = df.groupby(self.index_cols).sum().reset_index()
        df = legacy_common.agg_hierarchy(
            tree=seq_tree,
            df=df,
            index_cols=self.index_cols,
            data_cols=self.draw_cols,
            dimension=gbd_constants.columns.SEQUELA_ID,
        )
        df = df[self.index_cols + self.draw_cols]
        for col in self.index_cols:
            df[col] = df[col].astype(int)
        return df

    def residuals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculates cause/sequelae residuals."""
        cause_ylds = df.merge(
            self.como_version.sequela_list, on=gbd_constants.columns.SEQUELA_ID
        )
        cause_idx = [
            col for col in self.index_cols if col != gbd_constants.columns.SEQUELA_ID
        ] + [gbd_constants.columns.CAUSE_ID]
        cause_ylds = cause_ylds.groupby(cause_idx).sum()
        cause_ylds = cause_ylds[self.draw_cols].reset_index()
        dims = self.dimensions.get_simulation_dimensions(
            measure_id=gbd_constants.measures.YLD, at_birth=False
        )
        location_id = cause_ylds[gbd_constants.columns.LOCATION_ID].unique().item()
        ratio_df = residuals.calc(
            location_id=location_id,
            ratio_df=self.como_version.global_ratios,
            output_type=gbd_constants.columns.SEQUELA_ID,
            drawcols=dims.data_list(),
            seq_ylds=df,
            cause_ylds=cause_ylds,
        )
        ratio_df = ratio_df[self.index_cols + self.draw_cols]
        for col in self.index_cols:
            ratio_df[col] = ratio_df[col].astype(int)
        return ratio_df
