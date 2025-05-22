import argparse
import logging
import os
import pathlib
import sys
from typing import Dict, List, Optional

import pandas as pd

import ihme_cc_cache
from dataframe_io import WRITE_LOGGER_NAME
from gbd import constants as gbd_constants
from jobmon.client.api import Tool
from jobmon.client.task import Task

from como.legacy import common
from como.lib import constants as como_constants
from como.lib import draw_io, minimum_incidence, version
from como.lib.components import causes, injuries, sequelae
from como.lib.tasks.minimum_incidence_cod_task import MinimumIncidenceCodTaskFactory

logging.getLogger(WRITE_LOGGER_NAME).setLevel(logging.DEBUG)

_THIS_FILE = os.path.realpath(__file__)


class IncidenceTaskFactory:
    """Factory to create a task for incidence."""

    def __init__(
        self, como_version: version.ComoVersion, task_registry: Dict[str, Task], tool: Tool
    ) -> None:
        self.como_version = como_version
        self.tmi_cause_ids: Optional[List[int]] = None
        self.task_registry = task_registry
        command_template = (
            "{python} {script} "
            "--como_dir {como_dir} "
            "--location_id {location_id} "
            "--sex_id {sex_id} "
            "--n_processes {n_processes}"
        )
        self.task_template = tool.get_task_template(
            template_name="como_inci",
            command_template=command_template,
            node_args=["location_id", "sex_id"],
            task_args=["como_dir"],
            op_args=["python", "script", "n_processes"],
        )

    @staticmethod
    def get_task_name(location_id: int, sex_id: int) -> str:
        """Returns the incidence task name for a given location and sex."""
        return common.name_task(
            "como_inci",
            {
                gbd_constants.columns.LOCATION_ID: location_id,
                gbd_constants.columns.SEX_ID: sex_id,
            },
        )

    def get_task(self, location_id: int, sex_id: int, n_processes: int) -> Task:
        """Creates and returns an incidence task using the passed location and sex."""
        # make task
        upstream_tasks = []
        if self.tmi_cause_ids is not None:
            for cause_id in self.tmi_cause_ids:
                upstream_name = MinimumIncidenceCodTaskFactory.get_task_name(cause_id)
                upstream_tasks.append(self.task_registry[upstream_name])
        name = self.get_task_name(location_id, sex_id)
        task = self.task_template.create_task(
            python=sys.executable,
            script=_THIS_FILE,
            como_dir=self.como_version.como_dir,
            location_id=location_id,
            sex_id=sex_id,
            n_processes=n_processes,
            name=name,
            compute_resources={
                "cores": 25,
                "memory": "45G",  # Note: allocate ~3G/year
                "runtime": "6h",
            },
            upstream_tasks=upstream_tasks,
        )

        self.task_registry[name] = task
        return task


class IncidenceTask:
    """Task for incidence."""

    def __init__(
        self, como_version: version.ComoVersion, location_id: int, sex_id: int
    ) -> None:
        self.como_version = como_version
        self._location_id = location_id
        self._sex_id = sex_id
        self._ss_factory = draw_io.SourceSinkFactory(como_version)
        # Get TMI index and draw columns
        dimensions = self.como_version.nonfatal_dimensions
        self.tmi_index: List[str] = dimensions.get_cause_dimensions(
            measure_id=gbd_constants.measures.INCIDENCE
        ).index_names
        self.draw_cols: List[str] = dimensions.get_cause_dimensions(
            [gbd_constants.measures.INCIDENCE]
        ).data_list()
        self.tmi_cache_filepath: pathlib.Path = (
            pathlib.Path(self.como_version.como_dir) / como_constants.TMI_CACHE_PATH
        )

    def collect_sequela(self, n_processes: int = 20) -> pd.DataFrame:
        """Collect the sequela inputs."""
        seq_collector = sequelae.SequelaInputCollector(
            self.como_version, location_id=self._location_id, sex_id=self._sex_id
        )
        df_list = seq_collector.collect_sequela_inputs(
            n_processes=n_processes, measure_id=gbd_constants.measures.INCIDENCE
        )
        df_list = [x for x in df_list if isinstance(x, pd.DataFrame)]

        # compile into a single frame
        return pd.concat(df_list)

    def collect_injuries(self, n_processes: int = 20) -> pd.DataFrame:
        """Collect the injury inputs."""
        inj_collector = injuries.ENInjuryInputCollector(
            self.como_version, location_id=self._location_id, sex_id=self._sex_id
        )
        df_list = inj_collector.collect_injuries_inputs(
            n_processes=n_processes, measure_id=[gbd_constants.measures.INCIDENCE]
        )

        # compile into a single frame
        return pd.concat(df_list)

    def collect_birth_prev(self, n_processes: int = 20) -> pd.DataFrame:
        """Collect the birth prevalence inputs."""
        birth_prev_collector = sequelae.BirthPrevInputCollector(
            self.como_version, location_id=self._location_id, sex_id=self._sex_id
        )
        df_list = birth_prev_collector.collect_birth_prev_inputs(n_processes=n_processes)

        # compile into a single frame
        return pd.concat(df_list)

    def _compute_sequela_incidence(self, df: pd.DataFrame) -> pd.DataFrame:
        """Computes and aggregates sequela incidence."""
        computer = sequelae.SequelaResultComputer(self.como_version)
        df = computer.aggregate_sequela(df)
        for col in computer.index_cols:
            df[col] = df[col].astype(int)
        return df

    def _compute_injuries_incidence(self, df: pd.DataFrame) -> pd.DataFrame:
        """Computes and aggregates injury incidence."""
        computer = injuries.InjuryResultComputer(self.como_version)
        df = computer.aggregate_injuries(df)
        df = computer.aggregate_ncodes(df)
        for col in computer.index_cols:
            df[col] = df[col].astype(int)
        return df

    def _compute_cause_incidence(
        self, sequela_df: pd.DataFrame, injuries_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Computes and aggregates cause level incidence usng sequelae and injuries."""
        computer = causes.CauseResultComputer(self.como_version)

        # aggregate sequela to the cause level
        sequela_df = sequela_df.groupby(computer.index_cols).sum()
        sequela_df = sequela_df[computer.draw_cols].reset_index()

        # aggregate injuries to the cause level
        injuries_df = injuries_df.groupby(computer.index_cols).sum()
        injuries_df = injuries_df[computer.draw_cols].reset_index()

        df = pd.concat([sequela_df, injuries_df])
        df = computer.aggregate_cause(df, self.como_version.inc_cause_set_version_id)

        df = df[computer.index_cols + computer.draw_cols]
        for col in computer.index_cols:
            df[col] = df[col].astype(int)
        return df

    def _get_tmi(self, tmi_cache: ihme_cc_cache.FileBackedCacheReader) -> pd.DataFrame:
        """Reads TMI data from disk by location and sex."""
        tmi_dfs: List[pd.DataFrame] = []
        cfr_df = tmi_cache.get(como_constants.TMI_CACHE_CFR)
        cause_list = cfr_df[gbd_constants.columns.CAUSE_ID].unique()
        for cause_id in cause_list:
            tmi_filepath = como_constants.TMI_FILEPATH.format(
                cause_id=cause_id, location_id=self._location_id, sex_id=self._sex_id
            )
            full_filepath = self.tmi_cache_filepath / tmi_filepath
            try:
                tmi_df = pd.read_hdf(full_filepath, key="data")
            # For restricted causes
            except FileNotFoundError:
                continue
            tmi_dfs.append(tmi_df)
        return pd.concat(tmi_dfs)

    def _get_scalable_seqs(self, tmi_cache: ihme_cc_cache.FileBackedCacheReader) -> List[int]:
        """Reads in a list of scalable seqs from disk."""
        scalable_seqs = tmi_cache.get(como_constants.TMI_CACHE_SEQS)
        return scalable_seqs[gbd_constants.columns.SEQUELA_ID]

    def _scale_seqs(
        self, seq_df: pd.DataFrame, tmi_df: pd.DataFrame, scalable_seqs: List[int]
    ) -> pd.DataFrame:
        """Given seq and TMI data, calculates a TMI scalar and applies to scalable
        seqs.
        """
        # Keep static seqs separate...
        static_seq_df = seq_df[~seq_df[gbd_constants.columns.SEQUELA_ID].isin(scalable_seqs)]
        # ...from scalable seqs
        seq_df = seq_df[seq_df[gbd_constants.columns.SEQUELA_ID].isin(scalable_seqs)]
        scalar_df = minimum_incidence.get_scalars(
            tmi_df=tmi_df,
            scalable_seq_df=seq_df,
            static_seq_df=static_seq_df,
            index_cols=self.tmi_index,
            draw_cols=self.draw_cols,
        )
        # If there are non-one scalar values, scale the seq draws
        if not (scalar_df[como_constants.TMI_SCALAR_COL] == 1).all():
            # First, save the scalars to disk
            minimum_incidence.save_scalars(
                scalar_df=scalar_df,
                tmi_cache_filepath=self.tmi_cache_filepath,
                location_id=self._location_id,
                sex_id=self._sex_id,
            )
            # Then do the actual scaling
            seq_df = minimum_incidence.scale_seqs(
                scalable_seq_df=seq_df,
                scalar_df=scalar_df,
                index_cols=self.tmi_index,
                draw_cols=self.draw_cols,
            )
        # Recombine scalable and unscalable seqs
        return pd.concat([seq_df, static_seq_df])

    def run_task(self, n_processes: int) -> None:
        """Runs all incidence computation and saves results to disk.

        All computation assumes values are in number (aggregatable) space.
        """
        seq_df = self.collect_sequela(n_processes=n_processes)
        birth_df = self.collect_birth_prev(n_processes=n_processes)
        seq_df = pd.concat([seq_df, birth_df])
        inj_df = self.collect_injuries(n_processes=n_processes)
        if self.como_version.minimum_incidence_cod:
            manifest_path = self.tmi_cache_filepath / como_constants.TMI_CACHE_MANIFEST_PATH
            tmi_cache = ihme_cc_cache.FileBackedCacheReader(manifest_path)
            tmi_df = self._get_tmi(tmi_cache=tmi_cache)
            scalable_seqs = self._get_scalable_seqs(tmi_cache=tmi_cache)
            seq_df = self._scale_seqs(
                seq_df=seq_df, tmi_df=tmi_df, scalable_seqs=scalable_seqs
            )
        # Write outputs via draw_sink
        if "sequela" in self.como_version.components:
            self._ss_factory.sequela_result_sink.push(
                self._compute_sequela_incidence(seq_df.copy(deep=True)),
                append=False,
                complib="blosc:zstd",
                complevel=1,
            )
        if "injuries" in self.como_version.components:
            self._ss_factory.injuries_result_sink.push(
                self._compute_injuries_incidence(inj_df.copy(deep=True)),
                append=False,
                complib="blosc:zstd",
                complevel=1,
            )
        self._ss_factory.cause_result_sink.push(
            self._compute_cause_incidence(seq_df, inj_df),
            append=False,
            complib="blosc:zstd",
            complevel=1,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="collect sequela inputs and move them")
    parser.add_argument("--como_dir", type=str, help="directory of como run")
    parser.add_argument("--location_id", type=int, help="location_id to include in this run")
    parser.add_argument("--sex_id", type=int, help="sex_id to include in this run")
    parser.add_argument(
        "--n_processes", type=int, default=20, help="how many subprocesses to use"
    )
    args = parser.parse_args()

    cv = version.ComoVersion(args.como_dir, read_only=True)
    cv.load_cache()
    task = IncidenceTask(cv, args.location_id, args.sex_id)
    task.run_task(args.n_processes)
