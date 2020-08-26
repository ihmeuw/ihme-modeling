import os
import argparse
import pandas as pd

from gbd.constants import measures
from jobmon.client.swarm.workflow.python_task import PythonTask

from como.version import ComoVersion
from como.common import name_task
from como.io import SourceSinkFactory
from como.components.sequela import (SequelaResultComputer,
                                     SequelaInputCollector,
                                     BirthPrevInputCollector)
from como.components.injuries import (InjuryResultComputer,
                                      ENInjuryInputCollector)
from como.components.cause import CauseResultComputer


THIS_FILE = os.path.realpath(__file__)


class IncidenceTaskFactory:

    def __init__(self, como_version, task_registry):
        self.como_version = como_version
        self.task_registry = task_registry

    @staticmethod
    def get_task_name(location_id, sex_id):
        return name_task("como_inci", {"location_id": location_id,
                                       "sex_id": sex_id})

    def get_task(self, location_id, sex_id, n_processes):
        name = self.get_task_name(location_id, sex_id)
        task = PythonTask(
            script=THIS_FILE,
            args=[
                "--como_dir", self.como_version.como_dir,
                "--location_id", location_id,
                "--sex_id", sex_id,
                "--n_processes", n_processes
            ],
            name=name,
            num_cores=25,
            m_mem_free="60G",
            max_attempts=5,
            max_runtime_seconds=(60 * 60 * 6),
            tag="inci")
        self.task_registry[name] = task
        return task


class IncidenceTask:

    def __init__(self, como_version, location_id, sex_id):
        self.como_version = como_version
        self._location_id = location_id
        self._sex_id = sex_id
        self._ss_factory = SourceSinkFactory(como_version)

    def collect_sequela(self, n_processes=20):
        # collect the sequela inputs
        seq_collector = SequelaInputCollector(
            self.como_version,
            location_id=self._location_id,
            sex_id=self._sex_id,
            year_id=None,
            age_group_id=None)
        df_list = seq_collector.collect_sequela_inputs(
            n_processes=n_processes,
            measure_id=[measures.INCIDENCE])
        df_list = [x for x in df_list if isinstance(x, pd.DataFrame)]

        # compile into a single frame
        return pd.concat(df_list)

    def collect_injuries(self, n_processes=20):
        inj_collector = ENInjuryInputCollector(
            self.como_version,
            location_id=self._location_id,
            sex_id=self._sex_id,
            year_id=None,
            age_group_id=None)
        df_list = inj_collector.collect_injuries_inputs(
            n_processes=n_processes, measure_id=[measures.INCIDENCE])
        df_list = [x for x in df_list if isinstance(x, pd.DataFrame)]

        # compile into a single frame
        return pd.concat(df_list)

    def collect_birth_prev(self, n_processes=20):
        birth_prev_collector = BirthPrevInputCollector(
            self.como_version,
            location_id=self._location_id,
            sex_id=self._sex_id,
            year_id=None)
        df_list = birth_prev_collector.collect_birth_prev_inputs(
            n_processes=n_processes)
        df_list = [x for x in df_list if isinstance(x, pd.DataFrame)]

        # compile into a single frame
        return pd.concat(df_list)

    def _compute_sequela_incidence(self, df):
        computer = SequelaResultComputer(self.como_version)
        df = computer.aggregate_sequela(df)
        for col in computer.index_cols:
            df[col] = df[col].astype(int)
        return df

    def _compute_injuries_incidence(self, df):
        computer = InjuryResultComputer(self.como_version)
        df = computer.aggregate_injuries(df)
        for col in computer.index_cols:
            df[col] = df[col].astype(int)
        return df

    def _compute_cause_incidence(self, sequela_df, injuries_df):
        computer = CauseResultComputer(self.como_version)

        # aggregate sequela to the cause level
        sequela_df = sequela_df.groupby(computer.index_cols).sum()
        sequela_df = sequela_df[computer.draw_cols].reset_index()

        # aggregate injuries to the cause level
        injuries_df = injuries_df.groupby(computer.index_cols).sum()
        injuries_df = injuries_df[computer.draw_cols].reset_index()

        df = pd.concat([sequela_df, injuries_df])
        df = computer.aggregate_cause(
            df, self.como_version.inc_cause_set_version_id)

        df = df[computer.index_cols + computer.draw_cols]
        for col in computer.index_cols:
            df[col] = df[col].astype(int)
        return df

    def run_task(self, n_processes):

        seq_df = self.collect_sequela(n_processes=n_processes)
        birth_df = self.collect_birth_prev(n_processes=n_processes)
        seq_df = pd.concat([seq_df, birth_df])
        if "sequela" in self.como_version.components:
            df = self._compute_sequela_incidence(seq_df.copy())
            self._ss_factory.sequela_result_sink.push(
                df, append=False, complib='blosc:zstd', complevel=1)

        inj_df = self.collect_injuries(n_processes=n_processes)
        if "injuries" in self.como_version.components:
            df = self._compute_injuries_incidence(inj_df.copy())
            self._ss_factory.injuries_result_sink.push(
                df, append=False, complib='blosc:zstd', complevel=1)

        df = self._compute_cause_incidence(seq_df, inj_df)
        self._ss_factory.cause_result_sink.push(
            df, append=False, complib='blosc:zstd', complevel=1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="collect sequela inputs and move them")
    parser.add_argument(
        "--como_dir",
        type=str,
        help="directory of como run")
    parser.add_argument(
        "--location_id",
        nargs='*',
        type=int,
        default=[],
        help="location_ids to include in this run")
    parser.add_argument(
        "--sex_id",
        nargs='*',
        type=int,
        default=[],
        help="sex_ids to include in this run")
    parser.add_argument(
        "--n_processes",
        type=int,
        default=20,
        help="how many subprocesses to use")
    args = parser.parse_args()

    cv = ComoVersion(args.como_dir)
    cv.load_cache()
    task = IncidenceTask(cv, args.location_id, args.sex_id)
    task.run_task(args.n_processes)
