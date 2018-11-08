import os
import argparse

import pandas as pd

from jobmon.workflow.python_task import PythonTask

from como.version import ComoVersion
from como.common import name_task
from como.io import SourceSinkFactory
from como.components.sequela import (SequelaResultComputer,
                                     SequelaInputCollector)
from como.components.injuries import (InjuryResultComputer,
                                      ENInjuryInputCollector,
                                      SexualViolenceInputCollector)
from como.components.impairments import ImpairmentResultComputer


this_file = os.path.realpath(__file__)


class SimulationInputTaskFactory(object):

    def __init__(self, como_version, task_registry):
        self.como_version = como_version
        self.task_registry = task_registry

    @staticmethod
    def get_task_name(location_id, sex_id):
        return name_task("como_sim_input", {"location_id": location_id,
                                            "sex_id": sex_id})

    def get_task(self, location_id, sex_id, n_processes):
        name = self.get_task_name(location_id, sex_id)
        task = PythonTask(
            script=this_file,
            args=[
                "--como_dir", self.como_version.como_dir,
                "--location_id", location_id,
                "--sex_id", sex_id,
                "--n_processes", n_processes
            ],
            name=name,
            slots=25,
            mem_free=100,
            max_attempts=5,
            max_runtime=(60 * 60 * 6),
            tag="sim_input")
        self.task_registry[name] = task
        return task


class SimulationInputTask(object):

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
            sex_id=self._sex_id)
        df_list = seq_collector.collect_sequela_inputs(
            n_processes=n_processes,
            measure_id=5)
        # error_list = [x for x in seq_df_list if not isinstance(x,
        #               pd.DataFrame)]
        df_list = [x for x in df_list if isinstance(x, pd.DataFrame)]

        # compile into a single frame
        return pd.concat(df_list)

    def collect_injuries(self, n_processes=20):
        inj_collector = ENInjuryInputCollector(
            self.como_version,
            location_id=self._location_id,
            sex_id=self._sex_id)
        df_list = inj_collector.collect_injuries_inputs(
            n_processes=n_processes, measure_id=[3, 35, 36])
        df_list = [x for x in df_list if isinstance(x, pd.DataFrame)]

        # compile into a single frame
        return pd.concat(df_list)

    def collect_sexual_violence(self, n_processes=20):
        sexual_violence_collector = SexualViolenceInputCollector(
            self.como_version,
            location_id=self._location_id,
            sex_id=self._sex_id)
        df_list = sexual_violence_collector.collect_sexual_violence_inputs(
            n_processes=n_processes, measure_id=[3, 5])
        df_list = [x for x in df_list if isinstance(x, pd.DataFrame)]

        # compile into a single frame and aggregate over cause dimensions
        df = pd.concat(df_list)
        idx = self.como_version.nonfatal_dimensions.get_cause_dimensions(
            [3, 5]).index_names
        df = df.groupby(idx).sum().reset_index()
        return df

    def _compute_sequela_prevalence(self, df):
        computer = SequelaResultComputer(self.como_version)
        df = computer.aggregate_sequela(df)
        return df

    def _compute_injuries_prevalence(self, df):
        computer = InjuryResultComputer(self.como_version)
        df = df[df.measure_id.isin([35, 36])]
        df["measure_id"] = 5
        df = df.groupby(computer.index_cols).sum().reset_index()
        df = computer.aggregate_injuries(df)
        df.set_index(computer.index_cols, inplace=True)
        df = df.clip(0, 1).reset_index()
        return df

    def _compute_impairment_prevalence(self, df):
        computer = ImpairmentResultComputer(self.como_version, df.copy())
        computer.split_epilepsy()
        computer.split_id()
        computer.aggregate_reis()
        computer.aggregate_causes()
        d = self.como_version.nonfatal_dimensions.get_impairment_dimensions(5)
        df = computer.imp_df[d.index_names + d.data_list()]
        return df

    def run_task(self, n_processes=20):
        # collect sequela and export
        df = self.collect_sequela(n_processes=n_processes)
        self._ss_factory.sequela_input_sink.push(df, append=False,
                                                 format='fixed')

        # export sequela prevalence results
        if "sequela" in self.como_version.components:
            df = self._compute_sequela_prevalence(df)
            self._ss_factory.sequela_result_sink.push(
                df, append=False, complib='blosc:zstd', complevel=1)

        if "impairment" in self.como_version.components:
            df = self._compute_impairment_prevalence(df)
            self._ss_factory.impairment_result_sink.push(
                df, append=False, complib='blosc:zstd', complevel=1)

        # collect injuries and export
        df = self.collect_injuries(n_processes=n_processes)
        self._ss_factory.injuries_input_sink.push(df, append=False,
                                                  format='fixed')

        if "injuries" in self.como_version.components:
            df = self._compute_injuries_prevalence(df)
            self._ss_factory.injuries_result_sink.push(
                df, append=False, complib='blosc:zstd', complevel=1)

        # collect sexual violence and export
        df = self.collect_sexual_violence(n_processes=n_processes)
        self._ss_factory.sexual_violence_input_sink.push(df, append=False,
                                                         format='fixed')


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
    task = SimulationInputTask(cv, args.location_id, args.sex_id)
    task.run_task(args.n_processes)
