import os
import sys
import argparse
import logging

import numpy as np
import pandas as pd

from gbd.constants import measures
from dataframe_io import WRITE_LOGGER_NAME

from como.legacy.common import name_task
from como.lib.components import impairments, injuries, sequelae
from como.lib.draw_io import SourceSinkFactory
from como.lib.version import ComoVersion

logging.getLogger(WRITE_LOGGER_NAME).setLevel(logging.DEBUG)


THIS_FILE = os.path.realpath(__file__)


class SimulationInputTaskFactory:
    def __init__(self, como_version, task_registry, tool):
        self.como_version = como_version
        self.task_registry = task_registry

        command_template = (
            "{python} {script} "
            "--como_dir {como_dir} "
            "--location_id {location_id} "
            "--sex_id {sex_id} "
            "--n_processes {n_processes} "
        )
        self.task_template = tool.get_task_template(
            template_name="como_sim_input",
            command_template=command_template,
            node_args=[
                "sex_id",
                "location_id",
            ],
            task_args=["como_dir"],
            op_args=["python", "script", "n_processes"],
        )

    @staticmethod
    def get_task_name(location_id, sex_id):
        return name_task("como_sim_input", {"location_id": location_id, "sex_id": sex_id})

    def get_task(self, location_id, sex_id, n_processes):
        # make task
        name = self.get_task_name(location_id, sex_id)
        task = self.task_template.create_task(
            python=sys.executable,
            script=THIS_FILE,
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
        )

        self.task_registry[name] = task
        return task


class SimulationInputTask:
    def __init__(self, como_version, location_id, sex_id):
        self.como_version = como_version
        self._location_id = location_id
        self._sex_id = sex_id
        self._ss_factory = SourceSinkFactory(como_version)

    def collect_sequela(self, n_processes=20):
        # collect the sequela inputs
        seq_collector = sequelae.SequelaInputCollector(
            self.como_version,
            location_id=self._location_id,
            sex_id=self._sex_id,
        )
        df_list = seq_collector.collect_sequela_inputs(
            n_processes=n_processes, measure_id=measures.PREVALENCE
        )

        # compile into a single frame
        return pd.concat(df_list)

    def collect_injuries(self, n_processes=20):
        inj_collector = injuries.ENInjuryInputCollector(
            self.como_version,
            location_id=self._location_id,
            sex_id=self._sex_id,
        )
        df_list = inj_collector.collect_injuries_inputs(
            n_processes=n_processes,
            measure_id=[measures.YLD, measures.ST_PREVALENCE, measures.LT_PREVALENCE],
        )

        # compile into a single frame
        return pd.concat(df_list)

    def collect_sexual_violence(self, n_processes=20):
        sexual_violence_collector = injuries.SexualViolenceInputCollector(
            self.como_version,
            location_id=self._location_id,
            sex_id=self._sex_id,
        )
        df_list = sexual_violence_collector.collect_sexual_violence_inputs(
            n_processes=n_processes, measure_id=[measures.YLD, measures.PREVALENCE]
        )

        # compile into a single frame and aggregate over cause dimensions
        df = pd.concat(df_list)
        idx = self.como_version.nonfatal_dimensions.get_cause_dimensions(
            [measures.YLD, measures.PREVALENCE]
        ).index_names
        df = df.groupby(idx).sum().reset_index()
        return df

    def _compute_sequela_prevalence(self, df):
        computer = sequelae.SequelaResultComputer(self.como_version)
        df = computer.aggregate_sequela(df)
        return df

    def _compute_injuries_prevalence(self, df):
        computer = injuries.InjuryResultComputer(self.como_version)
        df = df[df.measure_id.isin([measures.ST_PREVALENCE, measures.LT_PREVALENCE])]
        df["measure_id"] = measures.PREVALENCE
        df = df.groupby(computer.index_cols).sum().reset_index()
        df = computer.aggregate_injuries(df)
        df = computer.aggregate_ncodes(df)
        df.set_index(computer.index_cols, inplace=True)
        df = df.clip(0, 1).reset_index()
        return df

    def _compute_impairment_prevalence(self, df):
        computer = impairments.ImpairmentResultComputer(self.como_version, df.copy())
        computer.split_id()
        computer.aggregate_reis(sex_id=list(np.atleast_1d(self._sex_id)))
        computer.aggregate_causes()
        d = self.como_version.nonfatal_dimensions.get_impairment_dimensions(
            measures.PREVALENCE
        )
        df = computer.imp_df[d.index_names + d.data_list()]
        return df

    def run_task(self, n_processes=20):
        # collect sequela and export
        df = self.collect_sequela(n_processes=n_processes)
        self._ss_factory.sequela_input_sink.push(df, append=False, format="fixed")

        # export sequela prevalence results
        if "sequela" in self.como_version.components:
            df = self._compute_sequela_prevalence(df)
            self._ss_factory.sequela_result_sink.push(
                df, append=False, complib="blosc:zstd", complevel=1
            )

        if "impairment" in self.como_version.components:
            df = self._compute_impairment_prevalence(df)
            self._ss_factory.impairment_result_sink.push(
                df, append=False, complib="blosc:zstd", complevel=1
            )

        # collect injuries and export
        df = self.collect_injuries(n_processes=n_processes)
        self._ss_factory.injuries_input_sink.push(df, append=False, format="fixed")

        if "injuries" in self.como_version.components:
            df = self._compute_injuries_prevalence(df)
            self._ss_factory.injuries_result_sink.push(
                df, append=False, complib="blosc:zstd", complevel=1
            )

        # collect sexual violence and export
        df = self.collect_sexual_violence(n_processes=n_processes)
        self._ss_factory.sexual_violence_input_sink.push(df, append=False, format="fixed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="collect sequela inputs and move them")
    parser.add_argument("--como_dir", type=str, help="directory of como run")
    parser.add_argument(
        "--location_id",
        nargs="*",
        type=int,
        default=[],
        help="location_ids to include in this run",
    )
    parser.add_argument(
        "--sex_id",
        nargs="*",
        type=int,
        default=[],
        help="sex_ids to include in this run",
    )
    parser.add_argument(
        "--n_processes", type=int, default=20, help="how many subprocesses to use"
    )
    args = parser.parse_args()

    cv = ComoVersion(args.como_dir)
    cv.load_cache()
    task = SimulationInputTask(cv, args.location_id, args.sex_id)
    task.run_task(args.n_processes)