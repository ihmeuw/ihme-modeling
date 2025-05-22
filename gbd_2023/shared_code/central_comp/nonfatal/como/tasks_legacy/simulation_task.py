import os
import sys
import argparse

import numpy as np
import pandas as pd

from draw_sources.draw_sources import DrawSink
from gbd.constants import measures

from como.legacy.common import name_task, broadcast, apply_restrictions
from como.legacy.components.disability_weights import DisabilityWeightInputs
from como.legacy.simulate import ComoSimulator
from como.legacy.tasks.disability_weight_task import DisabilityWeightTaskFactory
from como.legacy.tasks.simulation_input_task import SimulationInputTaskFactory
from como.lib.components import causes, injuries, sequelae
from como.lib.components.impairments import ImpairmentResultComputer
from como.lib.draw_io import SourceSinkFactory
from como.lib.resource_file_io import get_impairments
from como.lib.version import ComoVersion

THIS_FILE = os.path.realpath(__file__)


class SimulationTaskFactory:
    def __init__(self, como_version, task_registry, tool):
        self.como_version = como_version
        self.task_registry = task_registry

        command_template = (
            "{python} {script} "
            "--como_dir {como_dir} "
            "--location_id {location_id} "
            "--sex_id {sex_id} "
            "--year_id {year_id} "
            "--n_processes {n_processes} "
        )
        self.task_template = tool.get_task_template(
            template_name="como_sim",
            command_template=command_template,
            node_args=[
                "sex_id",
                "location_id",
                "year_id",
            ],
            task_args=["como_dir"],
            op_args=["python", "script", "n_processes"],
        )

    @staticmethod
    def get_task_name(location_id, sex_id, year_id):
        return name_task(
            "como_sim",
            {"location_id": location_id, "sex_id": sex_id, "year_id": year_id},
        )

    def get_task(self, location_id, sex_id, year_id, n_processes):
        dw_task = DisabilityWeightTaskFactory.get_task_name(location_id)
        sim_input_task = SimulationInputTaskFactory.get_task_name(location_id, sex_id)
        upstream_tasks = [self.task_registry[task] for task in [dw_task, sim_input_task]]

        # make task
        name = self.get_task_name(location_id, sex_id, year_id)
        task = self.task_template.create_task(
            python=sys.executable,
            script=THIS_FILE,
            como_dir=self.como_version.como_dir,
            location_id=location_id,
            year_id=year_id,
            sex_id=sex_id,
            n_processes=n_processes,
            name=name,
            compute_resources={
                "cores": 25,
                "memory": "45G",  # Note: allocate ~3G/year
                "runtime": "3h",
            },
            upstream_tasks=upstream_tasks,
        )

        self.task_registry[name] = task
        return task


class SimulationTask:
    def __init__(self, como_version, location_id, sex_id, year_id):
        self.como_version = como_version
        self.dimensions = self.como_version.nonfatal_dimensions

        if location_id:
            self.dimensions.simulation_index["location_id"] = location_id
        if year_id:
            self.dimensions.simulation_index["year_id"] = year_id
        if sex_id:
            self.dimensions.simulation_index["sex_id"] = sex_id

        self._ss_factory = SourceSinkFactory(como_version)

    @property
    def injuries_long_term_prev(self):
        dim = self.dimensions.get_simulation_dimensions(measures.LT_PREVALENCE)
        filters = dim.index_dim.to_dict()["levels"]
        return self._ss_factory.injuries_input_source.content(filters=filters)

    @property
    def injuries_short_term_prev(self):
        dim = self.dimensions.get_simulation_dimensions(measures.ST_PREVALENCE)
        filters = dim.index_dim.to_dict()["levels"]
        return self._ss_factory.injuries_input_source.content(filters=filters)

    @property
    def injuries_short_term_ylds(self):
        dim = self.dimensions.get_simulation_dimensions(measures.YLD)
        filters = dim.index_dim.to_dict()["levels"]
        return self._ss_factory.injuries_input_source.content(filters=filters)

    @property
    def sexual_violence_ylds(self):
        dim = self.dimensions.get_simulation_dimensions(measures.YLD)
        filters = dim.index_dim.to_dict()["levels"]
        return self._ss_factory.sexual_violence_input_source.content(filters=filters)

    @property
    def sexual_violence_prev(self):
        dim = self.dimensions.get_simulation_dimensions(measures.PREVALENCE)
        filters = dim.index_dim.to_dict()["levels"]
        return self._ss_factory.sexual_violence_input_source.content(filters=filters)

    @property
    def sequela_prev(self):
        dim = self.dimensions.get_simulation_dimensions(measures.PREVALENCE)
        filters = dim.index_dim.to_dict()["levels"]
        return self._ss_factory.sequela_input_source.content(filters=filters)

    @property
    def disability_weights(self):
        # get the disability weights
        dws_inputs = DisabilityWeightInputs(
            self.como_version,
            self.dimensions.get_simulation_dimensions(measures.PREVALENCE),
        )
        dws_inputs.get_dws()
        dws_inputs.get_id_dws()
        dws_inputs.get_epi_dws()
        return dws_inputs

    def run_task(self, n_processes):
        # compile inputs
        long_en_prev = self.injuries_long_term_prev
        short_en_prev = self.injuries_short_term_prev
        seq_prev = self.sequela_prev

        # run simulation
        prevalence_df = pd.concat(
            [seq_prev, self._prepare_ncode_aggregates(long_en_prev.copy())], sort=True
        )
        prevalence_df["measure_id"] = measures.PREVALENCE
        como = self.simulate(prevalence_df, n_processes)

        # get the inputs for final ylds
        seq_dim = self.dimensions.get_sequela_dimensions(measures.YLD)
        simulated_ylds = como.ylds[seq_dim.index_names + seq_dim.data_list()]

        en_ylds = self._compute_en_ylds(
            simulated_ylds, long_en_prev, self.injuries_short_term_ylds
        )

        if "sequela" in self.como_version.components:
            df = self._compute_sequela_ylds(simulated_ylds)
            self._ss_factory.sequela_result_sink.push(
                df, append=False, complib="blosc:zstd", complevel=1
            )

        if "injuries" in self.como_version.components:
            df = self._compute_injuries_ylds(en_ylds)
            self._ss_factory.injuries_result_sink.push(
                df, append=False, complib="blosc:zstd", complevel=1
            )

        # final cause ylds
        df = self._compute_cause_ylds(simulated_ylds, en_ylds)
        self._ss_factory.cause_result_sink.push(
            df, append=False, complib="blosc:zstd", complevel=1
        )

        # final cause prevalence
        df = self._compute_cause_prev(
            prevalence_df, long_en_prev, short_en_prev
        )
        self._ss_factory.cause_result_sink.push(
            df, append=False, complib="blosc:zstd", complevel=1
        )

        if "impairment" in self.como_version.components:
            imp_config = get_impairments()
            imp_prev = (
                prevalence_df.drop(["healthstate_id"], axis=1)
                .merge(imp_config, how="inner", on="sequela_id")
                .drop(["sequela_id", "rei_id"], axis=1)
            )
            imp_prev = imp_prev.rename({"fake_id": "sequela_id"}, axis="columns")
            imp_como = self.simulate(imp_prev, n_processes)

            # get the inputs for final ylds
            imp_simulated_ylds = imp_como.ylds[seq_dim.index_names + seq_dim.data_list()]
            imp_simulated_ylds = imp_simulated_ylds.rename(
                {"sequela_id": "fake_id"}, axis="columns"
            )
            imp_simulated_ylds = imp_simulated_ylds.merge(
                imp_config, how="inner", on="fake_id"
            ).drop(["fake_id", "healthstate_id"], axis=1)
            df = self._compute_impairment_ylds(imp_simulated_ylds)
            self._ss_factory.impairment_result_sink.push(
                df, append=False, complib="blosc:zstd", complevel=1
            )

    def simulate(self, prevalence_df, n_processes):
        sim_idx = self.dimensions.get_simulation_dimensions(measures.PREVALENCE)

        # simulate
        como_sim = ComoSimulator(
            self.como_version, sim_idx, prevalence_df, self.disability_weights
        )
        como_sim.create_simulations(n_simulants=self.como_version.n_simulants)
        como_sim.run_all_simulations(n_processes=n_processes)
        return como_sim

    def _compute_en_ylds(self, simulated_ylds, long_term_en_prev, short_term_en_ylds):
        # compute the en matrix
        sim_dim = self.dimensions.get_simulation_dimensions(measures.PREVALENCE)
        denom = self._prepare_ncode_aggregates(long_term_en_prev.copy())
        denom = denom[sim_dim.index_names + ["sequela_id"] + sim_dim.data_list()]
        numer = long_term_en_prev[
            sim_dim.index_names + ["cause_id", "sequela_id"] + sim_dim.data_list()
        ]
        en_mat = broadcast(
            broadcast_onto_df=numer,
            broadcast_df=denom,
            index_cols=sim_dim.index_names + ["sequela_id"],
            operator="/",
        )
        en_mat.fillna(0, inplace=True)
        en_mat["measure_id"] = measures.YLD

        # apply the matrix to the ylds
        en_mat = en_mat.merge(simulated_ylds, on=sim_dim.index_names + ["sequela_id"])
        en_mat = en_mat.join(
            pd.DataFrame(
                data=(
                    en_mat.filter(regex="draw.*x").values
                    * en_mat.filter(regex="draw.*y").values
                ),
                index=en_mat.index,
                columns=sim_dim.data_list(),
            )
        )
        en_mat = apply_restrictions(
            self.como_version.cause_restrictions, en_mat, sim_dim.data_list()
        )

        # clean the short term ylds
        rei_map = self.como_version.injury_sequela[["cause_id", "sequela_id", "rei_id"]]
        en_mat = en_mat.merge(rei_map, on=["sequela_id", "cause_id"])
        en_mat = en_mat[sim_dim.index_names + ["cause_id", "rei_id"] + sim_dim.data_list()]
        en_mat.fillna(0, inplace=True)

        # compute the combine ylds
        en_ylds = pd.concat([en_mat, short_term_en_ylds])
        en_ylds = en_ylds.groupby(sim_dim.index_names + ["cause_id", "rei_id"])
        en_ylds = en_ylds.sum()[sim_dim.data_list()].reset_index()
        return en_ylds

    def _compute_cause_ylds(self, simulated_ylds, en_ylds):
        computer = causes.CauseResultComputer(self.como_version)

        # compute injuries ylds
        inj_ylds = pd.concat([en_ylds, self.sexual_violence_ylds])
        inj_ylds = inj_ylds.groupby(computer.index_cols).sum()
        inj_ylds = inj_ylds[computer.draw_cols].reset_index()

        # compute simulated ylds
        cause_ylds = simulated_ylds.merge(self.como_version.sequela_list, on="sequela_id")
        cause_ylds = cause_ylds.groupby(computer.index_cols).sum()
        cause_ylds = cause_ylds[computer.draw_cols].reset_index()

        # compute residuals
        all_ylds = pd.concat([inj_ylds, cause_ylds])
        residual_ylds = computer.residuals(all_ylds, simulated_ylds)

        # aggregate up cause hierarchy
        all_ylds = pd.concat([all_ylds, residual_ylds])
        all_ylds = computer.aggregate_cause(all_ylds, self.como_version.cause_set_version_id)
        return all_ylds

    def _compute_cause_prev(self, sequela_prev, long_en_prev, short_en_prev):
        computer = causes.CauseResultComputer(self.como_version)

        # combine long- and short-term injury sequela & aggregate from the sequela to the cause level
        # Note: this bit calculates the most-detailed cause prev since the sequela under a cause are mutually exclusive so the most-detailed cause prevalence is equal to the sum of prevalence across sequela
        all_prev = pd.concat(
            [sequela_prev, long_en_prev, short_en_prev, self.sexual_violence_prev]
        )
        all_prev["measure_id"] = measures.PREVALENCE
        all_prev = all_prev.groupby(computer.index_cols).sum()
        all_prev = all_prev[computer.draw_cols].reset_index()

        # enforce threshold constraints on prevalence for injuries
        # Note: Some injuries have prevalence > 1 due to the
        # collapse across long- and short-term prevalence (i.e., the .sum()
        # above)
        all_prev.set_index(computer.index_cols, inplace=True)
        all_prev = all_prev.clip(0, 1).reset_index()

        # calculate the prevalence for each non-most detailed cause
        agg_cause_prev = computer.aggregate_cause_prevalence(
            all_prev,
            self.como_version.cause_set_version_id,
        )

        # aggregate the entire cause hierarchy to get the sum of child causes
        all_prev = computer.aggregate_cause(all_prev, self.como_version.cause_set_version_id)

        all_prev.set_index(computer.index_cols, inplace=True)
        all_prev = all_prev.clip(0, 1).reset_index()

        # combine the cause prevalences
        all_prev = all_prev[~all_prev.cause_id.isin(agg_cause_prev.cause_id.unique())]
        all_prev = pd.concat([all_prev, agg_cause_prev])

        df = all_prev[(all_prev[computer.draw_cols] > 1).any(1)]
        if not df.empty:
            raise ValueError(
                "A prevalence value above the max threshold was found."
                f" cause_id: {df.cause_id.unique().tolist()}; age_group_id: "
                f"{df.age_group_id.unique().tolist()}"
            )

        return all_prev

    def _compute_injuries_ylds(self, en_ylds):
        computer = injuries.InjuryResultComputer(self.como_version)
        en_ylds = computer.aggregate_injuries(en_ylds)
        en_ylds = computer.aggregate_ncodes(en_ylds)
        return en_ylds

    def _compute_impairment_ylds(self, df: pd.DataFrame) -> pd.DataFrame:
        computer = ImpairmentResultComputer(self.como_version, df.copy())
        computer.split_id()
        # Temporary update to write out sequela-level impairment draws
        dim = self.dimensions.get_dimension_by_component(
            component="sequela", measure_id=measures.YLD
        )
        temp_sink = DrawSink(
            params={
                "draw_dir": os.path.join(
                    self.como_version.como_dir, "draws", "impairment", "sequela"
                ),
                "file_pattern": "FILEPATH",
                "data_cols": dim.data_list(),
                "index_cols": dim.index_names,
            }
        )
        temp_sink.push(
            computer.imp_df, append=False, complib="blosc:zstd", complevel=1
        )
        computer.aggregate_reis(
            sex_id=list(np.atleast_1d(self.dimensions.simulation_index["sex_id"]))
        )
        computer.aggregate_causes()
        dim = self.dimensions.get_impairment_dimensions(measures.YLD)
        df = computer.imp_df[dim.index_names + dim.data_list()]
        return df

    def _compute_sequela_ylds(self, df):
        # need to add residuals
        computer = sequelae.SequelaResultComputer(self.como_version)
        residual_ylds = computer.residuals(df)
        df = pd.concat([df, residual_ylds])
        df = computer.aggregate_sequela(df)
        return df

    def _prepare_ncode_aggregates(self, df):
        seq_dim = self.dimensions.get_sequela_dimensions(measures.PREVALENCE)
        seq_idx = seq_dim.index_names + ["healthstate_id"]
        df = df.groupby(seq_idx)[seq_dim.data_list()].sum().reset_index()
        return df


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
        "--year_id",
        nargs="*",
        type=int,
        default=[],
        help="sex_ids to include in this run",
    )
    parser.add_argument(
        "--n_processes", type=int, default=23, help="how many subprocesses to use"
    )
    args = parser.parse_args()

    cv = ComoVersion(args.como_dir)
    cv.load_cache()
    task = SimulationTask(cv, args.location_id, args.sex_id, args.year_id)
    task.run_task(args.n_processes)
