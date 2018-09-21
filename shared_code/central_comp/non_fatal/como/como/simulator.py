import os
from multiprocessing import Process, Queue

import pandas as pd
from copy import deepcopy

from simulations import IndependentComorbidity


sentinel = None
os.umask(0o0002)  # Set dUSERt file mask to readable-for all users


class SimulationRunner(object):

    def __init__(self, simulations={}):
        self.simulations = simulations
        self.simulation_results = {}

    def add_simulation_to_queue(self, simkey, sim, *args, **kwargs):
        self.simulations[simkey] = (sim, args, kwargs)

    def run_single_simulation(self, simkey):
        sim = self.simulations[simkey][0]
        args = self.simulations[simkey][1]
        kwargs = self.simulations[simkey][2]
        sim.simulate(*args, **kwargs)
        return sim

    def _q_run_single_simulation(self, inq, outq):
        for simkey in iter(inq.get, sentinel):
            try:
                sim = self.run_single_simulation(simkey)
                result = self.get_simulation_results(sim)
                outq.put((simkey, result))
            except Exception, e:
                outq.put(('simkey: {} '.format(simkey), e))

    def get_simulation_results(self, simulation):
        return simulation.ylds, simulation.agg_causes

    def run_all_simulations_mp(self, n_processes=23):
        inq = Queue()
        outq = Queue()

        # Create and feed sim procs
        sim_procs = []
        for i in range(min([n_processes, len(self.simulations)])):
            p = Process(
                target=self._q_run_single_simulation,
                args=(inq, outq))
            sim_procs.append(p)
            p.start()

        # run the silulations
        for simkey in self.simulations.keys():
            inq.put(simkey)

        # make the workers die after
        for _ in sim_procs:
            inq.put(sentinel)

        # get results
        for simkey in self.simulations.keys():
            proc_result = outq.get()
            self.simulation_results[proc_result[0]] = proc_result[1:]

        # close up the queue
        for p in sim_procs:
            p.join()

    def run_all_simulations_sp(self):
        for simkey in self.simulations.keys():
            sim = self.run_single_simulation(simkey)
            result = self.get_simulation_results(sim)
            self.simulation_results[simkey] = (result,)


class ComoSimulator(object):

    def __init__(self, como_version, dimensions, sequela_inputs,
                 injury_inputs, disability_weights):
        self.como_version = como_version
        self.sequela_inputs = sequela_inputs
        self.injury_inputs = injury_inputs
        self.disability_weights = disability_weights

        self.dimensions = deepcopy(dimensions)
        self.dimensions.index_dim.replace_level("measure_id", 5)
        self.runner = None

    @property
    def _sim_idx(self):
        return self.dimensions.index_names

    @property
    def prevalence_inputs(self):
        df = pd.DataFrame()
        if self.sequela_inputs is not None:
            if self.sequela_inputs.sequela_prevalence is not None:
                df = df.append(self.sequela_inputs.sequela_prevalence)
        if self.injury_inputs is not None:
            if self.injury_inputs.long_term_ncode_prevalence is not None:
                df = df.append(self.injury_inputs.long_term_ncode_prevalence)
        return df

    @property
    def ylds(self):
        results = self.runner.simulation_results
        df_list = []
        for element in results.keys():
            ylds = results[element][0][0].copy()
            idx = pd.MultiIndex.from_tuples(
                [element] * len(ylds), names=self._sim_idx)
            ylds.index = idx
            df_list.append(ylds.reset_index())
        df = pd.concat(df_list)
        df["measure_id"] = 3
        df["sequela_id"] = df.sequela_id.astype(int)
        return df

    @property
    def agg_causes(self):
        results = self.runner.simulation_results
        df_list = []
        for element in results.keys():
            agg_causes = results[element][0][1].copy()
            idx = pd.MultiIndex.from_tuples(
                [element] * len(agg_causes), names=self._sim_idx)
            agg_causes.index = idx
            df_list.append(agg_causes.reset_index())
        return pd.concat(df_list)

    def create_simulations(self, simulation=IndependentComorbidity,
                           *args, **kwargs):
        """create simulations for all unique elements of the data."""
        self.runner = SimulationRunner()

        # set up column indexer
        age_idxr = self._sim_idx.index("age_group_id")
        sim_cols = ["sequela_id"] + self.dimensions.data_list()

        # prepare to slice age specific intellectual disability dws
        id_dws = self.disability_weights.id_dws
        seq_dw = self.como_version.sequela_list[
            ["sequela_id", "healthstate_id"]]
        inj_dw = self.como_version.injury_dws_by_sequela[
            ["sequela_id", "healthstate_id"]]
        all_dw = seq_dw.append(inj_dw)
        all_dw = all_dw.drop_duplicates()

        # prepare to slice prevalence
        all_prev_draws = self.prevalence_inputs.set_index(self._sim_idx)

        # set up a simulation for each non unique dimension
        for element in self.dimensions.index_dim.iter_slices():

            # slice prevalence draws
            prev_draws = all_prev_draws.loc[element, sim_cols]

            # merge with sequela_id
            age_id_dws = id_dws[id_dws.age_group_id == element[age_idxr]]
            dws = self.disability_weights.dws.append(age_id_dws)
            dws = all_dw.merge(dws)
            dws = dws.loc[dws.sequela_id.isin(prev_draws.sequela_id.unique()),
                          sim_cols]

            # create simulation object
            sim = simulation(
                prev_draws, dws, self.dimensions.data_list(),
                self.como_version.agg_cause_map)
            self.runner.add_simulation_to_queue(element, sim, *args, **kwargs)

    def run_all_simulations(self, n_processes=23):
        if n_processes > 1:
            self.runner.run_all_simulations_mp(n_processes=n_processes)
        else:
            self.runner.run_all_simulations_sp()
