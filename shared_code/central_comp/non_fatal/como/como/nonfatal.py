from itertools import product

from como.inputs import ModelableEntityInputWalker, DisabilityWeightInputs
from como.sequela import SequelaInputContainer, SequelaResultContainer
from como.injuries import InjuryInputContainer, InjuryResultContainer
from como.cause import CauseResultContainer
from como.impairments import ImpairmentResultContainer
from como.simulator import ComoSimulator


class ComputeNonfatal(object):

    inputs_strategy_map = {
        ("sequela", 3): [(SequelaInputContainer, 3)],
        ("sequela", 5): [(SequelaInputContainer, 5)],
        ("sequela", 6): [(SequelaInputContainer, 6)],
        ("cause", 3): [(SequelaInputContainer, 3), (InjuryInputContainer, 3)],
        ("cause", 5): [(SequelaInputContainer, 5), (InjuryInputContainer, 5)],
        ("cause", 6): [(SequelaInputContainer, 6), (InjuryInputContainer, 6)],
        ("injuries", 3): [(InjuryInputContainer, 3)],
        ("injuries", 5): [(InjuryInputContainer, 5)],
        ("injuries", 6): [(InjuryInputContainer, 6)],
        ("impairment", 3): [(SequelaInputContainer, 3)],
        ("impairment", 5): [(SequelaInputContainer, 5)],
        ("impairment", 6): [(SequelaInputContainer, 6)]
    }
    result_strategy_map = {
        "sequela": SequelaResultContainer,
        "cause": CauseResultContainer,
        "injuries": InjuryResultContainer,
        "impairment": ImpairmentResultContainer,
    }
    sim_combos = [
        ("sequela", 3), ("injuries", 3), ("cause", 3), ("impairment", 3),
        ("cause", 5)]

    def __init__(
            self, como_version, location_id=[], year_id=[], age_group_id=[],
            sex_id=[], measure_id=[]):
        # configuration attributes
        self.cv = como_version
        dimensions = self.cv.dimensions
        if location_id:
            dimensions.index_dim.replace_level("location_id", location_id)
        if year_id:
            dimensions.index_dim.replace_level("year_id", year_id)
        if age_group_id:
            dimensions.index_dim.replace_level("age_group_id", age_group_id)
        if sex_id:
            dimensions.index_dim.replace_level("sex_id", sex_id)
        if measure_id:
            dimensions.index_dim.replace_level("measure_id", measure_id)
        self.dimensions = dimensions
        self.components = self.cv.components

        # computed attributes
        self.input_container_cache = {}
        self.result_container_cache = {}
        self.como_sim = None

    def _get_input_strategy(self, component, measure_id, method_type):
        method_list = []
        for strategy in self.inputs_strategy_map[(component, measure_id)]:
            ContainerClass = strategy[0]
            io_measure_id = strategy[1]

            try:
                container = self.input_container_cache[ContainerClass]
            except KeyError:
                container = ContainerClass(self.cv, self.dimensions)
                self.input_container_cache[ContainerClass] = container

            method_list.extend(
                container.get_io_methods(io_measure_id, method_type))

        return method_list

    def import_data(self, n_processes):
        # figure out which methods we need to run to get input data
        reader_methods = []
        setter_methods = []
        getter_methods = []
        for comp in self.components:
            for measure_id in self.dimensions.index_dim.levels.measure_id:
                for meth in self._get_input_strategy(comp, measure_id, "read"):
                    if meth not in reader_methods:
                        reader_methods.append(meth)
                for meth in self._get_input_strategy(comp, measure_id, "set"):
                    if meth not in setter_methods:
                        setter_methods.append(meth)
                for meth in self._get_input_strategy(comp, measure_id, "get"):
                    if meth not in getter_methods:
                        getter_methods.append(meth)

        # set up for reading
        me_reader = ModelableEntityInputWalker()
        for reader_method in reader_methods:
            me_reader = reader_method(me_reader)

        # read the data
        me_reader.read_all_inputs(n_processes)

        # set the data back on the containers
        for setter_method in setter_methods:
            setter_method(me_reader)

        # get remaining data
        for getter_method in getter_methods:
            getter_method()

    def simulate(self, n_simulants, n_processes, *args, **kwargs):

        # get the disability weights
        dws_inputs = DisabilityWeightInputs(self.cv, self.dimensions)
        dws_inputs.get_dws()
        dws_inputs.get_id_dws()

        # get the input containers
        sequela_container = self.input_container_cache[SequelaInputContainer]
        injury_container = self.input_container_cache[InjuryInputContainer]

        # run the simulation
        self.como_sim = ComoSimulator(
            self.cv, self.dimensions, sequela_container, injury_container,
            dws_inputs)
        self.como_sim.create_simulations(n_simulants=n_simulants,
                                         *args, **kwargs)
        self.como_sim.run_all_simulations(n_processes=n_processes)

    def compute_results(self, n_simulants, n_processes, *args, **kwargs):

        if self.como_sim is None:
            # maybe run the simulation

            run_sim = False
            all_combos = product(
                self.cv.components,
                self.cv.dimensions.index_dim.levels.measure_id)
            for result in all_combos:
                if result in self.sim_combos:
                    run_sim = True
                    break
            if run_sim:
                self.simulate(n_simulants=n_simulants, n_processes=n_processes,
                              *args, **kwargs)

        # set up our result computers
        if "sequela" in self.components:
            ResultClass = self.result_strategy_map["sequela"]
            try:
                sequela_result = self.result_container_cache[ResultClass]
            except KeyError:
                sequela_result = ResultClass(
                    como_version=self.cv,
                    dimensions=self.dimensions,
                    sequela_inputs=self.input_container_cache[
                        SequelaInputContainer],
                    como_sim=self.como_sim)
            self.result_container_cache[ResultClass] = sequela_result

        if "cause" in self.components:
            ResultClass = self.result_strategy_map["cause"]
            try:
                cause_result = self.result_container_cache[ResultClass]
            except KeyError:
                cause_result = ResultClass(
                    como_version=self.cv,
                    dimensions=self.dimensions,
                    sequela_inputs=self.input_container_cache[
                        SequelaInputContainer],
                    injury_inputs=self.input_container_cache[
                        InjuryInputContainer],
                    como_sim=self.como_sim)
            self.result_container_cache[ResultClass] = cause_result

        if "impairment" in self.components:
            ResultClass = self.result_strategy_map["impairment"]
            try:
                imp_result = self.result_container_cache[ResultClass]
            except KeyError:
                imp_result = ResultClass(
                    como_version=self.cv,
                    dimensions=self.dimensions,
                    sequela_inputs=self.input_container_cache[
                        SequelaInputContainer],
                    como_sim=self.como_sim)
            self.result_container_cache[ResultClass] = imp_result

        if "injuries" in self.components:
            ResultClass = self.result_strategy_map["injuries"]
            try:
                imp_result = self.result_container_cache[ResultClass]
            except KeyError:
                imp_result = ResultClass(
                    como_version=self.cv,
                    dimensions=self.dimensions,
                    injury_inputs=self.input_container_cache[
                        InjuryInputContainer],
                    como_sim=self.como_sim)
            self.result_container_cache[ResultClass] = imp_result

        # compute results
        for component in self.components:
            ResultClass = self.result_strategy_map[component]
            container = self.result_container_cache[ResultClass]
            for measure_id in self.dimensions.index_dim.levels.measure_id:
                compute_methods = container.get_compute_methods(measure_id)
                for compute_method in compute_methods:
                    compute_method()

    def write_results(self):
        for component in self.components:
            ResultClass = self.result_strategy_map[component]
            result_container = self.result_container_cache[ResultClass]
            for measure_id in self.dimensions.index_dim.levels.measure_id:
                data_attr = result_container.get_result_attr(measure_id)
                if data_attr is not None:
                    result_container.write_result_draws(data_attr, measure_id)
