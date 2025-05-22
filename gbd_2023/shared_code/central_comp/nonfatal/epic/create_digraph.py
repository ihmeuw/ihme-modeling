import copy
import json
import pathlib
from typing import Dict, List, Optional

import networkx as nx
import pandas as pd

import db_queries
from gbd import constants as gbd_constants

from epic.legacy.util.constants import FilePaths, Params, Process

_THIS_DIR = pathlib.Path(__file__).resolve().parent


class CombineMaps(object):
    def __init__(
        self,
        release_id: int,
        include_como: bool = False,
        include_anemia: bool = False,
        map_dir: str = str(_THIS_DIR.parent.parent / "lib" / "maps")
    ) -> None:

        self.include_como = include_como
        self.include_anemia = include_anemia
        self._loaded_maps = {}
        self.all_processes = {}
        self.release_id = release_id
        self.G = nx.DiGraph()  # meid graph
        self.P = nx.DiGraph()  # process graph
        self.start_node = Params.EPIC_START_NODE
        self.load_maps(map_dir=map_dir)
        self.build_graphs(map_dir=map_dir)
        self._best_models: Optional[pd.DataFrame] = None
        self._inputs: Optional[List[str]] = None
        self._outputs: Optional[List[str]] = None

    def load_json_map(self, path: str) -> Dict[str, str]:
        """Reads in the JSON EpiC map at a passed path as a dictionary."""
        with open(path) as json_file:
            try:
                process_dict = json.load(json_file, parse_int=True)
            except TypeError:
                with open(path) as json_file:
                    process_dict = json.load(json_file)
        self._loaded_maps[path] = process_dict
        return process_dict

    def add_process_dict(self, process_dict) -> None:
        """Adds a process from an EpiC JSON map to a dictionary of all processes."""
        process_keys = set(process_dict.keys())
        for process_map in self._loaded_maps.keys():
            claimed_keys = set(self._loaded_maps[process_map].keys())
            if process_keys in claimed_keys:
                raise Exception(
                    "cannot add process map because some process names are"
                    " already in use. {} contains: {}".format(
                        process_map, claimed_keys.intersection(process_keys)
                    )
                )
        self.all_processes.update(process_dict)

    @property
    def best_models(self) -> pd.DataFrame:
        """A dataframe mapping ME ID to best MVID and release ID."""
        if self._best_models is None:
            self._best_models = self.get_best_models()
        return self._best_models

    @property
    def inputs(self) -> List[str]:
        """A list of all EpiC inputs that aren't also EpiC outputs."""
        if self._inputs is None:
            self._inputs = self.get_inputs()
        return self._inputs

    @property
    def outputs(self) -> List[str]:
        """A list of all EpiC outputs that aren't also EpiC inputs."""
        if self._outputs is None:
            self._outputs = self.get_inputs()
        return self._outputs

    def get_best_models(self) -> pd.DataFrame:
        """Pulls input ME best model versions for a given release."""
        int_inputs = [int(me) for me in self.inputs]
        mvid_list = db_queries.get_best_model_versions(
            entity="modelable_entity", ids=int_inputs, release_id=self.release_id
        )
        mvid_list = mvid_list[
            [
                gbd_constants.columns.MODELABLE_ENTITY_ID,
                gbd_constants.columns.MODEL_VERSION_ID,
                gbd_constants.columns.RELEASE_ID,
                "best_start",
            ]
        ]
        mvid_list = mvid_list.rename(
            columns={gbd_constants.columns.RELEASE_ID: "mapped_release_id"}
        )
        mvid_list[gbd_constants.columns.RELEASE_ID] = self.release_id
        release_df = db_queries.get_ids("release")
        release_dict = release_df.set_index("release_id")["release_name"].to_dict()
        mvid_list["mapped_release_name"] = mvid_list["mapped_release_id"].map(
            release_dict
        )
        return mvid_list

    def get_inputs(self) -> List[str]:
        """Returns all input values from JSON maps that aren't upstream EpiC outputs."""
        # figure out hook
        outputs: List[str] = []
        inputs: List[str] = []
        for k in self.all_processes.keys():
            if not self.include_como and k == Process.COMO:
                # reject all the remaining statements in the current
                # iteration of the loop and move control back to the top of
                # the loop, i.e. do not add como inputs and outputs to the lists
                continue
            outputs = outputs + list(self.all_processes[k]["out"].keys())
            inputs = inputs + list(self.all_processes[k]["in"].keys())

        return list(set(inputs) - set(outputs))

    def get_outputs(self) -> List[str]:
        """Returns all output values from JSON maps that aren't downstream EpiC inputs."""
        # figure out hook
        outputs: List[str] = []
        inputs: List[str] = []
        for k in self.all_processes.keys():
            if not self.include_como and k == Process.COMO:
                # reject all the remaining statements in the current
                # iteration of the loop and move control back to the top of
                # the loop, i.e. do not add como inputs and outputs to the lists
                continue
            outputs += list(self.all_processes[k]["out"].keys())
            inputs += list(self.all_processes[k]["in"].keys())

        return list(set(outputs) - set(inputs))

    def gen_meid_graph_from_inputs(self) -> None:
        """Creates a networkx Digraph that connects input modelable_entity_ids to their output
        modelable_entity_ids for all EPIC processes.
        """
        downstream_dict = copy.deepcopy(self.all_processes)
        if not self.include_como:
            downstream_dict.pop(Process.COMO, None)

        search_q: List[str] = []
        for key in downstream_dict.keys():
            search_q.extend(list(downstream_dict[key]["in"].keys()))
        search_q = list(set(search_q))

        while search_q:
            search_key = search_q.pop()
            if not self.G.has_node(search_key):
                self.G.add_node(search_key)
            for key in downstream_dict.keys():
                if search_key in list(downstream_dict[key]["in"].keys()):
                    outs = list(self.all_processes[key]["out"].keys())
                    edge_data_dict = {"process": key}
                    edges = list(
                        zip(
                            [search_key] * len(outs), outs, [edge_data_dict] * len(outs)
                        )
                    )
                    # Adding the same edge twice has no effect but any edge
                    # data will be updated when each duplicate edge is added.
                    self.G.add_edges_from(edges)
                    keeps = [x for x in outs if x not in search_q]
                    search_q.extend(keeps)
        self.add_input_hook()

    def add_input_hook(self) -> None:
        """Add an input hook to all meid nodes with in_degree==0."""
        self.G.add_node(self.start_node)
        edges = list(zip([self.start_node] * len(self.inputs), self.inputs))
        self.G.add_edges_from(edges)

    def gen_process_graph(self) -> None:
        """
        Creates a networkx DiGraph that connects all EPIC processes in the order they should
        be run.

        If any of the outputs from one process are inputs to another process, the two are
        connected with the outputs process being u in graph terminology and the inputs
        process being v.
        """
        downstream_dict = copy.deepcopy(self.all_processes)
        downstream_dict.pop(Process.COMO, None)

        # Add all processes as nodes in the process DAG. Add in and out meid
        # information as attributes of each node.
        for key in downstream_dict.keys():
            ins = [int(x) for x in list(downstream_dict[key]["in"].keys())]
            outs = [int(x) for x in list(downstream_dict[key]["out"].keys())]
            self.P.add_node(node_for_adding=key, ins=ins, outs=outs)

        # Add directed edges to the process DAG
        for search_key in downstream_dict.keys():
            outs = self.P.nodes[search_key]["outs"]
            for key in downstream_dict.keys():
                if search_key == key:
                    pass
                else:
                    ins = self.P.nodes[key]["ins"]
                    intersect = set(outs).intersection(ins)
                    if intersect and not self.P.has_edge(search_key, key):
                        self.P.add_edge(search_key, key)

        # add input hook to all process nodes with in_degree==0
        add_hook = [n for n in self.P.nodes() if self.P.in_degree(n) == 0]
        self.P.add_node(self.start_node)
        edges = list(zip([self.start_node] * len(add_hook), add_hook))
        self.P.add_edges_from(edges)

    def downstream_only(self, identity):
        """Get's all EpiC processes that feed into a given downstream, typically COMO."""
        downstream_dict = {}
        downstream_dict[identity] = self.all_processes[identity]

        search_q = [list(downstream_dict[identity]["in"].keys())]
        while search_q:
            search_key = search_q.pop()
            for key in self.all_processes.keys():
                if search_key in list(self.all_processes[key]["out"].keys()):
                    search_q = list(
                        set(search_q + list(self.all_processes[key]["out"].keys()))
                    )
                downstream_dict[key] = self.all_processes[key]
        return downstream_dict

    def load_maps(self, map_dir: str) -> None:
        """Loads all relevant EpiC JSON maps from a passed map directory."""
        print("Loading maps")
        print("Loading exclusivity map")
        jmap = self.load_json_map(
            pathlib.Path(map_dir) / "json" / FilePaths.Maps.EXCLUSIVITY
        )
        self.add_process_dict(jmap)
        print("Loading severity splits map")
        jmap = self.load_json_map(
            pathlib.Path(map_dir) / "json" / FilePaths.Maps.SEVERITY_SPLIT
        )
        self.add_process_dict(jmap)
        print("Loading modeled severity splits map")
        jmap = self.load_json_map(
            pathlib.Path(map_dir) / "json" / FilePaths.Maps.MODELED_SPLIT
        )
        self.add_process_dict(jmap)
        print("Loading super squeeze map")
        jmap = self.load_json_map(
            pathlib.Path(map_dir) / "json" / FilePaths.Maps.SUPER_SQUEEZE
        )
        self.add_process_dict(jmap)
        print("Loading COMO map")
        jmap = self.load_json_map(pathlib.Path(map_dir) / "json" / FilePaths.Maps.COMO)
        self.add_process_dict(jmap)
        if self.include_anemia:
            print("Loading anemia causal attribution map")
            jmap = self.load_json_map(
                pathlib.Path(map_dir) / "json" / FilePaths.Maps.CAUSAL_ATTRIBUTION
            )
            self.add_process_dict(jmap)
            print("Loading TMREL map")
            jmap = self.load_json_map(
                pathlib.Path(map_dir) / "json" / FilePaths.Maps.TMREL
            )
            self.add_process_dict(jmap)

    def build_graphs(self, map_dir: str) -> None:
        """Writes the combined maps in a single file to disk."""
        print("Building process graph")
        self.gen_process_graph()
        final = self.downstream_only(Process.COMO)
        with open(pathlib.Path(map_dir) / "json" / "final.json", "w") as outfile:
            json.dump(final, outfile, sort_keys=True, indent=2)
