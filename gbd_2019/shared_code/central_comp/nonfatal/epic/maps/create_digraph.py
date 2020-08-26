import argparse
import copy
import json
import pandas as pd
import numpy as np
import os
import time
from typing import Dict, List, Tuple

from collections import deque
from dateutil import parser
import networkx as nx

from networkx.drawing.nx_agraph import to_agraph, pygraphviz_layout
from networkx.readwrite import json_graph
import matplotlib.pyplot as plt

from db_queries import get_best_model_versions
from db_tools import ezfuncs
from epic.util.constants import Params
import gbd.constants as gbd
from gbd.decomp_step import (
    decomp_step_from_decomp_step_id,
    decomp_step_id_from_decomp_step
)
from rules.enums import ResearchAreas, Tools
from save_results._validation import _validate_decomp_step


class CombineMaps(object):

    def __init__(
        self,
        decomp_step: str = None,
        gbd_round_id: int = gbd.GBD_ROUND_ID,
        include_como: bool = False):

        _validate_decomp_step(
            decomp_step,
            gbd_round_id,
            ResearchAreas.EPI,
            Tools.SAVE_RESULTS,
            mark_best=True
        )

        self.include_como = include_como
        self._loaded_maps = {}
        self.all_processes = {}
        self.decomp_step = decomp_step
        self.gbd_round_id = gbd_round_id
        self.decomp_step_id = decomp_step_id_from_decomp_step(
            step=self.decomp_step,
            gbd_round_id=self.gbd_round_id
        )
        self.G = nx.DiGraph() # meid graph
        self.P = nx.DiGraph() # process graph
        self.start_node = Params.EPIC_START_NODE
        self.this_file = os.path.realpath(__file__)
        self.this_dir = os.path.dirname(self.this_file)
        self.load_maps(self.this_dir)
        self.build_graphs(self.this_dir)
        self._best_models = None
        self._inputs = None
        self._outputs = None

    def load_json_map(self, path: str) -> Dict[str, str]:
        with open(path) as json_file:
            process_dict = json.load(json_file, parse_int=True)
        self._loaded_maps[path] = process_dict
        return process_dict

    def add_process_dict(self, process_dict) -> None:
        process_keys = set(process_dict.keys())
        for process_map in self._loaded_maps.keys():
            claimed_keys = set(self._loaded_maps[process_map].keys())
            if process_keys in claimed_keys:
                raise Exception(
                    "cannot add process map because some process names are"
                    " already in use. {} contains: {}".format(
                        process_map, claimed_keys.intersection(process_keys)))
        self.all_processes.update(process_dict)

    @property
    def best_models(self) -> pd.DataFrame:
        if self._best_models is None:
            self._best_models = self.get_best_models()
        return self._best_models

    @property
    def inputs(self) -> List[str]:
        if self._inputs is None:
            self._inputs = self.get_inputs()
        return self._inputs

    @property
    def outputs(self) -> List[str]:
        if self._outputs is None:
            self._outputs = self.get_inputs()
        return self._outputs

    def get_best_models(self) -> pd.DataFrame:
        mvid_list = ezfuncs.query(
            """
            SELECT
                cause.cause_id, mv.modelable_entity_id,
                mv.model_version_id, mv.decomp_step_id, mv.best_start
            FROM epi.model_version mv
            LEFT JOIN epi.modelable_entity_cause cause
                ON mv.modelable_entity_id = cause.modelable_entity_id
            WHERE
                mv.model_version_status_id = :best
                and mv.gbd_round_id = :gbd_round_id
                and mv.decomp_step_id IN (:decomp_step_id, :iterative)
            """,
            conn_def="epi",
            parameters={
                "best": 1,
                "gbd_round_id": self.gbd_round_id,
                "decomp_step_id": self.decomp_step_id,
                "iterative": decomp_step_id_from_decomp_step(
                    step=gbd.decomp_step.ITERATIVE,
                    gbd_round_id=self.gbd_round_id
                )
            }
        )

        # Determine what version (decomp/iterative) to use:
        all_ntd_cause = [346, 347, 348, 349, 350, 351, 352, 353, 354, 355, 356,
                         357, 358, 359, 360, 361, 362, 363, 364, 365, 405, 843,
                         935, 936]
        ntd_decomp_me = [1500, 1503, 10402, 1513, 1514, 1515, 2999, 3109,
                         20265, 1516, 1517, 1518, 3001, 3110, 20266, 1519,
                         1520, 1521, 3000, 3139, 3111, 20009, 2797, 1474,
                         1469, 2965, 1475, 10524, 10525, 1476, 2966, 1470,
                         1471, 10480, 1477, 10537, 1472, 1468, 1466, 1473,
                         1465, 16393, 1478]
        new_gbd_2019_cause = [1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011,
                              1012, 1013, 1014, 1015, 1016, 1017]
        has_decomp_version = mvid_list.loc[
            mvid_list.decomp_step_id == self.decomp_step_id,
            ].modelable_entity_id.unique().tolist()
        use_iterative = mvid_list.loc[
            ~mvid_list.modelable_entity_id.isin(ntd_decomp_me)
            & mvid_list.cause_id.isin(all_ntd_cause + new_gbd_2019_cause)
            & ~mvid_list.modelable_entity_id.isin(has_decomp_version)
            ].modelable_entity_id.unique().tolist()
        mvid_list = mvid_list.loc[
            (mvid_list.decomp_step_id == self.decomp_step_id) |
            (mvid_list.modelable_entity_id.isin(use_iterative))]
        mvid_list["decomp_step"] = mvid_list["decomp_step_id"].apply(
            decomp_step_from_decomp_step_id)
        return mvid_list

    def get_inputs(self) -> List[str]:
        # figure out hook
        outputs = []
        inputs = []
        for k in self.all_processes.keys():
            if not self.include_como and k == "como":
                # reject all the remaining statements in the current
                # iteration of the loop and move control back to the top of
                # the loop, i.e. do not add como inputs and outputs to the lists
                continue
            outputs = outputs + list(self.all_processes[k]["out"].keys())
            inputs = inputs + list(self.all_processes[k]["in"].keys())

        seto = set(outputs)
        seti = set(inputs)
        return list(seti - seto)

    def get_outputs(self) -> List[str]:
        # figure out hook
        outputs = []
        inputs = []
        for k in self.all_processes.keys():
            if not self.include_como and k == "como":
                # reject all the remaining statements in the current
                # iteration of the loop and move control back to the top of
                # the loop, i.e. do not add como inputs and outputs to the lists
                continue
            outputs = outputs + list(self.all_processes[k]["out"].keys())
            inputs = inputs + list(self.all_processes[k]["in"].keys())

        seto = set(outputs)
        seti = set(inputs)
        return list(seto - seti)

    def unix_time(self, row: pd.DataFrame) -> int:
        dt = row.best_start
        unixtime = time.mktime(dt.timetuple())
        return unixtime

    def get_best_start(self, meid_list, best_models):
        """best_start times are returned from the database as pandas
        Timestamps. Convert Timestamps to  time in seconds from unix epoch
        (https://en.wikipedia.org/wiki/Unix_time) and return a df of
        best_start unix epoch times for given modelable_entity_id list.
        """
        df = best_models.loc[
            best_models.modelable_entity_id.isin(meid_list)
        ].copy()

        if not df.empty:
            # convert 'Timestamp' types to second floats
            df.loc[:,"best_start"] = df.apply(self.unix_time, axis=1)
        # make square in case nothing is returned for a given meid
        df = pd.merge(pd.DataFrame({"modelable_entity_id": meid_list}),
            df, how='left', on="modelable_entity_id")

        # fill NaNs/NaTs with 0
        df = df.fillna(0)

        return df

    def gen_meid_graph_from_inputs(self) -> None:
        """
        Creates a networkx Digraph that connects input
        modelable_entity_ids to their output modelable_entity_ids for all
        EPIC processes.
        """

        downstream_dict = {}
        downstream_dict = copy.deepcopy(self.all_processes)
        if not self.include_como:
            downstream_dict.pop("como", None)

        search_q = []
        for key in downstream_dict.keys():
            search_q.extend(list(downstream_dict[key]["in"].keys()))
        search_q = list(set(search_q))

        while search_q:
            search_key = search_q.pop()
            if not self.G.has_node(search_key):
                self.G.add_node(search_key)
            for key in downstream_dict.keys():
                if search_key in list(downstream_dict[key]["in"].keys()):
                    outs = list(
                        self.all_processes[key]["out"].keys())
                    edge_data_dict = {"process": key}
                    edges = list(zip([search_key]*len(outs),outs,
                        [edge_data_dict]*len(outs)))
                    # Adding the same edge twice has no effect but any edge
                    # data will be updated when each duplicate edge is added.
                    self.G.add_edges_from(edges)
                    keeps= [x for x in outs if x not in search_q]
                    search_q.extend(keeps)
        self.add_input_hook()

    def add_input_hook(self) -> None:
        """
        Add an input hook to all meid nodes with in_degree==0.
        """
        input_nodes = self.inputs
        self.G.add_node(self.start_node)
        edges = list(zip([self.start_node]*len(input_nodes), input_nodes))
        self.G.add_edges_from(edges)

    def gen_process_graph(self, source=None):
        """
        Creates a networkx DiGraph that connects all EPIC processes
        in the order they should be run.

        if any of the outputs from one process are inputs to another process,
        the two are connected with the outputs process being u in graph
        terminology and the inputs process being v.
        """

        downstream_dict = {}
        downstream_dict = copy.deepcopy(self.all_processes)
        downstream_dict.pop("como", None)

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
        add_hook = [n for n in self.P.nodes() if self.P.in_degree(n)==0]
        self.P.add_node(self.start_node)
        edges = list(zip([self.start_node]*len(add_hook), add_hook))
        self.P.add_edges_from(edges)

    def compare_best_start(self):
        """
        Compare best_start times for in and out attributes of each node
        neighboring the input hook (start_node).

        For a given node, if any of the 'in' attributes have greater best_start
        times than any of the 'out' attributes, that node and its descendants
        are added to the return list.

        The return list is used to create a subgraph that dictates what tasks
        are added to the jobmon workflow.
        """

        best_models = self.best_models
        process_list = []
        one_day_in_seconds = 86400
        for n in nx.topological_sort(self.P):
            if n == self.start_node:
                pass
            else:
                # get best_start times
                ins = self.P.nodes[n]["ins"]
                outs = self.P.nodes[n]["outs"]
                df = self.get_best_start(ins+outs, best_models)

                # separate into in meids and out meids
                idf = df.loc[df.modelable_entity_id.isin(ins)]
                odf = df.loc[df.modelable_entity_id.isin(outs)]

                # compare
                is_new = []
                for i in idf.best_start:
                    for o in odf.best_start:
                        # input and outputs that have been set to 0
                        # because get_best_model_versions returned no
                        # information will not be added to the list
                        # of is_new items
                        is_new.append(i - o >= one_day_in_seconds)
                if np.array(is_new).any():
                    process_list.extend([n]+list(nx.descendants(self.P,n)))

        return list(set(process_list))

    def export(self, G, graph_desc):
        here = os.path.dirname(os.path.realpath(__file__))
        d3_data = json_graph.adjacency_data(G)
        with open(os.path.join(here, f"{graph_desc}.json"), "w") as outfile:
            json.dump(d3_data, outfile, sort_keys=True, indent=2)

    def draw_graph(self, graph):
        plt.title('draw_networkx')
        pos=pygraphviz_layout(graph, prog='dot', args='-Gnodesep=1')
        nx.draw(graph, pos, node_size=30, with_labels=False, arrows=True)
        plt.savefig('nx_test.png')
        plt.close()

    def save_graph(self, graph, file_name):
        plt.title('draw_networkx')
        pos=pygraphviz_layout(graph, prog='dot')
        newpos = {}
        for k,v in pos.items():
            newpos[k] = (k,v[0],v[1])
        sorted_by_x = sorted(
            list(newpos.values()), key=lambda tup: (tup[2],tup[1]))
        rank = []
        nodesep = 1000
        for k, xx, yy in sorted_by_x:
            if yy not in rank:
                rank.append(yy)
                newpos[k] = (xx, yy)
                newxx = xx + nodesep
            else:
                newpos[k] = (newxx, yy)
                newxx += nodesep
        nx.draw(graph, newpos, node_size=30, with_labels=False, arrows=True)
        plt.savefig(file_name)
        plt.close()

    def downstream_only(self, identity):
        downstream_dict = {}
        downstream_dict[identity] = self.all_processes[identity]

        search_q = [list(downstream_dict[identity]["in"].keys())]
        while search_q:
            search_key = search_q.pop()
            for key in self.all_processes.keys():
                if search_key in list(self.all_processes[key]["out"].keys()):
                    search_q = list(
                        set(search_q + list(self.all_processes[key]["out"].keys())))
                downstream_dict[key] = self.all_processes[key]
        return downstream_dict

    def load_maps(self, this_dir):
        print("Loading maps")
        jmap = self.load_json_map(
            os.path.join(this_dir, "json", "exclusivity.json"))
        self.add_process_dict(jmap)
        jmap = self.load_json_map(
            os.path.join(this_dir, "json", "severity_splits.json"))
        self.add_process_dict(jmap)
        jmap = self.load_json_map(
            os.path.join(this_dir, "json", "super_squeeze.json"))
        self.add_process_dict(jmap)
        jmap = self.load_json_map(
            os.path.join(this_dir, "json", "como.json"))
        self.add_process_dict(jmap)

    def build_graphs(self, this_dir):
        print("Building process graph")
        self.gen_process_graph()
        final = self.downstream_only("como")
        with open(os.path.join(this_dir, "final.json"), "w") as outfile:
            json.dump(final, outfile, sort_keys=True, indent=2)
