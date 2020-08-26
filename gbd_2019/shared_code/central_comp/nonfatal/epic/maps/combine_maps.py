import os
import json


class CombineMaps(object):

    def __init__(self):
        self._loaded_maps = {}
        self.all_processes = {}

    def load_json_map(self, path):
        with open(path) as json_file:
            process_dict = json.load(json_file, parse_int=True)
        self._loaded_maps[path] = process_dict
        return process_dict

    def add_process_dict(self, process_dict):
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
    def outputs(self):
        # figure out hook
        outputs = []
        inputs = []
        for k in self.all_processes.keys():
            outputs = outputs + list(self.all_processes[k]["out"].keys())
            inputs = inputs + list(self.all_processes[k]["in"].keys())

        seto = set(outputs)
        seti = set(inputs)
        return list(seto - seti)

    @property
    def inputs(self):
        # figure out hook
        outputs = []
        inputs = []
        for k in self.all_processes.keys():
            outputs = outputs + list(self.all_processes[k]["out"].keys())
            inputs = inputs + list(self.all_processes[k]["in"].keys())

        seto = set(outputs)
        seti = set(inputs)
        return list(seti - seto)

    def _scan_for_artifact_out(self, identity):
        for k in self.all_processes.keys():
            if identity in self.all_processes[k]["out"].keys():
                return self.all_processes[k]["out"][identity]

    def add_process_hook(self):
        self.all_processes["process_hook"] = {}
        self.all_processes["process_hook"]["class"] = "__main__.Hook"
        self.all_processes["process_hook"]["in"] = {}
        self.all_processes["process_hook"]["out"] = {}
        for key in self.outputs:
            self.all_processes["process_hook"]["in"][key] = (
                self._scan_for_artifact_out(key))

    def downstream_only(self, identity):
        downstream_dict = {}
        downstream_dict[identity] = self.all_processes[identity]

        search_q = [downstream_dict[identity]["in"].keys()]
        while search_q:
            search_key = search_q.pop()
            for key in self.all_processes.keys():
                if search_key in self.all_processes[key]["out"].keys():
                    search_q = list(
                        set(search_q + self.all_processes[key]["out"].keys()))
                downstream_dict[key] = self.all_processes[key]
        return downstream_dict

    def upstream_only(self, identity):
        upstream_dict = {}
        upstream_dict[identity] = self.all_processes[identity]

        search_q = upstream_dict[identity]["out"].keys()
        while search_q:
            search_key = search_q.pop()
            for key in self.all_processes.keys():
                if search_key in self.all_processes[key]["in"].keys():
                    search_q = list(
                        set(search_q + self.all_processes[key]["out"].keys()))
                    upstream_dict[key] = self.all_processes[key]
        return upstream_dict


if __name__ == "__main__":

    cm = CombineMaps()
    jmap = cm.load_json_map("./json/exclusivity.json")
    cm.add_process_dict(jmap)
    jmap = cm.load_json_map("./json/severity_splits.json")
    cm.add_process_dict(jmap)
    jmap = cm.load_json_map("./json/super_squeeze.json")
    cm.add_process_dict(jmap)
    jmap = cm.load_json_map("./json/como.json")
    cm.add_process_dict(jmap)

    here = "FILEPATH"
    jmap = cm.downstream_only("como")
    with open(os.path.join(here, "final.json"), "w") as outfile:
        json.dump(jmap, outfile, sort_keys=True, indent=2)
