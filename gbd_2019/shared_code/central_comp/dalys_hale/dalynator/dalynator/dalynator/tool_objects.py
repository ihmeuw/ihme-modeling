import os
import json

import gbd.constants as gbd

import dalynator.type_checking as tp


class ToolObject(object):

    _info_file = "directory_info.json"
    _directory_info_hardcode = {}
    _tool_name = "base"

    """Data structure for centralizing information about input data.
    Given a root_dir and a version_id, attempts to ingest a file
    /root_dir/version_id/_info_file (a json file) and uses the content to return
    the location and structure of draw files and summaries.  If the
    _info file does not exist, falls back on the dictionary
    _directory_info_hardcode for the given child class."""

    def __init__(self, root_dir, version_id=None):
        self.root_dir = root_dir
        self.version_id = version_id
        if version_id:
            self.directory_info = self.retrieve_directory_info()
            self.draw_dir = self.directory_info["draw_dir"]
            self.file_pattern = self.directory_info["file_pattern"]
            self.abs_path_to_draws = os.path.join(
                self.root_dir, self._tool_name,
                str(self.version_id), self.draw_dir)
            if "summaries_dir" in self.directory_info:
                self.summaries_dir = self.directory_info["summaries_dir"]
                self.abs_path_to_summaries = os.path.join(
                    self.root_dir, self._tool_name,
                    str(self.version_id), self.summaries_dir)

    @classmethod
    def tool_object_by_name(cls, tool_name):
        """Given a tool name, returns the specific child class
        where cls.tool_name == tool_name"""
        tool_dict = {tool._tool_name: tool for tool in cls.__subclasses__()}
        if tool_name in tool_dict:
            return tool_dict[tool_name]
        else:
            raise ValueError(
                "{} does not correspond to any ToolObject subclass.")

    def retrieve_directory_info(self):
        directory_info = os.path.join(
            self.root_dir, self._tool_name,
            str(self.version_id), self._info_file)
        if os.path.isfile(directory_info):
            with open(directory_info) as json_file:
                return json.load(json_file)
        else:
            return self._directory_info_hardcode


class CodCorrectObject(ToolObject):

    _tool_name = 'codcorrect'
    _directory_info_hardcode = {
        "draw_dir": "FILEPATH",
        "file_pattern": "FILEPATH",
        "summaries_dir": "FILEPATH"}
    gbd_process_id = gbd.gbd_process['COD']
    metadata_type_id = gbd.gbd_metadata_type['CODCORRECT']


class FauxCorrectObject(ToolObject):

    _tool_name = 'fauxcorrect'
    _directory_info_hardcode = {
        "draw_dir": "FILEPATH",
        "file_pattern": "FILEPATH",
        "summaries_dir": "FILEPATH"}
    gbd_process_id = gbd.gbd_process['FAUXCORRECT']
    metadata_type_id = gbd.gbd_metadata_type['FAUXCORRECT']


class ComoObject(ToolObject):

    _tool_name = 'como'
    _directory_info_hardcode = {
        "draw_dir": "FILEPATH",
        "file_pattern": "FILEPATH"}
    gbd_process_id = gbd.gbd_process['EPI']
    metadata_type_id = gbd.gbd_metadata_type['COMO']


class PafObject(ToolObject):

    _tool_name = 'pafs'
    _directory_info_hardcode = {
        "draw_dir": "",
        "file_pattern": "FILEPATH"}
    gbd_process_id = gbd.gbd_process['RISK']
    metadata_type_id = gbd.gbd_metadata_type['RISK']


def cod_or_faux_correct(root_dir,
                        codcorrect_version=None,
                        fauxcorrect_version=None):

    if codcorrect_version and not fauxcorrect_version:
        codcorrect_version = tp.is_best_or_positive_int(
            codcorrect_version, "codcorrect version")
        return ToolObject.tool_object_by_name(
            "codcorrect")(root_dir, codcorrect_version)
    elif not codcorrect_version and fauxcorrect_version:
        fauxcorrect_version = tp.is_best_or_positive_int(
            fauxcorrect_version, "fauxcorrect version")
        return ToolObject.tool_object_by_name(
            "fauxcorrect")(root_dir, fauxcorrect_version)
    else:
        raise ValueError("One and only one of 'codcorrect_version' "
                         "or 'fauxcorrect_version' must be not None.")
