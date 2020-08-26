import json
import os

import dalynator
from db_tools.ezfuncs import query
from gbd.constants import gbd_process, gbd_metadata_type, \
        gbd_process_version_status
from gbd_outputs_versions.compare_version import CompareVersion
from gbd_outputs_versions.db import set_engine, DBEnvironment as DBEnv
from gbd_outputs_versions.gbd_process import GBDProcessVersion

MD_PROC_MAP = {
    gbd_metadata_type.COMO: gbd_process.EPI,
    gbd_metadata_type.CODCORRECT: gbd_process.COD,
    gbd_metadata_type.FAUXCORRECT: gbd_process.FAUXCORRECT
}


class VersionManager(object):
    """
    Researchers, analysts and managers use a versioning system in
    coloquial discussion of the various GBD tools (e.g. Burdenator,
    CodCorrect, COMO) that is different from the versioning system used
    by the 'gbd' database. Reasons for this are historical and rooted in
    differing needs across the research, viz and database teams.

    Regardless of the reasons, the translation between these systems is
    roughly (research speak -> db/viz speak):

        - Research area (e.g. Causes of Death) -> gbd_process
        - Research tool (e.g. CodCorrect) -> metadata_type
        - (Specific run/version of research tool outputs ->
            gbd_process_version_metadata.val)
        - (Outputs from multiple tools in one GBD Compare view ->
            compare_version)
        - (Tables researches expect to be visible in GBD Compare ->
            compare_version_output)

    This class attempts to reduce (or at least consolidate) some of the
    friction between the research and database versioning systems.
    """


    VALID_INPUT_PROCESS_IDS = [
        gbd_process.COD,
        gbd_process.EPI,
        gbd_process.FAUXCORRECT
    ]
    VALID_OUTPUT_PROCESS_IDS = [
        gbd_process.ETIOLOGY,
        gbd_process.RISK,
        gbd_process.SUMMARY,
    ]
    VALID_METADATA_TYPE_IDS = [
        gbd_metadata_type.BURDENATOR,
        gbd_metadata_type.CODCORRECT,
        gbd_metadata_type.FAUXCORRECT,
        gbd_metadata_type.COMO,
        gbd_metadata_type.DALYNATOR,
        gbd_metadata_type.POPULATION,
        gbd_metadata_type.RISK,
    ]

    def __init__(self, gbd_round_id, decomp_step, upload_to_test=True,
                 read_from_prod=False):

        self.gbd_round_id = gbd_round_id
        self.decomp_step = decomp_step
        self.read_from_prod = read_from_prod

        if upload_to_test:
            self.upload_env = DBEnv.DEV
        else:
            self.upload_env = DBEnv.PROD
        if read_from_prod:
            self.validate_env = DBEnv.PROD
        else:
            self.validate_env = self.upload_env
        self._upload_eng = set_engine(self.upload_env)
        self._validate_eng = set_engine(self.validate_env)

        self.code_version = dalynator.__version__
        self.compare_version_id = None
        self.input_process_versions = {}
        self.output_process_versions = {}
        self.metadata = {}

        self._frozen = False
        self._versions_file = None

    @classmethod
    def from_file(cls, versions_file):
        """Create a VersionManager from a previously frozen versions_file

        Args:
            versions_file (str): File where process version info and metadata
                from a previously frozen VersionManager are written

        Returns:
            a VersionManager
        """
        versions_file = os.path.abspath(os.path.expanduser(versions_file))
        with open(versions_file, 'r') as fp:
            versions = json.load(fp)
        gbd_round_id = versions['gbd_round_id']
        decomp_step = versions['decomp_step']
        dbenv = versions['dbenv']
        validate_env = versions['validate_env']
        if dbenv == DBEnv.DEV.value:
            upload_to_test = True
        elif dbenv == DBEnv.PROD.value:
            upload_to_test = False
        else:
            raise ValueError("Unrecognized DBEnv {}".format(dbenv))
        if validate_env == DBEnv.DEV.value:
            read_from_prod = False
        elif validate_env == DBEnv.PROD.value:
            read_from_prod = True
        else:
            raise ValueError("Unrecognized DBEnv for validate_env {}".format(
                validate_env))

        vm = VersionManager(gbd_round_id, decomp_step,
                            upload_to_test=upload_to_test,
                            read_from_prod=read_from_prod)
        vm.code_version = versions['code_version']
        vm.compare_version_id = versions['compare_version_id']
        vm.input_process_versions = {
            int(k): v for k, v in versions['in'].items()}
        vm.output_process_versions = {
            int(k): v for k, v in versions['out'].items()}
        vm.metadata = {
            int(k): v for k, v in versions['metadata'].items()}
        vm._frozen = True
        vm._versions_file = versions_file
        return vm

    def activate_compare_version(self):
        """Set the compare version to Active. Should only be done after results
        are finished uploading"""
        cv = CompareVersion(self.compare_version_id, env=self.upload_env)
        cv._update_status(gbd_process_version_status['ACTIVE'])

    def activate_process_version(self, process_version_id):
        """Set the process version to Active. Should only be done after results
        are finished uploading"""
        pv = GBDProcessVersion(process_version_id, env=self.upload_env)
        pv._update_status(gbd_process_version_status['ACTIVE'])

    def deactivate_compare_version(self):
        """Set the compare version to Delete. Should only be done after run
        has failed"""
        cv = CompareVersion(self.compare_version_id, env=self.upload_env)
        cv._update_status(gbd_process_version_status['DELETED'])

    def deactivate_process_version(self, process_version_id):
        """Set the process version to Delete. Should only be done after
        run has failed"""
        pv = GBDProcessVersion(process_version_id, env=self.upload_env)
        pv.deactivate_process_version()

    def freeze(self, versions_file):
        """Create new process version(s) and compare version in the database,
        and write matching information to the filesystem (for future
        loading and comparison). Once the version information is 'frozen'
        against a specific versions_file, it should not be modifiable.

        Args:
            versions_file (str): File where process version info and
                input/output metadata should be written
        """
        versions_file = os.path.abspath(os.path.expanduser(versions_file))
        if os.path.exists(versions_file):
            raise RuntimeError("Versions file '{}' already exists. If you "
                               "want to manage new versions, you'll need to "
                               "delete this file (and manage the database as "
                               "appropriate)".format(versions_file))
        if self._frozen:
            raise RuntimeError("This VersionManager is already frozen, either "
                               "because it has been loaded from a file or "
                               "freeze() has already been called")
        else:
            self._frozen = True
            self._versions_file = versions_file
            if gbd_metadata_type.BURDENATOR in self.metadata:
                pvid = self._create_rf_process_version()
                self.output_process_versions[gbd_process.RISK] = pvid
                pvid = self._create_eti_process_version()
                self.output_process_versions[gbd_process.ETIOLOGY] = pvid
            elif gbd_metadata_type.DALYNATOR in self.metadata:
                pvid = self._create_daly_process_version()
                self.output_process_versions[gbd_process.SUMMARY] = pvid
            else:
                raise ValueError("No burdenator or dalynator metadata "
                                 "versions are set")

            if not self.read_from_prod:
                self._create_compare_version()
            with open(versions_file, 'w') as fp:
                content = {'code_version': self.code_version,
                           'compare_version_id': self.compare_version_id,
                           'in': self.input_process_versions,
                           'out': self.output_process_versions,
                           'metadata': self.metadata,
                           'gbd_round_id': self.gbd_round_id,
                           'decomp_step': self.decomp_step,
                           'dbenv': self.upload_env.value,
                           'validate_env': self.validate_env.value}
                json.dump(content, fp)

    def get_input_process_version_id(self, gbd_process_id):
        """Get the version ID for a specific input process (e.g. COD)"""
        return self.input_process_versions[gbd_process_id]

    def get_output_process_version_id(self, gbd_process_id):
        """Get the version ID for a specific output process (e.g. RISK)"""
        return self.output_process_versions[gbd_process_id]

    def set_tool_run_version(self, metadata_type_id, value):
        """
        Sets the run number (version) of either an input tool (CodCorrect,
        COMO) or of the tool being run (Burdenator or Dalynator). Behind the
        scenes, does the work to match this up with a gbd_process_version_id
        so the database records can be created appropriately.

        Args:
            metadata_type_id (int): the id of the tool as enumerated in
                gbd.constants.gbd_metadata_type
            value (int): The run number (version) of the tool, in the same
                context as passed via the Burdenator CLI. (NOTE: this is not
                the gbd_process_version_id)
        """
        self.validate_metadata(metadata_type_id, value)
        self.metadata[metadata_type_id] = value

        if metadata_type_id == gbd_metadata_type.CODCORRECT:
            pvid = self._get_pv_from_metadata(metadata_type_id, value)
            self._set_input_process_version_id(gbd_process.COD, pvid)
        if metadata_type_id == gbd_metadata_type.COMO:
            pvid = self._get_pv_from_metadata(metadata_type_id, value)
            self._set_input_process_version_id(gbd_process.EPI, pvid)

    def validate_metadata(self, metadata_type_id, value):

        if metadata_type_id not in VersionManager.VALID_METADATA_TYPE_IDS:
            raise RuntimeError("{} is not a valid *nator "
                               "metadata_type_id".format(metadata_type_id))
        if self._frozen and metadata_type_id not in self.metadata:
            raise ValueError("Metadata is already frozen, cannot add values")
        if self._frozen and metadata_type_id in self.metadata:
            current_value = self.metadata[metadata_type_id]
            if current_value != value:
                raise ValueError("Metadata is frozen and current value "
                                 "({current}) does not match the argument "
                                 "({new})".format(current=current_value,
                                                  new=value))
        return True

    def _create_compare_version(self):
        """Should record all process_versions created in the run, plus the
        CodCorrect and COMO process versions used as inputs"""
        description = self._generate_description()
        cv = CompareVersion.add_new_version(
            gbd_round_id=self.gbd_round_id,
            decomp_step=self.decomp_step,
            compare_version_description=description,
            env=self.upload_env)

        # Get this list of input process versions, including the burdenator
        # proc version
        process_versions = (list(self.input_process_versions.values()) +
                            list(self.output_process_versions.values()))
        cv.add_process_version(process_versions)

        self.compare_version_id = cv.compare_version_id
        return cv.compare_version_id

    def _create_daly_process_version(self):
        gbd_process_id = gbd_process.SUMMARY
        return self._create_gbd_process_version(gbd_process_id)

    def _create_eti_process_version(self):
        gbd_process_id = gbd_process.ETIOLOGY
        return self._create_gbd_process_version(gbd_process_id)

    def _create_gbd_process_version(self, gbd_process_id):
        version_note = self._generate_description()
        pv = GBDProcessVersion.add_new_version(
            gbd_process_id=gbd_process_id,
            gbd_process_version_note=version_note,
            code_version=self.code_version,
            gbd_round_id=self.gbd_round_id,
            decomp_step=self.decomp_step,
            metadata=self.metadata,
            env=self.upload_env,
            validate_env=self.validate_env)
        return pv.gbd_process_version_id

    def _create_rf_process_version(self):
        gbd_process_id = gbd_process.RISK
        return self._create_gbd_process_version(gbd_process_id)

    def _generate_description(self):
        if gbd_metadata_type.DALYNATOR in self.metadata:
            v = self.metadata[gbd_metadata_type.DALYNATOR]
            description = "Dalynator v{}".format(v)
        elif gbd_metadata_type.BURDENATOR in self.metadata:
            v = self.metadata[gbd_metadata_type.BURDENATOR]
            description = "Burdenator v{}".format(v)
        else:
            raise KeyError("Either burdenator or dalynator version must be "
                           "set")

        if gbd_metadata_type.CODCORRECT in self.metadata:
            v = self.metadata[gbd_metadata_type.CODCORRECT]
            description = "{}, CodCorrect: {}".format(description, v)
        if gbd_metadata_type.FAUXCORRECT in self.metadata:
            v = self.metadata[gbd_metadata_type.FAUXCORRECT]
            description = "{}, FauxCorrect: {}".format(description, v)
        if gbd_metadata_type.COMO in self.metadata:
            v = self.metadata[gbd_metadata_type.COMO]
            description = "{}, COMO: {}".format(description, v)
        if gbd_metadata_type.POPULATION in self.metadata:
            v = self.metadata[gbd_metadata_type.POPULATION]
            description = "{}, Pop: {}".format(description, v)
        if gbd_metadata_type.RISK in self.metadata:
            v = self.metadata[gbd_metadata_type.RISK]
            description = "{}, PAFs: {}".format(description, v)

        return description

    def _get_pv_from_metadata(self, metadata_type_id, metadata_value):
        if metadata_type_id not in MD_PROC_MAP:
            raise ValueError("Process version lookup not supppored for "
                             "metadata_type_id {}".format(metadata_type_id))

        gbd_process_id = MD_PROC_MAP[metadata_type_id]
        q = """
            SELECT gbd_process_version_id
            FROM gbd.gbd_process_version_metadata
            JOIN gbd.gbd_process_version USING(gbd_process_version_id)
            WHERE gbd_process_id={gpi}
            AND metadata_type_id={mti}
            AND val={v}
            AND gbd_process_version_status_id IN (1, 2)
        """.format(gpi=gbd_process_id, mti=metadata_type_id, v=metadata_value)
        res = query(q, self._validate_eng)
        return max(res.gbd_process_version_id)

    def _set_input_process_version_id(self, gbd_process_id, pvid):
        if self._frozen:
            raise RuntimeError("Process versions for this *nator run are "
                               "frozen. If you need to modify them, delete "
                               "the gbd_processes.json file in the root "
                               "directory and manage the DB tables as you "
                               "see fit.")
        if gbd_process_id not in VersionManager.VALID_INPUT_PROCESS_IDS:
            raise RuntimeError("{} is not a valid *nator input "
                               "gbd_process_id".format(gbd_process_id))
        self.input_process_versions[gbd_process_id] = int(pvid)

    def _set_output_process_version_id(self, gbd_process_id):
        if self._frozen:
            raise RuntimeError("Process versions for this *nator run are "
                               "frozen. If you need to modify them, delete "
                               "the gbd_processes.json file in the root "
                               "directory and manage the DB tables as you "
                               "see fit.")
        if gbd_process_id not in VersionManager.VALID_OUTPUT_PROCESS_IDS:
            raise RuntimeError("{} is not a valid *nator output "
                               "gbd_process_id".format(gbd_process_id))

        if gbd_process_id == gbd_process.RISK:
            new_pvid = self._create_rf_process_version()
        elif gbd_process_id == gbd_process.ETIOLOGY:
            new_pvid = self._create_eti_process_version()
        elif gbd_process_id == gbd_process.SUMMARY:
            new_pvid = self._create_daly_process_version()
        else:
            raise ValueError(
                "GBD process {} is not a valid process for either the "
                "Dalynator or Burdenator".format(gbd_process_id))
        new_pvid = int(new_pvid)
        self.output_process_versions[gbd_process_id] = new_pvid
        return new_pvid

    def __eq__(self, other):
        attrs_to_match = [
            "gbd_round_id", "code_version", "compare_version_id",
            "input_process_versions", "output_process_versions", "metadata",
            "_frozen"]
        conditions = []

        for attr in attrs_to_match:
            match = getattr(self, attr) == getattr(other, attr)
            conditions.append(match)

        return all(conditions)
