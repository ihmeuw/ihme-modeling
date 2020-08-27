import itertools
import os
import pandas as pd
import pickle
import time
from typing import Dict, List, Optional, Tuple

import gbd.constants as gbd
from gbd.decomp_step import decomp_step_id_from_decomp_step
from gbd_outputs_versions import gbd_process
import hierarchies.tree

import fauxcorrect
from fauxcorrect.parameters.cause import CauseParameters
from fauxcorrect.parameters.envelope import EnvelopeParameters
from fauxcorrect.parameters.lifetable import LifeTableParameters
from fauxcorrect.parameters.location import LocationParameters
from fauxcorrect.parameters.model_version import ModelVersionParameters
from fauxcorrect.parameters.population import PopulationParameters
from fauxcorrect.queries import codcorrect_version, fauxcorrect_version
from fauxcorrect.utils import constants


class MachineParameters:

    """
    Single object for managing a central deaths machinery run - either
    CoDCorrect or FauxCorrect.

    Contains the run-specific metadata as well as methods that create
    the run's file-system and database assets.

    Properties:
        year_ids (List[int]): the year_ids included in the run. Defaults to
            [1990, 2000, 2015].

        year_start_ids (List[int]): the year_start_ids used to calculate
            pct_change.  1 to 1 correspondance with year_end_ids.
            Defaults to [], no pct change summarization run.

        year_end_ids (List[int]): the year_end_ids used to calculate
            pct_change.  1 to 1 correspondance with year_start_ids.
            Defaults to [], no pct change summarization run.

        sex_ids (List[int]): the sex_ids included in the run. Defaults to
            [1, 2].

        measure_ids (List[int]): the measure_ids included in the run. Should
            always include measure_id 1 (deaths); measure 4 (YLLs) are optional.

        location_set_ids (List[int]): the location_set_ids to include in the
            run. FauxCorrect will only use the standard location set for
            location aggregation.

        gbd_round_id (int): the GBD round id of the run. Defaults to the current
            GBD round id

        decomp_step (str): the decomp step of the run. Defaults to decomp_step
            step1 at this time.

        decomp_step_id (int): the decomp_step_id for the run. Inferred from the
            decomp_step argument.

        process (str): the process we are computing. Determines the cause sets
            and locations sets we'll use for the run. Two options, either
            { 'codcorrect' | 'fauxcorrect' }

        scalar_version_id (int): the source CoDCorrect version for scalars

        most_detailed_cause_ids (List[int]): a list of the most detailed cause
            ids from the cause_set_id.

        cause_ids (List[int]): a list of all cause ids from the cause_set_id.

        cause_rescaling_tree (hierarchies.tree.Tree): the tree representing the
            codcorrect cause hierarchy; used to scale causes to the All Cause
            Mortality Envelope.

        cause_aggregation_tree (hierarchies.tree.Tree): the tree representing
            the reporting cause hierarchy; used to aggregate causes up to the
            global root, all-cause.

        most_detailed_location_ids (List[int]): the list of most detailed
            location ids from the standard location computation set.

        location_ids (List[int]): returns a list of every location_id from all
            the location sets used in this run.

        location_aggregation_tree (hierarchies.tree.Tree): returns the tree
            representation of the standard location hierarchy.

        best_model_version_ids (List[int]): returns a list of all the model
            version ids that were marked best for this run of FauxCorrect.


    Methods:
        new(): creates a new run version. This is used instead of the
            initialization method to construct new versions of the software.

        recreate_from_cache(): reads from a pickled Parameters
            object saved to disk to create the run-specific object for
            CoDCorrect or FauxCorrect.

        create_new_version_id(): creates a new version_id in the appropriate
            db table (cod.codcorrect_version or cod.fauxcorrect_version).
            Returns the newly created version id.

        cause_tree_from_cause_set_id(): given a cause_set_id, returns the tree
            representation of the cause hierarchy. Raises an exception if the
            cause_set_id is not valid for the process.

        contains_cause_set_id(): true or false, is the provided cause_set_id
            contained in this FauxCorrect version?

        location_tree_from_location_set_id(): given a location_set_id, returns
            the tree representation of the location hierarchy. Raises an
            exception if the location_set_id is not valid for the process.

        contains_location_set_id(): true or false, is the provided
            location_set_id contained in this FauxCorrect version?

        contains_model_version_id(): true or false, does the FauxCorrect
            version contain the provided model_version_id?

        models_contain_cause_id(): true or false, do the FauxCorrect version
            models contain a model for the provided cause_id?

        get_cause_from_model_version_id(): returns the cause_id that the
            provided model_version_id is associated with.

        get_required_indices_from_model_version_id(): returns the square
            pd.DataFrame representing all of the demographic indices that must
            exist in the model_version_id for it to be considered a valid
            model.

        validate_model_versions(): runs the validations on the model version
            ids to ensure that no models overlap in ages, that no models exceed
            the age limits as determined by the cause metadata, that no models
            contain a restricted sex id, and that the models contain all of the
            required indices.

        cache_parameters(): saves the MachineParameters to disk for use
            later in the run's pipeline as well as for vetting in the future.

        create_gbd_process_version(): creates the necessary database assets in
            the gbd database for a fauxcorrect run. These include a process
            version id and process version metadata.

    """
    GBD_PROCESS_ID: Optional[int] = None
    GBD_PROCESS: Optional[str] = None
    CAUSE_SET_IDS: Optional[List[int]] = None
    DESCRIPTION: str = None

    def __init__(self, version_id: int):
        self.version_id: int = version_id
        self.parent_dir: str = self.create_parent_dir(version_id)

        # instantiate null fields
        self._parent_dir: Optional[str] = None
        self._year_ids: Optional[List[int]] = None
        self._year_start_ids: Optional[List[int]] = None
        self._year_end_ids: Optional[List[int]] = None
        self._sex_ids: Optional[List[int]] = None
        self._location_set_ids: Optional[List[int]] = None
        self._measure_ids: Optional[List[int]] = None
        self._gbd_round_id: Optional[int] = None
        self._decomp_step: Optional[str] = None
        self._decomp_step_id: Optional[int] = None
        self._n_draws: Optional[int] = None
        self._process: Optional[str] = None
        self._scalar_version_id: Optional[int] = None
        self._gbd_process_version_id: Optional[int] = None
        self._expected_metadata: Optional[pd.DataFrame] = None

        self._databases: Optional[List[str]] = None

        # instantiate null parameter fields
        self._location_parameters: Optional[Dict[int, LocationParameters]] = (
            None
        )
        self._cause_parameters: Optional[Dict[int, CauseParameters]] = None
        self._model_version_parameter: Optional[ModelVersionParameters] = None
        self._envelope_parameter: Optional[EnvelopeParameters] = None
        self._population_parameter: Optional[PopulationParameters] = None

        self._eligible_metadata: Optional[pd.DataFrame] = None

    @classmethod
    def new(
            cls,
            year_ids: List[int],
            location_set_ids: List[int],
            sex_ids: List[int] = [gbd.sex.MALE, gbd.sex.FEMALE],
            measure_ids: List[int] = [gbd.measures.DEATH, gbd.measures.YLL],
            gbd_round_id: int = gbd.GBD_ROUND_ID,
            decomp_step: str = gbd.decomp_step.ONE,
            n_draws: int = constants.Draws.N_DRAWS,
            process: str = GBD_PROCESS,
            databases: List[str] = [constants.DataBases.GBD],
            year_start_ids: Optional[List[int]] = None,
            year_end_ids: Optional[List[int]] = None
    ):

        version_id: int = cls.get_new_version_id()
        inst = cls(version_id)

        inst.year_ids: Optional[List[int]] = year_ids
        inst.year_start_ids: Optional[List[int]] = year_start_ids
        inst.year_end_ids: Optional[List[int]] = year_end_ids
        inst.location_set_ids: Optional[List[int]] = location_set_ids
        inst.sex_ids: Optional[List[int]] = sex_ids
        inst.measure_ids: Optional[List[int]] = measure_ids
        inst.gbd_round_id: Optional[int] = gbd_round_id
        inst.decomp_step: Optional[str] = decomp_step
        inst.decomp_step_id: Optional[int] = decomp_step_id_from_decomp_step(
            step=decomp_step, gbd_round_id=gbd_round_id
        )
        inst.n_draws: Optional[int] = n_draws
        inst.process: Optional[str] = process
        # scalar_version_id will be retrieved for FauxcCorrect AND CoDCorrect
        # runs but will only be used in FauxCorrect runs
        inst.scalar_version_id: Optional[int] = (
            fauxcorrect_version.get_scalar_version_id(gbd_round_id)
        )
        inst.databases: Optional[List[str]] = databases
        # Need to instantiate envelope, population, and life table parameter
        # objs before creating the process version row.
        inst._create_envelope_parameter()
        inst._create_population_parameter()
        inst._create_life_table_parameter()
        metadata, version_note = inst._create_process_version_info(process)
        # Create version row in the appropriate table for the run
        inst.create_new_version_row(
            version_id,
            inst.DESCRIPTION,
            inst.decomp_step_id,
            inst.gbd_round_id
        )
        # Create gbd process version id for the run
        inst.gbd_process_version_id = (
            inst.create_gbd_process_version(metadata, version_note)
        )
        # Create parameter objects
        inst._create_cause_parameters()
        inst._create_location_parameters()
        inst._create_model_version_parameter()
        # Create eligible metadata
        inst._create_eligible_metadata()

        return inst

    @classmethod
    def create_new_version_id() -> None:
        raise NotImplementedError

    @classmethod
    def create_parent_dir(cls, version_id: int,
                          process_name: Optional[str]=None):
        if process_name:
            process = process_name
        else:
            process = cls.GBD_PROCESS
        return os.path.join(constants.FilePaths.ROOT_DIR,
            process, str(version_id)
        )

    @classmethod
    def recreate_from_cache(cls, cache_name: str):
        with open(cache_name, 'rb') as cache_file:
            inst = pickle.load(cache_file)
        return inst

    @classmethod
    def recreate_from_version_id(cls, version_id: int,
                                 process_name: Optional[str]=None):
        parent_dir = cls.create_parent_dir(version_id, process_name)
        cache_name = os.path.join(
            parent_dir,
            constants.FilePaths.PARAM_DIR,
            constants.FilePaths.PARAM_FILE
        )
        return cls.recreate_from_cache(cache_name)

    def cache_parameters(self, cache_name: Optional[str]=None) -> str:
        if not cache_name:
            cache_name = os.path.join(
                self.parent_dir,
                constants.FilePaths.PARAM_DIR,
                constants.FilePaths.PARAM_FILE
            )
        with open(cache_name, 'wb') as cache_file:
            pickle.dump(self, cache_file)
        return cache_name

    def create_gbd_process_version(
            self,
            metadata: Dict[int, int],
            version_note: str
    ) -> int:
        process_version = gbd_process.GBDProcessVersion.add_new_version(
            gbd_process_id=self.GBD_PROCESS_ID,
            gbd_process_version_note=version_note,
            code_version=_get_code_version(),
            gbd_round_id=self.gbd_round_id,
            decomp_step=self.decomp_step,
            metadata=metadata
        )
        return process_version.gbd_process_version_id

    @property
    def year_ids(self) -> List[int]:
        return self._year_ids

    @year_ids.setter
    def year_ids(self, val) -> None:
        self._year_ids = val

    @property
    def year_start_ids(self) -> List[int]:
        return self._year_start_ids

    @year_start_ids.setter
    def year_start_ids(self, val) -> None:
        self._year_start_ids = val

    @property
    def year_end_ids(self) -> List[int]:
        return self._year_end_ids

    @year_end_ids.setter
    def year_end_ids(self, val) -> None:
        self._year_end_ids = val

    @property
    def change_year_ids(self) -> List[int]:
        return self._year_ids

    @property
    def sex_ids(self) -> List[int]:
        return self._sex_ids

    @property
    def all_sex_ids(self) -> List[int]:
        return self.sex_ids + [gbd.sex.BOTH]

    @sex_ids.setter
    def sex_ids(self, val) -> None:
        self._sex_ids = val

    @property
    def most_detailed_age_group_ids(self) -> List[int]:
        return constants.Ages.MOST_DETAILED_GROUP_IDS

    @property
    def all_age_group_ids(self) -> List[int]:
        return (
                self.most_detailed_age_group_ids +
                gbd.GBD_COMPARE_AGES +
                [gbd.age.ALL_AGES]
        )

    @property
    def measure_ids(self) -> List[int]:
        return self._measure_ids

    @measure_ids.setter
    def measure_ids(self, val) -> None:
        self._measure_ids = val

    @property
    def location_set_ids(self) -> List[int]:
        return self._location_set_ids

    @location_set_ids.setter
    def location_set_ids(self, val) -> None:
        self._location_set_ids = val

    @property
    def gbd_round_id(self) -> int:
        return self._gbd_round_id

    @gbd_round_id.setter
    def gbd_round_id(self, val) -> None:
        self._gbd_round_id = val

    @property
    def decomp_step(self) -> str:
        return self._decomp_step

    @decomp_step.setter
    def decomp_step(self, val) -> None:
        self._decomp_step = val

    @property
    def decomp_step_id(self) -> int:
        return self._decomp_step_id

    @decomp_step_id.setter
    def decomp_step_id(self, val) -> None:
        self._decomp_step_id = val

    @property
    def n_draws(self) -> int:
        return self._n_draws

    @n_draws.setter
    def n_draws(self, value) -> None:
        self._n_draws = value

    @property
    def process(self) -> str:
        return self._process

    @process.setter
    def process(self, val) -> None:
        self._process = val

    @property
    def scalar_version_id(self) -> int:
        return self._scalar_version_id

    @scalar_version_id.setter
    def scalar_version_id(self, val) -> None:
        self._scalar_version_id = val

    @property
    def databases(self) -> List[str]:
        return self._databases

    @databases.setter
    def databases(self, value) -> None:
        self._databases = value

    @property
    def most_detailed_cause_ids(self) -> List[int]:
        raise NotImplementedError

    @property
    def cause_ids(self) -> List[int]:
        return list({
            cause_id for cause_set in self.CAUSE_SET_IDS
            for cause_id in self._cause_parameters[cause_set].cause_ids
        })

    @property
    def cause_rescaling_tree(self) -> hierarchies.tree.Tree:
        return self._cause_parameters[constants.CauseSetId.CODCORRECT].tree

    @property
    def cause_aggregation_tree(self) -> None:
        raise NotImplementedError

    @property
    def envelope_version_id(self):
        return self._envelope_parameter.run_id

    @property
    def life_table_version_id(self):
        return self._life_table_parameter.run_id

    @property
    def most_detailed_location_ids(self) -> List[int]:
        return (
            self._location_parameters[constants.LocationSetId.OUTPUTS]
            .most_detailed_ids
        )

    @property
    def location_ids(self) -> List[int]:
        return list({
            loc_id for loc_set in self.location_set_ids
            for loc_id in self._location_parameters[loc_set].location_ids
        })

    @property
    def aggregate_location_ids(self) -> List[int]:
        return [loc_id for loc_id in self.location_ids
                if loc_id not in self.most_detailed_location_ids]

    @property
    def location_aggregation_tree(self) -> hierarchies.tree.Tree:
        return self._location_parameters[constants.LocationSetId.OUTPUTS].tree

    @property
    def best_model_version_ids(self) -> List[int]:
        return self._model_version_parameter.best_ids

    @property
    def best_model_metadata(self):
        return self._model_version_parameter.best_metadata

    @property
    def population_version_id(self):
        return self._population_parameter.run_id

    @property
    def gbd_process_version_id(self) -> int:
        return self._gbd_process_version_id

    @property
    def expected_metadata(self) -> pd.DataFrame:
        """The cause_ids, sex_ids, age_starts, and age_ends expected for a
        full machinery run"""
        if self._expected_metadata is None:
            self._expected_metadata = self._get_expected_metadata()
        return self._expected_metadata

    @gbd_process_version_id.setter
    def gbd_process_version_id(self, value: int) -> None:
        self._gbd_process_version_id = value

    def cause_tree_from_cause_set_id(
            self,
            cause_set_id: int
    ) -> hierarchies.tree.Tree:
        if not self.contains_cause_set_id(cause_set_id):
            raise ValueError(
                f"cause_set_id, {cause_set_id}, is not a valid cause set in "
                f"this {self.GBD_PROCESS} version: version {self.version_id}."
            )
        return self._cause_parameters[cause_set_id].tree

    def contains_cause_set_id(self, cause_set_id: int) -> bool:
        return cause_set_id in self.CAUSE_SET_IDS

    def location_tree_from_location_set_id(
            self,
            location_set_id: int
    ) -> hierarchies.tree.Tree:
        if not self.contains_location_set_id(location_set_id):
            raise ValueError(
                f"location_set_id, {location_set_id}, is not a valid location "
                f"set in this {self.GBD_PROCESS} version: version {self.version_id}."
            )
        return self._location_parameters[location_set_id].tree

    def contains_location_set_id(self, location_set_id: int) -> bool:
        return location_set_id in self.location_set_ids

    def contains_model_version_id(self, model_version_id: int) -> bool:
        return self._model_version_parameter.contains_model_version(
            model_version_id=model_version_id
        )

    def models_contain_cause_id(self, cause_id: int) -> bool:
        return self._model_version_parameter.contains_cause_id(
            cause_id=cause_id
        )

    def get_cause_from_model_version_id(self, model_version_id: int) -> int:
        return self._model_version_parameter.get_cause_id_from_version_id(
            model_version_id=model_version_id
        )

    def get_metadata_by_sex_id(self, sex_id: int) -> pd.DataFrame:
        return self._model_version_parameter.get_metadata_by_sex_id(
            sex_id=sex_id
        )

    def get_metadata_from_model_version_id(
            self,
            model_version_id: int
    ) -> pd.DataFrame:
        return self._model_version_parameter.get_metadata_from_version_id(
            model_version_id=model_version_id
        )

    def get_required_indices_from_model_version_id(
            self,
            model_version_id: int,
            age_start_column: str,
            age_end_column: str
    ) -> pd.DataFrame:
        """
        Return a pd.DataFrame representing all required indices for the given
        model_version_id.

        Arguments:
            model_version_id (int)

        Returns:
            pd.DataFrame with columns year, location, age, and sex and all
            required values dictated by the model version's metadata in the
            CoD database.

        Raises:
            RuntimeError - if the model_version_id requested is not in our
            collection of best models for this Fauxcorrect run.
        """
        if not self.contains_model_version_id(model_version_id):
            raise RuntimeError(
                f"Model version: {model_version_id} not found in best models "
                f"for {self.GBD_PROCESS} v{self.version_id} run."
            )
        model_metadata = (
            self._model_version_parameter.get_metadata_dict_from_version_id(
                model_version_id=model_version_id,
                eligible_metadata=self._eligible_metadata
            )
        )
        cartesian_index = pd.MultiIndex.from_product(
            iterables=[
                self.year_ids,
                self.most_detailed_location_ids,
                _get_age_range(
                    model_metadata[age_start_column],
                    model_metadata[age_end_column]
                ),
                [model_metadata[constants.Columns.SEX_ID]]
            ],
            names=[
                constants.Columns.YEAR_ID,
                constants.Columns.LOCATION_ID,
                constants.Columns.AGE_GROUP_ID,
                constants.Columns.SEX_ID
            ]
        )
        return pd.DataFrame(index=cartesian_index).reset_index()

    def validate_model_versions(self, cause_set_id) -> None:
        self._model_version_parameter.run_validations(
            self._cause_parameters[cause_set_id],
            self._eligible_metadata
        )

    def create_input_model_tracking_report(self) -> None:
        input_models = self._eligible_metadata.copy()
        input_models = input_models[
            constants.Columns.MODEL_TRACKER_COLS
        ].sort_values(constants.Columns.MODEL_TRACKER_SORTBY_COLS)
        cache_file = os.path.join(
            self.parent_dir,
            constants.FilePaths.INPUT_FILES_DIR,
            constants.FilePaths.INPUT_MODELS_FILE.format(
                process=self.process,
                version_id=self.version_id
            )
        )
        input_models.to_csv(cache_file, index=False, encoding="utf8")

    def _create_cause_parameters(self) -> None:
        self._cause_parameters: Dict[int, CauseParameters] = {}
        for cause_set in self.CAUSE_SET_IDS:
            self._cause_parameters[cause_set] = CauseParameters(
                cause_set_id=cause_set,
                gbd_round_id=self.gbd_round_id,
                decomp_step_id=self.decomp_step_id
            )

    def _create_envelope_parameter(self) -> None:
        self._envelope_parameter: EnvelopeParameters = EnvelopeParameters(
            gbd_round_id=self.gbd_round_id,
            decomp_step=self.decomp_step
        )

    def _create_life_table_parameter(self) -> None:
        self._life_table_parameter: LifeTableParameters = LifeTableParameters(
            gbd_round_id=self.gbd_round_id,
            decomp_step=self.decomp_step
        )

    def _create_location_parameters(self) -> None:
        self._location_parameters: Dict[int, LocationParameters] = {}
        for loc_set in self.location_set_ids:
            self._location_parameters[loc_set] = LocationParameters(
                location_set_id=loc_set,
                gbd_round_id=self.gbd_round_id
            )

    def _create_model_version_parameter(self) -> None:
        raise NotImplementedError

    def _create_process_version_info(self,
        process: str
    ) -> Tuple[Dict[int, int], str]:
        metadata: Dict[int, int] = {
            constants.GBD.MetadataTypeIds.ENVELOPE: self.envelope_version_id,
            constants.GBD.MetadataTypeIds.POPULATION: (
                self.population_version_id
            )
        }
        process_map: Dict[str, Tuple[int, str]] = {
            constants.GBD.Process.Name.CODCORRECT: (
                {
                    constants.GBD.MetadataTypeIds.CODCORRECT: self.version_id,
                    constants.GBD.MetadataTypeIds.LIFE_TABLE: (
                        self.life_table_version_id
                    )
                },
                constants.GBD.Process.VersionNote.CODCORRECT.format(
                    version_id=self.version_id,
                    population_version=self.population_version_id,
                    envelope_version=self.envelope_version_id,
                    life_table_version=self.life_table_version_id
                )
            ),
            constants.GBD.Process.Name.FAUXCORRECT: (
                {
                    constants.GBD.MetadataTypeIds.FAUXCORRECT: self.version_id
                },
                constants.GBD.Process.VersionNote.FAUXCORRECT.format(
                    version_id=self.version_id,
                    scalar_version=self.scalar_version_id,
                    population_version=self.population_version_id,
                    envelope_version=self.envelope_version_id
                )
            )
        }
        additional_metadata, version_note = process_map[process]
        metadata.update(additional_metadata)
        return metadata, version_note

    def _create_population_parameter(self) -> None:
        self._population_parameter: PopulationParameters = PopulationParameters(
            gbd_round_id=self.gbd_round_id,
            decomp_step=self.decomp_step,
            location_set_id=constants.LocationSetId.OUTPUTS
        )

    def _create_eligible_metadata(self) -> pd.DataFrame:
        eligible_metadata = self._add_cause_metadata()
        nans = eligible_metadata.cause_age_start.isna()
        these_are_nans = eligible_metadata[nans]
        no_nans = eligible_metadata[~nans]
        inferred = (
            self._infer_age_restrictions_from_metadata(no_nans)
        )
        self._eligible_metadata = pd.concat(
            [inferred, these_are_nans],
            ignore_index=True
        )


    def _add_cause_metadata(self) -> pd.DataFrame:
        """
        Merges expected cause-sex-age demographics onto best models from
        ModelVersionParameters. Adds cause_age_start and cause_age_end columns.

        Returns:
            A dataframe with cause_age_start and cause_age_end columns that
                represent the age restrictions placed on the cause.
        """
        # Use an outer merge here to catch any unexpected results
        return pd.merge(
            self.expected_metadata,
            self._model_version_parameter.best_metadata,
            on=[constants.Columns.CAUSE_ID, constants.Columns.SEX_ID],
            how='outer'
        )

    def _infer_age_restrictions_from_metadata(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        CoD modelers can have multiple models for the same cause and sex
        as long as they are split up into different, non-overlapping age
        ranges. Update the cause metadata so that cause_age_start
        and cause_age_end accomodate multiple, age-range-split models. These
        columns are used to filter return draws during draw validation.

        Arguments:
            df (pd.DataFrame)

        Returns:
            A dataframe with cause_age_start and cause_age_end columns that
                represent the age restrictions placed on the model
        """
        group = (
            [
                constants.Columns.CAUSE_ID,
                constants.Columns.MODEL_VERSION_TYPE_ID,
                constants.Columns.SEX_ID
            ]
        )
        df.loc[:, constants.Columns.RANK] = (
            df.groupby(group)[constants.Columns.AGE_START].apply(
                lambda x: x.rank()
            )
        )
        df.loc[:, constants.Columns.MAX_RANK] = (
            df.groupby(group)[constants.Columns.RANK].transform(max)
        )
        # HIV and IC models do have age restrictions but we know that there
        # are no age restrictions for shocks (model_version_type_id 5).
        df.loc[:, constants.Columns.CAUSE_AGE_START] = (
            df.apply(lambda row: row[constants.Columns.CAUSE_AGE_START] if
                row[constants.Columns.MODEL_VERSION_TYPE_ID] != 5 and
                (row[constants.Columns.AGE_START] <
                row[constants.Columns.CAUSE_AGE_START] or
                row[constants.Columns.AGE_START] >
                row[constants.Columns.CAUSE_AGE_START] and
                row[constants.Columns.RANK] == 1) else
                row[constants.Columns.AGE_START],
                axis=1
            )
        )
        df.loc[:, constants.Columns.CAUSE_AGE_END] = (
            df.apply(lambda row: row[constants.Columns.CAUSE_AGE_END] if
                row[constants.Columns.MODEL_VERSION_TYPE_ID] != 5 and
                (row[constants.Columns.AGE_END] >
                row[constants.Columns.CAUSE_AGE_END] or
                row[constants.Columns.AGE_END] <
                row[constants.Columns.CAUSE_AGE_END] and
                row[constants.Columns.RANK] ==
                row[constants.Columns.MAX_RANK]) else
                row[constants.Columns.AGE_END],
                axis=1
            )
        )
        df = df.drop(
            [constants.Columns.RANK, constants.Columns.MAX_RANK],
            axis=1
        )
        return df

    def _get_expected_metadata(self) -> pd.DataFrame:
        """
        Creates total expected cause-sex-age demographics for the machinery run
        from a specific a cause_set_id.

        """
        try:
            cause_params = (
                self._cause_parameters[
                        constants.CauseSetId.COMPUTATION
                ]
            )
            cause_metadata = cause_params.metadata_dataframe
            # filter out yld_only causes
            cause_metadata = cause_metadata.loc[
                cause_metadata[constants.Columns.YLD_ONLY]!=1]
            # merge on hierarchy object and filter to causes we estimate
            # (i.e. causes for which modelers produce and upload results)
            cause_metadata = pd.merge(
                cause_metadata,
                cause_params.hierarchy[
                    [
                        constants.Columns.CAUSE_ID,
                        constants.Columns.IS_ESTIMATE
                    ]
                ],
                how='left',
                on=constants.Columns.CAUSE_ID
            )
            cause_metadata = cause_metadata.loc[
                (cause_metadata[constants.Columns.IS_ESTIMATE]==1) |
                (cause_metadata[constants.Columns.CAUSE_ID].isin(
                    [constants.SpecialMappings.NON_MELANOMA,
                    constants.SpecialMappings.EYE_CANCER])
                )
            ]
            # add column with list of age_group_ids that fall within range of
            # cause_age_start cause_age_end for use in correct.py module
            cause_metadata[constants.Columns.CAUSE_AGES] = (
                cause_metadata.apply(
                    lambda row: [
                        a
                        for a in self.most_detailed_age_group_ids
                        if (
                            a >= row[constants.Columns.CAUSE_AGE_START] and
                            a <= row[constants.Columns.CAUSE_AGE_END]
                        )
                    ],
                    axis=1,
                )
            )
            return cause_metadata.drop(constants.Columns.IS_ESTIMATE, axis=1)

        except KeyError as e:
            raise RuntimeError(
                f"The _caues_parameters dictionary is missing a "
                f"CauseParameters object for cause_set_id = "
                f"{constants.CauseSetId.COMPUTATION}. This cause set is "
                f"needed to determine expected input models.")

    def start_model(self) -> None:
        raise NotImplementedError

    def mark_best(self) -> None:
        raise NotImplementedError

    def generate_project_directories(self):
        raise NotImplementedError


class FauxCorrectParameters(MachineParameters):
    """
    Single object for managing a FauxCorrect run.

    Contains the run-specific metadata as well as methods that create
    FauxCorrect's file-system and database assets.

    """
    GBD_PROCESS_ID: int = constants.GBD.Process.Id.FAUXCORRECT
    GBD_PROCESS: str = constants.GBD.Process.Name.FAUXCORRECT
    GBD_UPLOAD_TABLE: str = constants.GBD.DataBase.FAUXCORRECT_TABLE
    CAUSE_SET_IDS: List[int] = [
            constants.CauseSetId.FAUXCORRECT,
            constants.CauseSetId.COMPUTATION
    ]
    DESCRIPTION: str = f"New FauxCorrect run on {time.strftime('%b %d %Y')}"

    @classmethod
    def get_new_version_id(cls) -> int:
        return fauxcorrect_version.get_new_version_id()

    def create_new_version_row(
            self,
            new_version_id: int,
            description: str,
            decomp_step_id: int,
            gbd_round_id: int
    ) -> None:
        fauxcorrect_version.create_new_version_row(
            fauxcorrect_version_id=new_version_id,
            description=description,
            code_version=_get_code_version(),
            scalar_version_id=(
                fauxcorrect_version.get_scalar_version_id(gbd_round_id)
            ),
            gbd_round_id=gbd_round_id,
            decomp_step_id=decomp_step_id
        )

    @property
    def most_detailed_cause_ids(self) -> List[int]:
        return (
            self._cause_parameters[constants.CauseSetId.FAUXCORRECT]
            .most_detailed_ids
        )

    @property
    def cause_aggregation_tree(self) -> hierarchies.tree.Tree:
        if (
            not self.contains_cause_set_id(constants.CauseSetId.REPORTING)
            or
            not self.contains_cause_set_id(constants.CauseSetId.COMPUTATION)
        ):
            raise NotImplementedError(
                "Cause aggregation with the reporting cause_set tree is "
                "forbidden in FauxCorrect."
            )
        return self._cause_parameters[constants.CauseSetId.COMPUTATION].tree

    def _create_model_version_parameter(self) -> None:
        self._model_version_parameter = ModelVersionParameters(
            gbd_round_id=self.gbd_round_id,
            decomp_step_id=self.decomp_step_id,
            process=self.GBD_PROCESS
        )

    def start_model(self) -> None:
        fauxcorrect_version.update_status(
            self.version_id, constants.Status.RUNNING
        )

    def mark_best(self) -> None:
        fauxcorrect_version.unmark_current_best(
            self.gbd_round_id, self.decomp_step_id)
        fauxcorrect_version.mark_best(self.version_id)

    def generate_project_directories(self):
        project_dirs = [
            os.path.join(
                constants.FilePaths.DRAWS_UNSCALED_DIR,
                constants.FilePaths.DEATHS_DIR
            ),
            constants.FilePaths.PARAM_DIR,
            constants.FilePaths.INPUT_FILES_DIR
        ] + [
            os.path.join(*element) for element
            in itertools.product(
                [
                    os.path.join(
                        constants.FilePaths.UNAGGREGATED_DIR,
                        constants.FilePaths.SHOCKS_DIR,
                        constants.FilePaths.LOCATION_AGGREGATES
                    )
                ],
                constants.FilePaths.MEASURE_DIRS
            )
        ] + [
            os.path.join(*element) for element
            in itertools.product(
                [constants.FilePaths.DRAWS_SCALED_DIR],
                constants.FilePaths.MEASURE_DIRS
            )
        ] + [
            os.path.join(*element) for element
            in itertools.product(
                [
                    os.path.join(
                        constants.FilePaths.UNAGGREGATED_DIR,
                        constants.FilePaths.SHOCKS_DIR
                    )
                ],
                constants.FilePaths.MEASURE_DIRS
            )
        ] + [
            os.path.join(*element) for element
            in itertools.product(
                [constants.FilePaths.LOG_DIR],
                constants.FilePaths.STD_DIRS
            )
        ] + [
            os.path.join(*element) for element
            in itertools.product(
                [
                    os.path.join(
                        constants.FilePaths.SUMMARY_DIR,
                        constants.FilePaths.GBD_UPLOAD
                    )
                ],
                [str(measure) for measure in self.measure_ids],
                [str(year) for year in self.year_ids]
            )
        ] + [
            os.path.join(*element) for element
            in itertools.product(
                [
                    os.path.join(
                        constants.FilePaths.SUMMARY_DIR,
                        constants.FilePaths.COD_UPLOAD,
                        constants.FilePaths.DEATHS_DIR
                    )
                ],
                [str(year) for year in self.year_ids]
            )
        ]
        for directory in project_dirs:
            try:
                os.makedirs(
                    os.path.join(self.parent_dir, directory),
                    mode=0o775
                )
            except OSError:
                raise RuntimeError(
                    f"{self.GBD_PROCESS} version {self.version_id} has already been "
                    "created. Cannot overwrite this version."
                )


class CoDCorrectParameters(MachineParameters):
    """
    Single object for managing a CoDCorrect run.

    Contains the run-specific metadata as well as methods that create
    CoDCorrect's file-system and database assets.

    """

    GBD_PROCESS_ID: int = constants.GBD.Process.Id.CODCORRECT
    GBD_PROCESS: str = constants.GBD.Process.Name.CODCORRECT
    GBD_UPLOAD_TABLE: str = constants.GBD.DataBase.CODCORRECT_TABLE
    COD_UPLOAD_TABLE: str = constants.COD.DataBase.TABLE
    DIAGNOSTIC_UPLOAD_TABLE: str = constants.Diagnostics.DataBase.TABLE
    CAUSE_SET_IDS: List[int] = [
        constants.CauseSetId.CODCORRECT,
        constants.CauseSetId.COMPUTATION,
        constants.CauseSetId.REPORTING_AGGREGATES
    ]
    CAUSE_AGGREGATION_SET_IDS: List[int] = [
        constants.CauseSetId.COMPUTATION,
        constants.CauseSetId.REPORTING_AGGREGATES
    ]
    DESCRIPTION: str = f"New CoDCorrect run on {time.strftime('%b %d %Y')}"

    @classmethod
    def get_new_version_id(cls) -> int:
        return codcorrect_version.get_new_version_id()

    def create_new_version_row(
            self,
            new_version_id: int,
            description: str,
            decomp_step_id: int,
            gbd_round_id: int
    ) -> None:
        codcorrect_version.create_new_version_row(
            codcorrect_version_id=new_version_id,
            description=description,
            decomp_step_id=decomp_step_id,
            code_version=str(gbd_round_id),
            env_version=self.envelope_version_id
        )

        codcorrect_version.create_output_partition(
            codcorrect_version_id=new_version_id)

    @property
    def most_detailed_cause_ids(self) -> List[int]:
        return (
            self._cause_parameters[constants.CauseSetId.CODCORRECT]
            .most_detailed_ids
        )

    def _create_model_version_parameter(self) -> None:
        self._model_version_parameter = ModelVersionParameters(
            gbd_round_id=self.gbd_round_id,
            decomp_step_id=self.decomp_step_id,
            process=self.GBD_PROCESS
        )

    def start_model(self) -> None:
        codcorrect_version.update_status(
            self.version_id, constants.Status.RUNNING
        )

    def mark_best(self) -> None:
        codcorrect_version.unmark_current_best(self.decomp_step_id)
        codcorrect_version.mark_best(self.version_id)

    def generate_project_directories(self):
        project_dirs = [
            constants.FilePaths.DIAGNOSTICS_DIR,
            constants.FilePaths.DRAWS_DIR,
            constants.FilePaths.INPUT_FILES_DIR,
            constants.FilePaths.PARAM_DIR,
            constants.FilePaths.SHOCKS_DIR
        ] + [
            os.path.join(*element) for element
            in itertools.product(
                [constants.FilePaths.DRAWS_DIR],
                constants.FilePaths.MEASURE_DIRS
            )
        ] + [
            os.path.join(*element) for element
            in itertools.product(
                [constants.FilePaths.LOG_DIR],
                constants.FilePaths.STD_DIRS
            )
        ] + [
            os.path.join(*element) for element
            in itertools.product(
                [
                    constants.FilePaths.UNAGGREGATED_DIR,
                    constants.FilePaths.AGGREGATED_DIR
                ],
                [
                    constants.FilePaths.RESCALED_DIR,
                    constants.FilePaths.SHOCKS_DIR,
                    constants.FilePaths.UNSCALED_DIR
                ],
                constants.FilePaths.MEASURE_DIRS
            )
        ] + [
            os.path.join(
                constants.FilePaths.UNAGGREGATED_DIR,
                constants.FilePaths.UNSCALED_DIR,
                constants.FilePaths.DIAGNOSTICS_DIR,
                constants.FilePaths.DEATHS_DIR
            )
        ] + [
            os.path.join(*element) for element
            in itertools.product(
                [
                    os.path.join(
                        constants.FilePaths.SUMMARY_DIR,
                        constants.FilePaths.GBD_UPLOAD
                    )
                ],
                [constants.FilePaths.SINGLE_DIR],
                [str(measure) for measure in self.measure_ids],
                [str(year) for year in self.year_ids]
            )
        ] + [
            os.path.join(*element) for element
            in itertools.product(
                [
                    os.path.join(
                        constants.FilePaths.SUMMARY_DIR,
                        constants.FilePaths.COD_UPLOAD,
                        constants.FilePaths.DEATHS_DIR
                    )
                ],
                [str(year) for year in self.year_ids]
            )
        ]
        if self._year_start_ids:
            project_dirs = project_dirs + [
                os.path.join(*element) for element
                in itertools.product(
                    [
                        os.path.join(
                            constants.FilePaths.SUMMARY_DIR,
                            constants.FilePaths.GBD_UPLOAD
                            )
                    ],
                    [constants.FilePaths.MULTI_DIR],
                    [str(measure) for measure in self.measure_ids],
                    ["_".join([str(self._year_start_ids[i]),
                               str(self._year_end_ids[i])])
                     for i in range(len(self._year_start_ids))]
                )
            ]
        for directory in project_dirs:
            try:
                os.makedirs(
                    os.path.join(self.parent_dir, directory),
                    mode=0o755
                )
            except OSError:
                raise RuntimeError(
                    f"{self.process} version {self.version_id} has already "
                    "been created. Cannot overwrite this version."
                )

def _get_age_range(age_start: int, age_end: int) -> List[int]:
    """Return a list of age group ids."""
    valid_ages = constants.Ages.MOST_DETAILED_GROUP_IDS
    return [age for age in valid_ages if age_start <= age <= age_end]


def _get_code_version() -> str:
    return str(fauxcorrect.__version__).replace("'", '"')
