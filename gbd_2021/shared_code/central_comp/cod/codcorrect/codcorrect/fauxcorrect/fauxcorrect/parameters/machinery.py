import itertools
import os
import pickle
import subprocess
import time
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

import db_queries
import gbd.constants as gbd
from gbd.decomp_step import decomp_step_id_from_decomp_step
from gbd_outputs_versions import gbd_process
import hierarchies.tree

import fauxcorrect
from fauxcorrect.parameters import base_cause_set
from fauxcorrect.parameters.cause import CauseParameters
from fauxcorrect.parameters.envelope import EnvelopeParameters
from fauxcorrect.parameters.lifetable import LifeTableParameters
from fauxcorrect.parameters.location import LocationParameters
from fauxcorrect.parameters.model_version import ModelVersionParameters
from fauxcorrect.parameters.population import PopulationParameters
from fauxcorrect.queries import codcorrect_version, fauxcorrect_version
from fauxcorrect.utils import constants, helpers


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

        base_location_set_id (int): The location set that forms the "base" of
            the run, meaning the set the actual correction/scaling and aggregation
            is done on.

        aggregation_location_set_ids (List[int]): the location sets to aggregate
            up to from the results from the base location set

        base_cause_set_id (int): The cause set that forms the "base" of the run,
            where correction/scaling and aggregation is done on.

        aggregation_cause_set_ids (List[int]): the cause sets to aggregate up to
            from the results of correction from the base cause set. Only used in
            CodCorrect.

        gbd_round_id (int): the GBD round id of the run. Defaults to the current
            GBD round id

        decomp_step (str): the decomp step of the run. Defaults to decomp_step
            step1 at this time.

        decomp_step_id (int): the decomp_step_id for the run. Inferred from the
            decomp_step argument.

        process (str): the process we are computing. Determines the cause sets
            and locations sets we'll use for the run. Two options, either
            { 'codcorrect' | 'fauxcorrect' }

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

        scatter_version_id: CodCorrect run id to compare
            this run against via scatters.

        correction_hierarchy: The correction (scaling) hierarchy used for
            applying the CodCorrect correction. Generated from the base cause set

    Methods:
        recreate_from_cache(): reads from a pickled Parameters
            object saved to disk to create the run-specific object for
            CoDCorrect or FauxCorrect.

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
    ADDITIONAL_CAUSE_SET_IDS: Optional[List[int]] = None

    def __init__(
            self,
            gbd_round_id: int,
            decomp_step: str,
            test: bool,
            year_ids: List[int],
            base_location_set_id: int,
            aggregation_location_set_ids: List[int],
            base_cause_set_id: int,
            aggregation_cause_set_ids: List[int],
            measure_ids: List[int],
            databases: List[str],
            sex_ids: List[int] = [gbd.sex.MALE, gbd.sex.FEMALE],
            n_draws: int = constants.Draws.MAX_DRAWS,
            process: str = GBD_PROCESS,
            year_start_ids: Optional[List[int]] = None,
            year_end_ids: Optional[List[int]] = None,
            scatter_version_id: Optional[int] = None
    ):

        # Retrieve a CodCorrect version and a COD output version for this run
        self.version_id: int = codcorrect_version.get_new_codcorrect_version_id()
        self.cod_output_version_id: int = codcorrect_version.get_new_cod_output_version_id()

        # fill in input parameters
        self.gbd_round_id: int = gbd_round_id
        self.decomp_step: str = decomp_step
        self.decomp_step_id: int = decomp_step_id_from_decomp_step(
            step=decomp_step, gbd_round_id=gbd_round_id
        )
        self.test = test
        self.year_ids: List[int] = year_ids
        self.year_start_ids: List[int] = year_start_ids
        self.year_end_ids: List[int] = year_end_ids

        self.base_location_set_id: int = base_location_set_id
        self.aggregation_location_set_ids: List[int] = \
            aggregation_location_set_ids
        self.base_cause_set_id: int = base_cause_set_id
        self.aggregation_cause_set_ids: List[int] = \
            aggregation_cause_set_ids

        self.sex_ids: List[int] = sex_ids
        self.measure_ids: List[int] = measure_ids
        self.n_draws: int = n_draws
        self.process: str = process
        self.databases: List[str] = databases

        # Populate necessary fields
        self.draw_cols: List[str] = [f'draw_{n}' for n in range(self.n_draws)]
        self.envelope_draw_cols: List[str] = [
            f'env_{n}' for n in range(self.n_draws)
        ]
        self.description = self._set_description()
        self.location_set_ids: List[int] = \
            [base_location_set_id] + aggregation_location_set_ids
        self.cause_set_ids = self._set_cause_set_ids()
        self.age_metadata = self._create_age_metadata()
        self.most_detailed_age_group_ids = self._create_most_detailed_age_group_ids()

        # Retrieve comparison version metadata for scatters.
        # Everything is None if scatter_version_id isn't passed in
        self.scatter_version_id = scatter_version_id
        (
            self.scatter_process_version_id,
            self.scatter_gbd_round_id,
            self.scatter_decomp_step
        ) = codcorrect_version.get_codcorrect_version_metadata(scatter_version_id)

        # Create parameter objects, eligible metadata, expected metadata.
        # Need to instantiate envelope, population, and life table parameter
        # objs before creating the process version row.
        round_args = {
            "gbd_round_id": self.gbd_round_id,
            "decomp_step": self.decomp_step
        }
        self._envelope_parameter = EnvelopeParameters(**round_args)
        self._life_table_parameter = LifeTableParameters(**round_args)
        self._population_parameter = PopulationParameters(
            location_set_id=self.base_location_set_id,
            **round_args
        )
        self._cause_parameters = self._create_cause_parameters()
        self._model_version_parameter = ModelVersionParameters(
            cause_parameter=self._cause_parameters[self.base_cause_set_id],
            process=self.GBD_PROCESS,
            test=self.test,
            **round_args
        )
        self._location_parameters = self._create_location_parameters()
        self.expected_metadata = self._get_expected_metadata()
        self._eligible_metadata = self._create_eligible_metadata()

        # Create scaling heirarchy (unvalidated)
        self.correction_hierarchy = base_cause_set.create_correction_hierarchy(
            base_cause_set_id, gbd_round_id, decomp_step
        )

        # Once all class members are present, we can begin making real changes
        self.parent_dir: str = self.create_parent_dir(self.version_id)

        # Insert the new cod/fauxcorrect version and process version
        self.create_new_cod_output_version_row()
        self.gbd_process_version_id: int = self.create_gbd_process_version()

        # If uploading to codcorrect, create new diagnostic version row
        if constants.DataBases.CODCORRECT in self.databases:
            codcorrect_version.create_new_diagnostic_version_row(self.version_id)

    @classmethod
    def create_parent_dir(cls, version_id: int,
                          process_name: Optional[str] = None):
        """
        Creates the base (parent) directory for the run, but does
        NOT generate the rest of the file structure.
        """
        if process_name:
            process = process_name
        else:
            process = cls.GBD_PROCESS
        return os.path.join(
            constants.FilePaths.ROOT_DIR,
            process, str(version_id)
        )

    @classmethod
    def recreate_from_cache(cls, cache_name: str):
        with open(cache_name, 'rb') as cache_file:
            inst = pickle.load(cache_file)
        return inst

    @classmethod
    def recreate_from_version_id(cls, version_id: int,
                                 process_name: Optional[str] = None):
        parent_dir = cls.create_parent_dir(version_id, process_name)
        cache_name = os.path.join(
            parent_dir,
            constants.FilePaths.PARAM_DIR,
            constants.FilePaths.PARAM_FILE
        )
        return cls.recreate_from_cache(cache_name)

    def cache_parameters(self, cache_name: Optional[str] = None) -> str:
        if not cache_name:
            cache_name = os.path.join(
                self.parent_dir,
                constants.FilePaths.PARAM_DIR,
                constants.FilePaths.PARAM_FILE
            )
        with open(cache_name, 'wb') as cache_file:
            pickle.dump(self, cache_file)
        return cache_name

    def create_gbd_process_version(self) -> int:
        metadata, version_note = self._create_process_version_info()
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
    def all_sex_ids(self) -> List[int]:
        return self.sex_ids + [gbd.sex.BOTH]

    @property
    def all_age_group_ids(self) -> List[int]:
        return (
            self.most_detailed_age_group_ids +
            gbd.GBD_COMPARE_AGES +
            [gbd.age.ALL_AGES]
        )

    @property
    def most_detailed_cause_ids(self) -> List[int]:
        return (
            self._cause_parameters[self.base_cause_set_id]
            .most_detailed_ids
        )

    @property
    def cause_ids_to_correct(self) -> List[int]:
        """
        Returns a list of the cause ids that go into
        the correction process of Cod/FauxCorrect,
        which is decided by model version type id.

        HIV, shocks, and imported case models do not
        go into the correction.
        """
        return self._eligible_metadata[
            ~self._eligible_metadata[
                constants.Columns.MODEL_VERSION_TYPE_ID
            ].isin(constants.ModelVersionTypeId.EXEMPT_TYPE_IDS)
        ].cause_id.drop_duplicates().tolist()

    @property
    def cause_ids(self) -> List[int]:
        return list({
            cause_id for cause_set in self.cause_set_ids
            for cause_id in self._cause_parameters[cause_set].cause_ids
        })

    @property
    def covid_cause_ids(self) -> List[int]:
        return self._eligible_metadata[
            self._eligible_metadata[constants.Columns.MODEL_VERSION_TYPE_ID].isin(
                [constants.ModelVersionTypeId.INDIRECT_COVID]
            )
        ].cause_id.drop_duplicates().tolist()

    @property
    def cause_rescaling_tree(self) -> hierarchies.tree.Tree:
        return self._cause_parameters[self.base_cause_set_id].tree

    @property
    def cause_aggregation_tree(self) -> None:
        raise NotImplementedError

    @property
    def envelope_version_id(self):
        return self._envelope_parameter.run_id

    @property
    def life_table_run_id(self):
        return self._life_table_parameter.life_table_run_id

    @property
    def tmrlt_run_id(self):
        return self._life_table_parameter.tmrlt_run_id
    

    @property
    def most_detailed_location_ids(self) -> List[int]:
        return (
            self._location_parameters[self.base_location_set_id]
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
        return self._location_parameters[self.base_location_set_id].tree

    @property
    def best_model_version_ids(self) -> List[int]:
        return self._model_version_parameter.best_ids

    @property
    def best_model_metadata(self):
        return self._model_version_parameter.best_metadata

    @property
    def population_version_id(self):
        return self._population_parameter.run_id

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
        return cause_set_id in self.cause_set_ids

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
                    self.most_detailed_age_group_ids,
                    age_group_id_start=model_metadata[age_start_column],
                    age_group_id_end=model_metadata[age_end_column],
                    gbd_round_id=self.gbd_round_id,
                    test=self.test

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

    def validate_model_versions(self) -> None:
        """
        Runs model version validations on the base cause set.
        """
        cause_violations_ok = (
            self.process != constants.GBD.Process.Name.CODCORRECT and not self.test
        )
        self._model_version_parameter.run_validations(
            self._cause_parameters[self.base_cause_set_id],
            self._eligible_metadata,
            age_violations_ok=True,
            cause_violations_ok=cause_violations_ok
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

    def cache_correction_hierarchy(self) -> None:
        cache_file = os.path.join(
            self.parent_dir,
            constants.FilePaths.INPUT_FILES_DIR,
            constants.FilePaths.CORRECTION_HIERARCHY
        )
        self.correction_hierarchy.to_csv(cache_file, index=False, encoding="utf8")

    def _set_description(self) -> str:
        if self.process == constants.GBD.Process.Name.FAUXCORRECT:
            proper_name = constants.GBD.Process.Name.FAUXCORRECT_PROPER
        elif self.process == constants.GBD.Process.Name.CODCORRECT:
            proper_name = constants.GBD.Process.Name.CODCORRECT_PROPER

        test_str = "test " if self.test else ""
        description = (
            f"{proper_name} v{self.version_id} {test_str}run on "
            f"{time.strftime('%b %d %Y')}"
        )

        return description

    def _set_cause_set_ids(self) -> List[int]:
        """
        Returns a list of all the cause set ids to be used in this run,
        where the first in the list is the base cause set.

        For tests, we're running barebones, without any cause sets to
        aggregate other than the base cause set
        """
        return [self.base_cause_set_id] + self.aggregation_cause_set_ids

    def _create_most_detailed_age_group_ids(self) -> List[int]:
        """
        Get the most detailed age group ids for the GBD round.

        Note:
            These are sorted by age group id, not age group start/end
        """
        return self.age_metadata.loc[
            self.age_metadata[constants.Columns.MOST_DETAILED] == 1,
            constants.Columns.AGE_GROUP_ID
        ].tolist()

    def _create_age_metadata(self) -> pd.DataFrame:
        """
        Returns age metadata for the run. Specific to the
        GBD round.
        """
        return db_queries.get_age_metadata(
            db_queries.api.internal.get_age_group_set(),
            self.gbd_round_id
        )

    def _create_cause_parameters(self) -> Dict[int, CauseParameters]:
        cause_parameters: Dict[int, CauseParameters] = {}

        for cause_set in self.cause_set_ids:
            cause_parameters[cause_set] = CauseParameters(
                cause_set_id=cause_set,
                gbd_round_id=self.gbd_round_id,
                decomp_step=self.decomp_step
            )
        return cause_parameters

    def _create_location_parameters(self) -> Dict[int, LocationParameters]:
        location_parameters: Dict[int, LocationParameters] = {}
        for loc_set in self.location_set_ids:
            location_parameters[loc_set] = LocationParameters(
                gbd_round_id=self.gbd_round_id,
                decomp_step=self.decomp_step,
                location_set_id=loc_set
            )
        return location_parameters

    def _create_process_version_info(
            self
    ) -> Tuple[Dict[int, int], str]:
        test_str = " (test)" if self.test else ""
        metadata: Dict[int, int] = {
            constants.GBD.MetadataTypeIds.CODCORRECT: self.version_id,
            constants.GBD.MetadataTypeIds.ENVELOPE: self.envelope_version_id,
            constants.GBD.MetadataTypeIds.POPULATION: (
                self.population_version_id
            ),
            constants.GBD.MetadataTypeIds.LIFE_TABLE_WITH_SHOCK: (
                self.life_table_run_id
            ),
            constants.GBD.MetadataTypeIds.TMRLT: (
                self.tmrlt_run_id
            )
        }

        version_note = constants.GBD.Process.VersionNote.CODCORRECT.format(
            version_id=self.version_id,
            test_str=test_str,
            population_version=self.population_version_id,
            envelope_version=self.envelope_version_id,
            life_table_version=self.life_table_run_id,
            tmrlt_version=self.tmrlt_run_id
        )
        return metadata, version_note

    def _create_eligible_metadata(self) -> pd.DataFrame:
        eligible_metadata = self._add_cause_metadata()
        nans = eligible_metadata.cause_age_start.isna()
        these_are_nans = eligible_metadata[nans]
        no_nans = eligible_metadata[~nans]
        inferred = (
            self._infer_age_restrictions_from_metadata(no_nans)
        )
        return pd.concat([inferred, these_are_nans], ignore_index=True)

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
            self._model_version_parameter.best_metadata,
            self.expected_metadata,
            on=[constants.Columns.CAUSE_ID, constants.Columns.SEX_ID],
            how='left'
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

        Example:
            Cause metadata expects the full age range (age group id 2 to 235,
            cause_age_start = 2, cause_age_end = 235). There's two model versions
            for this cause, covering two age ranges: age group id 2 to age group
            id 6 (early neonates to age 9) and age group id 7 to 235 (10+).

            cause_age_end for the young age model will be adjusted to 6 and
            cause_age_start for the older age model will be adjusted to 7

        This function will:
            Fix cause_age_start and cause_age_end for causes with multiple
            models by age.

        This function will not:
            Adjust the terminal age groups expected based on the age groups
            in the model version metadata. Ex: if age groups 2 - 235 are expected,
            if a model version has data for 6 - 235, cause_age_start will not be adjusted.


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
            df.apply(
                lambda row: row[constants.Columns.CAUSE_AGE_START] if
                row[constants.Columns.MODEL_VERSION_TYPE_ID] != 5 and
                (helpers.age_group_id_to_age_start(row[constants.Columns.AGE_START]) <
                 helpers.age_group_id_to_age_start(row[constants.Columns.CAUSE_AGE_START]) or
                 helpers.age_group_id_to_age_start(row[constants.Columns.AGE_START]) >
                 helpers.age_group_id_to_age_start(row[constants.Columns.CAUSE_AGE_START]) and
                 row[constants.Columns.RANK] == 1) else
                row[constants.Columns.AGE_START],
                axis=1
            )
        )
        df.loc[:, constants.Columns.CAUSE_AGE_END] = (
            df.apply(
                lambda row: row[constants.Columns.CAUSE_AGE_END] if
                row[constants.Columns.MODEL_VERSION_TYPE_ID] != 5 and
                (helpers.age_group_id_to_age_start(row[constants.Columns.AGE_END]) >
                 helpers.age_group_id_to_age_start(row[constants.Columns.CAUSE_AGE_END]) or
                 helpers.age_group_id_to_age_start(row[constants.Columns.AGE_END]) <
                 helpers.age_group_id_to_age_start(row[constants.Columns.CAUSE_AGE_END]) and
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
        if self.base_cause_set_id not in self._cause_parameters:
            raise RuntimeError(
                f"The _cause_parameters dictionary is missing a "
                f"CauseParameters object for cause_set_id = "
                f"{self.base_cause_set_id}. This cause set is "
                f"needed to determine expected input models.")

        cause_params = (
            self._cause_parameters[
                self.base_cause_set_id
            ]
        )
        cause_metadata = cause_params.metadata_dataframe

        # merge on hierarchy object and filter to causes we estimate
        # (i.e. causes for which modelers produce and upload results)
        cause_metadata = pd.merge(
            cause_metadata,
            cause_params.hierarchy[
                [
                    constants.Columns.CAUSE_ID,
                    constants.Columns.IS_ESTIMATE_COD
                ]
            ],
            how='left',
            on=constants.Columns.CAUSE_ID
        )

        # filter out causes where we don't expect models
        cause_metadata = cause_metadata.loc[
            (cause_metadata[constants.Columns.IS_ESTIMATE_COD] == 1)
        ]
        # add column with list of age_group_ids that fall within range of
        # cause_age_start cause_age_end for use in correct.py module
        cause_metadata[constants.Columns.CAUSE_AGES] = (
            cause_metadata.apply(
                lambda row: _get_age_range(
                    self.most_detailed_age_group_ids,
                    age_group_id_start=row[constants.Columns.CAUSE_AGE_START],
                    age_group_id_end=row[constants.Columns.CAUSE_AGE_END],
                    gbd_round_id=self.gbd_round_id,
                    test=self.test
                ),
                axis=1,
            )
        )
        return cause_metadata.drop(constants.Columns.IS_ESTIMATE_COD, axis=1)

    def start_model(self) -> None:
        raise NotImplementedError(
            "Not implemented for base MachineParameters class")

    def mark_best(self) -> None:
        raise NotImplementedError(
            "Not implemented for base MachineParameters class")

    def generate_project_directories(self):
        raise NotImplementedError(
            "Not implemented for base MachineParameters class")


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
    ADDITIONAL_CAUSE_SET_IDS: List[int] = [
        constants.CauseSetId.COMPUTATION,
        constants.CauseSetId.REPORTING_AGGREGATES
    ]

    def create_new_cod_output_version_row(self) -> None:
        codcorrect_version.create_new_cod_output_version_row(
            cod_output_version_id=self.cod_output_version_id,
            description=self.description,
            decomp_step_id=self.decomp_step_id,
            code_version=str(self.gbd_round_id),
            env_version=self.envelope_version_id
        )

        codcorrect_version.create_output_partition(
            cod_output_version_id=self.cod_output_version_id
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
            constants.FilePaths.SHOCKS_DIR,
            constants.FilePaths.COVID_SCALARS,
            constants.Scatters.PLOT_DIR.format(version_id=self.version_id)
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
        ] + [
            os.path.join(
                constants.FilePaths.COVID_SCALARS,
                constants.FilePaths.SUMMARY_DIR
            )
        ]
        if self.year_start_ids:
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
                    ["_".join([str(self.year_start_ids[i]),
                               str(self.year_end_ids[i])])
                     for i in range(len(self.year_start_ids))]
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


def recreate_from_process_and_version_id(
        version_id: int,
        machine_process: str
) -> MachineParameters:
    """
    Wrapper for subclass instantiation from a version id.
    We need an easy way to pass in the machine process
    ('codcorrect', 'fauxcorrect') and a version specific to that
    process and get back our machinery parameter set.

    Args:
        version_id: the version of the process to instantiate a
            machinery parameter object
        machine_process: which machinery to recreate parameters for.
            Either 'codcorrect' or 'fauxcorrect'

    Raises:
        ValueError if machine_process is not one of two options
    """
    if machine_process not in constants.GBD.Process.Name.OPTIONS:
        raise ValueError(
            f"'machine_process' must be in "
            f"{constants.GBD.Process.Name.OPTIONS}. "
            f'Received "{machine_process}".'
        )

    if machine_process == constants.GBD.Process.Name.CODCORRECT:
        return CoDCorrectParameters.recreate_from_version_id(
            version_id=version_id
        )
    elif machine_process == constants.GBD.Process.Name.FAUXCORRECT:
        return FauxCorrectParameters.recreate_from_version_id(
            version_id=version_id
        )


def _get_age_range(
        most_detailed_age_group_ids: List[int],
        age_group_id_start: int,
        age_group_id_end: int,
        gbd_round_id: int,
        test: bool = False
) -> List[int]:
    """
    From most detailed age groups, return a list of age group ids
    that fall within the range of age_group_id_start and age_group_id_end,
    inclusive.

    Method:
        Pull ages associated with age groups; determine which age groups
        are within range based on actual age, NOT age group id

        If it's a test run, we can't assume the age group ids given are
        valid in the current round. So if they are missing from the
        most detailed age groups for the given round, they're replaced
        by the first and/or last age groups (by age) in the current round.
    """
    age_metadata = db_queries.get_age_metadata(
        db_queries.api.internal.get_age_group_set(), gbd_round_id
    )

    # In a test, the age groups might not always match up
    # If that's the case, replace them safely. Otherwise
    # we can assume all the given age group ids exist in the
    # age hierarchy

    # Starting age group id is not in current hierarchy
    if test and not any(age_metadata.age_group_id.isin([age_group_id_start])):
        # replace with youngest age
        age_start = age_metadata.age_group_years_start.min()
    else:
        age_start = age_metadata.loc[
            age_metadata.age_group_id == age_group_id_start,
            'age_group_years_start'
        ].iat[0]

    # Ending age group id not in current hierarchy
    if test and not any(age_metadata.age_group_id.isin([age_group_id_end])):
        # replace with oldest age
        age_end = age_metadata.age_group_years_start.max()
    else:
        age_end = age_metadata.loc[
            age_metadata.age_group_id == age_group_id_end,
            'age_group_years_start'
        ].iat[0]

    age_groups_in_range = age_metadata.loc[
        (age_metadata.age_group_years_start >= age_start) &
        (age_metadata.age_group_years_start <= age_end),
        constants.Columns.AGE_GROUP_ID
    ].tolist()

    age_groups_in_range = [
        age_group_id for age_group_id in age_groups_in_range
        if age_group_id in most_detailed_age_group_ids
    ]

    return age_groups_in_range


def _get_code_version() -> str:
    """Returns the short git hash from the last commit."""
    return (
        subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD'])
            .decode('ascii')
            .strip()
    )
