import os
import subprocess
import time
from typing import Any, Dict, List, Optional, Tuple, Union
import pickle

import pandas as pd

import db_queries
from gbd import release
import gbd.constants as gbd
import gbd_outputs_versions
import hierarchies.tree

from codcorrect.lib.parameters import base_cause_set
from codcorrect.legacy.parameters.cause import CauseParameters
from codcorrect.legacy.parameters.envelope import EnvelopeParameters
from codcorrect.legacy.parameters.lifetable import LifeTableParameters
from codcorrect.legacy.parameters.location import LocationParameters
from codcorrect.legacy.parameters.model_version import ModelVersionParameters
from codcorrect.legacy.parameters.population import PopulationParameters
from codcorrect.lib import db
from codcorrect.legacy.utils import constants
from codcorrect.lib.utils import files, helpers


class MachineParameters:

    """
    Single object for managing a central deaths machinery run for
    CoDCorrect.

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
            from the results of correction from the base cause set.

        release_id (int): the release id of the run.

        most_detailed_cause_ids (List[int]): a list of the most detailed cause
            ids from the cause_set_id.

        cause_ids (List[int]): a list of all cause ids from the cause_set_id.

        cause_rescaling_tree (hierarchies.tree.Tree): the tree representing the
            codcorrect cause hierarchy; used to scale causes to the All Cause
            Mortality Envelope.

        most_detailed_location_ids (List[int]): the list of most detailed
            location ids from the standard location computation set.

        location_ids (List[int]): returns a list of every location_id from all
            the location sets used in this run.

        location_aggregation_tree (hierarchies.tree.Tree): returns the tree
            representation of the standard location hierarchy.

        best_model_version_ids (List[int]): returns a list of all the model
            version ids that were marked best for this run of CoDCorrect.

        scatter_version_id: CodCorrect run id to compare
            this run against via scatters.

        correction_hierarchy: The correction (scaling) hierarchy used for
            applying the CoDCorrect correction. Generated from the base cause set

        file_system: File system abstraction that handles logic regarding directories,
            read/writes, etc during the CoDCorrect run.

    Methods:
        cause_tree_from_cause_set_id(): given a cause_set_id, returns the tree
            representation of the cause hierarchy. Raises an exception if the
            cause_set_id is not valid for the process.

        contains_cause_set_id(): true or false, is the provided cause_set_id
            contained in this CodCorrect version?

        location_tree_from_location_set_id(): given a location_set_id, returns
            the tree representation of the location hierarchy. Raises an
            exception if the location_set_id is not valid for the process.

        contains_location_set_id(): true or false, is the provided
            location_set_id contained in this CoDCorrect version?

        contains_model_version_id(): true or false, does the CoDCorrect
            version contain the provided model_version_id?

        models_contain_cause_id(): true or false, do the CoDCorrect version
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
            the gbd database for a CoDCorrect run. These include a process
            version id and process version metadata.

    """
    GBD_PROCESS_ID: int = constants.GBD.Process.Id.CODCORRECT
    GBD_PROCESS: str = constants.GBD.Process.Name.CODCORRECT
    GBD_UPLOAD_TABLE: str = constants.GBD.DataBase.CODCORRECT_TABLE
    DIAGNOSTIC_UPLOAD_TABLE: str = constants.Diagnostics.DataBase.TABLE
    ADDITIONAL_CAUSE_SET_IDS: List[int] = [
        constants.CauseSetId.COMPUTATION,
        constants.CauseSetId.REPORTING_AGGREGATES
    ]

    def __init__(
            self,
            release_id: int,
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
            year_start_ids: Optional[List[int]] = None,
            year_end_ids: Optional[List[int]] = None,
            scatter_version_id: Optional[int] = None,
    ):

        # Retrieve a CodCorrect version and a COD output version for this run
        self.version_id: int = db.get_new_codcorrect_version_id()
        self.cod_output_version_id: int = db.get_new_cod_output_version_id()

        # fill in input parameters
        self.release_id: int = release_id
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
            self.scatter_release_id
        ) = db.get_codcorrect_version_metadata(scatter_version_id)

        # Create parameter objects, eligible metadata, expected metadata.
        # Need to instantiate envelope, population, and life table parameter
        # objs before creating the process version row.
        self._envelope_parameter = EnvelopeParameters(release_id)
        self._life_table_parameter = LifeTableParameters(release_id)
        self._population_parameter = PopulationParameters(
            location_set_id=self.base_location_set_id,
            release_id=release_id
        )
        self._cause_parameters = self._create_cause_parameters()
        self._model_version_parameter = ModelVersionParameters(
            cause_parameter=self._cause_parameters[self.base_cause_set_id],
            release_id=release_id
        )
        self._location_parameters = self._create_location_parameters()
        self.expected_metadata = self._get_expected_metadata()
        self._eligible_metadata = self._create_eligible_metadata()

        # Create scaling heirarchy
        self.correction_hierarchy = base_cause_set.create_correction_hierarchy(
            base_cause_set_id, release_id
        )

        # Set up file system variable but don't create any directories
        self.parent_dir: str = self.get_parent_dir(self.version_id)
        self.file_system = files.FileSystem(
            self.parent_dir, self.year_ids, self.year_start_ids, self.year_end_ids,
        )

        # Once all class members are present, we can begin making real changes
        # Insert the new codcorrect version + metadata, create GBD process version
        db.create_new_cod_output_version_row(
            cod_output_version_id=self.cod_output_version_id,
            description=self.description,
            release_id=self.release_id,
            code_version=str(self.release_id),
            env_version=self.envelope_version_id
        )
        db.create_cod_output_version_metadata(
            cod_output_version_id=self.cod_output_version_id,
            codcorrect_version_id=self.version_id
        )

        # Create GBD process version after creating CoD output version as the validations
        # for the former depend on the latter
        self.gbd_process_version_id: int = self.create_gbd_process_version()

        # If uploading to codcorrect, create new diagnostic version row
        if constants.DataBases.CODCORRECT in self.databases:
            db.create_new_diagnostic_version_row(self.version_id)

    @classmethod
    def load(cls, version_id: int, root_path: str = files.ROOT) -> "MachineParameters":
        """Recreated cached MachineParameters from a CodCorrect run.

        Expected to be called without any knowledge of the file system, hence
        this is a static method that does not require an instance of FileSystem.
        """
        file_path = files.PARAMETERS_FILE_FORMAT.format(root_path=root_path, version_id=version_id)
        with open(file_path, "rb") as cache_file:
            parameters = pickle.load(cache_file)
        return parameters

    def get_parent_dir(self, version_id: int):
        """
        Returns the base (parent) directory for the run, but does
        NOT generate and directories
        """
        return str(os.path.join(files.ROOT, str(version_id)))

    def cache_parameters(self) -> None:
        """Cache CodCorrect parameters via caching this class itself."""
        file_path = self.file_system.get_file_path(files.PARAMETERS) / "parameters.pkl"
        with open(file_path, "wb") as cache_file:
            pickle.dump(self, cache_file)

    def create_gbd_process_version(self) -> int:
        metadata, version_note = self._create_process_version_info()
        process_version = gbd_outputs_versions.GBDProcessVersion.add_new_version(
            gbd_process_id=self.GBD_PROCESS_ID,
            gbd_process_version_note=version_note,
            code_version=_get_code_version(),
            release_id=self.release_id,
            metadata=metadata
        )
        return process_version.gbd_process_version_id

    @property
    def all_sex_ids(self) -> List[int]:
        return self.sex_ids + [gbd.sex.BOTH]

    @property
    def aggregate_age_group_ids(self) -> List[int]:
        """Returns the aggregate age group ids for the run, computed for summaries."""
        return (
            gbd.GBD_COMPARE_AGES +
            [gbd.age.ALL_AGES, gbd.age.AGE_STANDARDIZED] +
            constants.Ages.END_OF_ROUND_AGE_GROUPS
        )

    @property
    def all_age_group_ids(self) -> List[int]:
        return (
            self.most_detailed_age_group_ids +
            self.aggregate_age_group_ids
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
        the correction process of CoDCorrect,
        which is decided by model version type id.

        Shocks and imported case models do not
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
    def correction_exclusion_map(self):
        return self._cause_parameters[self.base_cause_set_id].correction_exclusion_map

    @property
    def cause_rescaling_tree(self) -> hierarchies.tree.Tree:
        return self._cause_parameters[self.base_cause_set_id].tree

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
                f"this version: version {self.version_id}."
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
                f"set in this version: version {self.version_id}."
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
            collection of best models for this CoDCorrect run.
        """
        if not self.contains_model_version_id(model_version_id):
            raise RuntimeError(
                f"Model version: {model_version_id} not found in best models "
                f"for v{self.version_id} run."
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
                    release_id=self.release_id,
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
        cause_violations_ok = not self.test
        self._model_version_parameter.run_validations(
            self._cause_parameters[self.base_cause_set_id],
            self._eligible_metadata,
            age_violations_ok=True,
            cause_violations_ok=cause_violations_ok
        )

    def cache_input_files(self) -> None:
        """Cache some of the input files: correction hierarchy, input model report."""
        input_models = self._eligible_metadata.copy()
        input_models = input_models[
            constants.Columns.MODEL_TRACKER_COLS
        ].sort_values(constants.Columns.MODEL_TRACKER_SORTBY_COLS)

        self.file_system.cache_input_model_tracking_report(self.version_id, input_models)
        self.file_system.cache_correction_hierarchy(self.correction_hierarchy)

    def _set_description(self) -> str:
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
        Returns age metadata for the run.
        """
        return db_queries.get_age_metadata(release_id=self.release_id)

    def _create_cause_parameters(self) -> Dict[int, CauseParameters]:
        cause_parameters: Dict[int, CauseParameters] = {}

        for cause_set in self.cause_set_ids:
            cause_parameters[cause_set] = CauseParameters(
                cause_set_id=cause_set,
                release_id=self.release_id,
            )
        return cause_parameters

    def _create_location_parameters(self) -> Dict[int, LocationParameters]:
        location_parameters: Dict[int, LocationParameters] = {}
        for loc_set in self.location_set_ids:
            location_parameters[loc_set] = LocationParameters(
                release_id=self.release_id,
                location_set_id=loc_set
            )
        return location_parameters

    def _create_process_version_info(
            self
    ) -> Tuple[Dict[int, Any], str]:
        """Create process version info, including GBD metadata and a process version note.
        """
        test_str = " (test)" if self.test else ""
        metadata: Dict[int, Union[int, List[int], Dict[int, List[int]]]] = {
            gbd.gbd_metadata_type.CODCORRECT: self.version_id,
            gbd.gbd_metadata_type.ENVELOPE: self.envelope_version_id,
            gbd.gbd_metadata_type.POPULATION: self.population_version_id,
            gbd.gbd_metadata_type.LIFE_TABLE_WITH_SHOCKS: self.life_table_run_id,
            gbd.gbd_metadata_type.TMRLT: self.tmrlt_run_id,
            gbd.gbd_metadata_type.AGE_GROUP_IDS: {
                measure_id: self.all_age_group_ids for measure_id in self.measure_ids
            },
            gbd.gbd_metadata_type.YEAR_IDS: self.year_ids,
            gbd.gbd_metadata_type.N_DRAWS: self.n_draws,
            gbd.gbd_metadata_type.LOCATION_SET_VERSION_ID: [
                lp.set_version_id for _, lp in self._location_parameters.items()
            ],
            gbd.gbd_metadata_type.CAUSE_SET_VERSION_ID: [
                 cp.set_version_id for _, cp in self._cause_parameters.items()
            ],
            gbd.gbd_metadata_type.MEASURE_IDS: self.measure_ids,
            gbd.gbd_metadata_type.WITH_HIV: constants.MortalityEnvelope.WITH_HIV,
        }

        # If given percent change years, add them to metadata
        if self.year_start_ids and self.year_end_ids:
            metadata[gbd.gbd_metadata_type.YEAR_START_IDS] = self.year_start_ids
            metadata[gbd.gbd_metadata_type.YEAR_END_IDS] = self.year_end_ids

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
        # IC models have age restrictions but we know that there
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
                    release_id=self.release_id,
                    test=self.test
                ),
                axis=1,
            )
        )
        return cause_metadata.drop(constants.Columns.IS_ESTIMATE_COD, axis=1)


def _get_age_range(
        most_detailed_age_group_ids: List[int],
        age_group_id_start: int,
        age_group_id_end: int,
        release_id: int,
        test: bool = False
) -> List[int]:
    """
    From most detailed age groups, return a list of age group ids
    that fall within the range of age_group_id_start and age_group_id_end,
    inclusive.

    Method:
        Pull ages associated with age groups; determine which age groups
        are within range based on actual age, NOT age group id
    """
    age_metadata = db_queries.get_age_metadata(release_id=release_id)

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
        subprocess.check_output(['git', 'rev-parse', 'HEAD'])
            .decode('ascii')
            .strip()
    )
