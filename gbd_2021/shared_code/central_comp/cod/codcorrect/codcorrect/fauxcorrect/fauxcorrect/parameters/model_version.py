import logging
from typing import List, Optional

import pandas as pd

from gbd.constants import decomp_step as ds
from gbd.decomp_step import (
    decomp_step_id_from_decomp_step,
    decomp_step_from_decomp_step_id
)
from db_tools.ezfuncs import query

from fauxcorrect.parameters.cause import CauseParameters
from fauxcorrect.queries import queries
from fauxcorrect.utils import (
    constants, exceptions as exc, helpers, step_control
)


class ModelVersionParameters:
    """
    Model version parameters represent all of the best models from CoD modeling
    that will be used as input for a CoD- or FauxCorrect run.

    A best model is one that is marked best and included in the provided
    gbd_round_id and decomposition step id -- "iterative" models will also be
    included.

    Properties
    ----------

        best_ids (List[int]): a list of all the model_version_ids.

        best_metadata (pd.DataFrame): a collection of all the relevant model
            metadata for the best models. Columns include:
                'model_version_id', 'cause_id', 'model_version_type_id',
                'is_best', 'sex_id', 'age_start', 'age_end', 'decomp_step_id'

        cause_ids (List[int]): a list of all the cause_ids associated with the
            best models versions for this parameter object.


    Methods
    -------

        get_metadata_dict_from_version_id - returns a record of model metadata
            from the provided model_version_id as a dictionary.

        get_metadata_from_cause_id - returns a pd.DataFrame of all model
            metadata for models associated with the provided cause_id.

        run_validations: (calls the following three methods)

        validate_models_with_cause_metadata - ensures that no model's metadata
            violates the invariants for its cause. Checks across sex_id and
            age_group_id.

        validate_no_duplicated_models - ensures that no two models exist for
            any cause_id, sex_id, age_group_id, model_version_type_id
            combination.

        validate_no_missing_causes(): validate that no causes are missing.

        validate_no_overlapping_ages - ensures that no two models overlap in
            age groups for any cause_id, sex_id, model_version_type_id
            combination.
    """

    def __init__(
            self,
            gbd_round_id: int,
            decomp_step: str,
            cause_parameter: CauseParameters,
            process: str,
            test: bool = False
    ):
        """
        Create an instance of Model Version Parameters for a CoD or Faux-Correct
        run.

        Arguments:
             gbd_round_id (int): GBD round for the run
             decomp_step_id (int): the specific step id in GBD that
                 the model versions are associated.
             process (str): { 'codcorrect' | 'fauxcorrect' }
             test (bool): True if this is a test run of Cod/FauxCorrect.
                If so, model version info from the previous step is used
                as we assume models from the previous step are vetted and
                good to go.
        """
        if process is None:
            raise RuntimeError("Process cannot be 'None'.")

        self.gbd_round_id: int = gbd_round_id
        self.decomp_step: str = decomp_step
        self.decomp_step_id: int = decomp_step_id_from_decomp_step(
            decomp_step, gbd_round_id)
        self.process: str = process
        self.test: bool = test

        # Overwrite with info from the previous decomp step as
        # we want to use model versions we know will work
        if self.test:
            self.gbd_round_id, self.decomp_step_id = \
                helpers.get_previous_modeling_round_and_step_id(
                    self.decomp_step_id)

        # Define our connection to cod database
        self.conn_def: str = constants.ConnectionDefinitions.COD

        # Initialize attributes derived from input arguments.
        # We only want metadata on causes that are is_estimate_cod = 1
        # in the base cause set
        self.best_metadata: pd.DataFrame = self._get_best_metadata(
            cause_ids=cause_parameter.hierarchy[
                (cause_parameter.hierarchy.is_estimate_cod == 1)
            ].cause_id.unique().tolist()
        )
        self.best_ids: List[int] = self._get_best_ids()
        self.cause_ids: List[int] = self._get_cause_ids()
        self.decomp_exempt_metadata: pd.DataFrame = self._get_exempt_metadata()

    def _get_best_ids(self) -> List[int]:
        return self.best_metadata.loc[
            ~self.best_metadata.model_version_id.isnull(),
            constants.Columns.MODEL_VERSION_ID
        ].tolist()

    def _get_best_metadata(self, cause_ids: List[int]) -> pd.DataFrame:
        """
        Returns the best model version metadata as a pd.DataFrame.

        ALL available best models are pulled and then subsetted
        only only causes in the cause_ids list.
        """
        decomp_models = query(
            queries.ModelVersion.GET_BEST_METADATA,
            conn_def=self.conn_def,
            parameters={
                'gbd': self.gbd_round_id,
                'decomp': self.decomp_step_id,
                'mvtids': constants.ModelVersionTypeId.NON_SHOCK_TYPE_IDS
            }
        )
        _assign_scale_column(decomp_models)

        # Pull in shock models, which live in iterative
        shock_models = query(
            queries.ModelVersion.GET_BEST_METADATA,
            conn_def=self.conn_def,
            parameters={
                'gbd': self.gbd_round_id,
                'decomp': decomp_step_id_from_decomp_step(ds.ITERATIVE, self.gbd_round_id),
                'mvtids': [
                    constants.ModelVersionTypeId.SHOCKS
                ]
            }
        )
        _assign_scale_column(shock_models)

        # Pull in indirect COVID models, which live in step 2 for tests but iterative normally
        indirect_covid_step = ds.TWO if self.test else ds.ITERATIVE
        indirect_covid_models = query(
            queries.ModelVersion.GET_BEST_METADATA,
            conn_def=self.conn_def,
            parameters={
                'gbd': self.gbd_round_id,
                'decomp': decomp_step_id_from_decomp_step(
                    indirect_covid_step, self.gbd_round_id
                ),
                'mvtids': [
                    constants.ModelVersionTypeId.INDIRECT_COVID
                ]
            }
        )
        _assign_scale_column(indirect_covid_models)

        # Pull in models that are exempt from decomp.
        exempt_models = query(
            queries.ModelVersion.GET_BEST_METADATA_BY_CAUSE,
            conn_def=self.conn_def,
            parameters={
                'gbd': self.gbd_round_id,
                'decomp': decomp_step_id_from_decomp_step(
                    ds.ITERATIVE, gbd_round_id=self.gbd_round_id),
                'mvtids': [
                    constants.ModelVersionTypeId.CUSTOM,
                    constants.ModelVersionTypeId.HYBRID
                ],
                # can't pass in empty list, so if there's no
                # causes in iterative, pass in impossible value
                'causes': step_control.get_iterative_cause_ids(
                    self.gbd_round_id,
                    decomp_step_from_decomp_step_id(
                        self.decomp_step_id)
                ) or [-1]
            }
        )
        
        _assign_scale_column(exempt_models)

        # concatenate decomp models, shock models, exempt models together
        # Filter out empty dataframes so we preserve column type
        models_to_append = [
            df for df in [decomp_models, shock_models, indirect_covid_models, exempt_models]
            if not df.empty
        ]
        all_models = pd.concat(
            models_to_append, sort=False
        ).reset_index(drop=True)

        # remove iterative models if a numbered decomp_step_id model is
        # available
        all_models = self._remove_iterative(all_models)
        cause_set_only = all_models[all_models.cause_id.isin(cause_ids)]
        return cause_set_only

    def _remove_iterative(self, all_models: pd.DataFrame) -> pd.DataFrame:
        decomp_and_iterative = all_models.duplicated(
            keep=False, subset=constants.Columns.UNIQUE_COLUMNS)
        keep = ~decomp_and_iterative
        has_decomp_model = (
            (decomp_and_iterative) &
            (all_models.decomp_step_id == self.decomp_step_id)
        )
        return all_models.loc[keep | has_decomp_model].reset_index(drop=True)

    def _get_exempt_metadata(self) -> pd.DataFrame:
        exempt_cause_ids = step_control.get_iterative_cause_ids(
            self.gbd_round_id,
            decomp_step_from_decomp_step_id(self.decomp_step_id))
        return self.best_metadata.query(
            'cause_id in @exempt_cause_ids'
        ).reset_index(drop=True)

    def _get_cause_ids(self) -> List[int]:
        return self.best_metadata.cause_id.unique().tolist()

    def contains_model_version(self, model_version_id: int) -> bool:
        return model_version_id in self.best_ids

    def contains_cause_id(self, cause_id: int) -> bool:
        return cause_id in self.cause_ids

    def contains_sex_id(self, sex_id: int) -> bool:
        return sex_id in self.best_metadata.sex_id.unique()

    def get_metadata_dict_from_version_id(
            self,
            model_version_id: int,
            eligible_metadata: Optional[pd.DataFrame] = None
    ) -> dict:
        """
        Returns a single row of model metadata from the provided
        model_version_id as a dictionary.

        Arguments:
            model_version_id (int)
            eligible_metadata (Optional[pd.DataFrame])

        Returns:
            A dictionary representing a single record of model metadata.

        Raises:
            RuntimeError if the provided model_version_id is not in our list of
            best models.
        """
        if not self.contains_model_version(model_version_id):
            raise RuntimeError(
                f"model_version_id: {model_version_id} is not one of the best "
                f"models for this {self.process.capitalize()} run."
            )
        if eligible_metadata is None:
            metadata = self.best_metadata.query(
                'model_version_id == @model_version_id'
            )
        else:
            metadata = eligible_metadata.query(
                'model_version_id == @model_version_id'
            )
        return metadata.to_dict(orient='records')[0]

    def get_metadata_from_version_id(
            self,
            model_version_id: int
    ) -> pd.DataFrame:
        if not self.contains_model_version(model_version_id):
            raise RuntimeError(
                f"model_version_id: {model_version_id} is not one of the best "
                f"models for this {self.process.capitalize()} run."
            )
        return self.best_metadata.query(
            'model_version_id == @model_version_id'
        ).reset_index(drop=True)

    def get_cause_id_from_version_id(self, model_version_id: int) -> int:
        return self.get_metadata_dict_from_version_id(model_version_id).get(
            constants.Columns.CAUSE_ID
        )

    def get_metadata_from_cause_id(self, cause_id: int) -> pd.DataFrame:
        """
        Returns all rows of model metadata from the provided cause_id.

        Arguments:
            cause_id (int)

        Returns:
            A dataframe of model version metadata associated with a single
            cause id.

        Raises:
            RuntimeError if the provided cause_id is not in our model metadata.
        """
        if not self.contains_cause_id(cause_id):
            raise RuntimeError(
                f"cause_id: {cause_id} was not found in the model metadata. "
                f"Valid causes are {self.cause_ids}."
            )
        return self.best_metadata.query('cause_id == @cause_id')

    def get_metadata_by_sex_id(self, sex_id: int) -> pd.DataFrame:
        """
        Returns all rows of model metadata associated with the provided sex_id.

        Arguments:
            sex_id (int)

        Returns:
            A dataframe of model version metadata associated with a single
            sex id.

        Raises:
            RuntimeError if the provided sex_id is not in our model metadata.

        """
        if not self.contains_sex_id(sex_id):
            raise RuntimeError(
                f"sex_id: {sex_id} was not found in the model metadata. "
                f"Valid sexes are {self.best_metadata.sex_id.unique()}."
            )
        return self.best_metadata.query('sex_id == @sex_id')

    def validate_models_with_cause_metadata(
            self,
            cause_params: CauseParameters,
            age_violations_ok: bool = True
    ) -> None:
        """
        Validates the age group ids and sex ids for each model against their
        respective cause's metadata.

        Raises:
            InvalidModelVersionParameters if any model versions disagree with
            their respective cause's metadata in age_start, age_end, or sex.

        """
        logging.info(
            "Validating best model version ids against their respective "
            "cause metadata."
        )
        model_errors = []
        for model_version in self.best_ids:
            model_metadata = self.get_metadata_dict_from_version_id(
                model_version
            )
            try:
                cause_metadata = cause_params.get_cause_specific_metadata(
                    model_metadata.get(constants.Columns.CAUSE_ID)
                )
            except RuntimeError:
                continue
            if (model_metadata[constants.Columns.AGE_START] <
                    cause_metadata[constants.Columns.AGE_START]):
                lower_age_error = (
                    "Invalid model version found:\nmodel version: "
                    f"{model_version} has an age_group_id smaller than the "
                    "valid age groups for cause_id "
                    f"{model_metadata[constants.Columns.CAUSE_ID]}."
                )
                if age_violations_ok:
                    lower_age_error += (
                        " Additional age groups will be zeroed out."
                    )
                    logging.info(lower_age_error)
                else:
                    logging.error(lower_age_error)
                    model_errors.append(model_version)

            if (model_metadata[constants.Columns.AGE_END] >
                    cause_metadata[constants.Columns.AGE_END]):
                upper_age_error = (
                    "Invalid model version found:\nmodel version: "
                    f"{model_version} has an age_group_id larger than the "
                    "valid age groups for cause_id "
                    f"{model_metadata[constants.Columns.CAUSE_ID]}."
                )
                if age_violations_ok:
                    upper_age_error += (
                        " Additional age groups will be zeroed out"
                    )
                    logging.info(upper_age_error)
                else:
                    logging.error(upper_age_error)
                    model_errors.append(model_version)

            if (model_metadata[constants.Columns.SEX_ID] not in
                    cause_metadata[constants.Columns.SEX_ID]):
                logging.error(
                    "Invalid model version found:\nmodel version: "
                    f"{model_version} has a restricted sex_id for cause_id "
                    f"{model_metadata[constants.Columns.CAUSE_ID]}."
                )
                model_errors.append(model_version)

        if model_errors:
            raise exc.InvalidModelVersionParameters(
                "Invalid model versions found. The following models contain "
                f"invalid ages or an invalid sex:\n{model_errors}."
            )

        logging.info("Validation against cause metadata successful.")

    def validate_no_duplicated_models(self) -> None:
        """
        Checks for duplicated model_versions across cause, sex, age_start,
        age_end, and model_version_type_id.

        Raises:
            InvalidModelVersionParameters if there is any duplication across
            models for cause, sex, age_start, age_end, and model version type.
        """
        logging.info(
            "Validating no duplicated models for each cause, sex, age start, "
            "age end, and model version type."
        )
        duplicate_df = self.best_metadata[
            self.best_metadata.duplicated(
                subset=constants.Columns.UNIQUE_COLUMNS)
        ]
        if not duplicate_df.empty:
            logging.error(
                "Invalid duplication found in model version metadata for the "
                f"following models: {duplicate_df.model_version_id.tolist()}."
            )
            raise exc.InvalidModelVersionParameters(
                "Invalid duplication found in model version metadata for the "
                f"following models: {duplicate_df.model_version_id.tolist()}."
            )
        logging.info("Validation for duplicate models successful.")

    def validate_no_missing_causes(
            self,
            eligible: pd.DataFrame,
            cause_violations_ok: bool = True
    ) -> None:
        missing_models = (
            eligible[eligible.model_version_id.isnull()][
                [constants.Columns.CAUSE_ID, constants.Columns.SEX_ID]
            ]
        )
        if not missing_models.empty:
            txt = (
                f"There are missing models. Model version metadata do not "
                f"match the full set of expected models as defined by the "
                f"cause hierarchies. Missing models:\n{missing_models}"
            )
            if not cause_violations_ok:
                raise exc.InvalidModelVersionParameters(txt)
            else:
                logging.info(txt)

    def validate_no_overlapping_ages(self) -> None:
        """
        Validates our model versions to ensure that all age groups are distinct
        if they share cause_id, sex_id, and model_version_type_id.

        Raises:
            InvalidModelVersionParameters if there is overlap in age groups for
            any two models.
        """
        logging.info(
            "Validating no overlapping age groups for each cause, sex, model "
            "version type."
        )

        # iterate over the causes and sex_id and sub-select the model rows.
        model_groups = self.best_metadata.groupby([
            constants.Columns.CAUSE_ID,
            constants.Columns.SEX_ID,
            constants.Columns.MODEL_VERSION_TYPE_ID
        ])
        model_groups.apply(
            _validate_no_overlapping_ages_in_models
        )
        logging.info("Validation for overlapping ages successful.")

    def run_validations(
            self,
            cause_params: CauseParameters,
            eligible_metadata: pd.DataFrame,
            age_violations_ok: bool = True,
            cause_violations_ok: bool = True
    ) -> None:
        """Run all of the validations together."""
        self.validate_no_duplicated_models()
        self.validate_no_overlapping_ages()
        self.validate_models_with_cause_metadata(
            cause_params, age_violations_ok)
        self.validate_no_missing_causes(
            eligible_metadata, cause_violations_ok)


def _validate_no_overlapping_ages_in_models(model_metadata):
    """
    Validate that causes with multiple model versions do not have age groups
    that overlap between their multiple model versions.

    Args:
        model_metadata: cause and sex-specific model versions. Must have
        columns 'age_start' and 'age_end' which are age_group_ids
    """
    # convert age start/end, which are age group ids, to the more comparable
    # age year start and end
    model_metadata[constants.Columns.AGE_START] = \
        model_metadata[constants.Columns.AGE_START].apply(
            helpers.age_group_id_to_age_start
    )
    model_metadata[constants.Columns.AGE_END] = \
        model_metadata[constants.Columns.AGE_END].apply(
            helpers.age_group_id_to_age_start
    )

    model_errors = []
    if len(model_metadata) > 1:
        # Sort by age_start, smallest first.
        model_metadata.sort_values(
            constants.Columns.AGE_START, inplace=True
        )
        model_metadata.reset_index(inplace=True, drop=True)
        index = 0
        while index < max(model_metadata.index):
            first_model = model_metadata.loc[index]
            second_model = model_metadata.loc[index + 1]
            if first_model.age_end > second_model.age_start:
                model_errors.append(
                    (first_model.model_version_id,
                     second_model.model_version_id)
                )
            index += 1
    if model_errors:
        logging.error(
            "Invalid age overlap found in the following model version pairs:\n"
            f"{model_errors}."
        )
        raise exc.InvalidModelVersionParameters(
            "Invalid age overlap found in the following model version pairs: "
            f"{model_errors}."
        )


def _assign_scale_column(df: pd.DataFrame) -> None:
    """
    Sub-select out the model version types that should not be scaled (5, 6,
    & 7) and apply the appropriate flag to the 'is_scaled' column.

    Mutates original dataframe.
    """
    if df.empty:
        return

    unscaled_draws = df.model_version_type_id.isin(
        constants.ModelVersionTypeId.EXEMPT_TYPE_IDS
    )

    df[constants.Columns.IS_SCALED] = True
    df.loc[unscaled_draws, constants.Columns.IS_SCALED] = False

    df[constants.Columns.IS_SCALED] = df[constants.Columns.IS_SCALED].astype(
        bool)
