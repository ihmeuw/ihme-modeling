import pandas as pd
import logging
from typing import List, Optional

from db_tools.ezfuncs import query
import gbd.constants as gbd

from fauxcorrect.parameters.cause import CauseParameters
from fauxcorrect.queries import queries
from fauxcorrect.utils import constants, exceptions as exc

class ModelVersionParameters:
    """
    Model version parameters represent all of the best models from CoD modeling
    that will be used as input for a CoD- or FauxCorrect run.

    A best model is one that is marked best and included in the provided
    gbd_round_id and decomposition step -- "iterative" models will also be
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

    NOTE: The methods that retrieve particular metadata rows do not copy the
          data before returning. Be very careful when transforming this data.
    """

    def __init__(
            self,
            gbd_round_id: int = gbd.GBD_ROUND_ID,
            decomp_step_id: int = gbd.decomp_step.TWO,
            process: str = constants.GBD.Process.Name.CODCORRECT
    ):
        """
        Create an instance of Model Version Parameters for a CoD or Faux-Correct
        run.

        Arguments:
             gbd_round_id (int): defaults to the current GBD round
             decomp_step_id (int): the specific step in GBD decomposition that
                 the model versions are associated.
             process (str): { 'codcorrect' | 'fauxcorrect' }, default
                 'codcorrect'.
        """
        self.gbd_round_id: int = gbd_round_id
        self.decomp_step_id: int = decomp_step_id
        self.process: str = process

        # Define our connection to cod database
        self.conn_def: str = constants.ConnectionDefinitions.COD

        # Initialize attributes derived from input arguments
        self._best_metadata: pd.DataFrame = self._get_best_metadata()
        self._best_ids: List[int] = self._get_best_ids()
        self._cause_ids: List[int] = self._get_cause_ids()

        self._decomp_exempt_metadata: pd.DataFrame = self._get_exempt_metadata()

    @property
    def best_ids(self) -> List[int]:
        return self._best_ids

    @property
    def best_metadata(self) -> pd.DataFrame:
        return self._best_metadata

    @property
    def exempt_metadata(self) -> pd.DataFrame:
        return self._decomp_exempt_metadata

    @property
    def cause_ids(self) -> List[int]:
        return self._cause_ids

    def _get_best_ids(self) -> List[int]:
        return self.best_metadata.loc[
            ~self.best_metadata.model_version_id.isnull(),
            constants.Columns.MODEL_VERSION_ID
        ].tolist()

    def _get_best_metadata(self) -> pd.DataFrame:
        """Returns the best model version metadata as a pd.DataFrame."""
        params = {
            'gbd': self.gbd_round_id,
            'decomp': self.decomp_step_id,
            'mvtids': constants.ModelVersionTypeId.ALL_TYPE_IDS
        }
        decomp_models = query(
            queries.ModelVersion.GET_BEST_METADATA,
            conn_def=self.conn_def,
            parameters=params
        )
        decomp_models = _assign_scale_column(decomp_models)
        # Pull in models that are exempt from decomp.
        exempt_models = query(
            queries.ModelVersion.GET_BEST_METADATA_BY_CAUSE,
            conn_def=self.conn_def,
            parameters={
                'gbd': self.gbd_round_id,
                'decomp': constants.Decomp.Step.ITERATIVE,
                'mvtids': [
                    constants.ModelVersionTypeId.CUSTOM,
                    constants.ModelVersionTypeId.HYBRID
                ],
                'causes': constants.Decomp.Causes.EXEMPT_CAUSE_IDS
            }
        )
        # Apply is_scaled flag according to machine process.
        if self.process == constants.GBD.Process.Name.CODCORRECT:
            exempt_models = _assign_scale_column(exempt_models)
        elif self.process == constants.GBD.Process.Name.FAUXCORRECT:
            exempt_models[constants.Columns.IS_SCALED] = False
        else:
            raise ValueError(
                f"Machine process not recognized. Process value must be "
                f"{constants.GBD.Process.Name.CODCORRECT} or "
                f"{constants.GBD.Process.Name.FAUXCORRECT}. Received "
                f"{self.process}.")
        # concatenate decomp models and exempt models together
        all_models = pd.concat(
            [decomp_models, exempt_models]).reset_index(drop=True)
        # remove iterative models if a numbered decomp_step_id model is
        # available
        all_models = self._remove_iterative(all_models)
        return all_models

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
        return self._best_metadata.query(
            'cause_id in @constants.Decomp.Causes.EXEMPT_CAUSE_IDS'
        ).reset_index(drop=True)

    def _get_cause_ids(self) -> List[int]:
        return self.best_metadata.cause_id.unique().tolist()

    def contains_model_version(self, model_version_id: int) -> bool:
        return model_version_id in self.best_ids

    def contains_cause_id(self, cause_id: int) -> bool:
        return cause_id in self.cause_ids

    def contains_sex_id(self, sex_id: int) -> bool:
        return sex_id in self.best_metadata.sex_id.unique()

    def get_metadata_dict_from_version_id(self,
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
            age_violations_ok: bool=True
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
        cause_violations_ok: bool=True
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
        eligible_metadata: pd.DataFrame
    ) -> None:
        """Run all of the validations together."""
        self.validate_no_duplicated_models()
        self.validate_no_overlapping_ages()
        self.validate_models_with_cause_metadata(cause_params)
        self.validate_no_missing_causes(eligible_metadata)


def _validate_no_overlapping_ages_in_models(cause_sex_type_specific_models):
    model_errors = []
    if len(cause_sex_type_specific_models) > 1:
        cause_sex_type_specific_models.sort_values(
            constants.Columns.AGE_START, inplace=True
        )
        cause_sex_type_specific_models.reset_index(inplace=True, drop=True)
        index = 0
        while index < max(cause_sex_type_specific_models.index):
            first_model = cause_sex_type_specific_models.loc[index]
            second_model = cause_sex_type_specific_models.loc[index + 1]
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


def _assign_scale_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sub-select out the model version types that should not be scaled (5, 6,
    & 7) and apply the appropriate flag to the 'is_scaled' column.
    """
    scaled_draws = df[~df.model_version_type_id.isin(
        constants.ModelVersionTypeId.EXEMPT_TYPE_IDS
    )]
    unscaled_draws = df[df.model_version_type_id.isin(
        constants.ModelVersionTypeId.EXEMPT_TYPE_IDS
    )]
    scaled_draws[constants.Columns.IS_SCALED] = True
    unscaled_draws[constants.Columns.IS_SCALED] = False
    return pd.concat([scaled_draws, unscaled_draws]).reset_index(drop=True)
