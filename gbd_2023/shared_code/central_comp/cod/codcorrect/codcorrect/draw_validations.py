import numpy as np
import pandas as pd

from get_draws.api import get_draws

from codcorrect.lib import db
from codcorrect.lib.workflow import task_templates as tt
from codcorrect.legacy.parameters.machinery import MachineParameters
from codcorrect.legacy.utils import constants
from codcorrect.lib.utils import exceptions as exc


def read_input_draws(
        machine_parameters: MachineParameters,
        model_version_id: int
) -> pd.DataFrame:
    """
    Return all input draws using get_draws.api.get_draws.

    Arguments:
        machine_parameters (MachineParameters)
        model_version_id (int): the unique version id of the model
            results to read.

    Returns:
        pd.DataFrame containing all the draws for the provided model_version_id
        and cause_id.

    Raises:
        InputDrawsEmpty if no rows are returned from the get_draws call.
    """
    # prepare parameters
    downsample = machine_parameters.n_draws < constants.Draws.MAX_DRAWS
    cause_id = machine_parameters.get_cause_from_model_version_id(
        model_version_id=model_version_id
    )

    args = {
        constants.Draws.GBD_ID: cause_id,
        constants.Draws.SOURCE_PARAM: constants.Draws.SOURCE,
        constants.Draws.GBD_ID_TYPE: constants.Columns.CAUSE_ID,
        constants.Draws.VERSION_ID: model_version_id,
        constants.Draws.RELEASE_ID: machine_parameters.release_id,
        constants.Draws.DOWNSAMPLE: downsample,
        constants.Draws.N_DRAWS_PARAM: machine_parameters.n_draws,
        constants.Draws.YEAR_ID: machine_parameters.year_ids,
        constants.Draws.NUM_WORKERS: tt.Validate.compute_resources[constants.Jobmon.NUM_CORES]
    }
    # get_draws currently raises an error if the n_draws argument is
    # called but downsample is False. Pop the n_draws arg if downsample is
    # False
    if not downsample:
        del args[constants.Draws.N_DRAWS_PARAM]

    # retrive draws according to parameters
    draws = get_draws(**args)
    if draws.empty:
        raise exc.InputDrawsEmpty(
            "No draws returned from calling get_draws with the following "
            f"arguments: cause_id: {cause_id}, version_id: {model_version_id},"
            " source: 'codem', gbd_id_type: 'cause_id'."
        )
    draws = pd.merge(
        draws,
        machine_parameters.get_metadata_from_model_version_id(
            model_version_id=model_version_id
        ),
        on=[constants.Columns.SEX_ID, constants.Columns.CAUSE_ID],
        how='left'
    )
    return draws[
        constants.Columns.KEEP_VALIDATION + machine_parameters.draw_cols]


def validate_draws(
        params: MachineParameters,
        model_version_id: int,
        draws: pd.DataFrame,
        age_violations_ok: bool = True
) -> pd.DataFrame:
    """
    Validates the draws against the superset of demographic parameters as
    defined by the model version's metadata. Additional demographics that exist
    in the draws are excluded from the return draws.

    Some models contain less age groups than their cause metadata expects.
    The age_violations_ok argument determines whether or not these models
    pass validations.

    Arguments:
        params (MachineParameter): global parameter object, unique to this
            specific CoDCorrect run.
        model_version_id (int): the model version id validated in this script.
        draws (pd.DataFrame): all draws from the model version.
        age_violations_ok (bool): if True, allows machinery run to proceed when
            age groups in the draws match expected ages for the model
            version but do not match expected ages for the associated cause
    """
    try:
        draws = _merge_required_indices(
            params,
            model_version_id,
            constants.Columns.CAUSE_AGE_START,
            constants.Columns.CAUSE_AGE_END,
            draws
        )
    except exc.DrawsMissingException as e:
        draws = _merge_required_indices(
            params,
            model_version_id,
            constants.Columns.AGE_START,
            constants.Columns.AGE_END,
            draws
        )
        if not age_violations_ok:
            raise e
    # prune draws and retain only relevant columns
    return draws[
        constants.Columns.KEEP_VALIDATION + params.draw_cols
    ].reset_index(drop=True)


def _merge_required_indices(
        params: MachineParameters,
        model_version_id: int,
        age_start_column: str,
        age_end_column: str,
        draws: pd.DataFrame
) -> pd.DataFrame:
    """
    Merges draws with the superset of demographic parameters as
    defined by the model version's metadata. Additional demographics that exist
    in the draws are excluded from the return draws.

    Arguments:
        params (MachineParameter): global parameter object, unique to this
            specific machinery run.
        model_version_id (int): the model version id validated in this script.
        age_start_column (str): the column in the metadata from which
            starting age should be determined.
        age_end_column (str): the column in the metadata from which
            ending age should be determined.
        draws (pd.DataFrame): all draws from the model version.

    Raises:
        DrawsMissingException if any demographics are missing from our
        model version draws that, according to the model version metadata,
        should exist in the draw file.
    """
    df = pd.merge(
        params.get_required_indices_from_model_version_id(
            model_version_id,
            age_start_column,
            age_end_column
        ),
        draws,
        on=constants.Columns.DEMOGRAPHIC_INDEX,
        how='left'
    )
    missing_rows = df[pd.isnull(df).any(axis=1)]
    if not missing_rows.empty:
        if params.test:
            # fill in missing values
            df = _fill_in_missing_data_for_tests(df, draws, params)
        else:
            raise exc.DrawsMissingException(
                "Draw indices do not match the full set as defined by model "
                f"metadata. Missing indices:\n{missing_rows}"
            )
    return df


def _fill_in_missing_data_for_tests(
        df: pd.DataFrame,
        draws: pd.DataFrame,
        params: MachineParameters
)-> pd.DataFrame:
    """
    If draws or demographics are missing, fill them in.

    Args:
        df: dataframe after expected metadata is merged on
        draws: the raw draws that have been read in
        params: machinery parameter object
    """
    # fill in missing draws with 1
    # (not 0 because they need to scale to some value)
    df[params.draw_cols] = df[params.draw_cols].fillna(1)

    # fill in missing demographics after merge
    df[constants.Columns.CAUSE_ID] = draws[constants.Columns.CAUSE_ID].iat[0]
    df[constants.Columns.MODEL_VERSION_TYPE_ID] = \
        draws[constants.Columns.MODEL_VERSION_TYPE_ID].iat[0]
    df[constants.Columns.IS_SCALED] = \
        draws[constants.Columns.IS_SCALED].iat[0]

    # Set all envelope values to NaN because we don't have values for
    # the new age groups; correct.py will fill this back in
    df[constants.Columns.ENVELOPE] = np.nan

    return df
