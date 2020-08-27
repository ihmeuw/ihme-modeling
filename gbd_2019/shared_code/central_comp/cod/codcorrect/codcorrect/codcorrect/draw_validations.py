import os
import pandas as pd

from gbd import decomp_step
from get_draws.api import get_draws

from fauxcorrect.parameters.master import MachineParameters
from fauxcorrect.utils import constants, exceptions as exc, io


def read_input_draws(
        machine_parameters: MachineParameters,
        model_version_id: int
) -> pd.DataFrame:
    """
    Return all input draws using get_draws.api.get_draws.

    Arguments:
        machine_parameters (FauxCorrectParameters)
        model_version_id (int): the unique version id of the model
            results to read.

    Returns:
        pd.DataFrame containing all the draws for the provided model_version_id
        and cause_id.

    Raises:
        InputDrawsEmpty if no rows are returned from the get_draws call.
    """
    # prepare parameters
    downsample = constants.Draws.N_DRAWS < constants.Draws.MAX_DRAWS
    cause_id = machine_parameters.get_cause_from_model_version_id(
        model_version_id=model_version_id
    )
    args ={
        constants.Draws.GBD_ID: cause_id,
        constants.Draws.SOURCE_PARAM: constants.Draws.SOURCE,
        constants.Draws.GBD_ID_TYPE: constants.Columns.CAUSE_ID,
        constants.Draws.VERSION_ID: model_version_id,
        constants.Draws.DECOMP_STEP: _get_decomp_step_from_model_version(
            parameters=machine_parameters,
            model_version_id=model_version_id
        ),
        constants.Draws.GBD_ROUND_ID: machine_parameters.gbd_round_id,
        constants.Draws.DOWNSAMPLE: downsample,
        constants.Draws.N_DRAWS_PARAM: constants.Draws.N_DRAWS,
        constants.Draws.YEAR_ID: machine_parameters.year_ids,
        constants.Draws.NUM_WORKERS: constants.DAG.Tasks.Cores.VALIDATE_DRAWS
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
    return draws[constants.Columns.KEEP_VALIDATION]


def save_validated_draws(
        parameters: MachineParameters,
        model_version_id: int,
        draws: pd.DataFrame
) -> None:
    """
    Saves draws to the DeathMachine filesystem according to machine process
    and model_version_type_id in the draws.

    Arguments:
        params (MachineParameter): global parameter object, unique to this
            specific FauxCorrect run.
        model_version_id (int): the model version id validated in this script.
        draws (pd.DataFrame): all draws from the model version.

    Raises:
        RuntimeError if the model_version_type_id column is empty in the draw
        file or contains one than one type of id.
    """
    parent_dir = parameters.parent_dir
    process = parameters.process
    model_version_type_id = draws.model_version_type_id.unique()
    if len(model_version_type_id) > 1:
        raise RuntimeError(
            f"More than one {constants.Columns.MODEL_VERSION_TYPE_ID} in "
            f"model version {model_version_id}. Something when wrong."
        )
    elif not model_version_type_id:
        raise RuntimeError(
            f"The {constants.Columns.MODEL_VERSION_TYPE_ID} column is empty "
            f"in model version {model_version_id}. Cannot save draws."
        )
    model_version_type_id = int(model_version_type_id[0])
    if model_version_type_id in constants.ModelVersionTypeId.EXEMPT_TYPE_IDS:
        process = constants.GBD.Process.Name.SHOCKS
    save_map = {
        constants.GBD.Process.Name.CODCORRECT: _save_codcorrect_draws,
        constants.GBD.Process.Name.FAUXCORRECT: _save_fauxcorrect_draws,
        constants.GBD.Process.Name.SHOCKS: _save_shock_draws
    }
    save_map[process](parent_dir, model_version_id, draws)


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
            specific FauxCorrect run.
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
    return draws[constants.Columns.KEEP_VALIDATION].reset_index(drop=True)


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
        raise exc.DrawsMissingException(
            "Draw indices do not match the full set as defined by model "
            f"metadata. Missing indices:\n{missing_rows}"
        )
    return df


def _save_codcorrect_draws(
        parent_dir: str,
        model_version_id: int,
        draws: pd.DataFrame
) -> None:
    """
    Saves the validated CODEm and Custom draws to the CoDCorrect filesystem.

    Arguments:
        parent_dir (str): the root directory of the machine version.
        model_version_id (int): the model version id validated in this script,
            used to name the h5 file.
        draws (pd.DataFrame): the dataframe of validated draws being saved.
    """
    draw_dir = os.path.join(
        parent_dir,
        constants.FilePaths.UNAGGREGATED_DIR,
        constants.FilePaths.UNSCALED_DIR,
        constants.FilePaths.DEATHS_DIR
    )
    file_pattern = (
        constants.FilePaths.UNSCALED_DRAWS_FILE_PATTERN.format(
            model_version_id=model_version_id
        )
    )
    io.sink_draws(draw_dir, file_pattern, draws)


def _save_fauxcorrect_draws(
        parent_dir: str,
        model_version_id: int,
        draws: pd.DataFrame
) -> None:
    """
    Saves the validated CODEm and Custom draws to the FauxCorrect filesystem.

    Arguments:
        parent_dir (str): the root directory of the machine version.
        model_version_id (int): the model version id validated in this script,
            used to name the h5 file.
        draws (pd.DataFrame): the dataframe of validated draws being saved.
    """
    draw_dir = os.path.join(
        parent_dir,
        constants.FilePaths.DRAWS_UNSCALED_DIR,
        constants.FilePaths.DEATHS_DIR
    )
    file_pattern = (
        constants.FilePaths.UNSCALED_DRAWS_FILE_PATTERN.format(
            model_version_id=model_version_id
        )
    )
    io.sink_draws(draw_dir, file_pattern, draws)


def _save_shock_draws(
        parent_dir: str,
        model_version_id: int,
        draws: pd.DataFrame
) -> None:
    """
    Saves the validated shock/HIV/IC draws to the DeathMachine filesystem.

    Arguments:
        parent_dir (str): the root directory of the machine version.
        model_version_id (int): the model version id validated in this script,
            used to name the h5 file.
        draws (pd.DataFrame): the dataframe of validated draws being saved.
    """
    draw_dir = os.path.join(
        parent_dir,
        constants.FilePaths.UNAGGREGATED_DIR,
        constants.FilePaths.SHOCKS_DIR,
        constants.FilePaths.DEATHS_DIR
    )
    file_pattern = (
        constants.FilePaths.UNSCALED_DRAWS_FILE_PATTERN.format(
            model_version_id=model_version_id
        )
    )
    io.sink_draws(draw_dir, file_pattern, draws)


def _get_decomp_step_from_model_version(
        parameters: MachineParameters,
        model_version_id: int
) -> str:
    model = parameters.get_metadata_from_model_version_id(model_version_id)
    decomp_step_id = model.at[0, constants.Columns.DECOMP_STEP_ID]
    return decomp_step.decomp_step_from_decomp_step_id(decomp_step_id)