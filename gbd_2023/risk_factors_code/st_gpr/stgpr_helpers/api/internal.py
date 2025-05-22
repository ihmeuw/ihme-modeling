"""Top-level internal stgpr_helpers functions.

These functions are a facade over internal library code. They (optionally) start a session
with the appropriate database, then they call library code to execute the function.

This structure allows us to change library code as needed while maintaining the same top-level
interface. It also ensures a clean separation of interface from implementation details.

Internal functions should accept an optional session argument to use an existing session. If
no session is passed, then functions should start a new session.
"""

from typing import Any, Dict, List, Optional, Union

import pandas as pd
from sqlalchemy import orm

import db_stgpr
import db_tools_core
import stgpr_schema

from stgpr_helpers.lib import (
    fetch_utils,
    launch_utils,
    load_utils,
    location_utils,
    log_utils,
    parameter_utils,
    prep_utils,
    stgpr_version_utils,
    transform_utils,
)
from stgpr_helpers.lib.validation import model_quota as model_quota_validation
from stgpr_helpers.lib.validation import parameter as parameter_validation
from stgpr_helpers.lib.validation import stgpr_version as stgpr_version_validation


def create_stgpr_version(
    path_to_config: str,
    model_index_id: Optional[int] = None,
    stgpr_session: Optional[orm.Session] = None,
    epi_session: Optional[orm.Session] = None,
    covariates_session: Optional[orm.Session] = None,
    shared_vip_session: Optional[orm.Session] = None,
) -> int:
    """Creates a new ST-GPR version and adds it to the database.

    Args:
        path_to_config: path to config CSV containing model parameters.
        model_index_id: index of config parameters to use, if config contains multiple sets
            of model parameters.
        stgpr_session: session with the ST-GPR database. A session will be created if this
            is not passed.
        epi_session: session with the epi database. A session will be created if this
            is not passed.
        covariates_session: session with the covariates database. A session will be created
            if this is not passed
        shared_vip_session: session with the shared-vip database. A session will be created
            if this is not passed

    Returns:
        Created ST-GPR version ID.
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=stgpr_session
    ) as stgpr_session_scoped, db_tools_core.session_scope(
        settings.epi_db_conn_def, session=epi_session
    ) as epi_session_scoped, db_tools_core.session_scope(
        settings.covariate_db_conn_def, session=covariates_session
    ) as covariates_session_scoped, db_tools_core.session_scope(
        settings.shared_vip_db_conn_def, session=shared_vip_session
    ) as shared_vip_session_scoped:
        return stgpr_version_utils.create_stgpr_version(
            path_to_config=path_to_config,
            model_index_id=model_index_id,
            parameters_dict=None,
            user=None,
            stgpr_session=stgpr_session_scoped,
            epi_session=epi_session_scoped,
            covariates_session=covariates_session_scoped,
            shared_vip_session=shared_vip_session_scoped,
        )


def create_stgpr_version_from_params(
    parameters: Dict[str, Any],
    user: Optional[str] = None,
    stgpr_session: Optional[orm.Session] = None,
    epi_session: Optional[orm.Session] = None,
    covariates_session: Optional[orm.Session] = None,
    shared_vip_session: Optional[orm.Session] = None,
) -> int:
    """Creates a new ST-GPR version from a parameters dictionary and adds it to the database.

    Args:
        parameters: model parameters in a dictionary.
        user: username for creating an ST-GPR version.
        stgpr_session: session with the ST-GPR database. A session will be created if this
            is not passed.
        epi_session: session with the epi database. A session will be created if this
            is not passed.
        covariates_session: session with the covariates database. A session will be created
            if this is not passed
        shared_vip_session: session with the shared-vip database. A session will be created
            if this is not passed

    Returns:
        Created ST-GPR version ID.
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=stgpr_session
    ) as stgpr_session_scoped, db_tools_core.session_scope(
        settings.epi_db_conn_def, session=epi_session
    ) as epi_session_scoped, db_tools_core.session_scope(
        settings.covariate_db_conn_def, session=covariates_session
    ) as covariates_session_scoped, db_tools_core.session_scope(
        settings.shared_vip_db_conn_def, session=shared_vip_session
    ) as shared_vip_session_scoped:
        return stgpr_version_utils.create_stgpr_version(
            path_to_config=None,
            model_index_id=None,
            parameters_dict=parameters,
            user=user,
            stgpr_session=stgpr_session_scoped,
            epi_session=epi_session_scoped,
            covariates_session=covariates_session_scoped,
            shared_vip_session=shared_vip_session_scoped,
        )


def process_parameters(params_df: pd.DataFrame) -> Dict[str, Any]:
    """Processes parameters for use in modeling.

    Processing parameters involves the following steps:
    1. Ensure required parameters are present
    2. Drop unneeded columns
    3. Cast parameters to appropriate types
    4. Convert certain string parameters into lists
    5. Assign default parameter values
    6. Convert NaNs to None
    7. Strip leading 0 from density cutoffs, if present
    """
    return parameter_utils.process_parameters(params_df)


def validate_parameters(
    parameters: Dict[str, Any],
    raise_on_failure: bool,
    stgpr_session: Optional[orm.Session] = None,
    epi_session: Optional[orm.Session] = None,
    covariates_session: Optional[orm.Session] = None,
    shared_vip_session: Optional[orm.Session] = None,
) -> Optional[List[Dict[str, Any]]]:
    """Runs all of the parameter validations.

    Runs validations in toposorted batches, and confirms that required parameters are present
    before running each validation.

    Args:
        parameters: Dictionary of all parameters.
        raise_on_failure: Whether to raise an exception on validation failure.
        stgpr_session: Session with the ST-GPR database.
        epi_session: Session with the epi database.
        covariates_session: Session with the covariates database.
        shared_vip_session: Session with the shared-vip database.

    Raises:
        exc.ParameterValidationError: If a parameter validation fails and `raise_on_failure`
            is True.

    Returns:
        List of validation errors or None if validation succeeded.
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=stgpr_session
    ) as stgpr_session_scoped, db_tools_core.session_scope(
        settings.epi_db_conn_def, session=epi_session
    ) as epi_session_scoped, db_tools_core.session_scope(
        settings.covariate_db_conn_def, session=covariates_session
    ) as covariates_session_scoped, db_tools_core.session_scope(
        settings.shared_vip_db_conn_def, session=shared_vip_session
    ) as shared_vip_session_scoped:
        return parameter_validation.run_validations(
            parameters,
            raise_on_failure,
            stgpr_session_scoped,
            epi_session_scoped,
            covariates_session_scoped,
            shared_vip_session_scoped,
        )


def launch_model(
    stgpr_version_id: int,
    project: str,
    path_to_python_shell: str,
    path_to_main_script: str,
    log_path: Optional[str] = None,
    nparallel: int = 50,
) -> None:
    """Submits an ST-GPR model to the cluster.

    Args:
        run_id: an ST-GPR version ID.
        project: the cluster project to submit jobs to.
        path_to_python_shell: internal argument for pointing to Python shell. Relies on
            file structure in the stgpr package, so we pass it in from there.
        path_to_main_script: internal argument for pointing to workflow script. Relies on
            file structure in the stgpr package, so we pass it in from there.
        log_path: path to a directory for saving model logs.
        nparallel: Number of parallelizations to split data (by location_id).

    Raises:
        ValueError: if path to logs is on J
        RuntimeError: if the job submission fails
    """
    launch_utils.launch_model(
        stgpr_version_id,
        project,
        path_to_python_shell,
        path_to_main_script,
        log_path,
        nparallel,
    )


def prep_data(stgpr_version_id: int, session: Optional[orm.Session] = None) -> None:
    """Preps inputs needed for a model run.

    "Registration" (i.e. `create_stgpr_version`) involves validating parameters and adding
    them to the database as an ST-GPR version. Prep involves pulling inputs specified by the
    parameters and preparing them for an ST-GPR model. Prepped inputs include data (not
    square), covariates (square), custom stage 1 (square), location hierarchy, population
    estimates (square), and holdout metadata (square).

    Args:
        stgpr_version_id: ID of the ST-GPR version.
        session: session with the epi database. A session will be created if this is not
            passed.
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=session
    ) as scoped_session:
        prep_utils.prep(stgpr_version_id, scoped_session)


def load_data(
    stgpr_version_id: int,
    data_stage: stgpr_schema.DataStage,
    data_df: pd.DataFrame,
    session: Optional[orm.Session] = None,
) -> None:
    """Loads input data into the database.

    Args:
        stgpr_version_id: ID of the ST-GPR version for which to load input data.
        data_stage: Stage of data to upload.
        data_df: DataFrame of estimates to upload. Must have demographic columns, `seq`,
            `val`, `variance`, `nid`, `sample_size`, and `is_outlier`.
        session: session with the epi database. A session will be created if this is not
            passed.
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=session
    ) as scoped_session:
        load_utils.load_data(stgpr_version_id, data_stage, data_df, scoped_session)


def load_stage_1_estimates(
    stgpr_version_id: int, stage_1_df: pd.DataFrame, session: Optional[orm.Session] = None
) -> None:
    """Loads stage 1 estimates into the database.

    Stage 1 estimates don't vary by hyperparameter set, so load the same estimates for each
    model iteration.

    Args:
        stgpr_version_id: ID of the ST-GPR version for which to load stage 1 estimates.
        stage_1_df: DataFrame of estimates to upload. Must have demographic columns and
            `val`
        session: session with the epi database. A session will be created if this is not
            passed.

    Raises:
        ValueError: if `stage_1_df` is missing a required column or if any columns contains
            nulls or infinities.
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=session
    ) as scoped_session:
        load_utils.load_stage_1_estimates(stgpr_version_id, stage_1_df, scoped_session)


def load_stage_1_statistics(
    stgpr_version_id: int,
    stage_1_stats_df: pd.DataFrame,
    session: Optional[orm.Session] = None,
) -> None:
    """Loads stage 1 statistics into the database.

    Args:
        stgpr_version_id: ID of the ST-GPR version for which to load stage 1 estimates.
        stage_1_stats_df: DataFrame of estimates to upload. Must have columns `sex_id`,
            `custom_covariate_id`, `gbd_covariate_id`, `factor`, `beta`, `standard_error`,
            `z_value`, `p_value`
        session: session with the epi database. A session will be created if this is not
            passed.
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=session
    ) as scoped_session:
        load_utils.load_stage_1_statistics(stgpr_version_id, stage_1_stats_df, scoped_session)


def load_spacetime_estimates(
    stgpr_version_id: int,
    spacetime_df: pd.DataFrame,
    model_iteration_id: Optional[int] = None,
    session: Optional[orm.Session] = None,
) -> None:
    """Loads spacetime estimates into the database.

    For base, density cutoff, and out-of-sample evaluation models, a model iteration ID is
    not required (there will only be one model iteration associated with the ST-GPR version).

    For in-sample- and out-of-sample selection models, a specific model iteration ID is
    required.

    Args:
        stgpr_version_id: ID of the ST-GPR version for which to load stage 1 estimates.
        spacetime_df: DataFrame of estimates to upload. Must have demographic columns and
            `val`
        model_iteration_id: ID of the model iteration for which to load stage 1 estimates.
        session: session with the epi database. A session will be created if this is not
            passed.

    Raises:
        ValueError: if `spacetime_df` is missing a required column, if any columns contains
            nulls or infinities, or if `stgpr_version_id` corresponds to a selection model and
            `model_iteration_id` is None
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=session
    ) as scoped_session:
        load_utils.load_estimates_or_statistics(
            stgpr_version_id,
            spacetime_df,
            model_iteration_id,
            "Spacetime estimates",
            scoped_session,
        )


def load_gpr_estimates(
    stgpr_version_id: int,
    gpr_df: pd.DataFrame,
    model_iteration_id: Optional[int] = None,
    session: Optional[orm.Session] = None,
) -> None:
    """Loads GPR estimates into the database.

    For base, density cutoff, and out-of-sample evaluation models, a model iteration ID is
    not required (there will only be one model iteration associated with the ST-GPR version).

    For in-sample- and out-of-sample selection models, a specific model iteration ID is
    required.

    Args:
        stgpr_version_id: ID of the ST-GPR version for which to load GPR estimates.
        gpr_df: DataFrame of estimates to upload. Must have demographic columns and
            `val`
        model_iteration_id: ID of the model iteration for which to load GPR estimates.
        session: session with the epi database. A session will be created if this is not
            passed.

    Raises:
        ValueError: if `gpr_df` is missing a required column, if any columns contains
            nulls or infinities, or if `stgpr_version_id` corresponds to a selection model and
            `model_iteration_id` is None
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=session
    ) as scoped_session:
        load_utils.load_estimates_or_statistics(
            stgpr_version_id, gpr_df, model_iteration_id, "GPR estimates", scoped_session
        )


def load_final_estimates(
    stgpr_version_id: int,
    final_df: pd.DataFrame,
    model_iteration_id: Optional[int] = None,
    session: Optional[orm.Session] = None,
) -> None:
    """Loads final (raked) estimates into the database.

    For base, density cutoff, and out-of-sample evaluation models, a model iteration ID is
    not required (there will only be one model iteration associated with the ST-GPR version).

    For in-sample- and out-of-sample selection models, a specific model iteration ID is
    required.

    Args:
        stgpr_version_id: ID of the ST-GPR version for which to load final estimates.
        final_df: DataFrame of estimates to upload. Must have demographic columns and
            `val`
        model_iteration_id: ID of the model iteration for which to load final estimates.
        session: session with the epi database. A session will be created if this is not
            passed.

    Raises:
        ValueError: if `final_df` is missing a required column, if any columns contains
            nulls or infinities, or if `stgpr_version_id` corresponds to a selection model and
            `model_iteration_id` is None
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=session
    ) as scoped_session:
        load_utils.load_estimates_or_statistics(
            stgpr_version_id, final_df, model_iteration_id, "Final estimates", scoped_session
        )


def load_fit_statistics(
    stgpr_version_id: int,
    fit_stats_df: pd.DataFrame,
    model_iteration_id: Optional[int] = None,
    session: Optional[orm.Session] = None,
) -> None:
    """Loads fit statistics (i.e. RMSE) into the database.

    For base, density cutoff, and out-of-sample evaluation models, a model iteration ID is
    not required (there will only be one model iteration associated with the ST-GPR version).

    For in-sample- and out-of-sample selection models, a specific model iteration ID is
    required.

    Args:
        stgpr_version_id: ID of the ST-GPR version for which to load fit statistics.
        fit_stats_df: DataFrame of fit statistics to upload. Must have `model_stage_id`,
            `in_sample_rmse`, and `out_of_sample_rmse`.
        model_iteration_id: ID of the model iteration for which to load fit statistics.
        session: session with the epi database. A session will be created if this is not
            passed.

    Raises:
        ValueError: if `fit_stats_df` is missing a required column, if any columns except
            `out_of_sample_rmse` contains nulls or infinities, or if `stgpr_version_id`
            corresponds to a selection model and `model_iteration_id` is None
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=session
    ) as scoped_session:
        load_utils.load_estimates_or_statistics(
            stgpr_version_id,
            fit_stats_df,
            model_iteration_id,
            "Fit statistics",
            scoped_session,
        )


def load_amplitude_nsv(
    stgpr_version_id: int,
    amplitude_df: pd.DataFrame,
    nsv_df: Optional[pd.DataFrame],
    model_iteration_id: Optional[int] = None,
    session: Optional[orm.Session] = None,
) -> None:
    """Loads amplitude and non-sampling variance into the database.

    For base, density cutoff, and out-of-sample evaluation models, a model iteration ID is
    not required (there will only be one model iteration associated with the ST-GPR version).

    For in-sample- and out-of-sample selection models, a specific model iteration ID is
    required.

    Args:
        stgpr_version_id: ID of the ST-GPR version for which to load amplitude/NSV.
        amplitude_df: DataFrame of amplitude to upload. Must have `location_id`, `sex_id`,
            and `amplitude`.
        nsv_df: DataFrame of NSV to upload. Must have `location_id`, `sex_id`, and
            `non-sampling-variance`
        model_iteration_id: ID of the model iteration for which to load fit statistics.
        session: session with the epi database. A session will be created if this is not
            passed.

    Raises:
        ValueError: if `amplitude_df` is missing a required column, if any columns contain
            nulls or infinities, or if `stgpr_version_id` corresponds to a selection model
            and `model_iteration_id` is None
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=session
    ) as scoped_session:
        load_utils.load_amplitude_nsv(
            stgpr_version_id, amplitude_df, nsv_df, model_iteration_id, scoped_session
        )


def get_parameters(
    stgpr_version_id: int,
    session: Optional[orm.Session] = None,
    epi_best: bool = True,
    epi_session: Optional[orm.Session] = None,
) -> Dict[str, Any]:
    """Pulls parameters associated with an ST-GPR version ID.

    Args:
        stgpr_version_id: the ID with which to pull parameters.
        session: session with the epi database. A session will be created if this is not
            passed.
        epi_best: Whether to use best model information from the epi database (True) or the
            ST-GPR database (False).
        epi_session: Session with the epi database. Required if pulling best model
            information from epi rather than ST-GPR database.

    Returns:
        Dictionary of parameters for an ST-GPR run.
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=session
    ) as scoped_session:
        stgpr_version_validation.validate_stgpr_version_exists(
            stgpr_version_id, scoped_session
        )
        return db_stgpr.get_parameters(
            stgpr_version_id, scoped_session, epi_best, epi_session
        )


def get_data(
    stgpr_version_id: int,
    model_iteration_id: Optional[int] = None,
    data_stage: str = stgpr_schema.DataStage.original.name,
    location_id: Optional[List[int]] = None,
    year_id: Optional[List[int]] = None,
    sex_id: Optional[List[int]] = None,
    age_group_id: Optional[List[int]] = None,
    session: Optional[orm.Session] = None,
) -> pd.DataFrame:
    """Pulls data associated with an ST-GPR version ID and a data stage.

    Model iteration ID is only used if data stage is NSV. If no model iteration ID is
    provided, attempts to pull the best model iteration. In the case that there is no best
    model iteration, i.e. model is a selection model and is still running, an error will
    be thrown only if data with NSV is requested.

    Args:
        stgpr_version_id: the ST-GPR version ID with which to pull data.
        model_iteration_id: the model iteration ID with which to pull data.
        data_stage: Stage of data to pull. "original," "prepped," or "with_nsv"
        location_id: list of location ids. Defaults to None.
        year_id: list of year ids. Defaults to None.
        sex_id: list of sex ids. Defaults to None
        age_group_id: list of age group ids. Defaults to None.
        session: session with the epi database. A session will be created if this is not
            passed.

    Raises:
        exc.NoModelIterationProvided: if model_iteration_id is not provided and there is no
            best model iteration to default to and data with NSV is requested.

    Returns:
        DataFrame of data associated with an ST-GPR version.
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=session
    ) as scoped_session:
        return fetch_utils.get_data(
            stgpr_version_id,
            model_iteration_id,
            data_stage,
            db_stgpr.package_sproc_filters(location_id, year_id, sex_id, age_group_id),
            scoped_session,
        )


def get_custom_covariates(
    stgpr_version_id: int,
    location_id: Optional[List[int]] = None,
    year_id: Optional[List[int]] = None,
    sex_id: Optional[List[int]] = None,
    age_group_id: Optional[List[int]] = None,
    session: Optional[orm.Session] = None,
) -> Optional[pd.DataFrame]:
    """Pulls custom covariate data associated with an ST-GPR version ID.

    Args:
        stgpr_version_id: the ID with which to pull custom covariate data.
        location_id: list of location ids. Defaults to None.
        year_id: list of year ids. Defaults to None.
        sex_id: list of sex ids. Defaults to None
        age_group_id: list of age group ids. Defaults to None.
        session: session with the epi database. A session will be created if this is not
            passed.

    Returns:
        Dataframe of custom covariates for an ST-GPR run or None if there are no custom
        covariates for the given ST-GPR version ID.
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=session
    ) as scoped_session:
        return fetch_utils.get_custom_covariates(
            stgpr_version_id,
            db_stgpr.package_sproc_filters(location_id, year_id, sex_id, age_group_id),
            scoped_session,
        )


def get_stage_1_estimates(
    stgpr_version_id: int,
    location_id: Optional[List[int]] = None,
    year_id: Optional[List[int]] = None,
    sex_id: Optional[List[int]] = None,
    age_group_id: Optional[List[int]] = None,
    session: Optional[orm.Session] = None,
    level_space: bool = True,
) -> pd.DataFrame:
    """Pulls stage 1 estimates associated with an ST-GPR version ID.

    Args:
        stgpr_version_id: the ID with which to pull stage 1 estimates.
        location_id: list of location ids. Defaults to None.
        year_id: list of year ids. Defaults to None.
        sex_id: list of sex ids. Defaults to None
        age_group_id: list of age group ids. Defaults to None.
        session: session with the epi database. A session will be created if this is not
            passed.
        level_space: whether to return estimates in level space or modeling space. Stage 1
            estimates are saved in modeling space, so if `level_space` is true, transform
            back into level space.

    Returns:
        Dataframe of stage 1 estimates for an ST-GPR run.
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=session
    ) as scoped_session:
        return fetch_utils.get_stage_1_estimates(
            stgpr_version_id,
            db_stgpr.package_sproc_filters(location_id, year_id, sex_id, age_group_id),
            scoped_session,
            level_space,
        )


def get_stage_1_statistics(
    stgpr_version_id: int, session: Optional[orm.Session] = None
) -> Optional[pd.DataFrame]:
    """Pulls stage 1 statistics associated with an ST-GPR version ID.

    Args:
        stgpr_version_id: the ID with which to pull stage 1 statistics.
        session: session with the epi database. A session will be created if this is not
            passed.

    Returns:
        Dataframe of stage 1 statistics for an ST-GPR run or None if a custom stage 1 was
        used.
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=session
    ) as scoped_session:
        return db_stgpr.get_stage_1_statistics(stgpr_version_id, scoped_session)


def get_spacetime_estimates(
    stgpr_version_id: int,
    model_iteration_id: Optional[Union[int, List[int]]] = None,
    location_id: Optional[List[int]] = None,
    year_id: Optional[List[int]] = None,
    sex_id: Optional[List[int]] = None,
    age_group_id: Optional[List[int]] = None,
    session: Optional[orm.Session] = None,
    level_space: bool = True,
) -> pd.DataFrame:
    """Pulls spacetime estimates associated with an ST-GPR version ID or model iteration ID.

    For base, density cutoff, and out-of-sample evaluation models, a model iteration ID is
    not required (there will only be one model iteration associated with the ST-GPR version).

    For in-sample- and out-of-sample selection models, either a best model iteration (meaning
    the model has at least progressed past the evaluation stage) or a specific model
    iteration ID or IDs is required.

    Args:
        stgpr_version_id: the ST-GPR version ID with which to pull spacetime estimates.
        model_iteration_id: the model iteration ID or IDs with which to pull spacetime
            estimates.
        location_id: list of location ids. Defaults to None.
        year_id: list of year ids. Defaults to None.
        sex_id: list of sex ids. Defaults to None
        age_group_id: list of age group ids. Defaults to None.
        session: session with the epi database. A session will be created if this is not
            passed.
        level_space: whether to return estimates in level space or modeling space. Spacetime
            estimates are saved in modeling space, so if `level_space` is true, transform
            back into level space.

    Raises:
        exc.NoModelIterationProvided: if model_iteration_id is not provided and there is no
            best model iteration to default to.

    Returns:
        Dataframe of spacetime estimates for an ST-GPR run.
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=session
    ) as scoped_session:
        return fetch_utils.get_results(
            stgpr_version_id,
            model_iteration_id,
            "Spacetime estimates",
            db_stgpr.package_sproc_filters(location_id, year_id, sex_id, age_group_id),
            scoped_session,
            level_space,
        )


def get_gpr_estimates(
    stgpr_version_id: int,
    model_iteration_id: Optional[Union[int, List[int]]] = None,
    location_id: Optional[List[int]] = None,
    year_id: Optional[List[int]] = None,
    sex_id: Optional[List[int]] = None,
    age_group_id: Optional[List[int]] = None,
    session: Optional[orm.Session] = None,
    level_space: bool = True,
) -> pd.DataFrame:
    """Pulls GPR estimates associated with an ST-GPR version ID or model iteration ID.

    For base, density cutoff, and out-of-sample evaluation models, a model iteration ID is
    not required (there will only be one model iteration associated with the ST-GPR version).

    For in-sample- and out-of-sample selection models, either a best model iteration (meaning
    the model has at least progressed past the evaluation stage) or a specific model
    iteration ID or IDs is required.

    Args:
        stgpr_version_id: the ST-GPR version ID with which to pull GPR estimates.
        model_iteration_id: the model iteration ID or IDs with which to pull GPR estimates.
        location_id: list of location ids. Defaults to None.
        year_id: list of year ids. Defaults to None.
        sex_id: list of sex ids. Defaults to None
        age_group_id: list of age group ids. Defaults to None.
        session: session with the epi database. A session will be created if this is not
            passed.
        level_space: whether to return estimates in level space or modeling space. GPR
            estimates are saved in level space, so if `level_space` is false, transform
            into modeling space.

    Raises:
        exc.NoModelIterationProvided: if model_iteration_id is not provided and there is no
            best model iteration to default to.

    Returns:
        Dataframe of GPR estimates for an ST-GPR run.
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=session
    ) as scoped_session:
        return fetch_utils.get_results(
            stgpr_version_id,
            model_iteration_id,
            "GPR estimates",
            db_stgpr.package_sproc_filters(location_id, year_id, sex_id, age_group_id),
            scoped_session,
            level_space,
        )


def get_final_estimates(
    stgpr_version_id: int,
    model_iteration_id: Optional[int] = None,
    location_id: Optional[List[int]] = None,
    year_id: Optional[List[int]] = None,
    sex_id: Optional[List[int]] = None,
    age_group_id: Optional[List[int]] = None,
    session: Optional[orm.Session] = None,
    level_space: bool = True,
) -> pd.DataFrame:
    """Pulls final estimates associated with an ST-GPR version ID or model iteration ID.

    For base, density cutoff, and out-of-sample evaluation models, a model iteration ID is
    not required (there will only be one model iteration associated with the ST-GPR version).

    For in-sample- and out-of-sample selection models, either a best model iteration (meaning
    the model has at least progressed past the evaluation stage) or a specific model
    iteration ID is required. Pulling final results for multiple model iterations is not
    supported because final results are only created for the best model iteration.

    Args:
        stgpr_version_id: the ST-GPR version ID with which to pull final estimates.
        model_iteration_id: the model iteration ID with which to pull final estimates.
        location_id: list of location ids. Defaults to None.
        year_id: list of year ids. Defaults to None.
        sex_id: list of sex ids. Defaults to None
        age_group_id: list of age group ids. Defaults to None.
        session: session with the epi database. A session will be created if this is not
            passed.
        level_space: whether to return estimates in level space or modeling space. Final
            estimates are saved in level space, so if `level_space` is false, transform
            back into modeling space.

    Raises:
        exc.NoModelIterationProvided: if model_iteration_id is not provided and there is no
            best model iteration to default to.

    Returns:
        Dataframe of final estimates for an ST-GPR run.
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=session
    ) as scoped_session:
        return fetch_utils.get_results(
            stgpr_version_id,
            model_iteration_id,
            "Final estimates",
            db_stgpr.package_sproc_filters(location_id, year_id, sex_id, age_group_id),
            scoped_session,
            level_space,
        )


def get_fit_statistics(
    stgpr_version_id: int,
    model_iteration_id: Optional[Union[int, List[int]]] = None,
    session: Optional[orm.Session] = None,
) -> pd.DataFrame:
    """Pulls fit statistics associated with an ST-GPR version ID or model iteration ID.

    For base, density cutoff, and out-of-sample evaluation models, a model iteration ID is
    not required (there will only be one model iteration associated with the ST-GPR version).

    For in-sample- and out-of-sample selection models, either a best model iteration (meaning
    the model has at least progressed past the evaluation stage) or a specific model
    iteration ID or IDs is required.

    Args:
        stgpr_version_id: the ST-GPR version ID with which to pull fit statistics.
        model_iteration_id: the model iteration ID or IDs with which to pull fit statistics.
        session: session with the epi database. A session will be created if this is not
            passed.

    Raises:
        exc.NoModelIterationProvided: if model_iteration_id is not provided and there is no
            best model iteration to default to.

    Returns:
        Dataframe of fit statistics for an ST-GPR run.
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=session
    ) as scoped_session:
        return fetch_utils.get_results(
            stgpr_version_id,
            model_iteration_id,
            "Fit statistics",
            db_stgpr.package_sproc_filters(),
            scoped_session,
        )


def get_amplitude_nsv(
    stgpr_version_id: int,
    location_id: Optional[List[int]] = None,
    sex_id: Optional[List[int]] = None,
    model_iteration_id: Optional[Union[int, List[int]]] = None,
    session: Optional[orm.Session] = None,
) -> pd.DataFrame:
    """Pulls amplitude/NSV associated with an ST-GPR version ID or model iteration ID.

    For base, density cutoff, and out-of-sample evaluation models, a model iteration ID is
    not required (there will only be one model iteration associated with the ST-GPR version).

    For in-sample- and out-of-sample selection models, either a best model iteration (meaning
    the model has at least progressed past the evaluation stage) or a specific model
    iteration ID or IDs is required.

    Args:
        stgpr_version_id: the ST-GPR version ID with which to pull amplitude/NSV.
        model_iteration_id: the model iteration ID or IDs with which to pull amplitude/NSV.
        location_id: list of location ids. Defaults to None.
        sex_id: list of sex ids. Defaults to None
        session: session with the epi database. A session will be created if this is not
            passed.

    Raises:
        exc.NoModelIterationProvided: if model_iteration_id is not provided and there is no
            best model iteration to default to.

    Returns:
        Dataframe of amplitude for an ST-GPR run.
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(
        settings.stgpr_db_conn_def, session=session
    ) as scoped_session:
        return fetch_utils.get_results(
            stgpr_version_id,
            model_iteration_id,
            "Amplitude/NSV",
            db_stgpr.package_sproc_filters(location_id=location_id, sex_id=sex_id),
            scoped_session,
        )


def transform_data(mean: pd.Series, transform: str, reverse: bool = False) -> pd.Series:
    """Applies transformation to convert data into or out of modeling space.

    Args:
        mean: a Series of means to transform.
        transform: the name of the transform to apply. Options are log/logit/none
        reverse: whether to apply the transform in reverse.

    Returns:
        Transformed series of means.
    """
    return transform_utils.transform_data(mean, transform, reverse)


def transform_variance(
    mean: pd.Series, variance: pd.Series, transform: str, reverse: bool = False
) -> pd.Series:
    """Applies transformation to convert variance into or out of modeling space.

    Args:
        mean: a Series of means to use in variance transformation. Assumed to be the same
            length as `variance`.
        variance: a Series of variances to transform.
        transform: the name of the transform to apply. Options are log/logit/none
        reverse: whether to apply the transform in reverse.

    Returns:
        Transformed series of variances.
    """
    return transform_utils.transform_variance(mean, variance, transform, reverse)


def get_stgpr_locations(
    prediction_location_set_version_id: int,
    standard_location_set_version_id: int,
    session: orm.Session,
) -> pd.DataFrame:
    """Pulls location metadata needed for an ST-GPR model.

    Args:
        prediction_location_set_version_id: ID of the modeling location set version.
        standard_location_set_version_id: ID of the standard location set version.
        session: active session with the epi database.

    Returns:
        Location hierarchy for use in an ST-GPR model. It's basically a location hierarchy
        annotated with information about location levels and standard locations.
    """
    return location_utils.get_locations(
        prediction_location_set_version_id, standard_location_set_version_id, session
    )[0]


def get_model_quota(modelable_entity_id: int, session: orm.Session) -> Dict[str, int]:
    """Returns dictionary with model quota information for the given ME.

    Thin wrapper over db_stgpr.get_special_case_model_quota, filling in with default
    values if no results are returned for the ME.

    Model quotas are release-specific but do not vary by release, meaning X number of
    models are allow for any given release but the value of X is the same for all releases.

    Args:
        modelable_entity_id: ID of the modelable entity
        session: active session witht the stgpr database

    Returns:
        Dictionary with model quotas where keys are "quota_max_draws", representing the
        allowed number of models ran with max draws, and "quota_less_than_max_draws",
        representing the allowed number of models ran with less than max draws.
    """
    return model_quota_validation.get_model_quota(modelable_entity_id, session)


def get_model_count(
    modelable_entity_id: int, release_id: int, session: orm.Session
) -> Dict[str, int]:
    """Returns dict with number of models for the given ME/release by draw category.

    There are two draw categories: max draws (1000) or less than max draws (0, 100).
    Any non-deleted, non-best model contributes to the model count. Best model status
    is determined from the stgpr db, not the epi db.

    Args:
        modelable_entity_id: ID of the modelable entity
        release_id: ID of the release for model count
        session: active session with the stgpr database

    Returns:
        Dictionary with model counts where keys are "quota_max_draws", representing the
        number of models ran with max draws, and "quota_less_than_max_draws",
        representing the number of models ran with less than max draws.
    """
    return model_quota_validation.get_model_count(modelable_entity_id, release_id, session)


def configure_logging() -> None:
    """Sets up a basic logging configuration.

    Also filters out spammy pandas and pytables warnings.

    Note that `logging.basicConfig` is a no-op if logging is already configured, so calling
    this function won't override existing logging configurations.

    TODO: move this to the stgpr repo. It currently lives here because the stgpr repo is a
    mess, but once the stgpr code is in better shape, it should live there.
    """
    log_utils.configure_logging()
