import copy
from typing import Any, Dict

from sqlalchemy import orm

from db_tools import query_tools
import gbd

from orm_stgpr.db import lookup_tables, models
from orm_stgpr.lib.constants import dtype, parameters, queries


def get_parameters(
        session: orm.Session,
        stgpr_version_id: int
) -> Dict[str, Any]:
    """
    Pulls parameters associated with an existing ST-GPR version ID.

    Args:
        stgpr_version_id: the ID with which to pull parameters

    Returns:
        Dictionary of parameters for an ST-GPR run
    """
    # Make a skeleton of the parameters to return. This skeleton contains
    # default values of relevant parameters.
    # Use copy.deepcopy instead of pandas copy since the dataframe contains
    # lists which need to be copied recursively.
    params = copy.deepcopy(parameters.PARAMETER_SKELETON)

    # Update the skeleton with parameter values from the database.
    _udpate_parameters(params, session, stgpr_version_id)

    # Only return the parameters specified in the skeleton.
    return {
        k: v for k, v in params.items()
        if k in parameters.PARAMETER_SKELETON
    }


def _udpate_parameters(
        params: Dict[str, Any],
        session: orm.Session,
        stgpr_version_id: int
) -> None:
    """
    Pulls parameters from the database and adds them to the parameters
    dictionary in place.
    """
    stgpr_version = session\
        .query(models.StgprVersion)\
        .filter_by(stgpr_version_id=stgpr_version_id)\
        .one()

    _update_stgpr_version_parameters(params, stgpr_version, session)
    _update_stage_1_parameters(params, stgpr_version_id, session)
    _update_model_iteration_parameters(params, stgpr_version, session)


def _update_stgpr_version_parameters(
        params: Dict[str, Any],
        stgpr_version: models.StgprVersion,
        session: orm.Session
) -> None:
    _add_to_parameters(params, stgpr_version)

    params[parameters.LOCATION_SET_ID] = query_tools.exec_query(
        queries.GET_LOCATION_SET_ID,
        session,
        parameters={
            'location_set_version_id':
            stgpr_version.prediction_location_set_version_id
        }
    ).scalar()

    years = stgpr_version.prediction_year_ids.split(',')
    params[parameters.YEAR_START] = int(years[0])
    params[parameters.YEAR_END] = int(years[-1])
    params[parameters.DATA_TRANSFORM] = lookup_tables.TransformType(
        stgpr_version.transform_type_id
    ).name
    params[parameters.RUN_TYPE] = lookup_tables.RunType(
        stgpr_version.model_type_id
    ).name
    params[parameters.DECOMP_STEP] = \
        gbd.decomp_step.decomp_step_from_decomp_step_id(
            stgpr_version.decomp_step_id
    )


def _update_stage_1_parameters(
        params: Dict[str, Any],
        stgpr_version_id: int,
        session: orm.Session
) -> None:
    stage_1_param_set = session\
        .query(models.Stage1ParamSet)\
        .filter_by(stgpr_version_id=stgpr_version_id)\
        .one()
    _add_to_parameters(params, stage_1_param_set)


def _update_model_iteration_parameters(
        params: Dict[str, Any],
        stgpr_version: models.StgprVersion,
        session: orm.Session
) -> None:
    model_iterations = session\
        .query(models.ModelIteration.model_iteration_id)\
        .filter_by(stgpr_version_id=stgpr_version.stgpr_version_id)
    for iteration in model_iterations:
        param_sets = session\
            .query(models.ModelIterationParamSet)\
            .filter_by(model_iteration_id=iteration.model_iteration_id)

        # Density cutoff runs have multiple rows in model_iteration_param_set.
        if stgpr_version.model_type_id == lookup_tables.RunType.dd.value:
            for param_set in param_sets:
                st_param_set = session\
                    .query(models.StParamSet)\
                    .filter_by(st_param_set_id=param_set.st_param_set_id)\
                    .one()
                gpr_param_set = session\
                    .query(models.GprParamSet)\
                    .filter_by(gpr_param_set_id=param_set.gpr_param_set_id)\
                    .one()
                if param_set.data_density_end:
                    params[parameters.DENSITY_CUTOFFS].append(
                        int(param_set.data_density_end)
                    )
                _add_to_parameters(params, st_param_set, True)
                _add_to_parameters(params, gpr_param_set, True)
        else:
            # Non-density-cutoff runs have one row in model_iteration_param_set
            # but may contain multiple rows of spacetime and GPR param sets.
            param_set = param_sets.one()
            st_param_sets = session\
                .query(models.StParamSet)\
                .filter_by(st_param_set_id=param_set.st_param_set_id)
            gpr_param_sets = session\
                .query(models.GprParamSet)\
                .filter_by(gpr_param_set_id=param_set.gpr_param_set_id)
            for st_param_set in st_param_sets:
                _add_to_parameters(params, st_param_set)
            for gpr_param_set in gpr_param_sets:
                _add_to_parameters(params, gpr_param_set)


def _add_to_parameters(
        params: Dict[str, Any],
        table: models.Base,
        allow_duplicates: bool = False
) -> None:
    """
    Takes an action on a parameter before adding it to a dictionary.
    The following actions are supported:
    Replace: replace the default parameter value with the value in the
        database. Perform replace on "single" parameters.
    Append: append the parameter value in the database to the list of
        parameter values to return. Perform append on "crossval" parameters.
    Split: split the parameter value in the database into a list, then
        replace the default parameter value with this list. Perform split on
        "list" parameters.

    Args:
        params: dictionary of parameters to be updated in place
        table: sqlalchemy object from which to pull parameters
        allow_duplicates: whether duplicates crossval params are allowed
            in parameter lists
    """
    for param_name in table.single_params:
        param_value = getattr(table, param_name)
        if param_value is not None:
            params[param_name] = param_value

    for param_name in table.list_params:
        param_value = getattr(table, param_name)
        if param_value is not None:
            param_type = dtype.CONFIG_LIST[param_name]
            param_values = []
            for item in str(param_value).split(','):
                param_values.append(param_type(item))
            params[param_name] = param_values

    for param_name in table.crossval_params:
        param_value = getattr(table, param_name)
        if param_value is not None and \
                (param_value not in params[param_name] or allow_duplicates):
            params[param_name].append(param_value)
