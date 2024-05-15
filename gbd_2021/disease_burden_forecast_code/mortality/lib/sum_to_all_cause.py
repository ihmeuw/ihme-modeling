"""
This module aggregates the expected value of mortality or ylds up the cause
hierarchy and computes the following at each step:

    y_hat = expected value of mortality or ylds
    y_past = past mortality or ylds

This is also used to aggregate post-approximation mortality results which are
saved in normal space. All results are saved in their own directory.

It is assumed that the inputs for modeled cause specific mortality or ylds with
version <version>, contain y_hat data in log space. That is, only draws of the
expected value. For aggregated approximated mortality results, no assumptions
are made and modeling space is set with the --space argument. All data in
normal space will be saved in the `data` root directory. Log space data will be
saved in the `int` root directory.

Spaces:
    - Modeled cause results split by sex are in log rate space.
    - Modeled cause results not split by sex are in normal rate space. This can be specified.
    - Past cause mortality or ylds are in normal space.
    - Summing to aggregate causes happens in normal rate space.
"""

import numpy as np
import xarray as xr
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_database_interface.lib import db_session
from fhs_lib_database_interface.lib.constants import (
    CauseConstants,
    DimensionConstants,
    FHSDBConstants,
)
from fhs_lib_database_interface.lib.strategy_set import strategy
from fhs_lib_file_interface.lib import provenance
from fhs_lib_file_interface.lib.provenance import ProvenanceManager
from fhs_lib_file_interface.lib.version_metadata import FHSFileSpec
from fhs_lib_file_interface.lib.versioning import validate_versions_scenarios_listed
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr_scenario, save_xr_scenario
from tiny_structured_logger.lib import fhs_logging

from fhs_pipeline_mortality.lib import get_fatal_causes
from fhs_pipeline_mortality.lib.config_dataclasses import SumToAllCauseModelingArguments

logger = fhs_logging.get_logger()

FLOOR = 1e-28

EXPECTED_VALUE_SUFFIX = "_hat"
INTERMEDIATE_ROOT_DIR = "int"
DATA_ROOT_DIR = "data"
NORMAL_SPACE = "identity"
LOG_SPACE = "log"


def aggregate_yhats(
    modeling_args: SumToAllCauseModelingArguments,
    dryrun: bool = False,
) -> None:
    """Computes y hats.

    If acause is a modeled cause, then it is simply moved into the right folder. Otherwise,
    the subcauses of acause are aggregated.

    Args:
        modeling_args (SumToAllCauseModelingArguments): dataclass containing sum-to-all-cause
            modeling arguments.
        dryrun (bool): dryrun flag. Don't actually do anything, just pretend like you're doing
            it.
    """
    validate_versions_scenarios_listed(
        versions=[modeling_args.agg_version, modeling_args.input_version],
        output_versions=[modeling_args.agg_version],
        output_scenario=modeling_args.output_scenario,
    )

    if modeling_args.approximation:
        suffix = ""
        root_dir = DATA_ROOT_DIR
        space = NORMAL_SPACE
    else:
        suffix = EXPECTED_VALUE_SUFFIX
        root_dir = INTERMEDIATE_ROOT_DIR
        space = LOG_SPACE

    y_hat = _get_y_hat(
        modeling_args=modeling_args, space=space, suffix=suffix, root_dir=root_dir
    )
    num_draws = len(y_hat[DimensionConstants.DRAW].values)
    logger.info(f"The data has {num_draws} draws.")
    y_hat_out = FHSFileSpec(
        modeling_args.agg_version.with_root_dir(root_dir), f"{modeling_args.acause}{suffix}.nc"
    )
    if dryrun:
        logger.info("(Dry run). Not saving y_hat", bindings=dict(out_file=y_hat_out))
    else:
        # Save data
        logger.info("Saving y_hat", bindings=dict(out_file=y_hat_out))
        save_xr_scenario(
            xr_obj=y_hat,
            file_spec=y_hat_out,
            metric="rate",
            space=space,
        )


def _get_y_hat(
    modeling_args: SumToAllCauseModelingArguments,
    space: str,
    suffix: str,
    root_dir: str,
) -> xr.DataArray:
    """Gets expected value of cause specific mortality or yld rates.

    For modeled causes, if the data is split by sex, then it is assumed that it
    is in log rate space. If the data is not split by sex, then it is assumed
    that it is in normal rate space.

    For aggregate causes, it is assumed that the data is not split by sex and
    is saved in log rate space.

    The resulting y_hat is in log rate space.

    Args:
        modeling_args (SumToAllCauseModelingArguments): dataclass containing sum-to-all-cause
            modeling arguments.
        space (str): space the data should be returned in, e.g. "log" or "identity".
        suffix (str): stage 2 should save data with "_hat"
        root_dir (str): Root directory for saving data

    Returns:
        xr.DataArray: The expected value of the cause specific mortality or yld rate
    """
    with db_session.create_db_session(FHSDBConstants.FORECASTING_DB_NAME) as session:
        gk_causes = strategy.get_cause_set(
            session=session,
            strategy_id=CauseConstants.FATAL_GK_STRATEGY_ID,
            gbd_round_id=modeling_args.gbd_round_id,
        )[DimensionConstants.ACAUSE].values

    if modeling_args.period == "past":
        gk_causes = np.delete(gk_causes, np.where(gk_causes == "maternal"))
        gk_causes = np.delete(gk_causes, np.where(gk_causes == "ckd"))

    if modeling_args.acause in gk_causes:
        logger.info("modeled cause", bindings=(dict(modeled_cause=modeling_args.acause)))
        y_hat = _get_modeled_y_hat(modeling_args=modeling_args, space=space)

    else:
        logger.info(
            "aggregated cause.", bindings=(dict(aggregated_cause=modeling_args.acause))
        )
        y_hat = _get_aggregated_y_hat(
            modeling_args=modeling_args, space=space, suffix=suffix, root_dir=root_dir
        )

    if isinstance(y_hat, xr.Dataset):
        if len(y_hat.data_vars) == 1:
            y_hat = y_hat.rename({list(y_hat.data_vars.keys())[0]: "value"})
            return y_hat["value"]
        logger.info(
            "Using __xarray_dataarray_variable__, but other data_vars are present! "
            "(probably just acause)"
        )
        y_hat = y_hat.rename({"__xarray_dataarray_variable__": "value"})
    else:
        y_hat.name = "value"
    return y_hat


def _get_aggregated_y_hat(
    modeling_args: SumToAllCauseModelingArguments,
    space: str,
    suffix: str,
    root_dir: str,
) -> xr.DataArray:
    """Gets expected value of cause specific mortality rates.

    For aggregate causes, it is assumed that the data is not split by sex and
    is saved in log rate space.

    When the children are added to form the aggregated acause result, the
    summation happens in normal space. Therefore, we must exponentiate the
    children's rates, add them up, and log them to get an aggregated
    y_hat in log rate space. If data is in normal space, simply sum.

    The resulting y_hat is in log rate space.

    Args:
        modeling_args (SumToAllCauseModelingArguments): dataclass containing sum-to-all-cause
            modeling arguments.
        space (str): Space the data is loaded/saved in. (Summing happens in linear space.)
        suffix (str): stage 2 should save data with "_hat"
        root_dir (str): Root directory for saving data

    Raises:
        Exception: If there are any missing files.

    Returns:
        xr.DataArray: he expected value of the cause specific mortality rate.
    """
    fatal_causes = get_fatal_causes.get_fatal_causes_df(
        gbd_round_id=modeling_args.gbd_round_id
    )

    cause_id = fatal_causes[fatal_causes.acause == modeling_args.acause].cause_id.values[0]
    children = fatal_causes.query("parent_id == {}".format(cause_id))[
        DimensionConstants.ACAUSE
    ].values
    logger.info("y_hat is a sum of children", bindings=(dict(children=children)))

    # Create a list of child acause files which are not external causes and
    # check to make sure all the ones we want to sum up are actually present.

    base_vm = modeling_args.agg_version.with_root_dir(root_dir)

    potential_child_files = [
        FHSFileSpec(base_vm, f"{child}{suffix}.nc")
        for child in children
        if child not in (CauseConstants.ALL_ACAUSE, "_none")
    ]
    child_files = list(filter(provenance.ProvenanceManager.exists, potential_child_files))

    if len(potential_child_files) != len(child_files):
        missing_children = list(set(potential_child_files) - set(child_files))
        msg = f"You are missing files: {missing_children}."
        logger.error(
            msg,
            bindings=(
                dict(potential_child_files=potential_child_files, child_files=child_files),
            ),
        )
        raise Exception(msg)
    logger.debug("Summing these files: ", bindings=dict(child_files=child_files))

    exp_y_hat_sum = None
    for child_file in child_files:
        logger.info("Adding child file", bindings=dict(child_file=str(child_file)))
        exp_y_hat = transform_spaces(
            da=open_xr_scenario(file_spec=child_file).drop_vars(
                [DimensionConstants.MEASURE, "cov"], errors="ignore"
            ),
            src_space=space,
            dest_space="identity",
        )
        # Remove child acause coordinates so they don't interfere with
        # broadcasting and summing. Parent acause will be added back in later.
        if DimensionConstants.ACAUSE in exp_y_hat.coords:
            exp_y_hat = exp_y_hat.drop_vars(DimensionConstants.ACAUSE)
        if exp_y_hat_sum is None:
            exp_y_hat_sum = exp_y_hat
        else:
            exp_y_hat_broadcasted = xr.broadcast(exp_y_hat_sum, exp_y_hat)
            exp_y_hat_broadcasted = [data.fillna(0.0) for data in exp_y_hat_broadcasted]
            exp_y_hat_sum = sum(exp_y_hat_broadcasted)

    y_hat = transform_spaces(da=exp_y_hat_sum, src_space="identity", dest_space=space)
    if DimensionConstants.ACAUSE not in y_hat.coords:
        try:
            y_hat[DimensionConstants.ACAUSE] = modeling_args.acause
        except ValueError:
            y_hat = y_hat.squeeze("acause")
            y_hat[DimensionConstants.ACAUSE] = modeling_args.acause
    elif modeling_args.acause not in y_hat.coords[DimensionConstants.ACAUSE]:
        y_hat[DimensionConstants.ACAUSE] = [modeling_args.acause]
    return y_hat


def expand_sex_id(ds: xr.Dataset) -> xr.Dataset:
    """Expand dimension 'sex_id' function for use in open_mfdataset.

    Args:
        ds (xr.Dataset): The loaded dataset before concatenation with scalar
            coordinate for sex_id dimension.

    Returns:
        xr.Dataset: The dataset with expanded 1D sex_id
    """
    if DimensionConstants.SEX_ID in ds.dims:
        return ds
    return ds.expand_dims(DimensionConstants.SEX_ID)


def transform_spaces(da: xr.DataArray, src_space: str, dest_space: str) -> xr.DataArray:
    """Convert ``da`` from the ``src_space`` to the ``dest_space``.

    Each space should be either "identity" (linear) or "log".
    """
    if src_space == dest_space:
        return da
    if src_space == NORMAL_SPACE and dest_space == LOG_SPACE:
        return np.log(da)
    if src_space == LOG_SPACE and dest_space == NORMAL_SPACE:
        return np.exp(da)
    raise ValueError(f"Unknown space conversion from {src_space} to {dest_space}")


def transform_spaces_adding_floor(
    da: xr.DataArray, src_space: str, dest_space: str
) -> xr.DataArray:
    """Convert ``da`` from the ``src_space`` to the ``dest_space``, maybe adding a floor.

    We add a tiny floor constant when going into log space, so that we don't get -inf.

    Each space should be either "identity" (linear) or "log".
    """
    if src_space == NORMAL_SPACE and dest_space == LOG_SPACE:
        da = da + FLOOR
    return transform_spaces(da, src_space, dest_space)


def open_xr_scenario_resample(file_spec: FHSFileSpec, draws: int) -> xr.DataArray:
    """It is extremely common that we open a file and resample it immediately.
    """
    return resample(open_xr_scenario(file_spec=file_spec), draws)


def _get_modeled_y_hat(
    modeling_args: SumToAllCauseModelingArguments,
    space: str,
) -> xr.DataArray:
    """Gets mortality data for a modeled acause.

    For modeled causes, if the data is split by sex, then it is assumed that it
    is in log rate space. If the data is not split by sex, then it is assumed
    that it is in normal rate space.

    Args:
        modeling_args (SumToAllCauseModelingArguments): dataclass containing sum-to-all-cause
            modeling arguments.
        space (str): Space the data should be returned in

    Raises:
        IOError: If this cause has no modeled mortality/ylds for this version

    Returns:
        xr.DataArray: the mortality or yld data for a cause
    """
    input_file_spec = FHSFileSpec(modeling_args.input_version, f"{modeling_args.acause}.nc")
    logger.info(
        "No children. y_hat is from mort/yld file",
        bindings=dict(input_file=input_file_spec),
    )

    if ProvenanceManager.exists(input_file_spec):
        # Sex-combined data is assumed to be in linear space.
        src_space = "identity"
    else:
        # Sex-split data is assumed to be in log space.
        src_space = "log"

    if ProvenanceManager.exists(input_file_spec):
        y_hat = open_xr_scenario_resample(input_file_spec, modeling_args.draws)
        if isinstance(y_hat, xr.Dataset):
            y_hat = y_hat["value"]
    else:
        # Modeled data is split by sex.
        potential_input_files = [
            FHSFileSpec(modeling_args.input_version, f"{modeling_args.acause}_{sex_name}.nc")
            for sex_name in ["male", "female"]
        ]
        input_files = list(filter(ProvenanceManager.exists, potential_input_files))
        logger.info(
            "Input results are split by sex. Files are",
            bindings=dict(input_files=input_files),
        )
        if len(input_files) == 0:
            msg = "This cause has no modeled mortality/ylds for this version."
            logger.error(
                msg,
                bindings=(
                    dict(acause=modeling_args.acause, version=modeling_args.input_version)
                ),
            )
            raise IOError(msg)
        y_hat_list = [
            open_xr_scenario_resample(input_file_spec, modeling_args.draws)
            for input_file_spec in input_files
        ]
        if modeling_args.period == "future":
            y_hat_list = [
                y_hat.drop_vars([DimensionConstants.MEASURE, "cov"], errors="ignore")
                for y_hat in y_hat_list
            ]

        y_hat = xr.concat(map(expand_sex_id, y_hat_list), dim=DimensionConstants.SEX_ID)

        if modeling_args.period == "future" and len(input_files) == 1:
            sex_id = _sex_id_from_filename(input_files[0].filename)
            if DimensionConstants.SEX_ID in y_hat.dims:
                y_hat.coords[DimensionConstants.SEX_ID] = [sex_id]
            else:
                y_hat = y_hat.expand_dims({DimensionConstants.SEX_ID: [sex_id]})
            y_hat = y_hat.to_dataset(name="value")
            # Note: Previous cases treat acause as a dim, this one as a coord.
            y_hat.coords[DimensionConstants.ACAUSE] = modeling_args.acause

    if ProvenanceManager.exists(input_file_spec) or modeling_args.period == "past":
        y_hat = _ensure_acause_dim(y_hat, acause=modeling_args.acause)
    elif modeling_args.period == "future" and len(input_files) == 1:
        y_hat.coords[DimensionConstants.ACAUSE] = modeling_args.acause

    y_hat = transform_spaces(y_hat, src_space=src_space, dest_space="identity")
    y_hat = transform_spaces_adding_floor(y_hat, src_space="identity", dest_space=space)
    return y_hat


def _sex_id_from_filename(filename: str) -> int:
    """Return sex_id 1 or 2 based on filename containing "male" or "female"."""
    if "female" in filename:
        sex_id = 2
    else:
        sex_id = 1
    return sex_id


def _ensure_acause_dim(da: xr.Dataset, acause: str) -> xr.Dataset:
    """Ensure acause dimension is present in da."""
    if DimensionConstants.ACAUSE in da.dims:
        if acause not in da.coords[DimensionConstants.ACAUSE]:
            da[DimensionConstants.ACAUSE] = [acause]
    else:
        da = da.expand_dims({DimensionConstants.ACAUSE: [acause]})

    return da
