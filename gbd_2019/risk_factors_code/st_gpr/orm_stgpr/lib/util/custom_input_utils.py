import logging
import os
from typing import Optional

import pandas as pd
from sqlalchemy import orm

from orm_stgpr.lib import covariates
from orm_stgpr.lib.constants import columns, dtype, parameters
from orm_stgpr.lib.util import helpers, step_control
from orm_stgpr.lib.validation import data_validation

_FILL_FILE_FORMAT = 'FILEPATH'


def read_custom_stage_1(
        path_to_custom_stage_1: Optional[str],
        path_to_custom_covariates: Optional[str],
        stage_1_model_formula: Optional[str]
) -> Optional[pd.DataFrame]:
    """
    Validates custom stage 1 parameters and reads custom stage 1 model from a
    CSV, if present.
    Returns None if custom stage 1 is not present.
    See the custom stage 1 flowchart here: https://hub.ihme.washington.edu/x/gyEyBQ
    """
    if not path_to_custom_stage_1:
        if stage_1_model_formula:
            return None
        raise ValueError(
            'You must provide either a stage 1 model formula or path to '
            'custom stage 1 inputs. Found neither'
        )
    if path_to_custom_covariates:
        raise ValueError(
            'Cannot use both custom covariates and custom stage 1'
        )
    if stage_1_model_formula:
        raise ValueError(
            'You must provide either a stage 1 model formula or path to '
            'custom stage 1 inputs. Found both'
        )
    return _read_custom_stage_1_csv(path_to_custom_stage_1)


def read_custom_covariates(
        path_to_custom_covariates: Optional[str],
        path_to_custom_stage_1: Optional[str],
        gbd_round_id: int,
        decomp_step: str,
        best_model_id: Optional[int],
        session: orm.Session
) -> Optional[pd.DataFrame]:
    """
    Validates custom covariate parameters and reads custom covariates from a
    CSV or best model from the previous decomp step.
    Returns None if custom covariates are not present.
    This function should be read alongside the custom covariates flowchart:
    https://hub.ihme.washington.edu/x/gyEyBQ
    """
    if not path_to_custom_covariates:
        if path_to_custom_stage_1 or not best_model_id:
            return None
        best_covariates = covariates.get_custom_covariates(
            session, best_model_id
        )
        if best_covariates is None:
            return None
        if decomp_step in step_control.get_covariate_fill_steps(gbd_round_id):
            return _fill_covariates(best_model_id)
        return best_covariates
    if path_to_custom_stage_1:
        raise ValueError(
            'Cannot use both custom covariates and custom stage 1'
        )
    if not best_model_id:
        return _read_custom_covariates_csv(path_to_custom_covariates)

    actions = step_control.get_custom_covariate_actions(
        gbd_round_id, decomp_step
    )
    if step_control.ADD_DROP not in actions:
        if step_control.MODIFY_DATA not in actions:
            raise ValueError(
                'Changing custom covariates is not allowed for GBD round '
                f'{gbd_round_id}, decomp step {decomp_step}, but a value was '
                f'passed for {parameters.PATH_TO_CUSTOM_COVARIATES}: '
                f'{path_to_custom_covariates}'
            )
        return _modify_custom_covariate_data(
            path_to_custom_covariates, best_model_id, session
        )
    if step_control.MODIFY_DATA in actions:
        custom_covariates_df = _read_custom_covariates_csv(
            path_to_custom_covariates, can_drop_cols=True
        )
        if set(custom_covariates_df.columns) == set(columns.DEMOGRAPHICS):
            return None
        return custom_covariates_df
    return _add_drop_custom_covariates(
        path_to_custom_covariates, best_model_id, session
    )


def _read_custom_stage_1_csv(path_to_stage_1: str) -> Optional[pd.DataFrame]:
    if not path_to_stage_1:
        return None
    try:
        logging.info('Found custom stage 1')
        return pd\
            .read_csv(
                path_to_stage_1,
                usecols=columns.DEMOGRAPHICS + [columns.CUSTOM_STAGE_1],
                dtype=dtype.DEMOGRAPHICS)\
            .pipe(helpers.sort_columns)\
            .pipe(lambda df: data_validation.validate_no_duplicates(
                df, data_type='custom stage 1'))\
            .pipe(lambda df: data_validation.validate_no_nan_infinity(
                df, [columns.CUSTOM_STAGE_1], data_type='custom stage 1'))\
            .rename(columns={
                columns.CUSTOM_STAGE_1: columns.CUSTOM_STAGE_1_VALUE})
    except ValueError as error:
        # Calling pd.read_csv with the usecols argument will raise a ValueError
        # on missing columns. It's an informative error, but we still want to
        # catch it and provide some context before re-raising.
        if 'Usecols do not match columns' in error.args[0]:
            raise RuntimeError(
                f'Could not read custom stage 1 from {path_to_stage_1}'
            ) from error
        raise


def _fill_covariates(best_model_id: int) -> pd.DataFrame:
    """Use filled covariate that was generated during a baseline fill run"""
    filled_cov_path = _FILL_FILE_FORMAT.format(stgpr_version_id=best_model_id)
    if os.path.exists(filled_cov_path):
        return pd\
            .read_csv(filled_cov_path)\
            .pipe(helpers.sort_columns)
    raise RuntimeError(
        'Filled covariates have not yet been generated for ST-GPR run '
        f'{best_model_id}. Please file a ticket requesting that covariates '
        'be filled for this ST-GPR model'
    )


def _read_custom_covariates_csv(
        path_to_covs: str,
        can_drop_cols: bool = False
) -> Optional[pd.DataFrame]:
    if not path_to_covs:
        return None
    custom_covs_df = pd.read_csv(path_to_covs, dtype=dtype.DEMOGRAPHICS)
    missing_demo_cols = set(columns.DEMOGRAPHICS) - set(custom_covs_df.columns)
    if missing_demo_cols:
        raise RuntimeError(
            'Custom covariates data is missing the following columns: '
            f'{missing_demo_cols}'
        )
    cov_cols = [col for col in custom_covs_df if columns.CV_PREFIX in col]
    if not cov_cols and not can_drop_cols:
        raise RuntimeError(
            'Did not find any custom covariate columns in the data at '
            f'{path_to_covs}. Note that custom covariate columns must '
            f'start with "{columns.CV_PREFIX}". Found the following columns: '
            f'{custom_covs_df.columns.tolist()}'
        )
    logging.info(f'Found custom covariates: {",".join(cov_cols)}')
    return custom_covs_df\
        .loc[:, columns.DEMOGRAPHICS + cov_cols]\
        .pipe(helpers.sort_columns)\
        .pipe(lambda df: data_validation.validate_no_duplicates(
            df, data_type='custom covariates'))\
        .pipe(lambda df: data_validation.validate_no_nan_infinity(
            df, cov_cols, data_type='custom covariates'))


def _add_drop_custom_covariates(
        path_to_covs: str,
        best_model_id: int,
        session: orm.Session
) -> pd.DataFrame:
    covs_csv_df = _read_custom_covariates_csv(path_to_covs, True)
    covs_db_df = covariates.get_custom_covariates(session, best_model_id)
    covs_to_drop = set(covs_db_df.columns) - set(covs_csv_df.columns)
    covs_to_add = set(covs_csv_df.columns) - set(covs_db_df.columns)

    logging.info(f'Dropping covariates {covs_to_drop} from custom covariates')
    covs_db_df = covs_db_df.drop(columns=list(covs_to_drop))

    if covs_to_add and len(covs_csv_df) != len(covs_db_df):
        raise ValueError(
            f'Custom covariates read from {path_to_covs} have '
            f'{len(covs_csv_df)} rows, but custom covariates from the '
            f'previous decomp step have {len(covs_db_df)} rows. '
            'In order to add or drop covariates, the new covariate data '
            'must have the same number of rows as the covariate data from '
            'the previous decomp step'
        )
    for cov in covs_to_add:
        logging.info(f'Adding covariate {cov} to custom covariates')
        covs_db_df[cov] = covs_csv_df[cov]

    if set(covs_db_df.columns) == set(columns.DEMOGRAPHICS):
        # All custom covariates were dropped.
        return None

    return covs_db_df


def _modify_custom_covariate_data(
        path_to_custom_covariates: str,
        best_model_id: int,
        session: orm.Session
) -> pd.DataFrame:
    covs_csv_df = _read_custom_covariates_csv(path_to_custom_covariates)
    covs_db_df = covariates.get_custom_covariates(session, best_model_id)
    demo_set = set(columns.DEMOGRAPHICS)
    modify_cols = set(covs_db_df.columns) - demo_set
    logging.info(f'Updating custom covariate data for {list(modify_cols)}')

    missing_cols = modify_cols - set(covs_csv_df.columns) - demo_set
    if missing_cols:
        raise ValueError(
            f'Custom covariates read from {path_to_custom_covariates} are '
            f'missing required columns {list(missing_cols)}'
        )

    return covs_csv_df[covs_db_df.columns]
