"""
Project: GBD Vision Loss
Purpose: Functions for assisting with the underlying processes of the
etiology squeeze.
"""

# ---IMPORTS-------------------------------------------------------------------

import logging
import numpy as np
import pandas as pd

logging.basicConfig(
        level=logging.INFO,
        format=' %(asctime)s - %(levelname)s - %(message)s')

# ---FUNCTIONS-----------------------------------------------------------------

def get_dict_totals(dictionary, sev, exclude_rop, exclude_vita):
    """
    Given a dictionary of etiologies, obtain the sum for a given severity.

    Arguments:
        dictionary (dict): Dictionary of dataframes with cols 'age_group_id',
            'sex_id', 'year_id', 'location_id'.
        sev (str): Severity level. can take value "LOW_MOD", "LOW_SEV", or
            "BLIND".
        exclude_rop (bool): Denotes whether or not rop should be excluded
            from the total.
        exclude_vita (bool): Denotes whether or not vita should be excluded
            from the total.

    Returns:
        A dataframe of all of the dataframes summed up for each age_group_id,
        sex_id, year_id, and location_id.
    """

    df = pd.DataFrame()
    sev_specific_causes = [
        cause for cause in list(dictionary.keys()) if sev in cause]
    if exclude_rop:
        sev_specific_causes = [
            cause for cause in sev_specific_causes if "ROP" not in cause]
    if exclude_vita:
        sev_specific_causes = [
            cause for cause in sev_specific_causes if "VITA" not in cause]
    for cause in sev_specific_causes:
        df = df.append(dictionary[cause])
    # sum up all causes
    df = df.groupby(['age_group_id',
                     'sex_id',
                     'year_id',
                     'location_id'],
                    as_index=False)[[c for c in df if 'draw' in c]].sum()

    return df


def add_subtract_envelope(envelopes, causes, sev, cause, operation):
    """
    Either add or remove causes from an agglomeration of multiple causes.

    Arguments:
        envelopes (dict): Dictionary of envelope dataframes
        causes (dict): Dictionary of cause dataframes
        sev (str): Severity level (e.g. "BLIND")
        cause (str): Specific etiology (e.g. "ROP")
        operation (str): Accepts either "ADD" or "SUBTRACT"

    Returns:
        A dataframe of envelope level prevalence with the cause level
        prevalence added in or subtracted out, depending on the value of
        operation
    """

    cause = cause.upper()
    sev = sev.upper()
    env_df = envelopes[sev].copy()
    cause_df = causes['{s}_{c}'.format(s=sev, c=cause)].copy()
    logging.info(
       "{o}ING {c} from the {s} envelope".format(o=operation, c=cause, s=sev))
    env_df = pd.merge(env_df,
                      cause_df,
                      on=['location_id',
                          'age_group_id',
                          'year_id',
                          'sex_id'], suffixes=['_ENV', '_{}'.format(cause)])
    for i in range(1000):
        envelope_draw = env_df['draw_{}_ENV'.format(i)]
        cause_draw = env_df['draw_{i}_{c}'.format(i=i, c=cause)]
        if operation == "SUBTRACT":
            env_df['draw_{}'.format(i)] = envelope_draw - cause_draw
        elif operation == "ADD":
            env_df['draw_{}'.format(i)] = envelope_draw + cause_draw
        else:
            raise ValueError("""operation arg for add_subtract_envelope
                                takes either ADD or SUBTRACT
                             """)
    env_df.drop(
        [c for c in env_df.columns if cause in c], axis=1, inplace=True)
    env_df.drop(
        [c for c in env_df.columns if "ENV" in c], axis=1, inplace=True)

    if env_df['draw_{}'.format(i)].isnull().values.any():
        logging.info('Nans in draw_{} for sev {}'.format(i, sev))

    if np.all(env_df['draw_{}'.format(i)].dtype != np.float64):
        raise TypeError("The value needs to be a float64, currently {}".format(
            env_df['draw_{}'.format(i)].dtype))

    if np.all(env_df['draw_{}'.format(i)].values < 0):
        raise ValueError(
           "{} envelope prevalence draws need to be >= 0".format(sev))

    return env_df
