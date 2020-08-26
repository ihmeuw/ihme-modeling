from copy import copy
import os
from types import SimpleNamespace
from typing import Dict
from warnings import warn

import gbd
import numpy as np
import pandas as pd

from cascade_ode.argument_parser import METHOD_NAMES
from cascade_ode import importer

"""
Matrix of default features to decomp step (note step1 currently not supported)
For GBD 2019

|--------------------------------+-------+-------+-------+-------+-----------|
| feature                        | step1 | step2 | step3 | step4 | iterative |
|--------------------------------+-------+-------+-------+-------+-----------|
| enable hybrid cv               | NA    | F     | F     | F     | F         |
|--------------------------------+-------+-------+-------+-------+-----------|
| enable EMR/CSMR RE             | NA    | F     | T     | T     | T         |
|--------------------------------+-------+-------+-------+-------+-----------|
| disable non-standard locations | NA    | F     | F     | F     | F         |
|--------------------------------+-------+-------+-------+-------+-----------|
| fix bug preventing CSMR RE     | NA    | F     | T     | T     | T         |
|--------------------------------+-------+-------+-------+-------+-----------|
| average CSMR from codcorrect   | NA    | T     | F     | F     | F         |
|--------------------------------+-------+-------+-------+-------+-----------|
| Use CSMR from fauxcorrect      | NA    | F     | F     | T     | T         |
|--------------------------------+-------+-------+-------+-------+-----------|

(note: hybrid cv decided not to implement for gbd 2019)

Matrix of default features to decomp step For GBD 2020
(Note, currently identical to GBD 2019 step4 for all 2020 steps)

|--------------------------------+-------+-------+-------+-------+-----------|
| feature                        | step1 | step2 | step3 | step4 | iterative |
|--------------------------------+-------+-------+-------+-------+-----------|
| enable hybrid cv               | F     | F     | F     | F     | F         |
|--------------------------------+-------+-------+-------+-------+-----------|
| enable EMR/CSMR RE             | T     | T     | T     | T     | T         |
|--------------------------------+-------+-------+-------+-------+-----------|
| disable non-standard locations | F     | F     | F     | F     | F         |
|--------------------------------+-------+-------+-------+-------+-----------|
| fix bug preventing CSMR RE     | T     | T     | T     | T     | T         |
|--------------------------------+-------+-------+-------+-------+-----------|
| average CSMR from codcorrect   | F     | F     | F     | F     | F         |
|--------------------------------+-------+-------+-------+-------+-----------|
| Use CSMR from fauxcorrect      | T     | T     | T     | T     | T         |
|--------------------------------+-------+-------+-------+-------+-----------|



Where the methods come into play
|--------------------------------+-----------------------------------------|
| feature                        | location                                |
|--------------------------------+-----------------------------------------|
| enable hybrid cv               | cascade_loc.gen_data                    |
|--------------------------------+-----------------------------------------|
| enable EMR/CSMR RE             | cascade_loc.gen_effect                  |
|--------------------------------+-----------------------------------------|
| disable non-standard locations | GlobalCascade.__init__                  |
|--------------------------------+-----------------------------------------|
| fix bug preventing CSMR RE     | subset_data_by_location_and_sex         |
|--------------------------------+-----------------------------------------|
| average CSMR from codcorrect   | cascade_loc.average_data_by_integrand   |
|--------------------------------+-----------------------------------------|
| Use CSMR from fauxcorrect      | csmr.py, importer.promote_csmr_t2_to_t3 |
|--------------------------------+-----------------------------------------|


Description of each method change:

    enable EMR/CSMR Random effects:
        Prior to step3, the cascade disabled random effects for any EMR
        and for CSMR below super region.

    Average CSMR from codcorrect:
        Prior to step3, the cascade would compute average CSMR using the
        CSMR from codcorrect (if available). Presumably this is related to the
        fact that CSMR from codcorrect used to be more sparse than it currently
        is.

        Now the cascade does not average CSMR from codcorrect. It only averages
        CSMR in one particular case: if there is csmr in the bundle data, it
        will compute an average if the specific location/sex/year job doesn't
        have any applicable csmr data for that location/sex/year. In that case,
        it will use the most detailed locations to compute a pop-weighted
        average mean and standard deviation and append that as input data.

    fix bug preventing CSMR random effects:
        The cascade's CSMR averaging algorithm created new data in such a way
        that when dismod reads those rows, it did not compute random effects.

        Now, for step3 and beyond, any averaged CSMR being computed will have
        random effects (above subnational level -- dismod doesn't compute
        random effects for subnationals)

    Use CSMR from fauxcorrect
        If fauxcorrect CSMR estimates are available for the specified cause,
        use those instead of CSMR from codcorrect. Record version in model
        directory

There are 2 other method changes that are not enabled by default, but can
be specified at the command line. They are documented here for completeness'
sake but do not affect production dismod models:

    enable hybrid cv:
        hybrid cv is a different way of computing min_cv (a minimum
        coeffecient of variation enforced for all input data before
        being passed to dismod). It is defined as the max of
            1) cv of data point,
            2) min_cv from settings,
            3) eta

        This was contemplated as a method change for step3+
        but was not enabled.

    disable non-standard locations:
        a way to bypass the global standard locations dismod run. This was
        added as an option for testing/validation purposes but is not
        enabled.
"""


def default_method_from_step(
    decomp_step: str, gbd_round_id: int
) -> Dict[str, bool]:
    """
    Given a decomp step and gbd_round_id, return dictionary of
    methods and whether they are enabled
    """
    config = read_config()
    return get_methods(config, gbd_round_id, decomp_step)


def get_decomp_method_from_mv_and_flags(
    mvid: int, feature_flags: SimpleNamespace
) -> Dict[str, bool]:
    """
    Given a model version id and a set of feature flags, return a dictionary
    of methods for the model's decomp step, optionally with behavior overridden
    by feature flags

    Arguments:
        mvid (int): model version id
        feature_flags (SimpleNamespace): set of flags to override default
            behavior with

    Returns:
        Dict[str,bool]
    """
    df = importer.get_model_version(mvid)
    decomp_step = df.decomp_step.iat[0]
    gbd_round_id = df.gbd_round_id.iat[0]
    return determine_method(decomp_step, gbd_round_id, feature_flags)


def determine_method(
    decomp_step: str, gbd_round_id: int, feature_flags: SimpleNamespace
) -> Dict[str, bool]:
    """
    Get the default set of methods, possibly override default behavior
    with command line feature flags, then return methods dictionary
    """
    default_plan = default_method_from_step(decomp_step, gbd_round_id)
    plan = override_features(default_plan, feature_flags)
    return plan


def override_features(
    default_plan: Dict[str, bool], features: SimpleNamespace
) -> Dict[str, bool]:
    """Update the set of methods based on feature flags"""
    plan = copy(default_plan)

    for method in METHOD_NAMES:
        command_line_val = getattr(features, method)
        if command_line_val is not None:
            default_method = plan[method]
            if default_method != command_line_val:
                warn((
                    f"overwriting default behavior {method}={default_method} "
                    f"with {method}={command_line_val}"))
                plan[method] = command_line_val
    return plan


def read_config() -> pd.DataFrame:
    """
    Read the config csv outlining all methods for all rounds and decomp steps
    """
    decomp_config = os.path.join(
        os.path.abspath(os.path.dirname(__file__)),
        'config',
        'decomp_methods.csv')
    raw = pd.read_csv(decomp_config, skiprows=[0, 3], header=None)
    raw = raw.set_index(0)
    raw.index.name = 'method'
    raw.columns = pd.MultiIndex.from_arrays(raw.iloc[0:2].values,
                                            names=('gbd_round', 'decomp_step'))
    raw = raw[~raw.index.isin(raw.iloc[0:2].index)]
    for col in raw:
        raw[col] = raw[col].replace({'F': False, 'T': True, '-': np.nan})
    return raw


def get_methods(
    config: pd.DataFrame, gbd_round_id: int, decomp_step: str
) -> Dict[str, bool]:
    """
    From the config dataframe, extract the round/decomp specific set of methods
    """
    gbd_round = gbd.gbd_round.gbd_round_from_gbd_round_id(gbd_round_id)
    methods = config.loc[:, (gbd_round, decomp_step)].to_dict()
    methods['decomp_step'] = decomp_step
    return methods
