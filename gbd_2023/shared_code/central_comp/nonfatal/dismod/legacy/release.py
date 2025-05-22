"""
Module for determining release-specific cascade behavior.

Matrix of default features to release_id:

================================ === === ==== ==== ==== ==== ==== ====
 feature                          8   9   10   11   12   13   14  15
================================ === === ==== ==== ==== ==== ==== ====
 enable hybrid CV                 F   F   F    F    F    F    F    F
 enable EMR/CSMR RE               T   T   T    T    T    T    T    T
 disable non-standard locations   F   F   F    F    F    F    F    F
 fix bug preventing CSMR RE       T   T   T    T    T    T    T    T
 average CSMR from codcorrect     F   F   F    F    F    F    F    F
================================ === === ==== ==== ==== ==== ==== ====


Where the methods come into play:

=============================== ==============================================
feature                         location
=============================== ==============================================
enable hybrid CV                 cascade_loc.gen_data
enable EMR/CSMR RE               cascade_loc.gen_effect
disable non-standard locations   GlobalCascade.__init__
fix bug preventing CSMR RE       cascade_loc.subset_data_by_location_and_sex
average CSMR from CodCorrect     cascade_loc.average_data_by_integrand
=============================== ==============================================


Description of each method change:

    Enable EMR/CSMR Random effects:
        The cascade used to disable random effects for any expected mortality rates
        (EMR) and for cause-specific mortality rates (CSMR) below super region. This
        was a GBD2019 decomp methods change.

    Average CSMR from CodCorrect:
        The cascade used to compute average CSMR using the
        CSMR from CodCorrect (if available). Presumably this is related to the
        fact that CSMR from CodCorrect used to be more sparse than it currently
        is.

        Now the cascade does not average CSMR from CodCorrect. It only averages
        CSMR in one particular case: If there is CSMR in the bundle data, it
        will compute an average if the specific location/sex/year job doesn't
        have any applicable CSMR data for that location/sex/year. In that case,
        it will use the most-detailed locations to compute a pop-weighted
        average mean and standard deviation and append that as input data.

    Fix bug preventing CSMR random effects:
        The cascade's CSMR averaging algorithm created new data in such a way
        that when DisMod read those rows, it did not compute random effects.

        Now any averaged CSMR being computed will have random effects (above
        subnational level -- DisMod doesn't compute random effects for
        subnationals).


There are two other method changes that are not enabled by default, but can
be specified at the command line. They are documented here for completeness'
sake but do not affect production DisMod models:

    Enable hybrid CV:
        Hybrid CV is a different way of computing min_cv (a minimum
        coeffecient of variation enforced for all input data before
        being passed to DisMod). It is defined as the max of:

            1) CV of data point
            2) min_cv from settings
            3) eta, an offset to the log-transformed measurement

        This method has never been enabled.

    Disable non-standard locations:
        A way to bypass the global standard locations DisMod run. This was
        added as an option for testing/validation purposes but is not
        enabled.
"""

import os
from copy import copy
from logging import info
from types import SimpleNamespace
from typing import Dict
from warnings import warn

import numpy as np
import pandas as pd

from cascade_ode.legacy import importer
from cascade_ode.legacy.argument_parser import METHOD_NAMES


def get_release_method_from_mv_and_flags(
    mvid: int, feature_flags: SimpleNamespace
) -> Dict[str, bool]:
    """Gets a dictionary of methods for the model's release_id.

    Optionally overrides behavior with given feature flags.

    If methods not configured for release_id, uses the latest release_id configured.

    Arguments:
        mvid (int): model version id
        feature_flags (SimpleNamespace): set of flags to override default
            behavior with

    Returns:
        Dict[str,bool]
    """
    df = importer.get_model_version(mvid)
    release_id = df.release_id.iat[0]
    return determine_method(release_id, feature_flags)


def determine_method(release_id: int, feature_flags: SimpleNamespace) -> Dict[str, bool]:
    """Gets the default set of methods via returning methods dictionary.

    Possibly overrides default behavior with command line feature flags.
    """
    default_plan = default_method_from_release(release_id=release_id)
    plan = override_features(default_plan, feature_flags)
    return plan


def default_method_from_release(release_id: int) -> Dict[str, bool]:
    """Returns dictionary of methods and whether they are enabled given release_id."""
    config = read_config()

    return get_methods(config, release_id)


def override_features(
    default_plan: Dict[str, bool], features: SimpleNamespace
) -> Dict[str, bool]:
    """Updates the set of methods based on feature flags."""
    plan = copy(default_plan)

    for method in METHOD_NAMES:
        command_line_val = getattr(features, method)
        if command_line_val is not None:
            default_method = plan[method]
            if default_method != command_line_val:
                warn(
                    (
                        f"overwriting default behavior {method}={default_method} "
                        f"with {method}={command_line_val}"
                    )
                )
                plan[method] = command_line_val
    return plan


def read_config() -> pd.DataFrame:
    """Reads the config csv outlining all methods for all available release_ids."""
    release_config = 
    raw = pd.read_csv(release_config, header=None)
    raw = raw.set_index(0)
    raw.index.name = "method"
    raw.columns = raw.loc["release_id"].astype(int)
    raw = raw.drop("release_id")
    raw = raw.replace({"F": False, "T": True, "-": np.nan})
    return raw


def get_methods(config: pd.DataFrame, release_id: int) -> Dict[str, bool]:
    """Extracts the release_id-specific set of methods from the config df."""
    if release_id not in config.columns:
        max_release_id = max(config.columns)
        info(
            f"No methods configured for release_id {release_id}. "
            f"Using latest release_id {max_release_id}. Methods config: \n {config}"
        )
        release_id = max_release_id

    methods = config.loc[:, release_id].to_dict()
    return methods
