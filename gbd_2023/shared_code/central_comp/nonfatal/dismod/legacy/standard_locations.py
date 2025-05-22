import logging
from typing import Set

import numpy as np

from cascade_ode.legacy.demographics import Demographics
from cascade_ode.legacy.shared_functions import get_location_metadata


def extract_betas(cascade_loc):
    """
    Given a cascade_loc object, retrieve study and country level covariate
    betas that reflect a dismod run with data from non-standard locations
    omitted.

    This involves
        A) omitting data from non-standard locations
        B) running dismod_ode executable to get betas
        C) running cascade_loc.summarize_posterior to summarize posteriors

    Arguments:
        cascade_loc (cascade_ode.legacy.drill.cascade_loc): node in cascade
            to strip non-standard locations and extract betas from

    Returns:
        pd.DataFrame of betas with 5 columns, 'effect', 'integrand', 'name',
        'std', and 'mean'.
    """
    drop_non_standard_locations(
        cascade_loc=cascade_loc,
        release_id=cascade_loc.cascade.model_version_meta.release_id.iat[0],
    )
    cascade_loc.run_dismod()
    cascade_loc.summarize_posterior()
    posteriors = cascade_loc.post_summary.copy()
    betas = posteriors[posteriors.effect.str.contains("beta")]
    # get df of 1) integrand and 2) name of covariate (ie x_sex or
    # x_s_hospital)
    splits = betas.effect.str.split("_", n=2, expand=True)
    betas[["effect", "integrand", "name"]] = splits
    return betas[["effect", "integrand", "name", "mean", "std"]]


def apply_betas(betas, cascade_loc):
    """
    Update beta priors for a given cascade_loc, such that we update mean and
    standard deviation values for each integrand/covariate combination.

    Arguments:
        betas (pd.DataFrame): dataframe of betas to apply (see extract_betas)
        cascade_loc (cascade_ode.legacy.drill.cascade_loc): node in cascade to
            update priors for

    Returns:
        None (mutates cascade_loc in place)

    Note that if mean betas exceed upper or lower bounds by floating point
    error, the mean beta will be adjusted to equal the bound that it exceeded
    """
    betas = betas.copy().rename({"mean": "new_mean", "std": "new_std"}, axis=1)

    # dismod does not allow 0 for std column in effects file. Must replace
    # with inf
    inf = float("inf")
    betas.loc[betas["new_std"] == 0, "new_std"] = inf

    # create new effects df and overwrite mean/std with new values
    effects = cascade_loc.effects_prior.copy()
    effects = effects.merge(betas, how="left")
    effects = adjust_to_within_bounds(effects)

    update_mask = effects.new_mean.notnull()
    for col in ["mean", "std"]:
        effects.loc[update_mask, col] = effects.loc[update_mask, f"new_{col}"]
    effects = effects.drop(["new_mean", "new_std"], axis=1)

    # update and attribute
    cascade_loc.effects_prior = effects
    cascade_loc.datastore["effect"] = cascade_loc.effects_prior


def adjust_to_within_bounds(df):
    """
    if the new beta mean we're applying exceeds the upper or
    lower bounds by floating point error, adjust the mean to equal the bound

    CENCOM-4177
    """
    exceeds_upper_bound = df.new_mean.notnull() & (df.new_mean > df.upper)
    exceeds_lower_bound = df.new_mean.notnull() & (df.new_mean < df.lower)
    verify_tolerances(df, "upper", exceeds_upper_bound)
    verify_tolerances(df, "lower", exceeds_lower_bound)
    update_mean(df, "upper", exceeds_upper_bound)
    update_mean(df, "lower", exceeds_lower_bound)
    return df


def verify_tolerances(df, col, mask, tolerance=1e-4):
    """assert df.new_mean column is within tolerance of col"""
    assert np.allclose(df[mask]["new_mean"], df[mask][col], atol=tolerance)


def update_mean(df, col, mask):
    """update new_mean to equal col, given a mask"""
    df.loc[mask, "new_mean"] = df.loc[mask, col]


def drop_non_standard_locations(cascade_loc, release_id) -> None:
    """
    Drop locations from this cascade_loc's input data if they are not in the
    set of standard locations.

    Arguments:
        cascade_loc (cascade_ode.legacy.drill.cascade_loc):
        release_id (int): release to determine set of standard locations

    Returns:
        None
    """
    log = logging.getLogger(__name__)
    locs = get_standard_locations(release_id=release_id)

    # cascade data has location_id as a string, so lets match that
    locs = {str(x) for x in locs}

    log.info(
        "{} rows of data before dropping non-standard locations".format(len(cascade_loc.data))
    )
    cascade_loc.data = cascade_loc.data[cascade_loc.data.location_id.isin(locs)]
    log.info(
        "{} rows of data after dropping non-standard locations".format(len(cascade_loc.data))
    )

    # This involves updating the data file as well as the data attribute
    cascade_loc.datastore["data"] = cascade_loc.data

    if cascade_loc.data.empty:
        raise RuntimeError(
            "After dropping non-standard locations from input data, no rows"
            " of data remained. Cannot run dismod on no data! In other words,"
            " input data must contain at least some data from standard "
            "locations"
        )


def get_standard_locations(release_id: int) -> Set[int]:
    """
    returns a set of location ids that represents the set of standard
    locations for the given release_id.
    """
    standard_location_set_id = 101
    std_loc_df = get_location_metadata(
        location_set_id=standard_location_set_id, release_id=release_id
    )
    std_locs = set(std_loc_df.location_id.tolist())

    # the set of standard locations should be all locations in set 101
    # + any countries above subnationals that are in set 101
    # CENCOM-4176
    all_loc_df = get_location_metadata(
        location_set_id=Demographics.DEFAULT_LOCATION_SET_ID, release_id=release_id
    )

    countries_to_add = set(
        all_loc_df[
            (~all_loc_df.location_id.isin(std_locs)) & (all_loc_df.location_type == "admin0")
        ].location_id
    )

    all_locs = std_locs | countries_to_add
    return all_locs
