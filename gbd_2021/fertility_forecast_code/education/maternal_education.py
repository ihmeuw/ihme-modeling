"""This module is used to calculate maternal education. The get_maternal_edu
function returns both maternal education, and education with maternal edu filled
in for child age-groups.
"""
import logging

import xarray as xr

from fbd_core.etl import Aggregator, expand_dimensions, resample
from fbd_core.file_interface import FBDPath, open_xr

LOGGER = logging.getLogger(__name__)

SEXES = (1, 2)
FEMALE_SEX_ID = 2

CHILD_AGE_GROUPS = tuple(range(2, 8))
MAT_AGE_GROUPS = tuple(range(8, 15))
MAT_AGE_GROUP_ID = 198


def get_maternal_edu(education, gbd_round_id,
                     past_future, pop_version, location_ids):
    """Recalculate maternal education, which according to the education team is
    the education of women of age-group-IDs 8 to 14 multiplied by their
    age-weights and then summed over age.

    Only the age weights of groups 8 to 14 are kept, and then are rescaled so
    that the sum of those age weights is 1.

    Args:
        education (xarray.DataArray):
            Education data. Needs dimensions `age_group_id` and `sex_id`,
            but probably also has dimensions `location_id`, `draw`, `year_id`
            and maybe `scenario`.
        gbd_round_id (int):
            Numeric ID for the GBD round. Used to get the age-weights for the
            round from the database.
        past_pop_version (str):
            Version of past population to use for maternal education
            aggregation.
        future_pop_version (str):
            Version of future population to use for maternal education
            aggregation.
    Returns:
        (tuple[xarray.DataArray, xarray.DataArray]):
            * The first `xarray.DataArray` of the tuple is educational
              attainment for all age-groups and sexes. However, children that
              are too young to have their own education are filled in with
              maternal education.
            * The second `xarray.DataArray` of the tuple is maternal education
              -- only for the maternal age-group, given by `MAT_AGE_GROUP_ID`
              and females, given by `FEMALE_SEX_ID`.
    """

    pop_path = FBDPath("")  # Path removed for security reasons

    pop = open_xr(pop_path / "population.nc").data.sel(
        age_group_id=list(MAT_AGE_GROUPS), sex_id=FEMALE_SEX_ID,
        location_id=list(location_ids)
        )

    LOGGER.debug("Adding up education of moms to get maternal education.")
    mat_slice_edu = education.sel(sex_id=FEMALE_SEX_ID,
                                  age_group_id=list(MAT_AGE_GROUPS),
                                  location_id=list(location_ids))

    agg = Aggregator(pop)
    mat_edu = agg.aggregate_ages(list(MAT_AGE_GROUPS), MAT_AGE_GROUP_ID,
                                 data=mat_slice_edu).rate

    # age_group_id must be dropped. If not, expand_dimensions will broadcast
    # NaNs instead of our data into the new child age_group_id values.
    mat_edu_expanded = expand_dimensions(mat_edu.drop("age_group_id").squeeze(),
                                         sex_id=list(SEXES),
                                         age_group_id=list(CHILD_AGE_GROUPS))

    LOGGER.debug("Adding maternal education for both sexes and child age "
                 "groups to education data array.")
    # Even if ``education`` has data for child age groups, combine first will
    # make sure that the newly calculated maternal education will be used
    # instead.
    return mat_edu_expanded.combine_first(education), mat_edu
