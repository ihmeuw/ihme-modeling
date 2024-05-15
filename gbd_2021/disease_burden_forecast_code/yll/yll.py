"""Module for calculating ylls.

"""
import pandas as pd
import xarray as xr
from fhs_lib_database_interface.lib.query import age
from tiny_structured_logger.lib import fhs_logging

logger = fhs_logging.get_logger()

MAX_MEAN_AGE_OF_DEATH = 110  # Model lex is NOT available for mean-age-of-death


def calculate_ylls(
    deaths: xr.DataArray, ax: xr.DataArray, reference_lex: pd.DataFrame, gbd_round_id: int
) -> xr.DataArray:
    r"""Calculate YLLs.

    Calculates YLLs or YLL rates from reference life expectancy, deaths, and mean age of death
    (:math:`a_x`).

    The output is in count or rate space according to the space of ``deaths``: If ``deaths`` is
    in "counts" then the result is YLLs. If ``deaths`` is in rate space (deaths divided by
    population) are given, then this function calculates YLL rates.

    1. Take age-specific-only reference life expectancy with "reference life starting years" at
       0.01 granularity. Call this "ref lex".

    2. Take the standard forecasting/FHS `age_group_years_start` values (usually 5-year grid
       points) + loc-age-sex-specific `ax` (rounded to nearest 0.01 years), giving the mean
       age-of-death within each age-group (i.e. the mean age that people die, when they die
       within their age-group), to match the age start years of ref lex from (1).

    3. The ref lex where `age_group_years_start` + `ax` = reference life starting year (at
       0.01-year granularity) is the ref lex assigned to this age group.

       YLL calculation is then simply

       .. math::

           \mbox{YLL}_{las} = m_{las} \times \bar{e}_{a}

       where :math:`\mbox{YLL}_{las}` is location, age-group, sex specific YLLs (or YLL rates),
       :math:`m_{las}` is location, age-group, sex specific deaths (or mortality rates), and
       :math:`\bar{e}_{a}` is reference life expectancy (at life starting year), which is
       `only` age-group specific, by definition.

    Args:
        deaths (xarray.DataArray):
            location, age-group, sex specific deaths or death rates. Has at least
            ``location_id``, ``age_group_id``, and ``sex_id`` as dimensions.
        ax (xarray.DataArray):
            location, age-group, sex specific mean age of death within each age interval,
            :math:`a_x`, i.e. the mean number of years lived within that interval among those
            that `died` within that interval. Has at least ``location_id``, ``age_group_id``,
            and ``sex_id`` as dimensions.
        reference_lex (pandas.DataFrame):
            Reference life expectancy. Should just be age-specific. Has 2 columns:
            ``age_group_years_start`` and ``Ex``. Use
            ``fhs_lib_database_interface.query.get_reference_lex`` to get this information in
            the necessary format.
        gbd_round_id (int):
            Numeric ID for the GBD round.

    Returns:
        xarray.DataArray:
            location, age-group, sex specific YLLs or YLL rates. Has at least ``location_id``,
            ``age_group_id``, and ``sex_id`` as dimensions.

    Raises:
        RuntimeError:
            When mean age of death as calculated excedes the maximum.
    """
    logger.debug("Entering `calculate_ylls` function")
    age_group_years_start = (
        # TODO: Generally all callers to ages.get_ages are getting the same two columns.
        age.get_ages(gbd_round_id)[["age_group_id", "age_group_years_start"]]
        .set_index(["age_group_id"])
        .to_xarray()["age_group_years_start"]
    )

    # Calculate the mean age-of-death for all of the standard-FHS age-groups by adding the ax
    # (mean-years-lived within the age-group, for those who die within it) and the age-start
    # of each age group.
    mean_age_of_death = (age_group_years_start + ax).round(2)
    mean_age_of_death.name = "mean_age_of_death"
    if (mean_age_of_death > MAX_MEAN_AGE_OF_DEATH).any():
        logger.warning(
            "There aren't reference life expectancies available for age groups that start "
            f"after {MAX_MEAN_AGE_OF_DEATH} years. Clipping data to {MAX_MEAN_AGE_OF_DEATH}"
        )
        mean_age_of_death = mean_age_of_death.clip(max=MAX_MEAN_AGE_OF_DEATH)
    mean_age_of_death_df = mean_age_of_death.to_dataframe().reset_index()

    # Match the mean age-of-death for each FHS age-group with an `age_group_years_start` value
    # from the table of reference life-expectancies. The corresponding life expectancy is the
    # selected reference life for that age-group.
    selected_ref_lex_df = pd.merge(
        mean_age_of_death_df,
        reference_lex,
        left_on="mean_age_of_death",
        right_on="age_group_years_start",
    )
    selected_ref_lex = selected_ref_lex_df.set_index(list(mean_age_of_death.dims)).to_xarray()[
        "Ex"
    ]

    ylls = deaths * selected_ref_lex

    logger.debug("Leaving `calculate_ylls` function")
    return ylls