import click

import gbd
from gbd.constants import sex

from probability_of_death.lib import age_helpers
from probability_of_death.lib import generate as lib_generate
from probability_of_death.lib import location_helpers
from probability_of_death.lib import upload as lib_upload
from probability_of_death.lib import workflow as lib_workflow
from probability_of_death.lib import year_helpers


@click.command()
@click.argument("location_id", type=click.INT)
@click.argument("year_ids", type=click.STRING)
@click.argument("sex_ids", type=click.STRING)
@click.argument("age_group_ids", type=click.STRING)
@click.argument("gbd_round_id", type=click.INT)
@click.argument("decomp_step", type=click.STRING)
@click.argument("deaths_version", type=click.STRING)
def generate(
    location_id: int,
    year_ids: str,
    sex_ids: str,
    age_group_ids: str,
    gbd_round_id: int,
    decomp_step: str,
    deaths_version: str,
) -> None:
    """Run probability of death for a single location.

    Args:
        location_id: ID of location for which to calculate probability of death
        year_ids: IDs of years for which to calculate probabiltiy of death
        sex_ids: IDs of sexes for which to calculate probability of death
        age_group_ids: IDs of aggregate age groups for which to calculate probaility of death
        gbd_round_id: ID of GBD round for which to calculate probability of death
        decomp_step: step for which to calculate probability of death
        deaths_version: whether to use "best" or "latest" compare version with get_outputs
            when pulling deaths
    """
    lib_generate.generate_probability_of_death(
        location_id,
        [int(year_id) for year_id in year_ids.split(",")],
        [int(sex_id) for sex_id in sex_ids.split(",")],
        [int(age_group_id) for age_group_id in age_group_ids.split(",")],
        gbd_round_id,
        decomp_step,
        deaths_version,
    )


@click.command()
@click.argument("gbd_round_id", type=click.INT)
@click.argument("decomp_step", type=click.STRING)
def upload(gbd_round_id: int, decomp_step: str) -> None:
    """Creates and uploads to a GBD process version a probability of death run.

    Args:
        gbd_round_id: ID of the GBD round for this probability of death run
        decomp_step: decomp step for this probability of death run
    """
    lib_upload.upload(gbd_round_id, decomp_step)


@click.command()
@click.argument("decomp_step", type=click.STRING)
@click.argument("gbd_round_id", type=click.INT)
@click.option("--location_set_ids", default=None, type=click.STRING)
@click.option("--year_ids", default=None, type=click.STRING)
@click.option("--sex_ids", default=f"{sex.MALE},{sex.FEMALE},{sex.BOTH}", type=click.STRING)
@click.option("--age_group_ids", default=None, type=click.STRING)
@click.option("--gbd_round_id", default=gbd.constants.GBD_ROUND_ID, type=click.INT)
@click.option("--cluster_project", default="proj_centralcomp", type=click.STRING)
@click.option("--deaths_version", default="best", type=click.Choice(["best", "latest"]))
def workflow(
    decomp_step: str,
    gbd_round_id: int,
    location_set_ids: str,
    year_ids: str,
    sex_ids: str,
    age_group_ids: str,
    cluster_project: str,
    deaths_version: str,
) -> None:
    """Runs probability of death for all locations in a list of location sets.

    Args:
        location_set_ids: IDs of location sets for which to calculate probability of death
        year_ids: IDs of years for which to calculate probabiltiy of death
        sex_ids: IDs of sexes for which to calculate probability of death
        age_group_ids: IDs of age groups for which to calculate probaility of death
        gbd_round_id: ID of GBD round for which to calculate probability of death
        decomp_step: step for which to calculate probability of death
        cluster_project: the cluster project on which to run probability of death
        deaths_version: whether to use "best" or "latest" compare version with get_outputs
            when pulling deaths
    """
    age_group_ids = (
        [int(age_group_id) for age_group_id in age_group_ids.split(",")]
        if age_group_ids
        else age_helpers.get_default_age_groups(gbd_round_id)
    )
    location_set_ids = (
        [int(location_set_id) for location_set_id in location_set_ids.split(",")]
        if location_set_ids
        else location_helpers.get_default_locations(gbd_round_id)
    )
    year_ids = (
        [int(year_id) for year_id in year_ids.split(",")]
        if year_ids
        else year_helpers.get_default_years(gbd_round_id)
    )
    lib_workflow.run_workflow(
        location_set_ids,
        year_ids,
        [int(sex_id) for sex_id in sex_ids.split(",")],
        age_group_ids,
        gbd_round_id,
        decomp_step,
        cluster_project,
        deaths_version,
    )
