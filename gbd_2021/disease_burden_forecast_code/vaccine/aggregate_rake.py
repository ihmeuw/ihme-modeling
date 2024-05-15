"""Aggregate or rake subnational estimates for one vaccine.

For all vaccines in India, Brazil, USA and for MCV2 in China, we aggregate
subnational estimates to produce national estimates. For all other vaccines in
China only, we rake the subnational estimates from the national to ensure
location aggregation consistency. This is related to data availability around
national vs subnational vaccine campaign roll-outs from the GBD VPDs team. It
may change over time.


example call:
python aggregate_rake.py \
    --versions FILEPATH \
    -v FILEPATH  \
    --gbd-round-id 6 \
    --vaccine mcv2 \
    agg-rake
"""

from typing import List

import click
import xarray as xr
from fhs_lib_data_aggregation.lib.aggregator import Aggregator
from fhs_lib_database_interface.lib.constants import AgeConstants, SexConstants
from fhs_lib_database_interface.lib.query import location
from fhs_lib_file_interface.lib import xarray_wrapper
from fhs_lib_file_interface.lib.check_input import check_versions
from fhs_lib_file_interface.lib.file_system_manager import FileSystemManager
from fhs_lib_file_interface.lib.os_file_system import OSFileSystem
from fhs_lib_file_interface.lib.versioning import Versions

# Agglocs: USA, India, Brazil
AGGLOCS = [102, 135, 163]
# Rakelocs: China
RAKELOCS = 6


def aggregate_subnationals(
    gbd_round_id: int,
    vaccine: str,
    pop_da: xr.DataArray,
    vaccine_da: xr.DataArray,
) -> xr.DataArray:
    """Aggregate locations for one vaccine.

    Args:
        gbd_round_id (int): GBD round ID.
        vaccine (str): Vaccine name e.g. mcv2
        pop_da (xr.DataArray): Population data
        vaccine_da (xr.DataArray): Vaccine data
    Returns:
        xr.DataArray: DataArray with aggregated locations
    """
    for loc in AGGLOCS:
        # drop nationals for all locations except China:
        vaccine_da = vaccine_da.where(vaccine_da.location_id != loc, drop=True)
    if vaccine == "mcv2":  # only aggregate China subnationals for MCV2
        vaccine_da = vaccine_da.where(vaccine_da.location_id != RAKELOCS, drop=True)

    # aggregate all locations up the location hierarchy
    locs = location.get_location_set(gbd_round_id=gbd_round_id, include_aggregates=True)
    hierarchy = locs.set_index("location_id")["parent_id"].to_xarray()
    correction_factor = (
        location.get_regional_population_scalars(gbd_round_id)
        .set_index(["location_id"])
        .to_xarray()["mean"]
    )
    correction_factor.name = "pop_scalar"
    aggregator = Aggregator(pop_da)
    aggregated_da = aggregator.aggregate_locations(
        data=vaccine_da, loc_hierarchy=hierarchy, correction_factor=correction_factor
    ).rate

    return aggregated_da


def rake_china_subnationals(
    gbd_round_id: int,
    vaccine_da: xr.DataArray,
    pop_da: xr.DataArray,
) -> xr.DataArray:
    """Rake China subnational estimates from the national for all vaccines except MCV2.

    Args:
        gbd_round_id (int): GBD round ID
        vaccine_da (xr.DataArray): Vaccine data with aggregated location estimates
        pop_da (xr.DataArray): Population data

    Returns:
        xr.DataArray: DataArray with raked subnational estimates for China
    """
    # construct raking hierarchy with location ID 6 and subnational location IDs
    loc_table = location.get_location_set(gbd_round_id=gbd_round_id)
    rake_hierarchy = loc_table[["location_id", "parent_id", "level"]]
    china_subnats = loc_table.query(f"parent_id == {RAKELOCS}").location_id.tolist()
    china_subnats.append(RAKELOCS)
    not_china = list(
        set(loc_table.query("level in [3,4]").location_id.tolist()) - set(china_subnats)
    )
    rake_hierarchy = rake_hierarchy.query("location_id == @china_subnats")

    # rake subnational estimates from national
    aggregator = Aggregator(pop_da)
    china_da = vaccine_da.sel(location_id=china_subnats)
    china_da_raked = aggregator.rake_locations(
        data=china_da, location_hierarchy=rake_hierarchy
    )

    raked_da = xr.concat(
        [china_da_raked, vaccine_da.sel(location_id=not_china)], dim="location_id"
    )

    return raked_da


def agg_rake_main(versions: Versions, gbd_round_id: int, vaccine: str) -> xr.DataArray:
    """Main aggregation function.

    Args:
        versions (Versions): Versions object with list of versions
        gbd_round_id (int): Current gbd_round_id
        vaccine (str): Vaccine name e.g. mcv2
    """
    pop_da = xarray_wrapper.open_xr(
        versions.data_dir(gbd_round_id, "future", "population") / "population_agg.nc"
    )
    vaccine_da = xarray_wrapper.open_xr(
        versions.data_dir(gbd_round_id, "future", "vaccine") / f"vacc_{vaccine}.nc"
    )

    # vaccines data has no detailed age/sex IDs
    pop_da = pop_da.sel(
        age_group_id=AgeConstants.VACCINE_AGE_ID,
        sex_id=SexConstants.BOTH_SEX_ID,
    )

    if vaccine != "mcv2":
        # rake subnational estimates for all other vaccines in China only
        vaccine_da = rake_china_subnationals(gbd_round_id, vaccine_da, pop_da)
    aggregated_da = aggregate_subnationals(gbd_round_id, vaccine, pop_da, vaccine_da)

    fname = (
        versions.data_dir(gbd_round_id, "future", "vaccine") / f"_agg_rake/vacc_{vaccine}.nc"
    )
    xarray_wrapper.save_xr(aggregated_da, fname, metric="rate", space="identity")


@click.group()
@click.option(
    "--versions",
    "-v",
    type=str,
    required=True,
    multiple=True,
    help=("Vaccine and Population versions"),
)
@click.option(
    "--vaccine",
    type=str,
    required=True,
    help=("Vaccine name e.g. dtp3"),
)
@click.option(
    "--gbd-round-id",
    required=True,
    type=int,
    help="GBD round ID",
)
@click.pass_context
def cli(
    ctx: click.Context,
    versions: List[str],
    vaccine: str,
    gbd_round_id: int,
) -> None:
    """Main cli function to parse args and pass them to the subcommands.

    Args:
        ctx (click.Context): ctx object.
        versions (List[str]): Population and vaccine versions
        vaccine (str): Vaccine name
        gbd_round_id (int): Current gbd round id
    """
    versions = Versions(*versions)
    check_versions(versions, "future", ["population", "vaccine"])
    ctx.obj = {
        "versions": versions,
        "vaccine": vaccine,
        "gbd_round_id": gbd_round_id,
    }


@cli.command()
@click.pass_context
def agg_rake(ctx: click.Context) -> None:
    """Call to main function.

    Args:
        ctx (click.Context): context object containing relevant params parsed
            from command line args.
    """
    FileSystemManager.set_file_system(OSFileSystem())

    agg_rake_main(
        versions=ctx.obj["versions"],
        vaccine=ctx.obj["vaccine"],
        gbd_round_id=ctx.obj["gbd_round_id"],
    )


if __name__ == "__main__":
    cli()