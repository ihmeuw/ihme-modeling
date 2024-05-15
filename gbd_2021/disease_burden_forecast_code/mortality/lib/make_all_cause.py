"""FHS Pipeline Mortality Aggregated Over Causes.

A module that makes a new version of mortality that
consolidates data from externally (e.g. shocks and HIV) and internally modeled
causes (e.g. cvd_ihd -- things modeled with GK). Parent causes to shocks and HIV
are re-aggregated to incorporate HIV and shocks data.

This module should be run every time a new version of squeezed mortality,
shocks, or HIV is created. This new version directory will contain
**TRUE** all-cause mortality rate and risk-attributable burden.

This script takes squeezed mortality data from
``FILEPATH``,

hiv data from
``FILEPATH``,

and shocks data from
``FILEPATH``,

and creates a symlink to the files, or re-aggregated files as needed, in
``FILEPATH``.

The exported ``_all.nc`` file is meant for the population code that produces
``population.nc`` for the subsequent pipeline.
"""

from typing import Iterable, Optional, Set, Tuple

import pandas as pd
import xarray as xr
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_database_interface.lib.constants import (
    CauseConstants,
    DimensionConstants,
    StageConstants,
)
from fhs_lib_database_interface.lib.query import cause
from fhs_lib_file_interface.lib.provenance import ProvenanceManager
from fhs_lib_file_interface.lib.version_metadata import (
    FHSDirSpec,
    FHSFileSpec,
    VersionMetadata,
)
from fhs_lib_file_interface.lib.versioning import validate_versions_scenarios
from fhs_lib_file_interface.lib.xarray_wrapper import (
    copy_xr_setting_scenario,
    open_xr_scenario,
    save_xr_scenario,
)
from tiny_structured_logger.lib.fhs_logging import get_logger

logger = get_logger()

HIV_ACAUSES = ("hiv",)
OTH_PAND = "_oth_pand"
NTD = "_ntd"
MALARIA = "malaria"


def add_external_causes(
    covid_scalar_acauses: Iterable[str],
    covid_scalar_version: Optional[VersionMetadata],
    covid_version: VersionMetadata,
    final_version: VersionMetadata,
    gbd_round_id: int,
    draws: int,
    hiv_version: VersionMetadata,
    shocks_version: VersionMetadata,
    squeezed_mx_version: VersionMetadata,
    output_scenario: Optional[int],
) -> None:
    """Copy externally-modeled causes into the ``final_version``."""
    # Validate the final_version against the output_scenario
    validate_versions_scenarios(
        versions=[final_version],
        output_scenario=output_scenario,
        output_epoch_stages=[("future", "death")],
    )

    all_deaths_causes = _get_all_deaths_causes(
        covid_version=covid_version, gbd_round_id=gbd_round_id
    )

    squeezed_mx_acause, _, _ = _identify_causes_for_aggregation(
        covid_scalar_acauses=covid_scalar_acauses,
        covid_scalar_version=covid_scalar_version,
        covid_version=covid_version,
        gbd_round_id=gbd_round_id,
        all_deaths_causes=all_deaths_causes,
    )

    for acause in CauseConstants.SHOCKS_ACAUSES:
        _include_acause_in_version(shocks_version, final_version, acause, draws)

    for acause in HIV_ACAUSES:
        _include_acause_in_version(hiv_version, final_version, acause, draws)

    _include_acause_in_version(
        covid_version, final_version, CauseConstants.COVID_ACAUSE, draws
    )

    for acause in squeezed_mx_acause:
        _include_acause_in_version(squeezed_mx_version, final_version, acause, draws)

    _add_malaria_into_ntd(squeezed_mx_version, final_version)

    if covid_scalar_version is not None:
        _apply_covid_scalar(
            covid_scalar_version, final_version, squeezed_mx_version, covid_scalar_acauses
        )


def _get_all_deaths_causes(
    covid_version: VersionMetadata,
    gbd_round_id: int,
) -> Set[str]:
    """Return the set of all deaths causes, having removed any special causes."""
    all_deaths_causes = set(
        cause.get_stage_cause_set(
            StageConstants.DEATH,
            include_aggregates=True,
            gbd_round_id=gbd_round_id,
        )
    )

    # Remove OTH_PAND from cause hierarchy until we get its data from the covid team (do not
    # know the date). We'll have this cause in our results for the DALY paper submission.
    all_deaths_causes -= {OTH_PAND}

    return all_deaths_causes


def _identify_causes_for_aggregation(
    covid_scalar_acauses: Iterable[str],
    covid_scalar_version: Optional[VersionMetadata],
    covid_version: VersionMetadata,
    gbd_round_id: int,
    all_deaths_causes: Set[str],
) -> Tuple[Set[str], Set[str], pd.DataFrame]:
    """Return the collections of causes for aggregation.

    Returns:
        Tuple[Set[str], Set[str], pd.DataFrame]: the squeezed_mx_acause, cause_hierarchy_set,
            and external_parents_acause collections, in that order
    """
    (
        cause_hierarchy_external_model,
        cause_hierarchy_set,
    ) = _get_cause_hierarchy_external_model_and_set(
        gbd_round_id=gbd_round_id,
        covid_scalar_acauses_externally_modeled=covid_scalar_version is not None,
        covid_scalar_acauses=covid_scalar_acauses,
        covid_externally_modeled=True,
    )
    covid_cause_set = {CauseConstants.COVID_ACAUSE}
    covid_scalar_acauses_set = _get_covid_scalar_cause_set(
        covid_scalar_acauses=covid_scalar_acauses,
        covid_scalar_version=covid_scalar_version,
    )

    external_parents_acause, squeezed_mx_acause = _split_external_from_squeezed_causes(
        cause_hierarchy_external_model=cause_hierarchy_external_model,
        all_deaths_causes=all_deaths_causes,
        covid_cause_set=covid_cause_set,
        covid_scalar_acauses_set=covid_scalar_acauses_set,
    )

    return squeezed_mx_acause, external_parents_acause, cause_hierarchy_set


def _include_acause_in_version(
    input_version: VersionMetadata,
    output_version: VersionMetadata,
    acause: str,
    draws: int,
) -> None:
    """Copy the acause xarray file from input_version to output_version.

    As an optimization, if both have the same scenario configuration, we can just symlink.
    """
    input_filespec = FHSFileSpec(input_version, f"{acause}.nc")
    # construct dirspec for output version
    output_dirspec = FHSDirSpec(version_metadata=output_version)
    if output_version.scenario != input_version.scenario:
        copy_xr_setting_scenario(
            input_filespec=input_filespec,
            output_dirspec=output_dirspec,
            draws=draws,
            new_filename=None,
        )
    else:
        logger.info(f"Symlinking: {input_filespec.data_path()} into new path.")
        output_filespec = FHSFileSpec(output_version, f"{acause}.nc")
        ProvenanceManager.symlink(input_filespec, output_filespec, force=True)


def _add_malaria_into_ntd(
    squeezed_mx_version: VersionMetadata, final_version: VersionMetadata
) -> None:
    """Create an ntd output by summing malaria and ntd inputs.

    Why? Malaria is treated as a lvl2 cause in stage 2 (not a child of _ntd), now need to
    change it back to a lvl3 cause as a child for _ntd, add malaria to _ntd.
    """
    ntd = open_xr_scenario(FHSFileSpec(squeezed_mx_version, f"{NTD}.nc"))
    malaria = open_xr_scenario(FHSFileSpec(squeezed_mx_version, f"{MALARIA}.nc"))

    def square_and_sum(a: xr.DataArray, b: xr.DataArray) -> xr.DataArray:
        return sum(data.fillna(0.0) for data in xr.broadcast(a, b))

    ntd_sum = square_and_sum(ntd, malaria)

    save_xr_scenario(
        ntd_sum,
        FHSFileSpec(final_version, f"{NTD}.nc"),
        metric="rate",
        space="identity",
    )


def _apply_covid_scalar(
    covid_scalar_version: VersionMetadata,
    final_version: VersionMetadata,
    squeezed_mx_version: VersionMetadata,
    covid_scalar_acauses: Iterable[str] = (),
) -> None:
    """Apply scalar to pertussis, measles, maternal_indirect and lri."""
    for acause in covid_scalar_acauses:
        acause_da = open_xr_scenario(FHSFileSpec(squeezed_mx_version, f"{acause}.nc"))
        covid_scalar = open_xr_scenario(FHSFileSpec(covid_scalar_version, f"{acause}.nc"))

        covid_scalar = covid_scalar.sel(
            location_id=list(
                set(acause_da.location_id.values) & set(covid_scalar.location_id.values)
            ),
            age_group_id=acause_da.age_group_id.values,
        )

        covid_scalar = resample(covid_scalar, len(acause_da.draw.values))
        if acause == "lri":
            hib_filename = "lri_hib.nc"
            pneumo_filename = "lri_pneumo.nc"
            non_pneumo_non_hib_filename = "lri_non_pneumo_non_hib.nc"

            # read in lri children
            non_pneumo_non_hib = open_xr_scenario(
                FHSFileSpec(squeezed_mx_version, non_pneumo_non_hib_filename)
            )
            pneumo = open_xr_scenario(FHSFileSpec(squeezed_mx_version, pneumo_filename))
            hib = open_xr_scenario(FHSFileSpec(squeezed_mx_version, hib_filename))

            hib = resample(hib, len(acause_da.draw.values))
            pneumo = resample(pneumo, len(acause_da.draw.values))
            non_pneumo_non_hib = resample(non_pneumo_non_hib, len(acause_da.draw.values))

            # subtract the difference from non_pneumo_non_hib
            scaled_acause_da = acause_da * covid_scalar
            diff = acause_da - scaled_acause_da
            remaining_draws = non_pneumo_non_hib - diff
            scaled_non_pneumo_non_hib = xr.where(remaining_draws >= 0, remaining_draws, 0)

            # squeeze hib and pneumo to the scaled lri
            children_broadcasted = xr.broadcast(hib, pneumo, scaled_non_pneumo_non_hib)
            children_broadcasted = [data.fillna(0.0) for data in children_broadcasted]
            children_sum = sum(children_broadcasted)
            ratio = scaled_acause_da / children_sum
            squeezed_hib = ratio * hib
            squeezed_pneumo = ratio * pneumo
            squeezed_non_pneumo_non_hib = ratio * scaled_non_pneumo_non_hib

            year_other = [
                x for x in hib.year_id.values if x not in squeezed_hib.year_id.values
            ]
            hib_other = hib.sel(year_id=year_other)
            pneumo_other = pneumo.sel(year_id=year_other)
            non_pneumo_non_hib_other = non_pneumo_non_hib.sel(year_id=year_other)
            squeezed_hib_all_years = xr.concat([squeezed_hib, hib_other], dim="year_id")
            squeezed_pneumo_all_years = xr.concat(
                [squeezed_pneumo, pneumo_other], dim="year_id"
            )
            squeezed_non_pneumo_non_hib_all_years = xr.concat(
                [squeezed_non_pneumo_non_hib, non_pneumo_non_hib_other], dim="year_id"
            )

            hib_file_spec = FHSFileSpec(final_version, hib_filename)
            pneumo_file_spec = FHSFileSpec(final_version, pneumo_filename)
            non_pneumo_non_hib_file_spec = FHSFileSpec(
                final_version, non_pneumo_non_hib_filename
            )
            ProvenanceManager.remove(hib_file_spec)
            ProvenanceManager.remove(pneumo_file_spec)
            ProvenanceManager.remove(non_pneumo_non_hib_file_spec)

            save_xr_scenario(
                squeezed_hib_all_years,
                hib_file_spec,
                metric="rate",
                space="identity",
            )
            save_xr_scenario(
                squeezed_pneumo_all_years,
                pneumo_file_spec,
                metric="rate",
                space="identity",
            )
            save_xr_scenario(
                squeezed_non_pneumo_non_hib_all_years,
                non_pneumo_non_hib_file_spec,
                metric="rate",
                space="identity",
            )

        else:
            scaled_acause_da = acause_da * covid_scalar

        acause_other = acause_da.sel(
            year_id=[
                x for x in acause_da.year_id.values if x not in scaled_acause_da.year_id.values
            ]
        )
        scaled_acause_all_years = xr.concat([scaled_acause_da, acause_other], dim="year_id")
        save_xr_scenario(
            scaled_acause_all_years,
            FHSFileSpec(final_version, f"{acause}.nc"),
            metric="rate",
            space="identity",
        )
        save_xr_scenario(
            acause_da,
            FHSFileSpec(final_version, f"{acause}_original.nc"),
            metric="rate",
            space="identity",
        )


def _get_cause_hierarchy_external_model_and_set(
    gbd_round_id: int,
    covid_scalar_acauses_externally_modeled: bool,
    covid_externally_modeled: bool,
    covid_scalar_acauses: Iterable[str] = (),
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Query db and get cause hierarchy external model and set.

    Args:
        gbd_round_id (int): The GBD round to query.
        covid_scalar_acauses_externally_modeled (bool): Include the "covid scalar causes"
            amongst the external-modeled ones.
        covid_scalar_acauses (Tuple[str, ...]): Which ones are the "covid scalar causes"?
        covid_externally_modeled (bool): Include lri_corona amongst the external-modeled
            causes.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: cause hierarchy external model
            DataFrame and cause hierarchy set DataFrame.
    """
    cause_hierarchy_set = cause.get_cause_hierarchy(gbd_round_id)

    if not covid_externally_modeled:
        cause_hierarchy_set = cause_hierarchy_set[
            cause_hierarchy_set.acause != CauseConstants.COVID_ACAUSE
        ]

    relevant_external_modeled_acauses = CauseConstants.EXTERNAL_MODELED_ACAUSES
    if covid_externally_modeled:
        relevant_external_modeled_acauses += (CauseConstants.COVID_ACAUSE,)
    if covid_scalar_acauses_externally_modeled:
        relevant_external_modeled_acauses += covid_scalar_acauses

    # Note cause_external_modeled_subset may include OTH_PAND, even if it is removed from
    # cause_hierarchy_set below.
    cause_external_modeled_subset = cause_hierarchy_set.query(
        f"acause in {tuple(relevant_external_modeled_acauses)}"
    )

    # Remove it from cause hierarchy
    cause_hierarchy_set = cause_hierarchy_set[cause_hierarchy_set.acause != OTH_PAND]

    # Note in the caller this is _only_ used to produce a set of acauses that are parent
    # acauses.
    cause_hierarchy_external_model = _create_cause_to_parent_map(
        cause_hierarchy_set, cause_external_modeled_subset
    )

    return cause_hierarchy_external_model, cause_hierarchy_set


def _create_cause_to_parent_map(
    cause_id_to_acause_map: pd.DataFrame, acauses_with_path_to_top_parent: pd.DataFrame
) -> pd.DataFrame:
    """Translate acauses_with_path_to_top_parent to a straightforward child-parent map.

    Given a path_to_top_parent column, return a DataFrame mapping each acause to its parent. To
    do this, we need a caust_id-to-acause mapping, which must be provided by the first arg.
    """
    cause_hierarchy_external_model = acauses_with_path_to_top_parent[
        [DimensionConstants.ACAUSE, DimensionConstants.PATH_TO_TOP_PARENT]
    ]

    # Re-structure external cause hierarchy dataframe so each row is one cause-ancestor pair.
    cause_hierarchy_external_model[DimensionConstants.PATH_TO_TOP_PARENT] = (
        cause_hierarchy_external_model[DimensionConstants.PATH_TO_TOP_PARENT].str.split(",")
    )
    s = (
        cause_hierarchy_external_model[DimensionConstants.PATH_TO_TOP_PARENT]
        .apply(pd.Series, 1)
        .stack()
    )
    s.index = s.index.droplevel(-1)
    s.name = DimensionConstants.PARENT_ID_COL
    s = pd.to_numeric(s)
    del cause_hierarchy_external_model[DimensionConstants.PATH_TO_TOP_PARENT]
    cause_hierarchy_external_model = cause_hierarchy_external_model.join(s)

    # Merge name of ancestor causes to external cause hierarchy dataframe
    cause_hierarchy_external_model = pd.merge(
        cause_hierarchy_external_model,
        cause_id_to_acause_map[
            [DimensionConstants.CAUSE_ID, DimensionConstants.ACAUSE]
        ].rename(
            columns={
                DimensionConstants.CAUSE_ID: DimensionConstants.PARENT_ID_COL,
                DimensionConstants.ACAUSE: DimensionConstants.PARENT_ACAUSE,
            }
        ),
    )

    return cause_hierarchy_external_model


def _get_covid_scalar_cause_set(
    covid_scalar_acauses: Iterable[str],
    covid_scalar_version: VersionMetadata,
) -> Set[str]:
    """Return the set of COVID scalar cause set to include in computation."""
    if covid_scalar_version is not None:
        covid_scalar_acauses_set = set(covid_scalar_acauses)
    else:
        covid_scalar_acauses_set = set()

    return covid_scalar_acauses_set


def _split_external_from_squeezed_causes(
    cause_hierarchy_external_model: pd.DataFrame,
    all_deaths_causes: Set[str],
    covid_cause_set: Set[str],
    covid_scalar_acauses_set: Set[str],
) -> Tuple[Set[str], Set[str]]:
    """Return the sets of external and squeezed acauses."""
    external_parents_acause = (
        set(cause_hierarchy_external_model[DimensionConstants.PARENT_ACAUSE].tolist())
        - set(CauseConstants.SHOCKS_ACAUSES)
        - set(HIV_ACAUSES)
        - covid_cause_set
        - covid_scalar_acauses_set
    )
    squeezed_mx_acause = (
        set(all_deaths_causes)
        - set(CauseConstants.SHOCKS_ACAUSES)
        - set(HIV_ACAUSES)
        - covid_cause_set
        - set(external_parents_acause)
        - covid_scalar_acauses_set
        - {NTD}
    )

    return external_parents_acause, squeezed_mx_acause
