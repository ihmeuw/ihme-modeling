"""This script computes aggregated acause specific PAFs and scalars.

Example call:

.. code:: bash

    python compute_scalar.py --acause whooping --version 20180321_arc_log \
    --gbd-round-id 4 --years 1990:2017:2040

Outputs:

1) Risk-acause specific scalar.  Exported to the input paf/{version}
2) Acause specific scalars.  Exported to scalar/{version}
"""

import gc
import os
from collections import defaultdict
from typing import Any, Dict, List, Optional

import numpy as np
import xarray as xr
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_database_interface.lib.query.risk import get_risk_hierarchy
from fhs_lib_file_interface.lib.query import mediation
from fhs_lib_file_interface.lib.version_metadata import FHSFileSpec, VersionMetadata
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr_scenario, save_xr_scenario
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib.fhs_logging import get_logger

from fhs_pipeline_scalars.lib.constants import PAFConstants
from fhs_pipeline_scalars.lib.forecasting_db import get_most_detailed_acause_related_risks
from fhs_pipeline_scalars.lib.utils import data_value_check, save_paf

logger = get_logger()


def read_paf(
    acause: str,
    risk: str,
    gbd_round_id: int,
    past_or_future: str,
    version: str,
    draws: int,
    years: YearRange,
    reference_only: bool,
    unmediated: bool = False,
    custom_paf: Optional[str] = None,
) -> xr.DataArray:
    """Read past or forecast PAF.

    Args:
        acause (str): cause name.
        risk (str): risk name.
        gbd_round_id (int): gbd round id.
        past_or_future (str): "past" or "forecast".
        version (str): str indiciating folder where data comes from.
        draw (int): number of draws to keep.
        years: past_start:forecast_start:forecast_end.
        reference_only (bool): whether to compute reference only.
        unmediated (bool): whether to read in unmediated PAF. Defaults to False.
        custom_paf (Optional[str]): Version of custom PAFs.

    Returns:
        paf (xr.DataArray): data array of PAF.
    """
    input_file_name = f"{acause}_{risk}"
    if unmediated:
        input_file_name += "_unmediated"

    input_paf_file_spec = _decide_paf_file_spec_to_use(
        custom_paf=custom_paf,
        gbd_round_id=gbd_round_id,
        input_file_name=input_file_name,
        past_or_future=past_or_future,
        version=version,
    )

    paf = resample(open_xr_scenario(file_spec=input_paf_file_spec), draws)

    paf = paf.where(paf.year_id <= years.forecast_end, drop=True)

    if reference_only and ("scenario" in paf.dims):
        paf = paf.sel(scenario=[0])

    # some dimensional pruning before returning
    if "acause" in paf.coords:
        if "acause" in paf.dims:
            paf = paf.squeeze("acause")
        paf = paf.reset_coords(["acause"], drop=True)

    if "rei" in paf.coords:
        if "rei" in paf.dims:
            paf = paf.squeeze("rei")
        paf = paf.reset_coords(["rei"], drop=True)

    return paf.mean("draw")


def _decide_paf_file_spec_to_use(
    gbd_round_id: int,
    input_file_name: str,
    past_or_future: str,
    version: str,
    custom_paf: Optional[str],
) -> FHSFileSpec:
    """Decide the PAF file spec to use between the PAF and optional customized PAF inputs.

    If the ``custom_paf`` was specified and contains the ``filename``,
    then that will be used. If the ``version`` contains the ``filename``
    then that will be used. Otherwise an error will be raised if the file
    cannot be found between the two versions.
    """
    reference_paf_version_metadata = VersionMetadata.make(
        data_source=gbd_round_id,
        epoch=past_or_future,
        stage="paf",
        version=version,
    )

    reference_paf_file_spec = FHSFileSpec(
        version_metadata=reference_paf_version_metadata,
        sub_path=("risk_acause_specific",),
        filename=f"{input_file_name}.nc",
    )

    custom_paf_file_spec = None
    if custom_paf:
        custom_paf_version_metadata = VersionMetadata.make(
            data_source=gbd_round_id,
            epoch=past_or_future,
            stage="paf",
            version=custom_paf,
        )

        custom_paf_file_spec = FHSFileSpec(
            version_metadata=custom_paf_version_metadata,
            sub_path=("risk_acause_specific",),
            filename=f"{input_file_name}.nc",
        )

    if custom_paf_file_spec and custom_paf_file_spec.data_path().PFN().exists():
        input_paf_file_spec = custom_paf_file_spec
    elif reference_paf_file_spec.data_path().PFN().exists():
        input_paf_file_spec = reference_paf_file_spec
    else:
        raise FileNotFoundError(
            f"The target file {input_file_name} could not be read from "
            f"either the custom PAF version or the reference PAF version."
        )

    return input_paf_file_spec


def ancestor_descendant_risks(
    most_detailed_risks: List[str], gbd_round_id: int
) -> Dict[str, List[str]]:
    """Collect mapping of parent risk to all most-detailed descendant risks.

    Given some most-detailed risks, make a dict of all their ancestor risks, mapping to a list
    of their most-detailed risks underneath them.

    Returns a dictionary. key: parent risk; value: list of most-detailed risks below that one.

    Args:
        most_detailed_risks (List[str]): list of risks whose ancestors we want to know.
        gbd_round_id (int): the GBD Round ID

    Returns:
        Dict[str, List[str]]: a dict where keys are risks, and values are sub-risks.
            Ex: {'_env': ['air_hap', 'wash', ...],
                 'metab': ['metab_fpg', 'metab_bmi', ...], '_behav': ['activity',
                 'nutrition_child', ...]}

    Raises:
        TypeError: if path_to_top_parent for a particular risk is not recorded
            as a string type.  That would most likely be error on the db.
    """
    risk_table = get_risk_hierarchy(gbd_round_id)
    risk_id_dict = dict(risk_table[["rei_id", "rei"]].values)

    result = defaultdict(list)

    for leaf_risk in most_detailed_risks:
        risk_specific_metadata = risk_table.query("rei == @leaf_risk")

        if not risk_specific_metadata.empty:
            # comma-delimited string, like "169,202,82,83", or None
            path_to_root = risk_specific_metadata["path_to_top_parent"].item()
        else:
            # If there is no metadata for the risk (e.g. for a vaccine in
            # GBD2016 we didn't aggregate to all-risk from vaccines), then
            # return an empty dict.
            return result

        # NOTE the vaccines (hib, pcv, rota, measles, dtp3) currently have
        # "None" listed in their path_to_top_parent.
        if path_to_root:  # not None, must be string
            if type(path_to_root) is not str:
                raise TypeError(f"{path_to_root} is not of str type")
            # first of list is "_all", last of list is itself
            _all_to_self_list = path_to_root.split(",")

            for ancestor_id in _all_to_self_list[:-1]:  # keep _all, ignore self
                ancestor_risk = risk_id_dict[int(ancestor_id)]
                result[ancestor_risk].append(leaf_risk)

    return result


def aggregate_paf(
    acause: str,
    risks: List[str],
    gbd_round_id: int,
    past_or_future: str,
    version: str,
    draws: int,
    years: YearRange,
    reference_only: bool,
    cluster_risk: Optional[str] = None,
    custom_paf: Optional[str] = None,
) -> Optional[xr.DataArray]:
    """Aggregate PAFs through mediation.

    Args:
        acause (str): acause.
        risks (List[str]): set of risks associated with acause.
        gbd_round_id (int): gbd round id.
        past_or_future (str): 'past' or 'future'.
        version (str): indicating folder where data comes from/goes to.
        draws (int): number of draws.
        years (YearRange): past_start:forecast_start:forecast_end.
        reference_only (bool): if true, only return the reference scenario paf.
        cluster_risk (Optional[str]): whether this is a cluster risk. Impacts the directory
            where it's saved.
        custom_paf (Optional[str]): Version of custom PAFs.

    Raises:
        ValueError: if there are no risks for a given acause

    Returns:
        paf_aggregated (Optional[xr.DataArray]): dataarray of aggregated PAF if
            ``cluster_risk`` parameter is specified.
    """
    logger.info(f"Start aggregating {past_or_future} PAF:")

    logger.info(f"Acause: {acause}, Risks: {risks}, Cluster Risk: {cluster_risk}")

    if len(risks) == 0:
        error_message = f"0 risks for acause {acause}"
        logger.error(error_message)
        raise ValueError(error_message)

    # We loop over each risk, determine whether to use its total or unmediated
    # PAF based on the mediation matrix, and then compute its contribution.
    med = mediation.get_mediation_matrix(gbd_round_id)  # the mediation matrix

    # If you enumerate risks instead of keeping tracked of not skipped
    # reis, you run into an error if the first rei is skipped
    # As one_minus_paf_product is never defined
    reis_previously_aggregated = []

    for i, rei in enumerate(risks):
        # if cluster_risk is specified, risks is just the subset of this
        # cause's risks that fall under the cluster_risk (_env, _metab, etc.)
        # If cluster_risk is not specified, that risks are all the risks
        # associated with this cause.
        logger.info(f"Doing risk {rei}")

        unmediated = False  # Set as False for now, may update to True below

        if acause in med["acause"].values and rei in med["rei"].values:
            mediator_matrix = med.sel(acause=acause, rei=rei)

            # if there is *any* mediation, use pre-saved unmediated PAF
            if (mediator_matrix > 0).any():
                unmediated = True

        paf = read_paf(
            acause=acause,
            risk=rei,
            gbd_round_id=gbd_round_id,
            past_or_future=past_or_future,
            version=version,
            draws=draws,
            years=years,
            reference_only=reference_only,
            unmediated=unmediated,
            custom_paf=custom_paf,
        )

        if not reis_previously_aggregated:
            logger.debug("Index is 0. Starting the paf_prod.")
            one_minus_paf_product = 1 - paf
        else:
            logger.debug(f"Index is {i}.")
            # NOTE there no straight-forward way to check if two dataarrays
            # have the same coordinates, so we broadcast indiscriminately
            paf, one_minus_paf_product = xr.broadcast(paf, one_minus_paf_product)

            paf = paf.where(np.isfinite(paf)).fillna(0)
            one_minus_paf_product = one_minus_paf_product.where(
                np.isfinite(one_minus_paf_product)
            ).fillna(1)

            one_minus_paf_product = (1 - paf) * one_minus_paf_product
            del paf
            gc.collect()
        reis_previously_aggregated.append(rei)

    logger.info(f"Finished computing PAF for {acause}")

    paf_aggregated = 1 - one_minus_paf_product

    del one_minus_paf_product
    gc.collect()

    paf_aggregated = paf_aggregated.clip(
        min=PAFConstants.PAF_LOWER_BOUND, max=PAFConstants.PAF_UPPER_BOUND
    )

    save_paf(
        paf=paf_aggregated,
        gbd_round_id=gbd_round_id,
        past_or_future=past_or_future,
        version=version,
        acause=acause,
        cluster_risk=cluster_risk,
    )

    if cluster_risk:
        return None
    else:
        return paf_aggregated


def compute_scalar(
    acause: str,
    version: str,
    gbd_round_id: int,
    no_update_past: bool,
    save_past_data: bool,
    draws: int,
    years: YearRange,
    reference_only: bool,
    custom_paf: Optional[str],
    **kwargs: Any,
) -> None:
    """Compute and save scalars for acause, given input paf version.

    Args:
        acause (str): cause to compute scalars for
        version (str): date/version string pointing to folder to pull data from
        gbd_round_id (int): gbd round id.
        no_update_past (bool): whether to overwrite past scalars.
        draws (int): number of draws to keep.
        years (YearRange): past_start:forecast_start:forecast_end.
        save_past_data (bool): if true, save files for past data in FILEPATH
        reference_only (bool): if true, only return the reference scenario paf.
        custom_paf (Optional[str]): Version of custom PAFs.
        kwargs (dict[Any]): dictionary that captures all redundant kwargs.
    """
    all_most_detailed_risks = get_most_detailed_acause_related_risks(acause, gbd_round_id)

    if not all_most_detailed_risks:
        logger.info(f"{acause} does not have any cause-risk pafs")
        return None

    if save_past_data:
        past_and_future_needed = ["past", "future"]
    else:
        past_and_future_needed = ["future"]

    subrisk_map = ancestor_descendant_risks(all_most_detailed_risks, gbd_round_id=gbd_round_id)
    # We require that the above mapping maps non-leaf (aggregate) risks to leaf risks, and so
    # none of the values ("leaf risks") should include a key ("aggregate risk"). If they did,
    # we would double-count by multiplying an aggregate's 1-paf together with the leaves' 1-paf
    # values.
    for values in subrisk_map.values():
        if subrisk_map.keys() & values:
            raise ValueError("Bug: Some risks seem to be both aggregates and most-detailed!?")

    for past_or_future in past_and_future_needed:
        logger.info(f"OH BOY WE'RE DOING THE: {past_or_future}")
        output_version_metadata = VersionMetadata.make(
            data_source=gbd_round_id,
            epoch=past_or_future,
            stage="scalar",
            version=version,
        )
        output_file_spec = FHSFileSpec(
            version_metadata=output_version_metadata, filename=f"{acause}.nc"
        )

        if os.path.exists(str(output_file_spec.data_path())) and no_update_past:
            continue

        # Aggregate PAF for level-1 cluster risks
        # We don't need to use the PAF for scalar.

        for key in subrisk_map.keys():  # loop over all antecedent-risks
            logger.info("Looping over super/parent risks.")

            leaf_descendants = subrisk_map[key]

            if leaf_descendants:
                logger.info(f"Start aggregating cluster risk: {key}")

                aggregate_paf(
                    acause=acause,
                    risks=leaf_descendants,
                    gbd_round_id=gbd_round_id,
                    past_or_future=past_or_future,
                    version=version,
                    draws=draws,
                    years=years,
                    reference_only=reference_only,
                    cluster_risk=key,
                    custom_paf=custom_paf,
                )
                gc.collect()

        # Aggregate PAF for all risks.
        # We need to use the PAF for scalar.
        paf_mediated = aggregate_paf(
            acause=acause,
            risks=all_most_detailed_risks,
            gbd_round_id=gbd_round_id,
            past_or_future=past_or_future,
            version=version,
            draws=draws,
            years=years,
            reference_only=reference_only,
            custom_paf=custom_paf,
        )

        if paf_mediated is None:
            logger.info("No paf_mediated. Early return.")
            return

        scalar = 1.0 / (1.0 - paf_mediated)

        scalar = expand_dimensions(scalar, draw=range(draws))

        del paf_mediated
        gc.collect()

        logger.debug(f"Checking data value for {acause} scalar")
        data_value_check(scalar)  # make sure no NaNs or <0 in dataarray

        save_xr_scenario(
            xr_obj=scalar,
            file_spec=output_file_spec,
            metric="number",
            space="identity",
            acause=acause,
            version=version,
            gbd_round_id=gbd_round_id,
            no_update_past=str(no_update_past),
        )
