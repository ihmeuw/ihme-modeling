from dataclasses import asdict
from enum import Enum
from pathlib import Path
from typing import List, Optional

import click
import pandas as pd

import db_queries
from ihme_cc_cache import FileBackedCacheWriter
from ihme_cc_risk_utils.lib import cli_utils as common_cli_utils

from ihme_cc_paf_aggregator.cli import utils as cli_utils
from ihme_cc_paf_aggregator.lib import (
    constants,
    dbio,
    hierarchy,
    io,
    logging_utils,
    mediation,
    process_version,
    workflow,
)

logger = logging_utils.module_logger(__name__)


class _ArgDocs(Enum):
    """Single source of argument docs, for CLI help and docs.

    As a private class, sphinx-apidoc skips it.
    """

    input_models_path = (
        "If provided, the DataFrame read from this path will provide the best model "
        "versions of input PAFs to use. This is useful when best model versions may have "
        "changed, but we want to reproduce a prior run. The rei_id and input_rei_set_id "
        "arguments should not be provided in this case."
    )
    output_version_id = "New output version to create; defaults to latest + 1."
    mediation_matrix_path = (
        "Path to a mediation matrix file. Should be a csv containing columns rei_id, "
        "med_id, cause_id, mean_mediation."
    )
    hierarchy_path = (
        "Path to a file containing a risk hierarchy to be used for risk aggregation. "
        "Should be a csv containing columns rei_id and parent_id, and optionally cause_id "
        "if subsetting by risk-cause pairs. Cannot be used with rei_set_id."
    )
    rei_set_id = (
        "If no hierarchy_path is specified, REI set ID used in combination with release to "
        "determine the risk hierarchy to be used for risk aggregation. If left blank, "
        "defaults to 'GBD Computation risks for PAF aggregation' (ihme_cc_paf_aggregator.lib."
        f"constants.AGGREGATION_REI_SET_ID = {constants.AGGREGATION_REI_SET_ID})."
    )
    rei_set_version_id = (
        "If no hierarchy_path is specified, this can be set to pull a specific rei set "
        "version for the passed rei set and release. If left blank, defaults to active "
        "version."
    )
    mark_special = (
        "Whether the resulting process version should be marked as special status. This "
        "is done for '3/4/5' runs using the highest quality risk-cause pairs where results "
        "will not be visible in GBD Compare."
    )
    release_id = "The release ID for the demographics and data."
    n_draws = (
        "The number of draws to create. Must be >= the number of draws in the input data."
    )
    rei_id = (
        "A comma-separated list of REI IDs, if wanting to subset from all the PAFs "
        "available in an REI set for this release. Overrides input_models_path and "
        "input_rei_set_id."
    )
    input_rei_set_id = (
        "A comma-separated list of REI set IDs used to determine the most-detailed PAFs "
        "to include as input. If left blank, defaults to the GBD Computation and GBD "
        "Estimation Etiologies REI sets. Not used if input_models_path or rei_id arguments "
        "are provided."
    )
    skip_aggregation = "If True, aggregation to parent risks will not be done."
    year_id = (
        "A comma-separated list of year_ids. Defaults to standard years "
        "from get_demographics."
    )
    location_set_id = (
        "A location set ID used in combination with release to determine most-detailed "
        "locations. If left blank, defaults to GBD Computation (ihme_cc_paf_aggregator.lib."
        f"constants.COMPUTATION_LOCATION_SET_ID = {constants.COMPUTATION_LOCATION_SET_ID})."
    )
    location_id = (
        "A comma-separated list of location_ids. If provided, the most-detailed locations "
        "for the specified release and location set will be limited to this list prior to "
        "aggregation. This is mainly a convenience utility for smaller tests, and can only "
        "be used in conjunction with the --test flag."
    )
    test = (
        "If this flag is set, outputs will go to a test subfolder, and process version "
        "creation will be skipped."
    )
    resume = (
        "Whether to resume a previous PAF Aggregator workflow, specified by argument "
        "output_version_id."
    )


@click.command
@click.option(
    "input_models_path",
    "--input_models_path",
    type=str,
    required=False,
    callback=cli_utils.read_access_path,
    help=_ArgDocs.input_models_path.value,
)
@click.option(
    "output_version_id",
    "--output_version_id",
    type=int,
    required=False,
    default=cli_utils.increment_latest_version,
    help=_ArgDocs.output_version_id.value,
)
@click.option(
    "mediation_matrix_path",
    "--mediation_matrix_path",
    type=str,
    required=False,
    callback=cli_utils.read_access_path,
    help=_ArgDocs.mediation_matrix_path.value,
)
@click.option(
    "hierarchy_path",
    "--hierarchy_path",
    type=str,
    required=False,
    callback=cli_utils.read_access_path,
    help=_ArgDocs.hierarchy_path.value,
)
@click.option(
    "rei_set_id", "--rei_set_id", type=int, required=False, help=_ArgDocs.rei_set_id.value
)
@click.option(
    "rei_set_version_id",
    "--rei_set_version_id",
    type=int,
    required=False,
    help=_ArgDocs.rei_set_version_id.value,
)
@click.option(
    "mark_special",
    "--mark_special",
    type=bool,
    required=False,
    default=False,
    help=_ArgDocs.mark_special.value,
)
@click.option(
    "release_id", "--release_id", type=int, required=False, help=_ArgDocs.release_id.value
)
@click.option("n_draws", "--n_draws", type=int, required=True, help=_ArgDocs.n_draws.value)
@click.option(
    "rei_id",
    "--rei_id",
    type=str,
    required=False,
    callback=common_cli_utils.parse_comma_sep_to_int_list,
    help=_ArgDocs.rei_id.value,
)
@click.option(
    "input_rei_set_id",
    "--input_rei_set_id",
    type=str,
    required=False,
    callback=common_cli_utils.parse_comma_sep_to_int_list,
    help=_ArgDocs.input_rei_set_id.value,
)
@click.option(
    "year_id",
    "--year_id",
    type=str,
    required=False,
    callback=common_cli_utils.parse_comma_sep_to_int_list,
    help=_ArgDocs.year_id.value,
)
@click.option(
    "location_set_id",
    "--location_set_id",
    type=int,
    required=False,
    help=_ArgDocs.location_set_id.value,
)
@click.option(
    "location_id",
    "--location_id",
    type=str,
    required=False,
    callback=common_cli_utils.parse_comma_sep_to_int_list,
    help=_ArgDocs.location_id.value,
)
@click.option(
    "skip_aggregation",
    "--skip_aggregation",
    type=bool,
    required=False,
    default=False,
    help=_ArgDocs.skip_aggregation.value,
)
@click.option("--test", is_flag=True, help=_ArgDocs.test.value)
@click.option("--resume", is_flag=True, help=_ArgDocs.resume.value)
def _run_paf_aggregation(
    input_models_path: Optional[Path],
    output_version_id: int,
    mediation_matrix_path: Path,
    hierarchy_path: Optional[Path],
    rei_set_id: Optional[int],
    rei_set_version_id: Optional[int],
    mark_special: bool,
    release_id: Optional[int],
    n_draws: int,
    rei_id: Optional[List[int]],
    input_rei_set_id: Optional[List[int]],
    year_id: Optional[List[int]],
    location_set_id: Optional[int],
    location_id: Optional[List[int]],
    skip_aggregation: bool,
    test: bool,
    resume: bool,
) -> None:
    """Run a set of PAF aggregation jobs."""
    run_paf_aggregation(**locals())


def run_paf_aggregation(
    input_models_path: Optional[Path],
    output_version_id: int,
    mediation_matrix_path: Path,
    hierarchy_path: Optional[Path],
    rei_set_id: Optional[int],
    rei_set_version_id: Optional[int],
    mark_special: bool,
    release_id: Optional[int],
    n_draws: int,
    rei_id: Optional[List[int]],
    input_rei_set_id: Optional[List[int]],
    year_id: Optional[List[int]],
    location_set_id: Optional[int],
    location_id: Optional[List[int]],
    skip_aggregation: bool,
    test: bool,
    resume: bool,
) -> None:
    """Run a set of PAF aggregation jobs."""
    cli_args = "\n".join(f"{k}: {v}" for k, v in locals().items())
    logger.info(f"run_paf_aggregation called with:\n{cli_args}")

    if resume:
        # assumption is that we are resuming a run that has fully cached
        # items and stopped during the actual jobmon workflow execution
        output_dir = io.directory_from_paf_compile_version_id(output_version_id, test)
        manifest_path = 
        # CacheWriter is a subclass of CacheReader, it can also read
        cache_writer = FileBackedCacheWriter(manifest_path)
        config_settings = constants.PafAggregatorSettings(
            **cache_writer.get(constants.CacheContents.SETTINGS)
        )
        # update the settings in the cache with the resume value
        config_settings.resume = resume
        cache_writer.put(
            obj=asdict(config_settings),
            obj_name=constants.CacheContents.SETTINGS,
            relative_path=,
        )
    else:
        cli_utils.validate_hierarchy_rei_set_version(
            hierarchy_path, rei_set_id, rei_set_version_id
        )
        cli_utils.validate_location_test(location_id, test)
        cli_utils.warn_on_agg_with_reis_specified(
            input_models_path, rei_id, input_rei_set_id, skip_aggregation
        )
        cli_utils.validate_exclusive_risk_inputs(input_models_path, rei_id, input_rei_set_id)

        rei_set_id = rei_set_id or constants.AGGREGATION_REI_SET_ID
        input_rei_set_id = input_rei_set_id or constants.GBD_RISKS_AND_ETIOLOGIES
        location_set_id = location_set_id or constants.COMPUTATION_LOCATION_SET_ID

        # Collect settings. Inidividual settings will be used
        # by workers, and settings also record what was passed to the CLI

        # years from the DB by release or from an arg
        years_from_release = dbio.years_from_release(release_id)
        if year_id is not None:
            cli_utils.validate_years(year_id, years_from_release)
        else:
            year_id = years_from_release

        locations = dbio.get_detailed_locations(location_set_id, release_id)
        if location_id is not None:
            cli_utils.validate_location_id(
                location_set_id=location_set_id,
                release_id=release_id,
                user_location_id=location_id,
                release_locations=locations,
            )
            locations = location_id

        all_paf_causes = dbio.get_expected_paf_causes(release_id)

        config_settings = constants.PafAggregatorSettings(
            n_draws=n_draws,
            year_id=year_id,
            release_id=release_id,
            skip_aggregation=skip_aggregation,
            location_id=locations,
            version_id=output_version_id,
            resume=resume,
            output_dir=str(io.create_new_output_directory(output_version_id, test)),
            all_paf_causes=all_paf_causes,
        )

        # Collect items in a cache
        manifest_path = 
        manifest_path.parent.mkdir(exist_ok=True, parents=True)
        cache_writer = FileBackedCacheWriter(manifest_path)
        cache_writer.put(
            obj=asdict(config_settings),
            obj_name=constants.CacheContents.SETTINGS,
            relative_path=,
        )

        age_metadata = db_queries.get_age_metadata(release_id=config_settings.release_id)
        cache_writer.put(
            obj=age_metadata,
            obj_name=constants.CacheContents.AGE_METADATA,
            relative_path=,
        )

        hierarchy_df = (
            hierarchy.hierarchy_from_db(
                rei_set_id=rei_set_id,
                release_id=config_settings.release_id,
                rei_set_version_id=rei_set_version_id,
            )
            if hierarchy_path is None
            else hierarchy.hierarchy_from_path(hierarchy_path)
        )
        cache_writer.put(
            obj=hierarchy_df,
            obj_name=constants.CacheContents.RISK_HIERARCHY,
            relative_path=,
        )

        mediation_matrix = (
            mediation.default_mediation_matrix(config_settings.release_id)
            if mediation_matrix_path is None
            else mediation.mediation_matrix_from_path(mediation_matrix_path)
        )
        mediation.validate_mediation_matrix(mediation_matrix)
        # expand mediation matrix to most-detailed subcauses for CKD
        child_ckd_causes = dbio.get_child_ckd_causes(release_id)
        mediation_matrix = mediation.expand_mediation_matrix_subcauses(
            mediation_matrix=mediation_matrix,
            parent_cause_id=constants.CKD_PARENT_CAUSE_ID,
            child_cause_ids=child_ckd_causes,
            set_mediation_to_one=True,
        )
        cache_writer.put(
            obj=mediation_matrix,
            obj_name=constants.CacheContents.MEDIATION_MATRIX,
            relative_path=,
        )

        # model versions from the DB by release or from a specified file
        if input_models_path is None:
            input_models_df = dbio.get_best_input_models(
                release_id=release_id,
                mediation_matrix=mediation_matrix,
                input_rei_set_id=input_rei_set_id,
                rei_id=rei_id,
            )
        else:
            input_models_df = pd.read_csv(input_models_path)
        input_models_df = cli_utils.validate_and_format_compile_inputs(
            raw_inputs_df=input_models_df,
            n_draws=config_settings.n_draws,
            year_id=config_settings.year_id,
            release_id=release_id,
        )
        # historical name, skip round-trip check because datetimes don't round-trip in csv
        # but, we like the human-readable format instead of hdf5
        cache_writer.put(
            obj=input_models_df,
            obj_name=constants.CacheContents.INPUT_MODELS,
            relative_path=,
            round_trip_check=False,
        )

        cache_writer.validate_from_list(constants.CacheContents.list())

        # human-readable file, legacy existence
        relative_risk_types = dbio.get_relative_risk_types(
            input_models_df.model_version_id.tolist()
        )
        relative_risk_types.to_csv(
        )

    wf = workflow.create_workflow(manifest_path)
    wf.run(resume=resume)

    # save some metadata gleaned from a single output draw file,
    one_draw_file = io.draw_df_from_dir(config_settings.output_dir)
    cache_writer.put(
        obj=one_draw_file[constants.AGE_META_COLS].drop_duplicates().reset_index(drop=True),
        obj_name=constants.PostRunCacheContents.AGE_META_FROM_DRAW,
        relative_path=constants.AGE_MEASURE_METADATA_FILEPATH,
    )
    cache_writer.put(
        obj=one_draw_file[constants.REI_META_COLS].drop_duplicates().reset_index(drop=True),
        obj_name=constants.PostRunCacheContents.REI_META_FROM_DRAW,
        relative_path=constants.REI_MEASURE_METADATA_FILEPATH,
        storage_format=,
    )

    if not test:
        process_version.create_process_version(
            manifest_path=manifest_path, env=constants.PROD, mark_special=mark_special
        )
        input_models_df.to_csv(
            index=False,
        )
        relative_risk_types.to_csv(
            index=False,
        )
    return


# Manually update the launch function's docstring to reflect _ArgDocs
run_paf_aggregation.__doc__ += "\n\nArguments:\n" + "\n".join(
    [f"        {data.name}: {data.value}" for data in _ArgDocs]
)


if __name__ == "__main__":
    _run_paf_aggregation()
