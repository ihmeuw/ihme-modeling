import traceback
from enum import Enum
from typing import List, Optional

import click

import gbd_outputs_versions
from gbd.constants import gbd_process
from jobmon.client.workflow import WorkflowRunStatus

from ihme_cc_sev_calculator.lib import (
    constants,
    input_utils,
    io,
    logging_utils,
    parameters,
    slack_utils,
)
from ihme_cc_sev_calculator.lib.workflow import workflow

logger = logging_utils.module_logger(__name__)


class _ArgDocs(Enum):
    """Single source of argument docs, for CLI help and docs.

    As a private class, sphinx-apidoc skips it.
    """

    compare_version_id = (
        "The compare version ID to pull machinery versions from. Machinery versions can be "
        "overridden by passing in specific versions."
    )
    paf_version_id = (
        "The PAF version ID to pull PAFs for. If provided, overrides the PAF version in the "
        "compare version. Must have all years requested via year_ids."
    )
    como_version_id = (
        "The COMO version ID to pull YLDs for. If provided, overrides the COMO version in the "
        f"compare version. Must have year {constants.ARBITRARY_MACHINERY_YEAR_ID}."
    )
    codcorrect_version_id = (
        "The CodCorrect version ID to pull YLLs for. If provided, overrides the CodCorrect "
        "version in the compare version. Must have year "
        f"{constants.ARBITRARY_MACHINERY_YEAR_ID}."
    )
    release_id = "The release ID for the demographics and data. Required."
    n_draws = "The number of draws to create. Required."
    year_ids = (
        "A comma-separated list of year_ids. Defaults to standard estimation years "
        "from get_demographics for team 'epi'."
    )
    location_set_ids = (
        "A comma-separated list of location_set_ids. Defaults to "
        f"{constants.DEFAULT_LOCATION_SET_ID}. If given, location set "
        f"{constants.DEFAULT_LOCATION_SET_ID} must be present."
    )
    measures = (
        "String names of measures to generate. Allowed values are "
        f"{', '.join(constants.MEASURE_MAP.keys())}. Defaults to all allowed values. "
        "'rr_max' is always required. In the case that it's not provided, 'rr_max'' is "
        "added automatically."
    )
    percent_change = (
        "Flag, if provided, calculates percent change by year. If running for a single year, "
        "the flag will be ignored. Percent change years are currently hard-coded to "
        f"{constants.PERCENT_CHANGE_YEARS}."
    )
    by_cause = (
        "Flag, if provided, saves risk-cause-specific SEVs in addition to risk-level SEVs."
    )
    resume = (
        "Flag, if provided, denotes that a specific SEV Calculator version should be rerun. "
        "If so, ignores all arguments other than version_id."
    )
    version_id = "The SEV Calculator version ID to resume. Must be provided if resuming."
    test = (
        "Flag, if provided, denotes that this SEV Calculator run is a test. Process versions "
        "will not be activated, nor will they be added to a compare version."
    )
    overwrite_compare_version = (
        "Flag, if provided, denotes that this SEV Calculator run should overwrite SEV/RRmax "
        "versions in the passed compare version, if all relevant machinery versions are "
        "drawn from that compare version."
    )


@click.command
@click.option(
    "compare_version_id",
    "--compare_version_id",
    type=int,
    required=False,
    help=_ArgDocs.compare_version_id.value,
)
@click.option(
    "paf_version_id",
    "--paf_version_id",
    type=int,
    required=False,
    help=_ArgDocs.paf_version_id.value,
)
@click.option(
    "como_version_id",
    "--como_version_id",
    type=int,
    required=False,
    help=_ArgDocs.como_version_id.value,
)
@click.option(
    "codcorrect_version_id",
    "--codcorrect_version_id",
    type=int,
    required=False,
    help=_ArgDocs.codcorrect_version_id.value,
)
@click.option(
    "release_id", "--release_id", type=int, required=False, help=_ArgDocs.release_id.value
)
@click.option("n_draws", "--n_draws", type=int, required=False, help=_ArgDocs.n_draws.value)
@click.option(
    "year_ids",
    "--year_ids",
    type=str,
    required=False,
    callback=input_utils.parse_comma_sep_to_int_list,
    help=_ArgDocs.year_ids.value,
)
@click.option(
    "location_set_ids",
    "--location_set_ids",
    type=str,
    required=False,
    callback=input_utils.parse_comma_sep_to_int_list,
    help=_ArgDocs.location_set_ids.value,
)
@click.option(
    "measures",
    "--measures",
    type=str,
    required=False,
    callback=input_utils.parse_comma_sep_to_str_list,
    help=_ArgDocs.measures.value,
)
@click.option("--percent_change", is_flag=True, help=_ArgDocs.percent_change.value)
@click.option("--by_cause", is_flag=True, help=_ArgDocs.by_cause.value)
@click.option("--resume", is_flag=True, help=_ArgDocs.resume.value)
@click.option(
    "version_id", "--version_id", type=int, required=False, help=_ArgDocs.version_id.value
)
@click.option("--test", is_flag=True, help=_ArgDocs.test.value)
@click.option(
    "--overwrite_compare_version", is_flag=True, help=_ArgDocs.overwrite_compare_version.value
)
def _launch(
    compare_version_id: Optional[int],
    paf_version_id: Optional[int],
    como_version_id: Optional[int],
    codcorrect_version_id: Optional[int],
    release_id: Optional[int],
    n_draws: Optional[int],
    year_ids: Optional[List[int]],
    location_set_ids: Optional[List[int]],
    measures: Optional[List[str]],
    percent_change: bool,
    by_cause: bool,
    resume: bool,
    version_id: Optional[int],
    test: bool,
    overwrite_compare_version: bool,
) -> None:
    """Launch the SEV Calculator."""
    launch(**locals())


def launch(
    compare_version_id: Optional[int],
    paf_version_id: Optional[int],
    como_version_id: Optional[int],
    codcorrect_version_id: Optional[int],
    release_id: Optional[int],
    n_draws: Optional[int],
    year_ids: Optional[List[int]],
    location_set_ids: Optional[List[int]],
    measures: Optional[List[str]],
    percent_change: bool,
    by_cause: bool,
    resume: bool,
    version_id: Optional[int],
    test: bool,
    overwrite_compare_version: bool,
) -> None:
    """Launch the SEV Calculator."""
    input_utils.validate_resume(resume, version_id)

    if resume:
        logger.info(f"Resuming SEV Calculator v{version_id}")
        params = parameters.Parameters.read_from_cache(version_id)
        params.resume = resume
    else:
        # Decide a new internal version, going off of RR max as we always run RR max
        # and currently the internal versions for SEVs and RRmax in a singular run
        # is expected to be the same
        version_id = gbd_outputs_versions.get_new_internal_version_id(
            gbd_process.RR_MAX, fill_gaps=True, min_value=470
        )
        logger.info(f"Starting SEV Calculator v{version_id}. Processing parameters")
        params = parameters.process_parameters(
            compare_version_id=compare_version_id,
            paf_version_id=paf_version_id,
            como_version_id=como_version_id,
            codcorrect_version_id=codcorrect_version_id,
            release_id=release_id,
            n_draws=n_draws,
            year_ids=year_ids,
            location_set_ids=location_set_ids,
            measures=measures,
            percent_change=percent_change,
            by_cause=by_cause,
            resume=resume,
            version_id=version_id,
            test=test,
            overwrite_compare_version=overwrite_compare_version,
        )

        io.create_run_dirs(
            output_dir=params.output_dir,
            all_rei_ids=params.all_rei_ids,
            by_cause=params.by_cause,
        )
        params.cache()

    if not params.test:
        slack_utils.send_slack_greeting(params)

    # Run workflow in try catch in case of inexpected failure
    logger.info("Beginning workflow creation")
    try:
        wf = workflow.Workflow(params)
        wf.build_all_tasks()

        logger.info("Binding workflow to jobmon database")
        workflow_id = wf.bind()
        if not params.test:
            gbd_outputs_versions.slack.send_jobmon_gui_link(
                machinery_name="SEV Calculator",
                version_id=params.version_id,
                workflow_id=workflow_id,
            )

        logger.info("Running workflow")
        status = wf.run()
    except KeyboardInterrupt:
        # If run is interrupted, exit the function early w/o sending a slack goodbye
        logger.info("SEV Calculator run interrupted by user. Exiting")
        return
    except Exception as e:
        logger.info(f"Uncaught exception attempting to set up and run jobmon workflow: {e}")
        logger.info(traceback.format_exc())
        status = WorkflowRunStatus.ERROR

    logger.info(f"SEV Calculator v{params.version_id} finished with status {status}.")
    if not params.test:
        slack_utils.send_slack_goodbye(params, status=status)


# Manually update the launch function's docstring to reflect _ArgDocs
launch.__doc__ += "\n\nArguments:\n" + "\n".join(
    [f"        {data.name}: {data.value}" for data in _ArgDocs]
)


if __name__ == "__main__":
    _launch()
