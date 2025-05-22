import os
import subprocess  # nosec: B404
import warnings
from enum import Enum
from pathlib import Path
from typing import List, Optional, Union

import click
import pandas as pd

import db_queries
import ihme_cc_averted_burden
import ihme_cc_risk_utils
from gbd.constants import release
from ihme_cc_risk_utils.lib import cli_utils

from ihme_cc_paf_calculator.lib import (
    constants,
    dbio,
    input_utils,
    io_utils,
    logging_utils,
    mediation,
    model_utils,
)

logger = logging_utils.module_logger(__name__)

warnings.filterwarnings("ignore", category=pd.io.pytables.PerformanceWarning)


class _ArgDocs(Enum):
    """Single source of argument docs, for CLI help and docs.

    As a private class, sphinx-apidoc skips it.
    """

    rei_id = "ID of the risk for which to calculate PAF."
    cluster_proj = "What project to run jobs under on the cluster."
    release_id = "The release ID for the demographics and data."
    year_id = (
        "A comma-separated list of year_ids. Defaults to standard years "
        "from get_demographics for gbd_team 'epi' and the passed release_id. "
        "Note that the standard years for the 'epi' and 'usre' teams coincide "
        "for the USRE release."
    )
    n_draws = (
        "The number of draws to create. Must be between (0, 1000] and <= the number of draws "
        f"in the input models. Default is {constants.DEFAULT_N_DRAWS}."
    )
    codcorrect_version_id = (
        "Version of CoDCorrect to use when pulling YLLs for GBD releases. Used when "
        "running PAFs for low birth weight and short gestation, particulate matter pollution,"
        " and any risk with CKD as an outcome for GBD releases. If not given and is needed, "
        "set to a default depending on the release. If not needed, set to None."
    )
    como_version_id = (
        "Version of COMO to use when pulling YLDs for GBD releases. Used when running "
        "PAFs for blood lead exposure, occupational noise, and any risk with CKD as an "
        "outcome for GBD releases. If not given and is needed, set to a default depending "
        "on the release. If not needed, set to None."
    )
    rei_set_id = (
        "Which REI set to pull REI metadata from for the passed rei_id and release_id. "
        f"Default is {constants.DEFAULT_REI_SET_ID}."
    )
    cause_set_id = (
        "Which cause set to pull relevant cause metadata from for the passed rei_id and "
        f"release_id. Default is {constants.DEFAULT_CAUSE_SET_ID}."
    )
    skip_save_results = (
        "This flag is set to skip running save_results_risk on the final PAF draws. If False "
        "(or not provided), save_results_risk will be run with mark_best=True. Default is "
        f"{constants.DEFAULT_SKIP_SAVE_RESULTS}."
    )
    resume = (
        "This flag is set to resume a previous PAF calculation. Requires a model_version_id. "
        f"Default is {constants.DEFAULT_RESUME}."
    )
    model_version_id = (
        "Used in conjunction with resume, this identifies the specific "
        "run directory for a resume. Requires resume=True."
    )
    root_run_dir = (
        "Root directory for saving results of the PAF calculation. Default is "
        f"{constants.DEFAULT_ROOT_RUN_DIR}, or {constants.DEFAULT_DEV_ROOT_RUN_DIR} when "
        "using the --test flag."
    )
    test = (
        "Set this flag to run in test mode. When set, the currently-activated conda env "
        "and the test epi DB are used. When not set the centrally-deployed conda env and "
        f"the prod epi DB are used. Default is {constants.DEFAULT_TEST}."
    )
    description = (
        "Optional text to append to the generic model version description, "
        f"{constants.MODEL_DESCRIPTION}. Can be used to add details about a specific PAF "
        "calculator run."
    )


@click.command
@click.option("rei_id", "--rei_id", type=int, required=True, help=_ArgDocs.rei_id.value)
@click.option(
    "cluster_proj",
    "--cluster_proj",
    type=str,
    required=True,
    help=_ArgDocs.cluster_proj.value,
)
@click.option(
    "release_id", "--release_id", type=int, required=True, help=_ArgDocs.release_id.value
)
@click.option(
    "year_id",
    "--year_id",
    type=str,
    callback=cli_utils.parse_comma_sep_to_int_list,
    help=_ArgDocs.year_id.value,
)
@click.option("n_draws", "--n_draws", type=int, default=1000, help=_ArgDocs.n_draws.value)
@click.option(
    "codcorrect_version_id",
    "--codcorrect_version_id",
    type=int,
    required=False,
    help=_ArgDocs.codcorrect_version_id.value,
)
@click.option(
    "como_version_id",
    "--como_version_id",
    type=int,
    required=False,
    help=_ArgDocs.como_version_id.value,
)
@click.option(
    "rei_set_id",
    "--rei_set_id",
    type=int,
    default=constants.DEFAULT_REI_SET_ID,
    help=_ArgDocs.rei_set_id.value,
)
@click.option(
    "cause_set_id",
    "--cause_set_id",
    type=int,
    default=lambda: input_utils.get_cause_set_id(
        click.get_current_context().params.get("rei_id")
    ),
    help=_ArgDocs.cause_set_id.value,
)
@click.option(
    "root_run_dir", "--root_run_dir", type=str, default=None, help=_ArgDocs.root_run_dir.value
)
@click.option(
    "model_version_id",
    "--model_version_id",
    type=int,
    required=False,
    help=_ArgDocs.model_version_id.value,
)
@click.option("--test", is_flag=True, help=_ArgDocs.test.value, is_eager=True)
@click.option("--skip_save_results", is_flag=True, help=_ArgDocs.skip_save_results.value)
@click.option("--resume", is_flag=True, help=_ArgDocs.resume.value)
@click.option(
    "description", "--description", type=str, default=None, help=_ArgDocs.description.value
)
def _launch_paf_calculator(
    rei_id: int,
    cluster_proj: str,
    year_id: Optional[List[int]],
    n_draws: int,
    release_id: int,
    codcorrect_version_id: Optional[int],
    como_version_id: Optional[int],
    rei_set_id: int,
    cause_set_id: int,
    skip_save_results: bool,
    resume: bool,
    model_version_id: Optional[int],
    root_run_dir: Optional[str],
    test: bool,
    description: Optional[str],
) -> None:
    """Launch a PAF Calculator workflow."""
    launch_paf_calculator(**locals())


def launch_paf_calculator(
    rei_id: int,
    cluster_proj: str,
    release_id: int,
    year_id: Optional[Union[int, List[int]]] = None,
    n_draws: int = constants.DEFAULT_N_DRAWS,
    codcorrect_version_id: Optional[int] = None,
    como_version_id: Optional[int] = None,
    rei_set_id: int = constants.DEFAULT_REI_SET_ID,
    cause_set_id: int = constants.DEFAULT_CAUSE_SET_ID,
    skip_save_results: bool = constants.DEFAULT_SKIP_SAVE_RESULTS,
    resume: bool = constants.DEFAULT_RESUME,
    model_version_id: Optional[int] = None,
    root_run_dir: Optional[str] = None,
    test: bool = constants.DEFAULT_TEST,
    description: Optional[str] = None,
) -> int:
    """Launch a PAF Calculator workflow, returning staged PAF model version ID."""
    cli_args = "\n".join(f"{k} = {v}" for k, v in locals().items())
    logger.info(f"launch_paf_calculator called with:\n{cli_args}")

    if root_run_dir is None:
        root_run_dir = (
            constants.DEFAULT_DEV_ROOT_RUN_DIR if test else constants.DEFAULT_ROOT_RUN_DIR
        )
        logger.info(f"Setting root_run_dir to {root_run_dir}")

    if not resume:
        if year_id is None:
            year_id = dbio.years_from_release(release_id)
            logger.info(f"Setting year_id based on release_id {release_id} to {year_id}.")
        elif isinstance(year_id, int):
            year_id = [year_id]

        # Validate inputs
        logger.info("Validating inputs")
        rei_metadata = db_queries.get_rei_metadata(
            rei_set_id=rei_set_id, release_id=release_id, include_all_metadata=True
        )
        demographics = db_queries.get_demographics(
            gbd_team="usre" if release_id == release.USRE else "epi", release_id=release_id
        )
        input_utils.validate_inputs(
            rei_id=rei_id,
            year_id=year_id,
            n_draws=n_draws,
            release_id=release_id,
            skip_save_results=skip_save_results,
            rei_set_id=rei_set_id,
            model_version_id=model_version_id,
            rei_metadata=rei_metadata,
            demographics=demographics,
            test=test,
        )

        # Determine if the passed rei_id is an intervention where the outcome is a risk factor
        # rather than a cause. Currently only supports drugs in the Averted Burden REI set.
        intervention_outcome_rei_id = None
        if rei_set_id == constants.AVERTED_BURDEN_REI_SET_ID:
            intervention_outcome_rei_id = ihme_cc_averted_burden.get_all_rei_ids_for_drug(
                rei_id=rei_id
            ).outcome_rei_id
        # Pull MEs related to the risk and corresponding best MVIDs and validate
        me_ids = ihme_cc_risk_utils.get_rei_me_ids(rei_id=rei_id, release_id=release_id)
        input_utils.validate_paf_model_type(rei_id=rei_id, me_ids=me_ids)
        # If the passed rei_id is an intervention acting on a risk factor, additionally pull
        # REI metadata for the GBD Computation REI set and MEs related to the risk factor.
        if intervention_outcome_rei_id:
            intervention_rei_metadata = input_utils.edit_rei_metadata(
                rei_id, rei_metadata.copy(), me_ids
            )
            rei_metadata = db_queries.get_rei_metadata(
                rei_set_id=constants.COMPUTATION_REI_SET_ID,
                release_id=release_id,
                include_all_metadata=True,
            )
            intervention_outcome_me_ids = ihme_cc_risk_utils.get_rei_me_ids(
                rei_id=intervention_outcome_rei_id, release_id=release_id
            )
            me_ids = pd.concat([me_ids, intervention_outcome_me_ids])
            rr_metadata = ihme_cc_risk_utils.get_relative_risk_metadata(
                rei_id=intervention_outcome_rei_id, release_id=release_id, me_ids=me_ids
            )
        else:
            rr_metadata = ihme_cc_risk_utils.get_relative_risk_metadata(
                rei_id=rei_id, release_id=release_id, me_ids=me_ids
            )
            # Drop PAFs of one.
            rr_metadata = input_utils.drop_pafs_of_one(
                rr_metadata=rr_metadata, release_id=release_id
            )

        # edited_me_ids drops paf and paf_unmediated ME ids, and converts model_version_id to
        # int. This is done to handle the possibility of NaN paf model version ids, as occurs
        # when no pafs have been saved yet.
        edited_me_ids = input_utils.validate_input_models(
            rei_id=rei_id,
            year_id=year_id,
            release_id=release_id,
            demographics=demographics,
            me_ids=me_ids,
            rr_metadata=rr_metadata,
        )

        # Validate machinery versions once we have a list of causes for the risk.
        # Will update to default versions if given None and risk uses the machinery
        codcorrect_version_id, como_version_id = (
            input_utils.validate_input_machinery_versions(
                rei_id=rei_id,
                release_id=release_id,
                codcorrect_version_id=codcorrect_version_id,
                como_version_id=como_version_id,
                cause_ids=rr_metadata["cause_id"].unique().tolist(),
                year_id=year_id,
                n_draws=n_draws,
            )
        )

        # Validate mediator PAF years if we will be performing mediated subcause splitting.
        input_utils.validate_mediator_paf_years(
            rei_id=rei_id,
            rr_metadata=rr_metadata,
            release_id=release_id,
            year_id=year_id,
            codcorrect_version_id=codcorrect_version_id,
            como_version_id=como_version_id,
        )

        # cache mediators
        mediators = rr_metadata[rr_metadata["source"] == "delta"]["med_id"].unique().tolist()
        mediator_rei_metadata = rei_metadata[rei_metadata["rei_id"].isin(mediators)].copy()
        # Once inputs are validated, keep only keep REI metadata and other minor edits
        rei_metadata = input_utils.edit_rei_metadata(
            rei_id=intervention_outcome_rei_id or rei_id,
            rei_metadata=rei_metadata,
            me_ids=edited_me_ids,
        )

        # Get cause and age metadata for caching
        cause_metadata = db_queries.get_cause_metadata(
            cause_set_id=cause_set_id, release_id=release_id
        )
        age_metadata = db_queries.get_age_metadata(release_id=release_id)

        # Determine the modelable entity IDs
        paf_me_ids = model_utils.get_paf_modelable_entities(
            me_ids.query(f"rei_id == {rei_id}")
        )

        # Get total mediation factors for caching and confirm that MEs align with the
        # mediation matrix
        mediation_factors = mediation.get_total_mediation_factor_draws(
            rei_id=rei_id, n_draws=n_draws, release_id=release_id
        )
        restricted_mediation_factors = mediation.restrict_mediation_factors_to_rr_metadata(
            mediation_factors=mediation_factors, rr_metadata=rr_metadata
        )
        edited_paf_me_ids = input_utils.validate_unmediated_paf_if_required_else_set_to_none(
            modelable_entities=paf_me_ids, mediation_factors=restricted_mediation_factors
        )

        input_utils.validate_model_description(
            description, rei_id, restricted_mediation_factors
        )
        db_env = (
            constants.SAVE_RESULTS_DB_DEV_ENV if test else constants.SAVE_RESULTS_DB_PROD_ENV
        )
        # Create model version IDs
        paf_staged_mvids = model_utils.stage_model_versions(
            modelable_entities=edited_paf_me_ids,
            rei_id=rei_id,
            rei_set_id=rei_set_id,
            release_id=release_id,
            db_env=db_env,
            extra_description=description,
        )

        # Create version-specific output directory and any expected subdirs
        output_dir = io_utils.get_output_dir(
            root_dir=root_run_dir,
            rei_id=rei_id,
            model_version_id=paf_staged_mvids.paf.model_version_id,
        )
        io_utils.create_dirs(
            rei_id=rei_id, rei_set_id=rei_set_id, output_dir=output_dir, resume=resume
        )

        # Create and cache settings of this run
        settings = constants.PafCalculatorSettings(
            rei_id=intervention_outcome_rei_id or rei_id,
            intervention_rei_id=None if not intervention_outcome_rei_id else rei_id,
            cluster_proj=cluster_proj,
            year_id=year_id,
            n_draws=n_draws,
            release_id=release_id,
            skip_save_results=skip_save_results,
            codcorrect_version_id=codcorrect_version_id,
            como_version_id=como_version_id,
            rei_set_id=rei_set_id,
            output_dir=str(output_dir.resolve()),
            resume=resume,
            test=test,
            paf_modelable_entity_id=edited_paf_me_ids.paf,
            paf_unmediated_modelable_entity_id=edited_paf_me_ids.paf_unmediated,
            paf_model_version_id=paf_staged_mvids.paf.model_version_id,
            paf_unmediated_model_version_id=paf_staged_mvids.paf_unmediated.model_version_id,
            paf_lbw_model_version_id=paf_staged_mvids.paf_lbw.model_version_id,
            paf_sg_model_version_id=paf_staged_mvids.paf_sg.model_version_id,
            paf_unavertable_model_version_id=(
                paf_staged_mvids.paf_unavertable.model_version_id
            ),
        )

        io_utils.write_version_settings_and_cache(
            settings=settings,
            root_dir=output_dir,
            rei_metadata=rei_metadata,
            mediator_rei_metadata=None if not mediators else mediator_rei_metadata,
            intervention_rei_metadata=(
                None if not intervention_outcome_rei_id else intervention_rei_metadata
            ),
            cause_metadata=cause_metadata,
            age_metadata=age_metadata,
            demographics=demographics,
            me_ids=edited_me_ids,
            rr_metadata=rr_metadata,
            paf_staged_mvid=paf_staged_mvids.paf,
            paf_unmediated_staged_mvid=paf_staged_mvids.paf_unmediated,
            paf_lbw_staged_mvid=paf_staged_mvids.paf_lbw,
            paf_sg_staged_mvid=paf_staged_mvids.paf_sg,
            paf_unavertable_staged_mvid=paf_staged_mvids.paf_unavertable,
            mediation_factors=restricted_mediation_factors,
        )
    else:
        if model_version_id is None:
            raise ValueError("One must specify model_version_id when using --resume.")

        logger.info(f"Resuming PAF model version {model_version_id}")
        output_dir = io_utils.get_output_dir(
            root_dir=root_run_dir, rei_id=rei_id, model_version_id=model_version_id
        )
        logger.info("Overriding passed arguments with previously cached settings")
        settings = io_utils.read_settings(output_dir)
        settings.resume = resume
        # We do not allow resuming a workflow with deleted PAF models.
        dbio.validate_no_deleted_paf_models(settings=settings)
        # workflow needs to know the new resume setting
        # the cache will maintain timestamped history of settings updates
        io_utils.write_version_settings(settings, output_dir)
        # Set failed PAF model statuses back to submitted.
        dbio.submit_failed_paf_models(settings=settings)

    logger.info(f"Logging at ")
    logger.info(f"Launching PAF model version {settings.paf_model_version_id}")
    sbatch_launch(output_dir)
    return settings.paf_model_version_id


def sbatch_launch(root_dir: Path) -> None:
    """Writes an sbatch script and launches a workflow with it."""
    settings = io_utils.read_settings(root_dir)

    if settings.test:
        sub_call = subprocess.run(
            ["conda", "info", "--base"], capture_output=True
        )  # nosec: B603, B607
        conda_install = sub_call.stdout.decode().strip()
        conda_env = os.environ["CONDA_PREFIX"]
    else:
        conda_install = constants.PROD_CONDA_INSTALL
        conda_env = constants.PROD_CONDA_ENV

    launch_script = 
    with open(launch_script, "w") as fp:
        fp.write(
            constants.LAUNCH_SCRIPT_TEMPLATE.format(
                cluster_proj=settings.cluster_proj,
                model_root_dir=root_dir,
                conda_install=conda_install,
                conda_env=conda_env,
            )
        )
    # start a small sbatch that launches the workflow
    subprocess.run(["sbatch", str(launch_script)])  # nosec: B603, B607


# Manually update the launch function's docstring to reflect _ArgDocs
launch_paf_calculator.__doc__ += "\n\nArguments:\n" + "\n".join(
    [f"        {data.name}: {data.value}" for data in _ArgDocs]
)


if __name__ == "__main__":
    _launch_paf_calculator()
