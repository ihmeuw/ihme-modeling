import pathlib
from typing import List, Tuple

import click
import pandas as pd

import db_tools_core
import ihme_cc_averted_burden
import save_results
from gbd.constants import measures, sex
from save_results.api.internal import StagedResult

from ihme_cc_paf_calculator.lib import constants, io_utils

PROD_CONN_DEF = "privileged-epi-save-results"
DEV_CONN_DEF = "privileged-epi-save-results-test"


@click.command
@click.option(
    "output_dir",
    "--output_dir",
    type=str,
    required=True,
    help="root directory of a specific paf calculator run",
)
@click.option(
    "model_version_id",
    "--model_version_id",
    type=int,
    required=True,
    help="PAF model version to save results for",
)
def main(output_dir: str, model_version_id: int) -> None:
    """Runs save_results for PAF estimates.

    Temporary draws, once saved, are deleted.
    """
    _run_task(output_dir, model_version_id)


def _run_task(root_dir: str, model_version_id: int) -> None:
    """Runs save_results for PAF estimates."""
    root_dir = pathlib.Path(root_dir)

    settings = io_utils.read_settings(root_dir)
    demographics = io_utils.get(root_dir, constants.CacheContents.DEMOGRAPHICS)
    rei_metadata = io_utils.get(root_dir, constants.CacheContents.REI_METADATA)
    me_ids = io_utils.get(root_dir, constants.CacheContents.MODEL_VERSIONS)

    input_file_pattern, staged_result = _get_file_pattern_and_staged_result(
        model_version_id=model_version_id, root_dir=root_dir, settings=settings
    )

    save_results.api.internal.save_results_risk_staged(
        input_dir=root_dir,
        input_file_pattern=input_file_pattern,
        risk_type="paf",
        staged_result=staged_result,
        sex_id=_determine_sex_ids(rei_metadata),
        year_id=settings.year_id,
        measure_id=_determine_measure_ids(rei_metadata),
        n_draws=settings.n_draws,
        mark_best=True,
    )

    # Create and insert PAF metadata into epi.database
    paf_model_metadata, model_version_metadata = _create_paf_metadata(
        model_version_id, settings, rei_metadata, me_ids
    )
    _insert_paf_metadata(paf_model_metadata, model_version_metadata, settings.test)

    # After running save_results, which restages PAF draws, we can delete the temporary ones.
    io_utils.delete_paf_draws(
        root_dir=root_dir, file_pattern=input_file_pattern, demographics=demographics
    )


def _get_file_pattern_and_staged_result(
    model_version_id: int, root_dir: pathlib.Path, settings: constants.PafCalculatorSettings
) -> Tuple[str, StagedResult]:
    """Get file pattern for saved PAF draws and StagedResult.

    Handles total (normal) PAF case, unmediated case, and, for LBW/SG,
    its child risks.
    """
    mvid_map = {
        settings.paf_model_version_id: (
            constants.DRAW_FILE_PATTERN,
            constants.CacheContents.PAF_MVID_STAGED_RESULT,
        ),
        settings.paf_unmediated_model_version_id: (
            constants.DRAW_UNMEDIATED_FILE_PATTERN,
            constants.CacheContents.PAF_UNMED_MVID_STAGED_RESULT,
        ),
        settings.paf_lbw_model_version_id: (
            constants.LBW_DRAW_FILE_PATTERN,
            constants.CacheContents.PAF_LBW_MVID_STAGED_RESULT,
        ),
        settings.paf_sg_model_version_id: (
            constants.SG_DRAW_FILE_PATTERN,
            constants.CacheContents.PAF_SG_MVID_STAGED_RESULT,
        ),
        settings.paf_unavertable_model_version_id: (
            constants.UNAVERTABLE_FILE_PATTERN,
            constants.CacheContents.PAF_UNAVTB_MVID_STAGED_RESULT,
        ),
    }

    if model_version_id not in mvid_map:
        raise RuntimeError(
            f"Internal error: unrecognized model_version_id: {model_version_id}"
        )

    input_file_pattern, stage_result_file = mvid_map[model_version_id]
    staged_result = StagedResult(**(io_utils.get(root_dir, stage_result_file)))

    return (input_file_pattern, staged_result)


def _determine_measure_ids(rei_metadata: pd.DataFrame) -> List[int]:
    """Determine relevant measure_ids for a risk.

    Expects one row in rei_metadata for the risk. Throws an error otherwise.
    """
    if len(rei_metadata) != 1:
        raise RuntimeError(f"Expected exactly one row for rei_metadata:\n{rei_metadata}")

    measure_ids = []
    if rei_metadata["yld"].iat[0] == 1:
        measure_ids.append(measures.YLD)

    if rei_metadata["yll"].iat[0] == 1:
        measure_ids.append(measures.YLL)

    if not measure_ids:
        raise RuntimeError(
            "According to REI metadata, there are no corresponding measures:\n"
            f"{rei_metadata[['rei_id', 'yld', 'yll']]}"
        )

    return measure_ids


def _determine_sex_ids(rei_metadata: pd.DataFrame) -> List[int]:
    """Determine relevant sex_ids for a risk.

    Expects one row in rei_metadata for the risk. Throws an error otherwise.
    """
    if len(rei_metadata) != 1:
        raise RuntimeError(f"Expected exactly one row for rei_metadata:\n{rei_metadata}")

    # IRON_DEFICIENCY_REI_ID has male and female metadata, but only generates female PAFs in
    # the PAF Calculator. (The male PAFs are PAFs of one.)
    if rei_metadata.rei_id.item() == constants.IRON_DEFICIENCY_REI_ID:
        return [sex.FEMALE]

    sex_ids = []
    if rei_metadata["male"].iat[0] == 1:
        sex_ids.append(sex.MALE)

    if rei_metadata["female"].iat[0] == 1:
        sex_ids.append(sex.FEMALE)

    if not sex_ids:
        raise RuntimeError(
            "According to REI metadata, there are no corresponding sexes:\n"
            f"{rei_metadata[['rei_id', 'male', 'female']]}"
        )

    return sex_ids


def _create_paf_metadata(
    model_version_id: int,
    settings: constants.PafCalculatorSettings,
    rei_metadata: pd.DataFrame,
    me_ids: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Create PAF metadata to insert into the appropriate tables on epi.

    More PAF model metadata, particular settings like n_draws and years, can
    and should be stored in the database in the future.
    """
    # Handling for LBW/SG, which creates PAFs for three risks rather than one like all others
    if (
        settings.rei_id == constants.LBWSGA_REI_ID
        and model_version_id != settings.paf_model_version_id
    ):
        if model_version_id == settings.paf_lbw_model_version_id:
            rei_id = constants.LOW_BIRTH_WEIGHT_REI_ID
        else:
            rei_id = constants.SHORT_GESTATION_REI_ID
    # Handling for Averted Burden, which can create two PAFs with the addition of Unavertable
    elif settings.rei_set_id == constants.AVERTED_BURDEN_REI_SET_ID:
        drug_rei_id = (
            settings.intervention_rei_id if settings.intervention_rei_id else settings.rei_id
        )
        if model_version_id == settings.paf_unavertable_model_version_id:
            rei_id = ihme_cc_averted_burden.get_all_rei_ids_for_drug(
                rei_id=drug_rei_id
            ).unavertable_rei_id
        else:
            rei_id = drug_rei_id
    else:
        rei_id = settings.rei_id

    paf_model_metadata = pd.DataFrame(
        {
            "paf_model_version_id": [model_version_id],
            "rei_id": [rei_id],
            "codcorrect_version_id": [settings.codcorrect_version_id],
            # The below three metadata fields do not vary by run but
            # may vary by REI set version ID. In the future, that parameter,
            # along with other settings, should be stored in this table
            "distribution": [rei_metadata["exposure_type"].iat[0]],
            "tmrel_lower": [rei_metadata["tmrel_lower"].iat[0]],
            "tmrel_upper": [rei_metadata["tmrel_upper"].iat[0]],
        }
    )
    model_version_metadata = (
        me_ids.query("~draw_type.isin(['paf', 'paf_unmediated']) & ~model_version_id.isna()")
        .rename(
            columns={
                "modelable_entity_id": "input_modelable_entity_id",
                "model_version_id": "input_model_version_id",
            }
        )
        .assign(paf_model_version_id=model_version_id)[
            [
                "paf_model_version_id",
                "input_model_version_id",
                "input_modelable_entity_id",
                "draw_type",
            ]
        ]
    )

    # Convert input model vesions to ints. They can be floats if any are missing
    model_version_metadata["input_model_version_id"] = model_version_metadata[
        "input_model_version_id"
    ].astype(int)

    return (paf_model_metadata, model_version_metadata)


def _insert_paf_metadata(
    paf_model_metadata: pd.DataFrame, model_version_metadata: pd.DataFrame, test: bool
) -> None:
    """Insert PAF metadata into the appropriate tables on epi.

    Inserts into epi.paf_model_metadata and epi.paf_model_version.
    """
    conn_def = DEV_CONN_DEF if test else PROD_CONN_DEF
    with db_tools_core.session_scope(conn_def) as session:
        paf_model_metadata.to_sql(
            name="paf_model_metadata",
            con=session.connection(),
            if_exists="append",
            index=False,
            method="multi",
        )
        model_version_metadata.to_sql(
            name="paf_model_version",
            con=session.connection(),
            if_exists="append",
            index=False,
            method="multi",
        )


if __name__ == "__main__":
    main()
