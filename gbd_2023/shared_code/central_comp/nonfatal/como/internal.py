"""COMO entry point."""

from typing import List, Optional, Tuple

from gbd.constants import RELEASE_ID, measures

from como.legacy import workflow

# appease linter, keeping mutable lists out of default args
DEFAULT_MEASURE_ID = [measures.YLD, measures.PREVALENCE, measures.INCIDENCE]
DEFAULT_COMPONENTS = ["cause", "sequela", "impairment", "injuries"]
DEFAULT_CONCURRENCY = {"como_inci": 250, "como_sim_input": 250}


def run_como(
    codcorrect_version: int,
    gbd_process_version_note: str,
    resume: Optional[bool] = False,
    como_dir: Optional[str] = None,
    release_id: Optional[int] = RELEASE_ID,
    location_set_id: Optional[int] = 35,
    nonfatal_cause_set_id: Optional[int] = 9,
    incidence_cause_set_id: Optional[int] = 12,
    impairment_rei_set_id: Optional[int] = 9,
    injury_rei_set_id: Optional[int] = 7,
    year_id: Optional[List[int]] = None,
    measure_id: Optional[List[int]] = DEFAULT_MEASURE_ID,
    n_draws: Optional[int] = 1000,
    n_simulants: Optional[int] = 20000,
    components: Optional[List[str]] = DEFAULT_COMPONENTS,
    change_years: Optional[List[Tuple[int]]] = None,
    agg_loc_sets: Optional[List[int]] = None,
    minimum_incidence_cod: bool = True,
    test_process_version: bool = False,
    internal_upload: bool = True,
    public_upload: bool = False,
    public_upload_test: bool = False,
    include_reporting_cause_set: bool = False,
    skip_sequela_summary: bool = False,
    task_template_concurrency: dict = DEFAULT_CONCURRENCY,
) -> None:
    """Runs the comorbidity simulation.

    Args:
        codcorrect_version: A valid CodCorrect version_id. Used to pull
                            residuals.py YLLs.
        gbd_process_version_note: A descriptive note that will be loaded into
            the gbd.gbd_process_version.gbd_process_version_note field
        resume: Optional workflow resume flag.
        como_dir: Optional workflow resume directory for existing como version.
        release_id: Optional release id. If omitted, the current release
                      will be used.
        location_set_id: Optional ID of the location set. If omitted,
                         location_set_id 35 (Model Results) will be used.
        nonfatal_cause_set_id: Optional ID of the nonfatal cause set. If
                               omitted, cause_set_id 9 (Nonfatal Capstone)
                               will be used.
        incidence_cause_set_id: Optional ID of the incidence cause set. If
                                omitted, cause_set_id 12 (GBD nonfatal
                                incidence) will be used.
        impairment_rei_set_id: Optional ID of the impairment rei set. If
                               omitted, rei_set_id 9 (GBD Estimation
                               Impairments) will be used.
        injury_rei_set_id: Optional ID of the injury rei set. If omitted,
                           rei_set_id 7 (GBD Injuries) will be used.
        year_id: Optional list of years. If omitted, estimation years for the
                 current release will be used.
        measure_id: Optional list of measures to produce. If omitted, results
                    for YLD, prevalence & incidence will be generated.
        n_draws: Optional number of draws. If omitted, 1,000 draws will be
                 used.
        n_simulants: Optional number of simulants. If omitted, 20,000
                     simulants will be used.
        components: Optional list of components. If omitted, cause, sequela,
                    injuries and impairments will be used.
        change_years: Optional list of change year tuples. If omitted, will
                      default to None.
        agg_loc_sets: Optional list of aggregate location sets. If omitted,
                      will default to None.
        minimum_incidence_cod:  Optional, whether to calculate minimum incidence
                                from codcorrect and potentially adjust cause-level
                                incidence accordingly.
        test_process_version: Optional, whether or not the process version created is
                              a test version, default False, prod process version.
        internal_upload: Optional internal gbd db upload results flag. Default
                         is True.
        public_upload: Optional public gbd db upload results flag. Default is
                       False.
        public_upload_test: Optional public gbd-test db upload flag. Default
                            is False.
        include_reporting_cause_set: Optional flag to include the reporting
                                     cause set (cause_set_id=16 - Reporting
                                     only cause aggregates). Default is
                                     False.
        skip_sequela_summary: Optional flag to skip sequelae summary generation.
                              Also skips sequelae summary upload. Default is False.
        task_template_concurrency: Optional dictionary of task template concurrency
            with keys as task template names and values as the number of concurrent
            tasks to run for that task template.

    Prompts:
        Input: waits for user to validate missing best model inputs before
               continuing workflow.

    Errors:
        ValueError: if a `codcorrect_version` is not supplied.
        ValueError: if `como_dir` is not supplied on a resume.
    """
    if resume and not como_dir:
        raise ValueError("como_dir is required.")

    if public_upload_test and internal_upload:
        raise RuntimeError(
            "Cannot currently run a public upload test and also upload to an internal DB. If "
            "you'd like to run a public upload test, please run with internal_upload=False. "
            "If you'd like to upload to the internal DB, please run with either "
            "public_upload=False or public_upload_test=False."
        )

    como_dir = None if not resume else como_dir
    workflow.run_como(
        como_dir=como_dir,
        release_id=release_id,
        gbd_process_version_note=gbd_process_version_note,
        location_set_id=location_set_id,
        nonfatal_cause_set_id=nonfatal_cause_set_id,
        incidence_cause_set_id=incidence_cause_set_id,
        impairment_rei_set_id=impairment_rei_set_id,
        injury_rei_set_id=injury_rei_set_id,
        year_id=year_id,
        measure_id=measure_id,
        n_draws=n_draws,
        n_simulants=n_simulants,
        components=components,
        change_years=change_years,
        agg_loc_sets=agg_loc_sets,
        minimum_incidence_cod=minimum_incidence_cod,
        codcorrect_version=codcorrect_version,
        test_process_version=test_process_version,
        internal_upload=internal_upload,
        public_upload=public_upload,
        public_upload_test=public_upload_test,
        include_reporting_cause_set=include_reporting_cause_set,
        resume=resume,
        skip_sequela_summary=skip_sequela_summary,
        task_template_concurrency=task_template_concurrency,
    )
