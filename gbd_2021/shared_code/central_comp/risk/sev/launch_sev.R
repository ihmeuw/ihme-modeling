#' sev calculator
#'
#' Required args:
#' @param version_id (int) version of sevs, if not specified, will use max + 1.
#' @param paf_version_id (int) version of pafs, if not specified, will use max. Used only for SEVs, not for RRmax
#' @param codcorrect_version (int) version of CoDCorrect to use when calculating cause aggregate PAFs. Used only for SEVs not RRmax.
#' @param como_version (int) version of COMO to use when calculating cause aggregate PAFs. Used only for SEVs not RRmax.

#' Optional args:
#' @param year_id (list[int]) Which years do you want to calculate?
#' @param location_set_id (list[int]) what location sets to aggregate?
#' @param pct_change (bool) If running for multiple year_ids, do you want to run percent change as well? If running for a single year_id, the function is indifferent to this argument.
#' @param n_draws (int) How many draws should be used? Cannot be set higher than actual number of draws in input data, only downsampling, not upsampling is allowed. Default is 1000.
#' @param gbd_round_id (int) Default is 7
#' @param decomp_step (str) step of decomp to run pafs for. step1-5, or iterative.
#' @param resume (bool) will resume a workflow for an existing SEV run
#' @param cluster_proj (str) what project to run save_results job under on the cluster.
#' @param by_cause (bool) save risk-cause-specific SEVs in addition to risk-level SEVs.
#' @param measures (list[str]) measures to produce: "rrmax" or c("rrmax", "sev"). Case-insensitive, default is c("rrmax", "sev"). RRmax is required to run SEVs.
#' @param working_dir (str) Working directory for launching scripts, if working out of a feature branch or development repo.
#'
#' @return nothing. Will save draws in directory, make summaries, and upload to gbd db
#'
#' @examples
launch_sev <- function(version_id = NULL, paf_version_id = NULL, codcorrect_version = NULL,
                       como_version = NULL, year_id = NULL, location_set_id = 35, pct_change = TRUE,
                       n_draws = 1000, gbd_round_id = 7, decomp_step = NULL, resume = FALSE,
                       cluster_proj = "proj", by_cause = FALSE,
                       measures = c("rrmax", "sev"), working_dir = "FILEPATH") {

  # load libraries and functions
  library(magrittr)
  library(jobmonr, lib.loc = "FILEPATH")

  source("FILEPATH/get_demographics.R")
  source("FILEPATH/get_location_metadata.R")
  source("FILEPATH/get_population.R")
  source("FILEPATH/get_rei_metadata.R")
  source("FILEPATH/db.R")

  #--CHECK RUN ID AND ENVIRONMENT-----------------------------------------------
  if (!(.Platform$OS.type == "unix"))
    stop("Can only launch parallel jobs for this script on the cluster")

  out_dir <- "FILEPATH"
  if(is.null(version_id)) {
    version_id <- list.files(out_dir) %>% as.numeric %>% max(., na.rm = T)
    version_id <- version_id + 1
  }
  out_dir <- paste0(out_dir, "/", version_id)
  if (!resume & dir.exists(out_dir)) {
    stop("SEV v", version_id, " already exists and this is not a resumed run. ",
         "Please choose a new version or leave out the version argument")
  }
  dir.create(out_dir, showWarnings = FALSE)
  dir.create(paste0(out_dir,"/rrmax/summaries/upload"), showWarnings = FALSE, recursive = TRUE)
  dir.create(paste0(out_dir,"/rrmax/exposure_max_min"), showWarnings = FALSE)
  dir.create(paste0(out_dir,"/draws"), showWarnings = FALSE)
  dir.create(paste0(out_dir,"/summaries"), showWarnings = FALSE)
  if (by_cause) {
    dir.create(paste0(out_dir,"/risk_cause/draws"), showWarnings = FALSE, recursive = TRUE)
    dir.create(paste0(out_dir,"/risk_cause/summaries"), showWarnings = FALSE, recursive = TRUE)
  }

  message(format(Sys.time(), "%D %H:%M:%S"), " SEV v", version_id)

  # validate args
  message("Validating args")
  demo <- get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)
  annual_years <- min(demo$year_id):max(demo$year_id)
  if (is.null(year_id)) year_id <- demo$year_id
  if (!all(year_id %in% annual_years))
    stop("Not all provided year_id(s) are valid GBD year_ids, (",
         paste(setdiff(year_id, annual_years), collapse = ", "), ")")
  if (length(year_id) == 1) pct_change <- FALSE
  if (!(1 <= n_draws & n_draws <= 1000))
    stop("n_draws must be between 1-1000.")
  paf_versions <- list.files("FILEPATH/", pattern="^[0-9]+$") %>% as.numeric
  if(is.null(paf_version_id)) paf_version_id <- max(paf_versions, na.rm=T)
  if (!(paf_version_id %in% paf_versions))
    stop(paf_version_id, " is not a valid PAF version")
  paf_version <- fread(paste0("FILEPATH/", paf_version_id, "/version.csv"))
  decomp_step_id <- get_decomp_step_id(decomp_step, gbd_round_id)
  paf_decomp_step_id <- paf_version$decomp_step_id %>% unique
  if (decomp_step_id != paf_decomp_step_id)
    stop("PAF v", paf_version_id, " is from decomp_step_id ", paf_decomp_step_id, ", not ", decomp_step_id)
  paf_gbd_round_id <- paf_version$gbd_round_id %>% unique
  if (gbd_round_id != paf_gbd_round_id)
    stop("PAF v", paf_version_id, " is from gbd_round_id ", paf_gbd_round_id, ", not ", gbd_round_id)
  # only rrmax and sev measures are valid, and rrmax is required for sevs
  rrmax_measure <- "rrmax"
  sev_measure <- "sev"
  valid_measures <- c(rrmax_measure, sev_measure)
  measures <- lapply(measures, tolower)
  if (!all(measures %in% valid_measures))
    stop("One or more measures is invalid. Valid measures are ", paste(valid_measures, collapse = ", "))
  if ((sev_measure %in% measures) & (!rrmax_measure %in% measures))
    stop("RRmax measure must be included to calculate SEVs")
  if (sev_measure %in% measures & is.null(codcorrect_version))
    stop("CoDCorrect version cannot be NULL when calculating SEVs")
  if (sev_measure %in% measures & is.null(como_version))
    stop("COMO version cannot be NULL when calculating SEVs")
  # these inputs are unused when we're only running rrmax
  if (!sev_measure %in% measures) {
    codcorrect_version <- NA
    como_version <- NA
  }
  message("paf_version_id ", paf_version_id,
          ", codcorrect_version ", codcorrect_version,
          ", como_version ", como_version,
          ", year_id (", paste(year_id, collapse = ", "),
          "), location_set_id ", location_set_id,
          ", pct_change ", pct_change,
          ", n_draws ", n_draws,
          ", gbd_round_id ", gbd_round_id,
          ", decomp_step ", decomp_step,
          ", resume ", resume,
          ", cluster_proj ", cluster_proj,
          ", by_cause ", by_cause,
          ", measures (", paste(measures, collapse = ", "), ")",
          ", working_dir ", working_dir)

  pop_run_id <- unique(get_population(gbd_round_id = gbd_round_id, decomp_step = decomp_step)$run_id)
  sev_metadata <- data.table(version_id = version_id,
                             paf_version = paf_version_id,
                             codcorrect_version = codcorrect_version,
                             como_version = como_version,
                             year_id = paste(year_id, collapse = ", "),
                             location_set_id = paste(location_set_id, collapse = ", "),
                             pct_change = pct_change,
                             n_draws = n_draws,
                             gbd_round_id = gbd_round_id,
                             decomp_step = decomp_step,
                             by_cause = by_cause,
                             measures = paste(measures, collapse = ", "),
                             working_dir = working_dir,
                             pop_run_id = pop_run_id)
  write.csv(sev_metadata, paste0(out_dir, "/version.csv"), row.names = FALSE, na = "")

  # find REIs to calc SEVs for
  paf_reis <- fread(paste0("FILEPATH/", paf_version_id, "/existing_reis.csv.gz"))
  get_rei_hierarchy <- function(rei_set_id, gbd_round_id, decomp_step) {
    return(get_rei_metadata(
      rei_set_id = rei_set_id, gbd_round_id = gbd_round_id, decomp_step = decomp_step
    )[rei_id %in% c(339, 380), most_detailed := 1])
  }
  rep_reis <- get_rei_hierarchy(rei_set_id = 1, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
  comp_reis <- get_rei_hierarchy(rei_set_id = 2, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
  reis_without_rr <- c("abuse_ipv_hiv", "abuse_ipv_paf", "drugs_illicit_direct",
                       "envir_lead_blood", "occ_injury", "unsafe_sex")
  temperature_reis <- c("temperature", "temperature_high", "temperature_low")
  reis <- comp_reis[(most_detailed == 1) & !(rei %in% reis_without_rr) & !(rei %in% temperature_reis)]
  # if only running rrmax, don't restrict risks to a specific PAF run
  if (sev_measure %in% measures) {
    rei_ids <- intersect(paf_reis$rei_id, reis$rei_id)
    aggregate_rei_ids <- intersect(
      paf_reis$rei_id, setdiff(
        rep_reis[!(rei %in% reis_without_rr) & !(rei %in% temperature_reis), ]$rei_id,
        rei_ids)
    )
  } else {
    rei_ids <- reis$rei_id
    aggregate_rei_ids <- setdiff(
        rep_reis[!(rei %in% reis_without_rr) & !(rei %in% temperature_reis), ]$rei_id,
        rei_ids
    )
  }

  user <- Sys.info()[["user"]]
  log_dir <- paste0("FILEPATH/", user)
  dir.create(file.path(log_dir, "output"), showWarnings = FALSE)
  dir.create(file.path(log_dir, "errors"), showWarnings = FALSE)

  # cache population for rr_max, aggregation, and summarization steps
  population_files <- list.files(out_dir, pattern = "^population", full.names = TRUE)
  if (length(location_set_id) != length(population_files)) {
    message("Caching population")
    for(lsid in location_set_id) {
      pop <- get_population(year_id=union(year_id, demo$year_id), location_id="all",
                            sex_id="all", age_group_id="all", gbd_round_id=gbd_round_id,
                            decomp_step=decomp_step, location_set_id=lsid,
                            run_id = pop_run_id)
      write.csv(
        pop[, run_id := NULL],
        paste0(out_dir, "/population_", lsid, ".csv"),
        row.names=F
      )
    }
  }

  # get the list of all expected locations including aggregates
  location_metadata <- rbindlist(
    lapply(location_set_id,
           function(x) {
             get_location_metadata(location_set_id=x, gbd_round_id=gbd_round_id,
                 decomp_step=decomp_step)
           }),
    use.names = TRUE)
  all_locations <- unique(location_metadata$location_id)

  #-- SET UP JOBMON WORKFLOW AND TEMPLATES -------------------------------------

  message("Building workflow and task templates")
  rshell <- "FILEPATH"
  python <- paste0(working_dir, "FILEPATH")
  cluster <<- "slurm"

  # create a sev workflow and set default resources
  sev_tool <- jobmonr::tool(name="sev_calculator")
  jobmonr::set_default_tool_resources(
    sev_tool,
    default_cluster_name=cluster,
    resources=list(
      "project"=cluster_proj,
      "working_dir"=working_dir,
      "stdout"=paste0(log_dir, "/output"),
      "stderr"=paste0(log_dir, "/errors")
    )
  )
  sev_workflow <- jobmonr::workflow(
      tool=sev_tool,
      workflow_args=paste0("sev_calculator_", version_id),
      name=paste0("SEV Calculator v", version_id)
  )

  # task templates and resource parameters
  source(paste0(working_dir, "/jobmon_templates.R"))

  #-- CREATE TASKS -------------------------------------------------------------

  # create RRmax tasks by risk
  message("Creating RR max tasks")
  rr_max_tasks_by_risk <- list()
  for (rei_id in rei_ids) {
    rid <- rei_id
    rr_max_task <- jobmonr::task(
      task_template=template_rr_max,
      compute_resources=get_resources_rr_max(reis[rei_id==rid]$rei),
      name=paste0("sev_rr_max_", rei_id),
      rei_id=as.integer(rei_id),
      gbd_round_id=as.integer(gbd_round_id),
      decomp_step=decomp_step,
      year_id=paste(year_id, collapse = ","),
      n_draws=as.integer(n_draws),
      out_dir=out_dir,
      rshell=rshell,
      scriptname=paste0(working_dir, "/rr_max.R")
    )
    rr_max_tasks_by_risk[[rei_id]] <- rr_max_task
  }

  # create tasks to calculate RR max for aggregate risks
  message("Creating RR max aggregation tasks")
  aggregation_rei_set <- ifelse(gbd_round_id < 7, 2, 13)
  child_reis <- get_rei_hierarchy(
    rei_set_id = aggregation_rei_set, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
  child_reis <- child_reis[!rei_id %in% c(334, 335, 86, 87), ]
  child_reis <- child_reis[, .(parent_id=as.numeric(tstrsplit(path_to_top_parent, ","))),
                       by=c("rei_id", "most_detailed")]
  child_reis <- child_reis[most_detailed == 1, ]

  for (agg_rei_id in aggregate_rei_ids) {
    child_rei_ids <- child_reis[parent_id == agg_rei_id, ]$rei_id
    agg_rr_task <- jobmonr::task(
      task_template=template_agg_rr,
      compute_resources=resources_agg_rr,
      name=paste0("sev_agg_rr_", agg_rei_id),
      upstream_tasks=unlist(rr_max_tasks_by_risk[child_rei_ids]),
      agg_rei_id=as.integer(agg_rei_id),
      child_rei_ids=paste(child_rei_ids, collapse = ","),
      gbd_round_id=as.integer(gbd_round_id),
      n_draws=as.integer(n_draws),
      out_dir=out_dir,
      rshell=rshell,
      scriptname=paste0(working_dir, "/agg_rr_max.R")
    )
    # add to the RR max task list since output is RR max
    rr_max_tasks_by_risk[[agg_rei_id]] <- agg_rr_task
  }
  # add back in these aggregate reporting risks for the rest of the pipeline
  rei_ids <- c(rei_ids, aggregate_rei_ids)

  # create rrmax summarization tasks per risk. Dependent on
  # rrmax tasks.
  message("Creating rrmax summarization tasks")
  sum_rrmax_tasks <- list()
  for (rei_id in rei_ids) {
    sum_rrmax_task <- jobmonr::task(
      task_template=template_summary_rrmax,
      compute_resources=resources_summary_rrmax,
      name=paste0("sev_summary_rrmax_", rei_id),
      upstream_tasks=list(rr_max_tasks_by_risk[[rei_id]]),
      sev_version_id=as.integer(version_id),
      rei_id=as.integer(rei_id),
      gbd_round_id=as.integer(gbd_round_id),
      python=python,
      scriptname=paste0(working_dir, "/summarize_rrmax.py")
    )
    sum_rrmax_tasks[[rei_id]] <- sum_rrmax_task
  }

  message("Creating RR max upload task")
  rr_max_upload_task <- jobmonr::task(
    task_template=template_upload,
    compute_resources=resources_upload,
    name="rr_max_upload",
    upstream_tasks=unlist(sum_rrmax_tasks),
    version_id=as.integer(version_id),
    gbd_round_id=as.integer(gbd_round_id),
    decomp_step=decomp_step,
    change=as.integer(pct_change),
    measure="rrmax",
    python=python,
    scriptname=paste0(working_dir, "/upload.py")
  )

  if (sev_measure %in% measures) {
    # create SEV calc tasks by location and risk. Upstream task is the RR max
    # for the given risk.
    message("Creating SEV calc tasks")
    sev_calc_tasks_by_risk_loc <- list()
    for (rei_id in rei_ids) {
      sev_calc_tasks_by_risk_loc[[rei_id]] <- list()
      for (location_id in demo$location_id) {
        sev_calc_task <- jobmonr::task(
          task_template=template_sev_calc,
          compute_resources=resources_sev_calc,
          name=paste0("sev_calc_", rei_id, "_", location_id),
          upstream_tasks=list(rr_max_tasks_by_risk[[rei_id]]),
          location_id=as.integer(location_id),
          rei_id=as.integer(rei_id),
          year_id=paste(year_id, collapse = ","),
          n_draws=as.integer(n_draws),
          gbd_round_id=as.integer(gbd_round_id),
          decomp_step=decomp_step,
          out_dir=out_dir,
          paf_version=as.integer(paf_version_id),
          codcorrect_version=as.integer(codcorrect_version),
          como_version=as.integer(como_version),
          by_cause=by_cause,
          rshell=rshell,
          scriptname=paste0(working_dir, "/sev_calc.R")
        )
        sev_calc_tasks_by_risk_loc[[rei_id]][[location_id]] <- sev_calc_task
      }
    }

    # if temperature risks are included in the PAF compile, create temperature etl tasks
    # by location. These pull in the modeler-provided SEVs for all temperature risks for
    # the given location. No upstream dependencies.
    include_temperature <- any(rep_reis[rei %in% temperature_reis]$rei_id %in% paf_reis$rei_id)
    if (include_temperature) {
      message("Creating temperature etl tasks")
      temp_etl_tasks_by_loc <- list()
      for (location_id in demo$location_id) {
        temp_etl_task <- jobmonr::task(
          task_template=template_temp_etl,
          compute_resources=resources_temp_etl,
          name=paste0("sev_temp_etl_", location_id),
          location_id=as.integer(location_id),
          year_id=paste(year_id, collapse = " "),
          n_draws=as.integer(n_draws),
          sev_version_id=as.integer(version_id),
          gbd_round_id=as.integer(gbd_round_id),
          decomp_step=decomp_step,
          by_cause=as.integer(by_cause),
          python=python,
          scriptname=paste0(working_dir, "/temperature_etl.py")
        )
        temp_etl_tasks_by_loc[[location_id]] <- temp_etl_task
      }
      # add back in the temperature risks for aggregation/summarization
      rei_ids <- c(rei_ids, rep_reis[rei %in% temperature_reis]$rei_id)
    }

    # create location aggregation tasks for each risk. Upstream dependencies are
    # all SEV calculation tasks for the given risk.
    message("Creating location aggregation tasks")
    loc_agg_tasks_by_risk = list()
    for (rei_id in rei_ids) {
      if (rei_id %in% rep_reis[rei %in% temperature_reis]$rei_id) {
        # temperature loc agg depends on all etl tasks finishing
        upstream_task_list <- unlist(temp_etl_tasks_by_loc)
      } else {
        # loc agg depends on all sev tasks for the given risk
        upstream_task_list <- unlist(sev_calc_tasks_by_risk_loc[[rei_id]])
      }
      loc_agg_task <- jobmonr::task(
        task_template=template_loc_agg,
        compute_resources=resources_loc_agg,
        name=paste0("sev_loc_agg_", rei_id),
        upstream_tasks=upstream_task_list,
        sev_version_id=as.integer(version_id),
        rei_id=as.integer(rei_id),
        location_set_id=paste(location_set_id, collapse = " "),
        n_draws=as.integer(n_draws),
        gbd_round_id=as.integer(gbd_round_id),
        decomp_step=decomp_step,
        by_cause=as.integer(by_cause),
        python=python,
        scriptname=paste0(working_dir, "/aggregate.py")
      )
      loc_agg_tasks_by_risk[[rei_id]] <- loc_agg_task
    }

    # create summarization tasks per location. Dependent on all location
    # aggreagation tasks.
    message("Creating summarization tasks")
    summary_tasks <- list()
    pct_change_arg <- ifelse(pct_change, "--change", "")
    for (location_id in all_locations) {
      summary_task <- jobmonr::task(
        task_template=template_summary,
        compute_resources=resources_summary,
        name=paste0("sev_summary_", location_id),
        upstream_tasks=unlist(loc_agg_tasks_by_risk),
        sev_version_id=as.integer(version_id),
        location_id=as.integer(location_id),
        year_id=paste(year_id, collapse = " "),
        gbd_round_id=as.integer(gbd_round_id),
        change=pct_change_arg,
        by_cause=as.integer(by_cause),
        python=python,
        scriptname=paste0(working_dir, "/summarize.py")
      )
      summary_tasks[[location_id]] <- summary_task
    }

    # create upload task, dependent on all summary tasks
    message("Creating SEV upload task")
    sev_upload_task <- jobmonr::task(
      task_template=template_upload,
      compute_resources=resources_upload,
      name="sev_upload",
      upstream_tasks=unlist(summary_tasks),
      version_id=as.integer(version_id),
      gbd_round_id=as.integer(gbd_round_id),
      decomp_step=decomp_step,
      change=as.integer(pct_change),
      measure="sev",
      python=python,
      scriptname=paste0(working_dir, "/upload.py")
    )
  }

  message("Creating compare version task")
  compare_version_upstream <- if(sev_measure %in% measures) {
    list(rr_max_upload_task, sev_upload_task)
  } else {
    list(rr_max_upload_task)
  }
  compare_version_task <- jobmonr::task(
    task_template=template_compare_version,
    compute_resources=resources_upload,
    name="create_compare_version",
    upstream_tasks=compare_version_upstream,
    version_id=as.integer(version_id),
    gbd_round_id=as.integer(gbd_round_id),
    decomp_step=decomp_step,
    sevs_were_generated=as.integer(sev_measure %in% measures),
    python=python,
    scriptname=paste0(working_dir, "/create_compare_version.py")
  )

  message("Running workflow")
  if (length(rr_max_tasks_by_risk) > 0) {
    bound_workflow <- jobmonr::add_tasks(sev_workflow, unlist(rr_max_tasks_by_risk, use.names=FALSE))
    bound_workflow <- jobmonr::add_tasks(sev_workflow, unlist(sum_rrmax_tasks, use.names=FALSE))
    bound_workflow <- jobmonr::add_tasks(sev_workflow, c(rr_max_upload_task))
  }
  if (sev_measure %in% measures) {
    if (length(sev_calc_tasks_by_risk_loc) > 0)
      bound_workflow <- jobmonr::add_tasks(sev_workflow, unlist(sev_calc_tasks_by_risk_loc, use.names=FALSE))
    if (include_temperature)
      bound_workflow <- jobmonr::add_tasks(sev_workflow, unlist(temp_etl_tasks_by_loc, use.names=FALSE))
    bound_workflow <- jobmonr::add_tasks(sev_workflow, unlist(loc_agg_tasks_by_risk, use.names=FALSE))
    bound_workflow <- jobmonr::add_tasks(sev_workflow, unlist(summary_tasks, use.names=FALSE))
    bound_workflow <- jobmonr::add_tasks(sev_workflow, c(sev_upload_task))
  }
  bound_workflow <- jobmonr::add_tasks(sev_workflow, c(compare_version_task))
  status <- jobmonr::run(bound_workflow, resume=resume, seconds_until_timeout=(60 * 60 * 48))
  if (status != "D") {
    stop("The workflow failed")
  } else {
    message("The workflow completed successfully!")
  }
}
