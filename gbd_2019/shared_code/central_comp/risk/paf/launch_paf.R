#' Given a risk, calculate draws of PAFs.
#'
#' Required args:
#' @param rei_id (int) Id of the risk you are running PAFs for.
#' Optional args:
#' @param year_id (list[int]) Which years do you want to calculate? Default is standard epi years for GBD 2019.
#' @param n_draws (int) How many draws should be used? Cannot be set higher than actual number of draws in input data, only downsampling, not upsampling is allowed. Default is 1000.
#' @param gbd_round_id (int) Default is 6, GBD 2019
#' @param decomp_step (str) step of decomp to run pafs for. step1-5, or iterative.
#' @param save_results (bool) Do you want to run save_results on the final PAF draws? Default is TRUE.
#' @param resume (bool) Do you want to resume where your last jobs finished? Default is FALSE.
#' @param cluster_proj (str) what project to run save_results job under on the cluster.
#' @param m_mem_free (str) how much memory to request for each job. Default is NULL, will run at 4G for categorical risks and 6G for continous risks. You can override if your jobs are getting killed...
#'
#' @return nothing. Will save draws in directory and optionally run save_results as well if requested.
#'
#' @examples
launch_paf <- function(rei_id, year_id = c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019),
                       n_draws = 1000, gbd_round_id = 6, decomp_step = NULL,
                       save_results = TRUE, resume = FALSE, cluster_proj = NULL,
                       m_mem_free=NULL, mediate=FALSE) {

  # load libraries and functions
  library(magrittr)
  library(data.table)
  library(ini)
  library(RMySQL)
  library(logging)
  source("FILEPATH/get_demographics.R")
  source("FILEPATH/get_model_results.R")
  setwd("FILEPATH")
  source("./utils/cluster.R")
  source("./utils/data.R")
  source("./utils/db.R")
  source("save.R")

  #--CHECK RUN ID AND ENVIRONMENT-----------------------------------------------
  rei_meta <- get_rei_meta(rei_id)
  rei <- rei_meta$rei
  cont <- rei_meta$calc_type == 2
  if (rei %in% c("drugs_alcohol", "drugs_illicit_direct", "air_pm", "air_ozone",
                 "abuse_ipv_hiv", "occ_injury")) {
    err_msg <- sprintf("%s is a custom PAF calculated by the modeler, not supported by this code base.", rei)
    logerror(err_msg);stop(err_msg)
  }
  if ((rei %in% c("nutrition_lbw", "nutrition_preterm")))
    stop(rei, " is a calculated with nutrition_lbw_preterm. Please submit jobs for ",
         "the joint risk, rei_id = 339 instead.")
  if(rei_meta$calc_type == 0)
    stop(rei, " is an aggregate risk PAF and will be calculated centrally after most-detailed risk pafs are ready.")
  out_dir <- paste0("FILEPATH", rei)
  if (mediate) {
    out_dir <- paste0("FILEPATH", rei)
  } else {
    out_dir <- paste0("FILEPATH", rei)
  }
  dir.create(out_dir, showWarnings = FALSE)

  # set up logging
  main_log_file <- paste0(out_dir, "/launch_paf_", format(Sys.time(), "%m-%d-%Y %I:%M:%S"), ".log")
  logReset()
  basicConfig(level='DEBUG')
  addHandler(writeToFile, file=main_log_file)

  message(sprintf("Logging here %s", main_log_file))
  loginfo("PAF calc for %s", rei)

  # validate args
  loginfo("Validating args")
  demo <- suppressMessages(get_demographics(gbd_team = "epi_ar", gbd_round_id = gbd_round_id))
  sex_ids <- demo$sex_id
  if(is.na(rei_meta$female)) sex_ids <- 1
  if(is.na(rei_meta$male)) sex_ids <- 2
  if (!all(year_id %in% demo$year_id)) {
    err_msg <- paste0("Not all provided year_id(s) are valid GBD year_ids. Invalid years: ",
                      paste(setdiff(year_id, demo$year_id), collapse = ", "))
    logerror(err_msg);stop(err_msg)
  }
  if(is.null(decomp_step)) { logerror("decomp_step cannot be NULL");stop("decomp_step cannot be NULL") }
  mes <- get_rei_mes(rei_id, gbd_round_id, decomp_step)
  missing_models <- mes[draw_type != "paf" & is.na(model_version_id), ]
  if (rei %in% c("metab_fpg_cont", "metab_bmi_adult")) missing_models <- missing_models[draw_type != "exposure_sd", ]
  if (nrow(missing_models) > 0) {
    err_msg <- paste0("Not all needed PAF inputs (", paste(unique(missing_models$draw_type), collapse = ", "),
                      ") have a best model version. See modelable_entity_id(s): ",
                      paste(unique(missing_models$modelable_entity_id), collapse = ", "))
    logerror(err_msg);stop(err_msg)
  }
  exp_years <- rbindlist(lapply(mes[draw_type == "exposure", modelable_entity_id], function(x)
    suppressMessages(get_model_results(
      "epi", gbd_id=x, age_group_id=22, location_id=1, year_id=year_id,
      gbd_round_id = gbd_round_id, decomp_step = decomp_step))))$year_id %>% unique
  if (!all(year_id %in% exp_years)) {
    err_msg <- paste0("Not all provided year_id(s) are present in current best ",
                      "exposures. Missing years: ", paste(setdiff(year_id, exp_years), collapse = ", "))
    logerror(err_msg);stop(err_msg)
  }
  if (cont & !(rei %in% c("metab_fpg_cont", "metab_bmi_adult"))) {
    exp_sd_years <- rbindlist(lapply(mes[draw_type == "exposure_sd", modelable_entity_id], function(x)
      suppressMessages(get_model_results(
        "epi", gbd_id=x, age_group_id=22, location_id=1, year_id=year_id,
        gbd_round_id = gbd_round_id, decomp_step = decomp_step))))$year_id %>% unique
    if (!all(year_id %in% exp_sd_years)) {
      err_msg <- paste0("Not all provided year_id(s) are present in current best ",
                        "exposure SDs. Missing years: ", paste(setdiff(year_id, exp_sd_years), collapse = ", "))
      logerror(err_msg);stop(err_msg)
    }
  }
  if (!(1 <= n_draws & n_draws <= 1000)) {
    err_msg <- "n_draws must be between 1-1000."
    logerror(err_msg);stop(err_msg)
  }
  if (n_draws < 100)  {
    err_msg <- "n_draws must be at least 100 for GBD 2019 decomp."
    logerror(err_msg);stop(err_msg)
  }
  if (rei == "air_pmhap" & save_results==TRUE) {
    err_msg <- paste0("save_results must be FALSE if running ", rei, " PAFs.")
    logerror(err_msg);stop(err_msg)
  }
  if (mediate == TRUE & save_results==TRUE) {
    err_msg <- paste0("save_results must be FALSE if running non-mediated PAFs.")
    logerror(err_msg);stop(err_msg)
  }
  loginfo("rei_id=%d, year_id=[%s], n_draws=%d, gbd_round_id=%d, decomp_step=%s, save_results=%s, resume=%s, cluster_proj=%s, mediate=%s",
          rei_id, paste(year_id, collapse = ", "), n_draws, gbd_round_id, decomp_step,
          save_results, resume, cluster_proj, mediate)

  #--WIPE DIRECTORY IF NOT IN RESUME MODE --------------------------------------

  if (!resume) {
    logdebug("Wiping intermediate directory as not running in resume mode. You have 10 seconds to change your mind...")
    Sys.sleep(10)
    logdebug("deleting files from %s now.", out_dir)
    unlink(list.files(out_dir, pattern = "^save_results_", full.names = T))
    unlink(list.files(out_dir, pattern = ".csv$", full.names = T))
    write.csv(mes, paste0(out_dir, "/mes.csv"), row.names=F)
  }

  #--CALCULATE THE PAFS---------------------------------------------------------

  user <- Sys.info()[["user"]]
  log_dir <- paste0("FILEPATH", user)
  dir.create(file.path(log_dir, "output"), showWarnings = FALSE)
  dir.create(file.path(log_dir, "errors"), showWarnings = FALSE)

  # calculate max/min exposure for PAF (and later SEVs) if continuous
  if (cont & resume == F & !(rei %in% c("metab_fpg_cont", "metab_bmi_adult"))) {
    loginfo("Launching exposure maxmin calc for %s", rei)
    dir.create(paste0(out_dir, "/exposure"), showWarnings = FALSE)
    qsub(job_name = paste0("exp_maxmin_", rei), script = "exp_max_min.R",
         m_mem_free = "2G", fthread = 1, logs = log_dir,
         arguments = paste(rei_id, gbd_round_id, decomp_step, out_dir, sep = " "),
         cluster_project = cluster_proj)
    job_hold(paste0("exp_maxmin_", rei))
    if (!file.exists(paste0(out_dir, "/exposure/exp_max_min.csv"))) {
      err_msg <- "Job failed, unable to continue to PAF calculation."
      logerror(err_msg);stop(err_msg)
    }
  }

  if (resume == F & rei %like% "diet") {
    # RR array job by location and sex
    loginfo("Launching MRBRT job to pull RRs for %s", rei)
    unlink(paste0(out_dir, "/mrbrt/rr.csv"))
    qsub(job_name = paste0("mrbrt_rr_", rei),
         script = paste0("custom/diet_rr.R"),
         m_mem_free = "10G", fthread = 1,
         logs = log_dir,
         arguments = paste(rei_id, out_dir, sep = " "),
         cluster_project = cluster_proj)
    job_hold(paste0("mrbrt_rr_", rei))
    if (!file.exists(paste0(out_dir, "/mrbrt/rr.csv"))) {
      err_msg <- "Job failed, unable to continue to PAF calculation."
      logerror(err_msg);stop(err_msg)
    }
  }

  # PAF array job by location and sex
  loginfo("Launching PAF calc for %s", rei)
  params <- expand.grid(location_id = demo$location_id, sex_id = sex_ids) %>% data.table
  # if running in resume, we only want to launch for files that don't exist
  if (resume) {
    fini <- NULL
    loginfo("In resume mode, finding jobs to relaunch.")
    for (i in 1:nrow(params)) {
      if (file.exists(paste0(out_dir, "/", params[i, location_id], "_", params[i, sex_id], ".csv"))) {
        fini <- c(fini, i)
      }
    }
    if (!is.null(fini)) params <- params[-fini, ]
  }
  # some risks have their own script
  script <- ifelse(rei %in% c("drugs_illicit_suicide", "envir_lead_blood",
                              "nutrition_lbw_preterm", "abuse_ipv_paf",
                              "unsafe_sex", "metab_fpg_cont", "metab_bmi_adult",
                              "activity", "air_pmhap"),
                   paste0("custom/", rei, ".R"), "paf_calc.R")
  if (grepl("^diet_", rei)) script <- paste0("custom/diet_paf.R")

  if (is.null(m_mem_free)) m_mem_free <- ifelse(cont, "10G", "4G") # continous risks need more memory
  fthread <- ifelse(grepl("^diet_", rei), 2, ifelse(cont, 6, 1))

  retry_qsub(job_name = paste0("paf_calc_", rei), script = script, params = params,
             out_dir = out_dir, m_mem_free = m_mem_free, fthread = fthread,
             logs = log_dir,
             arguments = paste(rei_id,
                               paste0("'c\\(", paste(year_id, collapse = ","), "\\)'"),
                               n_draws, gbd_round_id, decomp_step, out_dir, mediate, sep = " "),
             cluster_project = cluster_proj)

  #--SAVE RESULTS---------------------------------------------------------------

  if (save_results) {
    loginfo("Launching save_results job now.")
    mes <- fread(paste0(out_dir, "/mes.csv"))
    description <- paste0("exposure mvid ", paste(unique(mes[draw_type == "exposure"]$model_version_id), collapse = ";"))
    if (rei_id == 339) description <- paste0("exposure mvid ", min(mes[draw_type == "exposure"]$model_version_id), " to ", max(mes[draw_type == "exposure"]$model_version_id))
    if (nrow(mes[draw_type == "exposure_sd"]) != 0 & !(rei %in% c("metab_fpg_cont", "metab_bmi_adult"))) description <- paste0(description, " - exposure sd mvid ", paste(unique(mes[draw_type == "exposure_sd"]$model_version_id), collapse = ";"))
    if (rei %like% "diet") {
      tmrel <- fread("FILEPATH/2019_tmrels.csv")[rei == rei_meta$rei, ]
      description <- paste0(description, " - mrbrt relative risk - tmrel ", tmrel$tmrel_lower, " to ", tmrel$tmrel_upper)
    } else {
      if (nrow(mes[draw_type == "rr"]) != 0) description <- paste0(description, " - relative risk mvid ", paste(unique(mes[draw_type == "rr"]$model_version_id), collapse = ";"))
      if (nrow(mes[draw_type == "tmrel"]) != 0) description <- paste0(description, " - tmrel mvid ", paste(unique(mes[draw_type == "tmrel"]$model_version_id), collapse = ";"))
    }
    if (cont) description <- paste0(description, " - ", rei_meta$exp_dist, " distribution")
    meas_ids <- c(3, 4)
    if(is.na(rei_meta$yll)) meas_ids <- 3
    if(is.na(rei_meta$yld)) meas_ids <- 4
    save_results_risk(cluster_proj, out_dir, rei, ifelse(rei_id == 150, 8805, unique(mes[draw_type == "paf"]$modelable_entity_id)),
                      year_id, meas_ids, sex_ids, "paf", description, out_dir,
                      "{location_id}_{sex_id}.csv", gbd_round_id, decomp_step, n_draws)
    # for lbw/sga joint paf, also save the pafs for individual lbw and sga
    if (rei_id == 339) {
      for (uni in c(334, 335)) {
        mes <- get_rei_mes(uni, gbd_round_id, decomp_step)
        rei <- get_rei_meta(uni)$rei
        out_dir <- paste0("FILEPATH/", rei)
        save_results_risk(cluster_proj, out_dir, rei, unique(mes[draw_type == "paf"]$modelable_entity_id),
                          year_id, meas_ids, sex_ids, "paf", description, out_dir,
                          "{location_id}_{sex_id}.csv", gbd_round_id, decomp_step, n_draws)
      }
    }
    # for bmi and fpg, run save results on the exposure sd generate in PAF calc
    if (rei %in% c("metab_bmi_adult", "metab_fpg_cont")) {
      description <- paste0("exposure mvid ", paste(unique(mes[draw_type == "exposure"]$model_version_id), collapse = ";"),
                            " - generated in PAF calc with SD optimization")
      save_results_epi(cluster_proj, log_dir, rei, unique(mes[draw_type == "exposure_sd"]$modelable_entity_id),
                       year_id, 19, description, paste0(out_dir, "/exp_sd"),
                       gbd_round_id, decomp_step, n_draws)
    }
  }

  # shut down logging
  logReset()

}
