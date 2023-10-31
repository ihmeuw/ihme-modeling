#' Given a risk, calculate draws of PAFs.
#'
#' Required args:
#' @param rei_id (int) Id of the risk you are running PAFs for.
#' @param decomp_step (str) step of decomp to run pafs for. step1-5, or iterative.
#' @param cluster_proj (str) what project to run jobs under on the cluster.
#' Optional args:
#' @param year_id (list[int]) Which years do you want to calculate? Default is standard epi years for given gbd_round_id.
#' @param n_draws (int) How many draws should be used? Cannot be set higher than actual number of draws in input data, only downsampling, not upsampling is allowed. Default is 1000.
#' @param gbd_round_id (int) Default is 7, GBD 2020
#' @param save_results (bool) Do you want to run save_results on the final PAF draws? Default is TRUE.
#' @param resume (bool) Do you want to resume where your last jobs finished? Default is FALSE.
#' @param m_mem_free (str) how much memory to request for each job. Default is NULL, will run at 4G for categorical risks and 6G for continous risks. You can override if your jobs are getting killed...
#' @param codcorrect_version (int) version of CoDCorrect to use when pulling YLLs. Required when running PAFs for BMD and not used otherwise.
#' @param update_bmi_fpg_exp_sd (bool) When running PAFs for BMI and FPG, do you want to update exposure SD as well? Default is TRUE.
#'
#' @return nothing. Will save draws in directory and optionally run save_results as well if requested.
#'
#' @examples
launch_paf <- function(rei_id, year_id = NULL, n_draws = 1000, gbd_round_id = 7,
                       decomp_step = NULL, save_results = TRUE, resume = FALSE,
                       cluster_proj = NULL, m_mem_free = NULL, codcorrect_version = 0,
                       update_bmi_fpg_exp_sd = TRUE) {

    # load libraries and functions
    library(magrittr)
    library(data.table)
    library(ini)
    library(RMySQL)
    library(logging)
    source("FILEPATH/get_demographics.R")
    source("FILEPATH/get_model_results.R")
    source("FILEPATH/get_draws.R")
    setwd("FILEPATH")
    source("mediate_rr.R")
    source("save.R")
    source("./utils/cluster.R")
    source("./utils/data.R")
    source("./utils/db.R")
    source("./utils/validations.R")

    #--CHECK RUN ID AND ENVIRONMENT-----------------------------------------------
    rei_meta <- get_rei_meta(rei_id, gbd_round_id)
    rei <- rei_meta$rei
    cont <- rei_meta$rei_calculation_type == 2
    if (is_custom_paf(rei)) {
        err_msg <- sprintf("%s is a custom PAF calculated by the modeler, not supported by this code base.", rei)
        logerror(err_msg);stop(err_msg)
    }
    if ((rei %in% c("nutrition_lbw", "nutrition_preterm")))
        stop(rei, " is a calculated with nutrition_lbw_preterm. Please submit jobs for ",
             "the joint risk, rei ID 339 instead.")
    if(rei_meta$rei_calculation_type == 0)
        stop(rei, " is an aggregate risk PAF and will be calculated centrally after most-detailed risk pafs are ready.")

    # Where mediation applies, unmediated pafs will be saved in a separate directory.
    base_dir <- "FILEPATHf"
    out_dir <- paste0(base_dir, "/", rei)
    dir.create(out_dir, showWarnings = FALSE)
    mediation <- get_mediation(rei_id)
    out_dir_unmed <- paste0(base_dir, "/unmediated/", rei)
    if (nrow(mediation) > 0) dir.create(out_dir_unmed, showWarnings = FALSE, recursive = TRUE)

    # set up logging
    unlink(list.files(out_dir, pattern = "^launch_paf_", full.names = T))
    main_log_file <- paste0(out_dir, "/launch_paf_", format(Sys.time(), "%Y%m%d_%I%M"), ".log")
    logReset()
    basicConfig(level='DEBUG')
    addHandler(writeToFile, file=main_log_file)

    message(sprintf("Logging here %s", main_log_file))
    unmediated_msg <- ifelse(nrow(mediation) > 0, " (including unmediated PAFs for relevant outcomes)", "")
    if (resume) {
        loginfo("Resuming previous PAF calc%s for %s. WARNING: this is not a new run!",
                unmediated_msg, rei)
    } else {
        loginfo("PAF calc%s for %s", unmediated_msg, rei)
    }

    # validate args
    loginfo("Validating args...")
    demo <- get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)
    sex_ids <- demo$sex_id
    if (is.na(rei_meta$female)) sex_ids <- 1
    if (is.na(rei_meta$male)) sex_ids <- 2
    if (is.null(year_id)) year_id <- demo$year_id
    if (!all(year_id %in% min(demo$year_id):max(demo$year_id))) {
        err_msg <- paste0("Not all provided year_id(s) are valid GBD year_ids. Invalid years: ",
                          paste(setdiff(year_id, demo$year_id), collapse = ", "))
        logerror(err_msg);stop(err_msg)
    }
    if(is.null(decomp_step)) { logerror("decomp_step cannot be NULL");stop("decomp_step cannot be NULL") }
    if(is.null(cluster_proj)) { logerror("cluster_proj cannot be NULL");stop("cluster_proj cannot be NULL") }

    mes <- get_rei_mes(rei_id, gbd_round_id, decomp_step)
    missing_models <- mes[!draw_type %in% c("paf", "paf_unmediated") & is.na(model_version_id), ]
    if (rei %in% c("metab_bmi_adult", "metab_fpg") & update_bmi_fpg_exp_sd)
        missing_models <- missing_models[draw_type != "exposure_sd", ]
    if (nrow(missing_models) > 0) {
        err_msg <- paste0("Not all needed PAF inputs (", paste(unique(missing_models$draw_type), collapse = ", "),
                          ") have a best model version. See modelable_entity_id(s): ",
                          paste(unique(missing_models$modelable_entity_id), collapse = ", "))
        logerror(err_msg);stop(err_msg)
    }
    if (nrow(mediation) > 0 & !"paf_unmediated" %in% mes$draw_type) {
        stop("This risk has mediation factors but requires a modelable entity set up for ",
             "saving unmediated PAFs. Please file a ticket.")
    }
    missing_years <- rbindlist(lapply(mes[draw_type == "exposure", modelable_entity_id], function(x)
        suppressMessages(get_model_results(
            "epi", gbd_id=x, age_group_id=22, location_id=1, year_id=year_id,
            gbd_round_id = gbd_round_id, decomp_step = decomp_step)
        )), use.names = TRUE)[, .(modelable_entity_id, year_id)] %>% unique %>%
        setnames(., "year_id", "present_year") %>%
        .[, missing_years := !all(year_id %in% present_year), by="modelable_entity_id"] %>%
        .[missing_years == TRUE]
    if (nrow(missing_years) > 0) {
        err_msg <- paste0("Not all provided year_id(s) are present in current best ",
                          "exposures for modelable_entity_id(s) ",
                          paste(unique(missing_years$modelable_entity_id), collapse = ", "),
                          ". Missing years: ",
                          paste(setdiff(year_id, unique(missing_years$present_year)), collapse = ", "))
        logerror(err_msg);stop(err_msg)
    }
    if (cont & !(rei %in% c("metab_bmi_adult", "metab_fpg") & update_bmi_fpg_exp_sd)) {
        exp_sd_years <- rbindlist(lapply(mes[draw_type == "exposure_sd", modelable_entity_id], function(x)
            suppressMessages(get_model_results(
                "epi", gbd_id=x, age_group_id=22, location_id=1, year_id=year_id,
                gbd_round_id = gbd_round_id, decomp_step = decomp_step))), use.names = TRUE)$year_id %>% unique
        if (!all(year_id %in% exp_sd_years)) {
            err_msg <- paste0("Not all provided year_id(s) are present in current best ",
                              "exposure SDs. Missing years: ", paste(setdiff(year_id, exp_sd_years), collapse = ", "))
            logerror(err_msg);stop(err_msg)
        }
    }
    if ("tmrel" %in% mes$draw_type) {
        tmrel_years <- fread(
            paste0("FILEPATH", mes[draw_type == "tmrel"]$model_version_id,
                   "/summaries/summaries.csv"))$year_id %>% unique
        if (!all(year_id %in% tmrel_years)) {
            err_msg <- paste0("Not all provided year_id(s) are present in current best ",
                              "TMRELs. Missing years: ", paste(setdiff(year_id, tmrel_years),
                                                               collapse = ", "))
            logerror(err_msg);stop(err_msg)
        }
    }

    # find all causes to calculate PAFs for: includes causes that are represented
    # in the distal risk's RR model plus causes that are mediated through another
    # risk using a delta between the two risks. Look up the relative risk types
    # and model versions for each cause.
    if ("rr" %in% mes$draw_type) {
        rr_metadata <- get_rr_metadata(mes[draw_type == "rr"]$model_version_id)
        # for sbp, drop hypertensive heart disease since 100%
        if (rei == "metab_sbp") rr_metadata <- rr_metadata[cause_id != 498, ]
        rr_metadata[, `:=` (source="rr", med_id=rei_id, mean_delta=1)]
    } else {
        rr_metadata <- data.table()
    }
    mediators <- get_mediator_cause_pairs(rei_id)[!cause_id %in% rr_metadata$cause_id, ]
    if (nrow(mediators) > 0) {
        mm_error_message <- paste0("Please submit a help desk ticket to Central Computation.")
        if (any(duplicated(mediators$cause_id))) {
            # It's currently only okay if CKD has multiple mediators, otherwise
            # we do not have code to handle this
            if (mediators[duplicated(mediators$cause_id), cause_id] != 589)
                stop("Mediation matrix contains causes not in RRs that are mediated through ",
                     "more than one risk. ", mm_error_message)
        }
        med_missing_delta <- unique(mediators[is.na(mean_delta)]$med_id)
        if (length(med_missing_delta) > 0) {
            stop("Delta value not found for mediator rei ID(s): ",
                 paste(med_missing_delta, collapse=", "), ". ", mm_error_message)
        }
        mf_below_1_causes <- unique(mediation[cause_id %in% mediators$cause_id]$cause_id)
        # For CKD, we expect MF to be < 1 if there are multiple mediators
        mf_below_1_causes <- setdiff(mf_below_1_causes, 589)
        if (length(mf_below_1_causes) > 0) {
            stop("Medation factors < 1 found for two-stage outcomes(s) ",
                 paste(mf_below_1_causes, collapse=", "), ". ", mm_error_message)
        }
        if (!all(unlist(lapply(mediators$med_id, valid_mediator_risk, gbd_round_id))) |
            !cont) {
            stop("Mediation for this risk involves categorical or custom-calculated risks. ",
                 mm_error_message)
        }
        med_mes <- rbindlist(lapply(unique(mediators$med_id), function(x) {
            get_rei_mes(x, gbd_round_id, decomp_step)[!draw_type %like% "paf", ]
        }), use.names = TRUE)
        missing_med_models <- med_mes[is.na(model_version_id), ]
        if (nrow(missing_med_models) > 0) {
            err_msg <- paste0("Not all needed RR inputs for mediator rei ID(s) have a best ",
                              "model version. See modelable_entity_id(s): ",
                              paste(unique(missing_med_models$modelable_entity_id), collapse = ", "))
            logerror(err_msg);stop(err_msg)
        }
        med_rr_metadata <- rbindlist(lapply(med_mes$model_version_id, get_rr_metadata), use.names = TRUE)
        setnames(med_rr_metadata, "rei_id", "med_id")
        med_rr_metadata <- merge(
            med_rr_metadata, mediators, by=c("cause_id", "med_id"), all.y = TRUE
        )
        missing_med_causes <- med_rr_metadata[is.na(model_version_id), c("med_id", "cause_id")]
        if (nrow(missing_med_causes) > 0) {
            setnames(missing_med_causes, "med_id", "mediator_rei_id")
            stop("Mediator RRs are missing causes that are required by the mediation matrix. ",
                 mm_error_message, "\nSee missing cause(s):\n",
                 paste(capture.output(print(missing_med_causes)), collapse = "\n"))
        }
        med_rr_metadata[, source := "delta"]
        mes <- rbind(mes, med_mes)
        rr_metadata <- rbind(rr_metadata, med_rr_metadata, fill=TRUE)
    }

    # check to see if relative risks vary by year. if they do, make sure all needed
    # years are present. otherwise, add a single year that's present to rr_metadata
    # that we'll use as the reference year instead of pulling duplicate data.
    for (rr_model_version_id in unique(rr_metadata$model_version_id)) {
        rr_summ <- fread(paste0("FILEPATH", rr_model_version_id, "/summaries/summaries.csv"))
        if(!"exposure" %in% names(rr_summ)) rr_summ[, exposure := as.numeric(NA)]
        rr_summ[, year_mean := mean(mean),
                by = c("age_group_id", "sex_id", "location_id", "cause_id",
                       "mortality", "morbidity", "exposure", "parameter"), ]
        rr_by_year_id <- !all(rr_summ$mean == rr_summ$year_mean)
        rr_year_id <- rr_summ$year_id %>% unique
        rm(rr_summ)
        rr_metadata[model_version_id==rr_model_version_id, `:=` (
            by_year_id=rr_by_year_id, year_id=ifelse(rr_by_year_id, as.integer(NA), max(rr_year_id))
        )]
        if (rr_by_year_id & !all(year_id %in% rr_year_id)) {
            rid <- unique(rr_sum$rei_id)
            rei_id_msg <- ifelse(rei_id == rid, "", paste0(" for mediator rei_id ", rid))
            err_msg <- paste0("Relative risks vary by year and not all provided year_id(s) ",
                              "are present in current best RRs. Missing years", rei_id_msg, ": ",
                              paste(setdiff(year_id, rr_year_id), collapse = ", "))
            logerror(err_msg);stop(err_msg)
        }
    }
    rr_metadata[, model_version_id := NULL]

    if (!(1 <= n_draws & n_draws <= 1000)) {
        err_msg <- "n_draws must be between 1-1000."
        logerror(err_msg);stop(err_msg)
    }
    if (rei == "air_pmhap" & save_results==TRUE) {
        err_msg <- paste0("save_results must be FALSE if running ", rei, " PAFs.")
        logerror(err_msg);stop(err_msg)
    }
    if (rei %in% c("metab_bmd")) {
        if (codcorrect_version == 0) {
            err_msg <- paste0("CoDCorrect version must be supplied if running ", rei, " PAFs.")
            logerror(err_msg);stop(err_msg)
        }
        codcorrect_df <- get_draws(
            gbd_id_type = "cause_id", gbd_id = 294, location_id = 1, measure_id = 1,
            gbd_round_id = gbd_round_id, decomp_step = decomp_step, source = "codcorrect",
            version_id = codcorrect_version)
        codcorrect_draws <- sum(names(codcorrect_df) %like% "draw_")
        if (!all(year_id %in% unique(codcorrect_df$year_id))) {
            err_msg <- paste0("Not all provided year_id(s) are present in CoDCorrect v", codcorrect_version,
                              ". Missing years: ", paste(setdiff(year_id, unique(codcorrect_df$year_id)), collapse = ", "))
            logerror(err_msg);stop(err_msg)
        }
        if (n_draws > sum(names(codcorrect_df) %like% "draw_")) {
            err_msg <- paste0("PAFs must be run at less than or equal to ", sum(names(codcorrect_df) %like% "draw_"),
                              " draws when using CoDCorrect v", codcorrect_version, ".")
            logerror(err_msg);stop(err_msg)
        }
    }
    loginfo("rei_id %d, year_id [%s], n_draws %d, gbd_round_id %d, decomp_step %s, save_results %s, resume %s, cluster_proj %s",
            rei_id, paste(year_id, collapse = ", "), n_draws, gbd_round_id, decomp_step,
            save_results, resume, cluster_proj)

    #--WIPE DIRECTORY IF NOT IN RESUME MODE --------------------------------------
    if (!resume) {
        logdebug("Wiping intermediate directory as not resuming a previous run.")
        for (dir in c(out_dir, out_dir_unmed)) {
            logdebug("deleting files from %s now.", dir)
            unlink(list.files(dir, pattern = "^save_results_", full.names = T))
            unlink(list.files(dir, pattern = ".csv$", full.names = T))
        }
        if (rei == "nutrition_lbw_preterm") {
            logdebug("deleting files from %s/nutrition_lbw now.", base_dir)
            unlink(list.files(paste0(base_dir, "/nutrition_lbw"), pattern = "^save_results_", full.names = T))
            unlink(list.files(paste0(base_dir, "/nutrition_lbw"), pattern = ".csv$", full.names = T))
            logdebug("deleting files from %s/nutrition_preterm now.", base_dir)
            unlink(list.files(paste0(base_dir, "/nutrition_preterm"), pattern = "^save_results_", full.names = T))
            unlink(list.files(paste0(base_dir, "/nutrition_preterm"), pattern = ".csv$", full.names = T))
        }
        write.csv(mes, paste0(out_dir, "/mes.csv"), row.names=F, na="")
        if("rr" %in% mes$draw_type) write.csv(rr_metadata, paste0(out_dir, "/rr_metadata.csv"), row.names=F, na="")
        write.csv(mes[!draw_type %like% "^paf" & !is.na(model_version_id),
                      .(input_model_version_id=model_version_id, draw_type)],
                  paste0(out_dir, "/paf_model_version.csv"), row.names=F, na="")
        write.csv(data.table(
            rei_id=rei_id, codcorrect_version_id=ifelse(codcorrect_version==0, NA, codcorrect_version),
            distribution=rei_meta$exposure_type, tmrel_lower=rei_meta$tmrel_lower,
            tmrel_upper=rei_meta$tmrel_upper), paste0(out_dir, "/paf_model_metadata.csv"),
            row.names=F, na="")
    }

    #--CALCULATE THE PAFS---------------------------------------------------------
    user <- Sys.info()[["user"]]
    log_dir <- paste0("FILEPATH", user)
    dir.create(file.path(log_dir, "output"), showWarnings = FALSE)
    dir.create(file.path(log_dir, "errors"), showWarnings = FALSE)

    # calculate max/min exposure for PAF (and later SEVs) if continuous
    if (cont & resume == F) {
        loginfo("Launching exposure max/min job for %s", rei)
        dir.create(paste0(out_dir, "/exposure"), showWarnings = FALSE)
        age_specific <- !is.na(rei_meta$age_specific_exp)
        qsub(job_name = paste0("exp_maxmin_", rei), script = "exp_max_min.R",
             m_mem_free = "2G", fthread = 1, logs = log_dir,
             arguments = paste(rei_id, gbd_round_id, decomp_step, out_dir,
                               age_specific, sep = " "),
             cluster_project = cluster_proj)
        job_hold(paste0("exp_maxmin_", rei))
        if (!file.exists(paste0(out_dir, "/exposure/exp_max_min.csv"))) {
            err_msg <- "Job failed, unable to continue to PAF calculation."
            logerror(err_msg);stop(err_msg)
        }
    }

    # PAF array job by location and sex
    params <- expand.grid(location_id = demo$location_id, sex_id = sex_ids) %>% data.table
    # if running in resume, we only want to launch for files that don't exist
    if (resume) {
        fini <- NULL
        loginfo("Looking for PAF jobs from previous run that may need to be launched...")
        params[, file := paste0(out_dir, "/", location_id, "_", sex_id, ".csv")]
        fini <- list.files(out_dir, full.names=T)
        params <- params[!file %in% fini,]
        params[, file := NULL]
        if (nrow(params) != 0) {
            loginfo("Previous run incomplete, %d files missing! Launching PAF calc jobs for %s",
                    nrow(params), rei)
        } else {
            loginfo("Previous run complete, %d files missing!", nrow(params))
            # check if save results already ran, if so, exit and don't run again
            fini <- file.info(fini[!fini %like% "launch_paf"])
            last_file <- rownames(fini)[which.max(fini$mtime)]
            if (last_file %like% "save_results") {
                last_file <- system(paste0("cat ", last_file), intern = TRUE)
                if (any(last_file %like% "finished, model")) {
                    loginfo(
                        "save_results_risk already run as well, previous model version ID %s. PAF calc complete!",
                        rev(strsplit(last_file[last_file %like% "finished, model"], " ")[[1]])[1])
                    save_results <- FALSE
                }
            }
        }
    } else {
        loginfo("Launching PAF calc jobs for %s", rei)
    }

    qsub_args <- paste(rei_id, paste0("'c\\(", paste(year_id, collapse = ","), "\\)'"),
                       n_draws, gbd_round_id, decomp_step, out_dir, out_dir_unmed, sep = " ")

    # some risks have their own script
    if (rei %in% c("abuse_ipv_paf", "air_pmhap", "drugs_illicit_suicide",
                   "envir_lead_blood", "nutrition_lbw_preterm", "unsafe_sex")) {
        script <- paste0("custom/", rei, ".R")
    } else {
        script <- "paf_calc.R"
        qsub_args <- paste(c(qsub_args, codcorrect_version, update_bmi_fpg_exp_sd), collapse = " ")
    }

    if (is.null(m_mem_free)) {
        if (cont) {
            # Continuous risks need more memory than categorical. If they have exposure-dependent
            # RRs they will need even more. The memory was estimated from qpid logs of the BMI
            # PAF for estimation years, 1000 draws, and includes a safety margin.
            m_mem_free <- ifelse (3 %in% rr_metadata$relative_risk_type_id, "20G", "10G")
        } else {
            # request more memory for annual categorical PAFs. Some joint PAFs require more
            # based on qpid logs.
            higher_mem_risks <- c("air_pmhap", "nutrition_lbw_preterm")
            if (length(year_id) > length(demo$year_id)) {
                m_mem_free <- ifelse (rei %in% higher_mem_risks, "15G", "10G")
            } else {
                m_mem_free <- ifelse (rei %in% higher_mem_risks, "6G", "4G")
            }
        }
    }
    fthread <- ifelse(cont, 6, 1)

    # Allow extra runtime for some risks with a large number of outcomes as well as
    # any risk with two-stage outcomes
    if (rei %in% c("metab_bmi_adult") |
       (rei %in% c("metab_fpg") & (length(year_id) > length(demo$year_id))) |
       "delta" %in% rr_metadata$source) {
        runtime <- "48:00:00"
    } else {
        runtime <- "24:00:00"
    }

    retry_qsub(job_name = paste0("paf_calc_", rei), script = script, params = params,
               out_dir = out_dir, m_mem_free = m_mem_free, fthread = fthread,
               logs = log_dir, arguments = qsub_args, cluster_project = cluster_proj,
               runtime = runtime)

    #--SAVE RESULTS---------------------------------------------------------------
    if (save_results) {
        loginfo("Launching save_results_risk job for rei %s", rei)
        mes <- fread(paste0(out_dir, "/mes.csv"))[rei_id == rei_meta$rei_id | draw_type == "rr", ]
        description <- paste0("exposure ", paste(sort(mes[draw_type == "exposure"]$model_version_id), collapse = ","))
        if (rei_id == 339) description <- paste0("exposure ", min(mes[draw_type == "exposure"]$model_version_id), " to ", max(mes[draw_type == "exposure"]$model_version_id))
        if (nrow(mes[draw_type == "exposure_sd"]) != 0 &
            !(rei %in% c("metab_bmi_adult", "metab_fpg") & update_bmi_fpg_exp_sd))
            description <- paste0(description, " - exposure sd ", paste(sort(mes[draw_type == "exposure_sd"]$model_version_id), collapse = ","))
        if (nrow(mes[draw_type == "rr"]) != 0)
            description <- paste0(description, " - relative risk ", paste(sort(mes[draw_type == "rr"]$model_version_id), collapse = ","))
        if (nrow(mes[draw_type == "tmrel"]) != 0) {
            description <- paste0(description, " - tmrel ", paste(sort(mes[draw_type == "tmrel"]$model_version_id), collapse = ","))
        } else if (cont) {
            description <- paste0(description, " - tmrel ", rei_meta$tmrel_lower, " to ", rei_meta$tmrel_upper)
        }
        if (cont) description <- paste0(description, " - ", rei_meta$exposure_type, " distribution")
        if (rei %in% c("metab_bmd")) description <- paste0(description, " - codcorrect ", codcorrect_version)
        meas_ids <- c(3, 4)
        if(is.na(rei_meta$yll)) meas_ids <- 3
        if(is.na(rei_meta$yld)) meas_ids <- 4
        save_results_risk(cluster_proj, out_dir, rei, unique(mes[draw_type == "paf"]$modelable_entity_id),
                          year_id, meas_ids, sex_ids, "paf", description, out_dir,
                          "{location_id}_{sex_id}.csv", gbd_round_id, decomp_step, n_draws)
        # also save unmediated PAFs if we have them
        if (nrow(mediation) > 0) {
            description <- paste0(description, " - unmediated")
            write.csv(fread(paste0(out_dir, "/paf_model_metadata.csv")),
                      paste0(out_dir_unmed, "/paf_model_metadata.csv"), row.names=F, na="")
            write.csv(fread(paste0(out_dir, "/paf_model_version.csv")),
                      paste0(out_dir_unmed, "/paf_model_version.csv"), row.names=F, na="")
            save_results_risk(cluster_proj, out_dir, rei, unique(mes[draw_type == "paf_unmediated"]$modelable_entity_id),
                              year_id, meas_ids, sex_ids, "paf", description, out_dir_unmed,
                              "{location_id}_{sex_id}.csv", gbd_round_id, decomp_step, n_draws)
        }
        # for lbw/sga joint paf, also save the pafs for individual lbw and sga
        if (rei_id == 339) {
            for (uni in c(334, 335)) {
                mes <- get_rei_mes(uni, gbd_round_id, decomp_step)
                rei <- get_rei_meta(uni, gbd_round_id)$rei
                out_dir <- paste0(base_dir, "/", rei)
                write.csv(fread(paste0(base_dir, "/nutrition_lbw_preterm/paf_model_metadata.csv")),
                          paste0(out_dir, "/paf_model_metadata.csv"), row.names=F, na="")
                write.csv(fread(paste0(base_dir, "/nutrition_lbw_preterm/paf_model_version.csv"))[, rei_id := uni],
                          paste0(out_dir, "/paf_model_version.csv"), row.names=F, na="")
                save_results_risk(cluster_proj, out_dir, rei, unique(mes[draw_type == "paf"]$modelable_entity_id),
                                  year_id, meas_ids, sex_ids, "paf", description, out_dir,
                                  "{location_id}_{sex_id}.csv", gbd_round_id, decomp_step, n_draws)
            }
        }
        # for bmi and fpg, run save results on the exposure sd generate in PAF calc
        if (rei %in% c("metab_bmi_adult", "metab_fpg") & update_bmi_fpg_exp_sd) {
            description <- paste0("exposure ", paste(unique(mes[draw_type == "exposure"]$model_version_id), collapse = ","),
                                  " - generated in PAF calc with SD optimization")
            epi_meta <- query(paste0(
                "SELECT bundle_id, crosswalk_version_id
                         FROM epi.model_version
                         WHERE model_version_id = ", mes[draw_type == "exposure"]$model_version_id), "epi")
            save_results_epi(cluster_proj, log_dir, rei, unique(mes[draw_type == "exposure_sd"]$modelable_entity_id),
                             year_id, 19, description, paste0(out_dir, "/exposure_sd"),
                             epi_meta$bundle_id, epi_meta$crosswalk_version_id,
                             gbd_round_id, decomp_step, n_draws, "{location_id}_{sex_id}.csv")
        }
    }

    # shut down logging
    logReset()

}
