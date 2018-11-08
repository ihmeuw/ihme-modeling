# Given a risk, calculate draws of PAFs.

# required args:
# rei_id - (int) Id of the risk you are running PAFs for.
# optional args:
# year_id - (list[int]) Which years do you want to calculate? Default is standard epi years for GBD 2017.
# n_draws - (int) How many draws should be used? Cannot be set higher than actual
#                 number of draws in input data, only downsampling, not upsampling is allowed. Default is 1000.
# gbd_round_id - (int) Default is 5, GBD 2017
# save_results - (bool) Do you want to run save_results on the final PAF draws? Default is TRUE.
# resume - (bool)
# cluster_proj - (str) what project to run save_results job under on the cluster. Default is proj_paf.
# slots - (int) number of slots for each job. Default is NULL, will run at 4 slots for categorical risks and 10 slots for continous risks. Only specify if you're really sure.
# log - (bool) do you want to write log file to "FILEPATH/{rei}/launch_paf.Rout or just print log messages to screen? Default is TRUE, log file.

# returns: nothing. Will save draws in directory and optionally run save_results as well if requested.

launch_paf <- function(rei_id, year_id = c(1990, 1995, 2000, 2005, 2010, 2017),
                       n_draws = 1000, gbd_round_id = 5, save_results = TRUE,
                       resume = FALSE, cluster_proj = "proj_paf", slots=NULL, log=TRUE) {

    # load libraries and functions
    library(magrittr)
    library(data.table)
    library(ini)
    library(RMySQL)
    source("FILEPATH/get_demographics.R")
    source("FILEPATH/get_model_results.R")
    setwd("FILEPATH/paf")
    source("./utils/cluster.R")
    source("./utils/data.R")
    source("./utils/db.R")
    source("save.R")

    #--CHECK RUN ID AND ENVIRONMENT-----------------------------------------------

    if (!(.Platform$OS.type == "unix"))
        stop("Can only launch parallel jobs for this script on the cluster")

    rei_meta <- get_rei_meta(rei_id)
    rei <- rei_meta$rei
    cont <- rei_meta$calc_type == 2
    if (rei %in% c("drugs_alcohol", "drugs_illicit_direct", "air_pm", "air_ozone",
                   "abuse_ipv_hiv", "occ_injury"))
        stop(rei, " is a custom PAF calculated by the modeler, not supported by this code base.")
    if ((rei %in% c("nutrition_lbw", "nutrition_preterm")))
        stop(rei, " is a calculated with nutrition_lbw_preterm. Please submit jobs for ",
             "the joint risk, rei_id = 339 instead.")
    if(rei_meta$calc_type == 0)
        stop(rei, " is an aggregate risk PAF and will be calculated centrally after most-detailed risk pafs are ready.")
    out_dir <- paste0("FILEPATH/", rei, "/")
    dir.create(out_dir, showWarnings = FALSE)
    if (log) {
        suppressWarnings(sink())
        unlink(paste0(out_dir, "/launch_paf.Rout"))
        message("Logging here ", out_dir, "launch_paf.Rout")
        con <- file(paste0(out_dir, "launch_paf.Rout"), open = "wt")
        sink(con)
        sink(con, type="message")
    }
    message(format(Sys.time(), "%D %H:%M:%S"), " PAF calc for ", rei)

    # validate args
    message("Validating args")
    demo <- get_demographics(gbd_team = "epi_ar", gbd_round_id = gbd_round_id)
    sex_ids <- demo$sex_id
    if(is.na(rei_meta$female)) sex_ids <- 1
    if(is.na(rei_meta$male)) sex_ids <- 2
    if (!all(year_id %in% demo$year_id))
        stop("Not all provided year_id(s) are valid GBD year_ids. Invalid years: ",
             paste(setdiff(year_id, demo$year_id), collapse = ", "))
    exp_years <- rbindlist(lapply(get_rei_mes(rei_id, gbd_round_id)[draw_type == "exposure", modelable_entity_id], function(x)
        get_model_results("epi", gbd_id=x, age_group_id=22, location_id=1)))$year_id %>% unique
    if (!all(year_id %in% exp_years))
        stop("Not all provided year_id(s) are present in current best ",
             "exposures. Missing years: ", paste(setdiff(year_id, exp_years), collapse = ", "))
    exp_sd_years <- rbindlist(lapply(get_rei_mes(rei_id, gbd_round_id)[draw_type == "exposure_sd", modelable_entity_id], function(x)
        get_model_results("epi", gbd_id=x, age_group_id=22, location_id=1)))$year_id %>% unique
    if (!all(year_id %in% exp_sd_years) & !is.null(exp_sd_years) & !(rei %in% c("metab_fpg_cont", "metab_bmi_adult")))
        stop("Not all provided year_id(s) are present in current best ",
             "exposure SDs. Missing years: ", paste(setdiff(year_id, exp_sd_years), collapse = ", "))
    if (!(1 <= n_draws & n_draws <= 1000))
        stop("n_draws must be between 1-1000.")
    message("rei_id - ", rei_id,
            "\nyear_id - ", paste(year_id, collapse = ", "),
            "\nn_draws - ", n_draws,
            "\ngbd_round_id - ", gbd_round_id,
            "\nsave_results - ", save_results,
            "\nresume - ", resume,
            "\ncluster_proj - ", cluster_proj)

    #--WIPE DIRECTORY IF NOT IN RESUME MODE --------------------------------------

    if (!resume) {
        message("Wiping intermediate directory as not running in resume mode.",
                " You have 10 seconds to change your mind...")
        Sys.sleep(10)
        message("deleting files from ", out_dir, " now.")
        unlink(list.files(out_dir, pattern = "^save_results_", full.names = T))
        unlink(list.files(out_dir, pattern = ".csv$", full.names = T))
        if (rei == "nutrition_lbw_preterm") {
            message("deleting files from FILEPATH/nutrition_lbw now.")
            unlink(list.files("FILEPATH/nutrition_lbw", pattern = "^save_results_", full.names = T))
            unlink(list.files("FILEPATH/nutrition_lbw", pattern = ".csv$", full.names = T))
            message("deleting files from FILEPATH/nutrition_preterm now.")
            unlink(list.files("FILEPATH/nutrition_preterm", pattern = "^save_results_", full.names = T))
            unlink(list.files("FILEPATH/nutrition_preterm", pattern = ".csv$", full.names = T))

        }
        mes <- get_rei_mes(rei_id, gbd_round_id)
        write.csv(mes, paste0(out_dir, "mes.csv"), row.names=F)
    }

    #--CALCULATE THE PAFS---------------------------------------------------------

    user <- Sys.getenv("USER")
    log_dir <- paste0("FILEPATH/", user)

    # calculate max/min exposure for PAF (and later SEVs) if continuous
    if (cont & resume == F & !(rei %in% c("metab_fpg_cont", "metab_bmi_adult"))) {
        message("Launching exposure maxmin calc for ", rei)
        dir.create(paste0(out_dir, "exposure"), showWarnings = FALSE)
        qsub(job_name = paste0("exp_maxmin_", rei), script = "exp_max_min.R",
             slots = 10, logs = log_dir,
             arguments = paste(rei_id, gbd_round_id, out_dir, sep = " "),
             cluster_project = cluster_proj)
        job_hold(paste0("exp_maxmin_", rei))
        if (!file.exists(paste0(out_dir, "exposure/exp_max_min.csv")))
            stop("Job failed, unable to continue to PAF calculation.")
    }

    # PAF array job by location and sex
    message("Launching PAF calc for ", rei)
    params <- expand.grid(location_id = demo$location_id, sex_id = sex_ids) %>% data.table
    # if running in resume, we only want to launch for files that don't exist
    if (resume) {
        fini <- NULL
        message("In resume mode, finding jobs to relaunch.")
        for (i in 1:nrow(params)) {
            if (file.exists(paste0(out_dir, params[i, location_id], "_", params[i, sex_id], ".csv"))) {
                fini <- c(fini, i)
            }
        }
        if (!is.null(fini)) params <- params[-fini, ]
    }
    # some risks have their own script
    script <- ifelse(rei %in% c("drugs_illicit_suicide", "envir_lead_blood",
                                "nutrition_lbw_preterm", "abuse_ipv_paf",
                                "unsafe_sex", "metab_fpg_cont", "metab_bmi_adult",
                                "activity"),
                     paste0("custom/", rei, ".R"), "paf_calc.R")
    if (is.null(slots)) slots <- ifelse(cont, 10, 4) # continous risks need more slots
    if (rei %in% c("metab_bmi_adult", "metab_fpg_cont", "activity")) slots <- 20
    retry_qsub(job_name = paste0("paf_calc_", rei), script = script, params = params,
               out_dir = out_dir, slots = slots, logs = log_dir,
               arguments = paste(rei_id,
                                 paste0("'c(", paste(year_id, collapse = ","), ")'"),
                                 n_draws, gbd_round_id, out_dir, sep = " "),
               cluster_project = cluster_proj)

    #--SAVE RESULTS---------------------------------------------------------------

    if (save_results) {
        message("Launching save_results job now.")
        mes <- fread(paste0(out_dir, "mes.csv"))
        description <- paste0("exposure mvid ", paste(unique(mes[draw_type == "exposure"]$model_version_id), collapse = ";"))
        if (rei_id == 339) description <- paste0("exposure mvid ", min(mes[draw_type == "exposure"]$model_version_id), " to ", max(mes[draw_type == "exposure"]$model_version_id))
        if (nrow(mes[draw_type == "exposure_sd"]) != 0 & !(rei %in% c("metab_fpg_cont", "metab_bmi_adult"))) description <- paste0(description, " - exposure sd mvid ", paste(unique(mes[draw_type == "exposure_sd"]$model_version_id), collapse = ";"))
        if (nrow(mes[draw_type == "rr"]) != 0) description <- paste0(description, " - relative risk mvid ", paste(unique(mes[draw_type == "rr"]$model_version_id), collapse = ";"))
        if (nrow(mes[draw_type == "tmrel"]) != 0) description <- paste0(description, " - tmrel mvid ", paste(unique(mes[draw_type == "tmrel"]$model_version_id), collapse = ";"))
        if (cont) description <- paste0(description, " - ", rei_meta$exp_dist, " distribution")
        meas_ids <- c(3, 4)
        if(is.na(rei_meta$yll)) meas_ids <- 3
        if(is.na(rei_meta$yld)) meas_ids <- 4
        save_results_risk(cluster_proj, out_dir, rei, ifelse(rei_id == 150, 8805, unique(mes[draw_type == "paf"]$modelable_entity_id)),
                          year_id, meas_ids, sex_ids, "paf", description, out_dir,
                          "{location_id}_{sex_id}.csv")
        # for lbw/sga joint paf, also save the pafs for individual lbw and sga
        if (rei_id == 339) {
            mes <- get_rei_mes(334, gbd_round_id)
            rei <- get_rei_meta(334)$rei
            out_dir <- paste0("FILEPATH/", rei, "/")
            save_results_risk(cluster_proj, out_dir, rei, unique(mes[draw_type == "paf"]$modelable_entity_id),
                              year_id, meas_ids, sex_ids, "paf", description, out_dir,
                              "{location_id}_{sex_id}.csv")
            mes <- get_rei_mes(335, gbd_round_id)
            rei <- get_rei_meta(335)$rei
            out_dir <- paste0("FILEPATH/", rei, "/")
            save_results_risk(cluster_proj, out_dir, rei, unique(mes[draw_type == "paf"]$modelable_entity_id),
                              year_id, meas_ids, sex_ids, "paf", description, out_dir,
                              "{location_id}_{sex_id}.csv")
        }
        # for bmi and fpg, run save results on the exposure sd generate in PAF calc
        if (rei %in% c("metab_bmi_adult", "metab_fpg_cont")) {
            description <- paste0("exposure mvid ", paste(unique(mes[draw_type == "exposure"]$model_version_id), collapse = ";"),
                                  " - generated in PAF calc with SD optimization")
            save_results_epi(cluster_proj, log_dir, rei, unique(mes[draw_type == "exposure_sd"]$modelable_entity_id),
                             year_id, 19, description, paste0(out_dir, "exp_sd"))
        }
    }

    if (log) {
        sink(type="message")
        sink()
        close(con)
    }

}
