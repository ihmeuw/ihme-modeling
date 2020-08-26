#' Compile draws of pafs for burdenator, aggregate up risk hierarchy and mediate
#'
#' @param version_id (int) version of pafs, if not specified, with use max + 1.
#' @param year_id (list[int]) Which years do you want to calculate? Default is standard epi years for GBD 2019.
#' @param n_draws (int) How many draws should be used? Cannot be set higher than actual number of draws in input data, only downsampling, not upsampling is allowed. Default is 1000.
#' @param gbd_round_id (int) Default is 6, GBD 2019
#' @param decomp_step (str) step of decomp to run pafs for. step1-5, or iterative.
#' @param resume (bool) Do you want to resume where your last jobs finished? Default is FALSE.
#' @param cluster_proj (str) what project to run save_results job under on the cluster. Default is proj_centralcomp.
#'
#' @return nothing. Will save draws in directory
#'
#' @examples
launch_compile <- function(version_id = NULL, year_id = c(1990, 2010, 2019),
                           n_draws = 1000, gbd_round_id = 6, decomp_step=NULL,
                           resume = FALSE, cluster_proj = "proj_centralcomp") {

    # load libraries and functions
    library(magrittr)
    library(data.table)
    library(logging)
    source("FILEPATH/get_demographics.R")
    setwd("FILEPATH")
    source("./utils/cluster.R")
    source("./utils/db.R")

    #--CHECK RUN ID AND ENVIRONMENT-----------------------------------------------
    out_dir <- paste0("FILEPATH")
    if(is.null(version_id)) {
        version_id <- list.files(out_dir)
        version_id <- setdiff(version_id, c("temp")) %>% as.numeric %>% max
        version_id <- version_id + 1
    }
    out_dir <- paste0(out_dir, "/", version_id)
    dir.create(out_dir, showWarnings = FALSE)

    # set up logging
    main_log_file <- paste0(out_dir, "/launch_compile_", format(Sys.time(), "%m-%d-%Y %I:%M:%S"), ".log")
    logReset()
    basicConfig(level='DEBUG')
    addHandler(writeToFile, file=main_log_file)

    message(sprintf("Logging here %s", main_log_file))
    loginfo("PAF compile v%d", version_id)

    # validate args
    loginfo("Validating args")
    demo <- get_demographics(gbd_team = "epi_ar", gbd_round_id = gbd_round_id)
    if (!all(year_id %in% demo$year_id)) {
        err_msg <- paste0("Not all provided year_id(s) are valid GBD year_ids. Invalid years: ",
                          paste(setdiff(year_id, demo$year_id), collapse = ", "))
        logerror(err_msg);stop(err_msg)
    }
    if (!(0 <= n_draws & n_draws <= 1000)) {
        err_msg <- "n_draws must be between 0-1000."
        logerror(err_msg);stop(err_msg)
    }
    loginfo("year_id=[%s], n_draws=%d, gbd_round_id=%d, decomp_step=%s, resume=%s, cluster_proj=%s",
            paste(year_id, collapse = ", "), n_draws, gbd_round_id, decomp_step,
            resume, cluster_proj)

    user <- Sys.getenv("USER")
    log_dir <- paste0("FILEPATH", user)

    #--WIPE DIRECTORY IF NOT IN RESUME MODE --------------------------------------
    if (!resume) {
        logdebug("Wiping intermediate directory as not running in resume mode. You have 10 seconds to change your mind...")
        Sys.sleep(10)
        logdebug("deleting files from %s now.", out_dir)
        unlink(list.files(out_dir, pattern = ".csv.gz$", full.names = T))
    }

    #--REFORMAT PAF FILES-------------------------------------------------------
    if (!resume) {
        decomp_step_id <- get_decomp_step_id(decomp_step, gbd_round_id)
        best_pafs <- query(paste0(
            "SELECT rei_id, rei, rei_name, modelable_entity_id, modelable_entity_name,
            model_version_id, description, best_start
            FROM epi.model_version
            JOIN epi.modelable_entity USING (modelable_entity_id)
            JOIN epi.modelable_entity_rei USING (modelable_entity_id)
            JOIN shared.rei USING (rei_id)
            JOIN (SELECT modelable_entity_id FROM epi.modelable_entity_metadata
                WHERE modelable_entity_metadata_type_id = 29
                    and modelable_entity_metadata_value = 1
                    and last_updated_action != 'DELETE') gbd using(modelable_entity_id)
            JOIN (SELECT modelable_entity_id FROM epi.modelable_entity_metadata
                WHERE modelable_entity_metadata_type_id = 17
                    and modelable_entity_metadata_value = 'paf'
                    and last_updated_action != 'DELETE') paf using(modelable_entity_id)
            WHERE
                model_version_status_id = 1 and
                gbd_round_id = ", gbd_round_id, " AND
                decomp_step_id = ", decomp_step_id), "epi")
        meningitis <- query(paste0(
            "SELECT
                rei_id, rei, rei_name, modelable_entity_id, modelable_entity_name,
                model_version_id, description, best_start
            FROM
                epi.modelable_entity me
            JOIN epi.modelable_entity_rei mer using(modelable_entity_id)
            JOIN shared.rei rhh using(rei_id)
            JOIN
                (select modelable_entity_id, model_version_id, best_start, description
                from epi.model_version
                where model_version_status_id = 1 and gbd_round_id = ",gbd_round_id,"
                and decomp_step_id = ", decomp_step_id,") mv using (modelable_entity_id)
            WHERE
                me.modelable_entity_id IN (10494, 10495, 10496, 24739, 24741, 24740)"),"epi")
        write.csv(rbind(best_pafs, meningitis),
            paste0("FILEPATH/PAF_inputs_v", version_id, ".csv"), row.names=F)
        write.csv(rbind(best_pafs, meningitis),
            paste0(out_dir, "/PAF_inputs_v", version_id, ".csv"), row.names=F)
    }

    #--COMPILE THE PAFS---------------------------------------------------------

    # array job by location
    params <- expand.grid(location_id = demo$location_id) %>% data.table
    params[, file := paste0(out_dir, "/", location_id, "_", max(year_id), ".csv.gz")]
    # if running in resume, we only want to launch for files that don't exist
    if (resume) {
        fini <- NULL
        loginfo("In resume mode, finding jobs to relaunch.")
        fini <- list.files(out_dir, full.names = TRUE)
        message(length(fini), " files found.")
        if (!is.null(fini)) params <- params[!file %in% fini,]
        message(nrow(params), " jobs to go!")
    }
    write.csv(params, paste0(out_dir, "/params.csv"), row.names = F)
    tries <- 3
    # submit until all files exist up to ntries
    while (tries != 0 & nrow(params) != 0) {
        qsub(job_name = paste0("paf_compile_v", version_id), script = "aggregate_and_mediate.R",
             m_mem_free = "200G", fthread = 1, logs = log_dir, array = nrow(params),
             arguments = paste(paste0(out_dir, "/params.csv"),
                               paste0("'c\\(", paste(year_id, collapse = ","), "\\)'"),
                               n_draws, gbd_round_id, decomp_step, out_dir, sep = " "),
             cluster_project = cluster_proj)
        job_hold(paste0("paf_compile_v", version_id))
        fini <- NULL
        loginfo("Checking to make sure all output files exist...")
        fini <- list.files(out_dir, full.names = TRUE)
        # make sure that not every single job failed
        if (is.null(fini)) {
            unlink(paste0(out_dir, "/params.csv"))
            err_msg <- "All jobs failed. Check out your logs and submit a help ticket!"
            logerror(err_msg);stop(err_msg)
        }
        tries <- tries - 1
        params <- params[!file %in% fini,]
        loginfo("%d files missing, %d retries remaining.", nrow(params), tries)
        write.csv(params, paste0(out_dir, "/params.csv"), row.names = F)
    }
    # fail if still not all jobs done
    if (nrow(params) != 0) {
        err_msg <- paste0("Failing jobs were relaunched ", tries-1, "x more and still did not ",
                    "all complete.")
        logerror(err_msg);stop(err_msg)
    } else {
        loginfo("Success! All jobs complete.")
    }
    unlink(paste0(out_dir, "/params.csv"))

}
