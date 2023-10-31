#' Compile draws of pafs for burdenator, aggregate up hierarchy and mediate
#'
#' Required args:
#' @param decomp_step (str) step of decomp to run pafs for. step1-5, or iterative.
#' @param codcorrect_version (int) CoDCorrect version for pulling YLL draws. needed because CoDCorrect was run in step2 for 2020 but PAFs are run in iterative.
#' Optional args:
#' @param version_id (int) version of pafs; if not specified, will use max + 1.
#' @param year_id (list[int]) Which years do you want to calculate? Default is standard epi years for given gbd_round_id.
#' @param n_draws (int) How many draws should be used? Cannot be set higher than actual number of draws in input data, only downsampling, not upsampling is allowed. Default is 1000.
#' @param gbd_round_id (int) Default is 7, GBD 2020
#' @param resume (bool) Do you want to resume where your last jobs finished? Default is FALSE.
#' @param cluster_proj (str) what project to run jobs under on the cluster. Default is proj_centralcomp.
#' @param do_aggregation (bool) Aggregate risks up the REI aggregation hierarchy and to save PAFs at all levels in addition to just most-detailed. Default is TRUE
#' @param add_uhc_reis (bool) Add on UHC risk aggregates in addition to those in the REI aggregation hierarchy. Cannot be TRUE if do_aggregation is FALSE. Default is FALSE.
#'
#' @return nothing. Will save draws in directory
#'
#' @examples
launch_compile <- function(decomp_step, codcorrect_version, version_id = NULL,
                           year_id = NULL, n_draws = 1000, gbd_round_id = 7,
                           resume = FALSE, cluster_proj = "proj_centralcomp",
                           do_aggregation = TRUE, add_uhc_reis = FALSE) {
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
        version_id <- system(paste0("cd ", out_dir, ";find . -maxdepth 1 -mindepth 1 -type d | grep -v amenable"), intern=T)
        version_id <- gsub("./", "", version_id) %>% as.numeric %>% max
        version_id <- version_id + 1
    }
    out_dir <- paste0(out_dir, "/", version_id)
    dir.create(out_dir, showWarnings = FALSE)

    # set up logging
    main_log_file <- paste0(out_dir, "/launch_compile_", format(Sys.time(), "%Y%m%d_%I%M"), ".log")
    logReset()
    basicConfig(level='DEBUG')
    addHandler(writeToFile, file=main_log_file)

    message(sprintf("Logging here %s", main_log_file))
    loginfo("PAF compile v%d", version_id)

    # validate args
    loginfo("Validating args")
    demo <- get_demographics(gbd_team = "epi_ar", gbd_round_id = gbd_round_id)
    if (is.null(year_id)) {
        year_id <- get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)$year_id
    }
    if (!all(year_id %in% demo$year_id)) {
        err_msg <- paste0("Not all provided year_id(s) are valid GBD year_ids. Invalid years: ",
                          paste(setdiff(year_id, demo$year_id), collapse = ", "))
        logerror(err_msg);stop(err_msg)
    }
    if (!(0 <= n_draws & n_draws <= 1000)) {
        err_msg <- "n_draws must be between 0-1000."
        logerror(err_msg);stop(err_msg)
    }
    if (!do_aggregation & add_uhc_reis) {
        err_msg <- paste(
            "To include UHC REI aggregates with add_uhc_reis, do_aggregation",
            "must also be TRUE."
        )
        logerror(err_msg);stop(err_msg)
    }

    loginfo(paste("year_id=[%s], n_draws=%d, gbd_round_id=%d, decomp_step=%s, resume=%s,",
                  "cluster_proj=%s, codcorrect_version=%s, do_aggregation=%s, add_uhc_reis=%s"),
            paste(year_id, collapse = ", "), n_draws, gbd_round_id, decomp_step,
            resume, cluster_proj, codcorrect_version, do_aggregation, add_uhc_reis)

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
        get_n_draws_and_years <- function(model_version_id) {
            base_dir <- list.files(
                paste0("FILEPATH", model_version_id, "/draws"),
                full.names = TRUE)[1] %>% paste0(. , "/metadata")
            n_draws <- as.numeric(gsub("_draws", "", list.files(base_dir)))
            year_id <- paste(sort(tstrsplit(tstrsplit(
                list.files(base_dir, recursive = TRUE), "_")[[3]], "\\.")[[1]]), collapse = ", ")
            return(list(n_draws, year_id))
        }

        decomp_step_id <- get_decomp_step_id(decomp_step, gbd_round_id)
        me_metadata_type_id <- get_me_metadata_type_id(gbd_round_id)
        best_pafs <- query(paste0(
            "SELECT rei_id, rei, rei_name, modelable_entity_id, modelable_entity_name,
            model_version_id, description, best_start
            FROM epi.model_version
            JOIN epi.modelable_entity USING (modelable_entity_id)
            JOIN epi.modelable_entity_rei USING (modelable_entity_id)
            JOIN shared.rei USING (rei_id)
            JOIN (SELECT modelable_entity_id FROM epi.modelable_entity_metadata
                WHERE modelable_entity_metadata_type_id = ", me_metadata_type_id, "
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
        best_pafs[, c("n_draws", "year_id") := get_n_draws_and_years(model_version_id),
                  by = 1:nrow(best_pafs)]
        write.csv(rbind(best_pafs, meningitis, fill = TRUE),
                  paste0("FILEPATH/PAF_inputs_v", version_id, ".csv"), row.names=F)
        write.csv(rbind(best_pafs, meningitis, fill = TRUE),
                  paste0(out_dir, "/PAF_inputs_v", version_id, ".csv"), row.names=F)

        setnames(best_pafs, c("year_id", "n_draws"), c("paf_year_id", "paf_n_draws"))
        if (nrow(best_pafs[paf_n_draws < n_draws,]) > 0) {
            err_msg <- paste0(
                "Not all risk PAFs have at least ", n_draws, " draws, see rei IDs: ",
                paste(best_pafs[paf_n_draws < n_draws,]$rei_id, collapse = ", "), "."
            )
            logerror(err_msg);stop(err_msg)
        }
        best_pafs[, missing_years := paste0(setdiff(year_id, as.numeric(strsplit(
          paf_year_id, ", ")[[1]])), collapse = ","), by = 1:nrow(best_pafs)]
        if (nrow(best_pafs[missing_years != "",]) > 0) {
            err_msg <- paste0(
                "Not all risk PAFs have required years present, see rei IDs: ",
                paste(best_pafs[missing_years != "",]$rei_id, collapse = ", "), "."
            )
            logerror(err_msg);stop(err_msg)
        }

        # Write report of relative risk types
        paf_mvids <- paste(best_pafs$model_version_id, collapse=",")
        rr_types <- query(paste0(
            "SELECT rei_id, rei_name, cause_id, cause_name, relative_risk_type_name,
                input_model_version_id AS model_version_id, model_version_status,
                best_start, best_end
            FROM epi.paf_model_version pmv
            JOIN epi.model_version mv ON pmv.input_model_version_id = mv.model_version_id
            JOIN epi.model_version_status USING (model_version_status_id)
            JOIN epi.relative_risk_metadata USING (model_version_id)
            JOIN epi.relative_risk_type USING (relative_risk_type_id)
            JOIN shared.rei USING (rei_id)
            JOIN shared.cause USING (cause_id)
            WHERE paf_model_version_id IN (", paf_mvids, ")
            ORDER BY rei_id, cause_id"), "epi")
        write.csv(rr_types, paste0("FILEPATH/RR_types_v", version_id, ".csv"), row.names=F)
        write.csv(rr_types, paste0(out_dir, "/RR_types_v", version_id, ".csv"), row.names=F)
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
                               n_draws, gbd_round_id, decomp_step, out_dir,
                               codcorrect_version, do_aggregation,
                               add_uhc_reis, sep = " "),
             cluster_project = cluster_proj)
        job_hold(paste0("paf_compile_v", version_id))
        fini <- NULL
        loginfo("Checking to make sure all output files exist...")
        fini <- list.files(out_dir, full.names = TRUE)
        # make sure that not every single job failed
        if (is.null(fini)) {
            unlink(paste0(out_dir, "/params.csv"))
            err_msg <- "All jobs failed. Check out your logs!"
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
