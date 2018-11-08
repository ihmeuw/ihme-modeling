# compile draws of pafs for burdenator, aggregate up hierarchy and mediate

# optional args:
# verion_id - (int) version of pafs, if not specified, with use max + 1.
# year_id - (list[int]) Which years do you want to calculate? Default is standard epi years for GBD 2017.
# n_draws - (int) How many draws should be used? Cannot be set higher than actual
#                 number of draws in input data, only downsampling, not upsampling is allowed. Default is 1000.
# gbd_round_id - (int) Default is 5, GBD 2017
# resume - (bool)
# cluster_proj - (str) what project to run save_results job under on the cluster. Default is proj_rfprep.

# returns: nothing. Will save draws in directory

launch_compile <- function(version_id = NULL, year_id = c(1990, 1995, 2000, 2005, 2010, 2017),
                           n_draws = 1000, gbd_round_id = 5, resume = FALSE,
                           cluster_proj = "proj_rfprep") {

    # load libraries and functions
    library(magrittr)
    library(data.table)
    source("FILEPATH/get_demographics.R")
    setwd("FILEPATH/paf")
    source("./utils/cluster.R")
    source("./utils/db.R")

    #--CHECK RUN ID AND ENVIRONMENT-----------------------------------------------

    if (!(.Platform$OS.type == "unix"))
        stop("Can only launch parallel jobs for this script on the cluster")

    out_dir <- paste0("FILEPATH/")
    if(is.null(version_id)) {
        version_id <- list.files(out_dir)
        version_id <- setdiff(version_id, c("temp")) %>% as.numeric %>% max
        version_id <- version_id + 1
    }
    out_dir <- paste0(out_dir, version_id, "/")
    dir.create(out_dir, showWarnings = FALSE)
    message(format(Sys.time(), "%D %H:%M:%S"), " PAF compile v", version_id)

    # validate args
    message("Validating args")
    demo <- get_demographics(gbd_team = "epi_ar", gbd_round_id = gbd_round_id)
    if (!all(year_id %in% demo$year_id))
        stop("Not all provided year_id(s) are valid GBD year_ids, (",
             paste(setdiff(year_id, demo$year_id), collapse = ", "), ")")
    if (!(1 <= n_draws & n_draws <= 1000))
        stop("n_draws must be between 1-1000.")
    message("year_id - ", paste(year_id, collapse = ", "),
            "\nn_draws - ", n_draws,
            "\ngbd_round_id - ", gbd_round_id,
            "\nresume - ", resume,
            "\ncluster_proj - ", cluster_proj)

    user <- Sys.getenv("USER")
    log_dir <- paste0("FILEPATH/", user)

    #--WIPE DIRECTORY IF NOT IN RESUME MODE --------------------------------------

    if (!resume) {
        message("Wiping intermediate directory as not running in resume mode.",
                " You have 10 seconds to change your mind...")
        Sys.sleep(10)
        message("deleting files from ", out_dir, " now.")
        unlink(list.files(out_dir, pattern = ".csv.gz$", full.names = T))
    }

    #--TRACK VERSIONS USED AND REFORMAT PAF FILES FOR LESS I/O -----------------

    if (!resume) {
        best_pafs <- query(paste0(
            "SELECT rei_id, rei_name, modelable_entity_id, modelable_entity_name,
            model_version_id, description, best_start
            FROM epi.model_version
            JOIN epi.modelable_entity USING (modelable_entity_id)
            JOIN epi.modelable_entity_rei USING (modelable_entity_id)
            JOIN shared.rei USING (rei_id)
            JOIN (SELECT modelable_entity_id FROM epi.modelable_entity_metadata
                WHERE modelable_entity_metadata_type_id = 26
                    and modelable_entity_metadata_value = 1
                    and last_updated_action != 'DELETE') gbd using(modelable_entity_id)
            JOIN (SELECT modelable_entity_id FROM epi.modelable_entity_metadata
                WHERE modelable_entity_metadata_type_id = 17
                    and modelable_entity_metadata_value = 'paf'
                    and last_updated_action != 'DELETE') paf using(modelable_entity_id)
            WHERE model_version_status_id = 1 and gbd_round_id = ", gbd_round_id), "epi")
        dir.create(paste0("FILEPATH/", version_id), showWarnings = FALSE)
        write.csv(best_pafs, paste0("FILEPATH/", version_id, "/PAF_model_version_inputs.csv"), row.names=F)
        already_done <- fread(paste0("FILEPATH/", version_id-1, "/PAF_model_version_inputs.csv"))$model_version_id %>% unique
        to_do <- best_pafs[!model_version_id %in% already_done,]$rei_id %>% unique
        dir.create("FILEPATH/", showWarnings = FALSE)
        for(rid in to_do) {
            message("Deleting old version of ", rid, " PAFs")
            if(dir.exists(paste0("FILEPATH/",rid)))
                unlink(list.files(paste0("FILEPATH/",rid), pattern = ".csv$", full.names = T))
        }
        params <- best_pafs[rei_id %in% to_do & modelable_entity_id != 8805, .(rei_id, model_version_id)]
        params <- params[, .(location_id = demo$location_id), by = .(rei_id, model_version_id)]
        write.csv(params, "FILEPATH/params.csv", row.names = F)
        system(paste0("qsub -P ", cluster_proj, " -pe multi_slot 2 -l mem_free=4g",
                      " -e ", log_dir, "/errors -o ", log_dir, "/output",
                      " -cwd -N resave_pafs -t 1:", nrow(params),
                      " utils/python.sh FILEPATH gbd_env",
                      " python utils/resave_pafs.py ", "FILEPATH/params.csv"))
        job_hold("resave_pafs")
        unlink("FILEPATH/params.csv")
    }

    #--COMPILE THE PAFS---------------------------------------------------------

    # array job by location
    params <- expand.grid(location_id = demo$location_id) %>% data.table
    # if running in resume, we only want to launch for files that don't exist
    if (resume) {
        fini <- NULL
        message("In resume mode, finding jobs to relaunch.")
        for (i in 1:nrow(params)) {
            if (file.exists(paste0(out_dir, params[i, location_id], "_", max(year_id), ".csv.gz"))) {
                fini <- c(fini, i)
            }
        }
        if (!is.null(fini)) params <- params[-fini, ]
    }
    write.csv(params, paste0(out_dir, "params.csv"), row.names = F)
    tries <- 3
    # submit until all files exist up to ntries
    while (tries != 0 & nrow(params) != 0) {
        qsub(job_name = paste0("paf_compile_v", version_id), script = "aggregate_and_mediate.R",
             slots = 10, logs = log_dir, array = nrow(params),
             arguments = paste(paste0(out_dir, "params.csv"), paste0("'c(", paste(year_id, collapse = ","), ")'"),
                               n_draws, gbd_round_id, out_dir, sep = " "),
             cluster_project = cluster_proj)
        job_hold(paste0("paf_compile_v", version_id))
        fini <- NULL
        message("Checking to make sure all output files exist...")
        for (i in 1:nrow(params)) {
            ## Ensure that all files are there & didn't get killed by oom etc
            if (file.exists(paste0(out_dir, params[i, location_id], "_", max(year_id), ".csv.gz"))) {
                fini <- c(fini, i)
            }
        }
        # make sure that not every single job failed
        if (is.null(fini)) {
            unlink(paste0(out_dir, "params.csv"))
            stop("All jobs failed. Check out your logs!")
        }
        tries <- tries - 1
        params <- params[-fini, ]
        message(nrow(params), " files missing, ", tries, " retries remaining.")
        write.csv(params, paste0(out_dir, "params.csv"), row.names = F)
    }
    # fail if still not all jobs done
    if (nrow(params) != 0) {
        stop("Failing jobs were relaunched ", tries-1, "x more and still did not ",
             "all complete. Check out your logs!")
    } else {
        message("Success! All jobs complete.")
    }
    unlink(paste0(out_dir, "params.csv"))


}
