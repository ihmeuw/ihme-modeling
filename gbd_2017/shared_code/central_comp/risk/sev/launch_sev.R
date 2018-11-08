# sev calculator

# optional args:
# verion_id - (int) version of sevs, if not specified, will use max + 1.
# paf_verion_id - (int) version of pafs, if not specified, will use max
# year_id - (list[int]) Which years to calculate? Default is standard epi years for GBD 2017.
# location_set_id - (list[int]) what location sets to aggregate? Default is 35.
# pct_change - (bool) do you want to run percent change as well?
# n_draws - (int) How many draws should be used? Cannot be set higher than actual
#                 number of draws in input data, only downsampling, not upsampling is allowed. Default is 1000.
# gbd_round_id - (int) Default is 5, GBD 2017
# resume - (bool) will resume starting in sev calc phase, not mid loc agg or summarization
# cluster_proj - (str) what project to run save_results job under on the cluster. Default is proj_rfprep.

# returns: nothing. Will save draws in directory, make summaries, and upload to gbd db

launch_sev <- function(version_id = NULL, paf_version_id = NULL,
                       year_id = c(1990, 1995, 2000, 2005, 2010, 2017),
                       location_set_id = 35, pct_change = TRUE, n_draws = 1000,
                       gbd_round_id = 5, resume = FALSE, cluster_proj = "proj_rfprep") {

    # load libraries and functions
    library(magrittr)
    source("FILEPATH/get_demographics.R")
    source("FILEPATH/get_rei_metadata.R")
    source("FILEPATH/get_population.R")
    setwd("FILEPATH/sev/")
    source("./utils/cluster.R")

    #--CHECK RUN ID AND ENVIRONMENT-----------------------------------------------

    if (!(.Platform$OS.type == "unix"))
        stop("Can only launch parallel jobs for this script on the cluster")

    out_dir <- paste0("FILEPATH")
    if(is.null(version_id)) {
        version_id <- list.files(out_dir) %>% as.numeric %>% max(., na.rm = T)
        version_id <- version_id + 1
    }
    out_dir <- paste0(out_dir, "/", version_id)
    dir.create(out_dir, showWarnings = FALSE)
    dir.create(paste0(out_dir,"/rrmax"), showWarnings = FALSE)
    dir.create(paste0(out_dir,"/draws"), showWarnings = FALSE)
    dir.create(paste0(out_dir,"/summaries"), showWarnings = FALSE)

    message(format(Sys.time(), "%D %H:%M:%S"), " SEV v", version_id)

    # validate args
    message("Validating args")
    demo <- get_demographics(gbd_team = "epi_ar", gbd_round_id = gbd_round_id)
    if (!all(year_id %in% demo$year_id))
        stop("Not all provided year_id(s) are valid GBD year_ids, (",
             paste(setdiff(year_id, demo$year_id), collapse = ", "), ")")
    if (!(1 <= n_draws & n_draws <= 1000))
        stop("n_draws must be between 1-1000.")
    paf_versions <- list.files("FILEPATH") %>% as.numeric
    if(is.null(paf_version_id)) paf_version_id <- max(paf_versions, na.rm=T)
    if (!(paf_version_id %in% paf_versions))
        stop(paf_version_id, " is not a valid PAF version")
    message("paf_version_id - ", paf_version_id,
            "\nyear_id - ", paste(year_id, collapse = ", "),
            "\nn_draws - ", n_draws,
            "\ngbd_round_id - ", gbd_round_id,
            "\nresume - ", resume,
            "\ncluster_proj - ", cluster_proj)

    # find reis to calc SEVs for by finding intersection of risks that we have
    # PAFs for and most detailed risks in the computation hierarchy excluding
    # unsafe sex, hearing, IPV (direct PAF), blood lead, drug use (direct PAF),
    # and IPV (HIV) as we don't have RRs due to PAF modeling strategy
    paf_reis <- fread(paste0("zcat < FILEPATH/", paf_version_id, "/existing_reis.csv.gz"))
    reis <- get_rei_metadata(rei_set_id = 2, gbd_round_id = gbd_round_id)
    reis <- reis[(most_detailed == 1) & !(rei_id %in% c(170, 131, 168, 242, 138, 201)), ]
    rei_ids <- intersect(paf_reis$rei_id, reis$rei_id)

    #--WIPE DIRECTORY IF NOT IN RESUME MODE --------------------------------------

    if (!resume) {
        message("Wiping draw directory as not running in resume mode.",
                " You have 30 seconds to change your mind...")
        Sys.sleep(30)
        message("deleting ", out_dir, " now.")
        unlink(paste0(out_dir,"/draws/"), recursive = T)
        dir.create(paste0(out_dir,"/draws"), showWarnings = FALSE)
    }

    #--CALCULATE SEVS ----------------------------------------------------------

    user <- Sys.getenv("USER")
    log_dir <- paste0("FILEPATH/", user)

    # array job by location and risk
    params <- expand.grid(location_id = demo$location_id, risk_id = rei_ids) %>% data.table
    # if running in resume, we only want to launch for files that don't exist
    if (resume) {
        fini <- NULL
        message("In resume mode, finding jobs to relaunch.")
        for (i in 1:nrow(params)) {
            if (file.exists(paste0(out_dir, "/draws/", params[i, risk_id], "/", params[i, location_id], ".csv"))) {
                fini <- c(fini, i)
            }
        }
        if (!is.null(fini)) params <- params[-fini, ]
    }
    retry_qsub(job_name = paste0("sev_calc_", version_id),
               script = "sev_calc.R", params = params,
               out_dir = paste0(out_dir, "/draws"), slots = 4, logs = log_dir,
               arguments = paste(paste0("'c(", paste(year_id, collapse = ","), ")'"),
                                 n_draws, gbd_round_id, out_dir, paf_version_id, sep = " "),
               cluster_project = cluster_proj)

    #--AGGREGATE AND SUMMARIZE--------------------------------------------------

    for(lsid in location_set_id) {
        pop <- get_population(year_id=year_id, location_id="all", sex_id="all",
                              age_group_id="all", gbd_round_id=gbd_round_id,
                              location_set_id = lsid)[, run_id := NULL]
        write.csv(pop, paste0(out_dir, "/draws/population_", lsid, ".csv"), row.names=F)
    }

    # add back in most-detailed reporting risks that aren't most-detailed computation risks
    rei_ids <- c(rei_ids, 103, 135, 91, 105, 134, 108)

    # for each risk, aggregate locations
    tries <- 3
    while (tries != 0 & length(rei_ids) != 0) {
        for (rei_id in rei_ids) {
            system(paste0("qsub -P ", cluster_proj, " -l mem_free=20g -pe multi_slot 10",
                          " -e ", log_dir, "/errors -o ", log_dir, "/output",
                          " -cwd -N sev_loc_agg_", rei_id,
                          " utils/python.sh FILEPATH gbd_env",
                          " python aggregate.py",
                          " --sev_version_id ", version_id,
                          " --rei_id ", rei_id,
                          " --location_set_id ", paste(location_set_id, collapse=" "),
                          " --n_draws ", n_draws,
                          " --gbd_round_id ", gbd_round_id))
        }
        job_hold("sev_loc_agg")
        fini <- NULL
        message("Checking to make sure all output files exist...")
        for (rei_id in rei_ids) {
            if (file.exists(paste0(out_dir, "/draws/", rei_id, "/1.csv"))) {
                fini <- c(fini, rei_id)
            }
        }
        if (is.null(fini)) {
            stop("All loc agg jobs failed. Check out your logs!")
        }
        tries <- tries - 1
        rei_ids <- setdiff(rei_ids, fini)
        message(length(rei_ids), " jobs failed, ", tries, " retries remaining.")
    }
    if (length(rei_ids) != 0) {
        stop("Failing loc agg jobs were relaunched ", tries-1, "x more and still did not ",
             "all complete. Check out your logs!")
    } else {
        message("Success! All loc agg jobs complete.")
    }

    # summarize
    location_ids <- list.files(paste0(out_dir, "/draws/83/"))
    location_ids <- gsub(".csv", "", location_ids) %>% as.numeric
    tries <- 3
    while (tries != 0 & length(location_ids) != 0) {
        for (location_id in location_ids) {
            system(paste0("qsub -P ", cluster_proj, " -l mem_free=24g -pe multi_slot 12",
                          " -e ", log_dir, "/errors -o ", log_dir, "/output",
                          " -cwd -N sev_summary_", location_id,
                          " utils/python.sh FILEPATH gbd_env",
                          " python summarize.py",
                          " --sev_version_id ", version_id,
                          " --location_id ", location_id,
                          " --year_id ", paste(year_id, collapse=" "),
                          ifelse(pct_change, " --change", ""),
                          " --gbd_round_id ", gbd_round_id))
        }
        job_hold("sev_summary")
        fini <- NULL
        message("Checking to make sure all output files exist...")
        for (location_id in location_ids) {
            if (file.exists(paste0(out_dir, "/summaries/single_year_", location_id, ".csv"))) {
                fini <- c(fini, location_id)
            }
        }
        if (is.null(fini)) {
            stop("All summary jobs failed. Check out your logs!")
        }
        tries <- tries - 1
        location_ids <- setdiff(location_ids, fini)
        message(length(location_ids), " jobs failed, ", tries, " retries remaining.")
    }
    if (length(location_ids) != 0) {
        stop("Failing summary jobs were relaunched ", tries-1, "x more and still did not ",
             "all complete. Check out your logs!")
    } else {
        message("Success! All summary jobs complete.")
    }

    #--UPLOAD ------------------------------------------------------------------

    system(paste0("qsub -P ", cluster_proj, " -l mem_free=40g -pe multi_slot 20",
                  " -e ", log_dir, "/errors -o ", log_dir, "/output",
                  " -cwd -N sev_upload",
                  " utils/python.sh FILEPATH gbd_env",
                  " python upload.py",
                  " ", version_id, " ", paf_version_id, " ", gbd_round_id))

}
