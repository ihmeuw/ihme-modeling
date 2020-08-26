# sev calculator

# optional args:
# verion_id - (int) version of sevs, if not specified, will use max + 1.
# paf_verion_id - (int) version of pafs, if not specified, will use max
# year_id - (list[int]) Which years do you want to calculate?
# location_set_id - (list[int]) what location sets to aggregate?
# pct_change - (bool) do you want to run percent change as well?
# n_draws - (int) How many draws should be used? Cannot be set higher than actual
#                 number of draws in input data, only downsampling, not upsampling is allowed. Default is 1000.
# gbd_round_id - (int) Default is 6, GBD 2019
# resume - (bool) will resume starting in sev calc phase only, not mid loc agg or summarization
# cluster_proj - (str) what project to run save_results job under on the cluster.

# returns: nothing. Will save draws in directory, make summaries, and upload to gbd db

launch_sev <- function(version_id = NULL, paf_version_id = NULL,
                       year_id = c(1990, 2010, 2019), location_set_id = c(89),
                       pct_change = TRUE, n_draws = 1000,
                       gbd_round_id = 6, decomp_step=NULL,
                       resume = FALSE, cluster_proj = "proj_centralcomp") {

    # load libraries and functions
    library(magrittr)
    library(logging)
    source("FILEPATH/get_demographics.R")
    source("FILEPATH/get_rei_metadata.R")
    source("FILEPATH/get_population.R")
    setwd("FILEPATH")
    source("./utils/cluster.R")

    #--CHECK RUN ID AND ENVIRONMENT-----------------------------------------------
    if (!(.Platform$OS.type == "unix"))
        stop("Can only launch parallel jobs for this script on the cluster")

    out_dir <- paste0("FILEPATH/sev")
    if(is.null(version_id)) {
        version_id <- list.files(out_dir) %>% as.numeric %>% max(., na.rm = T)
        version_id <- version_id + 1
    }
    out_dir <- paste0(out_dir, "/", version_id)
    dir.create(out_dir, showWarnings = FALSE)
    dir.create(paste0(out_dir,"/exp_pct"), showWarnings = FALSE)
    dir.create(paste0(out_dir,"/rrmax"), showWarnings = FALSE)
    dir.create(paste0(out_dir,"/draws"), showWarnings = FALSE)
    dir.create(paste0(out_dir,"/summaries"), showWarnings = FALSE)

    # set up logging
    main_log_file <- paste0(out_dir, "/sev_", format(Sys.time(), "%m-%d-%Y %I:%M:%S"), ".log")
    logReset()
    basicConfig(level='DEBUG')
    addHandler(writeToFile, file=main_log_file)

    loginfo("SEV v%s", version_id)

    # validate args
    loginfo("Validating args")
    demo <- get_demographics(gbd_team = "epi_ar", gbd_round_id = gbd_round_id)
    if (!all(year_id %in% demo$year_id))
        stop("Not all provided year_id(s) are valid GBD year_ids, (",
             paste(setdiff(year_id, demo$year_id), collapse = ", "), ")")
    if (!(1 <= n_draws & n_draws <= 1000))
        stop("n_draws must be between 1-1000.")
    paf_versions <- list.files("FILEPATH/pafs/") %>% as.numeric
    if(is.null(paf_version_id)) paf_version_id <- max(paf_versions, na.rm=T)
    if (!(paf_version_id %in% paf_versions))
        stop(paf_version_id, " is not a valid PAF version")
    loginfo("paf_version_id %s, year_id %s, n_draws %s, gbd_round_id %s, decomp_step %s, resume %s, cluster_proj %s",
            paf_version_id,paste(year_id, collapse = ", "),n_draws,
            gbd_round_id, decomp_step, resume, cluster_proj)

    # find reis to calc SEVs for by finding intersection of risks that we have
    # PAFs for and most detailed risks in the computation hierarchy
    paf_reis <- fread(cmd=paste0("zcat < FILEPATH/pafs/", paf_version_id, "/existing_reis.csv.gz"))
    reis <- get_rei_metadata(rei_set_id = 2, gbd_round_id = gbd_round_id)
    reis <- reis[!(rei_id %in% c(170, 131, 168, 242, 138, 201, 338, 337)), ]
    reis <- reis[rei_id %in% unique(paf_reis$rei_id) & (most_detailed==1 | rei_id %in% c(339, 380)),]
    rei_ids <- reis$rei_id

    #--WIPE DIRECTORY IF NOT IN RESUME MODE --------------------------------------
    if (!resume) {
        loginfo("Wiping draw directory as not running in resume mode. You have 30 seconds to change your mind...")
        Sys.sleep(30)
        loginfo("deleting %s now.", out_dir)
        unlink(paste0(out_dir,"/draws/"), recursive = T)
        dir.create(paste0(out_dir,"/draws"), showWarnings = FALSE)
    }

    #--CALCULATE SEVS ----------------------------------------------------------
    user <- Sys.getenv("USER")
    log_dir <- paste0("FILEPATH", user)

    # array job by location and risk
    params <- expand.grid(location_id = demo$location_id, risk_id = rei_ids) %>% data.table
    retry_qsub(job_name = paste0("sev_calc_", version_id),
               script = "sev_calc.R", params = params,
               out_dir = paste0(out_dir, "/draws"),
               queue = "all.q",
               m_mem_free = "10G",
               fthread = 1,
               logs = log_dir,
               arguments = paste(paste0("'c\\(", paste(year_id, collapse = ","), "\\)'"),
                                 n_draws, gbd_round_id, decomp_step, out_dir,
                                 paf_version_id, sep = " "),
               cluster_project = cluster_proj)

    # array job by location and risk aggregates
    paf_reis <- fread(cmd=paste0("zcat < FILEPATH/pafs/", paf_version_id, "/existing_reis.csv.gz"))
    reis <- get_rei_metadata(rei_set_id = 2, gbd_round_id = gbd_round_id)
    reis <- reis[!(rei_id %in% c(170, 131, 168, 242, 138, 201, 338, 337, 331)), ]
    reis <- reis[rei_id %in% unique(paf_reis$rei_id) & (!rei_id %in% as.numeric(list.files(paste0(out_dir,"/draws")))),]
    rei_ids <- reis$rei_id
    params <- expand.grid(location_id = demo$location_id, risk_id = rei_ids) %>% data.table
    retry_qsub(job_name = paste0("sev_calc_agg_", version_id),
               script = "sev_calc.R", params = params,
               out_dir = paste0(out_dir, "/draws"),
               m_mem_free = "40G",
               fthread = 1,
               logs = log_dir,
               arguments = paste(paste0("'c\\(", paste(year_id, collapse = ","), "\\)'"),
                                 n_draws, gbd_round_id, decomp_step, out_dir,
                                 paf_version_id, sep = " "),
               cluster_project = cluster_proj)

    #--AGGREGATE AND SUMMARIZE--------------------------------------------------
    rei_ids <- as.numeric(list.files(paste0(out_dir, "/draws")))
    rei_ids <- rei_ids[!is.na(rei_ids)]

    # for each risk, aggregate locations
    tries <- 3
    while (tries != 0 & length(rei_ids) != 0) {
        for (rei_id in rei_ids) {
            system(paste0("qsub -P ", cluster_proj, " -l m_mem_free=60G,fthread=10,h_rt=72:00:00 -q all.q ",
                          " -e ", log_dir, "/errors -o ", log_dir, "/output",
                          " -cwd -N sev_loc_agg_", rei_id,
                          " utils/python.sh aggregate.py",
                          " --sev_version_id ", version_id,
                          " --rei_id ", rei_id,
                          " --location_set_id ", paste(location_set_id, collapse=" "),
                          " --n_draws ", n_draws,
                          " --gbd_round_id ", gbd_round_id,
                          " --decomp_step ", decomp_step))
        }
        job_hold("sev_loc_agg")
        fini <- NULL
        loginfo("Checking to make sure all output files exist...")
        for (rei_id in rei_ids) {
            if (file.exists(paste0(out_dir, "/draws/", rei_id, "/44642.csv"))) {
                fini <- c(fini, rei_id)
            }
        }
        if (is.null(fini)) {
            stop("All loc agg jobs failed. Check out your logs!")
        }
        tries <- tries - 1
        rei_ids <- setdiff(rei_ids, fini)
        loginfo("%s jobs failed, %s retries remaining.",length(rei_ids), tries)
    }
    if (length(rei_ids) != 0) {
        stop("Failing loc agg jobs were relaunched ", tries-1, "x more and still did not ",
             "all complete. Check out your logs.")
    } else {
        loginfo("Success! All loc agg jobs complete.")
    }

    # summarize
    location_ids <- list.files(paste0(out_dir, "/draws/169/"))
    location_ids <- gsub(".csv", "", location_ids) %>% as.numeric
    fini <- NULL
    loginfo("Checking to make sure all output files exist...")
    for (location_id in location_ids) {
        if (file.exists(paste0(out_dir, "/summaries/single_year_", location_id, ".csv"))) {
            fini <- c(fini, location_id)
        }
    }
    location_ids <- setdiff(location_ids, fini)
    loginfo("%s jobs to go.", length(location_ids))
    tries <- 3
    while (tries != 0 & length(location_ids) != 0) {
        for (location_id in location_ids) {
            system(paste0("qsub -P ", cluster_proj, " -l m_mem_free=40G,fthread=12,h_rt=72:00:00 -q long.q ",
                          " -e ", log_dir, "/errors -o ", log_dir, "/output",
                          " -cwd -N sev_summary_", location_id,
                          " utils/python.sh summarize.py",
                          " --sev_version_id ", version_id,
                          " --location_id ", location_id,
                          " --year_id ", paste(year_id, collapse=" "),
                          ifelse(pct_change, " --change", ""),
                          " --gbd_round_id ", gbd_round_id))
        }
        job_hold("sev_summary")
        fini <- NULL
        loginfo("Checking to make sure all output files exist...")
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
        loginfo("%s jobs failed, %s retries remaining.",length(location_ids), tries)
    }
    if (length(location_ids) != 0) {
        stop("Failing summary jobs were relaunched ", tries-1, "x more and still did not ",
             "all complete. Check out your logs.")
    } else {
        loginfo("Success! All summary jobs complete.")
    }

    #--UPLOAD ------------------------------------------------------------------
    system(paste0("qsub -P ", cluster_proj, " -l m_mem_free=200G,fthread=2,h_rt=72:00:00 -q long.q ",
                  " -e ", log_dir, "/errors -o ", log_dir, "/output",
                  " -cwd -N sev_upload",
                  " utils/python.sh upload.py",
                  " ", version_id, " ", paf_version_id, " ", gbd_round_id, " ", decomp_step))

    # shut down logging
    logReset()

}
