# Given a rei (or other entity) and microdata, fit ensemble weights to distribution

# required args:
# rei - (str) what entity (does not need to actually be an rei) are you fitting weights for? if already exists /FILEPATH/{rei}, will create a new version within that folder, otherwise will make a new one
# microdata - (str or datatable/frame) can either be a file path to the data or the dataframe/table itself if you have it in memory. Code will then save it here /FILEPATH/microdata.csv for reproducibility.
# cluster_account - (str) what account to run jobs under on the cluster.
# optional args:
# fit_by_sex - (bool) do you want to fit weights by sex? Defaul to TRUE.
# version - (int) version of results here /FILEPATH/{version}, will default to autoincrement. if specified, will wipe version if it already exists.
# n_cores - (int) number of cores for each job. Default is 30, but you can profile your jobs to see if you need more or less.

# returns: nothing. Will save all outputs here /FILEPATH/{version}

fit_ensemble_weights <- function (rei, microdata, cluster_account, fit_by_sex = TRUE,
                                  version = NULL, n_cores = 30) {

    #--CHECK RUN ID AND ENVIRONMENT---------------------------------------------

    # Make sure using on cluster
    if (system("which sbatch", intern = TRUE) != "/FILEPATH/")
        stop("fit_ensemble_weights() must be run on the SLURM cluster.")
    valid_aggregates <- c("location_id", "sex_id", "year_id")
    aggregate_id <- c()
    if (!all(aggregate_id %in% valid_aggregates))
        stop("Not all provided aggregate_id(s) are valid, (",
             paste(setdiff(aggregate_id, valid_aggregates), collapse = ", "), ")")

    # Load packages
    library(data.table)
    library(magrittr)
    library(ggplot2)
    library(gtools)
    source("/FILEPATH/get_demographics_template.R")
    # job hold function
    job_hold <- function(job_name) {
        start_time <- proc.time()
        Sys.sleep(5)
        flag <- 0
        while (flag == 0) {
            stats <- fread(text=system("squeue --me --format \"%j,%t,%K\"", intern = T))
            job_stats <- stats[NAME %like% job_name]

            if (nrow(job_stats) == 0) {
                flag <- 1
            } else {
                running_tasks <- job_stats[ST == "R"]$ARRAY_TASK_ID
                array_stats <- job_stats[ST == "PD"]
                if (nrow(array_stats) == 0) {
                    queued <- ""
                    n_queued <- 0
                } else {
                    # parse the number of queued tasks, which can look like "3-10%5" or simply "10"
                    queued <- unlist(strsplit(array_stats$ARRAY_TASK_ID, "%"))[1]
                    queued_tasks <- unlist(strsplit(queued, "-"))
                    n_queued <- ifelse (length(queued_tasks) > 1,
                                        as.integer(queued_tasks[2]) - as.integer(queued_tasks[1]) + 1,
                                        1
                    )
                }
                message(sprintf(
                    "%s total job(s), %d running, %d queued",
                    length(running_tasks) + n_queued,
                    length(running_tasks),
                    n_queued)
                )
                # for array jobs, list running tasks
                if (job_stats[1]$ARRAY_TASK_ID != "N/A") {
                    message(sprintf("Tasks running: %s", running_tasks %>% sort %>% paste(., collapse=", ")))
                    message(sprintf("Tasks queued: %s", queued))
                }
                Sys.sleep(60)
            }
        }
        job_runtime <- proc.time() - start_time
        job_runtime <- round(job_runtime[3] / 60, 0)
        Sys.sleep(10)
        message(sprintf("Job %s has completed (elapsed time: %s minutes).", job_name, job_runtime))
    }

    # make directory if doesn't exist and find version otherwise delete it if it exists
    out_dir <- paste0("/FILEPATH/", rei)
    dir.create(out_dir, showWarnings = F)
    if (is.null(version)) {
        version <- list.files(out_dir) %>% as.numeric
        if(length(version) == 0) version <- 0
        version <- max(version, na.rm = T) + 1
    } else {
        unlink(paste0(out_dir, "/", version), recursive = T)
    }
    message("Launching ensemble weights for ", rei, ", version ", version)

    #--Fit----------------------------------------------------------------------

    # Make diagnostic output folder
    dir.create(paste0('/FILEPATH/'), showWarnings = F)
    dir.create(paste0('/FILEPATH/'), showWarnings = F)
    dir.create(paste0('/FILEPATH/'), showWarnings = F)

    # pull microdata from file path or already existing data frame
    if (is.data.frame(microdata)) {
        df <- as.data.table(microdata)
    } else if(file.exists(microdata)) {
        df <- fread(microdata)
    } else {
        stop("microdata is neither a datatable/frame or a valid file path.")
    }
    req_cols <- c("nid","location_id","year_id","sex_id","age_year","data")
    missing_cols <- setdiff(req_cols, names(df))
    if(length(missing_cols) != 0) stop("microdata is missing the following columns: ", paste(missing_cols, collapse = ", "))
    write.csv(df[, req_cols, with = F],
              paste0("/FILEPATH/microdata.csv"), row.names=F)

    ## Launch jobs to fit ensemble by location, year, source (by sex happens within fit code)
    user <- Sys.getenv("USER")
    log_dir <- paste0("/FILEPATH/", user)
    dir.create(log_dir, showWarnings = FALSE)
    by_sex <- ifelse(fit_by_sex, 1, 0)
    params <- df[, .(nid, location_id, year_id)] %>% unique
    write.csv(params, paste0("FILEPATH/params.csv"), row.names = F)
    sbatch_cmd <- paste0("sbatch -J ensemble_fit_", rei, " -A ", cluster_account, " ",
                         "--mem 4G -c ", n_cores, " -a 1-", nrow(params), "%500 -p all.q ",
                         "-o ", log_dir, "/FILEPATH/%x.o%A.%a -e ", log_dir, "/FILEPATH/%x.e%A.%a ",
                         "/FILEPATH/execRscript.sh -s ",
                         "/FILEPATH/fit.R ",
                         "FILEPATH/params.csv ", rei, " ", version, " ", by_sex)
    system(sbatch_cmd)
    job_hold(paste0("ensemble_fit_", rei))

    ## Aggregate all the individual weight files
    files <- list.files(paste0("/FILEPATH/"), full.names = T)
    if(length(files) == 0) stop("No jobs finished sucessfully.")
    message("Ensemble fits complete, now making aggregate weights and diagnostics!")
    out <- lapply(files, fread) %>% rbindlist(., use.names = T, fill = T)
    out[, ks_stat:=NULL]

    ## Prep weights for PAF calculation
    global <- copy(out)
    aggregate_id <- setdiff(c(valid_aggregates, "nid"), aggregate_id)
    global[, (aggregate_id) := NULL]
    global <- global[, lapply(.SD,mean)]
    sq_template <- get_demographics_template(gbd_team = "epi")
    global[, location_id := 1]
    sq_template[, location_id := 1]
    sq_template <- setkey(sq_template) %>% unique
    global <- merge(global, sq_template, by="location_id")
    write.csv(global, paste0("FILEPATH/weights.csv"), na = "", row.names = F)

    ## Prep data for heat mapping
    out <- out[order(as.numeric(location_id))]
    out_melted <- suppressWarnings(melt(out, id.vars = c("location_id", "year_id", "sex_id", "nid")))
    out_melted[, id := .GRP, by = c("location_id", "year_id", "sex_id", "nid")]

    ## Make heatmap
    pdf(paste0("/FILEPATH/dist_weight_heatmap.pdf"))
    plot<-ggplot(out_melted, aes(x = variable, y = id)) +
        geom_tile(aes(fill=value), colour = "white") +
        theme_bw() +
        scale_fill_gradient(low = "white", high = "steelblue") +
        theme(axis.text.x=element_text(angle=45, hjust = 1)) +
        labs(x = "Distribution", y = "Source") +
        theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank())
    print(plot)
    dev.off()

    ## compile all ks graphs into one
    append_pdf <- function(dir, starts_with) {
        files <- list.files(dir, pattern = paste0("^", starts_with), full.names = T)
        files <- paste((mixedsort(files)), collapse = " ")
        cmd <- paste0("/FILEPATH/ghostscript -dBATCH -dSAFER -dNOGC -DNOPAUSE -dNumRenderingThreads=4 -q -sDEVICE=pdfwrite -sOutputFile=",
                      dir, "/", starts_with, ".pdf ", files)
        system(cmd)
    }
    append_pdf(paste0("/FILEPATH/"), "eKS_fit")
    append_pdf(paste0("/FILEPATH/"), "eKS_KS")

    ## Output NIDs that did not finish
    missing_nids <- data.table(nid = setdiff(unique(df$nid), unique(out$nid)))
    write.csv(missing_nids, paste0("/FILEPATH/did_not_finish_nids.csv"), na = "", row.names = F)
}
