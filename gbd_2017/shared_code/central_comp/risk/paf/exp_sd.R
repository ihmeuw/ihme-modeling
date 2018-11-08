# Given continous risk, calculate draws of exposure SD.

# required args:
# rei_id  - (int) id what risk do you want to calculate sd for?
# data_path, bundle_id, me_name - specify only ONE of these three to specify where to pull input data from.
#   data_path  - (str) full filepath to a csv with columns "std_dev" and "mean"
#   bundle_id - (int) bundle_id for data in epi datbase
#   me_name - (str) st-gpr me_name to pull data from the current best data_id
# optional args:
# outlier_cols, outlier_cv - for outliering
#   outlier_cols - (list[str]) outlier using this list of columns if column value is 1. Example: c("cv_foo", "cv_bar")
#   outlier_cv - (bool) outlier data points if cv > 2. Default is TRUE.
# sd_scalar - (int) multiple the resulting SD draws by this value. Default is 1.
# gbd_round_id - (int) GBD round to pull current exposure mean and data from. Default is 5.
# save_results - (bool) run save_results on draws? Default is TRUE.
# cluster_proj - (str) what project to run save_results job under on the cluster. Default is proj_paf.
# log - (bool) do you want to write log file to "FILEPATH/exp_sd.Rout or just print log messages to screen? Default is TRUE, log file.

# returns: nothing. Will save draws in directory and optionally run save_results as well if requested.

exp_sd <- function(rei_id, data_path = NULL, bundle_id = NULL, me_name = NULL,
                   outlier_cols = NULL, outlier_cv = TRUE, sd_scalar = 1,
                   gbd_round_id = 5, save_results = TRUE, cluster_proj = "proj_paf", log = TRUE) {

    # load libraries and functions
    library(data.table)
    library(magrittr)
    library(ini)
    library(RMySQL)
    library(mvtnorm)
    library(rhdf5)
    library(parallel)

    source("FILEPATH/get_demographics.R")
    source("FILEPATH/get_model_results.R")
    source("FILEPATH/get_epi_data.R")
    setwd("FILEPATH/paf")
    source("./utils/db.R")
    source("save.R")

    set.seed(124535)

    #--CHECK IF SD IS CALCULATED FOR REI -----------------------------------------

    mes <- get_rei_mes(rei_id)
    exp_me <- mes[draw_type  == "exposure"]$modelable_entity_id
    if (rei_id == 122) exp_me <- 2436 # pufa, drop sat fat
    exp_sd_me <- mes[draw_type  == "exposure_sd"]$modelable_entity_id
    if (is.null(exp_sd_me))
        stop(rei_id, " doesn't have exposure SDs stored centrally currently.",
             "Submit a help desk ticket if you want to!")
    if (!(.Platform$OS.type == "unix"))
        stop("Can only launch parallel jobs for this script on the cluster")

    rei_meta <- get_rei_meta(rei_id)
    rei <- rei_meta$rei
    out_dir <- paste0("FILEPATH/", rei, "/exposure_sd")
    dir.create(out_dir, showWarnings = FALSE)
    if (log) {
        suppressWarnings(sink())
        unlink(paste0(out_dir, "/exp_sd.Rout"))
        message("Logging here ", out_dir, "/exp_sd.Rout")
        con <- file(paste0(out_dir, "/exp_sd.Rout"))
        sink(con)
        sink(con, type="message")
    }
    message(format(Sys.time(), "%D %H:%M:%S"), " Exposure SD calc for ", rei)

    message("Validating args")
    if (is.null(bundle_id) + is.null(data_path) + is.null(me_name) != 2)
        stop("Must provide an epi bundle_id OR file path to data OR me_name from st-gpr.")

    message("rei_id - ", rei_id,
            "\ndata_path - ", data_path,
            "\nbundle_id - ", bundle_id,
            "\nme_name - ", me_name,
            "\noutlier_cols - ", paste(outlier_cols, collapse = ", "),
            "\noutlier_cv - ", outlier_cv,
            "\nsd_scalar - ", sd_scalar,
            "\ngbd_round_id - ", gbd_round_id,
            "\nsave_results - ", save_results,
            "\ncluster_proj - ", cluster_proj)

    #--WIPE DIRECTORY ------------------------------------------------------------

    unlink(list.files(out_dir, pattern = ".csv$", full.names = T))

    #--CALCULATE THE SD ----------------------------------------------------------

    # pull exposure from epi db
    message("Pulling exposure and input data for ", rei)
    demo <- get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)
    exposure <- get_model_results("epi", gbd_id = exp_me, location_id = demo$location_id,
                                  year_id = "all", age_group_id = demo$age_group_id,
                                  sex_id = demo$sex_id, gbd_round_id = gbd_round_id)
    exp_mvid <- unique(exposure$model_version_id)
    exposure <- exposure[, .(location_id, year_id, age_group_id, sex_id, measure_id, mean)]

    # pull input data from epi db or flat file or st-gpr outputs
    if (!is.null(bundle_id)) {
        data <- get_epi_data(bundle_id=bundle_id)
        data[, std_dev := standard_error * sqrt(effective_sample_size)]
    } else if (!is.null(data_path)) {
        data <- fread(data_path)
        if (length(setdiff(c("mean","std_dev"), names(data))) != 0) stop(file_path,
                                                                         " is missing needed column(s) mean and/or std_dev.")
    } else {
        data_db <- fread("FILEPATH/data_db.csv")
        mn <- me_name
        data_id <- data_db[me_name == mn & best_end == "Current best",data_id]
        data <- suppressWarnings(data.table(h5read(paste0("FILEPATH/",data_id,".h5"), "data")))
        setnames(data, "data", "mean")
        data[, std_dev := sqrt(variance)]
    }

    if (outlier_cv) {
        data[, outlier := (std_dev / mean > 2) | (std_dev / mean < .1)]
        data <- data[outlier == F, ]
    }
    # also outlier if these columns == 1 (as provided by user)
    if (!is.null(outlier_cols))
        data <- data[apply(data[, outlier_cols, with = F] != 1, 1, all), ]

    # run regession on data
    message("Running regression lm(log(std_dev) ~ log(mean), data)")
    reg <- lm(log(std_dev) ~ log(mean), data)

    # make diagnostic plot
    pdf(paste0(out_dir, "/mean_sd.pdf"))
    with(data, plot(log(mean), log(std_dev), main = unique(rei), col = "blue"))
    abline(reg, lwd = 3)
    dev.off()
    message("Plot of log(mean) and log(std_dev) saved to ", out_dir, "/mean_sd.pdf")

    # gen draws of exp sd
    draws <- rmvnorm(n = 1000, reg$coefficients, vcov(reg)) %>% t
    draws <- melt(draws) %>% dcast(., . ~ Var1 + factor(Var2),
                                   value.var = "value") %>% data.table

    # save to flat files
    message("Saving draws exposure SD draws to ", out_dir)
    to_disk <- function(loc, exposure, draws, exp_sd_me, out_dir) {
        exp_sd <- cbind(exposure[location_id == loc, ], draws)
        invisible(alloc.col(exp_sd, 3007, verbose = F))
        exp_sd[, (paste0("draw_", 0:999)) := lapply(1:1000, function(draw)
            exp(get(paste0("(Intercept)_", draw)) + get(paste0("log(mean)_", draw)) * log(mean)) * sd_scalar)]
        exp_sd[, modelable_entity_id := exp_sd_me][, measure_id := 19]
        exp_sd[, c(paste0("log(mean)_", 1:1000),
            paste0("(Intercept)_", 1:1000), "mean", ".") := NULL]
        write.csv(exp_sd, file=paste0(out_dir, "/" , loc, ".csv"), row.names=F)
    }
    to_disk(demo$location_id[1],exposure = exposure, draws = draws,
            exp_sd_me = exp_sd_me, out_dir = out_dir)
    mclapply(demo$location_id, to_disk, exposure = exposure, draws = draws,
             exp_sd_me = exp_sd_me, out_dir = out_dir, mc.cores = 15)

    #--SAVE RESULTS---------------------------------------------------------------

    if (save_results) {
        description <- paste("exposure mvid", exp_mvid,
                             "- data from", ifelse(!is.null(bundle_id), paste("bundle_id", bundle_id), ifelse(!is.null(data_path), data_path, paste("stgpr data_id", data_id))),
                             "- regession log(std_dev) ~ log(mean)",
                             ifelse(sd_scalar != 1,paste("- sd_scalar", sd_scalar),""))
        log_dir <- paste0("FILEPATH/", Sys.getenv("USER"))
        message("Launching save_results job now.")
        save_results_epi(cluster_proj, log_dir, rei, exp_sd_me,
                         unique(exposure$year_id), 19, description, out_dir)
    }

    if (log) {
        sink(type="message")
        sink()
        close(con)
    }

}
