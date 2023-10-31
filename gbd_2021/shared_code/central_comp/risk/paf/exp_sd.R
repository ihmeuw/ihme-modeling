#' Given continous risk, calculate draws of exposure SD.
#'
#' Required args:
#' @param rei_id (int) id what risk do you want to calculate sd for?
#' @param decomp_step (str) step of decomp to run exposure standard deviation for. step1-5, or iterative.
#' @param cluster_proj (str) what project to run save_results job under on the cluster.
#' For GBD 2020 and later, crosswalk_version_id and corresponding bundle_id are required and data_path is not valid.
#' For GBD 2019 and earlier, either provide both a crosswalk_version_id and corresponding bundle_id OR specify a data_path to pull input data from.
#' @param bundle_id (int) bundle_id of data for data in epi database
#' @param crosswalk_version_id (int) crosswalk_version_id for data in epi database
#' @param data_path (str) full filepath to a csv with columns "std_dev" and "mean"
#' optional args:
#' @param bundle_std_dev_calc (str) How should we calculate standard deviation in your bundle? Should be an R equation in string form. Defaults to "standard_error * sqrt(effective_sample_size)". Other possible values might be "sqrt(variance)", etc. Only used when data is pulled from a crosswalk version.
#' outlier_cols, outlier_cv - for outliering
#' @param outlier_cols (list[str]) outlier using this list of columns if column value is 1. Example: c("cv_foo", "cv_bar")
#' @param outlier_cv (bool) outlier data points if cv > 2 as done in previous GBDs. Default is TRUE.
#' @param sd_scalar (int) multiply the resulting SD draws by this value. Default is 1.
#' @param gbd_round_id (int) GBD round to pull current exposure mean and data from. Default is 7, GBD 2020
#' @param save_results (bool) run save_results on draws? Default is TRUE.
#'
#' @return nothing. Will save draws in directory and optionally run save_results as well if requested.
#'
#' @examples
#' exp_sd(rei_id=111, bundle_id=1234, crosswalk_version_id=4796, bundle_std_dev_calc="sqrt(variance)", decomp_step = "iterative", save_results=FALSE, cluster_proj="my_proj")
exp_sd <- function(rei_id, cluster_proj, decomp_step, data_path = NULL, bundle_id = NULL,
                   bundle_std_dev_calc = "standard_error * sqrt(effective_sample_size)",
                   crosswalk_version_id = NULL, outlier_cols = NULL, outlier_cv = TRUE,
                   sd_scalar = 1, gbd_round_id = 7, save_results = TRUE) {

    # load libraries and functions
    library(data.table)
    library(magrittr)
    library(ini)
    library(RMySQL)
    library(mvtnorm)
    library(parallel)
    library(logging)

    source("FILEPATH/get_demographics.R")
    source("FILEPATH/get_model_results.R")
    source("FILEPATH/get_crosswalk_version.R")
    setwd("FILEPATH")
    source("./utils/cluster.R")
    source("./utils/db.R")
    source("save.R")

    set.seed(124535)
    n_draws <- 1000

    rei_meta <- get_rei_meta(rei_id, gbd_round_id)
    rei <- rei_meta$rei
    out_dir <- paste0("FILEPATH", rei, "/exposure_sd")
    dir.create(out_dir, showWarnings = FALSE)

    main_log_file <- paste0(out_dir, "/exp_sd_", format(Sys.time(), "%m-%d-%Y %I:%M:%S"), ".log")
    logReset()
    basicConfig(level='DEBUG')
    addHandler(writeToFile, file=main_log_file)

    message(sprintf("Logging here %s", main_log_file))
    loginfo("Exposure SD calc for %s", rei)

    loginfo("Validating args")
    if (gbd_round_id >= 7) {
        if (is.null(bundle_id) | is.null(crosswalk_version_id)) {
            err_msg <- "Must provide both a bundle_id and a crosswalk_version_id."
            logerror(err_msg);stop(err_msg)
        }
        if (!is.null(data_path)) {
            err_msg <- paste0("data_path is not used for gbd_round_id ", gbd_round_id, ".")
            logerror(err_msg);stop(err_msg)
        }
    } else {
        if ((is.null(data_path) & (is.null(bundle_id) | is.null(crosswalk_version_id))) |
            (!is.null(data_path) & (!is.null(bundle_id) | !is.null(crosswalk_version_id)))) {
            err_msg <- paste("Must provide either a data path OR both an",
                             "epi crosswalk_version_id and bundle_id.")
            logerror(err_msg);stop(err_msg)
        }
    }

    #--CHECK IF SD IS CALCULATED FOR REI -----------------------------------------
    mes <- get_rei_mes(rei_id, gbd_round_id, decomp_step)
    exp_me <- mes[draw_type  == "exposure"]$modelable_entity_id
    exp_sd_me <- mes[draw_type  == "exposure_sd"]$modelable_entity_id
    if (length(exp_sd_me) == 0)
        stop("rei_id ", rei_id, " doesn't have exposure SDs stored centrally currently.",
             "Submit a help desk ticket if you want to!")

    loginfo("rei_id=%s, data_path=%s, bundle_id=%s, crosswalk_version_id=%s, outlier_cols=[%s], outlier_cv=%s, sd_scalar=%s, gbd_round_id=%s, decomp_step=%s, save_results=%s, cluster_proj=%s",
            rei_id, data_path, bundle_id, crosswalk_version_id, paste(outlier_cols, collapse = ", "),
            outlier_cv, sd_scalar, gbd_round_id, decomp_step, save_results,
            cluster_proj)

    #--WIPE DIRECTORY ------------------------------------------------------------
    unlink(list.files(out_dir, pattern = ".csv$", full.names = T))

    #--CALCULATE THE SD ----------------------------------------------------------
    # pull exposure from epi db
    loginfo("Pulling exposure for %s", rei)
    demo <- get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)
    exposure <- get_model_results("epi", gbd_id = exp_me, location_id = demo$location_id,
                                  year_id = "all", age_group_id = demo$age_group_id,
                                  sex_id = demo$sex_id, gbd_round_id = gbd_round_id,
                                  decomp_step = decomp_step)
    exp_mvid <- unique(exposure$model_version_id)
    exposure <- exposure[, .(location_id, year_id, age_group_id, sex_id, measure_id, mean)]

    # pull input data from epi db or flat file or st-gpr outputs
    if (!is.null(crosswalk_version_id)) {
        loginfo("Pulling input data from crosswalk_version_id %d", crosswalk_version_id)
        data <- get_crosswalk_version(crosswalk_version_id=crosswalk_version_id)
        logdebug("Calculating standard deviation as %s", bundle_std_dev_calc)
        data[, std_dev := eval(parse(text=bundle_std_dev_calc))]
        # For ST-GPR bundles, the mean column is named val
        bundle_shape <- get_cw_bundle_shape(crosswalk_version_id)
        if (bundle_shape == 3) {
            data[, mean := val]
        }
    } else {
        loginfo("Pulling input data from %s", data_path)
        data <- fread(data_path)
    }
    missing_cols <- setdiff(c("mean","std_dev"), names(data))
    if (length(missing_cols) != 0) {
        err_msg <- paste0(ifelse(!is.null(crosswalk_version_id),
                                 paste0("crosswalk_version_id ", crosswalk_version_id),
                                 data_path),
                          " is missing needed column(s): ", missing_cols)
        logerror(err_msg);stop(err_msg)
    }

    if (outlier_cv) {
        # outlier if cv > 2 as done in previous GBDs
        data[, outlier := (std_dev / mean > 2) | (std_dev / mean < .1)]
        data <- data[outlier == F, ]
    }
    # also outlier if these columns == 1 (as provided by user)
    if (!is.null(outlier_cols))
        data <- data[apply(data[, outlier_cols, with = F] != 1, 1, all), ]

    # run regession on data
    loginfo("Running regression lm(log(std_dev) ~ log(mean), data)")
    reg <- lm(log(std_dev) ~ log(mean), data)

    # make diagnostic plot
    pdf(paste0(out_dir, "/mean_sd.pdf"))
    with(data, plot(log(mean), log(std_dev), main = unique(rei), col = "blue"))
    abline(reg, lwd = 3)
    dev.off()
    loginfo("Plot of log(mean) and log(std_dev) saved to %s/mean_sd.pdf", out_dir)

    # gen draws of exp sd
    draws <- rmvnorm(n = n_draws, reg$coefficients, vcov(reg)) %>% t
    draws <- melt(draws) %>% dcast(., . ~ Var1 + factor(Var2),
                                   value.var = "value") %>% data.table

    # save to flat files
    loginfo("Saving draws exposure SD draws to %s", out_dir)
    to_disk <- function(loc, exposure, draws, exp_sd_me, out_dir) {
        exp_sd <- cbind(exposure[location_id == loc, ], draws)
        invisible(alloc.col(exp_sd, 3007, verbose = F))
        exp_sd[, (paste0("draw_", 0:999)) := lapply(1:n_draws, function(draw)
            exp(get(paste0("(Intercept)_", draw)) + get(paste0("log(mean)_", draw)) * log(mean)) * sd_scalar)]
        exp_sd[, modelable_entity_id := exp_sd_me][, measure_id := 19]
        exp_sd[, c(paste0("log(mean)_", 1:n_draws),
                   paste0("(Intercept)_", 1:n_draws), "mean", ".") := NULL]
        write.csv(exp_sd, file=paste0(out_dir, "/" , loc, ".csv"), row.names=F)
    }
    mclapply(demo$location_id, to_disk, exposure = exposure, draws = draws,
             exp_sd_me = exp_sd_me, out_dir = out_dir, mc.cores = 6)

    #--SAVE RESULTS---------------------------------------------------------------
    if (save_results) {
        description <- paste("exposure mvid", exp_mvid,
                             "- data from", ifelse(!is.null(crosswalk_version_id),
                                                   paste("crosswalk_version_id", crosswalk_version_id),
                                                   paste("data_path", data_path)),
                             "- regession log(std_dev) ~ log(mean)",
                             ifelse(sd_scalar != 1,paste("- sd_scalar", sd_scalar),""))
        log_dir <- paste0("FILEPATH", Sys.info()[["user"]])
        loginfo("Launching save_results job now.")
        save_results_epi(cluster_proj, log_dir, rei, exp_sd_me,
                         unique(exposure$year_id), 19, description, out_dir,
                         bundle_id, crosswalk_version_id, gbd_round_id,
                         decomp_step, n_draws, "{location_id}.csv")
    }

    # shutdown log
    logReset()

}
