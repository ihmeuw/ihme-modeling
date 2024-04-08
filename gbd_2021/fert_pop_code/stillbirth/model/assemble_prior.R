
#########################################################################################################################
##                                                                                                                     ##
## Purpose: Average the results of the test_prior() function and create ensemble linear predictions                    ##
##                                                                                                                     ##
## Required Arguments:                                                                                                 ##
##    - data: dataset used in test_prior() for generating component models                                             ##
##    - cov_list: A character string of covariates in the covariate_name form -> must include any custom covariates    ##
##    - data_transform: Character string, either "log" or "logit" to be inherited by STGPR's transform_data() function ##
##        ex: data_transform = "log"                                                                                   ##                                                                                  ##
##                                                                                                                     ##
#########################################################################################################################

assemble_prior <- function(data, rmses, cov_list, data_transform, pred_ages, me,
                           custom_cov_list = NULL, polynoms = NULL, n_mods = 10,
                           plot_mods = F, age_trend = F, plot_mods_path = NULL, username = NULL, proj,
                           weight_col = "out_rmse", by_sex = T, location_set_id = 22, decomp_step) {

  if (F) {

    plot_mods_path <- "FILEPATH"
    username <- Sys.getenv("USER")

    # needed for this script (not yet assigned)
    location_set_id <- 22
    plot_mods <- T
    weight_col <- "out_rmse"

  }

  ############
  ## SET-UP ##
  ############

  library(boot)
  library(data.table)
  library(DBI)
  library(ggplot2)
  library(lme4)
  library(matrixStats)
  library(rhdf5)
  library(RMySQL)

  library(plyr)
  library(dplyr)

  #############
  ## SCRIPTS ##
  #############

  central <- "FILEPATH"

  source(paste0(central, "get_location_metadata.R"))

  source(paste0(working_dir, "bind_covariates.R"))
  source(paste0(working_dir, "helper_functions.R"))

  #############################
  ## DEFINE PRED.LM FUNCTION ##
  #############################

  pred.lm <- function(df, model, predict_re = 0) {

    ## RE form
    re.form <- ifelse(predict_re == 1, NULL, NA)

    ## Predict
    if (class(model) == "lmerMod") {

      prior <- predict(model, newdata = df, allow.new.levels = T, re.form = re.form)

    } else {

      prior <- predict(model, newdata = df)

    }

    return(prior)

  }

  #################
  ## DATA CHECKS ##
  #################

  nec_cols <- c("data", "location_id", "year_id", "age_group_id", "sex_id")

  invisible(lapply(nec_cols, function(x) {

    if (!x %in% names(data)) stop(paste0("Missing necessary column:", x))

  }))

  nec_cols <- c(weight_col)

  invisible(lapply(nec_cols, function(x) {

    if (!x %in% names(rmses)) stop(paste0("Missing necessary column:", x))

  }))

  if (plot_mods == T & length(plot_mods_path) != 1) stop("plot_mods == T but you have not specified a valid plot_mods_path!")

  if (n_mods > nrow(rmses)) {

      message("You specified to average over ", n_mods, " models, but only supplied ", nrow(rmses), ". Only the supplied models will be averaged.")
      n_mods <- nrow(rmses)

    }

  # drop data NAs (if square already, just make a new one)
  data <- data[!is.na(data)]
  locs <- get_location_metadata(location_set_id = location_set_id, gbd_round_id = gbd_round_id)

  # get standard locs for predictions
  subnat_locs <- get_location_metadata(location_set_id = 101, gbd_round_id = gbd_round_id)$location_id

  l_set_v_id <- unique(locs$location_set_version_id)

  ####################
  ## GET COVARIATES ##
  ####################

  # set up data set if it's 'all ages'
  if (length(unique(data$age_group_id)) == 1) {

    if (unique(data$age_group_id) == 22 | unique(data$age_group_id) == 27) {

      data[, age_group_id := 22] ##set to 22
      by_age <- 0

    } else if (!(unique(data$age_group_id) %in% c(2:5, 388:389, 238, 34, 6:20, 30:32, 235, 164))) {

      stop("Check about incorporating custom age group")

    } else {

      by_age <- 1

    }

  } else {

    by_age <- 1

  }

  sqr <- make_square(location_set_id, gbd_round_id = gbd_round_id, year_start = 1980, year_end = 2022, covariates = NA,
                     by_age = by_age, by_sex = by_sex, custom_age_group_id = pred_ages)

  ## get covariates
  sqr_and_names <- bind_covariates(sqr, cov_list = cov_list, custom_cov_list = custom_covs, decomp_step = decomp_step)
  sqr <- sqr_and_names[[1]] # this is the data w/ bound covariate estimates
  cov_list <- sqr_and_names[[2]] # these are the covariate_name_shorts

  if ("SBA_coverage_prop" %in% cov_list) {

    message("taking logit of SBA_coverage_prop for modeling")
    sqr[SBA_coverage_prop == 1, SBA_coverage_prop := 0.9999999] # prevent Inf in logit space
    sqr[, SBA_coverage_prop := boot::logit(SBA_coverage_prop)] # logit SBA for modeling
  }

  if ("IFD_coverage_prop" %in% cov_list) {

    message("taking logit of IFD_coverage_prop for modeling")
    sqr[IFD_coverage_prop == 1, IFD_coverage_prop := 0.9999999] # prevent Inf in logit space
    sqr[, IFD_coverage_prop := boot::logit(IFD_coverage_prop)] # logit IFD for modeling

  }

  if ("ANC1_coverage_prop" %in% cov_list) {

    message("taking logit of ANC1_coverage_prop for modeling")
    sqr[ANC1_coverage_prop == 1, ANC1_coverage_prop := 0.9999999] # prevent Inf in logit space
    sqr[, ANC1_coverage_prop := boot::logit(ANC1_coverage_prop)] # logit ANC1 for modeling

  }

  if ("ANC4_coverage_prop" %in% cov_list) {

    message("taking logit of ANC4_coverage_prop for modeling")
    sqr[ANC4_coverage_prop == 1, ANC4_coverage_prop := 0.9999999] # prevent Inf in logit space
    sqr[, ANC4_coverage_prop := boot::logit(ANC4_coverage_prop)] # logit ANC4 for modeling

  }

  if (by_sex == T) {

    sex_list <- c("M", "F")

    if (!(1 %in% unique(data$sex_id))) sex_list <- c("F")
    if (!(1 %in% unique(data$sex_id))) sqr <- sqr[sex_id == 2]

  } else {

    sex_list <- c("both_sexes")

  }

  #####################
  ## GET POLYNOMIALS ##
  #####################

  # this creates a column in the dataset, and also saves the names of those columns in the 'polynoms' vector
  if (!is.null(polynoms)) {

    polys <- strsplit(polynoms, "\\^")

    polynoms.t <- list()

    for (i in 1:length(polys)) {

      basecov <- polys[[i]][1]

      if (!basecov %in% names(sqr)) { stop("ATTEMPTING TO CREATE POLYNOMIAL, MISSING: ", polys[[i]][1])}

      sqr[, paste0(basecov, polys[[i]][2]) := get(basecov)^as.numeric(polys[[i]][2])]
      polynoms.t[[i]] <- paste0(basecov, polys[[i]][2])

    }

    polynoms <- unlist(polynoms.t)

  }

  # add polynoms to cov_list
  og_cov_list <- cov_list
  cov_list <- c(cov_list, polynoms)

  # get locs
  sqr <- merge(sqr, locs[, .(location_name, region_name, super_region_name, location_id)], by = "location_id")

  #################
  ## LOOP BY SEX ##
  #################

  output <- list()

  for (sexchar in sex_list) {

    # run models
    message("Model averaging for ", sexchar)
    s_id <- ifelse(sexchar == "both_sexes", 3, ifelse(sexchar == "M", 1, 2))

    sqr.s <- sqr[sex_id == s_id,]
    data.s <- data[sex_id == s_id,]
    data.s <- data.s[location_id %in% subnat_locs]

    rmses.s <- rmses[sex == sexchar]

    if (n_mods > nrow(rmses.s)) {

      message("You specified to average over ", n_mods, " models for sex ", sexchar, ", but only supplied ", nrow(rmses.s), ". Only the supplied models will be averaged.")
      n_mods.s <- nrow(rmses.s)

    } else {

      n_mods.s <- n_mods

    }

    sqr.s <- sqr.s[age_group_id %in% pred_ages]

    ########################
    ## CREATE PREDICTIONS ##
    ########################

    for (n in 1:n_mods.s) {

      message("  Predicting for model ", n)

      form <- rmses.s[n, covs]
      form <- paste0(paste0(data_transform, "(data)~", form))
      modtype <- ifelse(grepl("\\(1 \\|", form), "lmer", "lm")

      if (modtype == "lmer") {

        mod <- lmer(as.formula(form), data = data.s)

      }

      if (modtype == "lm") {

        mod <- lm(as.formula(form), data = data.s)

      }

      # predict on the square
      sqr.s[, paste0("pred", n) := transform_data(pred.lm(sqr.s, mod), data_transform, reverse = T)]
      sqr.s[, paste0("wt", n) := rmses.s[n, get(weight_col)]]

      rm(mod)

    }

    n <- n_mods.s

    #########################
    ## AVERAGE PREDICTIONS ##
    #########################

    message("Averaging Predictions!")
    wts <- 1 / rmses.s[1:n, get(weight_col)]

    invisible(lapply(1:n, function(x) {

      sqr.s[, paste0("numerator", x) := get(paste0("pred", x)) * wts[x]]

    }))

    sqr.s[, ave_result := rowSums(.SD)/sum(wts), .SDcols = grep("numerator", names(sqr.s), value = T)]

    # get age group id names for plotting to look nice
    message("Getting age group names..")

    if (unique(sqr.s$age_group_id) == 161) {

      sqr.s[, age_group_name := 0]

    } else {

      age_ids <- get_ids(table = "age_group")
      sqr.s <- merge(sqr.s, age_ids, by = "age_group_id", all.x = T)

    }

    message("Done getting age group names")

    saveRDS(sqr.s, file = "FILEPATH", version = 2)

    ######################
    ## PLOT PREDICTIONS ##
    ######################

    if (plot_mods == T) {

      param_map <- data.table(unique(sqr.s[, c("location_id")]))
      param_map_filepath <- "FILEPATH"
      write.csv(param_map, param_map_filepath, row.names = F)

      me_weeks <- substr(me, 13, 14)

      n_jobs <- paste0("1-", nrow(param_map), "%100")

      command <- paste0("sbatch -p all.q -c 1 -t 00:05:00 --mem=2G", "FILEPATH")
      system(command)

      message("  Waiting on plots...")
      job_hold("average_")
      message("Done plotting")

      ## check for missingness
      files <- list.files(path = "FILEPATH", pattern = "subplot")

      if (length(files) < length(unique(sqr.s$location_id))) {

        message(paste0(length(unique(sqr.s$location_id)) - length(files)), " output pdfs are missing!")
        Sys.sleep(6)

      }

    }

    sqr.s[, c(grep("numerator|pred|wt", names(sqr.s), value = T), cov_list) := NULL]

    output[[length(output) + 1]] <- sqr.s
    message("Done averaging for ", sexchar)

  }

  output <- rbindlist(output)
  return(output)

}
