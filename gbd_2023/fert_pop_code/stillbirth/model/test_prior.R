###################################################################################################################################
##                                                                                                                               ##
## Purpose: Test combinations of priors, calculate OOS validity, and generate weights                                            ##
##                                                                                                                               ##
## Outputs:                                                                                                                      ##
##   - A list of two data tables:                                                                                                ##
##      - A data.table with each row corresponding to a model, and the following columns:                                        ##
##         - out_rmse: the average out of sample rmse for that model                                                             ##
##         - in_rmse: the in sample rmse for that model                                                                          ##
##         - aic: the in-sample Aikake information criterion value for that model                                                ##
##         - Intercept_fixd: the global intercept for that model                                                                 ##
##         - Intercept_fixd_se: the standard error of the global intercept for that model                                        ##
##         - 'cov'_fixed: a column for each covariate, giving the fixed effect coefficient                                       ##
##         - 'cov'_fixed_se: a column for each covariate, giving the standard error of that covariate's fixed effect coefficient ##
##      - A data.table that is identical to the input data, except with the following extra columns:                             ##
##         - The covariate estimates specified                                                                                   ##
##         - Columns indicating which rows have been knocked out for each holdout                                                ##
##                                                                                                                               ##
###################################################################################################################################

test_prior <- function(crosswalk_version_id = NA, path_to_data = NA, release_id, cov_list, data_transform,
                       rank_method = "aic", modtype = "lmer", offset = 0.0001, count_mods = FALSE,
                       custom_covs = NULL, fixed_covs = NULL, random_effects = NULL, ban_pairs = NULL,
                       by_sex = T, username = username, polynoms = NULL, prior_sign = NULL, p_value = 0.05,
                       ko_prop = 0.25, kos = 5, remove_subnats = T, no_new_ages = T, only_data_locs = F,
                       drop_nids = FALSE, seed = 145153, location_set_id = 22, proj = "proj_fertilitypop",
                       m_mem_free = 2, forms_per_job = 10) {

  if (F) {

    crosswalk_version_id = NA
    release_id = 16
    cov_list = c("Antenatal Care (4 visits) Coverage (proportion)", "Antenatal Care (1 visit) Coverage (proportion)",
                 "In-Facility Delivery (proportion)", "Skilled Birth Attendance (proportion)", "Maternal care and immunization",
                 "Maternal Education (years per capita)", "Socio-demographic Index", "Healthcare access and quality index",
                 "Proportion of the population with at least 12 years of education", "Education Relative Inequality (Gini)")
    prior_sign = c(-1, -1, -1, -1, -1, -1, -1, -1, -1, -1)
    data_transform = "log"
    username = "USERNAME"
    rank_method = "oos.rmse"
    modtype = "lmer"
    custom_covs = NULL
    fixed_covs = NULL
    random_effects = c("(1|super_region_name)", "(1|region_name)")
    ban_pairs = NULL
    by_sex = F
    polynoms = NULL
    ko_prop = 0.25
    kos = 5
    forms_per_job = 10
    proj = "PROJECT NAME"
    m_mem_free = 6

    count_mods = F
    offset = 0.0001
    remove_subnats = T

    drop_nids = T
    location_set_id = 22
    no_new_ages = T
    only_data_locs = F
    p_value = 0.05
    seed = 145153

  }

  ############
  ## SET-UP ##
  ############

  # Load libraries and source functions
  library(data.table)
  library(DBI)
  library(ggplot2)
  library(ini)
  library(lme4)
  library(RMySQL)

  library(plyr)
  library(dplyr)

  library(mortcore)
  library(mortdb)

  ## Checks

  # check prior signs
  if (!is.null(prior_sign)) {

    if (length(prior_sign) != length(cov_list) + length(custom_covs)) stop("Length of prior_sign is not equal to length of cov_list!")

  }

  # check model types
  if (modtype == "lmer" & is.null(random_effects)) stop("Specify random effects")
  if (modtype == "lm" & !is.null(random_effects)) stop("Specify random effects")

  ## Set Paths

  central <- "FILEPATH"

  output_folder <- "FILEPATH"

  if (file.exists(output_folder)) {

    message(paste("Temporary directory", output_folder, "already exists, deleting contents"))
    unlink(output_folder, recursive = T)
    message("Done deleting")

  }

  dir.create(output_folder)

  #############
  ## SCRIPTS ##
  #############

  source(paste0(working_dir, "bind_covariates.R"))
  source(paste0(working_dir, "make_ko.R"))
  source(paste0(working_dir, "helper_functions.R"))

  source(paste0(central, "get_location_metadata.R"))
  source(paste0(central, "get_crosswalk_version.R"))

  locs <- get_location_metadata(location_set_id = location_set_id, release_id = release_id)[, .(location_id, ihme_loc_id, super_region_name, region_name, location_name, level)]

  ##############
  ## GET DATA ##
  ##############

  if (!is.na(crosswalk_version_id)) {

    data <- get_crosswalk_version(crosswalk_version_id)

  } else if (!is.na(path_to_data)) {

    data <- fread(path_to_data)

    crosswalk_data <- copy(data)

    data[, age_group_id := 22]
    data[, sex_id := 3]

    setnames(data, "mean_adj", "data")
    setnames(data, "sd_adj", "variance")

    data <- data[, .(nid, location_id, year_id, age_group_id, sex_id, data, variance)]

  } else {

    stop("ERROR: Must supply either a crosswalk version ID or path to data!")

  }

  if ("cv_custom_prior" %in% names(data)) {

    message("Dropping cv_custom_prior column")
    data[, cv_custom_prior := NULL]

  }

  # check data compatability w/ model
  if (nrow(data[is.na(data)]) > 0) {

    message("There are ", nrow(data[is.na(data)]), " rows with NA in data. These will be dropped!")
    data <- data[!is.na(data)]

  }

  if (length(unique(data$age_group_id)) <= 1 & any(grepl("age_group_id", c(fixed_covs, random_effects)))) {

    stop("`age_group_id` specified as a predictor, but the variable has fewer than 2 levels")

  }

  ####################
  ## GET COVARIATES ##
  ####################

  data_and_names <- bind_covariates(data, cov_list = cov_list, custom_cov_list = custom_covs, release_id = release_id)

  data <- data_and_names[[1]] # this is the data w/ bound covariate estimates
  cov_list <- data_and_names[[2]] # these are the covariate_name_shorts

  for (cov in cov_list) {

    if (nrow(data[is.na(get(cov)),]) > 0) {

      message("There are ",  nrow(data[is.na(get(cov)), ])," missing estimates for ", cov, ". These rows will be dropped!")
      Sys.sleep(5)
      data <- data[!is.na(get(cov))]

    }

  }

  for (cov in cov_list) {

    if (!cov %in% names(data)) stop(paste(cov, "missing from data... make sure that the covariate has estimates!"))

  }

  # create a column in the dataset that is saved in the 'polynoms' vector
  if (!is.null(polynoms)) {

    polys <- strsplit(polynoms, "\\^")
    polynoms.t <- list()

    for (i in 1:length(polys)) {

      basecov <- polys[[i]][1]

      if (!basecov %in% names(data)) stop("ATTEMPTING TO CREATE POLYNOMIAL, MISSING: ", polys[[i]][1])

      data[, paste0(basecov, polys[[i]][2]) == get(basecov)^as.numeric(polys[[i]][2])]
      polynoms.t[[i]] <- paste0(basecov, polys[[i]][2])

    }

    polynoms <- unlist(polynoms.t)

  }

  if ("SBA_coverage_prop" %in% cov_list) {

    message("taking logit of SBA_coverage_prop for modeling")
    data[SBA_coverage_prop == 1, SBA_coverage_prop := 0.9999999] # prevent Inf in logit space
    data[, SBA_coverage_prop := boot::logit(SBA_coverage_prop)] # convert to logit SBA for modeling

  }

  if ("IFD_coverage_prop" %in% cov_list) {

    message("taking logit of IFD_coverage_prop for modeling")
    data[IFD_coverage_prop == 1, IFD_coverage_prop := 0.9999999] # prevent Inf in logit space
    data[, IFD_coverage_prop := boot::logit(IFD_coverage_prop)] # convert to logit SBA for modeling

  }

  if ("ANC1_coverage_prop" %in% cov_list) {

    message("taking logit of ANC1_coverage_prop for modeling")
    data[ANC1_coverage_prop == 1, ANC1_coverage_prop := 0.9999999] # prevent Inf in logit space
    data[, ANC1_coverage_prop := boot::logit(ANC1_coverage_prop)] # convert to logit SBA for modeling

  }

  if ("ANC4_coverage_prop" %in% cov_list) {

    message("taking logit of ANC4_coverage_prop for modeling")
    data[ANC4_coverage_prop == 1, ANC4_coverage_prop := 0.9999999] # prevent Inf in logit space
    data[, ANC4_coverage_prop := boot::logit(ANC4_coverage_prop)] # convert to logit SBA for modeling

  }

  data <- merge(data, locs, by = "location_id")
  data <- offset.data(data, data_transform, offset) # offset function from st-gpr

  #####################################
  ## GET KOS AND SAVE TO TEMP FOLDER ##
  #####################################

  if (by_sex == T) {

    sex_list <- c("M", "F")
    if (!(1 %in% unique(data$sex_id))) sex_list <- c("F")

  } else {

    sex_list <- c("both_sexes")

  }

  for (sexchar in sex_list) {

    message("Prepping ", sexchar, " data for KO creation")

    if (sexchar == "M") sex <- 1
    if (sexchar == "F") sex <- 2
    if (sexchar == "both_sexes") sex <- c(1, 2, 3)

    data.s <- data[sex_id %in% sex, ]

    # set up data set if it's 'all ages'
    if (length(unique(data.s$age_group_id)) == 1) {

      if (unique(data.s$age_group_id) == 22 | unique(data.s$age_group_id) == 27) {

        data.s[, age_group_id := 22]
        by_age <- 0

      } else if (!(unique(data.s$age_group_id) %in% c(2:5, 388:389, 238, 34, 6:20, 30:32, 235, 164))) {

        stop("Error: look into incorporating custom age group")

      } else {

        by_age <- 1

      }

    } else {

      by_age <- 1
    }

    if (F) {

      remove_subnats <- T
      no_new_ages <- T
      only_data_locs <- F
      drop_nids <- T
      seed <- 10

    }

    ko_items <- prep_ko(data.s, location_set_id = location_set_id, by_age = by_age, by_sex = ifelse(by_sex, 1, 0), release_id = release_id)
    message("Done")

    message("Generating KOs")

    # generate knockouts -> the arguments are set up to take the output of the prep_ko function directly
    test <- get_kos(ko_items[[1]], ko_items[[2]], ko_items[[3]], ko_items[[4]], prop_to_hold = ko_prop, kos = kos,
                    seed = seed, no_new_ages = no_new_ages, only_data_locs = only_data_locs, drop_nids = drop_nids)
    message("Done")

    # writing the formatted dataset to avoid doing it for each child process
    saveRDS(test, file = paste0(output_folder, sexchar, "_full_data.rds"), version = 2)
    message("Saved ", sexchar, " prepped data to temp folder")

  }

  #############################
  ## GET ALL POSSIBLE MODELS ##
  #############################

  # check to make sure banned pairs are valid names
  invisible(lapply(unlist(ban_pairs), function(x) {

    if (!x %in% names(data)) message(x, " is specified as a banned pair, but it is not a valid covariate name short in the data!")

  }))

  # set up formula
  if (!is.null(random_effects)) {

    form <- paste0(data_transform, "(data)~", paste0(cov_list, collapse = "+"), "+",
                   paste0(polynoms, collapse = "+"), "+", paste0(fixed_covs, collapse = "+"), "+", paste0(random_effects, collapse = "+"))

  } else {

    form <- paste0(data_transform, "(data)~", paste0(cov_list, collapse = "+"), "+",
                   paste0(polynoms, collapse = "+"), "+", paste0(fixed_covs, collapse = "+"))

  }

  sub <- paste(unlist(lapply(ban_pairs, function(x) {

    thing <- c(rep(NA, times = length(x) - 1))

    for (i in 1:length(x) - 1) {

      if (i < length(x)) {

        bans <- c(rep(NA, times = length(x) - i))

        for (n in i + 1:length(x) - i) {

          bans[n - i] <- paste0("'", x[n], "'")

        }

        bans.t <- paste(bans, collapse = " | ")

        if (i < length(x) - 1) {

          thing[i] <- paste0("!('", x[i], "'  &&  (",  bans.t, "))")

        } else {

          thing[i] <- paste0("!('", x[i], "'  &&  ",  bans.t, ")")

        }

      }

    }

    return(paste(thing, collapse = " & "))

  })), collapse = "  &  ")

  message(paste("Getting formulas..."))
  message(paste("General formula:", form))
  message(paste("Banned sets:", sub))

  # add polynomials to cov_list:
  # save original cov list for prior_signs later first
  og_cov_list <- cov_list
  cov_list <- c(cov_list, polynoms)

  # function to remove ban pairs from a list of covariates
  remove.ban  <-  function(cov_list, ban) {
    out <- lapply(cov_list, function(covs) {
      if (length(covs) == 1 | (!all(sort(intersect(covs, ban)) == sort(ban))) | length(intersect(covs, ban)) == 0) return(covs)
    })

    return(out[!sapply(out, is.null)])

  }

  # create all combinations of covariates
  mod_list <- lapply(1:length(cov_list), function(x) combn(cov_list, x, simplify = FALSE)) %>% unlist(., recursive = F)

  if (!is.null(ban_pairs)) {

    # create combinations of ban sets
    keepers <- list()

    for (i in 1:length(ban_pairs)) {

      x <- ban_pairs[[i]]
      ban_list <- combn(x, 2, simplify = FALSE)

      # create lists for each ban set
      banned_lists <- lapply(ban_list, function(ban) remove.ban(mod_list, ban))

      # intersect them all
      keepers[[i]] <- Reduce(intersect, banned_lists)

    }

    temp_forms <- unlist(lapply(Reduce(intersect, keepers), function(x) { paste0(x, collapse = "+")}))

  } else {

    temp_forms <- unlist(lapply(mod_list, function(x) {paste0(x, collapse = "+")}))

  }

  # paste all pieces together
  if (!is.null(random_effects)) {

    if (!is.null(fixed_covs)) {

      forms.n <- paste(paste0(data_transform, "(data)"),
                       paste(temp_forms, paste0(fixed_covs, collapse = "+"), paste0(random_effects, collapse = "+"), sep = "+"), sep = "~")

      # add on the null mod
      null_mod <- paste(paste0(data_transform, "(data)"),
                        paste(paste0(fixed_covs, collapse = "+"), paste0(random_effects, collapse = "+"), sep = "+"), sep = "~")

    } else {

      forms.n <- paste(paste0(data_transform, "(data)"), paste(temp_forms, paste0(random_effects, collapse = "+"), sep = "+"), sep = "~")

      # add on the null mod
      null_mod <- paste(paste0(data_transform,"(data)"), paste(paste0(random_effects, collapse = "+"), sep = "+"), sep = "~")

    }

  } else {

    forms.n <- paste(paste0(data_transform, "(data)"), paste(temp_forms, paste0(fixed_covs, collapse = "+"), sep = "+"), sep = "~")

    # add on the null mod
    null_mod <- paste(paste0(data_transform, "(data)"), paste(paste0(fixed_covs, collapse = "+"), sep = "+"), sep = "~")

    if (!is.null(fixed_covs)) {

      forms.n <- paste(paste0(data_transform, "(data)"), paste(temp_forms, paste0(fixed_covs, collapse = "+"), sep = "+"), sep = "~")

      # add on the null mod
      null_mod <- paste(paste0(data_transform, "(data)"), paste(paste0(fixed_covs, collapse = "+"), sep = "+"), sep = "~")

    } else {

      forms.n <- paste(paste0(data_transform, "(data)"), paste(temp_forms, sep = "+"), sep = "~")

      # add on the null mod
      null_mod <- paste(paste0(data_transform, "(data)"), 1, sep = "~")

    }

  }

  forms <- c(null_mod, forms.n)
  message("Done getting formulas")

  if (count_mods == T) {

    message("Done--Returning ", length(forms), " formulas")

    if (by_sex == T) {

      message(paste("by_sex == T; real number of formulas to be evaluated is", length(forms) * 2))

    }

    return(forms)
    stop("Done")

  }

  if (by_sex == T) {

    message(paste(length(forms) * 2, "total formulas to evaluate"))

  } else {

    message(paste(length(forms), "total formulas to evaluate"))

  }

  saveRDS(forms, file = paste0(output_folder, "forms.rds"), version = 2)
  message(paste0("Model combinations saved to ", paste0(output_folder, "forms.rds")))

  #######################
  ## LAUNCH MODEL JOBS ##
  #######################

  file_list <- list()

  for (sexchar in sex_list) {

    # setup number of jobs to submit
    n <- length(forms)
    n_divisions <- forms_per_job

    # create start and end_ids
    seq <- data.table(start = seq(1, n, by = n_divisions))
    seq[, end := shift(start, type = "lead") - 1]
    seq[nrow(seq), end := n]
    seq[, id := seq(.N)]

    nsubs <- max(seq$id)
    message("Launching ", nsubs, " splitting jobs for sex: ", sexchar)

    data_path <- paste0(output_folder, sexchar, "_full_data.rds")
    forms_path <- paste0(output_folder, "forms.rds")

    for (i in 1:max(seq$id)) {

      if (F) {

        i <- 16
        sexchar <- "both_sexes"
        data_transform <- "log"

      }

      # get start and end forms
      date <- gsub("-", "_", Sys.Date())
      start <- seq[id == i, start]
      end <- seq[id == i, end]

      command <- paste0("sbatch -p all.q -t 00:15:00 --mem=", m_mem_free, "G -c 2 ", "FILEPATH")
      system(command)

    }

  }

  # job hold
  message("Waiting on jobs...")
  job_hold("oos_")
  message("Finished model testing")

  ##############################
  ## READ IN RESULTS AND RANK ##
  ##############################

  rmse_files <- list.files(path = output_folder, pattern = ".csv", full.names = T)

  file_length <- ifelse(by_sex & c(1, 2) %in% unique(data$sex_id), 2 * length(forms), length(forms))
  if (length(rmse_files) != file_length[1]) stop(paste0(file_length[1] - length(rmse_files), " model results are missing! Check error logs; jobs may have broken"))

  ## Read in the results
  message("Reading in results and ranking models")
  stack <- list()

  if (length(rmse_files > 0)) {

    for (i in 1:length(rmse_files)) {

      stack[[i]] <- fread(rmse_files[i])

    }

  } else {

    stop("All jobs broken")

  }

  rmses <- rbindlist(stack, fill = T)

  ## Rank Models
  if (rank_method == "oos.rmse") {

    # sort by oos rmse
    rmses <- setorder(rmses, out_rmse)

  }

  if (rank_method == "aic") {

    rmses <- setorder(rmses, aic)

  }

  ## Subset models that violate prior signs

  rmses <- restrict_violations(rmses = rmses, prior_sign = prior_sign, covs = cov_list, p_value = p_value)

  ######################
  ## FINAL PROCESSING ##
  ######################

  message("Cleaning out temp folder...")
  unlink(output_folder, recursive = T)

  message("Done")
  return(list(rmses, data, crosswalk_data))

}
