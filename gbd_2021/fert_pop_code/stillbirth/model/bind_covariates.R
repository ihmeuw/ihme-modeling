#################################################################################
##                                                                             ##
## Purpose: Load in and bind covariates for use in first-stage ensemble models ##
##                                                                             ##
#################################################################################

library(assertable)
library(data.table)
library(dplyr)

####################
## SOURCE SCRIPTS ##
####################

central <- "FILEPATH"

source(paste0(central, "get_covariate_estimates.R"))
source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "get_population.R"))
source(paste0(central, "get_ids.R"))

##############################
## BIND COVARIATES FUNCTION ##
##############################

bind_covariates <- function(df, cov_list, custom_cov_list = NULL, decomp_step = "iterative") {

  #################
  ## DATA CHECKS ##
  #################

  cols <- c("sex_id", "year_id")

  invisible(lapply(cols, function(x) {

    if (!x %in% names(df)) stop(paste0("Your data is missing a column named ", x, "!"))

  }))

  if (!"location_id" %in% names(df) & "ihme_loc_id" %in% names(df)) {

    message("Missing location_id, but location_set_id = 22 is being applied.")
    locs <- get_location_metadata(location_set_id = 22)[, .(ihme_loc_id, location_id)]
    df <- merge(df, locs, by = "ihme_loc_id")

  }

  if (!"location_id" %in% names(df) & !"ihme_loc_id" %in% names(df)) {

    message("You're missing location_id AND ihme_loc_id! You need at least one!")

  }

  if (any(df[["age_group_id"]] == 27)) stop("You have age_group_id 27 in your data. Currently, you're not aggregating covariates for age-standardized data.")

  # for custom covariates
  cust_covs <- unlist(lapply(custom_cov_list, "[", 1))
  cust_covs_paths <- unlist(lapply(custom_cov_list, "[", 2))
  cov_list <- c(cov_list, cust_covs)

  ################
  ## COVARIATES ##
  ################

  covariates <- get_ids(table = "covariate")
  cids <- covariates[covariate_name %in% cov_list | covariate_name_short %in% cov_list, covariate_id]

  covs              <- list()
  multi_age_covs    <- list()
  multi_sex_covs    <- list()
  multi_agesex_covs <- list()
  cov_name_short    <- list()

  for (i in 1:length(cov_list)) {

    if (cov_list[i] %in% cust_covs) { ## get covariate from custom location

      path <- cust_covs_paths[match(cov_list[i], cust_covs)]

      message(paste("Getting", cov_list[i], "from", path))

      ## make sure custom covariate is in right format
      if (grepl(".csv", path)) {

        nec_cols <- c("covariate_name_short", "location_id", "age_group_id", "sex_id", "year_id", "mean_value")
        x <- fread(path)

        ## this lapply makes sure that the columns in nec_cols exist in the df
        lapply(nec_cols, FUN = function(y) {

          if (!y %in% names(x)) {

            stop(paste("Missing a necessary column!:", y))

          }

        })

      } else {

        message("Specified path is not a .csv!")

      }

    } else { ## get covariate from database

      cid <- covariates[covariate_name == cov_list[i], covariate_id]
      message(paste("Getting", cov_list[i], "from covariate database"))

      x <- get_covariate_estimates(covariate_id = cid,
                                   location_id = unique(df$location_id),
                                   year_id = unique(df$year_id),
                                   gbd_round_id = gbd_round_id,
                                   decomp_step = decomp_step)
    }

    x <- x[, .(covariate_name_short, location_id, year_id, age_group_id, sex_id, mean_value)]
    ages <- unique(x$age_group_id)
    years <- unique(x$year_id)
    sexes <- unique(x$sex_id)
    cov_name_short[[i]] <- unique(x$covariate_name_short)

    if (length(ages) > 1) { ## if multiple age groups estimated

      message(paste("  Multiple age groups estimated for", cov_list[i]))

      if (length(sexes) > 1) { ## if multiple age groups estimated and multiple sex groups estimated

        message(paste("  Multiple sexes estimated for", cov_list[i]))

        x <- x[, .(covariate_name_short, location_id, sex_id, age_group_id, year_id, mean_value)]
        multi_agesex_covs[[i]] <- x

      } else {  ## if multiple age groups estimated and only 1 sex group

        x <- x[, .(covariate_name_short, location_id, age_group_id, year_id, mean_value)]
        multi_age_covs[[i]] <- x

      }
    }

    if (length(sexes) > 1 & length(ages) == 1) { ## if multiple sex groups estimated and only 1 age group

      message(paste("  Multiple sexes estimated for", cov_list[i]))
      x <- x[, .(covariate_name_short, location_id, sex_id, year_id, mean_value)]

      multi_sex_covs[[i]] <- x

    }

    if (length(sexes) == 1 & length(ages) == 1) { ## if only 1 age group and only 1 sex group is estimated

      x <- x[, .(covariate_name_short, location_id, year_id, mean_value)] ## if only 1 age group and only 1 sex group estimated

      miss_locs <- unique(df$location_id)[!unique(df$location_id) %in% unique(x$location_id)]

      if (length(miss_locs > 0)) {

        message("  You have the following location_ids in your data that are missing from the ", cov_list[i], " covariate: ", paste0(miss_locs, collapse = ", "))

      }

      covs[[i]] <- x

    }

  }

  #########################################
  ## AGGREGATE COVS FOR AGGREGATED DATA  ##
  #########################################

  if (("age_start" %in% names(df) & "age_end" %in% names(df)) || 3 %in% unique(df$sex_id)) {

    if (length(multi_age_covs) != 0 | length(multi_agesex_covs) > 0 | length(multi_sex_covs) != 0) {

      #########################
      ## AGGREGATE COVARIATE ##
      #########################

      agg <- function(agg_covs, merge_vars, check_vars) {

        new_est <- list()

        for (u in 1:length(agg_covs)) {

          temp_cov <- agg_covs[[u]]
          temp_cov <- temp_cov[sex_id == 2 & age_group_id %in% 7:15] # subset to maternal age groups

          covar <- unique(temp_cov[!is.na(covariate_name_short), covariate_name_short])

          temp_cov <- merge(temp_cov, pops, by = c("location_id", "year_id", "sex_id", "age_group_id"), all.x = T)
          assertable::assert_values(temp_cov, c("mean_value", "population"), test = "not_na")

          ## aggregate, weighted by population size
          temp_cov[, weighted_mean_value := weighted.mean(mean_value, population, na.rm = T), by = c("location_id", "year_id")]

          temp_cov[, covariate_name_short := covar]

          temp_cov <- unique(temp_cov[, c("covariate_name_short", "location_id", "year_id", "weighted_mean_value")])
          setnames(temp_cov, "weighted_mean_value", "mean_value")

          new_est[[u]] <- copy(temp_cov)

        }

        return(new_est)

      }

      ## do some quick checks to make sure there are no NAs before trying get_populations
      assertable::assert_values(df, c("location_id", "age_group_id", "year_id"), test = "not_na")

      message("    Getting populations for aggregation to maternal age group...")
      pops <- get_population(location_id = unique(df$location_id),
                             age_group_id = c(7:15, 22),
                             year_id = unique(df$year_id),
                             sex_id = 2,
                             gbd_round_id = gbd_round_id,
                             decomp_step = decomp_step)

      message("    Done getting populations")

      agg_list <- list()

      if (("age_start" %in% names(df) & "age_end" %in% names(df)) || 3 %in% unique(df$sex_id) && length(multi_agesex_covs) > 0) {

        message("  Aggregating age-sex specific covariates to merge on to data")
        multi_agesex_covs <- multi_agesex_covs[!sapply(multi_agesex_covs, is.null)]

        masc <- agg(multi_agesex_covs, c("location_id", "year_id", "age_group_id", "sex_id"), check_vars = c("location_id", "year_id", "age_group_id"))
        multi_agesex_covs <- list() ## empty this list
        agg_list <- append(agg_list, masc)

      }

      covs <- c(covs, agg_list)

    }

  }

  #############################
  ## RESHAPE COVARIATE LISTS ##
  #############################

  if (length(covs) > 0) {

    covs <- rbindlist(covs)
    covs_cast <- dcast(covs, location_id + year_id ~ covariate_name_short, value.var = "mean_value")

  }

  ################################
  ## MERGE COVARIATES ONTO DATA ##
  ################################

  rws <- nrow(df)
  cov_name_short <- unlist(cov_name_short)
  binder <- copy(df)

  message("Binding")

  if ("covs_cast" %in% ls()) {

    binder <- merge(binder, covs_cast, by = c("location_id", "year_id"))

  }

  if (nrow(binder) != rws) message("Row number change after merging covariates...this probably means you have age-sex-location-years in your data that aren't in the covariate")

  if (any(is.na(binder[, c(cov_name_short), with = F]))) {

    message("Some covariates have missing values after binding...this probably means you have age-sex-location-years in your data that aren't in the covariate")

  }

  print("Done binding")

  return(list(binder, cov_name_short))

}
