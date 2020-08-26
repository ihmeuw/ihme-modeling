## Generate draws of regression coefficients to determine how much to adjust age-specific qx values if there is
## a difference between the target qx and the qx of the standard life table, in the model life table process
## Coefficients are determined by matching up empirical life tables and running age-specific regressions to determine adjustment factors

library(data.table)
library(MASS)
library(ggplot2)
library(ggthemes)

library(mortdb, lib = "FILEPATH/r-pkg")
library(ltcore, lib = "FILEPATH/r-pkg")

## define function to estimate MLT parameters
## ============================================================================
gen_modelpar_sim <- function(elt_version, mlt_version){

  ## High-level settings and maps
  master_ids <- c("location_id", "sex_id", "source_type_id")
  loc_map <- get_locations(level = "all")
  age_map <- get_age_map("lifetable")

  ## Download life table data
  master_lts <- get_mort_outputs("life table empirical", "data", gbd_year = 2019, run_id = elt_version)

  ## Only keep non-outliered data
  master_lts <- master_lts[outlier_type_id == 1]

  ## Only keep qx
  master_lts <- master_lts[life_table_parameter_id == 3]

  ## Universal LTs only
  master_lts <- master_lts[life_table_category_id == 1]

  ## Save a dataset to use to generate region-mean LTs later
  region_lts <- copy(master_lts)

  ## logit transformation
  master_lts[, mean := logit_qx(mean)]

  ## Separate and merge 5q0 and 45q15 onto the dataset
  master_5q0 <- master_lts[age_group_id == 1]
  setnames(master_5q0, "mean", "qx_5q0")
  master_45q15 <- master_lts[age_group_id == 199]
  setnames(master_45q15, "mean", "qx_45q15")
  master_lts <- merge(master_lts[!age_group_id %in% c(1, 199), .SD,
                      .SDcols = c(master_ids, "life_table_category_id", "year_id", "age_group_id", "mean")],
                      master_5q0[, .SD, .SDcols = c(master_ids, "life_table_category_id", "year_id", "qx_5q0")],
                      by = c(master_ids, "life_table_category_id", "year_id"))
  master_lts <- merge(master_lts,
                      master_45q15[, .SD, .SDcols = c(master_ids, "life_table_category_id", "year_id", "qx_45q15")],
                      by = c(master_ids, "life_table_category_id", "year_id"))
  setnames(master_lts, "mean", "qx")

  ## Here, generate a separate dataset of the "standard" life tables, to merge onto the original empirical
  ## life table datasets for the matching aspect of the model
  setkey(master_lts, location_id, sex_id, source_type_id, year_id)
  stan_lts <- copy(master_lts)
  setnames(stan_lts,
           c("year_id", "qx", "qx_5q0", "qx_45q15"),
           c("stan_year_id", "stan_qx", "stan_5q0", "stan_45q15"))
  setkey(stan_lts, location_id, sex_id, source_type_id, stan_year_id)
  stan_merge_vars <- c(master_ids, "age_group_id", "stan_year_id", "stan_qx", "stan_5q0", "stan_45q15")

  location_source_matched_lts <- list()
  i <- 1

  ## Within a location/source/sex, match life tables together if within a 15-year band
  for(location in unique(master_lts$location_id)) {
    print(location)
    source_types <- unique(master_lts[location_id == location, source_type_id])
    for(source_type in source_types) {
      sexes <- unique(master_lts[location_id == location & source_type_id == source_type, sex_id])
      for(sex in sexes) {
        years <- unique(master_lts[location_id == location & source_type_id == source_type & sex_id == sex, year_id])
        if(length(years) >= 2) {
          for(year in years) {
            ## Here, we only want standard LTs that are 15 years after the observation we're matching to, up to the date of the year itself.
            ## Essentially, we don't want any stan LTs from the future or too far in the past.
            location_source_matched_lts[[i]] <- merge(master_lts[location_id == location & source_type_id == source_type & sex_id == sex & year_id == year],
                                                      stan_lts[location_id == location & source_type_id == source_type & (year - stan_year_id <= 15) & (year - stan_year_id > 0),
                                                               .SD, .SDcols = stan_merge_vars],
                                                      by = c(master_ids, "age_group_id"))
            i <- i + 1
          }
        }
      }
    }
  }

  location_source_matched_lts <- rbindlist(location_source_matched_lts)

  ## Get logit diff of 5q0, 45q15, and age-specific
  location_source_matched_lts[, diff_qx := qx - stan_qx]
  location_source_matched_lts[, diff_5q0 := qx_5q0 - stan_5q0]
  location_source_matched_lts[, diff_45q15 := qx_45q15 - stan_45q15]

  ## Generate region-level mean values of qx, and then do the same matching process to empirical LT observations
  ## By sex, get mean of lx values by gbd region and age
  ## Use mean of lx values by gbd region and age to generate full qx and 5q0/45q15 "standard" set
  region_agg_id_vars <- c("sex_id", "region_id")
  region_lts <- merge(region_lts[!age_group_id %in% c(1, 199)], loc_map[, .(location_id, region_id)], by = "location_id")
  region_lts <- region_lts[, .(qx = mean(mean)),
                           by = c("sex_id", "region_id", "age_group_id")]

  ## Calculate 5q0 and 45q15
  region_5q0 <- calc_qx(region_lts, age_start = 0, age_end = 5, id_vars = region_agg_id_vars)
  region_45q15 <- calc_qx(region_lts, age_start = 15, age_end = 60, id_vars = region_agg_id_vars)

  region_lts <- merge(region_lts, region_5q0, by = region_agg_id_vars)
  region_lts <- merge(region_lts, region_45q15, by = region_agg_id_vars)

  ## Convert LTs to logit space
  region_lts[, stan_qx := logit_qx(qx)]
  region_lts[, stan_5q0 := logit_qx(qx_5q0)]
  region_lts[, stan_45q15 := logit_qx(qx_45q15)]

  region_lts[, (c("qx", "qx_5q0", "qx_45q15")) := NULL]

  master_lts_region_merge <- merge(master_lts, loc_map[, .(location_id, region_id)], by = "location_id")
  master_lts_region_merge <- merge(master_lts_region_merge, region_lts,
                                   by = c(region_agg_id_vars, "age_group_id"))

  master_lts_region_merge[, diff_qx := qx - stan_qx]
  master_lts_region_merge[, diff_5q0 := qx_5q0 - stan_5q0]
  master_lts_region_merge[, diff_45q15 := qx_45q15 - stan_45q15]

  ## Append the empirical-standard matches and the empirical-region matches together, for a combined regression
  combined_lts <- rbindlist(list(location_source_matched_lts, master_lts_region_merge),
                            use.names = T, fill = T)

  ## Regression between the age-specific difflogit and difflogit5q0, for ages 0-5.
  ## Regression between the age-specific difflogit and the difflogit5q0 and 45q15 for ages 10-30
  ## Regression between age-specific difflogit and difflogit45q15 for ages 35-100
  ## Regression setup: separate regression by age, and predict 1000 draws directly from mean and covariance matrix
  ## Get draws of everything (same seed for each draw generation)
  gen_regression_draws <- function(age_id, sex) {
    regress_data <- combined_lts[age_group_id == age_id & sex_id == sex]
    start_formula <- "diff_qx ~ 0"
    if(age_id %in% c(28, 5, 6)) {
      reg_formula <- as.formula(paste(start_formula, "+ diff_5q0"))
    } else if(age_id >= 7 & age_id <= 8) {
      reg_formula <- as.formula(paste(start_formula, "+ diff_5q0 + diff_45q15"))
    } else if(age_id >= 9 & age_id != 28) {
      reg_formula <- as.formula(paste(start_formula, "+ diff_45q15"))
    }

    ## Run regression
    mod_results <- lm(data = regress_data,
                      formula = reg_formula)

    if(sex == 1) sex_formatted <- "male"
    if(sex == 2) sex_formatted <- "female"

    if(age_id %in% c(28, 5:8)) {
      plot_5q0 <- ggplot(regress_data) +
        geom_point(aes(x = diff_qx, y = diff_5q0)) +
        ggtitle(paste0(age_map[age_group_id == age_id, age_group_name], " ", sex_formatted, " logit diff qx vs. logit diff 5q0")) +
        theme_minimal()
    } else {
      plot_5q0 <- NULL
    }

    if(age_id >= 7 & age_id != 28) {
      plot_45q15 <- ggplot(regress_data) +
        geom_point(aes(x = diff_qx, y = diff_45q15)) +
        ggtitle(paste0(age_map[age_group_id == age_id, age_group_name], " ", sex_formatted, " logit diff qx vs. logit diff 45q15")) +
        theme_minimal()
    } else {
      plot_45q15 <- NULL
    }

    ## Predict draws
    ## The draws are draws of the coefficient values of simdifflogit45q15 and simdifflogit5q0
    set.seed(1234567)
    regression_draws <- mvrnorm(n = 1000, mu = coef(mod_results), Sigma = vcov(mod_results))
    regression_draws <- setDT(data.frame(regression_draws))
    regression_draws[, sim := .I - 1]

    ## Return formatted dataset
    regression_draws[, age_group_id := age_id]
    regression_draws[, sex_id := sex]
    if(age_id %in% c(28, 5, 6)) {
      regression_draws[, diff_45q15 := 0]
    } else if(age_id >= 9 & age_id != 28) {
      regression_draws[, diff_5q0 := 0]
    }
    return(list(regression_draws, plot_5q0, plot_45q15))
  }

  male_regression_results <- lapply(unique(combined_lts$age_group_id), gen_regression_draws, sex = 1)
  male_regression_draws <- rbindlist(lapply(male_regression_results, function(x) return(x[[1]])),
                                     use.names = T)

  female_regression_results <- lapply(unique(combined_lts$age_group_id), gen_regression_draws, sex = 2)
  female_regression_draws <- rbindlist(lapply(female_regression_results, function(x) return(x[[1]])),
                                       use.names = T)

  combined_draws <- rbindlist(list(male_regression_draws, female_regression_draws),
                              use.names = T)

  setnames(combined_draws,
           c("diff_45q15", "diff_5q0"),
           c("sim_difflogit45", "sim_difflogit5"))

  ## Print diagnostic PDFs
  pdf(paste0("FILEPATH",mlt_version,"/diagnostics/difflogit_explore.pdf"), width=7.5, height=5)
  lapply(male_regression_results, function(x) if(!is.null(x[[2]])) x[[2]])
  lapply(male_regression_results, function(x) if(!is.null(x[[3]])) x[[3]])
  lapply(female_regression_results, function(x) if(!is.null(x[[2]])) x[[2]])
  lapply(female_regression_results, function(x) if(!is.null(x[[3]])) x[[3]])
  dev.off()

  return(combined_draws)

} # end gen_modelpar_sim function

## END
