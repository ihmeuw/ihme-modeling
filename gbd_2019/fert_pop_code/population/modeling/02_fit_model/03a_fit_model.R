################################################################################
# Description: Fit the population model for a given location and drop age
# - combine all demographic and population matrices and other information into
#   TMB data and parameter list
# - create map object to optionally fix different demographic components
# - fit model or load previously fit model
# - save tmb model fit object
# - extract model fit results
# - run prior and posterior ccmpp
# - split to get under 1 populations using gbd under5 mortality proportions
# - save outputs
# - calculate absolute and error and save
################################################################################

library(data.table)
library(readr)
library(boot)
library(TMB)
library(optimx, lib.loc = "FILEPATH/r-pkg")
library(ini, lib.loc = "FILEPATH/r-pkg")
library(assertable, lib.loc = "FILEPATH/r-pkg")
library(mortdb, lib = "FILEPATH/r-pkg")
library(mortcore, lib = "FILEPATH/r-pkg")

rm(list = ls())
SGE_TASK_ID <- Sys.getenv("SGE_TASK_ID")
OMP_NUM_THREADS <- Sys.getenv("OMP_NUM_THREADS")
Sys.umask(mode = "0002")


# Get arguments -----------------------------------------------------------

# load step specific settings into Global Environment
parser <- argparse::ArgumentParser()
parser$add_argument("--pop_vid", type = "character",
                    help = "The version number for this run of population, used to read in settings file")
parser$add_argument('--task_map_dir', type="character",
                    help="The filepath to the task map file that specifies other arguments to run for")
parser$add_argument("--test", type = "character",
                    help = "Whether this is a test run of the process")
parser$add_argument("--USER", type = "character",
                    help = "User to access appropriate code directory")
args <- parser$parse_args()
if (interactive()) { # set interactive defaults, should never commit changes away from the test version to be safe
  args$pop_vid <- "99999"
  args$task_map_dir <- "FILEPATH"
  args$test <- "T"
  args$USER <- Sys.getenv("USER")
}
args$test <- as.logical(args$test)
list2env(args, .GlobalEnv); rm(args)

code_dir <- paste0("FILEPATH", "/population/modeling/popReconstruct/")

# use task id to get arguments from task map
task_map <- fread(task_map_dir)
loc_id <- task_map[task_id == SGE_TASK_ID, location_id]
drop_above_age <- task_map[task_id == SGE_TASK_ID, drop_above_age]
copy_results <- task_map[task_id == SGE_TASK_ID, as.logical(copy_results)]

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", pop_vid, "/run_settings.rdata")
load(settings_dir)
list2env(settings, envir = environment())

source(paste0(code_dir, "functions/formatting.R"))

# read in population modeling location hierarchy
location_hierarchy <- fread(paste0(output_dir, "/database/all_location_hierarchies.csv"))
location_hierarchy <- location_hierarchy[location_set_name == "population" & location_id == loc_id]
ihme_loc <- location_hierarchy[, ihme_loc_id]
super_region <- location_hierarchy[, super_region_name]

# read in census specific settings
census_specific_settings <- fread(paste0(output_dir, "/database/census_specific_settings.csv"))[ihme_loc_id == ihme_loc]

# read in location specific settings and set defaults if they are not specified
location_specific_settings <- fread(paste0(output_dir, "/database/location_specific_settings.csv"))[ihme_loc_id == ihme_loc]
inflate_young_old_sd_scalar <- ifelse(is.na(location_specific_settings$inflate_young_old_sd_scalar), 3,
                                      as.numeric(location_specific_settings$inflate_young_old_sd_scalar))
inflate_baseline_sd_scalar <- ifelse(is.na(location_specific_settings$inflate_baseline_sd_scalar), 2,
                                     as.numeric(location_specific_settings$inflate_baseline_sd_scalar))

default_region_specific_sd <- list("High-income" = 0.01,
                                   "Central Europe, Eastern Europe, and Central Asia" = 0.03,
                                   "Southeast Asia, East Asia, and Oceania" = 0.05,
                                   "Latin America and Caribbean" = 0.05,
                                   "South Asia" = 0.05,
                                   "North Africa and Middle East" = 0.05,
                                   "Sub-Saharan Africa" = 0.05)
census_default_sd <- ifelse(is.na(location_specific_settings$census_default_sd), default_region_specific_sd[[super_region]],
                            as.numeric(location_specific_settings$census_default_sd))
default_sd <- census_default_sd

# define scalars to multiply sd by dependent on age start of census data point
sd_age_scalar <- CJ(age_middle_floor = 0:terminal_age, sd_age_scalar = 1)
sd_age_scalar[age_middle_floor <= 5, sd_age_scalar := seq(inflate_young_old_sd_scalar, 1, length.out = 6)]
sd_age_scalar[age_middle_floor >= 50, sd_age_scalar := seq(1, inflate_young_old_sd_scalar, length.out = 46)]

# read in age group
age_groups <- fread(paste0(output_dir, "/database/age_groups.csv"))
age_groups_needed <- age_groups[if(age_int == 1) single_year_model else single_year_model,
                                list(age_group_id, age_group_years_start, age_group_years_end)]
fertility_age_groups_needed <- age_groups[(single_year_model), list(age_group_id, age_group_years_start, age_group_years_end)]
survival_cohort_age_groups_needed <- age_groups[(if(age_int == 1) single_year_model else single_year_model) | age_group_id == 164,
                                                list(age_group_id, age_group_years_start, age_group_years_end)]
migration_cohort_age_groups_needed <- age_groups[(if(age_int == 1) single_year_model else five_year_model) | age_group_id == 164,
                                                 list(age_group_id, age_group_years_start, age_group_years_end)]
setkeyv(age_groups_needed, c("age_group_years_start"))
setkeyv(fertility_age_groups_needed, c("age_group_years_start"))
setkeyv(migration_cohort_age_groups_needed, c("age_group_years_start"))
setkeyv(survival_cohort_age_groups_needed, c("age_group_years_start"))

ccmp_age_int <- 1

# actually fit the population model
if (!copy_results) {

  # Load demographic input data ---------------------------------------------

  # load data
  asfr_data <- fread(file = paste0(output_dir, "/inputs/asfr.csv"))[location_id == loc_id]
  srb_data <- fread(file = paste0(output_dir, "/inputs/srb.csv"))[location_id == loc_id]
  survival_data <- fread(file = paste0(output_dir, "/inputs/survival.csv"))[location_id == loc_id]
  migration_data <- fread(file = paste0(output_dir, "/inputs/migration.csv"))[location_id == loc_id]

  # convert to matrices for TMB
  asfr_matrix <- format_as_matrix(asfr_data, "mean", ages = fertility_age_groups_needed$age_group_years_start)
  srb_matrix <- matrix(srb_data$mean, ncol = length(srb_data$year_id))
  colnames(srb_matrix) <- srb_data$year_id
  survival_matrix <- format_as_matrix_by_sex(survival_data, "mean", ages = survival_cohort_age_groups_needed$age_group_years_start)
  migration_matrix <- format_as_matrix_by_sex(migration_data, "mean", ages = migration_cohort_age_groups_needed$age)


  # Load population input data ----------------------------------------------

  # load data
  census_data <- fread(file = paste0(output_dir, "/inputs/population.csv"))[location_id == loc_id]
  # drop age groups above the modeling pooling age except for the baseline year
  census_data[year_id != year_start & age_group_years_start > drop_above_age, drop := T]

  baseline_matrix <- format_value_matrix(census_data[year_id == year_start & outlier_type == "not outliered"],
                                         use_age_int = age_int, use_terminal_age = terminal_age,
                                         id_vars = c(census_data_id_vars, "sex_id"), baseline = T)
  census_matrix <- format_value_matrix(census_data[outlier_type == "not outliered" & year_id > year_start & !drop],
                                       use_age_int = age_int, use_terminal_age = terminal_age,
                                       id_vars = c(census_data_id_vars, "sex_id"))
  census_ages_matrix <- format_row_matrix(census_data[outlier_type == "not outliered" & year_id > year_start & !drop],
                                          use_age_int = age_int, use_terminal_age = terminal_age,
                                          id_vars = c(census_data_id_vars, "sex_id"))


  # Fit the TMB model -------------------------------------------------------

  sink(file = paste0(output_dir, loc_id, "/outputs/model_fit/fit_drop", drop_above_age, "_diagnostics.txt"))

  # get the columns of the full population projection matrices that correspond to the census years
  determine_census_years <- function(census_years, full_projection_years) {
    col_census_year <- sapply(census_years, function(y) which(full_projection_years == y) - 1)
  }

  row_fert_start <- min(which(rowSums(asfr_matrix) > 0))
  row_fert_end <- max(which(rowSums(asfr_matrix) > 0))
  tmb_data <- list(
    input_n_female = census_matrix$female,
    input_n_male = census_matrix$male,
    input_n_ages_female = census_ages_matrix$female,
    input_n_ages_male = census_ages_matrix$male,

    input_n0_female = baseline_matrix$female,
    input_n0_male = baseline_matrix$male,
    input_logit_s_female = boot::logit(survival_matrix$female),
    input_logit_s_male = boot::logit(survival_matrix$male),
    input_g_female = migration_matrix$female,
    input_g_male = migration_matrix$male,
    input_log_f = log(asfr_matrix[row_fert_start:row_fert_end, ]),
    input_log_srb = log(srb_matrix),

    age_int = ccmp_age_int,
    row_fert_start = row_fert_start - 1,
    row_fert_end = row_fert_end - 1,
    col_census_year = determine_census_years(year_start + unique(census_ages_matrix$male[, 1]), year_start:year_end),

    mu_s = -5,
    sig_s = 3,
    mu_g = -5,
    sig_g = 3,
    mu_f = -5,
    sig_f = 3,
    mu_srb = -5,
    sig_srb = 3,

    rho_g_t_mu = ifelse(is.na(location_specific_settings$migration_t_mu), 3,
                        location_specific_settings$migration_t_mu),
    rho_g_t_sig = ifelse(is.na(location_specific_settings$migration_t_sd), 0.1,
                         location_specific_settings$migration_t_sd),
    rho_g_a_mu = ifelse(is.na(location_specific_settings$migration_a_mu), -3,
                        location_specific_settings$migration_a_mu),
    rho_g_a_sig = ifelse(is.na(location_specific_settings$migration_a_sd), 0.1,
                         location_specific_settings$migration_a_sd)
  )

  tmb_par <- list(

    log_n0_female = log(tmb_data$input_n0_female[, 1, drop = F]),
    log_n0_male = log(tmb_data$input_n0_male[, 1, drop = F]),

    logit_s_female = tmb_data$input_logit_s_female,
    logit_s_male = tmb_data$input_logit_s_male,
    log_sigma_s = log(default_sd),

    g_female = tmb_data$input_g_female,
    g_male = tmb_data$input_g_male,
    log_sigma_g = log(default_sd),
    logit_rho_g_t = 0,
    logit_rho_g_a = 0,

    log_f = tmb_data$input_log_f,
    log_sigma_f = log(default_sd),

    log_srb = tmb_data$input_log_srb,
    log_sigma_srb = log(default_sd)
  )

  # check if fixing certain parameters
  map <- NULL
  if (fix_baseline) {
    map$log_n0_female <- rep(factor(NA), nrow(tmb_par$log_n0_female) * ncol(tmb_par$log_n0_female))
    dim(map$log_n0_female) <- c(nrow(tmb_par$log_n0_female), ncol(tmb_par$log_n0_female))
    map$log_n0_male <- rep(factor(NA), nrow(tmb_par$log_n0_male) * ncol(tmb_par$log_n0_male))
    dim(map$log_n0_male) <- c(nrow(tmb_par$log_n0_male), ncol(tmb_par$log_n0_male))
  }
  if (fix_survival) {
    map$logit_s_female <- rep(factor(NA), nrow(tmb_par$logit_s_female) * ncol(tmb_par$logit_s_female))
    dim(map$logit_s_female) <- c(nrow(tmb_par$logit_s_female), ncol(tmb_par$logit_s_female))
    map$logit_s_male <- rep(factor(NA), nrow(tmb_par$logit_s_male) * ncol(tmb_par$logit_s_male))
    dim(map$logit_s_male) <- c(nrow(tmb_par$logit_s_male), ncol(tmb_par$logit_s_male))

    map$log_sigma_s <- factor(NA)
  }
  if (fix_migration) {
    map$g_female <- rep(factor(NA), nrow(tmb_par$g_female) * ncol(tmb_par$g_female))
    dim(map$g_female) <- c(nrow(tmb_par$g_female), ncol(tmb_par$g_female))
    map$g_male <- rep(factor(NA), nrow(tmb_par$g_male) * ncol(tmb_par$g_male))
    dim(map$g_male) <- c(nrow(tmb_par$g_male), ncol(tmb_par$g_male))

    map$log_sigma_g <- factor(NA)
    map$logit_rho_g_t <- factor(NA)
    map$logit_rho_g_a <- factor(NA)
  }
  if (fix_fertility) {
    map$log_f <- rep(factor(NA), nrow(tmb_par$log_f) * ncol(tmb_par$log_f))
    dim(map$log_f) <- c(nrow(tmb_par$log_f), ncol(tmb_par$log_f))

    map$log_sigma_f <- factor(NA)
  }
  if (fix_srb) {
    map$log_srb <- rep(factor(NA), nrow(tmb_par$log_srb) * ncol(tmb_par$log_srb))
    dim(map$log_srb) <- c(nrow(tmb_par$log_srb), ncol(tmb_par$log_srb))

    map$log_sigma_srb <- factor(NA)
  }

  TMB::openmp(OMP_NUM_THREADS)
  TMB::compile(paste0(code_dir, "tmb/pop_model.cpp"))  # Compile the C++ file
  dyn.load(dynlib(paste0(code_dir, "tmb/pop_model")))  # Dynamically link the C++ code

  TMB::config(tape.parallel=0, DLL="pop_model")

  # make objective function
  cat("\n\n***** Make objective function\n"); flush.console()
  obj <- TMB::MakeADFun(tmb_data, tmb_par,
                        random=c("log_n0_female", "log_n0_male", "logit_s_female", "logit_s_male", "g_female", "g_male", "log_f", "log_srb"),
                        map=map, DLL="pop_model")

  # optimize objective function
  cat("\n\n***** Optimize objective function\n"); flush.console()
  opt_time <- proc.time()

  # first try nlminb optimizer since most locations fit with this and is a commonly used optimzer
  if (!loc_id %in% c(69)) {
    opt <- try(nlminb(start=obj$par, objective=obj$fn, gradient=obj$gr, control=list(eval.max=1e5, iter.max=1e5)))
    if (class(opt) != "try-error") opt$convcod <- opt$convergence else opt$convcod <- 1
  } else {
    opt <- NULL
    opt$convcod <- 1
  }

  # if still failing try some other optimizers
  if (all(opt$convcod != 0)) {
    for (method in c("L-BFGS-B", "BFGS", "CG", "nlm", "newuoa", "bobyqa", "nmkb")) {
      try({
        cat(paste("\n  *** Method: ", method, "\n"))

        # set this up so the break statement doesn"t error out
        opt <- NULL
        opt$convcod <- 1

        # try optimizing using the specified method but we don"t want a possible error to stop the program
        try({
          opt <- optimx(par=obj$par, fn=function(x) as.numeric(obj$fn(x)), gr=obj$gr,
                        method=method, control=list(iter.max=500, eval.max=500))
          print(opt)
        })

        # check if the optimzation converged, if so then we are done
        if (opt$convcod == 0) break
      })
    }
  }
  (opt_time <- proc.time() - opt_time)
  print(opt)
  if (opt$convcod != 0) stop("Model did not converge")

  # get standard errors
  cat("\n\n***** Extract standard errors\n"); flush.console()
  se_time <- proc.time()
  out <- TMB::sdreport(obj, getJointPrecision=T, getReportCovariance = T)
  (se_time <- proc.time() - se_time)

  save(out, file = paste0(output_dir, loc_id, "/outputs/model_fit/fit_drop", drop_above_age, ".rdata"))


  # Extract model outputs for ccmpp -----------------------------------------

  # function to extract means from the TMB fit
  extract_mean <- function(mean_vector, variable_name, years, ages = NULL) {
    # get the rows of the matrix that correspond to the variable of interest
    var_mean <- data.table(mean = mean_vector[names(mean_vector) == variable_name])
    var_mean[, year_id := rep(years, each=ifelse(!is.null(ages), length(ages), 1))]
    if (!is.null(ages)) var_mean[, age_group_years_start := ages]
    return(var_mean)
  }

  # extract mean and covariance matrix for all model parameters
  mean <- out$value
  # determine which hyperprior parameters were fit
  hyperprior_parameters <- names(out$par.fixed)
  rm(out)

  # extract hyperprior mean
  hyperprior_mean <- data.table(mean = mean[names(mean) %in% hyperprior_parameters])
  hyperprior_mean[, parameter := hyperprior_parameters]
  hyperprior_mean[, location_id := loc_id]

  # extract correlation mean
  rho_mean <- hyperprior_mean[grepl("logit_rho", parameter)]
  rho_mean[, mean := boot::inv.logit(mean)]
  # map correlation parameter names to better names
  rho_parameter_names <- data.table(parameter = c("logit_rho_g_t", "logit_rho_g_a"),
                                    parameter_name = c("rho_migration_time", "rho_migration_age"))
  rho_mean <- merge(rho_mean, rho_parameter_names, all.x=T, by = "parameter")
  rho_mean <- rho_mean[, list(location_id, parameter = parameter_name, mean)]

  # extract standard_deviation mean
  sd_mean <- hyperprior_mean[grepl("log_sigma", parameter)]
  sd_mean[, mean := exp(mean)]
  # map standard_deviation parameter names to better names
  sd_parameter_names <- data.table(parameter = c("log_sigma_n", "log_sigma_n0", "log_sigma_s", "log_sigma_g", "log_sigma_f", "log_sigma_srb"),
                                   parameter_name = c("census", "baseline", "survival", "migration", "fertility", "srb"))
  sd_parameter_names[, parameter_name := paste0("sd_", parameter_name)]
  sd_mean <- merge(sd_mean, sd_parameter_names, all.x=T, by = "parameter")
  sd_mean <- sd_mean[, list(location_id, parameter = parameter_name, mean)]

  hyperprior_mean <- rbind(sd_mean, rho_mean, use.names = T)
  setkeyv(hyperprior_mean, c("location_id", "parameter"))

  ## extract baseline population mean
  n0_female_mean <- extract_mean(mean, "log_n0_female", years = year_start, ages = age_groups_needed$age_group_years_start)
  n0_male_mean <- extract_mean(mean, "log_n0_male", years = year_start, ages = age_groups_needed$age_group_years_start)
  n0_mean <- rbind(n0_male_mean[, list(location_id = loc_id, year_id, sex_id = 1L,
                                       age_group_years_start, mean = exp(mean))],
                   n0_female_mean[, list(location_id = loc_id, year_id, sex_id = 2L,
                                         age_group_years_start, mean = exp(mean))])
  n0_mean <- mortcore::age_start_to_age_group_id(n0_mean, id_vars = c("location_id", "year_id", "sex_id"), keep_age_group_id_only = F)
  setcolorder(n0_mean, c("location_id", "year_id", "sex_id", "age_group_id", "age_group_years_start", "age_group_years_end", "mean"))
  setkeyv(n0_mean, c("location_id", "year_id", "sex_id", "age_group_years_start"))
  rm(n0_female_mean, n0_male_mean)

  ## extract survival ratio mean
  s_female_mean <- extract_mean(mean, "logit_s_female", years = years[c(-length(years))],
                                ages = survival_cohort_age_groups_needed$age_group_years_start)
  s_male_mean <- extract_mean(mean, "logit_s_male", years = years[c(-length(years))],
                              ages = survival_cohort_age_groups_needed$age_group_years_start)
  s_mean <- rbind(s_male_mean[, list(location_id = loc_id, year_id, sex_id = 1L, age_group_years_start, mean = boot::inv.logit(mean))],
                  s_female_mean[, list(location_id = loc_id, year_id, sex_id=2L, age_group_years_start, mean = boot::inv.logit(mean))])
  s_mean <- mortcore::age_start_to_age_group_id(s_mean, id_vars = c("location_id", "year_id", "sex_id"), keep_age_group_id_only = F)
  setcolorder(s_mean, c("location_id", "year_id", "sex_id", "age_group_id", "age_group_years_start", "age_group_years_end", "mean"))
  setkeyv(s_mean, c("location_id", "year_id", "sex_id", "age_group_years_start"))
  rm(s_female_mean, s_male_mean)

  ## extract net migration proportion mean
  g_female_mean <- extract_mean(mean, "g_female", years = years[c(-length(years))],
                                ages = migration_cohort_age_groups_needed$age_group_years_start)
  g_male_mean <- extract_mean(mean, "g_male", years = years[c(-length(years))],
                              ages = migration_cohort_age_groups_needed$age_group_years_start)
  g_mean <- rbind(g_male_mean[, list(location_id = loc_id, year_id, sex_id = 1L, age_group_years_start, mean)],
                  g_female_mean[, list(location_id = loc_id, year_id, sex_id = 2L, age_group_years_start, mean)])
  g_mean <- mortcore::age_start_to_age_group_id(g_mean, id_vars = c("location_id", "year_id", "sex_id"), keep_age_group_id_only = F)
  setcolorder(g_mean, c("location_id", "year_id", "sex_id", "age_group_id", "age_group_years_start", "age_group_years_end", "mean"))
  setkeyv(g_mean, c("location_id", "year_id", "sex_id", "age_group_years_start"))
  rm(g_female_mean, g_male_mean)

  ## extract age specific fertility mean
  fert_ages <- as.integer(rownames(tmb_data$input_log_f))
  f_mean <- extract_mean(mean, "log_f", years = years[c(-length(years))], ages = fert_ages)
  f_mean <- f_mean[, list(location_id = loc_id, year_id, age_group_years_start, mean = exp(mean))]

  # in order to use the ccmpp function, need to add on values for all ages
  fert_zero_ages <- setdiff(age_groups_needed$age_group_years_start, unique(f_mean$age_group_years_start))
  if (length(fert_zero_ages > 0)) {
    add_fert <- CJ(location_id = loc_id, year_id = years[c(-length(years))],
                   age_group_years_start = fert_zero_ages, mean = 0)
    f_mean <- rbind(f_mean, add_fert, fill=T)
    setkeyv(f_mean, c("location_id", "year_id", "age_group_years_start"))
  }
  f_mean <- mortcore::age_start_to_age_group_id(f_mean, id_vars = c("location_id", "year_id"), keep_age_group_id_only = F)
  setcolorder(f_mean, c("location_id", "year_id", "age_group_id", "age_group_years_start", "age_group_years_end", "mean"))
  setkeyv(f_mean, c("location_id", "year_id", "age_group_years_start"))
  rm(fert_zero_ages)

  ## extract sex ratio at birth mean
  srb_mean <- extract_mean(mean, "log_srb", years = years[c(-length(years))])
  srb_mean <- srb_mean[, list(location_id = loc_id, year_id, mean = exp(mean))]
  setkeyv(srb_mean, c("location_id", "year_id"))
  rm(mean); gc()


  # Save outputs ------------------------------------------------------------

  # save demographic input parameters if not fixed during model estimation
  if (!fix_fertility) readr::write_csv(f_mean, path = paste0(output_dir, loc_id, "/outputs/model_fit/asfr_drop", drop_above_age, ".csv"))
  if (!fix_survival) readr::write_csv(s_mean, path = paste0(output_dir, loc_id, "/outputs/model_fit/survival_ratio_drop", drop_above_age, ".csv"))
  if (!fix_srb) readr::write_csv(srb_mean, path = paste0(output_dir, loc_id, "/outputs/model_fit/srb_drop", drop_above_age, ".csv"))
  if (!fix_baseline) readr::write_csv(n0_mean, path = paste0(output_dir, loc_id, "/outputs/model_fit/baseline_pop_drop", drop_above_age, ".csv"))
  if (!fix_migration) readr::write_csv(g_mean, path = paste0(output_dir, loc_id, "/outputs/model_fit/migration_proportion_drop", drop_above_age, ".csv"))

  sink()


} else {

  # Copy over outputs from previous version ---------------------------------

  # useful if testing just post model fit code and want to reduce run time from fitting the whole population model
  # or if just rerunning a subset of locations
  copy_output_dir <- "FILEPATH + copy_vid"
  file.copy(from = paste0(copy_output_dir, loc_id, "/outputs/model_fit/fit_drop", drop_above_age, "_diagnostics.txt"),
            to = paste0(output_dir, loc_id, "/outputs/model_fit/fit_drop", drop_above_age, "_diagnostics.txt"), overwrite = T)
  file.copy(from = paste0(copy_output_dir, loc_id, "/outputs/model_fit/fit_drop", drop_above_age, ".rdata"),
            to = paste0(output_dir, loc_id, "/outputs/model_fit/fit_drop", drop_above_age, ".rdata"), overwrite = T)
  if (!fix_fertility) file.copy(from = paste0(copy_output_dir, loc_id, "/outputs/model_fit/asfr_drop", drop_above_age, ".csv"),
                                to = paste0(output_dir, loc_id, "/outputs/model_fit/asfr_drop", drop_above_age, ".csv"), overwrite = T)
  if (!fix_survival) file.copy(from = paste0(copy_output_dir, loc_id, "/outputs/model_fit/survival_ratio_drop", drop_above_age, ".csv"),
                               to = paste0(output_dir, loc_id, "/outputs/model_fit/survival_ratio_drop", drop_above_age, ".csv"), overwrite = T)
  if (!fix_srb) file.copy(from = paste0(copy_output_dir, loc_id, "/outputs/model_fit/srb_drop", drop_above_age, ".csv"),
                          to = paste0(output_dir, loc_id, "/outputs/model_fit/srb_drop", drop_above_age, ".csv"), overwrite = T)
  if (!fix_baseline) file.copy(from = paste0(copy_output_dir, loc_id, "/outputs/model_fit/baseline_pop_drop", drop_above_age, ".csv"),
                               to = paste0(output_dir, loc_id, "/outputs/model_fit/baseline_pop_drop", drop_above_age, ".csv"), overwrite = T)
  if (!fix_migration) file.copy(from = paste0(copy_output_dir, loc_id, "/outputs/model_fit/migration_proportion_drop", drop_above_age, ".csv"),
                                to = paste0(output_dir, loc_id, "/outputs/model_fit/migration_proportion_drop", drop_above_age, ".csv"), overwrite = T)
}


