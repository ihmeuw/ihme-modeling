
library(data.table)
library(TMB)
library(optimx)
library(boot)

rm(list=ls())
set.seed(98121)


# Get settings ------------------------------------------------------------

main_dir <- commandArgs(trailingOnly=T)[1]
ihme_loc <- commandArgs(trailingOnly=T)[2]
run <- as.integer(commandArgs(trailingOnly=T)[3])

source("settings.R")
get_settings(main_dir)

source("modeling/helper_functions.R")

# Load all input data -----------------------------------------------------

load_rdata_input_data("census_matrix", run)
load_rdata_input_data("fertility_matrix", run)
load_rdata_input_data("survival_matrix", run)
load_rdata_input_data("migration_matrix", run)
load_rdata_input_data("srb_matrix", run)
hyperparameters_data <- load_csv_input_data("hyperparameters", run)


# Format input data to go into TMB models ---------------------------------

row_fert_start <- min(which(rowSums(get(paste0("fertility_", age_int, "_matrix"))) > 0))
row_fert_end <- max(which(rowSums(get(paste0("fertility_", age_int, "_matrix"))) > 0))
tmb_data <- list(
  input_n_female = get(paste0("census_", age_int, "_matrix"))$female,
  input_n_male = get(paste0("census_", age_int, "_matrix"))$male,
  input_n_ages_female = get(paste0("census_ages_", age_int, "_matrix"))$female,
  input_n_ages_male = get(paste0("census_ages_", age_int, "_matrix"))$male,

  input_n0_female = get(paste0("baseline_", age_int, "_matrix"))$female,
  input_n0_male = get(paste0("baseline_", age_int, "_matrix"))$male,
  input_logit_s_female = logit(get(paste0(ifelse(age_int == 1, "full", "abridged"), "_survival_matrix"))$female),
  input_logit_s_male = logit(get(paste0(ifelse(age_int == 1, "full", "abridged"), "_survival_matrix"))$male),
  input_g_female = get(paste0("migration_", age_int, "_matrix"))$female,
  input_g_male = get(paste0("migration_", age_int, "_matrix"))$male,
  input_log_f = log(get(paste0("fertility_", age_int, "_matrix"))[row_fert_start:row_fert_end, ]),
  input_log_srb = log(get(paste0("srb_", age_int, "_matrix"))),

  age_int = age_int,
  row_fert_start = row_fert_start - 1,
  row_fert_end = row_fert_end - 1,
  col_census_year = determine_census_years(min(years) + unique(get(paste0("census_ages_", age_int, "_matrix"))$male[, 1]), years),

  mu_s = hyperparameters_data$mu_s,
  sig_s = hyperparameters_data$sig_s,
  mu_g = hyperparameters_data$mu_g,
  sig_g = hyperparameters_data$sig_g,
  mu_f = hyperparameters_data$mu_f,
  sig_f = hyperparameters_data$sig_f,
  mu_srb = hyperparameters_data$mu_srb,
  sig_srb = hyperparameters_data$sig_srb,

  rho_g_t_mu = hyperparameters_data$rho_g_t_mu,
  rho_g_t_sig = hyperparameters_data$rho_g_t_sig,
  rho_g_a_mu = hyperparameters_data$rho_g_a_mu,
  rho_g_a_sig = hyperparameters_data$rho_g_a_sig
)

tmb_par <- list(

  log_n0_female = log(tmb_data$input_n0_female[, 1, drop = F]),
  log_n0_male = log(tmb_data$input_n0_male[, 1, drop = F]),

  logit_s_female = tmb_data$input_logit_s_female,
  logit_s_male = tmb_data$input_logit_s_male,
  log_sigma_s = log(census_default_sd),

  g_female = tmb_data$input_g_female,
  g_male = tmb_data$input_g_male,
  log_sigma_g = log(census_default_sd),
  logit_rho_g_t = 0,
  logit_rho_g_a = 0,

  log_f = tmb_data$input_log_f,
  log_sigma_f = log(census_default_sd),

  log_srb = tmb_data$input_log_srb,
  log_sigma_srb = log(census_default_sd)
)



# check if fixing certain parameters
map <- NULL
if (fix_baseline[run]) {
  map$log_n0_female <- rep(factor(NA), nrow(tmb_par$log_n0_female) * ncol(tmb_par$log_n0_female))
  dim(map$log_n0_female) <- c(nrow(tmb_par$log_n0_female), ncol(tmb_par$log_n0_female))
  map$log_n0_male <- rep(factor(NA), nrow(tmb_par$log_n0_male) * ncol(tmb_par$log_n0_male))
  dim(map$log_n0_male) <- c(nrow(tmb_par$log_n0_male), ncol(tmb_par$log_n0_male))
}
if (fix_survival[run]) {
  map$logit_s_female <- rep(factor(NA), nrow(tmb_par$logit_s_female) * ncol(tmb_par$logit_s_female))
  dim(map$logit_s_female) <- c(nrow(tmb_par$logit_s_female), ncol(tmb_par$logit_s_female))
  map$logit_s_male <- rep(factor(NA), nrow(tmb_par$logit_s_male) * ncol(tmb_par$logit_s_male))
  dim(map$logit_s_male) <- c(nrow(tmb_par$logit_s_male), ncol(tmb_par$logit_s_male))

  map$log_sigma_s <- factor(NA)
}
if (fix_migration[run]) {
  map$g_female <- rep(factor(NA), nrow(tmb_par$g_female) * ncol(tmb_par$g_female))
  dim(map$g_female) <- c(nrow(tmb_par$g_female), ncol(tmb_par$g_female))
  map$g_male <- rep(factor(NA), nrow(tmb_par$g_male) * ncol(tmb_par$g_male))
  dim(map$g_male) <- c(nrow(tmb_par$g_male), ncol(tmb_par$g_male))

  map$log_sigma_g <- factor(NA)
  map$logit_rho_g_t <- factor(NA)
  map$logit_rho_g_a <- factor(NA)
}
if (fix_fertility[run]) {
  map$log_f <- rep(factor(NA), nrow(tmb_par$log_f) * ncol(tmb_par$log_f))
  dim(map$log_f) <- c(nrow(tmb_par$log_f), ncol(tmb_par$log_f))

  map$log_sigma_f <- factor(NA)
}
if (fix_srb[run]) {
  map$log_srb <- rep(factor(NA), nrow(tmb_par$log_srb) * ncol(tmb_par$log_srb))
  dim(map$log_srb) <- c(nrow(tmb_par$log_srb), ncol(tmb_par$log_srb))

  map$log_sigma_srb <- factor(NA)
}

sink(file = paste0(temp_dir, "/model/run_", run, "/fit/model_fit_diagnostics.txt"))

openmp(8)
if (!is.null(getLoadedDLLs()[["02_model"]])) dyn.unload(dynlib("02_model"))
TMB::compile("models/tmb/02_model.cpp")  # Compile the C++ file
dyn.load(dynlib("models/tmb/02_model"))  # Dynamically link the C++ code

config(tape.parallel=0, DLL="02_model")

# make objective function
cat("\n\n***** Make objective function\n"); flush.console()
obj <- MakeADFun(tmb_data, tmb_par,
                 random=c("log_n0_female", "log_n0_male", "logit_s_female", "logit_s_male", "g_female", "g_male", "log_f", "log_srb"),
                 map=map, DLL="02_model")

# optimize objective function
cat("\n\n***** Optimize objective function\n"); flush.console()
opt_time <- proc.time()

# first try nlminb optimizer since most locations fit with this and is a commonly used optimzer
opt <- try(nlminb(start=obj$par, objective=obj$fn, gradient=obj$gr, control=list(eval.max=1e5, iter.max=1e5)))
if (class(opt) != "try-error") opt$convcod <- opt$convergence else opt$convcod <- 1

# if still failing try some other optimizers
if (all(opt$convcod != 0)) {
  for (method in c("L-BFGS-B", "BFGS", "CG", "nlm", "newuoa", "bobyqa", "nmkb")) {
    try({
      cat(paste("\n  *** Method: ", method, "\n"))

      # set this up so the break statement doesn't error out
      opt <- NULL
      opt$convcod <- 1

      # try optimizing using the specified method but we don't want a possible error to stop the program
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
out <- sdreport(obj, getJointPrecision=T, getReportCovariance = T)
(se_time <- proc.time() - se_time)

save(out, opt, tmb_data, tmb_par, file=paste0(temp_dir, "/model/run_", run, "/fit/model_fit.rdata"))

sink()
