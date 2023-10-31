#' @author 
#' @date 2019/09/09
#' @description predict CFR values using and submit parallel job to compute 
#' mortality proportions for all GBD locations

rm(list=ls())

pacman::p_load(data.table, boot, ggplot2, parallel, foreach, doSNOW, openxlsx)
library(reticulate, lib.loc = "/filepath/")
library(mrbrt001, lib.loc = "/filepath/") # for R version 3.6.3

# SOURCE SHARED FUNCTIONS ------------------------------
invisible(sapply(list.files("/filepath/", full.names = T), source))

# USER INPUTS -------------------------------------------------------------
ds <- 'iterative'
gbd_round_id <- 7
main_dir  <- "filepath"
annual_paf <- T
calc_preds <- T

model_object_dir <- "filepath" # dir to pull MR-BRT draws
# name prefix used in offset csv, model object, MR-BRT directory, and plots
model_prefix <- "name"

date <- gsub("-", "_", Sys.Date())
# where nonfatal and fatal PAF draws are outputed
out_dir <- "filepath"
dir.create(out_dir, recursive = T, showWarnings = F)

preds_dir <- paste0(out_dir, "preds/")
dir.create(preds_dir, recursive = T, showWarnings = F)

check_dir <- paste0(out_dir, "checks/")
dir.create(check_dir, recursive = T, showWarnings = F)

# create output directores for upload for each etiology and for fatal/ nonfatal
list_dirs <- function(parent_dir){
  sub_dirs <- list(
    spn = paste0(parent_dir, "meningitis_pneumo/"),
    hib = paste0(parent_dir, "meningitis_hib/"),
    nm = paste0(parent_dir, "meningitis_meningo/"),
    other = paste0(parent_dir, "meningitis_other/"),
    gbs = paste0(parent_dir, "meningitis_gbs/")
  ) 
  return(sub_dirs)
}

sub_dirs_fatal <- list_dirs(paste0(out_dir, "/fatal/"))
for (dir in sub_dirs_fatal) {
  if (!dir.exists(dir)) dir.create(dir, recursive = T)
}

sub_dirs_nonfatal <- list_dirs(paste0(out_dir, "/nonfatal/"))
for (dir in sub_dirs_nonfatal) {
  if (!dir.exists(dir)) dir.create(dir, recursive = T)
}

# delete finished check files if rerunning
files <- list.files(check_dir, pattern='finished.*.txt')
for (f in files) {
  file.remove(file.path(check_dir, f))
}

param_map_path <- file.path(main_dir, "compute_mortality_parameters.csv")

# MEIDs for saving results
spn_mort_meid <- 10494
nm_mort_meid <- 10495
hib_mort_meid <- 10496
gbs_mort_meid <- 25362

spn_inc_meid <- 24739
nm_inc_meid <- 24741
hib_inc_meid <- 24740
gbs_inc_meid <- 25361

# create parameter map for upload jobs
upload_param_map <- data.table(etiology = c("meningitis_pneumo", "meningitis_meningo", "meningitis_hib", "meningitis_gbs",
                                            "meningitis_pneumo", "meningitis_meningo", "meningitis_hib", "meningitis_gbs"),
                               meid = c(spn_mort_meid, nm_mort_meid, hib_mort_meid, gbs_mort_meid,
                                        spn_inc_meid, nm_inc_meid, hib_inc_meid, gbs_inc_meid),
                               dir = c(sub_dirs_fatal[["spn"]], sub_dirs_fatal[["nm"]], sub_dirs_fatal[["hib"]], sub_dirs_fatal[["gbs"]],
                                       sub_dirs_nonfatal[["spn"]], sub_dirs_nonfatal[["nm"]], sub_dirs_nonfatal[["hib"]], sub_dirs_nonfatal[["gbs"]]), 
                               bundle_id = c(7181, 7181, 7181, 7181,
                                          29, 37, 33, 7958))

# MR-BRT wrapper functions
repo_dir <- "/filepath/"
source(paste0(repo_dir, "mr_brt_functions.R"))
source(paste0(main_dir, "/helper_functions/qsub.R"))

# PREDICT CFR AS A FUNCTION OF HAQ ----------------------------------------
# get GBD demographics
demo <- get_demographics('epi', gbd_round_id = gbd_round_id)
locations <- demo$location_id
years <- demo$year_id
if (annual_paf == T) years <- years[1]:years[length(years)]


if (calc_preds == T){
  # get HAQ
  haqi_dt <- get_covariate_estimates(covariate_id = 1099,
                                     location_id = locations,
                                     year_id = years,
                                     gbd_round_id = gbd_round_id,
                                     decomp_step = "step3")
  haqi_dt <- haqi_dt[, .(location_id, year_id, haqi = mean_value)]
  
  # read in specified MR-BRT model object
  model_fit <- py_load_object(filename = paste0(model_object_dir, model_prefix, ".pkl"))
  model_fit$fit_model()
  # create datasets to make predictions on x-covs
  x_pred_dt <- expand.grid(intercept    = 1,
                           year_after_2000 = c(0,1),
                           clinical_data = 0,
                           age_u5       = c(0, 1),
                           age_65plus   = c(0, 1),
                           haqi         = haqi_dt[, haqi],
                           etio_pneumo  = c(0, 1),
                           etio_meningo = c(0, 1),
                           etio_gbs     = c(0, 1),
                           etio_hib     = c(0, 1))
  x_pred_dt <- as.data.table(x_pred_dt)
  etio_cols <- c("etio_pneumo", "etio_meningo", "etio_hib", "etio_other", "etio_gbs")
  int_cols <- c("pneumo_haqi", "meningo_haqi", "hib_haqi", "other_haqi", "gbs_haqi")
  int_cols <- int_cols[int_cols != "other_haqi"]
  etio_cols <- etio_cols[etio_cols != "etio_other"]
  x_pred_dt[, (int_cols) := lapply(.SD, function(x) 
    x * x_pred_dt[['haqi']] ), .SDcols = etio_cols]
  
  # drop rows with more than 1 indicator variable marked 1
  x_pred_dt <- x_pred_dt[etio_pneumo + etio_meningo + etio_hib + etio_gbs <= 1]
  x_pred_dt <- x_pred_dt[age_u5 + age_65plus <= 1]
  x_pred_dt[, data_id := 1:nrow(x_pred_dt)]
  
  dat_pred1 <- MRData()
  
  dat_pred1$load_df(
    data = x_pred_dt, 
    col_covs=list("haqi", 
                  "year_after_2000", 
                  # "clinical_data",
                  "age_u5", "age_65plus",
                  "etio_pneumo", "etio_hib", "etio_meningo", "etio_gbs", 
                  "pneumo_haqi", "meningo_haqi", "hib_haqi", "gbs_haqi"
    )
  )
  
  predict_re <- F
  
  n_samples <- 1000L
  samples <- mrbrt001::core$other_sampling$sample_simple_lme_beta(sample_size = 1000L, model = model_fit)
  model_betas <- data.table(mean = apply(samples,2,mean),
                            lower = apply(samples,2,quantile,0.025),
                            upper = apply(samples,2,quantile,0.975))
  gamma_draws <- t(matrix(replicate(1000,model_fit$gamma_soln), nrow=6))
  draws3 <- model_fit$create_draws(
    data = dat_pred1,
    beta_samples = samples,
    gamma_samples = gamma_draws,
    random_study = predict_re )
  x_pred_dt$pred3 <- model_fit$predict(data = dat_pred1, sort_by_data_id = TRUE)
  x_pred_dt$pred3_lo <- apply(draws3, 1, function(x) quantile(x, 0.025))
  x_pred_dt$pred3_hi <- apply(draws3, 1, function(x) quantile(x, 0.975))

  draws3 <- as.data.table(draws3)
  names(draws3) <- paste("draw", 0:999, sep = "_")
  x_pred_dt <- cbind(x_pred_dt, draws3)
  preds <- copy(x_pred_dt)
  
  # convert one hot encoding to categorical case name field 
  preds[                 , case_name := "meningitis_other"]
  preds[etio_pneumo  == 1, case_name := "meningitis_pneumo"]
  preds[etio_hib     == 1, case_name := "meningitis_hib"]
  preds[etio_gbs     == 1, case_name := "meningitis_gbs"]
  preds[etio_meningo == 1, case_name := "meningitis_meningo"]
  
  # inverse logit back to linear space, both draws and summary
  colnames <- names(draws3)
  preds[, (colnames) := lapply(.SD, inv.logit), .SDcols = colnames]
  preds[, `:=` (mean_lin    = inv.logit(pred3),
                mean_lo_lin = inv.logit(pred3_lo), 
                mean_hi_lin = inv.logit(pred3_hi))]
  
  # get back location and year columns
  preds <- merge(preds, haqi_dt[, .(location_id, year_id, haqi)], by = "haqi")
  
  # drop nonsensical year columns
  preds <- preds[(year_after_2000 == 1 & year_id>2000) | (year_after_2000 == 0 & year_id<=2000)]
  
  # reshape from wide to long
  id_vars <- c("case_name", "haqi", "year_id", "year_after_2000", "location_id", "age_u5", "age_65plus")
  preds <- melt(preds, id.vars = id_vars, measure.vars = paste0("draw_", 0:999), variable.name = "draw",
                value.name = "logit_cfr")
  preds[, cfr := inv.logit(logit_cfr)]
   
  for (loc in locations) {
    tmp_dt <- preds[location_id == loc]
    # message(nrow(tmp_dt))
    fwrite(tmp_dt, paste0(preds_dir, loc, "_mrbrt_preds.csv"))
  }
}


# SQUEEZE MORTALITY RATES -------------------------------------------------
# create parameter map to where each row represents parameters for each job
param_map <- expand.grid(location_id = locations)
write.csv(param_map, param_map_path, row.names=F)
n_jobs <- nrow(param_map)

# Submit job
my_job_name <- "child_compute_meningitis_mort"
print(paste("submitting", my_job_name))
my_project <- "proj_hiv"
my_tasks <- paste0("1:", n_jobs)
my_queue <- 'all.q'
my_time <- '4:00:00'
if (annual_paf == T){
  parallel_script <- paste0(main_dir, "/compute_annual_paf_parallel.R")
  my_fthread <- 10
  my_mem <- "50G"
} else {
  parallel_script <- paste0(main_dir, "/compute_mortality_parallel.R")
  my_fthread <- 1
  my_mem <- "10G"
}
# shell_file <- paste0(main_dir, "r_shell.sh")
shell_file <- "/filepath/execRscript.sh"

my_args <- paste("--date", date,
                 "--out_dir", out_dir,
                 "--check_dir", check_dir,
                 "--param_map_path", param_map_path,
                 "--ds", ds)

qsub(
  job_name = my_job_name,
  project = my_project,
  queue = my_queue,
  tasks = my_tasks,
  mem = my_mem,
  fthread = my_fthread,
  j_archive = 0,
  time = my_time,
  shell_file = shell_file,
  script = parallel_script,
  args = my_args
)

# check for jobs to finish
Sys.sleep(60)
# Wait for jobs to finish before passing execution back to main step file
i <- 0
while (i == 0) {
  checks <- list.files(check_dir, pattern='finished.*.txt')
  count <- length(checks)
  print(paste("checking ", Sys.time(), " ", count, "of ", n_jobs, " jobs finished"))

  if (count == n_jobs) i <- i + 1 else Sys.sleep(60)
}
# if you need to rerun only certain locations -> change locations variable to only unfinished locations and rerun from there

# SAVE RESULTS ------------------------------------------------------------
# Submit jobs

dir_2020 <- "filepath"
table_2020 <- read.xlsx(paste0(dir_2020, "crosswalk_version_tracking.xlsx"))
table_2020 <- data.table(table_2020)

for (n in 1:nrow(upload_param_map)) {
  etio <- upload_param_map[n, etiology]
  meid <- upload_param_map[n, meid]
  upload_dir <- upload_param_map[n, dir]
  bundle <- upload_param_map[n, bundle_id]
  crosswalk_version_id <- table_2020[bundle_id == bundle & current_best == 1]$crosswalk_version
  # if (bundle == 7181) crosswalk_version_id <- 33185 # gbd 2019 data only
  
  my_job_name <- paste0(etio, "_", meid, "_save_results")
  print(paste("submitting", my_job_name))
  my_fthread <- 10
  my_mem <- "100G"
  my_time <- "08:00:00"
  parallel_script <- paste0(main_dir, "/save_paf_results_parallel.R")
  my_args <- paste("--meid", meid,
                   "--upload_dir", upload_dir,
                   "--check_dir", check_dir,
                   "--ds", ds, 
                   "--gbd_round_id", gbd_round_id,
                   "--bundle", bundle,
                   "--crosswalk_version_id", crosswalk_version_id)
  
  qsub(
    job_name = my_job_name,
    project = my_project,
    queue = my_queue,
    mem = my_mem,
    fthread = my_fthread,
    j_archive = 1,
    time = my_time,
    shell_file = shell_file,
    script = parallel_script,
    args = my_args
  )
}


