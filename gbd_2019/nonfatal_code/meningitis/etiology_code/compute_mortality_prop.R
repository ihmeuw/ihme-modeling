#' @author 
#' @date 2019/09/09
#' @description predict CFR values using and submit parallel job to compute 
#' mortality proportions for all GBD locations

rm(list=ls())

pacman::p_load(data.table, boot, ggplot2, parallel, foreach, doSNOW)

# USER INPUTS -------------------------------------------------------------
ds <- 'step4'

main_dir <- # filepath  
model_object_dir <- # filepath  # dir to pull MR-BRT model objects

date <- gsub("-", "_", Sys.Date())

out_dir <- # filepath
preds_dir # filepath
check_dir # filepath

# create output directores for upload for each etiology and for fatal/ nonfatal
fatal_out_dir <- # filepath
spn_fatal_out_dir <- paste0(fatal_out_dir, "meningitis_pneumo/")
hib_fatal_out_dir <- paste0(fatal_out_dir, "meningitis_hib/")
nm_fatal_out_dir <- paste0(fatal_out_dir, "meningitis_meningo/")
other_fatal_out_dir <- paste0(fatal_out_dir, "meningitis_other/")

nonfatal_out_dir <- # filepath
spn_nonfatal_out_dir <- paste0(nonfatal_out_dir, "meningitis_pneumo/")
hib_nonfatal_out_dir <- paste0(nonfatal_out_dir, "meningitis_hib/")
nm_nonfatal_out_dir <- paste0(nonfatal_out_dir, "meningitis_meningo/")
other_nonfatal_out_dir <- paste0(nonfatal_out_dir, "meningitis_other/")

# delete finished check files if rerunning
files <- list.files(check_dir, pattern='finished.*.txt')
for (f in files) {
  file.remove(file.path(check_dir, f))
}

param_map_path <- file.path(main_dir, "compute_mortality_parameters.csv")

# name prefix used in offset csv, model object, MR-BRT directory, and plots
model_prefix <- paste0("2019_08_15_one_hot_encode_etiologies")

# MEIDs for saving results
spn_mort_meid <- 10494
nm_mort_meid <- 10495
hib_mort_meid <- 10496

spn_inc_meid <- 24739
nm_inc_meid <- 24741
hib_inc_meid <- 24740

# create parameter map for upload jobs
upload_param_map <- data.table(etiology = c("meningitis_pneumo", "meningitis_meningo", "meningitis_hib",
                                            "meningitis_pneumo", "meningitis_meningo", "meningitis_hib"),
                               meid = c(spn_mort_meid, nm_mort_meid, hib_mort_meid,
                                        spn_inc_meid, nm_inc_meid, hib_inc_meid),
                               dir = c(spn_fatal_out_dir, nm_fatal_out_dir, hib_fatal_out_dir, 
                                       spn_nonfatal_out_dir, nm_nonfatal_out_dir, hib_nonfatal_out_dir))


# SOURCE FUNCTIONS --------------------------------------------------------
k <- # filepath
source(paste0(k, "current/r/get_covariate_estimates.R"))
source(paste0(k, "current/r/get_location_metadata.R"))
source(paste0(k, "current/r/get_demographics.R"))

# MR-BRT wrapper functions
repo_dir <- # filepath
source(paste0(repo_dir, "mr_brt_functions.R"))
source(paste0(main_dir, "/helper_functions/qsub.R"))

# PREDICT CFR AS A FUNCTION OF HAQ ----------------------------------------
# get GBD 2019 demographics
demo <- get_demographics('epi', gbd_round_id = 6)
locations <- demo$location_id
years <- demo$year_id

# get HAQ
haqi_dt <- get_covariate_estimates(covariate_id = 1099,
                                   location_id = locations,
                                   year_id = years,
                                   gbd_round_id = 6,
                                   decomp_step = ds)
haqi_dt <- haqi_dt[, .(location_id, year_id, haqi = mean_value)]

# read in specified MR-BRT model object
model_fit <- readRDS(paste0(model_object_dir, model_prefix, "_model_fit.RDS"))
# create datasets to make predictions on x-covs
x_pred_dt <- expand.grid(intercept    = 1,
                         haqi         = haqi_dt[, haqi],
                         etio_pneumo  = c(0, 1),
                         etio_meningo = c(0, 1),
                         etio_hib     = c(0, 1))
setDT(x_pred_dt)
# drop rows with more than 1 indicator variable marked 1
x_pred_dt[, sum := etio_pneumo + etio_meningo + etio_hib]
x_pred_dt <- x_pred_dt[sum <= 1]
# create datasets to make predictions on x-covs
z_pred_dt <- expand.grid(intercept    = 1,
                         etio_pneumo  = c(0, 1),
                         etio_meningo = c(0, 1),
                         etio_hib     = c(0, 1))
setDT(z_pred_dt)
# drop rows with more than 1 indicator variable marked 1
z_pred_dt[, sum := etio_pneumo + etio_meningo + etio_hib]
z_pred_dt <- z_pred_dt[sum <= 1]

pred1 <- predict_mr_brt(model_fit, newdata = x_pred_dt, z_newdata = z_pred_dt, write_draws = T)
pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_draws
setDT(preds)
# drop predictions with more than 1 indicator variable is marked 1
preds <- preds[X_etio_pneumo == Z_etio_pneumo
               & X_etio_hib == Z_etio_hib
               & X_etio_meningo == Z_etio_meningo]
# convert one hot encoding to categorical case name field
preds[                   , case_name := "meningitis_other"]
preds[X_etio_pneumo  == 1, case_name := "meningitis_pneumo"]
preds[X_etio_hib     == 1, case_name := "meningitis_hib"]
preds[X_etio_meningo == 1, case_name := "meningitis_meningo"]
cols.remove <- c("X_intercept", "X_etio_pneumo", "X_etio_hib", "X_etio_meningo",
                 "Z_intercept", "Z_etio_pneumo", "Z_etio_hib", "Z_etio_meningo")
preds[, (cols.remove) := NULL]
# get back location and year columns
preds <- cbind(preds, haqi_dt[, .(location_id, year_id)])
# reshape from wide to long
id_vars <- c("case_name", "X_haqi", "year_id", "location_id")
preds <- melt(preds, id.vars = id_vars, variable.name = "draw",
              value.name = "logit_cfr")
preds[, cfr := inv.logit(logit_cfr)]

for (loc in locations) {
  tmp_dt <- preds[location_id == loc]
  message(nrow(tmp_dt))
  fwrite(tmp_dt, paste0(preds_dir, loc, "_mrbrt_preds.csv"))
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
my_fthread <- 2
my_mem <- "5G"
my_time <- "01:00:00"
my_queue <- 'all.q'
parallel_script <- paste0(main_dir, "/compute_mortality_parallel.R")
shell_file <- paste0(main_dir, "r_shell.sh")

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
  j_archive = 1,
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

for (n in 1:nrow(upload_param_map)) {
  etio <- upload_param_map[n, etiology]
  meid <- upload_param_map[n, meid]
  upload_dir <- upload_param_map[n, dir]
  
  my_job_name <- paste0(etio, "_", meid, "_save_results")
  print(paste("submitting", my_job_name))
  my_project <- "proj_hiv"
  my_fthread <- 10
  my_mem <- "50G"
  my_time <- "08:00:00"
  my_queue <- 'all.q'
  shell_file <- paste0(main_dir, "r_shell.sh")
  parallel_script <- paste0(main_dir, "/save_paf_results_parallel.R")
  my_args <- paste("--meid", meid,
                   "--upload_dir", upload_dir,
                   "--check_dir", check_dir,
                   "--ds", ds)
  
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
