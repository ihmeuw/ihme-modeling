#
# run_pipeline_parallel.R
#

rm(list = ls())
library(dplyr)
library(parallel)

#####
# user params
#
WORK_DIR <- "FILEPATH"
source(paste0(WORK_DIR, "/config.R"))
source(paste0(WORK_DIR, "FILEPATH/prep_diet_data_function.R"))
source(paste0("FILEPATH/qsub_function.R"))


#####
# create directories
#

if (!dir.exists(OUT_DIR)) {
  if (!dir.exists(dirname(OUT_DIR))) {
    dir.create(dirname(OUT_DIR))
  }
  dir.create(OUT_DIR)
} else {
  warning("Directory '", OUT_DIR, "' already exists")
}

for (dir in SUB_DIRS) {
  if (!dir.exists(dir)) {
    dir.create(dir)
  } else {
    warning("Directory '", dir, "' already exists")
  }
}


submit_jobs <- function(pair, WORK_DIR) {
  
  # stage 1, template model
  stage1 <- submit_sub_job(pair, "01_create_template.R", paste0("_01_template_", VERSION_ID), WORK_DIR)
  
  # stage 2, template log linear model
  stage2 <- submit_sub_job(pair, "02_loglinear_models.R", paste0("_02_template_", VERSION_ID), WORK_DIR, 
                           hold = as.numeric(stage1))
  
  # stage 3, covariate selection
  stage3 <- submit_sub_job(pair, "03_covariate_selection.R", paste0("_03_cov_selection_", VERSION_ID), WORK_DIR,
                           hold = as.numeric(stage2))
  
  # stage 4, log-linear models
  stage4 <- submit_sub_job(pair, "04b_loglinear_compare_models.R", paste0("_04_loglinear_", VERSION_ID), WORK_DIR, 
                           hold = as.numeric(stage3))
  
  
  # stage 4, monotonic spline models
  stage4 <- submit_sub_job(pair, "04_mixed_effects_models.R", paste0("_04_mixed_effects_", VERSION_ID), WORK_DIR, 
                           hold = as.numeric(stage3))

  
  # stage 4, monotonic spline models
  stage5 <- submit_sub_job(pair, "05_evidence_score_continuous.R", paste0("_05_pub_bias_", VERSION_ID), WORK_DIR, 
                           hold = as.numeric(stage4))
  
}

#####
# data prep
#

stage0_results <- lapply(RO_PAIRS, function(ro_pair) {
  x <- try({
    prep_diet_data(
      ro_pair = ro_pair,
      obs_var = OBS_VAR,
      obs_se_var = OBS_SE_VAR,
      ref_vars = REF_EXPOSURE_COLS,
      alt_vars = ALT_EXPOSURE_COLS,
      allow_ref_gt_alt = FALSE,
      diet_dir = INPUT_DATA_DIR,
      study_id_var = "nid",
      verbose = TRUE
    )
  })
  
  saveRDS(x, paste0(OUT_DIR, "00_prepped_data/", ro_pair, ".RDS"))
  return(x)
})

names(stage0_results) <- RO_PAIRS
saveRDS(stage0_results, paste0(OUT_DIR, "stage0_results.RDS"))

# Copy over config.R script for record keeping and so it won't be modified while jobs are in queue
system(paste0("cp ", WORK_DIR, "/config.R ", OUT_DIR, "/config.R"))


# Submit stage jobs for each pair
mclapply(RO_PAIRS, function(pair) {
  submit_jobs(pair, WORK_DIR)
}, mc.cores = length(RO_PAIRS))

