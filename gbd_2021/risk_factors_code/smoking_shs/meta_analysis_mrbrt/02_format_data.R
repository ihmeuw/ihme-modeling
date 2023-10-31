#-------------------Header------------------------------------------------
# SECONHAND SMOKE - GBD 2020
# Purpose: Prepare data for mr-brt meta-analysis
#------------------Set-up--------------------------------------------------

library(dplyr)
library(parallel)
library(data.table)

WORK_DIR <- "FILEPATH"
CODE_PATH <- paste0(WORK_DIR, "FILEPATH")
source(paste0(CODE_PATH, "/config.R"))

#--------------------------------------------------------------------------
for (pair in RO_PAIRS) {
  
  df <- read.csv(paste0(INPUT_DATA_DIR, "/", pair, ".csv")) %>%
    filter(complete.cases(.[, c(REF_EXPOSURE_COLS, ALT_EXPOSURE_COLS)]))
  
  # Checking if there are observations whee reference value is higher than alternative exposure 
  if (sum(df$conc_ref > df$conc) > 0) stop("Observations with higher reference than alternative exposure")
  
  # Make sure bias covariates have at least two studies in both reference and alternative (assuming study count > 4?)
  bias_covs <- names(df)[names(df) %like% 'cv_']
  
  for (covs in bias_covs) {
    source_counts <- df %>%
      group_by(!! rlang::sym(covs)) %>%
      summarize(n_studies = length(unique(nid)) ) %>%
      filter(n_studies >= 2) %>%
      as.data.frame(.)
    
    if (!all(0:1 %in% source_counts[,1]) & length(unique(df$nid)) > 4) {
      bias_covs <- bias_covs[!bias_covs == covs]
    }
  }
  
  # Format dataset
  df <- df[, c(STUDY_ID_VAR, "age_start", "age_end", OBS_VAR, OBS_SE_VAR, REF_EXPOSURE_COLS, ALT_EXPOSURE_COLS, bias_covs)] %>%
    arrange(nid)
  
  if (lung_outlier ==TRUE & pair == "neo_lung"){
    df <- as.data.table(df)
    df <- df[!(nid == 387696 & log_rr <= -0.10),]
    df$ref <- as.numeric(df$ref)
    df <- as.data.frame(df)
  }
  
  out <- list(
    df=df, ro_pair=pair, x_covs=bias_covs,
    obs_var=OBS_VAR, obs_se_var=OBS_SE_VAR,
    ref_vars=REF_EXPOSURE_COLS, alt_vars=ALT_EXPOSURE_COLS,
    study_id_var=STUDY_ID_VAR
    
  )
  
 
  saveRDS(out, paste0(OUT_DIR, "FILEPATH", pair, ".RDS"))
  write.csv(df, paste0(OUT_DIR, "FILEPATH", pair, ".csv"))

}
 


