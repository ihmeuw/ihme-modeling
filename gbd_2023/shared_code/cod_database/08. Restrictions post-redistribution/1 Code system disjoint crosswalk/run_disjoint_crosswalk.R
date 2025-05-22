library(dplyr)
library(data.table)
library(arrow)

reticulate::use_python(FILEPATH)

cw <- reticulate::import("crosswalk")

args = commandArgs(TRUE)
run_id <- args[1]
cause <- args[2]
prior_strength <- as.integer(args[3])
len_locs <-  as.integer(args[4]) + 4
locations <- as.list(args[5:len_locs])

log_transform <- function(df, mean_col, sd_col) {
  df[, paste0("log_", c(mean_col, sd_col)) := cw$utils$linear_to_log(
    array(df[, get(mean_col)]), array(df[, get(sd_col)])
  ) %>% as.data.table
  ]
}

calculate_diff <- function(df, alt_mean, alt_sd, ref_mean, ref_sd) {
  dat_diff <- data.table(mean_alt = df[, get(alt_mean)], mean_se_alt = df[, get(alt_sd)], mean_ref = df[, get(ref_mean)], mean_se_ref = df[, get(ref_sd)])
  dat_diff[, `:=` (diff= mean_alt-mean_ref, diff_se= sqrt(mean_se_alt^2 + mean_se_ref^2))] 
  return(dat_diff[,c("diff", "diff_se")])
}

get_adjustment_factor <- function(df, prior_strength) {
  model = cw$CWModel(
    cwdata = cw$CWData(
      df = df,
      obs = "log_diff_mean",
      obs_se = "log_diff_se",
      alt_dorms = "alt_dorms",
      ref_dorms = "ref_dorms",
      study_id = NULL
    ),
    obs_type = "diff_log",
    cov_models = list(cw$CovModel("intercept", prior_beta_gaussian = list(ICD9 = array(c(0, prior_strength))))),
    gold_dorm = "ICD10"
  )
  model$fit()
  beta = model$beta[2]
  exp(-beta)
}

for (location in unique(locations)){
  print(prior_strength)
  print(paste(location, cause))
  print(sprintf(FILEPATH, run_id))

  df = open_dataset(sprintf(FILEPATH, run_id)) %>%
    filter(location_id == as.integer(location), cause_id == as.integer(cause)) %>% 
    collect %>%
    as.data.table %>%
    log_transform("cf_icd9", "cf_se_icd9") %>%
    log_transform("cf_icd10", "cf_se_icd10") %>%
    .[, c("log_diff_mean", "log_diff_se") := calculate_diff(
      .SD,
      alt_mean = "log_cf_icd9",
      alt_sd = "log_cf_se_icd9",
      ref_mean = "log_cf_icd10",
      ref_sd = "log_cf_se_icd10"
    ) %>% as.data.table] %>%
    .[, `:=`(alt_dorms = "ICD9", ref_dorms = "ICD10")] %>%
    .[, get_adjustment_factor(.SD, prior_strength), by = .(data_type_id, location_id, age_group_id, sex_id, cause_id)] %>%
    rename(adjustment_factor = V1)

  if (file.exists(sprintf("FILEPATH", run_id))){
    tmp = as.data.table(collect(open_dataset(sprintf(FILEPATH, run_id))))
    df = rbind(tmp, df, fill=TRUE)
  }
  write_parquet(df, sprintf(FILEPATH, run_id))
}