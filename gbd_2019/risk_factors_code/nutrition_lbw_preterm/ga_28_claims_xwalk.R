rm(list=ls())

os <- .Platform$OS.type
my_libs <- "FILEPATH"

library(data.table)
library(magrittr)
library(ggplot2)
library(metafor, lib.loc = "FILEPATH")
library(msm)
library(ggpubr, lib = my_libs)
library(cowplot, lib = my_libs)
repo_dir <- "FILEPATH"
source("FILEPATH/merge_on_location_metadata.R")
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
source("FILEPATH/mr_brt_functions.R")
source("FILEPATH/merge_on_location_metadata.R")
source("FILEPATH/get_location_metadata.R"  )

ratio_dir = "FILEPATH"
mrbrt_dir = "FILEPATH" 
diag_dir = file.path(mrbrt_dir, "diagnostics")
trim_percent = 0.05
num_yrs = 10
model.label = paste0("ga_28_xw_outlier_MS2000_NUMYR_", num_yrs, "_GS_other_X_claims_TRIM_", trim_percent)
dir.create(file.path(diag_dir, model.label))

ga_28 <- fread("FILEPATH")

ga_28 <- ga_28[measure == "prevalence"]
ga_28 <- ga_28[age_start ==0 & age_end == 0]

ga_28[, temp_index := .I]

ga_28[(cv_claims == 0 & cv_inpatient == 0), cv_other := 1]
ga_28[is.na(cv_other), cv_other := 0]

ga_28[cv_claims == 1, type := "claims"]
ga_28[cv_inpatient == 1, type := "inpatient"]
ga_28[cv_other == 1, type := "other"]

ga_28[sex == "Male", sex_id := 1]
ga_28[sex == "Female", sex_id := 2]

ga_28[, mean_log := log(mean)]
ga_28[, mean_se_log := sqrt((1/(mean - mean^2))^2 * standard_error^2)] 

saveRDS(ga_28, file.path(ratio_dir, "ga_28", "preadjusted_data.rds"))

ga_28[, mid_year := ( year_start + year_end ) / 2]
ga_28[, mid_year_cat := cut(mid_year, breaks = seq(1990, 2020, num_yrs))]

make_matches <- function(data, ratio.num, ratio.denom, direct_to_reference){
    
  print(paste0("This ratio informs the adjustment of ", ratio.denom, " (denominator) to ", ratio.num, " (numerator). Is ", ratio.denom, " the bundle gold standard? ", direct_to_reference))
  
  dat <- copy(data)
  
  matches <- merge(dat[get(ratio.denom) == 1, .(location_id, sex, sex_id, mid_year_cat, index.denom = temp_index, mean.denom = mean, se.denom = standard_error, nid.denom = nid)], 
                   dat[get(ratio.num) == 1, .(location_id, sex, sex_id, mid_year_cat, index.num = temp_index, mean.num = mean, se.num = standard_error, nid.num = nid)], allow.cartesian = T)
  
  matches[, ratio := mean.num / mean.denom]
  matches[, ratio_se := sqrt((mean.denom^2 / mean.num^2) * ((se.denom^2 / mean.denom^2)+(se.num^2/mean.num^2)))]
  
  matches[, (ratio.num) := 1]

  if(direct_to_reference == F){
    matches[, (ratio.denom) := -1]
  }
  
  return(matches)
  
}

matches <- make_matches(data = ga_28, ratio.num = "cv_claims", ratio.denom = "cv_other", direct_to_reference = T)
matches[is.na(cv_claims), cv_claims := 0]

matches[, ratio_log := log(ratio)]
matches[, ratio_se_log := sqrt((1/(ratio - ratio^2))^2 * ratio_se^2)]

matches[, is_xwalk_outlier := 0]
matches[mean.num <= 0 | mean.denom <= 0 | ratio_log == Inf | ratio_log == -Inf, is_xwalk_outlier := 1]

ms2000 <- ga_28[field_citation_value %like% "United States MarketScan Claims and Medicare Data - 2000", temp_index]
matches[index.num %in% ms2000, is_xwalk_outlier := 1]

matches[, .N, by = is_xwalk_outlier]

fit <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = model.label, 
  data = matches[is_xwalk_outlier == 0], 
  mean_var = "ratio_log",
  se_var = "ratio_se_log", trim_pct = trim_percent,
  method='trim_maxL',
  overwrite_previous = T,
  covs = list()
)

plot_mr_brt(fit)

saveRDS(fit, file.path(ratio_dir, "ga_28", "cv_claims_ratio.rds"))

