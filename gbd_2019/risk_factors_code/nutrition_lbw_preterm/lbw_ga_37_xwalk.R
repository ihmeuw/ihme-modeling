rm(list=ls())

os <- .Platform$OS.type
my_libs <- "FILEPATH"

library(data.table)
library(magrittr)
library(ggplot2)
library(metafor, lib.loc = FILEPATH)
library(ggpubr, lib = my_libs)
library(cowplot, lib = my_libs)
library(readxl)
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


# ----- Set constants & directories

input_dir = "FILEPATH"
output_dir = "FILEPATH"
mrbrt_dir = "FILEPATH" 
diag_dir = file.path(mrbrt_dir, "diagnostics")

locs = get_location_metadata(location_set_id = 9)

trim_percent = 0.05
num_yrs = 5
model.label = paste0("lbw_ga_37_xw_GS_ga_37_NUMYRS_", num_yrs, "_ME_lbw_ga_37_X_logmeannum_Z_withinStudy_yearDist_", trim_percent)
dir.create(file.path(diag_dir, model.label))


# ----- Get LBW data

lbw <- fread(file.path("FILEPATH"))

# Change to sex_id
lbw[sex == "Male", sex_id := 1]
lbw[sex == "Female", sex_id := 2]

# log transform
lbw[, mean_log := log(mean)]
lbw[, mean_se_log := sqrt((1/(mean - mean^2))^2 * standard_error^2)] # delta transformation

# ----- Get post-crosswalked ga_37 data

ga_37 <- data.table(read_excel("FILEPATH"))

# ----- Prepare for matching

ga_37[, cv_ga_37 := 1]
lbw[, cv_lbw := 1]

to_match <- rbind(ga_37, lbw, use.names = T, fill = T)
to_match[cv_lbw == 1, cv_ga_37 := 0]
to_match[cv_ga_37 == 1, cv_lbw := 0]

to_match[, mid_year := ( year_start + year_end ) / 2]
to_match[, mid_year_cat := cut(mid_year, breaks = seq(1975, 2025, num_yrs))]

to_match[, temp_index := .I]

make_matches <- function(data, ratio.num, ratio.denom, direct_to_reference){
  
  print(paste0("This ratio informs the adjustment of ", ratio.num, " (numerator) to ", ratio.denom, " (denominator). Is ", ratio.denom, " the bundle gold standard? ", direct_to_reference))
  
  dat <- copy(data)
  
  matches <- merge(dat[get(ratio.denom) == 1, .(location_id, sex, sex_id, mid_year_cat, index.denom = temp_index, mean.denom = mean, se.denom = standard_error, nid.denom = nid, year_id.denom = (year_start + year_end)/2)], 
                   dat[get(ratio.num) == 1, .(location_id, sex, sex_id, mid_year_cat, index.num = temp_index, mean.num = mean, se.num = standard_error, nid.num = nid, year_id.num = (year_start + year_end)/2)], allow.cartesian = T)
  
  matches[, ratio := mean.num / mean.denom]
  matches[, ratio_se := sqrt((mean.denom^2 / mean.num^2) * ((se.denom^2 / mean.denom^2)+(se.num^2/mean.num^2)))]
  
  matches[, (ratio.num) := 1]
  
  if(direct_to_reference == F){
    matches[, (ratio.denom) := -1]
  }
  
  return(matches)
  
}

matches <- make_matches(data = to_match, ratio.num = "cv_lbw", ratio.denom = "cv_ga_37", direct_to_reference = T)

matches[, within_study := 0]
matches[year_id.denom == year_id.num & nid.denom == nid.num, within_study := 1]

matches[, year_dist := abs(floor(year_id.denom - year_id.num))]
matches[year_dist > 4, year_dist := 4] 

matches[, ratio_log := log(ratio)]
matches[, ratio_se_log := sqrt((1/(ratio - ratio^2))^2 * ratio_se^2)]

matches[, mean.num_log := log(mean.num)]

# ----- Apply crosswalk outliers in both matches & dataset

matches[, is_xwalk_outlier := 0]
matches[mean.num <= 0 | mean.denom <= 0, is_xwalk_outlier := 1]

matches[mean.num > 0.6 | mean.denom > 0.6, is_xwalk_outlier := 1]
matches[se.denom > 30, is_xwalk_outlier := 1]

fit <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = model.label, 
  data = matches[is_xwalk_outlier == 0], 
  mean_var = "ratio_log",
  se_var = "ratio_se_log", trim_pct = trim_percent,
  method='trim_maxL',
  overwrite_previous = T,
  covs = list(
    cov_info("mean.num_log", "X"),
    cov_info("within_study", "Z"),
    cov_info("year_dist", "Z")
  )
)

plot_mr_brt(fit, dose_vars = "mean.num_log")

saveRDS(fit, file.path(output_dir, "lbw_ga_37", "lbw_ga_37_ratio.rds"))