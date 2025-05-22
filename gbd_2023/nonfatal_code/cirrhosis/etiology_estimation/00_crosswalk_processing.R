##########################################################################
### Purpose: Data processing - sex splitting, crosswalking, cirrhosis
##########################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "~/"
  l <- "FILEPATH"
} else {
  j <- "J:/"
  h <- "H:/"
  l <- "L:/"
}

date <- gsub("-", "_", Sys.Date())

library(crosswalk, lib.loc = FILEPATH)
library(mortdb, lib = FILPATH)
pacman::p_load(data.table, openxlsx, ggplot2, boot, gtools, msm, Hmisc)

gbd_round_id <- 7 
decomp_step <- "iterative"

# SET OBJECTS -------------------------------------------------------------

b_id <- BUNDLE_ID
a_cause <- ACAUSE
name <- NAME
bv_id <- BUNDLE_VERSION_ID
cv_drop <- c("")
outlier_status <- c(0)
sex_split <- T
id_vars <- "study_id"
sex_remove_x_intercept <- T
sex_covs <- ""
keep_x_intercept <- T
logit_transform <- T
reference <- ""
trim <- 0.1
measures <- "proportion"

file_path <- FILEPATH

if(logit_transform == T) {
  response <- "ldiff"
  data_se <- "ldiff_se"
  mrbrt_response <- "diff_logit"
} else {
  response <- "ratio_log"
  data_se <- "ratio_se_log"
  mrbrt_response <- "diff_log"
}



if(sex_split == T) { 
  ## Sex splitting only done in log space
  sex_split_response <- "log_ratio"
  sex_split_data_se <- "log_se"
  sex_mrbrt_response <- "diff_log"
}

draws <- paste0("draw_", 0:999)
mrbrt_dir <- FILEPATH
if (!dir.exists(mrbrt_dir)) dir.create(mrbrt_dir)

# SOURCE FUNCTIONS --------------------------------------------------------
source(FILEPATH)
source(FILEPATH)

source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_location_metadata.R")


# DATA PROCESSING FUNCTIONS -----------------------------------------------

# GET DATA FOR ADJUSTMENT 
orig_dt <- as.data.table(read.xlsx(file_path))
checkpoint <- copy(orig_dt)
cir_sex_dt <- copy(orig_dt)

# RUN SEX SPLIT -----------------------------------------------------------
# cir_sex_dt <- collapse_other(cir_sex_dt) only use for other
cir_sex_dt <- cir_sex_dt[measure %in% measures]
cir_sex_dt <- cir_sex_dt[is_outlier %in% outlier_status]
cir_sex_dt <- get_cases_sample_size(cir_sex_dt)
cir_sex_dt <- get_se(cir_sex_dt)
cir_sex_dt <- calculate_cases_fromse(cir_sex_dt)
cir_sex_dt <- cir_sex_dt[cases != 0 & sample_size != 0, ]
cir_sex_matches <- find_sex_match(cir_sex_dt)
unique(cir_sex_matches[, .N, by = c(sex_covs)])
mrbrt_sex_dt <- calc_sex_ratios(cir_sex_matches)
nrow(mrbrt_sex_dt)
length(unique(mrbrt_sex_dt$nid))


model_name <- paste0("sex_split_", date)
if (!dir.exists(paste0(mrbrt_dir, model_name))) dir.create(paste0(mrbrt_dir, model_name))

# Format the data for meta-regression 
if (sex_covs == "") {
  message("Formatting sex data without covariates")
  formatted_data <- CWData(
    df = mrbrt_sex_dt,
    obs = sex_split_response,
    obs_se = sex_split_data_se,
    alt_dorms = "dorm_alt",
    ref_dorms = "dorm_ref",
    study_id = id_vars
  )
} else {
  message("Formatting sex data with covariates")
  formatted_data <- CWData(
    df = mrbrt_sex_dt,
    obs = sex_split_response,
    obs_se = sex_split_data_se,
    alt_dorms = "dorm_alt",
    ref_dorms = "dorm_ref",
    covs = list(sex_covs),
    study_id = id_vars
  )
}


## Launch MR-BRT Sex Model -------------------------------------
if (file.exists(FILEPATH){
  sex_results <- readr::read_rds(FILEPATH)
} else {
  sex_results <- CWModel(
    cwdata = formatted_data,
    obs_type = sex_mrbrt_response,
    cov_models = list(CovModel("intercept")),
    gold_dorm = "Male",
    inlier_pct = (1 - trim)
  )
  save <- save_model_RDS(sex_results, mrbrt_dir, model_name)
}

save_r <- save_mrbrt(sex_results, mrbrt_dir, model_name)

sex_results$fixed_vars

# Make funnel plots
repl_python()

plots <- import("crosswalk.plots")

plots$funnel_plot(
  cwmodel = sex_results, 
  cwdata = formatted_data,
  continuous_variables = list(),
  obs_method = "Female",
  plot_note = a_cause, 
  plots_dir = paste0(mrbrt_dir, model_name), 
  file_name = model_name,
  write_file = TRUE
)

# ADJUST VALUES FOR SEX SPLITTING FUNCTION -----------------------
# Sex splitting 
full_dt <- copy(orig_dt)
full_dt <- full_dt[!(icd_code %in% c(1))] # get rid of any icd code data 
dt_sex_split <- split_both_sex(full_dt, sex_results)
final_sex_dt <- copy(dt_sex_split$final)
graph_sex_dt <- copy(dt_sex_split$graph)

write.csv(final_sex_dt, FILEPATH, row.names = FALSE)

sex_graph <- graph_sex_predictions(graph_sex_dt)
sex_graph
ggsave(filename =FILEPATH, plot = sex_graph, width = 6, height = 6)

# WRITE FINAL DATA 

bundle_data <- get_bundle_version(bv_id, fetch = "all")
bundle_data1 <- bundle_data[, .(id_var, seq)]
setnames(bundle_data1, "seq", "bv_seq")
final_sex_dt1 <- merge(final_sex_dt, bundle_data1, by = "id_var")
nrow(final_sex_dt) == nrow(final_sex_dt1)
final_sex_dt1[, `:=` (crosswalk_parent_seq = bv_seq, seq = NA, bv_seq = NA)]
final_sex_dt1[mean > 1, `:=` (mean = 1, note_modeler = paste0(note_modeler, " | capping sex split mean at 1"))]
final_sex_dt1[is.na(upper) & is.na(lower) & !is.na(uncertainty_type_value), uncertainty_type_value := NA]

sex_split_filepath <- FILEPATH
write.xlsx(final_sex_dt, sex_split_filepath, sheetName = "extraction", row.names = FALSE)

