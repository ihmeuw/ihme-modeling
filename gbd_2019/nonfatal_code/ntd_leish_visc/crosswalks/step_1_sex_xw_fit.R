# VL sex crosswalk fit

### ----------------------- Set-Up ------------------------------

os <- .Platform$OS.type
if (os=="windows") {
  j <-"FILEPATH"
  h <-"FILEPATH"
} else {
  j <-"FILEPATH"
  h <-paste0("FILEPATH", Sys.info()[7], "/")
}

source("FILEPATH")
library(data.table)
library(stringr)
library(metafor, lib.loc = "FILEPATH")
library(msm)

repo_dir <- paste0(j, "FILEPATH")
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
source(paste0(repo_dir, "plot_mr_brt_function.R"))

## Set-up run directory

run_file <- fread(paste0("FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "FILEPATH")

params_dir <- "FILEPATH"

### ----------------------- Match ------------------------------

# all_age VL prevalence
b_ADDRESS1 <- fread("FILEPATH")
# age_spec VL prevalence
b_ADDRESS2 <- fread("FILEPATH")
# all_age VL incidence
b_ADDRESS3 <- fread("FILEPATH")
# age_spec VL incidence
b_ADDRESS4 <- fread("FILEPATH")

col_names <- Reduce(intersect, list(names(b_ADDRESS1), names(b_ADDRESS2), names(b_ADDRESS3), names(b_ADDRESS4)))
all_data  <- rbind(b_ADDRESS3[, ..col_names], b_ADDRESS4[, ..col_names])

# subset out 0 cases here so pair does not end up in
all_data <- all_data[cases != 0]
all_data[, match := str_c(age_start, age_end, location_name, year_start, year_end, measure, case_name, site_memo)]

dm    <- all_data[, .N, by = c("age_start", "age_end", "location_name", "year_start", "year_end", "measure", "case_name", "site_memo")][N == 2]
matches <- dm[, str_c(age_start, age_end, location_name, year_start, year_end, measure, site_memo, case_name)]

# subset to the matches 

data_ss_subset <- all_data[match %in% matches]

# sort

setorder(data_ss_subset, age_start, age_end, location_name, year_start, year_end, measure, site_memo, case_name)

# correct names

data_ss_m <- data_ss_subset[sex == "Male", ]
data_ss_f <- data_ss_subset[sex == "Female",]

# validate -- if all true then ready to clean and prep for mrbrt

all(data_ss_m[, .(age_start, age_end, location_name, year_start, year_end, site_memo, measure, case_name)] == data_ss_f[, .(age_start, age_end, location_name, year_start, year_end, site_memo, measure, case_name)])

# mrbrt columns - NID - Author - year - cases_1 - sample_size_1 = mean_1 - se_1 - dx_1 //2's - ratio ref_to_alt - se_ratio - compar - group_id

mrbrt_sheet <- data.table(
  NID = paste0(data_ss_m[, nid], "-", data_ss_f[, nid]),
  Author = "",
  year = data_ss_m[, year_start],
  cases_1 = data_ss_m[, cases],
  sample_size_1 = data_ss_m[, sample_size],
  mean_1 = data_ss_m[, mean],
  se_1 = data_ss_m[, standard_error],
  dx_1 = "male",
  cases_2 = data_ss_f[, cases],
  sample_size_2 = data_ss_f[, sample_size],
  mean_2 = data_ss_f[, mean],
  se_2 = data_ss_f[, standard_error],
  dx_2 = "female")

fwrite(mrbrt_sheet, paste0(crosswalks_dir, "FILEPATH"))

#' [MR-BRT -- save model fit as rds object] 

# Load matched sheet

sex_cw_sheet <- fread(paste0(crosswalks_dir, "FILEPATH"))

# Log-Transform

sex_cw_sheet[, ratio := cases_1 / cases_2]
sex_cw_sheet[, ratio_se := sqrt(ratio*((se_1^2/cases_1^2)+(se_2^2/cases_2^2)))]
sex_cw_sheet[, ratio_log := log(ratio)]

sex_cw_sheet$ratio_se_log <- sapply(1:nrow(sex_cw_sheet), function(i) {
  ratio_i    <- sex_cw_sheet[i, "ratio"]
  ratio_se_i <- sex_cw_sheet[i, "ratio_se"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

# Fit Model

fit1 <- run_mr_brt(
  output_dir  = crosswalks_dir,
  model_label = "sex_matching_all_data_only_inc",
  data        = sex_cw_sheet,
  #covs        = covs1,
  mean_var    = "ratio_log",
  se_var      = "ratio_se_log",
  #method      = "trim_maxL",
  study_id    = "NID",
  #trim_pct    = 0.00,
  overwrite_previous = TRUE
)

check_for_outputs(fit1)
plot_mr_brt(fit1)

saveRDS(fit1, paste0(crosswalks_dir, "FILEPATH"))  