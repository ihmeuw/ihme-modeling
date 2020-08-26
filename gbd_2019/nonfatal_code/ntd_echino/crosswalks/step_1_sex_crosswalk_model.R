#' [Title: CE Sex Crosswalk Model
#' [Author: Chase Gottlich
#' [Date:  07/12/19 (Decomp 2)
#' [Notes: Create Sex-Split Fit
#'  #TBD TO DO SEX SPLIT LINKS TO CROSSWALK AUTOMATICALLY, done in interms, data includes clinical data linked to bundle 60

#############################################################################################
###'                                [General Set-Up]                                      ###
#############################################################################################

rm(list = ls())

#'[ CREATE NEW RUN]

source("/ihme/homes/hcg1/repos/ntd_models/ntd_models/custom_functions/processing.R")
#gen_rundir(root = "/ihme/ntds", acause = "ntd_echino", message = "GBD 2019 Final")

# packages

source("/ihme/cc_resources/libraries/current/r/get_bundle_data.R")
source("/ihme/cc_resources/libraries/current/r/get_bundle_version.R")
library(data.table)
library(stringr)
library(metafor, lib.loc = "/homes/hcg1/r_packages")
library(msm, lib.loc = "/homes/hcg1/r_packages")

# mrbrt

repo_dir <- paste0("/home/j/temp/reed/prog/projects/run_mr_brt/")
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
source(paste0(repo_dir, "plot_mr_brt_function.R"))

## Set-up run directory

run_file <- fread(paste0("/ihme/ntds/ntd_models/ntd_models/ntd_echino/params/run_file.csv"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "/crosswalks/")

params_dir <- "/ihme/ntds/ntd_models/ntd_models/ntd_echino/params/"

#############################################################################################
###'                                  [Match]                                             ###
#############################################################################################

all_data <- get_bundle_version(ADDRESS)
all_data <- all_data[sex != "Both"]

# subset out 0 cases here so pair does not end up in
all_data <- all_data[cases != 0]
all_data <- all_data[case_name != "hydatiosis (cystic)"] # issues
all_data[, match := str_c(age_start, age_end, location_name, year_start, year_end, clinical_data_type, case_name)]

#6050 matches
dm    <- all_data[, .N, by = c("age_start", "age_end", "location_name", "year_start", "year_end", "clinical_data_type", "case_name")][N == 2]

cat(paste0("Matches are ", nrow(dm)))

matches <- dm[, str_c(age_start, age_end, location_name, year_start, year_end, clinical_data_type, case_name)]

# subset to the matches 

data_ss_subset <- all_data[match %in% matches]
#data_ss_subset_noclin <- data_ss_subset[clinical_data_type == ""]

# sort

setorder(data_ss_subset, age_start, age_end, location_name, year_start, year_end, clinical_data_type, case_name)
#setorder(data_ss_subset_noclin, age_start, age_end, location_name, year_start, year_end, clinical_data_type, case_name)

# correct names

data_ss_m <- data_ss_subset[sex == "Male", ]
data_ss_f <- data_ss_subset[sex == "Female",]

#data_ss_m_noclin <- data_ss_subset_noclin[sex == "Male", ]
#data_ss_f_noclin <- data_ss_subset_noclin[sex == "Female",]

# validate -- if all true then ready to clean and prep for mrbrt

all(data_ss_m[, .(age_start, age_end, location_name, year_start, year_end, clinical_data_type, case_name)] == data_ss_f[, .(age_start, age_end, location_name, year_start, year_end, clinical_data_type, case_name)])
#all(data_ss_m_noclin[, .(age_start, age_end, location_name, year_start, year_end, clinical_data_type, case_name)] == 
#      data_ss_f_noclin[, .(age_start, age_end, location_name, year_start, year_end, clinical_data_type, case_name)])

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

model_name <- "all_data_2017clin_trim_10_gbd2019_final"

fwrite(mrbrt_sheet, paste0(crosswalks_dir, model_name, ".csv"))

#############################################################################################
###'                                   [Fit]                                              ###
#############################################################################################

#' [MR-BRT -- save model fit as rds object] 
  
# Load matched sheet
sex_cw_sheet <- fread(paste0(crosswalks_dir, model_name, ".csv"))

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
  model_label = model_name,
  data        = sex_cw_sheet,
  #covs        = covs1,
  mean_var    = "ratio_log",
  se_var      = "ratio_se_log",
  method      = "trim_maxL",
  study_id    = "NID",
  trim_pct    = 0.10,
  overwrite_previous = TRUE
)

check_for_outputs(fit1)
plot_mr_brt(fit1)

saveRDS(fit1, paste0(crosswalks_dir, model_name, "/model_fit_obj.rds"))  
#test <- readRDS(paste0(crosswalks_dir, "mrbrt_sex_crosswalk_inc_clinical_data/model_fit.rds"))