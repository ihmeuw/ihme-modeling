#' [Title: CL Sex Crosswalk Model
#' [Notes: Create Sex-Split Fit

#############################################################################################
###'                                [General Set-Up]                                      ###
#############################################################################################

rm(list = ls())

#'[ CREATE NEW RUN]

source("FILEPATH")

# packages

source("FILEPATH")
library(data.table)
library(stringr)
library(metafor, lib.loc = "FILEPATH")
library(msm, lib.loc = "FILEPATH")

# mrbrt

repo_dir <- paste0("FILEPATH")
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))

## Set-up run directory

run_file <- fread("FILEPATH")
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "FILEPATH")

params_dir <- "FILEPATH"


#############################################################################################
###'                                  [Match]                                             ###
#############################################################################################

all_data <- fread("FILEPATH")

# 
all_data <- all_data[cases != 0]
all_data[, match := str_c(age_start, age_end, location_name, year_start, year_end, measure, case_name, site_memo)]

cat(paste0("Matches : ", nrow(dm)))

#
dm    <- all_data[, .N, by = c("age_start", "age_end", "location_name", "year_start", "year_end", "measure", "case_name", "site_memo")][N == 2]

cat(paste0("Matches are ", nrow(dm)))

matches <- dm[, str_c(age_start, age_end, location_name, year_start, year_end, measure, site_memo, case_name)]

# subset to the matches 
data_ss_subset <- all_data[match %in% matches]

# sort
setorder(data_ss_subset, age_start, age_end, location_name, year_start, year_end, measure, site_memo, case_name)

# 
data_ss_m <- data_ss_subset[sex == "Male", ]
data_ss_f <- data_ss_subset[sex == "Female",]

# validate
all_valid <- all(data_ss_m[, .(age_start, age_end, location_name, year_start, year_end, site_memo, measure, case_name)] == data_ss_f[, .(age_start, age_end, location_name, year_start, year_end, site_memo, measure, case_name)])

if (!(all_valid)){
  stop("Not all valid -- male and female not same")
} else {
  cat("\n All valid - male and female line up \n  ")
}

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

model_name <- "FILEPATH"

fwrite(mrbrt_sheet, paste0(crosswalks_dir, model_name, ".csv"))

#############################################################################################
###'                                   [Fit]                                              ###
#############################################################################################

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

saveRDS(fit1, paste0(crosswalks_dir, model_name, "/FILEPATH"))  