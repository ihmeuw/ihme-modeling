#' [Title: FBT Sex Crosswalk Model
#' [Notes: Create All-Species Sex-Split Fit from between species comparisons

# Sex_Split using matching between specicies
# 1) Match 
# 2) MR-BRT



#############################################################################################
###'                                [General Set-Up]                                      ###
#############################################################################################

# clear area
rm(list = ls())

# create root
root <- paste0("FILEPATH")

#'[ 1) create new run id
#
source(paste0("FILEPATH/processing.R"))

# packages
source("FILEPATH/get_bundle_data.R")
library(data.table)
library(stringr)
library(metafor, lib.loc = "FILEPATH")
library(msm, lib.loc = "FILEPATH")

# mrbrt
repo_dir <- paste0("FILEPATH")
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
source(paste0(repo_dir, "plot_mr_brt_function.R"))

## Set-up run directory
run_file <- fread(paste0("FILEPATH/run_file.csv"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "FILEPATH")
params_dir <- "FILEPATH/"

#############################################################################################
###'                                  [Match]                                             ###
#############################################################################################

# compile all case data

data_a <- fread("FILEPATH/data.csv")
data_a[, species := "clono"]

data_b <- fread("FILEPATH/data.csv")
data_b[, species := "fascio"]

data_c <- fread("FILEPATH/data.csv")
data_c[, species := "fluke"]

data_d <- fread("FILEPATH/data.csv")
data_d[, species := "opistho"]

data_e <- fread("FILEPATH/data.csv")
data_e[, species := "paragon"]

data_f <- fread("FILEPATH/data.csv")
data_f[, species := "c_paragon"]
             
col_names <- Reduce(intersect, list(names(data_a), names(data_b), names(data_c), names(data_d), names(data_e), names(data_f)))
all_data <- rbind(data_a[, ..col_names], data_b[, ..col_names], data_c[, ..col_names], data_d[, ..col_names], data_e[, ..col_names], data_f[, ..col_names])

#' [Direct Matches]
  
all_data <- all_data[sex != "Both"]
  
# subset out 0 cases here so pair does not end up in
all_data <- all_data[cases != 0]
all_data[, match := str_c(age_start, age_end, location_name, year_start, year_end, species)]


dm      <- all_data[, .N, by = c("age_start", "age_end", "location_name", "year_start", "year_end", "species")][N == 2]
  
cat(paste0("Matches are ", nrow(dm)))
  
matches <- dm[, str_c(age_start, age_end, location_name, year_start, year_end, species)]
  
# subset to the matches 
  
data_ss_subset <- all_data[match %in% matches]
  
# sort
  
setorder(data_ss_subset, year_start, year_end, age_start, age_end, location_name, species)
  
# correct names
  
data_ss_m <- data_ss_subset[sex == "Male", ]
data_ss_f <- data_ss_subset[sex == "Female",]
  
# validate -- if all true then ready to clean and prep for mrbrt
  
all(data_ss_m[, .(year_start, year_end, age_start, age_end, location_name, species)] == data_ss_f[, .(year_start, year_end, age_start, age_end, location_name, species)])
  
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
  
model_name <- "all_species_no_trim"

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
  #method      = "trim_maxL",
  study_id    = "NID",
  #trim_pct    = 0.00,
  overwrite_previous = TRUE
  )
  
check_for_outputs(fit1)
plot_mr_brt(fit1)

# save out object
saveRDS(fit1, paste0(crosswalks_dir, model_name, "/model_fit_obj.rds"))   