#' [Title: Leprosy Sex Crosswalk Model
#' [Notes: Create Sex-Split Fit

#############################################################################################
###'                                [General Set-Up]                                      ###
#############################################################################################

rm(list = ls())

#'[ Create new run id if needed]
source("FILEPATH")
#gen_rundir(root = "FILEPATH", acause = "leprosy", message = "GBD 2019 Final")

# packages
source("FILEPATH")
library(data.table)
library(stringr)
library(metafor, lib.loc = "FILEPATH")
library(msm, lib.loc = "FILEPATH")

# mrbrt functions
repo_dir <- "FILEPATH"
source("FILEPATH"))
source("FILEPATH"))
source("FILEPATH"))
source("FILEPATH"))
source("FILEPATH"))
source("FILEPATH"))
source("FILEPATH"))
source("FILEPATH"))

## Set-up run directory
run_file <- fread("FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "/crosswalks/")

params_dir <- "FILEPATH"


#############################################################################################
###'                                  [Match]                                             ###
#############################################################################################

# matching off sheets
library(data.table)
library(openxlsx)
library(stringr)
library(tidyr)

Sci_lit    <- fread("FILEPATH")
Sinan      <- fread("FILEPATH")

col_names <- intersect(names(Sci_lit), names(Sinan))
data      <- rbind(Sci_lit[, ..col_names], Sinan[, ..col_names])

#' [Direct Matches]
data <- data[sex != "Both"]
  
# subset out 0 cases here so pair does not end up in
data <- data[cases != 0]

#'[match on quantities on the RHS, if change, change throughout]
data[, match := str_c(age_start, age_end, location_name, year_start, year_end, case_name, site_memo)]

dm      <- data[, .N, by = c("age_start", "age_end", "location_name", "year_start", "year_end", "case_name", "site_memo")][N == 2]
cat(paste0("Matches : ", nrow(dm)))
matches <- dm[, str_c(age_start, age_end, location_name, year_start, year_end, case_name, site_memo)]

# subset to the matches 
data_ss_subset <- data[match %in% matches]
  
# sort
setorder(data_ss_subset, year_start, year_end, age_start, age_end, location_name, site_memo, case_name)
  
# correct names
data_ss_m <- data_ss_subset[sex == "Male", ]
data_ss_f <- data_ss_subset[sex == "Female",]
  
# validate -- if all true then ready to clean and prep for mrbrt
all(data_ss_m[, .(year_start, year_end, age_start, age_end, location_name, site_memo)] == data_ss_f[, .(year_start, year_end, age_start, age_end, location_name, site_memo)])
  
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

model_name <- "new_dir"  
fwrite(mrbrt_sheet, "FILEPATH"))


#############################################################################################
###'                                   [Fit]                                              ###
#############################################################################################

#' [MR-BRT -- save model fit as rds object] 
  
# Load matched sheet
sex_cw_sheet <- fread("FILEPATH"))
  
# Log-Transform
sex_cw_sheet[, ratio := cases_1 / cases_2]
sex_cw_sheet[, ratio_se := sqrt(ratio*((se_1^2/cases_1^2)+(se_2^2/cases_2^2)))]
sex_cw_sheet[, ratio_log := log(ratio)]
  
sex_cw_sheet$ratio_se_log <- sapply(1:nrow(sex_cw_sheet), function(i) {
  ratio_i    <- sex_cw_sheet[i, "ratio"]
  ratio_se_i <- sex_cw_sheet[i, "ratio_se"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})
  
sex_cw_sheet <- fread("FILEPATH")

# Fit Model
fit1 <- run_mr_brt(
    output_dir  = paste0(crosswalks_dir),
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
  
saveRDS(fit1, "FILEPATH"))

# supporting https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3519584/
# exact match https://www.ncbi.nlm.nih.gov/pubmed/16295738
# also supports https://onlinelibrary.wiley.com/doi/full/10.1111/ijd.14148
