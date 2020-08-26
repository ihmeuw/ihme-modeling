
### ----------------------- Set-Up ------------------------------

#rm(list=ls())
source("FILEPATH")
source("FILEPATH")
library(data.table)
library(stringr)
repo_dir <- "FILEPATH"
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))

library(readxl)
library(dplyr)
library(data.table)
library(msm)
library(msm, lib.loc = "FILEPATH")
library(metafor, lib.loc = "FILEPATH")
library(metafor)


### ----------------------- Match ------------------------------

all_data<- dedup_dengue_data

data <- as.data.table(all_data)

#' [Direct Matches]
data <- all_data[sex != "Both"]
#3381 sex specific rows

# subset out 0 cases here so pair does not end up in

data <- data[cases != 0]
data<- subset(data, sample_size!=cases)

data[, match := str_c(age_start, age_end, location_name, year_start, year_end)]



dm      <- data[, .N, by = c("age_start", "age_end", "location_name", "year_start", "year_end" )][N > 1]
dm      <- dm[N == 2]
matches <- dm[, str_c(age_start, age_end, location_name, year_start, year_end)]
matches <- matches[!(is.na(matches))]
# subset to the matches 

data_ss_subset <- data[match %in% matches]

# sort

setorder(data_ss_subset, year_start, year_end, age_start, age_end, location_name)

# correct names

data_ss_m <- data_ss_subset[sex == "Male", ]
data_ss_f <- data_ss_subset[sex == "Female",]

# validate -- if all true then ready to clean and prep for mrbrt

all(data_ss_m[, .(year_start, year_end, age_start, age_end, location_name)] == data_ss_f[, .(year_start, year_end, age_start, age_end, location_name)])

# mrbrt columns - NID - Author - year - cases_1 - sample_size_1 = mean_1 - se_1 - dx_1 //2's - ratio ref_to_alt - se_ratio - compar - group_id

mrbrt_sheet <- data.table(
  NID = paste0(data_ss_m[, nid], "-", data_ss_f[, nid]),
  #Author = "",
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
  dx_2 = "female",
  comparison='male_female')


sex_cw_sheet<- mrbrt_sheet

#calculate ratio and se
sex_cw_sheet[, ratio := cases_1 / cases_2]
sex_cw_sheet[, ratio_se := sqrt(ratio*((se_1^2/cases_1^2)+(se_2^2/cases_2^2)))]

#outliering some data
sex_cw_sheet <- subset(sex_cw_sheet, ratio <= 1.5)


#log transform:
sex_cw_sheet[, ratio_log := log(ratio)]

sex_cw_sheet$ratio_se_log <- sapply(1:nrow(sex_cw_sheet), function(i) {
  ratio_i    <- sex_cw_sheet[i, "ratio"]
  ratio_se_i <- sex_cw_sheet[i, "ratio_se"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})


sex_cw_sheet <- subset(sex_cw_sheet, ratio != 1)
#1492

fwrite(sex_cw_sheet, "FILEPATH")


 # Fit Model
 
 fit1_dengue <- run_mr_brt(
    output_dir  = 'FILEPATH',
    model_label = "sex_crosswalk_results_dengue",
    data        = sex_cw_sheet,
    mean_var    = "ratio_log",
    se_var      = "ratio_se_log",
    study_id    = "NID",
    overwrite_previous = TRUE
  )
 
# 
  check_for_outputs(fit1_dengue)
# plot_mr_brt(fit1)

saveRDS(fit1_dengue, "FILEPATH")
