
### ----------------------- Set-Up ------------------------------

rm(list=ls())
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


## Set-up run directory

run_file <- fread(paste0("FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]

crosswalks_dir    <- paste0(run_dir, "")

### ----------------------- Match ------------------------------


all_data<- get_bundle_version(bundle_version_id = ADDRESS, export=FALSE)

data <- as.data.table(all_data)

#' [Direct Matches]
data <- all_data[sex != "Both"]


data <- data[cases != 0]
data<- subset(data, sample_size!=cases)

dat_original_mansoni<- subset(data, case_name=="S mansoni" | case_name=="S intercalatum"| case_name=="S mekongi")

dat_original_mansoni$case_diagnostics <- as.character(dat_original_mansoni$case_diagnostics)
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz, FEC" | dat_original_mansoni$case_diagnostics=="Kato-Katz, IHA" | dat_original_mansoni$case_diagnostics=="Kato-Katz, sedimentation" | dat_original_mansoni$case_diagnostics=="Kato-Katz, sedimentation, serology" |  dat_original_mansoni$case_diagnostics=="Kato-Katz"] <- "Kato-Katz"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="sedimentation" | dat_original_mansoni$case_diagnostics=="saline sedimentation technique for protozoan and intestinal parasites; digestion technique to determine the intensity of infection"] <- "sed"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="fecal smear, formol ether" | dat_original_mansoni$case_diagnostics=="fecal smear"] <- "Kato-Katz"

dat_original_mansoni<- subset(dat_original_mansoni, dat_original_mansoni$case_diagnostics != "centrifugation" & dat_original_mansoni$case_diagnostics != "FLOTAC" & dat_original_mansoni$case_diagnostics != "gold" & dat_original_mansoni$case_diagnostics!="MFIC" & dat_original_mansoni$case_diagnostics != "NA" & dat_original_mansoni$case_diagnostics != "CCA or Kato-Katz")

dat_original_mansoni$num_samples <- as.double(dat_original_mansoni$num_samples)

#renaming values under case_diagnostics

dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz" & dat_original_mansoni$num_samples==1] <- "kk1"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz" & dat_original_mansoni$num_samples==2] <- "kk2"
#
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz" & dat_original_mansoni$num_samples==3] <- "kk3"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz" & dat_original_mansoni$num_samples==4] <- "kk3"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="ELISA"] <- "elisa"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="PCR"] <- "pcr"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="CCA"] <- "cca"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="FEC"] <- "fec"

dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz"] <- "kk1"


unique(dat_original_mansoni$case_diagnostics)

dat_original_mansoni[, match := str_c(age_start, age_end, location_name, year_start, year_end, site_memo, case_diagnostics)]


dm      <- dat_original_mansoni[, .N, by = c("age_start", "age_end", "location_name", "year_start", "year_end", "site_memo", "case_diagnostics")][N > 1]
dm      <- dm[N == 2]
matches <- dm[, str_c(age_start, age_end, location_name, year_start, year_end, site_memo, case_diagnostics)]
matches <- matches[!(is.na(matches))]
# subset to the matches 

data_ss_subset <- dat_original_mansoni[match %in% matches]

# sort

setorder(data_ss_subset, year_start, year_end, age_start, age_end, location_name, site_memo, case_diagnostics)

# correct names

data_ss_m <- data_ss_subset[sex == "Male", ]
data_ss_f <- data_ss_subset[sex == "Female",]

# validate -- if all true then ready to clean and prep for mrbrt

all(data_ss_m[, .(year_start, year_end, age_start, age_end, location_name, site_memo, case_diagnostics )] == data_ss_f[, .(year_start, year_end, age_start, age_end, location_name, site_memo, case_diagnostics)])

# mrbrt columns - NID - Author - year - cases_1 - sample_size_1 = mean_1 - se_1 - dx_1 //2's - ratio ref_to_alt - se_ratio - compar - group_id

mrbrt_sheet_mansoni <- data.table(
  NID = paste0(data_ss_m[, nid], "-", data_ss_f[, nid]),
  Author = "",
  year = data_ss_m[, year_start],
  cases_1 = data_ss_m[, cases],
  sample_size_1 = data_ss_m[, sample_size],
  mean_1 = data_ss_m[, mean],
  se_1 = data_ss_m[, standard_error],
  dx_1 = "male",
  case_name="mansoni",
  diagnostic=data_ss_m[, case_diagnostics],
  cases_2 = data_ss_f[, cases],
  sample_size_2 = data_ss_f[, sample_size],
  mean_2 = data_ss_f[, mean],
  se_2 = data_ss_f[, standard_error],
  dx_2 = "female",
  case_name="mansoni",
  diagnostic=data_ss_f[, case_diagnostics],
  comparison='male_female')


fwrite(mrbrt_sheet_mansoni, paste0(crosswalks_dir, "FILEPATH"))

#' [MR-BRT -- save model fit as rds object] 

# Load matched sheet

sex_cw_sheet <- fread(paste0(crosswalks_dir, "FILEPATH"))

#calculate ratio and se
sex_cw_sheet[, ratio := cases_1 / cases_2]
sex_cw_sheet[, ratio_se := sqrt(ratio*((se_1^2/cases_1^2)+(se_2^2/cases_2^2)))]

#outliering some data
sex_cw_sheet <- subset(sex_cw_sheet, ratio <= 3)


#log transform:
sex_cw_sheet[, ratio_log := log(ratio)]

sex_cw_sheet$ratio_se_log <- sapply(1:nrow(sex_cw_sheet), function(i) {
  ratio_i    <- sex_cw_sheet[i, "ratio"]
  ratio_se_i <- sex_cw_sheet[i, "ratio_se"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})


sex_cw_sheet[,case_name:=NULL]
sex_cw_sheet[,diagnostic:=NULL]


 
 
 # Fit Model
 
 fit1_mansoni <- run_mr_brt(
    output_dir  = paste0(crosswalks_dir),
    model_label = "sex_crosswalk_mansoni",
    data        = sex_cw_sheet,
    #covs        = covs1,
    mean_var    = "ratio_log",
    se_var      = "ratio_se_log",
    #method      = "trim_maxL",
    study_id    = "NID",
    #trim_pct    = 0.00,
    overwrite_previous = TRUE
  )
 
# 
  check_for_outputs(fit1_mansoni)
# plot_mr_brt(fit1)

saveRDS(fit1_mansoni, paste0(crosswalks_dir, "FILEPATH"))
