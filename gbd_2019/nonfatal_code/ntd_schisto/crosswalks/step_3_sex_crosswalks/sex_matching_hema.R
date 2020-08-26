# Chagas Crosswalk Matching 

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

library(metafor, lib.loc = "FILEPATH")
library(msm, lib.loc = "FILEPATH")

## Set-up run directory

run_file <- fread(paste0("FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]

crosswalks_dir    <- paste0(run_dir, "FILEPATH")

### ----------------------- Match ------------------------------


data<- get_bundle_version(bundle_version_id = ADDRESS, export=TRUE)

data <- as.data.table(data)
#' [Direct Matches]

data <- data[sex != "Both"]
data <- data[cases != 0]
data<- subset(data, sample_size!=cases)

######run from here after runing sex_matching_mansoni#######################

#dat_original_mansoni<- subset(data, case_name=="S mansoni" | case_name=="S intercalatum"| case_name=="S mekongi")
dat_original_hema<- subset(data, case_name=="S haematobium")

#dat_original_jap<- subset(dat_original, case_name=="S japonicum")

dat_original_hema$case_diagnostics<- dat_original_hema$case_diagnostics
dat_original_hema$case_diagnostics <- as.character(dat_original_hema$case_diagnostics)

dat_original_hema$case_diagnostics[dat_original_hema$case_diagnostics=="sedimentation" | dat_original_hema$case_diagnostics=="sedimentation of urine" |   dat_original_hema$case_diagnostics=="sedimentation, filtration"] <- "sed"

dat_original_hema$case_diagnostics[dat_original_hema$case_diagnostics== "centrifugation"| dat_original_hema$case_diagnostics=="sedimentation, centrifugation" |  dat_original_hema$case_diagnostics=="sedimentation, centrifugation, filtration, microscopy"] <- "cen"

dat_original_hema$case_diagnostics[dat_original_hema$case_diagnostics== "urine filtration technique" | dat_original_hema$case_diagnostics=="urine filtration technique, microscopy" | dat_original_hema$case_diagnostics=="microscopy"] <- "filt"
dat_original_hema$case_diagnostics[dat_original_hema$case_diagnostics== "haematuria"] <- "dip"

dat_original_hema<- subset(dat_original_hema, dat_original_hema$case_diagnostics != "ELISA" & dat_original_hema$case_diagnostics != "fecal smear" & dat_original_hema$case_diagnostics != "IFAT" & dat_original_hema$case_diagnostics!="IFTB" & dat_original_hema$case_diagnostics != "Kato-Katz" & dat_original_hema$case_diagnostics != "NA" & dat_original_hema$case_diagnostics != "nucleopore filtration" & dat_original_hema$case_diagnostics != "urinealysis, microscopy")

dat_original_hema$case_diagnostics[dat_original_hema$case_diagnostics=="PCR"] <- "pcr"
dat_original_hema$case_diagnostics[dat_original_hema$case_diagnostics=="CCA"] <- "cca"

unique(dat_original_hema$case_diagnostics)

length(which(dat_original_hema$case_diagnostics == "sed")) 
length(which(dat_original_hema$case_diagnostics == "filt")) 
length(which(dat_original_hema$case_diagnostics == "dip")) 
length(which(dat_original_hema$case_diagnostics == "cca")) 
length(which(dat_original_hema$case_diagnostics == "pcr")) 
length(which(dat_original_hema$case_diagnostics == "cen")) 

dat_original_hema[, match := str_c(age_start, age_end, location_name, year_start, year_end, site_memo, case_diagnostics)]

# 34 direct matches (of the data design)??

dm      <- dat_original_hema[, .N, by = c("age_start", "age_end", "location_name", "year_start", "year_end", "site_memo", "case_diagnostics")][N > 1]
dm      <- dm[N == 2]
matches <- dm[, str_c(age_start, age_end, location_name, year_start, year_end, site_memo, case_diagnostics)]
matches <- matches[!(is.na(matches))]
# subset to the matches 

data_ss_subset <- dat_original_hema[match %in% matches]

# sort

setorder(data_ss_subset, year_start, year_end, age_start, age_end, location_name, site_memo, case_diagnostics)

# correct names

data_ss_m <- data_ss_subset[sex == "Male", ]
data_ss_f <- data_ss_subset[sex == "Female",]


all(data_ss_m[, .(year_start, year_end, age_start, age_end, location_name, site_memo, case_diagnostics )] == data_ss_f[, .(year_start, year_end, age_start, age_end, location_name, site_memo, case_diagnostics)])

# mrbrt columns - NID - Author - year - cases_1 - sample_size_1 = mean_1 - se_1 - dx_1 //2's - ratio ref_to_alt - se_ratio - compar - group_id

mrbrt_sheet_hema <- data.table(
  NID = paste0(data_ss_m[, nid], "-", data_ss_f[, nid]),
  Author = "",
  year = data_ss_m[, year_start],
  cases_1 = data_ss_m[, cases],
  sample_size_1 = data_ss_m[, sample_size],
  mean_1 = data_ss_m[, mean],
  se_1 = data_ss_m[, standard_error],
  dx_1 = "male",
  case_name="hema",
  diagnostic=data_ss_m[, case_diagnostics],
  cases_2 = data_ss_f[, cases],
  sample_size_2 = data_ss_f[, sample_size],
  mean_2 = data_ss_f[, mean],
  se_2 = data_ss_f[, standard_error],
  dx_2 = "female",
  case_name="hema",
  diagnostic=data_ss_f[, case_diagnostics],
  comparison='male_female')


fwrite(mrbrt_sheet_hema, paste0(crosswalks_dir, "FILEPATH"))

#' [MR-BRT -- save model fit as rds object] 

# Load matched sheet

sex_cw_sheet <- fread(paste0(crosswalks_dir, "FILEPATH"))

#calculate ratio and se
sex_cw_sheet[, ratio := cases_1 / cases_2]
sex_cw_sheet[, ratio_se := sqrt(ratio*((se_1^2/cases_1^2)+(se_2^2/cases_2^2)))]


#outliering some data
sex_cw_sheet <- subset(sex_cw_sheet, ratio <= 3)

#log transform
sex_cw_sheet[, ratio_log := log(ratio)]

sex_cw_sheet$ratio_se_log <- sapply(1:nrow(sex_cw_sheet), function(i) {
  ratio_i    <- sex_cw_sheet[i, "ratio"]
  ratio_se_i <- sex_cw_sheet[i, "ratio_se"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

sex_cw_sheet[,case_name:=NULL]
sex_cw_sheet[,diagnostic:=NULL]



fit1_hema <- run_mr_brt(
  output_dir  = paste0(crosswalks_dir),
  model_label = "sex_crosswalk_hema",
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
check_for_outputs(fit1_hema)
# plot_mr_brt(fit1)


saveRDS(fit1_hema, paste0(crosswalks_dir, "FILEPATH"))
