#######################################################################################
### Purpose:  Estimate Sex-ratio and crosswalks for GBD2019
#######################################################################################

bundle_id <- 757
acause <-"mental_other"
covariates <- c("cv_nesarc")
uses_csmr <- F
test_sex_by_super_region <- F
crosswalk_pairs <- ''
age_sex_split_estimates <- ""
need_to_age_split <- F
need_save_bundle_version <- 7073 # Set as true to save bundle version, otherwise specify bundle version here
sex_ratio_by_age <- F

library(data.table)
library(openxlsx)
library(msm)
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")

## Get latest review sheet ##
if(need_save_bundle_version == T){
  v_id <- save_bundle_version(bundle_id, "step2", include_clinical = TRUE)$bundle_version_id
} else {
  v_id <- need_save_bundle_version
}
review_sheet <- get_bundle_version(v_id)

## estimate Nesarc to AUS crosswalk for later

aus_data <- review_sheet[cv_nesarc == 0 & age_start == 18 & age_end == 99 & sex == "Both", .(mean_aus = mean, se_aus = standard_error)]

nesarc_data <- review_sheet[cv_nesarc == 1, ]
nesarc_data[, `:=` (cases = sum(cases), sample_size = sum(sample_size))]
nesarc_data[, `:=` (mean = cases / sample_size)]
nesarc_data[, `:=` (standard_error = sqrt(1/sample_size*mean*(1-mean)+1/(4*sample_size^2)*1.96^2))]
nesarc_data <- unique(nesarc_data[,.(mean_nesarc = mean, se_nesarc = standard_error)])

x_walk <- data.table(aus_data, nesarc_data)
x_walk[, `:=` (x_walk = mean_aus / mean_nesarc, x_walk_se = sqrt(((mean_aus^2) / (mean_nesarc^2)) * (((se_aus^2) / (mean_aus^2) + (se_nesarc^2) / (mean_nesarc^2)))))]

## Flag if age-split by regional pattern estimates exist ##
if(length(review_sheet[(grepl("age-split child", specificity)),unique(nid)]) > 0){
  print(paste0("STOP! The following nid still has age-split estimates by regional pattern in your bundle version: ", review_sheet[(grepl("age-split child", specificity)),unique(nid)]))
}

## Remove excluded estimates ##
review_sheet[is.na(group_review), group_review := 1]
review_sheet <- review_sheet[group_review == 1, ]
review_sheet[, study_covariate := "ref"]

review_sheet[is.na(standard_error) & !is.na(lower), standard_error := (upper - lower) / (qnorm(0.975,0,1)*2)]
review_sheet[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(1/sample_size * mean * (1-mean) + 1/(4*sample_size^2)*qnorm(0.975,0,1)^2)]
review_sheet[is.na(standard_error) & measure %in% c("incidence", "remission"), standard_error :=  ifelse(mean*sample_size <= 5, ((5-mean*sample_size) / sample_size + mean * sample_size * sqrt(5/sample_size^2))/5, ((mean*sample_size)^0.5)/sample_size)]

##### Estimate and apply sex-ratios -----------------------------------------------------------------------

## All data is sex-specific.

review_sheet_final <- copy(review_sheet)

##### Estimate and apply study-level covariates -----------------------------------------------------------------------

# only nesarc to aud crosswalk to apply

review_sheet_final[cv_nesarc == 1, `:=` (mean = mean*x_walk$x_walk, standard_error = sqrt((standard_error^2) * (x_walk$x_walk_se^2) + (standard_error^2) * (x_walk$x_walk^2) + (x_walk$x_walk_se^2) * (mean^2)))]

review_sheet_final[cv_nesarc == 1, `:=` (cases = NA, lower = NA, upper = NA)]
review_sheet_final[cv_nesarc == 1, `:=` (crosswalk_parent_seq = seq, seq = NA)]

for(c in covariates){
  c <- gsub("d_", "cv_", c)
  review_sheet_final[get(c) == 1, study_covariate := ifelse(is.na(study_covariate) | study_covariate == "ref", gsub("cv_", "", c), paste0(study_covariate, ", ", gsub("cv_", "", c)))]
}

# For upload validation #
review_sheet_final[study_covariate != "ref", `:=` (lower = NA, upper = NA, cases = NA)]
review_sheet_final[is.na(lower), uncertainty_type_value := NA]

review_sheet_final <- review_sheet_final[group_review == 1, ] 

crosswalk_save_folder <- paste0("FILEPATH", acause, "/", bundle_id, "FILEPATH")
dir.create(file.path(crosswalk_save_folder), showWarnings = FALSE)
crosswalk_save_file <- paste0(crosswalk_save_folder, "crosswalk_", Sys.Date(), ".xlsx")
write.xlsx(review_sheet_final, crosswalk_save_file, sheetName = "extraction")

##### Upload crosswalked dataset to database -----------------------------------------------------------------------

save_crosswalk_version(v_id, crosswalk_save_file, description = "Nesarc to Aus survey crosswalk applied")

