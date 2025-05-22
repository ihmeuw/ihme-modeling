######################################################################################################
######################################################################################################
###### HCV seroprevalence data processing 
######################################################################################################
######################################################################################################

rm(list=ls())

invisible(sapply(list.files("FILEPATH", full.names = T), source))
pacman::p_load(data.table, ggplot2, dplyr, readr, tidyverse, openxlsx, tidyr, plyr, readxl, boot, gtools, msm, Hmisc)
source("~FILEPATH/cw_mrbrt_helper_functions.R")


date <- gsub("-", "_", Sys.Date())
measures <- "prevalence"
b_id <- OBJECT
xv_id <- OBJECT 
bv_id <- OBJECT 
a_cause <- "hepatitis_c"


draws <- paste0("draw_", 0:999)
mrbrt_dir <- "FILEPATH"
if (!dir.exists(mrbrt_dir)) dir.create(mrbrt_dir)

outlier_status <- c(0) 
sex_split <- T
sex_covs <- "" 
id_vars <- "study_id"
sex_remove_x_intercept <- T
keep_x_intercept <- T
logit_transform <- T
reference <- ""
nash_cryptogenic <- F
reference_def <- "reference"
trim <- 0.1
measures <- "prevalence"


if(logit_transform == T) {
  response <- "ldiff"
  data_se <- "ldiff_se"
  mrbrt_response <- "diff_logit"
} else {
  response <- "ratio_log"
  data_se <- "ratio_se_log"
  mrbrt_response <- "diff_log"
}




loc_dt <- get_location_metadata(OBJECT, gbd_round_id = OBJECT)
loc_dt1 <- loc_dt[, .(location_id, super_region_name)]


################################################################################
## STEP 1 RETRIEVE ALL DATA
################################################################################


df_all_new = get_bundle_version(bv_id, fetch='all', export = FALSE)
table(df_all_new$measure)


df_csmr <- subset(df_all_new, measure=="mtspecific")
df_csmr$crosswalk_parent_seq <- df_csmr$seq
df_csmr$origin_seq <- df_csmr$seq
df_csmr$seq <- NA


df_prev <- df_all_new[measure %in% c("prevalence")]
table(df_prev$cv_blood_donors)
table(df_prev$cv_blood_donor)
table(df_prev$cv_pregnant)

df_prev_new <- subset(df_prev, (is.na(group_review) | group_review==1)) #ONLY APPLICABLE WHEN NOT RUNNING SEX RATIO

# get previous best crosswalk -----------------------------------------------------------
df_old <- get_crosswalk_version(OBJECT)
df_old_csmr <- subset(df_old, measure=="mtspecific")
df_old_csmr$seq <- NA

df_old_prev <- subset(df_old, measure=="prevalence")
df_old_prev$seq <- NA
df_old_prev2 <- df_old_prev[crosswalk_parent_seq %in% prev_bv_seq]

df_old_prev <- df_old_prev[, c("nid","seq", "crosswalk_parent_seq", "origin_seq", "is_outlier", "mean",  "location_id", "sex","year_start", "age_start")]
setnames(df_old_prev, c("crosswalk_parent_seq","seq",  "is_outlier"),c("crosswalk_parent_seq_old","seq_old", "is_outlier_old"))


####################################################################
# sex-split, crosswalk, age-split new data
####################################################################
source("FILEPATH")
remove_python_objects()
library(crosswalk002, lib.loc = "FILEPATH")

library(reticulate)
pd <- import("pandas")
sex_results  <- pd$read_pickle("FILEPATH")


sex_results$fixed_vars
sex_results$beta_sd
sex_results$gamma



# ADJUST VALUES FOR SEX SPLITTING FUNCTION -----------------------
# Sex splitting 
full_dt <- as.data.table(copy(df_prev_new))
full_dt <- full_dt[measure == measures, ]


# The offset does not affect the estimated value of pred_diff_se or pred_diff_mean but is needed to run through adjust_orig_vals
full_dt$crosswalk_parent_seq <- ""
dt_sex_split <- split_both_sex(full_dt, sex_results)
final_sex_dt <- copy(dt_sex_split$final)
graph_sex_dt <- copy(dt_sex_split$graph)

sex_graph <- graph_sex_predictions(graph_sex_dt)
sex_graph




# Apply the same crosswalk to BLOOD DONOR ----------------------------------------------------------
table(final_sex_dt$cv_blood_donors)
table(final_sex_dt$cv_blood_donor)
table(final_sex_dt$cv_pregnant)
final_sex_dt <- final_sex_dt[is.na(cv_blood_donor), cv_blood_donor :=cv_blood_donors]


pd <- import("pandas")
results  <- pd$read_pickle("FILEPATH")
results$fixed_vars
results$beta_sd
results$gamma

cv_drop <- c("cv_pregnant", "cv_antibody", "cv_RNA",  "cv_double_anti", "cv_healthcare_worker","cv_healthcare_workers","cv_blood_donors","cv_specific_occupation", "cv_schoolchildren", "cv_attending_unrelated_clinic", "cv_hospital_referrals",	"cv_medical_condition",	"cv_institutionalized", "cv_vaccination")

# ADJUST THE DATA  -------------------------------------------------
full_dt <- copy(final_sex_dt[measure %in% measures])
full_dt <- get_definitions(full_dt)
full_dt <- full_dt[[1]]
full_dt$definition <- gsub("_cv_", "", full_dt$definition)
unique(full_dt$definition)
adjusted <- make_adjustment(results, full_dt)
epidb <- copy(adjusted$epidb)
epidb <- epidb[(is.na(cv_RNA) | cv_RNA == 0), ]
epidb[cv_pregnant == 1, is_outlier := 1]

test3 <- epidb[, c("nid", "year_start", "year_end", "age_start", "age_end", "sex", "mean", "cases", "sample_size", "cv_blood_donor", "cv_blood_donors")]

cw_file_path <- FILEPATH


vetting <- copy(adjusted$vetting_dt)
vetting_plots <- prediction_plot(vetting)
vetting_plots


length(unique(epidb$nid))

# Under 25 subset ---------------------------------------------------------
age_pattern <- copy(epidb)
age_pattern <- age_pattern[, age_range := age_end - age_start ]
age_25 <- age_pattern[age_range <= 25,]
age_25_over <- age_pattern[age_range > 25,]


# GET OBJECTS -------------------------------------------------------------
name <- OBJECT
id <- OBJECT #
ver_id <- OBJECT 
decomp_step <- 'iterative' 
description <- paste0(DESCRIPTION)



ages <- get_age_metadata(OBJECT, gbd_round_id = OBJECT)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age <- ages[age_start >= 0, age_group_id]

df <- copy(age_25_over)
gbd_id <- id


region_pattern <- T
location_pattern_id <- 1

## GET TABLES
sex_names <- get_ids(table = "sex")
ages[, age_group_weight_value := NULL]
ages[age_start >= 1, age_end := age_end - 1]
ages[age_end == 124, age_end := 99]
super_region_dt <- get_location_metadata(location_set_id = 22, gbd_round_id=7)
super_region_dt <- super_region_dt[, .(location_id, super_region_id)]


## SAVE ORIGINAL DATA (DOESN'T REQUIRE ALL DATA TO HAVE SEQS)
original <- copy(df)
original[, id := 1:.N]

## FORMAT DATA
dt <- get_cases_sample_size(original)
dt <- get_se(dt)
dt <- calculate_cases_fromse(dt)
dt <- format_data(dt, sex_dt = sex_names)

## EXPAND AGE
split_dt <- expand_age(dt, age_dt = ages)

## GET PULL LOCATIONS
if (region_pattern == T){
  message("Splitting by region pattern")
  split_dt <- merge(split_dt, super_region_dt, by = "location_id")
  super_regions <- unique(split_dt$super_region_id) ##get super regions for dismod results
  locations <- super_regions
} else {
  locations <- location_pattern_id
  message("Splitting by global pattern")
}

##GET LOCS AND POPS
pop_locs <- unique(split_dt$location_id)
pop_years <- unique(split_dt$year_id)

## GET AGE PATTERN
print("getting age pattern")
age_pattern <- get_age_pattern(locs = locations, id = gbd_id, age_groups = age) #set desired super region here if you want to specify

if (region_pattern == T) {
  age_pattern1 <- copy(age_pattern)
  split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id", "super_region_id"))
} else {
  age_pattern1 <- copy(age_pattern)
  split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id"))
}

## GET POPULATION INFO
print("getting pop structure")
pop_structure <- get_pop_structure(locs = pop_locs, years = pop_years, age_group = age)
split_dt <- merge(split_dt, pop_structure, by = c("location_id", "sex_id", "year_id", "age_group_id"))

#####CALCULATE AGE SPLIT POINTS#######################################################################
## CREATE NEW POINTS
print("splitting data")
split_dt <- split_data(split_dt)

######################################################################################################

final_age_dt <- format_data_forfinal(split_dt, location_split_id = location_pattern_id, region = region_pattern,
                                     original_dt = original)


final_age_dt <- final_age_dt[standard_error >1, standard_error := NA]
final_age_dt <- final_age_dt[location_id != 95, ]
final_age_dt[!is.na(crosswalk_parent_seq), seq := NA]
locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 7)
gbr <- locs[grepl("GBR_", ihme_loc_id) & level == 5, unique(location_id)]
final_age_dt <- final_age_dt[!(location_id %in% gbr)]
length(unique(final_age_dt$nid))


final <- as.data.table(rbind.fill(final_age_dt, age_25))
final <- merge( df_old_prev[,c("nid",  "sex","origin_seq",  "location_id", "year_start", "age_start","is_outlier_old")], final, by =c("nid",  "sex","origin_seq",  "location_id", "year_start", "age_start"), all.y = TRUE )
final[(is_outlier!=is_outlier_old), `:=` (is_outlier = is_outlier_old)]



final <- as.data.table(rbind.fill(final,df_csmr))

test <- subset(age_25, seq!=crosswalk_parent_seq)
test <- test[, c("nid", "seq", "crosswalk_parent_seq")]

final[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
final[is.na(crosswalk_parent_seq), crosswalk_parent_seq := origin_seq]

final[is.na(crosswalk_parent_seq)]
final[, seq := NA]

# basic validation checks 
final[location_id ==95]
final[is.na(age_start)]
final[is.na(age_end)]
final[is.na(location_id)]
final[is.na(crosswalk_parent_seq) & is.na(seq), ]
final[!is.na(crosswalk_parent_seq) & !is.na(seq), ]

final

final[, unit_value_as_published := 1]
final <- final[standard_error >1, standard_error := NA]

file_path <- FILEPATH
write.xlsx(final, file_path, sheetName = "extraction", rowNames = FALSE)



###########
#STEP 6: Upload  new data 
description <- paste0(DESCRIPTION) 
result <- save_crosswalk_version( bundle_version_id_new, file_path, description)

xv2$seq <- NA
df_prev <- orig_dt[measure %in% c("prevalence")]

prev_bv_seq <- unique(df_prev$seq)
xv3  <- xv2[crosswalk_parent_seq %in% prev_bv_seq]
seqs <- as.data.table(xv3[, c("seq", "crosswalk_parent_seq")])

final <- as.data.table(rbind.fill(xv3,orig_dt_csmr))
final <- final[standard_error >1, standard_error := NA]

final$note_sr  <- substr(final$note_sr , 1, 2000)



file_path <- FILEPATH
write.xlsx(final, file_path, sheetName = "extraction", rowNames = FALSE)


description <- paste0(DESCRIPTION) #description needed for each crosswalk version; this is what is going to show up in Dismod
result <- save_crosswalk_version( bundle_version_id_new, file_path, description)
