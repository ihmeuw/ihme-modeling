## Data prep for Step 4

rm(list=ls())

## Set up working environment
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
}

my.lib <- paste0("FILEPATH")
central_fxn <- paste0("FILEPATH")
code_loc <- paste0("FILEPATH")
date <- gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2, openxlsx, readxl, readr, RMySQL, stringr, tidyr, plyr, dplyr, mvtnorm, msm)

## Source central functions
source(paste0(central_fxn, "get_age_metadata.R"))
source(paste0(central_fxn, "get_location_metadata.R"))
source(paste0(central_fxn, "save_bundle_version.R"))
source(paste0(central_fxn, "get_bundle_version.R"))
source(paste0(central_fxn, "save_crosswalk_version.R"))
source(paste0(central_fxn, "get_bundle_data.R"))
source(paste0(central_fxn, "upload_bundle_data.R"))
source(paste0(central_fxn, "get_draws.R"))

## Source other functions
source(paste0(code_loc, "getrawdata.R"))
source(paste0(code_loc, "sexratio.R"))
source(paste0(code_loc, "datascatters.R"))
source(paste0(code_loc, "aggregate_marketscan.r"))
source(paste0(code_loc, "samplematching_wageaggregation.R"))
source(paste0(code_loc, "prepmatchesforMRBRT.R"))
source(paste0("FILEPATH", "mr_brt_functions.R"))
source(paste0(code_loc, "applycrosswalks.R"))
source(paste0(code_loc, "update_seq.R"))
source("FILEPATH/age_splitting_fxns.R")
source("FILEPATH/norway_specific_age_split.R")
## Get metadata
loc_dt <- get_location_metadata(location_set_id = 22, gbd_round_id = 7)

all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=12, gbd_round_id = 6))
not_babies <- all_fine_ages[!age_group_id %in% c(2:4)]
not_babies[, age_end := age_group_years_end-1]
all_fine_babies <- as.data.table(get_age_metadata(age_group_set_id=18, gbd_round_id = 6))
group_babies <- all_fine_babies[age_group_id %in% c(28)]

age_dt <- rbind(not_babies, group_babies, fill=TRUE)
age_dt[, age_start := age_group_years_start]

age_dt[age_group_id==28, age_end := 0.999]

# This is the set of age-group metadata with correct weights for age-split code, with under 5, 5-9,...90-94 and 95+
under_fives <- age_dt[age_group_id %in% c(28,5), ]
under_fives <- under_fives[ , u5 := sum(age_group_weight_value)]
under_fives <- unique(under_fives[ , `:=` (age_group_id = 1, age_group_years_start = 0, age_group_years_end = 4, age_start = 0, age_end = 4, age_group_weight_value = u5)])
under_fives[ , u5 := NULL]
under_fives[age_group_name == "1 to 4", age_group_name := "0 to 4"]
under_fives <- under_fives[age_group_name == "0 to 4",]
age_dt2 <- age_dt[!(age_group_id %in% c(28,5)), ]
age_dt2 <- rbind(age_dt2, under_fives)[age_group_name != "<1 year"]

############################ MAKE CV LISTS
# List of cvs that are in the bundle as useful tags for manipulating data, but not actually considered in crosswalking
cv_manip <- c("cv_hospital", "cv_marketscan_inp_2000", "cv_marketscan_all_2010", "cv_marketscan_inp_2010", "cv_marketscan_all_2012", "cv_marketscan_inp_2012", "cv_taiwan_claims_data", "cv_marketscan", "cv_marketscan_data")
# List of cvs that are marked 1 in one or more reference data source; we tried them as separate crosswalks and abandoned
cv_ref <- c("cv_modified", "cv_mcdonalds", "cv_admin", "cv_marketscan_other", "cv_diag_phys_neuro", "cv_other", "cv_posner", "cv_schumacher")

# List of cvs to not use in making definitions
cv_drop <- c(cv_manip, cv_ref)

# List of cvs that positively identify components of alternative case defs
cv_alts <- c("cv_marketscan_all_2000")

############################ 1. GET AND FORMAT DATA; make separate tables for modeling sex-ratio, modeling crosswalks, and modeling in DisMod
## Get bundle version prepped:
step4_bundle_version <- get_bundle_version(22502, fetch = "all")
table(step4_bundle_version$clinical_data_type, useNA = "always")

bundle_columns <- names(step4_bundle_version)

step4_data <- step4_bundle_version[clinical_data_type!= "inpatient", ] 
table(step4_data$clinical_data_type, step4_data$measure, useNA = "always")

step4_data <- get_cases_sample_size(step4_data)

setnames(step4_data, old = c("mcdonalds", "schumacher", "modified"), new = c("cv_mcdonalds", "cv_schumacher", "cv_modified"))
step4_marketscan <- step4_data[field_citation_value == "Truven Health Analytics. United States MarketScan Claims and Medicare Data - 2000. Ann Arbor, United States: Truven Health Analytics.",]
unique(step4_marketscan$nid)
step4_data <- step4_data[nid == "244369", cv_marketscan_all_2000 := 1]
step4_data <- step4_data[is.na(cv_marketscan_all_2000), cv_marketscan_all_2000 := 0]
step4_data2 <- merge(step4_data, loc_dt[ , c("location_id", "super_region_id", "super_region_name", "region_id")], by = "location_id")
#Check for step4_data nrow same
step4_data <-  copy(step4_data2)
sum(is.na(step4_data2$super_region_name))

ref_dt <- copy(step4_data)

table(step4_data$cv_marketscan_all_2000, useNA = "always")
table(step4_data$cv_marketscan_inp_2000, useNA = "always")
table(step4_data$cv_marketscan_all_2010, useNA = "always")
table(step4_data$cv_marketscan_inp_2010, useNA = "always")
table(step4_data$cv_marketscan_all_2012, useNA = "always")
table(step4_data$cv_marketscan_inp_2012, useNA = "always")
table(step4_data$cv_marketscan, useNA = "always")

mrbrt_xw_dt <- copy(step4_data)
mrbrt_sex_dt <- copy(step4_data)

# Drop observations that are redundant based on age, sex, case-def etc

table(step4_data[group_review==0,]$specificity, step4_data[group_review==0,]$group, useNA = "always")
group_review0_nid <- unique(step4_data[group_review==0, ]$nid)
print(group_review0_nid)

# Decisions for modeling data
print(step4_data[nid==111207, c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
step4_data <- step4_data[!(nid==111207 & group_review==0), ] 

print(step4_data[nid==111269, c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
step4_data <- step4_data[!(nid==111269 & group_review==0), ]

print(step4_data[nid==111270, c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
step4_data <- step4_data[!(nid==111270 & group_review==0), ] 

print(step4_data[nid==111291, c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
step4_data <- step4_data[!(nid==111291 & cv_other==1), ]

print(step4_data[nid==111313, c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])


print(step4_data[nid==111318, c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])


print(step4_data[nid==111323, c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
print(step4_data[nid==111323, c("cv_diag_phys_neuro", "cv_mcdonalds", "cv_posner", "cv_schumacher", "cv_other", "cv_modified")])
table(step4_data$measure)


print(step4_data[nid==316267, c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
step4_data <- step4_data[!(nid==316267 & sex=="Both"), ]

print(step4_data[nid==355598, c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
step4_data <- step4_data[!(nid==355598 & sex=="Both"),]

print(step4_data[nid==111229, c("nid", "year_start", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
print(step4_data[nid==111229 & measure=="prevalence", c("nid", "year_start", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
# keep all prev (age-sex specific)
print(step4_data[nid==111229 & measure=="incidence", c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
step4_data <- step4_data[!(nid==111229 & measure=="incidence" & group_review==0), ] 

print(step4_data[nid==111247, c("nid", "year_start", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
print(step4_data[nid==111247 & measure=="incidence", c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
step4_data <- step4_data[!(nid==111247 & measure=="incidence" & group_review==0),]
print(step4_data[nid==111247 & measure=="prevalence", c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
step4_data <- step4_data[!(nid==111247 & measure=="prevalence" & is.na(cases)),] 
mrbrt_sex_dt <- mrbrt_sex_dt[!(nid==111247 & measure=="prevalence" & is.na(cases)),] 
mrbrt_xw_dt <- mrbrt_xw_dt[!(nid==111247 & measure=="prevalence" & is.na(cases)),]  

print(step4_data[nid==111280, c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
step4_data <- step4_data[!(nid==111280 & group_review==0),] 

print(step4_data[nid==316295, c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
print(step4_data[nid==316295 & measure=="prevalence", c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])

print(step4_data[nid==316295 & measure=="incidence", c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
step4_data <- step4_data[!(nid==316295 & measure=="incidence" & group_review==1), ]

print(step4_data[nid==111253, c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
print(step4_data[nid==111253 & measure=="prevalence", c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])

print(step4_data[nid==111253 & measure=="incidence", c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
step4_data <- step4_data[!(nid==111253 & measure=="incidence" & group_review==1), ] 

print(step4_data[nid==125203, c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
step4_data <- step4_data[!(nid==125203 & group_review==0),]

# Decisions for DisMod modeling data and sex-ratio modeling data
print(step4_data[nid==294420, c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
print(step4_data[nid==294420 & measure=="prevalence", c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "standard_error", "lower", "upper", "note_sr", "group", "specificity", "group_review", "case_definition", "cv_mcdonalds")])

print(step4_data[nid==294420 & measure=="incidence", c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "standard_error", "lower", "upper", "note_sr", "group", "specificity", "group_review", "case_definition", "cv_mcdonalds")])
step4_data <- step4_data[!(nid==294420 & measure=="incidence" & sex!="Both"), ]

mrbrt_sex_dt <- mrbrt_sex_dt[!(nid==294420 & measure=="incidence" & sex=="Both"), ]

print(step4_data[nid==316246, c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
step4_data <- step4_data[!(nid==316246 & sex!="Both"), ]
mrbrt_sex_dt <- mrbrt_sex_dt[!(nid==316246 & sex=="Both"), ]

print(step4_data[nid==355600, c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
step4_data <- step4_data[!(nid==355600 & sex!="Both"), ] 
mrbrt_sex_dt <- mrbrt_sex_dt[!(nid==355600 & sex=="Both"), ] 

print(step4_data[nid==125205, c("nid", "year_start", "year_end", "sex", "age_start", "age_end", "location_id", "measure", "mean", "cases", "group", "specificity", "group_review", "case_definition")])
step4_data <- step4_data[!(nid==125205 & group_review==0), ]  
mrbrt_sex_dt <- mrbrt_sex_dt[!(nid==125205 & group_review==1), ]  

# Let DisMod see all modeling data we've selected (but no change to outlier status)
step4_data <- step4_data[ , group_review:=1]

# Drop all both-sex data from sex-ratio modeling data, make separate datasets for modeling sex-ratio in prev and inc
mrbrt_sex_dt <- mrbrt_sex_dt[sex!="Both", ]
mrbrt_sex_prev <- mrbrt_sex_dt[measure=="prevalence", ]
mrbrt_sex_inc <- mrbrt_sex_dt[measure=="incidence", ]

out_path <- paste0("FILEPATH")

############# 2. Apply MR-BRT sex-ratio on prev
## Get both-sex prevalence data for modeling
step4_bothsx_prev <- step4_data[sex=="Both" & measure=="prevalence", ]

## Save data that don't require this processing to bind back later
step4_sxspec_or_inc <- step4_data[!(sex=="Both" & measure=="prevalence"), ]

## Make mid-age variable for making visualization and then visualize observations to be processed
step4_bothsx_prev <- step4_bothsx_prev[ , age_mid := (age_end-age_start)/2 + age_start]
print(step4_bothsx_prev[ , c("age_start", "age_end", "age_mid")])

## Estimate sample sex-ratios in modeling data as population sex-ratios
step4_bothsx_prev <- get_sex_ppln(step4_bothsx_prev, gbd_round_id = 6, "step4", age_dt = age_dt2)

## Predict ratios from model for modeling data
path_to_prev_summ <- "FILEPATH/model_summaries.csv"
sexratio_new_predictions <- predict_new(old_model_summary = path_to_prev_summ, bothsx_dt=step4_bothsx_prev, by = "none")
sex_ratios <- copy(sexratio_new_predictions) 
step4_bothsx_prev <- transform_bothsexdt(step4_bothsx_prev, sex_ratios, by = "none")
table(step4_bothsx_prev$specificity, step4_bothsx_prev$group)

## Visualize transformed observations
ggplot(step4_bothsx_prev, aes(age_start, mean, color = sex)) + geom_point()

## Update seqs in processed observations, bind back to the modeling data that didn't require this processing step
step4_bothsx_prev_seqsu <- update_seq(step4_bothsx_prev)
step4_data_2 <- rbind(step4_sxspec_or_inc, step4_bothsx_prev, use.names = TRUE, fill = TRUE)

setdiff(step4_data$nid, step4_data_2$nid) 
step4_seq <- unique(step4_data$seq)
step4_2_seq <- unique(c(step4_data_2$seq, step4_data_2$crosswalk_parent_seq))
setdiff(step4_seq, step4_2_seq)

table(step4_data_2[measure=="prevalence", ]$sex)


############# 3. Apply MR-BRT sex-ratio on inc
## Get both-sex incidence data for modeling
step4_bothsx_inc <- step4_data_2[sex=="Both" & measure=="incidence", ]

## Save data that don't require this processing to bind back later
step4_sxspec_or_prev <- step4_data_2[!(sex=="Both" & measure=="incidence"), ]

## Make mid-age variable for making visualization and then visualize observations to be processed
step4_bothsx_inc <- step4_bothsx_inc[ , age_mid := (age_end-age_start)/2 + age_start]

## Estimate sample sex-ratios in modeling data as population sex-ratios
step4_bothsx_inc <- get_sex_ppln(step4_bothsx_inc, gbd_round_id = 6, "step4", age_dt = age_dt2)

## Predict ratios from model for modeling data
path_to_inc_summ <- "FILEPATH/model_summaries.csv"
sexratio_new_predictions <- predict_new(old_model_summary = path_to_inc_summ, bothsx_dt = step4_bothsx_inc, by = "none")
sex_ratios <- copy(sexratio_new_predictions)
step4_bothsx_inc <- transform_bothsexdt(step4_bothsx_inc, sex_ratios = sex_ratios, by = "none")
table(step4_bothsx_inc$specificity, step4_bothsx_inc$group)

## Visualize transformed observations
ggplot(step4_bothsx_inc, aes(age_start, mean, color = sex)) + geom_point()

## Update seqs in processed observations, bind back to the modeling data that didn't require this processing step
step4_bothsx_inc_seqsu <- update_seq(step4_bothsx_inc)
step4_data_3 <- rbind(step4_sxspec_or_prev, step4_bothsx_inc_seqsu, use.names = TRUE, fill = TRUE)

setdiff(step4_data$nid, step4_data_3$nid) 
step4_3_seq <- unique(c(step4_data_3$seq, step4_data_3$crosswalk_parent_seq))
setdiff(step4_seq, step4_3_seq) 

table(step4_data_3$measure, step4_data_3$sex)

################## 4. Apply MS 2000 crosswalk
## Make sure modeling data have values for all alternative definitions

step4_data_3[cv_marketscan_all_2000 == 0, definition := "reference"]
step4_data_3[cv_marketscan_all_2000 == 1, definition := ""]

## Get modeling data requiring crosswalk for non-reference case definition
step4_nonref <- step4_data_3[definition!="reference", ]

## Save reference modeling data to bind back later
step4_ref <- step4_data_3[definition=="reference", ]

#Sanity Check:
nrow(step4_nonref) + nrow(step4_ref) == nrow(step4_data_3)

## Predictions for new data
marketscan_mod_summs <- "FILEPATH"
network_prediction_new <- unique(predict_xw(choice_fit = NULL, old_model_directory = marketscan_mod_summs, "logit_dif", step4_nonref), by = "cv_marketscan_2000")
## Transform nonreference modeling data
setnames(network_prediction_new, old = c("cv_marketscan_2000"), new = c("cv_marketscan_all_2000"))
raw_data <- copy(step4_nonref)
predicted <- network_prediction_new
lhs <- "logit_dif"
i = 1
step4_nonref_x <- transform_altdt(step4_nonref, network_prediction_new, "logit_dif")

## Update seqs in transformed observations, bind back to reference modeling data
step4_nonref_x_seqsu <- update_seq(step4_nonref_x)
step4_data_4 <- rbind(step4_ref, step4_nonref_x_seqsu, use.names = TRUE)

setdiff(step4_data$nid, step4_data_4$nid) 
step4_4_seq <- unique(c(step4_data_4$seq, step4_data_4$crosswalk_parent_seq))
setdiff(step4_seq, step4_4_seq) 
setdiff(step4_2_seq, step4_4_seq) 

################## 5. Age-split from DisMod age-pattern

norway <- step4_data_4[location_name == "Vestland",]
norway <- step4_data_4[location_id == "60133" | location_id == "60135" | location_id == "60137" |location_id == "60134" | location_id == "60132" | location_id == "60136",]

non_norway <- step4_data_4[location_id != "60133" & location_id != "60135" & location_id != "60137" &location_id != "60134" & location_id != "60132" & location_id != "60136",]
nrow(norway) + nrow(non_norway) == nrow(step4_data_4)

norway_prev <- norway[measure == "prevalence", ]
norway_inc <- norway[measure == "incidence", ]
norway_non <- norway[measure!="prevalence" & measure!="incidence",]

norway_prev_split <- norway_age_split(norway_prev, age_dt2, gbd_round_id = 6, age_pattern_loc = 1, measure = 5, age_pattern_mod_ent = 1955, step = "step4")
norway_inc_split <- norway_age_split(norway_inc, age_dt2, gbd_round_id = 6, age_pattern_loc = 1, measure = 6, age_pattern_mod_ent = 1955, step = "step4")

ms_non_norway_prev_split <- non_norway[measure=="prevalence", ]
ms_non_norway_inc_split <- non_norway[measure=="incidence", ]
ms_non_norway_non <- non_norway[measure!="prevalence" & measure!="incidence",]

ms_non_norway_prev_split <- dis_age_split(ms_non_norway_prev_split, age_dt2, gbd_round_id = 6, age_pattern_loc = 1, measure = 5, age_pattern_mod_ent = 1955, step = "step4")
ms_non_norway_inc_split <- dis_age_split(ms_non_norway_inc_split, age_dt2, gbd_round_id = 6, age_pattern_loc = 1, measure = 6, age_pattern_mod_ent = 1955, step = "step4")

all <- plyr::rbind.fill(norway_prev_split, norway_inc_split, norway_non, ms_non_norway_inc_split, ms_non_norway_prev_split, ms_non_norway_non)

step4_data_5 <- copy(all)

nrow(norway_prev_split) + nrow(norway_inc_split) + nrow(ms_non_norway_inc_split) + nrow(ms_non_norway_prev_split)

final_split <- copy(as.data.table(all))
check_dt <- final_split[(final_split$age_end-final_split$age_start>25) & age_start < 81 & final_split$mean!=0 & final_split$is_outlier!=1 & (final_split$measure=="prevalence" | final_split$measure=="incidence" | final_split$measure=="proportion"), ]
nrow(check_dt)
sum(is.na(check_dt$mean)) 
table(check_dt$is_outlier, useNA = "always") 
table(check_dt$measure, useNA = "always")
check_dt[, n.age:=(age_end+1 - age_start)/5]
check_dt[, drop := cases/n.age]
sum(check_dt$drop <= 1, na.rm = T) 
sum(check_dt[,drop > 1 | is.na(drop)], na.rm = T) 

nrow(check_dt[is.na(drop), ])

table(check_dt$group_review)

dropped_nids <- setdiff(step4_data$nid, step4_data_5$nid)
table(step4_data_4[nid %in% dropped_nids, ]$year_start)

step4_data_6 <- rbind(step4_data_5, step4_data_4[nid %in% dropped_nids, ], use.names = TRUE)
step4_data_6 <- update_seq(step4_data_6)

setdiff(step4_data$nid, step4_data_6$nid) 

step4_6_seq <- unique(c(step4_data_6$seq, step4_data_6$crosswalk_parent_seq))
dropped_seqs <- setdiff(step4_4_seq, step4_6_seq)
print(dropped_seqs)
table(step4_data_4[(seq %in% dropped_seqs | crosswalk_parent_seq %in% dropped_seqs), ]$year_start)

step4_data_7 <- rbind(step4_data_6, step4_data_4[(seq %in% dropped_seqs | crosswalk_parent_seq %in% dropped_seqs), ], use.names = TRUE)
step4_data_7 <- update_seq(step4_data_7)

step4_7_seq <- unique(c(step4_data_7$seq, step4_data_7$crosswalk_parent_seq))
setdiff(step4_seq, step4_7_seq) 

################## Clean up and upload
# Revert names
setnames(step4_data_7, old = c("cv_mcdonalds", "cv_schumacher", "cv_modified"), new = c("mcdonalds", "schumacher", "modified"))
step4_data_8 <- step4_data_7[ , c(bundle_columns, "crosswalk_parent_seq"), with=FALSE]

# Fix standard errors > 1 from clinical informatics
step4_data_8 <- step4_data_8[standard_error > 1, standard_error:=1]

step4_data_8 <- step4_data_8[ , c("group_review", "group", "specificity"):=NA]

print(unique(step4_data_8[location_id %in% c(95, 4621, 4623, 4624, 4618, 4619, 4625, 4626, 4622, 4620), ]$nid)) 
step4_data_8 <- step4_data_8[!(location_id %in% c(95, 4621, 4623, 4624, 4618, 4619, 4625, 4626, 4622, 4620)), ]

print(unique(step4_data_8[is_outlier==1, ]$note_sr))
print(step4_data_8[grepl("Outliered due to insufficient uncertain", note_sr), c("nid", "note_sr", "mean", "cases", "lower", "upper", "standard_error", "sample_size")])
step4_data_8 <- step4_data_8[grepl("Outliered due to insufficient uncertain", note_sr), `:=` (upper = 1, standard_error = NA, sample_size = NA)]

ggplot(step4_data_8, aes(age_start, mean)) + geom_point()

step4_data_8[recall_type == "", recall_type := "Not Set"]
step4_data_8[unit_type == "", unit_type := "Person"]
step4_data_8[urbanicity_type == "", urbanicity_type := "Unknown"]
step4_data_8[is.na(unit_value_as_published), unit_value_as_published := 1]

#Write data to Excel file and upload
upload_path <- paste0("FILEPATH", date,"_iterative_age_split_parent_fix_un_utla_split", ".xlsx")
write.xlsx(step4_data_8, upload_path, col.names=TRUE, sheetName = "extraction")
description <- "age split parent fix, Iterative 148, fake step 2, marketscan xwalked Lit, step 2 and 4 claims, outliered hx and 0 mean 0 cases"
step4_upload <- save_crosswalk_version(bundle_version_id=22502, data_filepath=upload_path, description = description)
