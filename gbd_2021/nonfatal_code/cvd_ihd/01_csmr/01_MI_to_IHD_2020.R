#### Prep data for Dismod and MRBRT MI proportions model
####

pacman::p_load(fs, readxl, data.table, ggplot2, tidyverse, tidyr, gtools, dplyr, DBI, RMySQL, haven, doBy, plyr, openxlsx)

central <- "/FILPEATH/"
for (func in paste0(central, list.files(central))) source(paste0(func))

timestamp <- Sys.Date() 

post <- fread("/FILEPATH/full_postredistribution.csv")
pre <- fread("/FILEPATH/full_preredistribution.csv")

data <- rbind(pre, post)

data <- select(data, -"cause_id")
wdata <- pivot_wider(data, names_from = cause_name, values_from = deaths)
wdata <- as.data.frame(wdata)

old_names <-  c("Angina", "Acute myocardial infarction", "Chronic ischemic heart disease")
new_names <- c("angina_deaths", "acute_deaths", "chronic_deaths")
wdata <- setnames(wdata, old_names, new_names)
wdata <- unnest(wdata, new_names)
wdata <- as.data.table(wdata)

wdata <- wdata[, ihd_deaths := angina_deaths + acute_deaths + chronic_deaths]
wdata <- wdata[, prop_acute := acute_deaths / ihd_deaths]
wdata <- wdata[, prop_chronic := chronic_deaths / ihd_deaths]
wdata <- wdata[, prop_angina := angina_deaths / ihd_deaths]

wdata <- wdata[, se := sqrt(prop_acute*(1-prop_acute)/ihd_deaths)] 
ages <- c(8:32, 235)
wdata <- wdata[age_group_id %in% ages]

## Prep data for Dismod 
#Remove unnecesary variables
ratio <- wdata
#Set round information
bundle_id <- 350
gbd_round_id <- 7
decomp_step <- "iterative"

age_range <- strsplit(wdata$age_group_name, " to ", fixed=T)
age_range <- data.frame(do.call(rbind, age_range))
names(age_range) <- c("age_start", "age_end")
age_range <- unique(age_range)
age_groups <- get_age_metadata(age_group_set_id=19, gbd_round_id=7)
age_groups <- merge(age_range, age_groups[, .(age_group_years_start, age_group_years_end, age_group_id)], by.x="age_start", by.y="age_group_years_start", all.x=T)
age_groups <- select(age_groups, -age_group_years_end)
age_groups$age_group_id[is.na(age_groups$age_group_id)] <- 235

age_groups$age_start <- as.numeric(as.character(age_groups$age_start))
age_groups$age_start[is.na(age_groups$age_start)] <- 95
age_groups$age_end <- as.numeric(as.character(age_groups$age_end))
age_groups$age_end[is.na(age_groups$age_end)] <- 125

#ratio <- data.frame(read_stata(paste0("/FILEPATH/mi_ihd_ratio_02_15_2017.dta")))

ratio <- merge(ratio, age_groups, by="age_group_id", all.x=T)

ratio$bundle_id <- 350
ratio$seq <- ""
ratio$nid <- 136010
ratio$input_type <- "extracted"
ratio$source_type <- "Vital registration - other/unknown"
ratio$site_memo <- ""
ratio$sex <- with(ratio, ifelse(sex_id==1, "Male", ifelse(sex_id==2, "Female", NA)))
ratio$sex_id <- NULL
ratio$sex_issue <-0
ratio$year_start <- ratio$year_id
ratio$year_end <- ratio$year_id
ratio$year_id <- NULL
ratio$year_issue <- 0
ratio$age_issue <- 0
ratio$age_demographer <- 1
ratio$measure <- "proportion"
ratio$mean <- ""
ratio$lower <- ""
ratio$upper <- ""
ratio$standard_error <- ""
ratio$effective_sample_size <- ""
ratio$cases <- ratio$acute
ratio$sample_size <- ratio$ihd_deaths
ratio$unit_type <- "Person"
ratio$unit_value_as_published <- 0
ratio$measure_issue <- 0
ratio$measure_adjustment <- 0
ratio$uncertainty_type <- "Sample size"
ratio$uncertainty_type_value <- ""
ratio$representative_name <- "Unknown"
ratio$urbanicity_type <- "Unknown"
ratio$recall_type <- "Point"
ratio$case_definition <- ifelse(ratio$type == "Postredistribution", "acute deaths/total (acute + chronic + angina); both MI and IHD are Post-redistribution", 
                                "acute deaths/total (acute + chronic + angina); both MI and IHD are Pre-redistribution")
ratio$note_modeler <- ""
ratio$note_modeler <- "subset to sample size (IHD deaths) >=20"
ratio$extractor <- "USERNAME"
ratio$is_outlier <- 0
ratio$recall_type_value <- ""
ratio$design_effect <- ""
ratio$sampling_type <- ""
ratio$field_citation_value <- ""
ratio$underlying_field_citation_value <- ""
ratio$response_rate <- ""
ratio$underlying_nid <- ""
ratio <- subset(ratio, sample_size>=20) #restrict to be able to upload
ratio <- subset(ratio, cases<=sample_size)

ratio <- ratio[,c("bundle_id", "seq", "nid", "input_type", "source_type", "location_id", "site_memo", "sex", "sex_issue", "year_start", "year_end", "year_issue", "age_start", "age_end",
                  "age_issue", "age_demographer", "measure", "mean", "lower", "upper", "standard_error", "effective_sample_size", "cases", "sample_size", "unit_type",
                  "unit_value_as_published", "measure_issue", "measure_adjustment", "uncertainty_type", "uncertainty_type_value", "representative_name", "urbanicity_type", 
                  "recall_type", "case_definition", "note_modeler", "extractor", "is_outlier", "recall_type_value", "design_effect", "sampling_type", "field_citation_value",
                  "underlying_field_citation_value", "response_rate", "underlying_nid", "type")]

## Save bundle data postredistribution
ratio_post <- ratio[type == "Postredistribution"]
ratio_post <- select(ratio_post, -type)

write.xlsx(ratio_post, 
           file=paste0("/FILEPATH/me_350_prep_postredist_", timestamp, ".xlsx"), sheetName = "extraction")

#Make sure there is nothing in the bundle 
bun_data <- get_bundle_data(350, gbd_round_id = 7, decomp_step = "iterative")

wipe_bundle <- select(bun_data, seq)
write.xlsx(wipe_bundle, file=paste0("/FILEPATH/me_350_wipe_bundle", timestamp, ".xlsx"))

#Wipe bundle
path_to_data <- "/FILEPATH/me_350_wipe_bundle_2020-08-07.xlsx"
result <- upload_bundle_data(bundle_id, decomp_step, path_to_data, gbd_round_id)

#Upload new data postredistribution
path_to_data <- "/FILEPATH/me_350_prep_postredist_2020-08-10.xlsx"
result <- upload_bundle_data(bundle_id, decomp_step, path_to_data, gbd_round_id)

#Save  bundle version
result <- save_bundle_version(bundle_id, decomp_step, gbd_round_id, include_clinical = NULL)
#Post-redistribution bundle version 
bundle_version_id <- result$bundle_version_id

#Get bundle version
bundle_version <- get_bundle_version(bundle_version_id, fetch = 'all')

#Save crosswalk version
bundle_version$crosswalk_parent_seq <- ""
write.xlsx(bundle_version, 
           file=paste0("/FILEPATH/me_350_xwalk_postredist_", timestamp, ".xlsx"), 
           sheetName = "extraction")

#Upload crosswalk version
data_filepath <- "/FILEPATH/me_350_xwalk_postredist_2020-08-10.xlsx"
description <- "GBD2020 postredistribution ratio"
result <- save_crosswalk_version(
  bundle_version_id=bundle_version_id,
  data_filepath=data_filepath,
  description=description)

crosswalk_version_id <- result$crosswalk_version_id
# crosswalk_version_id postredistribution 

## Save bundle data preredistribution
###############################################
ratio_pre <- ratio[type == "Preredistribution"]

write.xlsx(ratio_pre, 
           file=paste0("/FILEPATH/me_350_prep_preredist_", timestamp, ".xlsx"), 
           sheetName = "extraction")

## Wipe bundle again
bun_data <- get_bundle_data(350, gbd_round_id = 7, decomp_step = "iterative")
wipe_bundle <- select(bun_data, seq)
write.xlsx(wipe_bundle, file=paste0("/FILEPATH/me_350_wipe_bundle_", timestamp, ".xlsx"), sheetName="extraction")
path_to_data <- "/FILEPATH/me_350_wipe_bundle_2020-08-10.xlsx"
result <- upload_bundle_data(bundle_id, decomp_step, path_to_data, gbd_round_id)

bun_data <- get_bundle_data(350, gbd_round_id = 7, decomp_step = "iterative")

#Upload new data postredistribution
path_to_data <- "/FILEPATH/me_350_prep_preredist_2020-08-10.xlsx"
result <- upload_bundle_data(bundle_id, decomp_step, path_to_data, gbd_round_id)

#Save  bundle version
result <- save_bundle_version(bundle_id, decomp_step, gbd_round_id, include_clinical = NULL)
bundle_version_id <- result$bundle_version_id
#Pre-redistribution bundle version 

#Get bundle version
bundle_version <- get_bundle_version(bundle_version_id, fetch = 'all')

#Save crosswalk version
bundle_version$crosswalk_parent_seq <- ""
write.xlsx(bundle_version, 
           file=paste0("/FILEPATH/me_350_xwalk_preredist_", timestamp, ".xlsx"), 
           sheetName = "extraction")

#Upload crosswalk version
data_filepath <- "/FILEPATH/me_350_xwalk_preredist_2020-08-10.xlsx"
description <- "GBD2020 preredistribution ratio"
result <- save_crosswalk_version(
  bundle_version_id=bundle_version_id,
  data_filepath=data_filepath,
  description=description)

crosswalk_version_id <- result$crosswalk_version_id
#crosswalk_version_id 


