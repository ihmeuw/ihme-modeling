##########################################################################
### Author: USER
### Date: 07/18/2020
### Project: GBD Nonfatal Estimation
##########################################################################

rm(list=ls())
source("FILEPATH/get_bundle_version.R")
pacman::p_load(data.table, ggplot2, dplyr, plyr, stringr, openxlsx, gtools)

functions_dir <- "FILEPATH/"
source(paste0(functions_dir, "get_model_results.R"))
source(paste0(functions_dir, "get_age_spans.R"))
source(paste0(functions_dir, "get_bundle_version.R"))

date <- gsub("-", "_", Sys.Date())
bundle_id <-  277
cause_name <- "imp_epilepsy"
extractor <- "USERNAME" 
gbd_round_id <- 7
path_to_data <- paste0("FILEPATH/", date,"_iterative_", bundle_id, "_CSMR_data_added_pre_crosswalks.xlsx")


dt_all <- data.table(read.csv("FILEPATH/2020_08_31iterative_277_cod_step2_CSMR_data.csv"))
gbd_id <- 545
decomp_step_cod <- "18"
status <- "latest"
ages <- c(6:20, #5 to 79
          30, 31, 32,  #80 - 94
          34, #2 - 4
          235, #95 plus
          238, #12-23 Months
          388, #1-5 months 
          389) #6-11 months

csmr_step3_results <- get_model_results(gbd_team = "cod",
                                        gbd_id = 545,
                                        decomp_step = "step3",
                                        status = "latest",
                                        sex_id = 2,
                                        location_id = 211,
                                        age_group_id = ages,
                                        year_id = c(1985, 1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022))

#Renaming columns
setnames(csmr_step3_results, old = c("mean_death_rate", "upper_death_rate", "lower_death_rate"), new = c("mean", "upper", "lower"))

csmr_step3_results[, seq := NA]
setnames(csmr_step3_results, c('year_id'), c('year_start'))
csmr_step3_results <- csmr_step3_results[sex_id == 1, sex := 'Male']
csmr_step3_results <- csmr_step3_results[sex_id == 2, sex := 'Female']
csmr_step3_results <- csmr_step3_results[, -c('sex_id', 'population')]
csmr_step3_results[, year_end := year_start]

#Convert the age group ids to starting age years
ages <- get_age_spans()
age_using <- c(6:20, #5 to 79
               30, #80-84
               31, #85-90
               32, #90-94
               34, #2 - 4
               235, #95 plus
               238, #12-23 Months
               388, #1-5 months 
               389) #6-11 months
ages <- ages[age_group_id %in% age_using,]
setnames(ages, c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))

csmr_step3_results <- merge(csmr_step3_results, ages, by = "age_group_id", all.x = TRUE)
csmr_step3_results <- csmr_step3_results[, -c('age_group_id')]         

csmr_step3_results[, cases := '']
csmr_step3_results[, sample_size := '']
csmr_step3_results[, source_type := 'Facility - inpatient']
csmr_step3_results[, age_demographer := 1]
csmr_step3_results[, measure := 'mtspecific']
csmr_step3_results[, unit_type := 'Person']
csmr_step3_results[, unit_value_as_published := 1]
csmr_step3_results[, representative_name := 'Nationally representative only']
csmr_step3_results[, urbanicity_type := 'Unknown']
csmr_step3_results[, recall_type := 'Not Set']
csmr_step3_results[, extractor := 'USERNAME']
csmr_step3_results[, is_outlier := 0]
csmr_step3_results[, underlying_nid := '']
csmr_step3_results[, sampling_type := '']
csmr_step3_results[, recall_type_value := '']
csmr_step3_results[, input_type := '']
csmr_step3_results[, standard_error := '']
csmr_step3_results[, effective_sample_size := '']
csmr_step3_results[, design_effect := '']
csmr_step3_results[, response_rate := '']
csmr_step3_results[, uncertainty_type_value := 95]
csmr_step3_results[, uncertainty_type := 'Confidence interval']
csmr_step3_results[, note_sr := "CMSR step 3 data"]

drop <- colnames(csmr_step3_results)[!colnames(csmr_step3_results) %in% colnames(dt_all)]
csmr_step3_results <- csmr_step3_results[, -c(..drop)]
csmr_step3_results[, nid := 416752]


colnames(dt_all)[!colnames(dt_all) %in% colnames(csmr_step3_results)]
#Creating dummy observations to create sequences to use 

dt_all <- dt_all[!(location_id == 211 & sex == "Female"),]
dt_all[, note_sr := "CSMR step 2 data"]

dt_full <- rbind.fill(dt_all, csmr_step3_results)
dt_full <- data.table(dt_full)


epi_bun_w_dummy <- get_bundle_version(33689, fetch = "all")

summary(epi_bun_w_dummy[measure=="mtspecific", seq])
seqs <- 8185:505582                   
epi_red <- epi_bun_w_dummy[!seq %in% seqs, ]

dt_full[, seq := seqs]

epi_fuller <- rbind.fill(epi_red, dt_full)
epi_fuller <- data.table(epi_fuller)

epi_fuller[, crosswalk_parent_seq := seq]
epi_fuller[, seq := as.character(seq)]
epi_fuller[, seq := ""]

write.xlsx(epi_fuller, file = path_to_data, sheetName = "extraction")
