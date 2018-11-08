#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: PART ONE -   Run CODEm model for fatal outcomes
#          PART TWO -   Run DisMod model for case fatality rate
#          PART THREE - Calculate deaths, prevalence, and incidence from CFR, death rate, and duration
#          PART FOUR -  Format for COMO and save results to database
#          PART FIVE -  Prep motor impairment models
#		       PART SIX -   Upload impairment data through Epi Uploader, run DisMod model
#***********************************************************************************************************************


########################################################################################################################
##### START-UP #########################################################################################################
########################################################################################################################


#----CONFIG-------------------------------------------------------------------------------------------------------------
### load packages
pacman::p_load(magrittr, foreign, stats, MASS, data.table, dplyr, plyr, lme4, reshape2, parallel)
if (Sys.info()["sysname"]=="Linux") {
  library(rhdf5, lib.loc="FILEPATH")
  library(openxlsx, lib.loc="FILEPATH")
} else { 
  pacman::p_load(rhdf5, openxlsx)
}
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause           <- "tetanus"
cause_id         <- 340
mild_bundle_id   <- 47
modsev_bundle_id <- 48
gbd_round        <- 5
year_end         <- gbd_round + 2012

### draw numbers
draw_nums_gbd    <- 0:999
draw_cols_upload <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", draw_nums_gbd))

### make folders on cluster
cl.version.dir <- file.path("FILEPATH", acause, "nonfatal", custom_version, "draws")                                                
if (!dir.exists(cl.version.dir)) dir.create(cl.version.dir, recursive=TRUE)

### directories
home <- file.path(j_root, "FILEPATH", acause, "00_documentation")
j.version.dir <- file.path(home, "models", custom_version)
if (!dir.exists(j.version.dir)) dir.create(j.version.dir, recursive=TRUE)
j.version.dir.logs <- file.path(j.version.dir, "model_logs")
if (!dir.exists(j.version.dir.logs)) dir.create(j.version.dir.logs, recursive=TRUE)

### save description of model run
if (UPLOAD_NONTATAL=="yes" & CALCULATE_IMPAIRMENTS=="no") add_  <- "NF"
if (CALCULATE_IMPAIRMENTS=="yes" & UPLOAD_NONTATAL=="no") add_  <- "Impairments"
if (UPLOAD_NONTATAL=="yes" & CALCULATE_IMPAIRMENTS=="yes") add_ <- "NF and Impairments"
description <- paste0(add_, " - ", description)
cat(description, file=file.path(j.version.dir, "MODEL_DESCRIPTION.txt"))
#***********************************************************************************************************************


#----FUNCTIONS----------------------------------------------------------------------------------------------------------  
### reading .h5 files
"FILEPATH/read_hdf5_table.R" %>% source

### custom quantile calculator
"FILEPATH/collapse_point.R" %>% source
                      
### load shared functions
file.path(j_root, "FILEPATH/get_location_metadata.R") %>% source
file.path(j_root, "FILEPATH/get_population.R") %>% source
file.path(j_root, "FILEPATH/get_draws.R") %>% source
file.path(j_root, "FILEPATH/get_ids.R") %>% source
file.path(j_root, "FILEPATH/get_epi_data.R") %>% source
file.path(j_root, "FILEPATH/upload_epi_data.R") %>% source
#*********************************************************************************************************************** 


########################################################################################################################
##### PART ONE: CODEm MODEL FOR FATAL ##################################################################################
########################################################################################################################


#----PULL---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=22)[, ## lsid=22 is from covariates team, id=9 is from epi
                     .(location_id, ihme_loc_id, location_name, region_id, super_region_id, level, location_type, parent_id, sort_order)]
pop_locs <- unique(locations[level >= 3, location_id])

### pull CodCorrect results for tetanus
available_years <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=2833, source="epi", location_id=6, sex_id=1, age_group_id=6, gbd_round_id=gbd_round, status="best")[, year_id] %>% unique
cod_draws <- get_draws(gbd_id_type="cause_id", gbd_id=cause_id, source="codcorrect", location_id=pop_locs, year_id=available_years, gbd_round_id=gbd_round, measure_id=1, status=compare_version)
# save model version
if (!is.null(unique(cod_draws$output_version_id))) vers <- unique(cod_draws$output_version_id) else vers <- custom_version
cat(paste0("CodCorrect results - output version ", vers),
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

### get population data
population <- get_population(location_id=pop_locs, year_id=1980:year_end, age_group_id=-1, sex_id=1:2, gbd_round_id=gbd_round)
# save model version
cat(paste0("Population - model run ", unique(population$run_id)), 
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

# merge on population
cod_draws <- merge(cod_draws, population, by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=TRUE)[, c(draw_cols_upload, "population"), with=FALSE]
colnames(cod_draws) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("death_draw_", draw_nums_gbd), "pop")
#***********************************************************************************************************************


########################################################################################################################
##### PART TWO: DisMod MODEL FOR CFR ###################################################################################
########################################################################################################################


#----PULL---------------------------------------------------------------------------------------------------------------
### get CFR draws from DisMod model -- (proportion model measure_id=18)
cfr_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=2833, source="epi", location_id=pop_locs, sex_id=1:2, 
                       age_group_id=c(2:20, 30:32, 235), gbd_round_id=gbd_round, status="best")
# save model version
cat(paste0("Case fatality ratio DisMod model (me_id 2833) - model run ", unique(cfr_draws$model_version_id)),
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
# remove excess columns
cfr_draws <- cfr_draws[, draw_cols_upload, with=FALSE]
colnames(cfr_draws) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("cfr_draw_", draw_nums_gbd))
#***********************************************************************************************************************


########################################################################################################################
##### PART THREE: CALCULATE DEATHS, PREVALENCE, INCIDENCE ##############################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
### pull in duration data
duration <- read.csv(file.path(j_root, "FILEPATH/duration_draws.csv")) %>% data.table %>% 
  .[cause=="A07.a"] %>%
  .[, paste0("draw", draw_nums_gbd), with=FALSE]
colnames(duration) <- paste0("dur_draw_", draw_nums_gbd)

# merge codcorrect death draws with population, cfr draws, and duration draws
death_input <- merge(cfr_draws, cod_draws, by=c("location_id", "year_id", "age_group_id", "sex_id"))
death_input[, paste0("dur_draw_", draw_nums_gbd) := duration[, paste0("dur_draw_", draw_nums_gbd), with=FALSE]]
#***********************************************************************************************************************


#----CALCULATE----------------------------------------------------------------------------------------------------------
### calculate death rate as deaths / population
### calculate prevalence as ( death rate / CFR ) * duration
### calculate incidence as death rate / CFR
lapply(draw_nums_gbd, function(ii) {
    death_input[, paste0("drate_draw_", ii) := get(paste0("death_draw_", ii)) / pop]
    death_input[, paste0("prev_draw_", ii) := ( get(paste0("drate_draw_", ii)) / get(paste0("cfr_draw_", ii)) ) * get(paste0("dur_draw_", ii))]
    death_input[, paste0("inc_draw_", ii) := get(paste0("drate_draw_", ii)) / get(paste0("cfr_draw_", ii))]
    death_input[, paste0("case_draw_", ii) := get(paste0("inc_draw_", ii)) * pop]
})
#***********************************************************************************************************************


# ----SAVE--------------------------------------------------------------------------------------------------------------
### prevalence
predictions_prev_save <- death_input[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("prev_draw_", draw_nums_gbd)), with=FALSE]

### incidence
predictions_inc_save <- death_input[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("inc_draw_", draw_nums_gbd)), with=FALSE]

### case counts
predictions_case_save <- death_input[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("case_draw_", draw_nums_gbd)), with=FALSE]

### save results
if (WRITE_FILES == "yes") {
	write.csv(predictions_prev_save, file.path(j.version.dir, "01_prev_draws.csv"), row.names=FALSE)
	write.csv(predictions_inc_save, file.path(j.version.dir, "02_inc_draws.csv"), row.names=FALSE)
	write.csv(predictions_case_save, file.path(j.version.dir, "03_case_draws.csv"), row.names=FALSE)
}
#***********************************************************************************************************************


if (UPLOAD_NONTATAL == "yes") {

########################################################################################################################
##### PART FOUR: FORMAT FOR COMO #######################################################################################
########################################################################################################################


#----SAVE RESULTS-------------------------------------------------------------------------------------------------------
### format prevalence for como, measure_id==5
colnames(predictions_prev_save) <- draw_cols_upload
predictions_prev_save[, measure_id := 5]

### format incidence for como, measure_id==6
colnames(predictions_inc_save) <- draw_cols_upload
predictions_inc_save[, measure_id := 6]

### save nonfatal estimates to cluster
predictions <- rbind(predictions_prev_save, predictions_inc_save)
lapply(unique(predictions$location_id), function(x) write.csv(predictions[location_id==x],
                  file.path(cl.version.dir, paste0(x, ".csv")), row.names=FALSE))
print(paste0("Nonfatal results saved in ", cl.version.dir))

### upload results
# save tetanus draws, modelable_entity_id=1425
job <- paste0("qsub -N s_epi_", acause, "_1425 -pe multi_slot 45 -P proj_covariates -o FILEPATH/", username, " -e FILEPATH/", username, " ",
              "FILEPATH/r_shell.sh FILEPATH/save_results_wrapper.r",
              " --args",
              " --type epi",
              " --me_id ", 1425,
              " --input_directory ", cl.version.dir,
              " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
              " --year_ids ", paste(unique(predictions$year_id), collapse=","),
              " --best ", mark_model_best)
system(job); print(job)

# save severe tetanus draws, modelable_entity_id=1426
job <- paste0("qsub -N s_epi_", acause, "_1426 -pe multi_slot 45 -P proj_covariates -o FILEPATH/", username, " -e FILEPATH/", username, " ",
              "FILEPATH/r_shell.sh FILEPATH/save_results_wrapper.r",
              " --args",
              " --type epi",
              " --me_id ", 1426, 
              " --input_directory ", cl.version.dir,
              " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
              " --year_ids ", paste(unique(predictions$year_id), collapse=","),
              " --best ", mark_model_best)
system(job); print(job)
#***********************************************************************************************************************

}


if (CALCULATE_IMPAIRMENTS == "yes") {

########################################################################################################################
##### PART FIVE: PREP MOTOR IMPAIRMENT MODELS ##########################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
inc_draws <- death_input[age_group_id <= 3, ]
#***********************************************************************************************************************  


#----MOTOR IMPAIRMENT---------------------------------------------------------------------------------------------------
### calculate incidence of survival = incidence * (1 - cfr)
### calculate incidence of mild motor impairment at a proportion of 0.11 (meta-analysis)
### calculate incidence of moderate to severe motor impairment at a proportion of 0.07
lapply(draw_nums_gbd, function(ii) {
    inc_draws[, paste0("survival_draw_", ii) := get(paste0("inc_draw_", ii)) * ( 1 - get(paste0("cfr_draw_", ii)) )]
    inc_draws[, paste0("mild_draw_", ii) := get(paste0("survival_draw_", ii)) * 0.11]
    inc_draws[, paste0("modsev_draw_", ii) := get(paste0("survival_draw_", ii)) * 0.07]
})
mild_impairment <- inc_draws[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("mild_draw_", draw_nums_gbd)), with=FALSE]
mod_sev_impairment <- inc_draws[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("modsev_draw_", draw_nums_gbd)), with=FALSE]

### save results
if (WRITE_FILES == "yes") {
  write.csv(mild_impairment, file.path(j.version.dir, "04_mild_impairment_draws.csv"), row.names=FALSE)
  write.csv(mod_sev_impairment, file.path(j.version.dir, "05_mod_sev_impairment_draws.csv"), row.names=FALSE)
}
#***********************************************************************************************************************


########################################################################################################################
##### PART SIX: UPLOAD IMPAIRMENTS #####################################################################################
########################################################################################################################


#----MILD---------------------------------------------------------------------------------------------------------------
### clear epi data in the appropriate bundle
mild_epi_data <- get_epi_data(bundle_id=mild_bundle_id)[, .(seq)]
clear_old_path <- file.path(j_root, "FILEPATH", paste0("mild_impairment_clear_old_data_", custom_version, ".xlsx"))
write.xlsx(mild_epi_data, clear_old_path, row.names=FALSE, col.names=TRUE, sheetName="extraction")
upload_epi_data(bundle_id=mild_bundle_id, filepath=clear_old_path)
# make sure bundle is empty before moving on
mild_epi_data <- get_epi_data(bundle_id=mild_bundle_id)[, .(seq)]
if (length(mild_epi_data$seq) >= 1) stop(paste0("STOP | Bundle ", mild_bundle_id, " not empty for new upload"))

### format results for epi uploader template -- to model in DisMod
# mild impairment
mild_impairment <- mild_impairment %>% collapse_point
sex_map <- get_ids("sex")
mild_impairment <- merge(mild_impairment, sex_map, by="sex_id")
mild_impairment[, c("age_start", "age_end") := c(0, 0)] 
setnames(mild_impairment, "year_id", "year_start")
mild_impairment[, year_end := year_start]
mild_impairment[, c("age_group_id", "sex_id", "year_id") := NULL]
mild_impairment[, bundle_id := mild_bundle_id]
mild_impairment[, nid := 254292]
mild_impairment[, modelable_entity_id := 1427]
mild_impairment[, modelable_entity_name := "Mild impairment due to neonatal tetanus"]
mild_impairment[, measure := "incidence"]
mild_impairment[, c("underlying_nid", "parent_id", "underlying_field_citation_value", "field_citation_value", "file_path",
                    "page_num", "table_num", "site_memo", "cases", "sample_size", "effective_sample_size", "design_effect",
                    "measure_adjustment", "uncertainty_type", "standard_error", "recall_type_value", "sampling_type",
                    "response_rate", "case_name", "case_definition", "case_diagnostics", "group", "specificity",
                    "group_review", "note_modeler", "note_SR", "seq", "seq_parent") := ""]
mild_impairment[, c("sex_issue", "year_issue", "aeg_issue", "age_demographer", "measure_issue", "is_outlier",
                    "smaller_site_unit") := 0]
mild_impairment[, input_type := "adjusted"]
mild_impairment[, source_type := "Mixed or estimation"]
mild_impairment[, representative_name := "Nationally representative only"]
mild_impairment[location_id %in% locations[level >= 4, location_id], representative_name := "Representative for subnational location only"]
mild_impairment[, urbanicity_type := "Unknown"]
mild_impairment[, unit_type := "Person"]
mild_impairment[, unit_value_as_published := 1]
mild_impairment[, uncertainty_type_value := 95]
mild_impairment[, recall_type := "Point"]
mild_impairment[, extractor := username]
                                    
### save and upload model results for DisMod impairment models
new_path <- file.path(j_root, "FILEPATH", paste0("mild_impairment_new_data_", custom_version, ".xlsx"))
mild_impairment[, data_sheet_file_path := new_path]
write.xlsx(mild_impairment, new_path, row.names=FALSE, col.names=TRUE, sheetName="extraction")
upload_epi_data(bundle_id=mild_bundle_id, filepath=new_path)
#***********************************************************************************************************************


#----MOD/SEV------------------------------------------------------------------------------------------------------------
### clear epi data in the appropriate bundle
modsev_epi_data <- get_epi_data(bundle_id=modsev_bundle_id)[, .(seq)]
clear_old_path_modsev <- file.path(j_root, "FILEPATH", paste0("modsev_impairment_clear_old_data_", custom_version, ".xlsx"))
write.xlsx(modsev_epi_data, clear_old_path_modsev, row.names=FALSE, col.names=TRUE, sheetName="extraction")
upload_epi_data(bundle_id=modsev_bundle_id, filepath=clear_old_path_modsev)
# make sure bundle is empty before moving on
modsev_epi_data <- get_epi_data(bundle_id=modsev_bundle_id)[, .(seq)]
if (length(modsev_epi_data$seq) >= 1) stop(paste0("STOP | Bundle ", modsev_bundle_id, " not empty for new upload"))

### format results for epi uploader template -- to model in DisMod
# moderate to severe impairment
mod_sev_impairment <- mod_sev_impairment %>% collapse_point
mod_sev_impairment <- merge(mod_sev_impairment, sex_map, by="sex_id")
mod_sev_impairment[, c("age_start", "age_end") := c(0, 0)] 
setnames(mod_sev_impairment, "year_id", "year_start")
mod_sev_impairment[, year_end := year_start]
mod_sev_impairment[, c("age_group_id", "sex_id", "year_id") := NULL]
mod_sev_impairment[, bundle_id := modsev_bundle_id]
mod_sev_impairment[, nid := 254293]
mod_sev_impairment[, modelable_entity_id := 1428]
mod_sev_impairment[, modelable_entity_name := "Moderate to severe impairment due to neonatal tetanus"]
mod_sev_impairment[, measure := "incidence"]
mod_sev_impairment[, c("underlying_nid", "parent_id", "underlying_field_citation_value", "field_citation_value", "file_path",
                       "page_num", "table_num", "site_memo", "cases", "sample_size", "effective_sample_size", "design_effect",
                       "measure_adjustment", "uncertainty_type", "standard_error", "recall_type_value", "sampling_type",
                       "response_rate", "case_name", "case_definition", "case_diagnostics", "group", "specificity",
                       "group_review", "note_modeler", "note_SR", "seq", "seq_parent") := ""]
mod_sev_impairment[, c("sex_issue", "year_issue", "aeg_issue", "age_demographer", "measure_issue", "is_outlier",
                       "smaller_site_unit") := 0]
mod_sev_impairment[, input_type := "adjusted"]
mod_sev_impairment[, source_type := "Mixed or estimation"]
mod_sev_impairment[, representative_name := "Nationally representative only"]
mod_sev_impairment[location_id %in% locations[level >= 4, location_id], representative_name := "Representative for subnational location only"]
mod_sev_impairment[, urbanicity_type := "Unknown"]
mod_sev_impairment[, unit_type := "Person"]
mod_sev_impairment[, unit_value_as_published := 1]
mod_sev_impairment[, uncertainty_type_value := 95]
mod_sev_impairment[, recall_type := "Point"]
mod_sev_impairment[, extractor := username]

### save and upload model results for DisMod impairment models
new_path_modsev <- file.path(j_root, "FILEPATH", paste0("modsev_impairment_new_data_", custom_version, ".xlsx"))
mod_sev_impairment[, data_sheet_file_path := new_path_modsev]
write.xlsx(mod_sev_impairment, new_path_modsev, row.names=FALSE, col.names=TRUE, sheetName="extraction")
upload_epi_data(bundle_id=modsev_bundle_id, filepath=new_path_modsev)
#***********************************************************************************************************************

}