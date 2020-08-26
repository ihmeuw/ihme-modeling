#----HEADER-------------------------------------------------------------------------------------------------------------
# Author: "USERNAME"; "USERNAME"
# Purpose: PART ONE -   Run CODEm model for fatal outcomes
#          PART TWO -   Run DisMod model for case fatality rate using sex- and age-split data (MR-BRT, out-of-DisMod)
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
pacman::p_load(magrittr, foreign, stats, MASS, data.table, dplyr, plyr, lme4, reshape2, parallel, foreach, doParallel, reticulate)
if (Sys.info()["sysname"]=="Linux") {
  library(rhdf5) 
  library(openxlsx) 
} else { 
  pacman::p_load(rhdf5, openxlsx)
}
use_python('"FILEPATH"python')
gd <- import('get_draws.api')

setDTthreads(1)
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause           <- "tetanus"
cause_id         <- 340
mild_bundle_id   <- 47
modsev_bundle_id <- 48
gbd_round        <- 6
year_end         <- if (gbd_round %in% c(3, 4, 5)) gbd_round + 2012 else if (gbd_round==6) 2019

### draw numbers
if (FauxCorrect) {
  draw_nums_gbd  <- 0:99
} else {
  draw_nums_gbd  <- 0:999
}
draw_cols_upload <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", draw_nums_gbd))

### make folders on cluster
cl.version.dir <- file.path(FILEPATH)                                                
if (!dir.exists(cl.version.dir)) dir.create(cl.version.dir, recursive=TRUE)

### directories
home <- file.path(FILEPATH)
j.version.dir <- file.path(FILEPATH)
if (!dir.exists(j.version.dir)) dir.create(j.version.dir, recursive=TRUE)
j.version.dir.logs <- file.path(j.version.dir, "model_logs")
if (!dir.exists(j.version.dir.logs)) dir.create(j.version.dir.logs, recursive=TRUE)

### save description of model run
if (UPLOAD_NONFATAL=="yes" & CALCULATE_IMPAIRMENTS=="no") add_  <- "NF"
if (CALCULATE_IMPAIRMENTS=="yes" & UPLOAD_NONFATAL=="no") add_  <- "Impairments"
if (UPLOAD_NONFATAL=="yes" & CALCULATE_IMPAIRMENTS=="yes") add_ <- "NF and Impairments"
description <- paste0(add_, " - ", description)
cat(description, file=FILEPATH)
#***********************************************************************************************************************


#----FUNCTIONS----------------------------------------------------------------------------------------------------------  
### reading .h5 files
""FILEPATH"read_hdf5_table.R" %>% source

### custom quantile calculator
""FILEPATH"collapse_point.R" %>% source

### custom workflow functions
'%!in%' <- function(x,y)!('%in%'(x,y))

### load shared functions
source(""FILEPATH"get_location_metadata.R") 
source(""FILEPATH"get_population.R") 
source(""FILEPATH"get_draws.R")
source(""FILEPATH"get_ids.R")
source(""FILEPATH"get_bundle_data.R") 
source(""FILEPATH"upload_bundle_data.R") 
source('"FILEPATH"save_bundle_version.R')
source('"FILEPATH"get_bundle_version.R')
source('"FILEPATH"save_crosswalk_version.R')
source('"FILEPATH"get_crosswalk_version.R')
source(""FILEPATH"validate_input_sheet.R")

#*********************************************************************************************************************** 


########################################################################################################################
##### PART ONE: CODEm MODEL FOR FATAL ##################################################################################
########################################################################################################################


#----PULL---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=22)[, 
                     .(location_id, ihme_loc_id, location_name, region_id, super_region_id, level, location_type, parent_id, sort_order)]
pop_locs <- unique(locations[level >= 3, location_id])
standard_locations <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=101)$location_id %>% unique
dismod_locs <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=9)$location_id %>% unique

### pull CodCorrect results for tetanus
if (decomp) {
  cfr_dismod <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=2833, source="epi", sex_id=1, age_group_id=6, 
                          gbd_round_id=gbd_round, status="best", decomp_step=decomp_step, location_id=6) 
  available_years <- cfr_dismod[, year_id] %>% unique
} else {
  available_years <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=2833, source="epi", location_id=6, sex_id=1, age_group_id=6, gbd_round_id=gbd_round, status="best")[, year_id] %>% unique
}


if (decomp & FauxCorrect) {
  cod_draws <- get_draws(gbd_id_type="cause_id", gbd_id=cause_id, source="fauxcorrect", location_id=pop_locs, year_id=available_years, gbd_round_id=gbd_round, 
                         measure_id=1, status=compare_version, num_workers=20, decomp_step=decomp_step) 
} else if (decomp & !FauxCorrect) {  
  
  cod_draws <- gd$get_draws(gbd_id_type="cause_id", gbd_id=as.integer(cause_id), location_id=as.integer(pop_locs), year_id=as.integer(available_years),
                         source="codcorrect", measure_id=as.integer(1), version_id=as.integer(99), gbd_round_id=6, decomp_step="step4", num_workers=as.integer(20))  
  cod_draws <- data.table(cod_draws)
  
} else {
  cod_draws <- get_draws(gbd_id_type="cause_id", gbd_id=cause_id, source="codcorrect", location_id=pop_locs, year_id=available_years, gbd_round_id=gbd_round, 
                         measure_id=1, status=compare_version)
}

# save model version
if (!is.null(unique(cod_draws$output_version_id))) vers <- unique(cod_draws$output_version_id) else vers <- custom_version
cat(paste0("CodCorrect results - output version ", vers),
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)


### get population data
if (decomp) {
  population <- get_population(location_id=pop_locs, year_id=1980:year_end, age_group_id=-1, sex_id=1:2, gbd_round_id=gbd_round, decomp_step=decomp_step)
} else {
  population <- get_population(location_id=pop_locs, year_id=1980:year_end, age_group_id=-1, sex_id=1:2, gbd_round_id=gbd_round)
}

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
### get CFR draws from DisMod model 
if (decomp & FauxCorrect) {
  cfr_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=2833, source="epi", location_id=pop_locs, sex_id=1:2, 
                         age_group_id=c(2:20, 30:32, 235), gbd_round_id=gbd_round, status="best", decomp_step=decomp_step, downsample = TRUE, n_draws = 100)

} else if (decomp & !FauxCorrect) {
  
  cfr_draws <- gd$get_draws(gbd_id_type="modelable_entity_id", gbd_id=as.integer(2833), version_id = as.integer(473501), gbd_round_id=6, source="epi",  #442715 was original Step 4 best
                            decomp_step="iterative", location_id=as.integer(pop_locs), sex_id=as.integer(c(1, 2)),
                            age_group_id=as.integer(c(2:20, 30:32, 235)), num_workers=as.integer(10))   
  cfr_draws <- data.table(cfr_draws)  
  
} else {
  cfr_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=2833, source="epi", location_id=pop_locs, sex_id=1:2, 
                         age_group_id=c(2:20, 30:32, 235), gbd_round_id=gbd_round, status="best")
}

# save model version
cat(paste0("Case fatality ratio DisMod model (me_id 2833) - model run ", unique(cfr_draws$model_version_id)),
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

# remove excess columns
if (decomp_step=="step4") {
  cfr_draws <- cfr_draws[, draw_cols_upload, with=FALSE]  
} else {
  cfr_draws <- cfr_draws[, draw_cols_upload, with=FALSE]
}
colnames(cfr_draws) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("cfr_draw_", draw_nums_gbd))
#***********************************************************************************************************************


########################################################################################################################
##### PART THREE: CALCULATE DEATHS, PREVALENCE, INCIDENCE ##############################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
### pull in duration data
if (FauxCorrect) {
  custom_file <- paste0(FILEPATH)
  source_python(FILEPATH)
  duration <- downsample_draws(custom_file, 100L, cause_sub="A07.a")
  colnames(duration) <- c("cause", paste0("dur_draw_", draw_nums_gbd))
  duration <- as.data.table(duration)
} else {
  duration <- read.csv(file.path(j_root, ""FILEPATH"")) %>% data.table %>% 
    .[cause=="A07.a"] %>%
    .[, paste0("draw", draw_nums_gbd), with=FALSE]
  colnames(duration) <- paste0("dur_draw_", draw_nums_gbd)
}

# merge codcorrect death draws with population, cfr draws, and duration draws
death_input <- merge(cfr_draws, cod_draws, by=c("location_id", "year_id", "age_group_id", "sex_id"))
death_input <- death_input[, paste0("dur_draw_", draw_nums_gbd) := duration[, paste0("dur_draw_", draw_nums_gbd), with=FALSE]] 
#***********************************************************************************************************************


#----CALCULATE----------------------------------------------------------------------------------------------------------
death_input <- death_input[, paste0("drate_draw_", 0:999) := lapply(.SD, function(x) x / pop), .SDcols=paste0("death_draw_", 0:999)]
lapply(draw_nums_gbd, function(ii) {  
    death_input <- death_input[, paste0("prev_draw_", ii) := ( get(paste0("drate_draw_", ii)) / get(paste0("cfr_draw_", ii)) ) * get(paste0("dur_draw_", ii))]
    death_input <- death_input[, paste0("inc_draw_", ii) := get(paste0("drate_draw_", ii)) / get(paste0("cfr_draw_", ii))] 
})

death_input <- death_input[, paste0("case_draw_", 0:999) := lapply(.SD, function(x) x / pop), .SDcols=paste0("inc_draw_", 0:999)]

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
	fwrite(predictions_prev_save, file.path(FILEPATH), row.names=FALSE)
	fwrite(predictions_inc_save, file.path(FILEPATH), row.names=FALSE)
	fwrite(predictions_case_save, file.path(FILEPATH), row.names=FALSE)
}
if (WRITE_FILES == "yes" & CALCULATE_IMPAIRMENTS == "no") {
  fwrite(death_input, file.path(FILEPATH), row.names=FALSE)
}
#***********************************************************************************************************************


if (UPLOAD_NONFATAL == "yes") {  

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
lapply(unique(predictions$location_id), function(x) fwrite(predictions[location_id==x],
                  file.path(cl.version.dir, paste0(x, ".csv")), row.names=FALSE))
print(paste0("Nonfatal results saved in ", cl.version.dir))

### upload results
# save tetanus draws, modelable_entity_id=1425
job <- paste0("qsub -N s_epi_", acause, "_1425 -l m_mem_free=120G -l fthread=10 -l archive -l h_rt=10:00:00 -P proj_custom_models -q all.q -o "FILEPATH"", 
              username, " -e "FILEPATH"", username,
              " "FILEPATH"save_results_wrapper.r",
              " --args",
              " --type epi",
              " --me_id ", 1425,
              " --input_directory ", cl.version.dir,
              " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
              " --year_ids ", paste(unique(predictions$year_id), collapse=","),
              " --best ", mark_model_best)
if (decomp) job <- paste0(job, " --decomp_step ", decomp_step)
if (FauxCorrect) job <- paste0(job, " --draws ", length(colnames(predictions)[grepl("draw_", colnames(predictions))]))
system(job); print(job)

# save severe tetanus draws, modelable_entity_id=1426
job <- paste0("qsub -N s_epi_", acause, "_1426 -l m_mem_free=120G -l fthread=10 -l archive -l h_rt=15:00:00 -P proj_custom_models -q all.q -o "FILEPATH"", 
              username, " -e "FILEPATH"", username,
              " "FILEPATH"save_results_wrapper.r",
              " --args",
              " --type epi",
              " --me_id ", 1426,
              " --input_directory ", cl.version.dir,
              " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
              " --year_ids ", paste(unique(predictions$year_id), collapse=","),
              " --best ", mark_model_best)
if (decomp) job <- paste0(job, " --decomp_step ", decomp_step)
if (FauxCorrect) job <- paste0(job, " --draws ", length(colnames(predictions)[grepl("draw_", colnames(predictions))]))
system(job); print(job)
#***********************************************************************************************************************

}


if (CALCULATE_IMPAIRMENTS == "yes") {

death_input <- fread(file.path(j.version.dir, "04_death_input_obj.csv"))
  
########################################################################################################################
##### PART FIVE: PREP MOTOR IMPAIRMENT MODELS ##########################################################################
########################################################################################################################

#----PREP---------------------------------------------------------------------------------------------------------------
inc_draws <- death_input[age_group_id <= 3, ]
#***********************************************************************************************************************  


#----MOTOR IMPAIRMENT---------------------------------------------------------------------------------------------------
for (ii in draw_nums_gbd) {  
  inc_draws[, paste0("survival_draw_", ii) := get(paste0("inc_draw_", ii)) * ( 1 - get(paste0("cfr_draw_", ii)) )]
}
inc_draws <- inc_draws[, paste0("mild_draw_", draw_nums_gbd) := lapply(.SD, function(x) x * 0.11), .SDcols=paste0("survival_draw_", draw_nums_gbd)]
inc_draws <- inc_draws[, paste0("modsev_draw_", draw_nums_gbd) := lapply(.SD, function(x) x * 0.07), .SDcols=paste0("survival_draw_", draw_nums_gbd)]


mild_impairment <- inc_draws[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("mild_draw_", draw_nums_gbd)), with=FALSE]
mod_sev_impairment <- inc_draws[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("modsev_draw_", draw_nums_gbd)), with=FALSE]

### save results
if (WRITE_FILES == "yes") {
  fwrite(mild_impairment, file.path(FILEPATH), row.names=FALSE)
  fwrite(mod_sev_impairment, file.path(FILEPATH), row.names=FALSE)
}
#***********************************************************************************************************************


########################################################################################################################
##### PART SIX: UPLOAD IMPAIRMENTS #####################################################################################
########################################################################################################################


#----MILD---------------------------------------------------------------------------------------------------------------
### clear epi data in the appropriate bundle
print("Gettig mild bundle data")
if (decomp) {
  mild_epi_data <- get_bundle_data(bundle_id=mild_bundle_id, decomp_step=decomp_step, export=TRUE)[, .(seq)]
} else {
  mild_epi_data <- get_bundle_data(bundle_id=mild_bundle_id)[, .(seq)]
}
clear_old_path <- file.path(FILEPATH))
write.xlsx(mild_epi_data, clear_old_path, row.names=FALSE, col.names=TRUE, sheetName="extraction")

print("Clearing mild bundle for new upload")
if (decomp) {
  upload_bundle_data(bundle_id=mild_bundle_id, filepath=clear_old_path, decomp_step=decomp_step)
} else {
  upload_bundle_data(bundle_id=mild_bundle_id, filepath=clear_old_path)
}

if (decomp) {
  mild_epi_data <- get_bundle_data(bundle_id=mild_bundle_id, decomp_step=decomp_step)[, .(seq)]
} else {
  mild_epi_data <- get_bundle_data(bundle_id=mild_bundle_id)[, .(seq)]
}
if (length(mild_epi_data$seq) >= 1) stop(paste0("STOP | Bundle ", mild_bundle_id, " not empty for new upload"))

# mild impairment
mild_impairment <- mild_impairment %>% collapse_point
sex_map <- get_ids("sex")
mild_impairment <- merge(mild_impairment, sex_map, by="sex_id")
mild_impairment[, `:=` (age_start=0, age_end=0)]  ######
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
                    "group_review", "note_modeler", "note_sr", "seq", "seq_parent") := ""] 
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

if (gbd_round==6 & decomp_step=="step2") {
  gbd2017 <- data.table(read_excel(""FILEPATH))
  mild_impairment[location_id %in% gbd2017$location_id, step2_location_year := ""]
  mild_impairment[location_id %!in% gbd2017$location_id, step2_location_year := "New GBD 2019 location"]
}
                                
new_path <- file.path(""FILEPATH"", 
                                   paste0("mild_impairment_new_data_", custom_version, ".xlsx"))
mild_impairment[, data_sheet_file_path := new_path]
write.xlsx(mild_impairment, new_path, row.names=FALSE, col.names=TRUE, sheetName="extraction")

print("Uploading mild bundle with new data")
if (decomp) {
  active_bundle_upload <- upload_bundle_data(bundle_id=mild_bundle_id, filepath=new_path, decomp_step=decomp_step)
} else {
  active_bundle_upload <- upload_bundle_data(bundle_id=mild_bundle_id, filepath=new_path)
}
versions_path   <- file.path(FILEPATH)
version_df <- data.frame("date"         = custom_version,
                         "impairment"   = "mild",
                         "version"      = "active bundle",
                         "version_id"   = active_bundle_upload$request_id[1],
                         "data_path"    = new_path,
                         "description"  = "Mild impairment")
versions_file   <- fread(versions_path) %>% rbind(., version_df, fill=TRUE)

print("Getting the mild bundle with new, active data")
active_bundle   <- get_bundle_data(bundle_id=mild_bundle_id, decomp_step=decomp_step)

print("Saving a mild bundle version")
bundle_metadata <- save_bundle_version(bundle_id=mild_bundle_id, decomp_step=decomp_step)
# append bundle_metadata to file versions log
version_df      <- data.frame("date"         = custom_version,
                              "impairment"   = "mild",
                              "version"      = "bundle",
                              "version_id"   = bundle_metadata$bundle_version_id[1],
                              "data_path"    = new_path,
                              "description"  = "")
versions_file   <- fread(versions_path) %>% rbind(., version_df, fill=TRUE)
fwrite(versions_file, versions_path, row.names=FALSE)

print("Getting the mild bundle version")
bv <- get_bundle_version(bundle_version_id=bundle_metadata$bundle_version_id[1], export=TRUE)
bv <- bv[, crosswalk_parent_seq := seq]
bv <- bv[, c("group", "specificity", "group_review", "seq") := NA]
bv <- bv[location_id %in% dismod_locs]
bv_path <- file.path(FILEPATH)
write.xlsx(bv, bv_path, row.names=FALSE, col.names=TRUE, sheetName="extraction")

# save CROSSWALK VERSION which will be used in Dismod model and version_id log
print("Saving mild bundle crosswalk version for Dismod")
crosswalk_description <- paste0("For DisMod mild model, made ", custom_version)
xw_metadata           <- save_crosswalk_version(bundle_version_id=bundle_metadata$bundle_version_id[1], description=crosswalk_description,
                                                data_filepath=bv_path)
version_df            <- data.frame("date"         = custom_version,
                                    "impairment"   = "mild",
                                    "version"      = "crosswalk",
                                    "version_id"   = xw_metadata$crosswalk_version_id[1],
                                    "data_path"    = bv_path,
                                    "description"  = crosswalk_description)
versions_file <- fread(versions_path) %>% rbind(., version_df, fill=TRUE)
fwrite(versions_file, versions_path, row.names=FALSE)

#***********************************************************************************************************************



#----MOD/SEV------------------------------------------------------------------------------------------------------------
### clear epi data in the appropriate bundle
if (decomp) {
  modsev_epi_data <- get_bundle_data(bundle_id=modsev_bundle_id, decomp_step=decomp_step, export=TRUE)[, .(seq)]
} else {
  modsev_epi_data <- get_bundle_data(bundle_id=modsev_bundle_id)[, .(seq)]
}

clear_old_path_modsev <- file.path(FILEPATH)
write.xlsx(modsev_epi_data, clear_old_path_modsev, row.names=FALSE, col.names=TRUE, sheetName="extraction")
if (decomp) {
  upload_bundle_data(bundle_id=modsev_bundle_id, filepath=clear_old_path_modsev, decomp_step=decomp_step)
} else {
  upload_bundle_data(bundle_id=modsev_bundle_id, filepath=clear_old_path_modsev)
}

# make sure bundle is empty before moving on
if (decomp) {
  modsev_epi_data <- get_bundle_data(bundle_id=modsev_bundle_id, decomp_step=decomp_step)[, .(seq)]
} else {
  modsev_epi_data <- get_bundle_data(bundle_id=modsev_bundle_id)[, .(seq)]
}
if (length(modsev_epi_data$seq) >= 1) stop(paste0("STOP | Bundle ", modsev_bundle_id, " not empty for new upload"))

# moderate to severe impairment
mod_sev_impairment <- mod_sev_impairment %>% collapse_point
sex_map <- get_ids("sex")
mod_sev_impairment <- merge(mod_sev_impairment, sex_map, by="sex_id")
mod_sev_impairment[, `:=` (age_start=0, age_end=0)]  
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
                       "group_review", "note_modeler", "note_sr", "seq", "seq_parent") := ""]  
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

if (gbd_round==6 & decomp_step=="step2") {
  gbd2017 <- data.table(read_excel(""FILEPATH))
  mod_sev_impairment[location_id %in% gbd2017$location_id, step2_location_year := ""]
  mod_sev_impairment[location_id %!in% gbd2017$location_id, step2_location_year := "New GBD 2019 location"]
}

### save and upload model results for DisMod impairment models
new_path_modsev <- file.path(FILEPATH)
mod_sev_impairment[, data_sheet_file_path := new_path_modsev]
openxlsx::write.xlsx(mod_sev_impairment, new_path_modsev, row.names=FALSE, col.names=TRUE, sheetName="extraction")
if (decomp) {
  active_bundle_upload <- upload_bundle_data(bundle_id=modsev_bundle_id, filepath=new_path_modsev, decomp_step=decomp_step)
} else {
  active_bundle_upload <- upload_bundle_data(bundle_id=modsev_bundle_id, filepath=new_path_modsev)
}
version_df <- data.frame("date"         = custom_version,
                         "impairment"   = "moderate to severe",
                         "version"      = "active bundle",
                         "version_id"   = active_bundle_upload$request_id[1],
                         "data_path"    = new_path_modsev,
                         "description"  = "Moderate/severe impairment")
versions_file   <- fread(versions_path) %>% rbind(., version_df, fill=TRUE)

active_bundle   <- get_bundle_data(bundle_id=modsev_bundle_id, decomp_step=decomp_step)
bundle_metadata <- save_bundle_version(bundle_id=modsev_bundle_id, decomp_step=decomp_step)
versions_path   <- file.path(j_root, ""FILEPATH"")
# append bundle_metadata to file versions log
version_df      <- data.frame("date"         = custom_version,
                              "impairment"   = "moderate to severe",
                              "version"      = "bundle",
                              "version_id"   = bundle_metadata$bundle_version_id[1],
                              "data_path"    = new_path_modsev,
                              "description"  = "Moderate/severe impairment")
versions_file   <- fread(versions_path) %>% rbind(., version_df, fill=TRUE)
fwrite(versions_file, versions_path, row.names=FALSE)

bv <- get_bundle_version(bundle_version_id=bundle_metadata$bundle_version_id[1], export=TRUE)
bv <- bv[, crosswalk_parent_seq := seq]
bv <- bv[, c("group", "specificity", "group_review", "seq") := NA]
bv <- bv[location_id %in% dismod_locs]
bv_path <- file.path(FILEPATH)
openxlsx::write.xlsx(bv, bv_path, row.names=FALSE, col.names=TRUE, sheetName="extraction")

crosswalk_description <- paste0("For DisMod modsev model, made ", custom_version)
xw_metadata           <- save_crosswalk_version(bundle_version_id=bundle_metadata$bundle_version_id[1], description=crosswalk_description,
                                                data_filepath=bv_path)
version_df            <- data.frame("date"         = custom_version,
                                    "impairment"   = "moderate to severe",
                                    "version"      = "crosswalk",
                                    "version_id"   = xw_metadata$crosswalk_version_id[1],
                                    "data_path"    = bv_path,
                                    "description"  = crosswalk_description)
versions_file <- fread(versions_path) %>% rbind(., version_df, fill=TRUE)
fwrite(versions_file, versions_path, row.names=FALSE)

#***********************************************************************************************************************

}