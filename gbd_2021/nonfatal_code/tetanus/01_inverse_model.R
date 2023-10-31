#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:  USERNAME
# Date:    February 2017; Spring 2019 edits
# Path:    FILEPATH
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
use_python('/FILEPATH')
gd <- import('get_draws.api')

### set data.table fread threads to 1
setDTthreads(1)
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause           <- "tetanus"
cause_id         <- 340
mild_bundle_id   <- 47
modsev_bundle_id <- 48
gbd_round        <- 7
year_end         <- if (gbd_round %in% c(3, 4, 5)) gbd_round + 2012 else if (gbd_round==6) 2019 else if (gbd_round==7) 2022

### draw numbers
if (FauxCorrect) {
  draw_nums_gbd  <- 0:99
} else {
  draw_nums_gbd  <- 0:999
}
draw_cols_upload <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", draw_nums_gbd))

### make folders on cluster
cl.version.dir <- file.path("/FILEPATH", acause, "nonfatal", custom_version, "draws")
if (!dir.exists(cl.version.dir)) dir.create(cl.version.dir, recursive=TRUE)

### directories
home <- file.path("FILEPATH", acause, "00_documentation")
j.version.dir <- file.path(home, "models", custom_version)
if (!dir.exists(j.version.dir)) dir.create(j.version.dir, recursive=TRUE)
j.version.dir.logs <- file.path(j.version.dir, "model_logs")
if (!dir.exists(j.version.dir.logs)) dir.create(j.version.dir.logs, recursive=TRUE)

### save description of model run
if (UPLOAD_NONFATAL=="yes" & CALCULATE_IMPAIRMENTS=="no") add_  <- "NF"
if (CALCULATE_IMPAIRMENTS=="yes" & UPLOAD_NONFATAL=="no") add_  <- "Impairments"
if (UPLOAD_NONFATAL=="yes" & CALCULATE_IMPAIRMENTS=="yes") add_ <- "NF and Impairments"
description <- paste0(add_, " - ", description)
cat(description, file=file.path(j.version.dir, "MODEL_DESCRIPTION.txt"))
#***********************************************************************************************************************


#----FUNCTIONS----------------------------------------------------------------------------------------------------------
### reading .h5 files
"/FILEPATH/read_hdf5_table.R" %>% source

### custom quantile calculator
"/FILEPATH/collapse_point.R" %>% source

### custom workflow functions
'%!in%' <- function(x,y)!('%in%'(x,y))

### load shared functions
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_population.R")
source("/FILEPATH/get_draws.R")
source("/FILEPATH/get_ids.R")
source("/FILEPATH/get_bundle_data.R")
source("/FILEPATH/upload_bundle_data.R")
source('/FILEPATH/save_bundle_version.R')
source('/FILEPATH/get_bundle_version.R')
source('/FILEPATH/save_crosswalk_version.R')
source('/FILEPATH/get_crosswalk_version.R')
source("/FILEPATH/validate_input_sheet.R")
source("/FILEPATH/get_model_results.R")


#***********************************************************************************************************************


########################################################################################################################
##### PART ONE: CODEm MODEL FOR FATAL ##################################################################################
########################################################################################################################



#----PULL---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=22, decomp_step=decomp_step)[, ## lsid=22 is from covariates team, id=9 is from epi
                                                                               .(location_id, ihme_loc_id, location_name, region_id, super_region_id, level, location_type, parent_id, sort_order)]
pop_locs <- unique(locations[level >= 3, location_id])
standard_locations <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=101, decomp_step = decomp_step)$location_id %>% unique
dismod_locs <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=9, decomp_step = decomp_step)$location_id %>% unique

### pull CodCorrect results for tetanus
# just pulling modeled Dismod years (so just need from one location)
if (decomp) {
  cfr_dismod <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=2833, source="epi", sex_id=1, age_group_id=6,
                          gbd_round_id=gbd_round, status="best", decomp_step=decomp_step, location_id=6)
  available_years <- cfr_dismod[, year_id] %>% unique
  #available_years <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=2833, source="epi", location_id=6, sex_id=1, age_group_id=6, gbd_round_id=gbd_round, status="best", decomp_step=decomp_step)[, year_id] %>% unique
} else {
  available_years <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=2833, source="epi", location_id=6, sex_id=1, age_group_id=6, gbd_round_id=gbd_round, status="best")[, year_id] %>% unique
}


if (decomp & FauxCorrect) {
  cod_draws <- get_draws(gbd_id_type="cause_id", gbd_id=cause_id, source="fauxcorrect", location_id=pop_locs, year_id=available_years, gbd_round_id=gbd_round,
                         measure_id=1, status=compare_version, num_workers=20, decomp_step=fatal_decomp_step) # For GBD2019 Step 2, hardcoded version 5 (version=5, ) and took out "status=compare_version,"
} else if (decomp & !FauxCorrect & !codem_estimates) {
  cod_draws <- get_draws(gbd_id_type="cause_id", gbd_id=cause_id, location_id=pop_locs, year_id=available_years, source="codcorrect", measure_id=1, status = compare_version,
                  gbd_round_id=gbd_round, num_workers=20, decomp_step=fatal_decomp_step)
} else if (!decomp) {
  cod_draws <- get_draws(gbd_id_type="cause_id", gbd_id=cause_id, source="codcorrect", location_id=pop_locs, year_id=available_years, gbd_round_id=gbd_round,
                         measure_id=1, status=compare_version)
} else if (decomp & codem_estimates){
  cod_draws <- get_draws(gbd_id_type="cause_id", gbd_id=340, source="codem", location_id=pop_locs, year_id=available_years,
                         gbd_round_id=gbd_round, sex_id=c(1,2), measure_id=1, status=compare_version, num_workers=10, decomp_step=fatal_decomp_step)
}

# save model version

if (!is.null(unique(cod_draws$output_version_id))) vers <- unique(cod_draws$output_version_id) else vers <- custom_version
if(!codem_estimates){
  cat(paste0("CodCorrect results - output version ", vers),
  file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
} else {
  cat(paste0("CODEm results - output version ", vers),
  file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
}



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
### get CFR draws from DisMod model -- (proportion model measure_id=18)
if (decomp & FauxCorrect) {
  cfr_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=2833, source="epi", location_id=pop_locs, sex_id=1:2,
                         age_group_id=c(2:3, 388, 389, 238, 34, 6:20, 30:32, 235), gbd_round_id=gbd_round, status="best", decomp_step=decomp_step, downsample = TRUE, n_draws = 100)
} else if (decomp & !FauxCorrect) { #this section for Codcorrect or codem results (becuase both have 1000 draws)

  cfr_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=2833, source="epi", location_id=pop_locs, sex_id=1:2,
                         age_group_id=c(2:3, 388, 389, 238, 34, 6:20, 30:32, 235), gbd_round_id=gbd_round, status="best", decomp_step=decomp_step)

} else if (!decomp){
  cfr_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=2833, source="epi", location_id=pop_locs, sex_id=1:2,
                         age_group_id=c(2:20, 30:32, 235), gbd_round_id=gbd_round, status="best")
}

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
if (FauxCorrect) {
  ### Need to downsample appropriately from 1000 to 100 here, too, and custom outputs so need CC .py function
  custom_file <- paste0("/FILEPATH/duration_draws.csv")
  # Source downsample wrapper
  source_python("/FILEPATH/dsample_py.py")
  # Call python function written in sourced .py
  duration <- downsample_draws(custom_file, 100L, cause_sub="A07.a")
  colnames(duration) <- c("cause", paste0("dur_draw_", draw_nums_gbd))
  duration <- as.data.table(duration)
} else if (!FauxCorrect){
  duration <- read.csv(file.path("FILEPATH/duration_draws.csv")) %>% data.table %>%
    .[cause=="A07.a"] %>%
    .[, paste0("draw", draw_nums_gbd), with=FALSE]
  colnames(duration) <- paste0("dur_draw_", draw_nums_gbd)
}

# merge codcorrect death draws with population, cfr draws, and duration draws
death_input <- merge(cfr_draws, cod_draws, by=c("location_id", "year_id", "age_group_id", "sex_id"))
death_input <- death_input[, paste0("dur_draw_", draw_nums_gbd) := duration[, paste0("dur_draw_", draw_nums_gbd), with=FALSE]]
#***********************************************************************************************************************


#----CALCULATE----------------------------------------------------------------------------------------------------------
### calculate death rate as deaths / population
### calculate prevalence as ( death rate / CFR ) * duration
### calculate incidence as death rate / CFR

death_input <- death_input[, paste0("drate_draw_", 0:999) := lapply(.SD, function(x) x / pop), .SDcols=paste0("death_draw_", 0:999)]

lapply(draw_nums_gbd, function(ii) {
  death_input <- death_input[, paste0("prev_draw_", ii) := ( get(paste0("drate_draw_", ii)) / get(paste0("cfr_draw_", ii)) ) * get(paste0("dur_draw_", ii))]
  death_input <- death_input[, paste0("inc_draw_", ii) := get(paste0("drate_draw_", ii)) / get(paste0("cfr_draw_", ii))]
})

death_input <- death_input[, paste0("case_draw_", 0:999) := lapply(.SD, function(x) x * pop), .SDcols=paste0("inc_draw_", 0:999)]

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
  fwrite(predictions_prev_save, file.path(j.version.dir, "01_prev_draws.csv"), row.names=FALSE)
  fwrite(predictions_inc_save, file.path(j.version.dir, "02_inc_draws.csv"), row.names=FALSE)
  fwrite(predictions_case_save, file.path(j.version.dir, "03_case_draws.csv"), row.names=FALSE)
}
if (WRITE_FILES == "yes" & CALCULATE_IMPAIRMENTS == "no") {
  fwrite(death_input, file.path(j.version.dir, "04_death_input_obj.csv"), row.names=FALSE)
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
  job <- paste0("qsub -N s_epi_", acause, "_1425 -l m_mem_free=120G -l fthread=10 -l archive -l h_rt=10:00:00 -P proj_cov_vpd -q all.q -o /FILEPATH/",
                username, " -e /FILEPATH/", username,
                " /FILEPATH/r_shell.sh /FILEPATH/save_results_wrapper.r",
                " --args",
                " --type epi",
                " --me_id ", 1425,
                " --input_directory ", cl.version.dir,
                " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                " --year_ids ", paste(unique(predictions$year_id), collapse=","),
                " --best ", mark_model_best,
                " --bundle_id ", bundle_id,
                " --xw_id ", xw_id)
  if (decomp) job <- paste0(job, " --decomp_step ", decomp_step)
  if (FauxCorrect) job <- paste0(job, " --draws ", length(colnames(predictions)[grepl("draw_", colnames(predictions))]))
  system(job); print(job)

  # save severe tetanus draws, modelable_entity_id=1426
  job <- paste0("qsub -N s_epi_", acause, "_1426 -l m_mem_free=120G -l fthread=10 -l archive -l h_rt=15:00:00 -P proj_cov_vpd -q all.q -o /FILEPATH/",
                username, " -e /FILEPATH/", username,
                " /FILEPATH/r_shell.sh /FILEPATH/save_results_wrapper.r",
                " --args",
                " --type epi",
                " --me_id ", 1426,
                " --input_directory ", cl.version.dir,
                " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                " --year_ids ", paste(unique(predictions$year_id), collapse=","),
                " --best ", mark_model_best,
                " --bundle_id ", bundle_id,
                " --xw_id ", xw_id)
  if (decomp) job <- paste0(job, " --decomp_step ", decomp_step)
  if (FauxCorrect) job <- paste0(job, " --draws ", length(colnames(predictions)[grepl("draw_", colnames(predictions))]))
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
  for (ii in draw_nums_gbd) {  ## Rewrite this (a) part faster, using lapply or something similar but non-loop
    inc_draws[, paste0("survival_draw_", ii) := get(paste0("inc_draw_", ii)) * ( 1 - get(paste0("cfr_draw_", ii)) )]
  }
  inc_draws <- inc_draws[, paste0("mild_draw_", draw_nums_gbd) := lapply(.SD, function(x) x * 0.11), .SDcols=paste0("survival_draw_", draw_nums_gbd)]
  inc_draws <- inc_draws[, paste0("modsev_draw_", draw_nums_gbd) := lapply(.SD, function(x) x * 0.07), .SDcols=paste0("survival_draw_", draw_nums_gbd)]


  mild_impairment <- inc_draws[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("mild_draw_", draw_nums_gbd)), with=FALSE]
  mod_sev_impairment <- inc_draws[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("modsev_draw_", draw_nums_gbd)), with=FALSE]

  ### save results
  if (WRITE_FILES == "yes") {
    fwrite(mild_impairment, file.path(j.version.dir, "04_mild_impairment_draws.csv"), row.names=FALSE)
    fwrite(mod_sev_impairment, file.path(j.version.dir, "05_mod_sev_impairment_draws.csv"), row.names=FALSE)
  }
  #***********************************************************************************************************************


  ########################################################################################################################
  ##### PART SIX: UPLOAD IMPAIRMENTS #####################################################################################
  ########################################################################################################################


  #----MILD---------------------------------------------------------------------------------------------------------------
  ### clear epi data in the appropriate bundle
  print("Gettig mild bundle data")
  if (decomp) {
    mild_epi_data <- get_bundle_data(bundle_id=mild_bundle_id, decomp_step=decomp_step, export=TRUE, gbd_round_id = gbd_round)[, .(seq)]
  } else {
    mild_epi_data <- get_bundle_data(bundle_id=mild_bundle_id)[, .(seq)]
  }

  #clear bundle if not already empty
  if(nrow(mild_epi_data)>=1) {
    clear_old_path <- file.path("FILEPATH", "03_review/02_upload",
                                paste0("mild_impairment_clear_old_data_", custom_version, ".xlsx"))
    openxlsx::write.xlsx(mild_epi_data, clear_old_path, row.names=FALSE, col.names=TRUE, sheetName="extraction")

    print("Clearing mild bundle for new upload")
    if (decomp) {
      upload_bundle_data(bundle_id=mild_bundle_id, filepath=clear_old_path, decomp_step=decomp_step, gbd_round_id = gbd_round)
    } else {
      upload_bundle_data(bundle_id=mild_bundle_id, filepath=clear_old_path)
    }

  }

  # make sure bundle is empty before moving on
  if (decomp) {
    mild_epi_data <- get_bundle_data(bundle_id=mild_bundle_id, decomp_step=decomp_step, gbd_round_id = gbd_round)[, .(seq)]
  } else {
    mild_epi_data <- get_bundle_data(bundle_id=mild_bundle_id)[, .(seq)]
  }
  if (length(mild_epi_data$seq) >= 1) stop(paste0("STOP | Bundle ", mild_bundle_id, " not empty for new upload"))

  ### format results for epi uploader template -- to model in DisMod
  # mild impairment
  mild_impairment <- mild_impairment %>% collapse_point
  sex_map <- get_ids("sex")
  mild_impairment <- merge(mild_impairment, sex_map, by="sex_id")
  mild_impairment[, `:=` (age_start=0, age_end=0)]  ######
  setnames(mild_impairment, "year_id", "year_start")
  mild_impairment[, year_end := year_start]
  mild_impairment[, c("age_group_id", "sex_id") := NULL]
  mild_impairment[, bundle_id := mild_bundle_id]
  mild_impairment[, nid := 254292]
  mild_impairment[, modelable_entity_id := 1427]
  mild_impairment[, modelable_entity_name := "Mild impairment due to neonatal tetanus"]
  mild_impairment[, measure := "incidence"]
  mild_impairment[, c("underlying_nid", "parent_id", "underlying_field_citation_value", "field_citation_value", "file_path",
                      "page_num", "table_num", "site_memo", "cases", "sample_size", "effective_sample_size", "design_effect",
                      "measure_adjustment", "uncertainty_type", "standard_error", "recall_type_value", "sampling_type",
                      "response_rate", "case_name", "case_definition", "case_diagnostics", "group", "specificity",
                      "group_review", "note_modeler", "note_sr", "seq", "seq_parent") := ""]  # testing note_sr instead of note_SR
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

  ### NEW: Uploader will break on new locations (e.g subnationals), so need to fill out new step2_location_year col
  if (gbd_round==6 & decomp_step=="step2") {
    # try using the downloaded iterative bundle which doesn't contain new GBD 2019 locations
    gbd2017 <- data.table(read_excel("/FILEPATH/iterative_GetBundleData_bundle_47_request_298376.xlsx"))
    mild_impairment[location_id %in% gbd2017$location_id, step2_location_year := ""]
    mild_impairment[location_id %!in% gbd2017$location_id, step2_location_year := "New GBD 2019 location"]
  }

  ### save and upload new active results/"data" for DisMod impairment models
  new_path <- file.path("FILEPATH", "03_review/02_upload",
                        paste0("mild_impairment_new_data_", custom_version, ".xlsx"))
  mild_impairment[, data_sheet_file_path := new_path]
  openxlsx::write.xlsx(mild_impairment, new_path, row.names=FALSE, col.names=TRUE, sheetName="extraction")


  print("Uploading mild bundle with new data")
  if (decomp) {
    active_bundle_upload <- upload_bundle_data(bundle_id=mild_bundle_id, filepath=new_path, decomp_step=decomp_step, gbd_round_id =gbd_round)
  } else {
    active_bundle_upload <- upload_bundle_data(bundle_id=mild_bundle_id, filepath=new_path)
  }

  versions_path   <- file.path("FILEPATH/bundle_id_tracking/combined_impairments_version_log.csv")
  version_df <- data.frame("date"         = custom_version,
                           "impairment"   = "mild",
                           "version"      = "active bundle",
                           "version_id"   = active_bundle_upload$request_id[1],
                           "data_path"    = new_path,
                           "description"  = "Mild impairment")
  versions_file   <- fread(versions_path) %>% rbind(., version_df, fill=TRUE)

  ### save active data as bundle version, then crosswalk version for Dismod model
  # get active BUNDLE DATA and save bundle version and version_id log
  print("Getting the mild bundle with new, active data")
  active_bundle   <- get_bundle_data(bundle_id=mild_bundle_id, decomp_step=decomp_step, gbd_round_id = gbd_round)

  print("Saving a mild bundle version")
  bundle_metadata <- save_bundle_version(bundle_id=mild_bundle_id, decomp_step=decomp_step, gbd_round_id = gbd_round)
  # append bundle_metadata to file versions log
  version_df      <- data.frame("date"         = custom_version,
                                "impairment"   = "mild",
                                "version"      = "bundle",
                                "version_id"   = bundle_metadata$bundle_version_id[1],
                                "data_path"    = new_path,
                                "description"  = "")
  versions_file   <- fread(versions_path) %>% rbind(., version_df, fill=TRUE)
  fwrite(versions_file, versions_path, row.names=FALSE)

  # get newly saved BUNDLE VERSION and make additional column edits to upload crosswalk version
  print("Getting the mild bundle version")
  bv <- get_bundle_version(bundle_version_id=bundle_metadata$bundle_version_id[1], export=TRUE, fetch = "all")
  bv <- bv[, crosswalk_parent_seq := seq]
  bv <- bv[, c("group", "specificity", "group_review", "seq") := NA]
  ## NEW: subset to ONLY locations in location_set_id 9
  bv <- bv[location_id %in% dismod_locs]
  bv_path <- file.path("WORK/12_bundle", acause, mild_bundle_id, "03_review/02_upload",
                       paste0("mild_impairment_new_data_bv_", custom_version, ".xlsx"))
  openxlsx::write.xlsx(bv, bv_path, row.names=FALSE, col.names=TRUE, sheetName="extraction")

  # save CROSSWALK VERSION which will actually use in Dismod model and version_id log
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
    modsev_epi_data <- get_bundle_data(bundle_id=modsev_bundle_id, decomp_step=decomp_step, export=TRUE, gbd_round_id = gbd_round)[, .(seq)]
  } else {
    modsev_epi_data <- get_bundle_data(bundle_id=modsev_bundle_id)[, .(seq)]
  }

  #clear bundle if not yet empty
  if(nrow(modsev_epi_data) >= 1){
    clear_old_path_modsev <- file.path("FILEPATH", "03_review/02_upload",
                                       paste0("modsev_impairment_clear_old_data_", custom_version, ".xlsx"))
    openxlsx::write.xlsx(modsev_epi_data, clear_old_path_modsev, row.names=FALSE, col.names=TRUE, sheetName="extraction")
    print("Clearing mod/sev bundle for new upload")
    if (decomp) {
      upload_bundle_data(bundle_id=modsev_bundle_id, filepath=clear_old_path_modsev, decomp_step=decomp_step, gbd_round_id = gbd_round)
    } else {
      upload_bundle_data(bundle_id=modsev_bundle_id, filepath=clear_old_path_modsev)
    }
  }


  # make sure bundle is empty before moving on
  if (decomp) {
    modsev_epi_data <- get_bundle_data(bundle_id=modsev_bundle_id, decomp_step=decomp_step, gbd_round_id = gbd_round)[, .(seq)]
  } else {
    modsev_epi_data <- get_bundle_data(bundle_id=modsev_bundle_id)[, .(seq)]
  }
  if (length(modsev_epi_data$seq) >= 1) stop(paste0("STOP | Bundle ", modsev_bundle_id, " not empty for new upload"))

  ### format results for epi uploader template -- to model in DisMod
  # moderate to severe impairment
  mod_sev_impairment <- mod_sev_impairment %>% collapse_point
  sex_map <- get_ids("sex")
  mod_sev_impairment <- merge(mod_sev_impairment, sex_map, by="sex_id")
  mod_sev_impairment[, `:=` (age_start=0, age_end=0)]  ######
  setnames(mod_sev_impairment, "year_id", "year_start")
  mod_sev_impairment[, year_end := year_start]
  mod_sev_impairment[, c("age_group_id", "sex_id") := NULL]
  mod_sev_impairment[, bundle_id := modsev_bundle_id]
  mod_sev_impairment[, nid := 254293]
  mod_sev_impairment[, modelable_entity_id := 1428]
  mod_sev_impairment[, modelable_entity_name := "Moderate to severe impairment due to neonatal tetanus"]
  mod_sev_impairment[, measure := "incidence"]
  mod_sev_impairment[, c("underlying_nid", "parent_id", "underlying_field_citation_value", "field_citation_value", "file_path",
                         "page_num", "table_num", "site_memo", "cases", "sample_size", "effective_sample_size", "design_effect",
                         "measure_adjustment", "uncertainty_type", "standard_error", "recall_type_value", "sampling_type",
                         "response_rate", "case_name", "case_definition", "case_diagnostics", "group", "specificity",
                         "group_review", "note_modeler", "note_sr", "seq", "seq_parent") := ""]  # note_SR -> note_sr
  mod_sev_impairment[, c("sex_issue", "year_issue", "age_issue", "age_demographer", "measure_issue", "is_outlier",
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

  ### GBD 2020 S3: use EMR data from NE for mod/sev impairments model
  # find xw used in current bested NE model by pulling best model from one location, sex
  ne_emr_xw <- get_model_results(gbd_team = "epi", gbd_id = 8653, gbd_round_id = gbd_round, decomp_step = decomp_step,
                                     status = "best", location_id = 6, sex_id = 1)

  ne_emr <- get_crosswalk_version(crosswalk_version_id = unique(ne_emr_xw[, crosswalk_version_id]))[measure=="mtexcess"]
  #assign age start and age end to be the midpoint of the age_range. Then drop every other age group over 5, with top age group being 97.5
  my_emr <- copy(ne_emr)
  my_emr <- my_emr[age_end <= 100]
  my_emr[, age := (age_start + age_end)/2]
  my_emr[, c("age_start", "age_end"):= age]
  my_emr <- my_emr[!(age %in% c(12.5, 22.5, 32.5, 42.5, 52.5, 62.5, 72.5, 82.5, 92.5)),] # max age for which we estimate tetanus NF is age grp 235 (99 + )
  my_emr[, seq:=NA]
  #  remove columns that are added in bundle_version and xw stage because we are uploading this data to be active bundle data
  my_emr[, c("crosswalk_parent_seq","year_id", "age", "origin_id", "origin_seq") :=NULL]
  # have standard error column, remove variance because duplicative
  my_emr[, variance := NULL]
  my_emr[, note_modeler := paste0("EMR data from NE model meid 8653, model_version: ", unique(ne_emr_xw$model_version_id), ", crosswalk version: ", unique(ne_emr_xw$crosswalk_version_id))]

  my_emr[, bundle_id := modsev_bundle_id]
  my_emr[, modelable_entity_id := 1428]
  my_emr[, modelable_entity_name := "Moderate to severe impairment due to neonatal tetanus"]

  cat(paste0("Mod/Sev EMR input data from NE model input data crosswalk version: ", unique(ne_emr_xw$crosswalk_version_id)),
      file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

  mod_sev_impairment <- rbind(mod_sev_impairment, my_emr, fill = TRUE)

  ### NEW: Uploader will break on new locations (e.g subnationals), so need to fill out new step2_location_year col
  if (gbd_round==6 & decomp_step=="step2") {
    # try using the downloaded iterative bundle which doesn't contain new GBD 2019 locations
    gbd2017 <- data.table(read_excel("/FILEPATH/iterative_GetBundleData_bundle_48_request_299654.xlsx"))
    mod_sev_impairment[location_id %in% gbd2017$location_id, step2_location_year := ""]
    mod_sev_impairment[location_id %!in% gbd2017$location_id, step2_location_year := "New GBD 2019 location"]
  }

  ### save and upload model results for DisMod impairment models
  new_path_modsev <- file.path("FILEPATH","03_review/02_upload",
                               paste0("modsev_impairment_new_data_", custom_version, ".xlsx"))
  mod_sev_impairment[, data_sheet_file_path := new_path_modsev]
  openxlsx::write.xlsx(mod_sev_impairment, new_path_modsev, row.names=FALSE, col.names=TRUE, sheetName="extraction")
  if (decomp) {
    active_bundle_upload <- upload_bundle_data(bundle_id=modsev_bundle_id, filepath=new_path_modsev, decomp_step=decomp_step, gbd_round_id = gbd_round)
  } else {
    active_bundle_upload <- upload_bundle_data(bundle_id=modsev_bundle_id, filepath=new_path_modsev)
  }
  version_df <- data.frame("date"         = custom_version,
                           "impairment"   = "moderate to severe",
                           "version"      = "active bundle",
                           "version_id"   = active_bundle_upload$request_id[1],
                           "data_path"    = new_path_modsev,
                           "description"  = "Moderate/severe impairment")
  versions_file   <- fread(versions_path) %>% rbind(., version_df, fill=TRUE) # mild and modsev bundle logs are combined! idenitfy which rows belonged to which by the impairment column

  ### save active data as bundle version, then crosswalk version for Dismod model
  # get active BUNDLE DATA and save bundle version and version_id log
  active_bundle   <- get_bundle_data(bundle_id=modsev_bundle_id, decomp_step=decomp_step, gbd_round_id = gbd_round)
  bundle_metadata <- save_bundle_version(bundle_id=modsev_bundle_id, decomp_step=decomp_step, gbd_round_id = gbd_round)
  # append bundle_metadata to file versions log
  version_df      <- data.frame("date"         = custom_version,
                                "impairment"   = "moderate to severe",
                                "version"      = "bundle version",
                                "version_id"   = bundle_metadata$bundle_version_id[1],
                                "data_path"    = new_path_modsev,
                                "description"  = "Moderate/severe impairment")
  versions_file   <- fread(versions_path) %>% rbind(., version_df, fill=TRUE)
  fwrite(versions_file, versions_path, row.names=FALSE)

  # get newly saved BUNDLE VERSION and make additional column edits to upload crosswalk version
  bv <- get_bundle_version(bundle_version_id=bundle_metadata$bundle_version_id[1], export=TRUE, fetch = "all")
  bv <- bv[, crosswalk_parent_seq := seq]
  bv <- bv[, c("group", "specificity", "group_review", "seq") := NA]
  ## NEW: subset to ONLY locations in location_set_id 9
  bv <- bv[location_id %in% dismod_locs]
  bv_path <- file.path("FILEPATH", acause, modsev_bundle_id, "03_review/02_upload",
                       paste0("modsev_impairment_new_data_bv_", custom_version, ".xlsx"))
  openxlsx::write.xlsx(bv, bv_path, row.names=FALSE, col.names=TRUE, sheetName="extraction")

  # save CROSSWALK VERSION which will actually use in Dismod model and version_id log
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
