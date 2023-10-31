rm(list=ls())

library("data.table")
library("tidyr")
library("dplyr")
library("stringr")
library("reshape2")
library("msm")
library("xlsx")
library("binom")
library("openxlsx")


# set bundle and step
bundle_id <- 555
bundle_id_in <- 555

decomp_step <- 'iterative'
round <- 7

agePatternMeid <- 10140
#prop_meid <- 23991  


todo_splits <- c("age", "sex")
todo_xwalk <- c("passive")

outfile_stub <- paste0("bundle", bundle_id, "_simpleSplits", 
                       "_xwalk",
                       paste0(str_to_title(todo_xwalk), collapse = ""),
                       "_", decomp_step)

description <- "DESCRIPTION"



# set directories
if (Sys.info()["sysname"]=="Windows") {
  h_drive <- "FILEPATH"
  j_drive <- "FILEPATH"
} else {
  h_drive <- "FILEPATH"
  j_drive <- "FILEPATH"
}


# load shared functions
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_population.R")
source("FILEPATHget_age_metadata.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")

source("FILEPATH/age_split_data.R")
source("FILEPATH/sex_split_functions_simple.R")




 ## clear bundle
 seqPull <- get_bundle_data(bundle_id, decomp_step, gbd_round_id = round)
 
 clearSeqPath <- paste0("FILEPATH_", bundle_id, "_" ,decomp_step, ".xlsx")
 write.csv(seqPull, file = sub(".xlsx$", "_backup.csv", clearSeqPath), row.names = F)
 openxlsx::write.xlsx(data.frame(seq = seqPull[,seq]), clearSeqPath, sheetName = "extraction", row.names = F)
 
 upload_bundle_data(bundle_id = bundle_id, decomp_step = decomp_step, gbd_round_id = round, filepath = clearSeqPath)
 
 
 
 ## get the data from the bundle version created above
 raw_in <- get_bundle_data(556, decomp_step, gbd_round_id = round)
 raw_in <- raw_in[nid!=292827, ]
 
 vr <- fread(paste0(j_drive, "FILEPATH.csv"))
 
 litVr <- rbind(raw_in, vr, fill = T)
 litVr[, seq := NA]
 
 litVr <- unique(litVr)
 
 litVrPath <- paste0("FILEPATH", round, "FILEPATH", bundle_id, "_" ,decomp_step, ".xlsx")
 openxlsx::write.xlsx(litVr, litVrPath, sheetName = "extraction", row.names = F)

 upload_bundle_data(bundle_id = bundle_id, decomp_step = decomp_step, gbd_round_id = round, filepath = litVrPath)


# get the data from the bundle version created above
raw <- get_bundle_data(bundle_id, decomp_step, gbd_round_id = round)


# we'll want to add some modeler notes to track the age/sex splits; not all datasheets have this column so add if it doesn't exist
if (!("note_modeler" %in% names(raw))) {
  raw$note_modeler <- as.character()
}

if (!("group_review" %in% names(raw))) {
  raw$group_review <- as.integer()
}

if (!("specificity" %in% names(raw))) {
  raw$specificity <- as.character()
}

setDT(raw)

### SEX SPLIT ------------------------------------------------------------------------------------------

if ('sex' %in% todo_splits) {
  # pull the mr-brt model for the sex split
  preds <- readRDS(paste0(h_drive, "FILEPATH.rds"))
  preds$model_summaries$Y_mean <- preds$model_summaries$Y_mean * -1
  preds$model_draws <- preds$model_draws * -1
  
  # execute the split
  split_temp <- sex_split_data(raw, preds)
  
} else {
  split_temp <- copy(raw)
}


### AGE SPLIT ------------------------------------------------------------------------------------------



if ('age' %in% todo_splits) {
  
  tosplit_age <- split_temp[(age_end-age_start)>25 & source_type=="Case notifications - other/unknown",]
  
  # convert year_start and year_end to year_id by determining the closest GBD year (epi) to the study mid-point
  demog <- get_demographics("epi")
  years <- demog$year_id
  
  tosplit_age[, yearMid := (year_start + year_end)/2 ]
  tosplit_age$gbd_year <- sapply(tosplit_age$yearMid, function(x) {tail(years[which(abs(years - x) == min(abs(years - x)))], n=1)})
  
  age_meta <- get_age_metadata(age_group_set_id=12, gbd_round_id=6)[, age_group_weight_value := NULL]
  names(age_meta) <- gsub("group_years_", "", names(age_meta))
  
  incMaster <- get_model_results(gbd_team = "epi", gbd_id = agePatternMeid, gbd_round_id = round, location_id = unique(tosplit_age[, location_id]),  
                                 year_id = unique(tosplit_age[, gbd_year]),  sex_id = -1, age_group_id = -1, decomp_step = decomp_step)
  
  popMaster <- get_population(location_id = unique(tosplit_age[, location_id]), year_id = unique(tosplit_age[, gbd_year]),  sex_id = 1:2, 
                              age_group_id = -1, decomp_step = decomp_step, gbd_round_id = round)
  popMaster <- popMaster[, c("age_group_id", "location_id", "year_id", "sex_id", "population")]
  
  
  age_split <- do.call(rbind, lapply(1:nrow(tosplit_age), function(x) {age_split_data(tosplit_age, x, incMaster, popMaster, age_meta, description = "age split off DisMod")}))
  
  split_temp <- rbind(split_temp[!((age_end-age_start)>25 & source_type=="Case notifications - other/unknown"),], age_split, fill = T)
  
}



bkup <- copy(split_temp)

### APPLY PASSIVE SURVEILLANCE CROSSWALK ----------------------------------------------------------------------------------------

xwalk <- function(dt, cv, preds) {
  #cv <- "cv_passive"
  #dt <- split_temp
  
  # get the passive surveillance rows
  setnames(dt, old = cv, new = "cv_xwalkTemp")
  to_xwalk <- copy(dt)
  
  to_xwalk <- to_xwalk[cv_xwalkTemp!=0,]
  
  to_xwalk[, alpha := mean * (mean - mean^2 - standard_error^2) / standard_error^2] 
  to_xwalk[, beta := alpha * (1 - mean) / mean]
  
  pred_draws <- as.data.table(preds$model_draws)[, c("X_intercept", "Z_intercept") := NULL]
  
  draws <- do.call(cbind, lapply(1:nrow(to_xwalk), function(x) {
    rbeta(1000, to_xwalk$alpha[x], to_xwalk$beta[x]) / exp(as.numeric(pred_draws) * to_xwalk$cv_xwalkTemp[x])}))
  
  to_xwalk$mean <- to_xwalk$mean / exp(preds$model_summaries$Y_mean)
  to_xwalk$standard_error <- apply(draws,2,sd)
  to_xwalk[, c("lower", "upper", "sample_size", "uncertainty_type") := NA]
  to_xwalk[, crosswalk_parent_seq := seq]
  
  to_xwalk[, c("alpha", "beta") := NULL]
  
  
  dt <- rbind(dt[cv_xwalkTemp==0, ], to_xwalk)
  setnames(dt, old = "cv_xwalkTemp", new = cv)
  
  return(dt)
}


if ("passive" %in% todo_xwalk) {
  # pull the mr-brt model for the sex split
  preds <- readRDS(paste0(h_drive, "FILEPATH.rds"))
  
  split_temp <- xwalk(dt = split_temp, cv = "cv_passive", preds = preds)
}




### PRODUCE AND UPLOAD CROSSWALKED DATASET --------------------------------------------------------------------------------------

to_upload <- copy(split_temp)

to_upload[, seq := NA] #1:nrow(to_upload)]
to_upload <- to_upload[(group_review==1 | is.na(group_review)==TRUE),]
to_upload[is.na(lower)==T | is.na(upper)==T, c("uncertainty_type_value", "lower", "upper") := NA]

data_filepath <- paste0("FILEPATH", outfile_stub, ".xlsx")

to_upload[, c("group_review", "specificity") := NULL]

write.csv(to_upload, file = sub("xlsx$", "csv", data_filepath), row.names = F, na = "")
openxlsx::write.xlsx(to_upload, data_filepath, sheetName = "extraction")


# save bundle to get bundle version id
 result <- save_bundle_version(bundle_id, decomp_step, gbd_round_id = round)
 bundle_version_id <- result$bundle_version_id 
 print(bundle_version_id)


result <- save_crosswalk_version(
  bundle_version_id=bundle_version_id,
  data_filepath=data_filepath,
  description=description)

crosswalk_version_df <- get_crosswalk_version(result$crosswalk_version_id)
