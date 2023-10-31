rm(list=ls())

library("data.table")
library("tidyr")
library("dplyr")
library("reshape2")
library("msm")
library("xlsx")
library("binom")
library("XLConnect")
library("openxlsx")

# set bundle and step
bundle_id_in <- 18
bundle_id <- 18
decomp_step_in <- 'step1'
decomp_step <- 'iterative'
round <- 7

if (bundle_id==17) {
  agePatternMeid <- 1247
  name <- "prTyph"
}

if (bundle_id==18) {
  agePatternMeid <- 1252
  name <- "prPara"
}

incMeid <- 10140
  
  
  
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
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_demographics.R")



# clear bundle
seqPull <- get_bundle_data(bundle_id, decomp_step, gbd_round_id = round)

clearSeqPath <- paste0("FILEPATH", round, "FILEPATH", bundle_id, "_" ,decomp_step, ".xlsx")
write.csv(seqPull, file = sub(".xlsx$", "_backup.csv", clearSeqPath), row.names = F)
openxlsx::write.xlsx(data.frame(seq = seqPull[,seq]), clearSeqPath, sheetName = "extraction", row.names = F)

upload_bundle_data(bundle_id = bundle_id, decomp_step = decomp_step, gbd_round_id = round, filepath = clearSeqPath)



## get the data from the bundle version created above
raw_in <- fread("FILEPATH.csv")
vr <- fread(paste0(j_drive, "FILEPATH", name, "FILEPATH.csv"))

litVr <- rbind(raw_in, vr, fill = T) 
litVr[, seq := NA]

litVr <- unique(litVr)

litVrPath <- paste0("FILEPATH", round, "FILEPATH", bundle_id, "_" ,decomp_step, ".xlsx")
openxlsx::write.xlsx(litVr, litVrPath, sheetName = "extraction", row.names = F)

upload_bundle_data(bundle_id = bundle_id, decomp_step = decomp_step, gbd_round_id = round, filepath = litVrPath)


raw <- get_bundle_data(bundle_id, decomp_step, gbd_round_id = round)


# we'll want to add some modeler notes to track the age/sex splits; not all datasheets have this column so add if it doesn't exist
if (!("note_modeler" %in% names(raw))) {
  raw$note_modeler <- as.character()
}

if (!("group_review" %in% names(raw))) {
  raw$group_review <- as.integer()
}

if (!("group" %in% names(raw))) {
  raw$group <- as.integer()
}

if (!("specificity" %in% names(raw))) {
  raw$specificity <- as.character()
}

setDT(raw)






### SEX SPLIT ------------------------------------------------------------------------------------------

tosplit_sex <- copy(raw)
tosplit_sex <- tosplit_sex[sex=="Both", ]
tosplit_sex[, c("upper", "lower", "cases", "sample_size", "uncertainty_type_value") := NULL]
tosplit_sex_males <- copy(tosplit_sex)
tosplit_sex_females <- copy(tosplit_sex)
tosplit_sex_males[, `:=` (sex = "Male", standard_error = sqrt((standard_error^2)*2))]
tosplit_sex_females[, `:=` (sex = "Female", standard_error = sqrt((standard_error^2)*2))]
split_sex <- rbind(tosplit_sex_males, tosplit_sex_females)
split_sex[standard_error>1, standard_error := 1]


### AGE SPLIT ------------------------------------------------------------------------------------------

age_split_data <- function(dt, row, age_pattern, pops, age_meta) {

  split_row <- copy(dt)
  split_row <- split_row[row,]
  split_row[, refOdds := mean/(1-mean)]
  
  ages <- age_meta$age_group_id[age_meta$age_start<split_row$age_end & age_meta$age_end>split_row$age_start]
  
  
  pops <- pops[location_id==split_row[, location_id] & year_id==split_row[, gbd_year] & sex_id==split_row[, sex_id] & age_group_id %in% ages,]
  age_pattern <- age_pattern[location_id==split_row[, location_id] & year_id==split_row[, gbd_year] & sex_id==split_row[, sex_id] & age_group_id %in% ages,]
  
  age_pattern <- merge(age_pattern, pops, by = c("location_id", "year_id", "sex_id", "age_group_id"), all.x = T)
  age_pattern <- merge(age_pattern, age_meta, by = c("age_group_id"), all.x = T)
  setDT(age_pattern)

  age_pattern <- age_pattern[, .(mean, population, age_start, age_end)]
  age_pattern[, age_cat := floor(age_start/20)*20]
  age_pattern[, `:=` (age_start = min(age_start), age_end = max(age_end)), by = age_cat]
  age_pattern[, mean := weighted.mean(mean, w = population), by = age_cat]
  age_pattern[, population := sum(population), by = age_cat]
  age_pattern <- unique(age_pattern)
  
  
  age_pattern[, odds := mean/(1-mean)]
  age_pattern[, meanOdds := weighted.mean(odds, w = population)]

  age_split_row <- age_pattern[, c("odds", "meanOdds","age_start", "age_end", "population")][, merge := 1]
  split_row[, c("lower", "upper", "cases", "sample_size", "age_start", "age_end") := NULL][, merge := 1]
  split_row <- merge(split_row, age_split_row, by = "merge", all = T)[, merge := NULL]

  split_row[, oddsOfMean := odds * refOdds / meanOdds]
  split_row[, mean := oddsOfMean / (1 + oddsOfMean)]
  split_row[, standard_error := sqrt((split_row[, standard_error]^2)*sum(population)/population)]
  split_row[standard_error>1, standard_error := 1]
  
  split_row <- split_row[, c("odds", "oddsOfMean", "refOdds", "meanOdds", "population") := NULL]

  split_row[, `:=` (note_modeler = paste(note_modeler, "age split off DisMod"))]
  
  return(split_row)
}


predict_sex <- rbind(raw[sex!="Both",], split_sex, fill = T)

predict_sex[, `:=` (age_range = age_end - age_start, year_mid = (year_start + year_end)/2)]
predict_sex[sex=="Male", sex_id := 1][sex=="Female", sex_id := 2][sex=="Both", sex_id := 3]

tosplit_age <- predict_sex[age_range>25 & mean>0 & mean<1,]

age_meta <- get_age_metadata(age_group_set_id=12, gbd_round_id=round)[, age_group_weight_value := NULL]
names(age_meta) <- gsub("group_years_", "", names(age_meta))

# convert year_start and year_end to year_id by determining the closest GBD year (epi) to the study mid-point
demog <- get_demographics("epi")
years <- demog$year_id

tosplit_age[, yearMid := (year_start + year_end)/2 ]

tosplit_age$gbd_year <- sapply(tosplit_age$yearMid, function(x) {tail(years[which(abs(years - x) == min(abs(years - x)))], n=1)})

incMaster <- get_model_results(gbd_team = "epi", gbd_id = incMeid, gbd_round_id = round, location_id = unique(tosplit_age[, location_id]),  
                               year_id = unique(tosplit_age[, gbd_year]),  sex_id = -1, age_group_id = -1, decomp_step = decomp_step)
propMaster <- get_model_results(gbd_team = "epi", gbd_id = agePatternMeid, location_id = unique(tosplit_age[, location_id]),  
                               year_id = unique(tosplit_age[, gbd_year]),  sex_id = -1, age_group_id = -1, decomp_step = decomp_step, gbd_round_id = round)
popMaster <- get_population(location_id = unique(tosplit_age[, location_id]), year_id = unique(tosplit_age[, gbd_year]),  sex_id = 1:2, age_group_id = -1, 
                            decomp_step = decomp_step, gbd_round_id = round)

popMaster <- merge(popMaster, incMaster, by = c("age_group_id", "location_id", "year_id", "sex_id"), all.y = T)
popMaster <- popMaster[, population := population * mean][, c("age_group_id", "location_id", "year_id", "sex_id", "population")]


age_split <- do.call(rbind, lapply(1:nrow(tosplit_age), function(x) {age_split_data(tosplit_age, x, propMaster, popMaster, age_meta)}))
age_split[, crosswalk_parent_seq := seq]



### COMBINED AGE SPLIT AND SEX SPLIT DATASETS------------------------------------------------------------------------------------

to_upload <- rbind(predict_sex[age_range<=25,], age_split, fill = T)




### PRODUCE AND UPLOAD CROSSWALKED DATASET --------------------------------------------------------------------------------------

to_upload[, seq := NA] #1:nrow(to_upload)]
to_upload <- to_upload[(group_review==1 | is.na(group_review)==TRUE),]
to_upload[is.na(lower)==T | is.na(upper)==T, c("uncertainty_type_value", "lower", "upper") := NA]



data_filepath <- paste0("FILEPATH", round, "FILEPATH", bundle_id, ".xlsx")
csv_filepath <- sub("xlsx$", "csv", data_filepath)
write.csv(to_upload, csv_filepath, row.names = F)
openxlsx::write.xlsx(to_upload, data_filepath, sheetName = "extraction")



# save bundle to get bundle version id
result <- save_bundle_version(bundle_id, decomp_step, gbd_round_id = round)
bundle_version_id <- result$bundle_version_id

description <- "DESCRIPTION"

result <- save_crosswalk_version(
  bundle_version_id=bundle_version_id,
  data_filepath=data_filepath,
  description=description)

