###########################################################
### Date: Jan 9, 2024
### Project: Split TTH into asym/sym
### Purpose: GBD 2022 Nonfatal Estimation
###########################################################


## SET-UP
rm(list=ls())
message(paste0( Sys.time(), ": Beginning"))
pacman::p_load(data.table, ggplot2, readr, dplyr)


## SET OBJECTS
date <- gsub("-", "_", Sys.Date())
draws <- paste0("draw_", 0:999)
headache_dir <- 'FILEPATH'
sev_dir <- 'FILEPATH'


## CREATE DIRECTORIES FOR DATA TO BE SAVED
save_probsym_dir  <- paste0(headache_dir, date, "/probable_sym/")
save_defsym_dir   <- paste0(headache_dir, date, "/definite_sym/")
save_probasym_dir <- paste0(headache_dir, date, "/probable_asym/")
save_defasym_dir  <- paste0(headache_dir, date, "/definite_asym/")


for (file in c(save_probsym_dir, save_defsym_dir, save_probasym_dir, save_defasym_dir)) {
  if (!file.exists(file)) {
    dir.create(file, recursive=T)
    message(paste('Creating output dir', file))
  } else {
    message(paste(file ,'already exists'))
  }
}


## ARGUMENTS
# Enabling trailingOnly disregards initial args, allowing numbered variables to begin from 1 in the script.
args <- commandArgs(trailingOnly = TRUE)
loc <- as.numeric(args[1])
release_id <- args[2]
prob_me <- args[3]
def_me<- args[4]
# prob_me_v<- args[5]
# def_me_v<- args[6]
#loc <- 1 #must be changed before running from master
#release_id <- 16 #must be changed before running from master
#prob_me <- 24385 #must be changed before running from master
#def_me <- 24386 #must be changed before running from master
#prob_me_v<- 798408 #must be changed before running from master
#def_me_v<- 798407 #must be changed before running from master

message(paste0( Sys.time(), ": for location_id ", loc))


## GET IDS
ids <- get_demographics(gbd_team = "epi", release_id=release_id)
years <- ids$year_id
sexes <- ids$sex_id

## GET PREVALENCE DRAWS
message(paste0( Sys.time(), ": getting draws"))

draws_def <- get_draws(gbd_id_type = "modelable_entity_id", 
                       gbd_id = def_me, 
                       source = "epi",
                       measure_id = c(5, 6), 
                       location_id = loc, 
                       year_id = years, 
                       age_group_id = c(6:20,30:32, 235), 
                       sex_id = sexes, 
                       #version_id = def_me_v, 
                       release_id= release_id)
draws_prob <- get_draws(gbd_id_type = "modelable_entity_id", 
                        gbd_id = prob_me, 
                        source = "epi",
                        measure_id = c(5, 6), 
                        location_id = loc, 
                        year_id = years, 
                        age_group_id = c(6:20,30:32, 235), 
                        sex_id = sexes, 
                        #version_id = prob_me_v, 
                        release_id= release_id) 

draws_dt<- rbind(draws_def, draws_prob)
message(paste0( Sys.time(), ": got draws"))

draws_dt[, id := 1]


## READ IN SEVERITY DRAWS FROM RDS PRODUCED BY headache_severity_calc.R
message(paste0( Sys.time(), ": read in freq_dur draws"))

files <- list.files(sev_dir)
dates <- substr(files, 1, 10)
dates <- gsub("_", "-", dates)
last_date <- dates[which.max(as.POSIXct(dates))]
last_date <- gsub("-", "_", last_date)
timesym <- as.data.table(read_rds(paste0(sev_dir, last_date, ".rds")))
timesym[, id := 1]


## CREATE SEPARATE DFs FOR EACH PROB/DEF*AGE GROUP*SEX COMBINATION, AND FORMAT INTO WIDE FORMAT USING dcast()
for (a in 1:3) { # ITERATE THROUGH THE 3 AGE GROUPS
  for (t in c("def", "prob")){
    for (s in c("male", "female")) {
      # CREATE COLUMN NAME
      col_name <- paste0("timesym", t, "_tth", "_", s, "_", a)
      
      # CREATE NAME OF THE DF TO STORE PROCESSED DATA IN THE END
      name <- paste0(t, "_", s, "_", a)
      
      # SELECT COLUMNS OF INTEREST FROM "timesym"
      temp_data <- copy(timesym[, .(variable, id, get(col_name))])
      
      # RESHAPE DATA
      reshape_data <- dcast(temp_data, id ~ variable, value.var = "V3")
      
      # RENAME COLUMNS
      setnames(reshape_data, draws, paste0(t, "_", 0:999))
      
      # ASSIGN RESHAPED DATA INTO ENVIRONMENT WITH DYNAMIC NAME
      assign(name, reshape_data)
    }
  }
}


message(paste0( Sys.time(), ": calculate split draw by draw"))


## MERGE AND CALCULATE POPULATION-LEVEL ESTIMATES FOR EACH HEADACHE TYPE*AGE GROUP*SEX COMBINATION
# YOUNG (1) MALES (males) WITH A DEFINITE (def) DIAGNOSIS
def_draws_male_1 <- merge(draws_dt[modelable_entity_id == def_me & sex_id == 1 & age_group_id %in% c(6:11)], def_male_1, by = "id") # MERGE PREVALENCE DATAFRAME AND pTIS DATAFRAME
def_draws_male_1[, id := NULL]
defsym_draws_male_1 <- copy(def_draws_male_1)
defsym_draws_male_1[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("def_", x)))] # MULTIPLY PREVALENCE DRAWS WITH SYMPTOMATIC STATE ESTIMATE DRAWS
defsym_draws_male_1[, c(paste0("def_", 0:999), "model_version_id", "modelable_entity_id") := NULL]
defasym_draws_male_1 <- copy(def_draws_male_1)
defasym_draws_male_1[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * (1 - get(paste0("def_", x))))] # MULTIPLY PREVALENCE DRAWS WITH ASYMPTOMATIC STATE ESTIMATE DRAWS
defasym_draws_male_1[, c(paste0("def_", 0:999), "model_version_id", "modelable_entity_id") := NULL]

# REPEAT FOR THE OTHER COMBINATIONS
def_draws_female_1 <- merge(draws_dt[modelable_entity_id == def_me & sex_id == 2 & age_group_id %in% c(6:11)], def_female_1, by = "id")
def_draws_female_1[, id := NULL]
defsym_draws_female_1 <- copy(def_draws_female_1)
defsym_draws_female_1[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("def_", x)))]
defsym_draws_female_1[, c(paste0("def_", 0:999), "model_version_id", "modelable_entity_id") := NULL]
defasym_draws_female_1 <- copy(def_draws_female_1)
defasym_draws_female_1[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * (1 - get(paste0("def_", x))))]
defasym_draws_female_1[, c(paste0("def_", 0:999), "model_version_id", "modelable_entity_id") := NULL]

def_draws_male_2 <- merge(draws_dt[modelable_entity_id == def_me & sex_id == 1 & age_group_id %in% c(12:14)], def_male_2, by = "id")
def_draws_male_2[, id := NULL]
defsym_draws_male_2 <- copy(def_draws_male_2)
defsym_draws_male_2[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("def_", x)))]
defsym_draws_male_2[, c(paste0("def_", 0:999), "model_version_id", "modelable_entity_id") := NULL]
defasym_draws_male_2 <- copy(def_draws_male_2)
defasym_draws_male_2[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * (1 - get(paste0("def_", x))))]
defasym_draws_male_2[, c(paste0("def_", 0:999), "model_version_id", "modelable_entity_id") := NULL]

def_draws_female_2 <- merge(draws_dt[modelable_entity_id == def_me & sex_id == 2 & age_group_id %in% c(12:14)], def_female_2, by = "id")
def_draws_female_2[, id := NULL]
defsym_draws_female_2 <- copy(def_draws_female_2)
defsym_draws_female_2[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("def_", x)))]
defsym_draws_female_2[, c(paste0("def_", 0:999), "model_version_id", "modelable_entity_id") := NULL]
defasym_draws_female_2 <- copy(def_draws_female_2)
defasym_draws_female_2[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * (1 - get(paste0("def_", x))))]
defasym_draws_female_2[, c(paste0("def_", 0:999), "model_version_id", "modelable_entity_id") := NULL]

def_draws_male_3 <- merge(draws_dt[modelable_entity_id == def_me & sex_id == 1 & age_group_id %in% c(15:20,30:32,235)], def_male_3, by = "id")
def_draws_male_3[, id := NULL]
defsym_draws_male_3 <- copy(def_draws_male_3)
defsym_draws_male_3[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("def_", x)))]
defsym_draws_male_3[, c(paste0("def_", 0:999), "model_version_id", "modelable_entity_id") := NULL]
defasym_draws_male_3 <- copy(def_draws_male_3)
defasym_draws_male_3[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * (1 - get(paste0("def_", x))))]
defasym_draws_male_3[, c(paste0("def_", 0:999), "model_version_id", "modelable_entity_id") := NULL]

def_draws_female_3 <- merge(draws_dt[modelable_entity_id == def_me & sex_id == 2 & age_group_id %in% c(15:20,30:32,235)], def_female_3, by = "id")
def_draws_female_3[, id := NULL]
defsym_draws_female_3 <- copy(def_draws_female_3)
defsym_draws_female_3[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("def_", x)))]
defsym_draws_female_3[, c(paste0("def_", 0:999), "model_version_id", "modelable_entity_id") := NULL]
defasym_draws_female_3 <- copy(def_draws_female_3)
defasym_draws_female_3[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * (1 - get(paste0("def_", x))))]
defasym_draws_female_3[, c(paste0("def_", 0:999), "model_version_id", "modelable_entity_id") := NULL]

prob_draws_male_1 <- merge(draws_dt[modelable_entity_id == prob_me & sex_id == 1 & age_group_id %in% c(6:11)], prob_male_1, by = "id")
prob_draws_male_1[, id := NULL]
probsym_draws_male_1 <- copy(prob_draws_male_1)
probsym_draws_male_1[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("prob_", x)))]
probsym_draws_male_1[, c(paste0("prob_", 0:999), "model_version_id", "modelable_entity_id") := NULL]
probasym_draws_male_1 <- copy(prob_draws_male_1)
probasym_draws_male_1[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * (1 - get(paste0("prob_", x))))]
probasym_draws_male_1[, c(paste0("prob_", 0:999), "model_version_id", "modelable_entity_id") := NULL]

prob_draws_female_1 <- merge(draws_dt[modelable_entity_id == prob_me & sex_id == 2 & age_group_id %in% c(6:11)], prob_female_1, by = "id")
prob_draws_female_1[, id := NULL]
probsym_draws_female_1 <- copy(prob_draws_female_1)
probsym_draws_female_1[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("prob_", x)))]
probsym_draws_female_1[, c(paste0("prob_", 0:999), "model_version_id", "modelable_entity_id") := NULL]
probasym_draws_female_1 <- copy(prob_draws_female_1)
probasym_draws_female_1[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * (1 - get(paste0("prob_", x))))]
probasym_draws_female_1[, c(paste0("prob_", 0:999), "model_version_id", "modelable_entity_id") := NULL]

prob_draws_male_2 <- merge(draws_dt[modelable_entity_id == prob_me & sex_id == 1 & age_group_id %in% c(12:14)], prob_male_2, by = "id")
prob_draws_male_2[, id := NULL]
probsym_draws_male_2 <- copy(prob_draws_male_2)
probsym_draws_male_2[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("prob_", x)))]
probsym_draws_male_2[, c(paste0("prob_", 0:999), "model_version_id", "modelable_entity_id") := NULL]
probasym_draws_male_2 <- copy(prob_draws_male_2)
probasym_draws_male_2[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * (1 - get(paste0("prob_", x))))]
probasym_draws_male_2[, c(paste0("prob_", 0:999), "model_version_id", "modelable_entity_id") := NULL]

prob_draws_female_2 <- merge(draws_dt[modelable_entity_id == prob_me & sex_id == 2 & age_group_id %in% c(12:14)], prob_female_2, by = "id")
prob_draws_female_2[, id := NULL]
probsym_draws_female_2 <- copy(prob_draws_female_2)
probsym_draws_female_2[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("prob_", x)))]
probsym_draws_female_2[, c(paste0("prob_", 0:999), "model_version_id", "modelable_entity_id") := NULL]
probasym_draws_female_2 <- copy(prob_draws_female_2)
probasym_draws_female_2[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * (1 - get(paste0("prob_", x))))]
probasym_draws_female_2[, c(paste0("prob_", 0:999), "model_version_id", "modelable_entity_id") := NULL]

prob_draws_male_3 <- merge(draws_dt[modelable_entity_id == prob_me & sex_id == 1 & age_group_id %in% c(15:20,30:32,235)], prob_male_3, by = "id")
prob_draws_male_3[, id := NULL]
probsym_draws_male_3 <- copy(prob_draws_male_3)
probsym_draws_male_3[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("prob_", x)))]
probsym_draws_male_3[, c(paste0("prob_", 0:999), "model_version_id", "modelable_entity_id") := NULL]
probasym_draws_male_3 <- copy(prob_draws_male_3)
probasym_draws_male_3[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * (1 - get(paste0("prob_", x))))]
probasym_draws_male_3[, c(paste0("prob_", 0:999), "model_version_id", "modelable_entity_id") := NULL]

prob_draws_female_3 <- merge(draws_dt[modelable_entity_id == prob_me & sex_id == 2 & age_group_id %in% c(15:20,30:32,235)], prob_female_3, by = "id")
prob_draws_female_3[, id := NULL]
probsym_draws_female_3 <- copy(prob_draws_female_3)
probsym_draws_female_3[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("prob_", x)))]
probsym_draws_female_3[, c(paste0("prob_", 0:999), "model_version_id", "modelable_entity_id") := NULL]
probasym_draws_female_3 <- copy(prob_draws_female_3)
probasym_draws_female_3[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * (1 - get(paste0("prob_", x))))]
probasym_draws_female_3[, c(paste0("prob_", 0:999), "model_version_id", "modelable_entity_id") := NULL]


## MERGE MALE AND FEMALE DFs
defsym_draws <- rbind(defsym_draws_male_1, defsym_draws_female_1, defsym_draws_male_2, defsym_draws_female_2, defsym_draws_male_3, defsym_draws_female_3)
defasym_draws <- rbind(defasym_draws_male_1, defasym_draws_female_1, defasym_draws_male_2, defasym_draws_female_2, defasym_draws_male_3, defasym_draws_female_3)
probsym_draws <- rbind(probsym_draws_male_1, probsym_draws_female_1, probsym_draws_male_2, probsym_draws_female_2, probsym_draws_male_3, probsym_draws_female_3)
probasym_draws <- rbind(probasym_draws_male_1, probasym_draws_female_1, probasym_draws_male_2, probasym_draws_female_2, probasym_draws_male_3, probasym_draws_female_3)

message(paste0( Sys.time(), ": save files"))


## SAVE FILES
write.csv(defsym_draws, paste0(save_defsym_dir, loc, ".csv"), row.names = F)
write.csv(defasym_draws, paste0(save_defasym_dir, loc, ".csv"), row.names = F)
write.csv(probsym_draws, paste0(save_probsym_dir, loc, ".csv"), row.names = F)
write.csv(probasym_draws, paste0(save_probasym_dir, loc, ".csv"), row.names = F)

message(paste0( Sys.time(), ": complete"))