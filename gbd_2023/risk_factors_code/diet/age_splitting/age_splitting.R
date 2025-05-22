################################################################################
## DESCRIPTION: Apply age pattern to split diet data, primarily FAO data 
## INPUTS: DISMOD or MRBRT age pattern and current bundle data to be AS
## OUTPUTS: Age split data for each diet risk, ready for the crosswalk
## AUTHOR: 
## DATE CREATED:
################################################################################

rm(list=ls())

## Config ----------------------------------------------------------------------#

# System info & drives
os <- Sys.info()[1]
user <- Sys.info()[7]
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "H:/"
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""


#Load libraries 
pacman::p_load(data.table, openxlsx, ggplot2, reshape2, dplyr)
library(msm)
library(readxl)
library(reshape2)
library(purrr)
library(tidyr)
library(tidyverse)


'%ni%' <- Negate("%in%")
date <- gsub("-", "_", Sys.Date())


source("FILEPATH/01_age_sex_splitting/get_age_map.R")
source("FILEPATH/01_age_sex_splitting/expand_data.R")
source("FILEPATH//get_population.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_bundle_version.R")


# path 
gbd_round <- "gbd2023"

save <- TRUE
loc_set <- 35
release_id <- 16
age_group_set_id = 24
age_group_id <- c(2:32, 34, 235, 238, 388, 389)

version <- 1 #version corresponding to age patterns. used in input_dir
dismod_ap = TRUE #using old DISMOD age pattern or not. 

args <- commandArgs(trailingOnly = TRUE)

# to run through a job (remove outer loop over "to_save")--------------------#
if (!interactive()){
  task_map_filepath <- args[1]
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  print(paste("This is task", task_id))
}


# to run interactively -------------------------------------------------------#
if (interactive()){
task_map_filepath <- "FILEPATH/diet_ids.csv"
task_id <- 1
to_save <- c(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15)
#to_save <-c(1)
}

for (i in to_save){
  task_id <- i
  print(paste("This is task", task_id))


ids <- fread(task_map_filepath)
rei_id <- ids[ task_id,rei_id]
print(rei_id)
ihme_risk <- ids[task_id, me_name]
if(ihme_risk == "diet_grains"){ihme_risk = "diet_whole_grains"}
print(ihme_risk)
bundle_version <- ids[task_id, bv_id]
print(bundle_version)


save_dir = "FILEPATH/Diet_RF_age_splitted.CSV"


# load data 

# pattern data ------------------------------------------------------------

if (dismod_ap){
  all_ap = readstata13::read.dta13("/FILEPATH/as_data_trend.dta")
  setDT(all_ap)
  age_pattern = all_ap[risk == ihme_risk,]
  
  message(paste0("Age pattern from DISMOD has been read in for ", ihme_risk))
  
}else{
  age_pattern <- fread(paste0("FILEPATH/output_v", version, "/", ihme_risk, "_mrbrt_age_pattern_no_draws.csv"))
  message(paste0("Age pattern from MRBRT has been read in for ", ihme_risk)) 
}


#reshaping age_pattern into the correct age groups that correspond to latest release_id

age_map <- get_age_map(release_id = release_id, age_group_set_id = age_group_set_id)


#reshaping age_pattern into the correct age groups that correspond to latest release_id

if(dismod_ap){
age_pattern[, mean := rowMeans(.SD), .SDcols = paste0("pred_", 0:999)  ]
age_pattern[, se := apply(.SD,1, sd), .SDcols = paste0("pred_", 0:999)]
age_pattern[, c( paste0("pred_", 0:999)) := NULL]


risk_ap_over80 <- age_pattern %>% filter(age_lower >=80 )

risk_ap_over80 <- age_map %>% filter(age_group_years_start >= 80) %>%
  dplyr::rename(age_lower = age_group_years_start,
         age_upper = age_group_years_end) %>% 
  dplyr::mutate(risk = ihme_risk,
         mean = risk_ap_over80$mean,
         se = risk_ap_over80$se)


age_pattern <- rbind(age_pattern %>% filter(age_group_id != 30), risk_ap_over80)

## risk lower than under five children

risk_ap_under5 =  age_pattern %>% filter(age_lower %in% c(1, 5))
risk_ap_under5 <- age_map %>% filter(age_group_years_start %in% c(1, 2)) %>%
  dplyr::rename(age_lower = age_group_years_start,
         age_upper = age_group_years_end) %>% 
  dplyr::mutate(risk = ihme_risk,
         mean = risk_ap_under5$mean,
         se = risk_ap_under5$se)


age_pattern <- rbind(age_pattern %>% filter(age_group_id !=5), risk_ap_under5) 

### early neonatal

risk_ap_postnatal =  age_pattern %>% filter(age_group_id %in% c(4))

risk_ap_postnatal = age_map %>% filter(age_group_id %in% c(388, 389)) %>%
  dplyr::rename(age_lower = age_group_years_start,
         age_upper = age_group_years_end) %>% 
  dplyr::mutate(risk = ihme_risk,
         mean = risk_ap_postnatal$mean,
         se = risk_ap_postnatal$se)

age_pattern <- rbind(age_pattern %>% filter(age_group_id !=4), risk_ap_postnatal) 

}

names(age_pattern)

age_pattern <- age_pattern %>% merge(age_map)
age_pattern_sex<- rbind(age_pattern%>% mutate(sex_id=1),
                    age_pattern%>% mutate(sex_id=2),
                    age_pattern%>% mutate(sex_id=3)) %>%
  select(-c(age_lower, age_upper))


# bundle data -------------------------------------------------------------

# Standard
data <- get_bundle_version(bundle_version)
data <- data %>% mutate(sex_id = case_when(sex == "Male" ~ 1, 
                                            sex == "Female" ~ 2,
                                            TRUE ~ 3))

data[, location_id := ifelse(location_id == 4940, 93, location_id)] #sweden except stockholm recode to sweden
data[, location_id := ifelse(location_id == 4944, 93, location_id)] #sweden except stockholm recode to sweden
data[, location_id := ifelse(location_id == 44723, 4749, location_id)] #Norfolk recoded to parent England
data <- data[!(location_id %in% c(43871, 43876, 43878, 43897, 43907, 43914, 43933)), ]

data <- data %>% mutate(age_end = age_end + 1) %>%
  select(!age_group_id) %>%
  merge(age_map, by.x =c("age_start", "age_end"), by.y = c("age_group_years_start", "age_group_years_end"), all.x = TRUE)
data <- data %>% mutate(age_group_id = ifelse(is.na(age_group_id),22, age_group_id))
data_no_as <- data[age_group_id != 22]
data_no_as$was_age_split = 0

data <- data[age_group_id == 22]
unique(data.frame(data$age_start, data$age_end, data$age_group_id))

popMeta <- get_population(age_group_id = unique(age_map$age_group_id), year_id = (min(data$year_id):max(data$year_id)), 
                          sex_id = 1:3, with_ui=FALSE, release_id=16, location_id = unique(data$location_id)) 
popMeta <- popMeta %>% rename(pop_population = population)


# py.dissag ---------------------------------------------------------------

library(reticulate)
reticulate::use_python("FILEPATh/python")
splitter <- import("pydisagg.ihme.splitter")

data_config <- splitter$AgeDataConfig(
  index=c("seq", "location_id", "year_id", "sex_id"),
  age_lwr="age_start",
  age_upr="age_end",
  val="val",
  val_sd="standard_error"
)

pattern_config <- splitter$AgePatternConfig(
  by=list("sex_id"),
  age_key="age_group_id",
  age_lwr="age_group_years_start",
  age_upr="age_group_years_end",
  val="mean",
  val_sd="se"
)

pop_config <- splitter$AgePopulationConfig(
  index=c("age_group_id", "location_id", "year_id", "sex_id"),
  val="pop_population"
)

age_splitter <- splitter$AgeSplitter(
  data=data_config, pattern=pattern_config, population=pop_config
)

result <- age_splitter$split(
  data=data,
  pattern = age_pattern_sex,
  population = popMeta,
  model="rate",
  output_type="rate"
)

age_split_data <- result %>%
  dplyr::select(all_of(c("seq", "location_id", "year_id", "sex_id", "age_group_id", "age_split_result", "age_split_result_se")))

age_split_data <- age_split_data %>%
  merge(data %>% select(-c("location_id", "year_id", "sex_id", "age_group_id", "val", "standard_error", "age_start", "age_end")), by = "seq")

age_split_data <- age_split_data %>%
  rename(standard_error = age_split_result_se,
         val = age_split_result) %>%
  mutate(upper = val + 1.96 * standard_error,
         lower = val - 1.96 *standard_error,
         variance = standard_error ^2,
         sex = case_when(sex_id == 3 ~ "Both",
                         sex_id == 1 ~ "Male",
                         TRUE ~ "Female"),
         measure = "continuous",
         was_age_split = 1) %>%
  merge(age_map)%>% 
  rename(age_start = age_group_years_start,
         age_end = age_group_years_end) %>%
  select(all_of(c(names(data), "was_age_split"))) 
dt <- rbind(age_split_data, data_no_as)

if(save){
  write.csv(dt, paste0(save_dir, ihme_risk, ".csv"))
  message(paste0("Data has been age split for ", ihme_risk, " and saved to: ", save_dir))
}else{
  message(paste0("Data was NOT saved for ", ihme_risk))
}


}

