##########################################################################
### Project: GBD Nonfatal Estimation - Alcohol Dependence 
### Purpose: Alcohol Dependence Age Splitting GBD 2019
##########################################################################

# Clean up and initialize with the packages we need
rm(list = ls())


library(data.table)
library(xlsx)
library(writex)
library(msm)
library(ggplot2)
library(plyr)
library(parallel)
library(RMySQL)
library(stringr)
library(Hmisc)
library(dummies)
library(magrittr)
library(mortdb, lib = "FILEPATH")

date <- Sys.Date()
date <- gsub("-", "_", Sys.Date())

# GET OBJECTS -------------------------------------------------------------

date <- gsub("-", "_", date)
draws <- paste0("draw_", 0:999)

# GET FUNCTIONS -----------------------------------------------------------

functs <- c("get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids", "save_bundle_version", "save_crosswalk_version", "get_crosswalk_version")
invisible(lapply(functs, function(x) source(paste0("FILEPATH", x, ".R"))))

source("FILEPATH")

model <- 391208 # Dismod model version id    
id <- 1967  
gbd_id <- id

# INPUT POST CROSSWALK DATA FOR AGE SPLITTING------------------------------
dt <- as.data.table(read.xlsx("FILEPATH")) #3320
head(dt)
#setnames(dt, "seq_parent", "crosswalk_parent_seq")

## GET TABLES---------------------------------------------------------------

#Sex
sex_names <- get_ids(table = "sex")

#Age groups
age_dt <- get_age_metadata(12)
setnames(age_dt, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age_dt[, age_group_weight_value := NULL]
age_dt[age_start >= 1, age_end := age_end - 1]
age_dt[age_end == 124, age_end := 99]

age_groups <- age_dt[, age_group_id]

## SAVE ORIGINAL DATA (DOESN'T REQUIRE ALL DATA TO HAVE SEQS)------------
original_dt <- copy(dt)
original_dt[, id := 1:.N]

dt[, id := 1:.N]


## FORMAT DATA THAT WILL BE SPLIT----------------------------------------
dt[, `:=` (mean = as.numeric(mean), sample_size = as.numeric(sample_size), cases = as.numeric(cases),
           age_start = as.numeric(age_start), age_end = as.numeric(age_end), year_start = as.numeric(year_start))]
dt <- dt[is_outlier==0,] ##don't age split outliered data
dt <- dt[measure %in% c("prevalence", "incidence"),] 
#dt <- dt[!group_review==0 | is.na(group_review),] ##don't use group_review 0
dt <- dt[(age_end-age_start)>25,] #slit data age range > 25
dt <- dt[!mean == 0 & !cases == 0, ] ##don't split points with zero prevalence
num_age_split <- nrow(dt) 
message(paste0("You have ", num_age_split, " data points to be age split."))

#Merge data to be split with sex 
dt <- merge(dt, sex_names, by = "sex")
dt[measure == "prevalence", measure_id := 5]
dt[measure == "incidence", measure_id := 6]
dt[, year_id := round((year_start + year_end)/2, 0)] 

dt <- get_cases_sample_size(dt)
dt <- get_se(dt)
dt <- calculate_cases_fromse(dt)


## CREATE NEW AGE ROWS--------------------------------------------------
# Round age groups
dt[, age_start := age_start - age_start %%5]
dt[, age_end := age_end - age_end %%5 + 4]
dt <- dt[age_end > 99, age_end := 99]

# Expand for age   
dt[, n.age:=(age_end+1 - age_start)/5]
dt[, age_start_floor:=age_start]
dt[, drop := cases/n.age] #
dt <- dt[!drop<1,]
expanded <- rep(dt$id, dt$n.age) %>% data.table("id" = .)
split_dt <- merge(expanded, dt, by="id", all=T)
split_dt[, age.rep := 1:.N - 1, by =.(id)]
split_dt[, age_start:= age_start+age.rep*5]
split_dt[, age_end :=  age_start + 4]
split_dt <- merge(split_dt, age_dt, by = c("age_start", "age_end"), all.x = T)
split_dt[age_start == 0 & age_end == 4, age_group_id := 1]


## GET PULL LOCATIONS---------------------------------------------------
locations <- 1 #global pattern 

#GET LOCS AND POPS
pop_locs <- unique(split_dt$location_id)
pop_years <- unique(split_dt$year_id)


## GET AGE PATTERN------------------------------------------------------
print("getting age pattern")
age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = id, 
                         measure_id = c(5, 6), location_id = locations, source = "epi",   
                         sex_id = c(1,2), gbd_round_id = 6, decomp_step = "iterative", 
                         version_id = model, 
                         age_group_id = age_groups, year_id = 2010) #


global_population <- get_population(location_id = locations, year_id = 2010, sex_id = c(1, 2), 
                                age_group_id = age_groups, decomp_step = "step1")
global_population <- global_population[, .(age_group_id, sex_id, population, location_id)]

age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
age_pattern[, (draws) := NULL]
age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]


## AGE GROUP 1 (SUM POPULATION WEIGHTED RATES)--------------------------
age_1 <- copy(age_pattern)
age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5), ]
se <- copy(age_1)
se <- se[age_group_id==5, .(measure_id, sex_id, se_dismod, location_id)] 
age_1 <- merge(age_1, global_population, by = c("age_group_id", "sex_id", "location_id"))
age_1[, total_pop := sum(population), by = c("sex_id", "measure_id", "location_id")]
age_1[, frac_pop := population / total_pop]
age_1[, weight_rate := rate_dis * frac_pop]
age_1[, rate_dis := sum(weight_rate), by = c("sex_id", "measure_id", "location_id")]
age_1 <- unique(age_1, by = c("sex_id", "measure_id", "location_id"))
age_1 <- age_1[, .(age_group_id, sex_id, measure_id, location_id, rate_dis)]
age_1 <- merge(age_1, se, by = c("sex_id", "measure_id", "location_id"))
age_1[, age_group_id := 1]
age_pattern <- age_pattern[!age_group_id %in% c(2,3,4,5)]
age_pattern <- rbind(age_pattern, age_1)


## CASES AND SAMPLE SIZE------------------------------------------------
age_pattern[measure_id == 5, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
age_pattern[measure_id == 6, sample_size_us := rate_dis/se_dismod^2]
age_pattern[, cases_us := sample_size_us * rate_dis]
age_pattern[is.nan(sample_size_us), sample_size_us := 0] ##if all draws are 0 can't calculate cases and sample size b/c se = 0, but should both be 0
age_pattern[is.nan(cases_us), cases_us := 0]


## GET SEX ID 3---------------------------------------------------------
sex_3 <- copy(age_pattern)
sex_3[, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
sex_3[, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
sex_3[, rate_dis := cases_us/sample_size_us]
sex_3[measure_id == 5, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
sex_3[measure_id == 6, se_dismod := sqrt(cases_us)/sample_size_us]
sex_3[is.nan(rate_dis), rate_dis := 0] ##if sample_size is 0 can't calculate rate and standard error, but should both be 0
sex_3[is.nan(se_dismod), se_dismod := 0]
sex_3 <- unique(sex_3, by = c("age_group_id", "measure_id", "location_id"))
sex_3[, sex_id := 3]
age_pattern <- rbind(age_pattern, sex_3)

age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod)]

split_dt <- merge(split_dt, age_pattern, by = c("sex_id", "age_group_id", "measure_id"))


## GET POPULATION STRUCTURE-----------------------------------------------
print("getting pop structure") 
populations <- get_population(location_id = pop_locs, year_id = pop_years, decomp_step = "step1",
                              sex_id = c(1, 2, 3), age_group_id = age_groups)
age_1 <- copy(populations) ##create age group id 1 by collapsing lower age groups
age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5)]
age_1[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
age_1 <- unique(age_1, by = c("location_id", "year_id", "sex_id"))
age_1[, age_group_id := 1]
populations <- populations[!age_group_id %in% c(2, 3, 4, 5)]
populations <- rbind(populations, age_1)  ##add age group id 1 back on

#merge population info 
split_dt <- merge(split_dt, populations, by = c("location_id", "sex_id", "year_id", "age_group_id"))


## CALCULATE AGE SPLIT POINTS-----------------------------------------------
print("splitting data")
split_dt[, total_pop := sum(population), by = "id"]
split_dt[, sample_size := (population / total_pop) * sample_size]
split_dt[, cases_dis := sample_size * rate_dis]
split_dt[, total_cases_dis := sum(cases_dis), by = "id"]
split_dt[, total_sample_size := sum(sample_size), by = "id"]
split_dt[, all_age_rate := total_cases_dis/total_sample_size]
split_dt[, ratio := mean / all_age_rate]
split_dt[, mean := ratio * rate_dis]
split_dt[, cases := mean * sample_size]


## FORMAT DATA TO FINISH----------------------------------------------------
split_dt[, group := 1]
split_dt[, specificity := "age,sex"]
split_dt[, group_review := 1]
split_dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
blank_vars <- c("lower", "upper", "effective_sample_size", "standard_error", "uncertainty_type", "uncertainty_type_value", "seq")
split_dt[, (blank_vars) := NA]
split_dt <- get_se(split_dt)
split_dt[, note_modeler := paste0(note_modeler, "| age split using the global age pattern_", date)]
split_ids <- split_dt[, unique(id)]
split_dt <- rbind(original_dt[!id %in% split_ids], split_dt, fill = T)
split_dt <- split_dt[, c(names(original_dt)), with = F]
split_dt[, id := NULL]


## EXPORT FINAL SEX SPLIT DATA----------------------------------------------- 
writexl::write_xlsx(split_dt, "FILEPATH")