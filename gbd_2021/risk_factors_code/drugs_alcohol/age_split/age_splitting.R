##########################################################################
### Purpose: Alcohol Use Age Splitting GBD 2019
##########################################################################

# Clean up and initialize with the packages we need
rm(list = ls())

library(data.table)
library(openxlsx)
library(msm, lib.loc = 'FILEPATH')
library(ggplot2)
library(plyr)
library(parallel)
library(RMySQL)
library(stringr)
library(Hmisc, lib.loc = 'FILEPATH')
library(dummies, lib.loc = 'FILEPATH')
library(magrittr)
library(mortdb, lib = 'FILEPATH')

date <- Sys.Date()

# GET OBJECTS -------------------------------------------------------------

repo_dir <- 'FILEPATH'
functions_dir <- 'FILEPATH'
date <- gsub("-", "_", date)
draws <- paste0("draw_", 0:99)

# GET FUNCTIONS -----------------------------------------------------------

functs <- c("get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids", "save_bundle_version", "save_crosswalk_version", "get_crosswalk_version")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))

source('FILEPATH')
locs <- get_location_metadata(22)
ages <- c(2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,  17,  18,  19,  20,  30,  31,  32, 235)
pops <- get_population(age_group_id = ages,
                       sex_id = c(1,2),
                       location_id = unique(locs$location_id),
                       year_id = 1990:2019,
                       gbd_round_id = 6,
                       decomp_step = "step3"
)

# INPUT POST CROSSWALK DATA FOR DISMOD RUN---------------------------------
# message(paste0("Run ST GPR"))
topic <- "current_drink"    

if (topic == "gday"){
  id <- 3360   
  model <- 397286 # Dismod model version id

  } else if (topic == "current_drink"){
  
  id <- 3364
  model <- 397409
  run_id <- 87353
  
} 


# INPUT POST CROSSWALK DATA FOR AGE SPLITTING------------------------------

if (topic == "gday"){
  dt <- fread('FILEPATH')
}

if (topic == "current_drink"){
  
  dt <- read.xlsx('FILEPATH')
  dt <- as.data.table(dt)
}


## EXPLORING 

ggplot(dt, aes(year_id)) + geom_bar() +
  scale_x_continuous("Year")


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

# Regions 
region_dt <- get_location_metadata(location_set_id = 22)
region_dt <- region_dt[, .(location_id, region_id)]

## SAVE ORIGINAL DATA (DOESN'T REQUIRE ALL DATA TO HAVE SEQS)------------
original_dt <- copy(dt)
original_dt[, id := 1:.N]

dt[, id := 1:.N]

## FORMAT DATA THAT WILL BE SPLIT----------------------------------------
dt[, `:=` (mean = as.numeric(val), sample_size = as.numeric(sample_size), cases = as.numeric(cases),
           age_start = as.numeric(age_start), age_end = as.numeric(age_end), year_start = as.numeric(year_id))]

dt <- dt[is_outlier==0,] ##don't age split outliered data

if (topic == "gday"){
  dt <- dt[measure %in% c("continuous"),] 
}

if (topic == "current_drink"){
  dt <- dt[measure %in% c("proportion"),] 
}

dt <- dt[age_group_id == 999,] #this is how we coded nonstandard groups for stgpr
dt$age_group_id <- NULL
num_age_split <- nrow(dt)
message(paste0("You have ", num_age_split, " data points to be age split."))

#Merge data to be split with sex 
dt <- merge(dt, sex_names, by = "sex")
dt[measure == "prevalence", measure_id := 5]
dt[measure == "incidence", measure_id := 6]
dt[measure == "proportion", measure_id := 18]
dt[measure == "continuous", measure_id := 19]

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

expanded <- rep(dt$id, dt$n.age) %>% data.table("id" = .)
split_dt <- merge(expanded, dt, by="id", all=T)
split_dt[, age.rep := 1:.N - 1, by =.(id)]
split_dt[, age_start:= age_start+age.rep*5]
split_dt[, age_end :=  age_start + 4]
split_dt <- merge(split_dt, age_dt, by = c("age_start", "age_end"), all.x = T)
split_dt[age_start == 0 & age_end == 4, age_group_id := 1]


## GET PULL LOCATIONS---------------------------------------------------
split_dt <- merge(split_dt, region_dt, by = "location_id")
locations <- unique(split_dt$region_id) 

#GET LOCS AND POPS
pop_locs <- unique(split_dt$location_id)
pop_years <- unique(split_dt$year_id)


## GET AGE PATTERN------------------------------------------------------
print("getting age pattern")

if (topic == "gday"){
  age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = id, 
                           measure_id = c(19), location_id = locations, source = "epi", #Locations = global  
                           sex_id = c(1,2), gbd_round_id = 6, decomp_step = "iterative", # status = "best", 
                           version_id = model, 
                           age_group_id = age_groups, year_id = 2010) ##imposing age pattern. YEAR_ID =2010 because most of the data is from 2010
}


if (topic == "current_drink"){
 
  path <- 'FILEPATH'
  list <- list.files(path)
  
  age_pattern <- NULL
  for (i in 1:length(list)){
    print(i)
    df <- fread(paste0(path,list[i]))
    df <- df[year_id == 2010]
    age_pattern <- rbind(age_pattern, df)
  }
  
  age_pattern$measure_id <- 18

  message("Aggregating ST-GPR draws to create regional age patterns")
  age_pattern <- merge(age_pattern,locs[,.(location_id,region_id)],by="location_id")
  age_pattern <- merge(age_pattern,pops,by=c("location_id","sex_id","age_group_id","year_id"))
  
  new_drawvars <- paste0("draw_smk_",0:99)
  # get draws of the number of drinkers in each age, location, year, and sex
  age_pattern[,(new_drawvars) := lapply(.SD, function(x) x*population), .SDcols=draws, by=c("location_id", "year_id", "age_group_id", "sex_id")]
  # aggregate these draws by region
  # now we have number of drinkers in each region, year, location, age
  age_pattern[,(new_drawvars) := lapply(.SD, function(x) sum(x)), .SDcols=new_drawvars, by=c("region_id", "year_id", "age_group_id", "sex_id")]
  # sum population by region
  age_pattern[,region_pop := sum(population),by=c("region_id", "year_id", "age_group_id", "sex_id")]
  # then divide to get new prevalence draws for each age, sex, year, and region:
  age_pattern[,(draws) := lapply(.SD, function(x) x/region_pop), .SDcols=new_drawvars, by=c("region_id", "year_id", "age_group_id", "sex_id")]
  # then clean up the draws:
  age_pattern[,(new_drawvars) := NULL]
  null_out <- c("region_id","population","run_id","age_start","age_end","age_group","region_pop")
  age_pattern[,(null_out) := NULL]
  message("Regional draws finished!")
  
  unique(age_pattern$location_id)
  head(age_pattern)
  
  age_pattern <- merge(age_pattern, locs[,c("location_id", "region_id")], by = "location_id")
  age_pattern$location_id <- NULL
  age_pattern <- unique(age_pattern)

  age_pattern <- age_pattern[region_id %in% locations]
  age_pattern <- setnames(age_pattern, old = "region_id", new = "location_id")
  
  }

# depending on which model, this may be global or regional
global_population <- get_population(location_id = locations, year_id = 2010, sex_id = c(1, 2), 
                                    age_group_id = age_groups, decomp_step = "step1")
global_population <- global_population[, .(age_group_id, sex_id, population, location_id)]

age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
age_pattern[, (draws) := NULL]
age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]
head(age_pattern)

backup <- copy(age_pattern)

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

backup <- copy(age_pattern)


## CASES AND SAMPLE SIZE------------------------------------------------
age_pattern[measure_id == 5 | measure_id == 18, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
age_pattern[measure_id == 6, sample_size_us := rate_dis/se_dismod^2]
age_pattern[, cases_us := sample_size_us * rate_dis]
age_pattern[is.nan(sample_size_us), sample_size_us := 0] ##if all draws are 0 can't calculate cases and sample size b/c se = 0, but should both be 0
age_pattern[is.nan(cases_us), cases_us := 0]

backup <- copy(age_pattern)

## GET SEX ID 3---------------------------------------------------------
sex_3 <- copy(age_pattern)
sex_3[, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
sex_3[, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
sex_3[, rate_dis := cases_us/sample_size_us]
sex_3[measure_id == 5 | measure_id == 18, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
sex_3[measure_id == 6, se_dismod := sqrt(cases_us)/sample_size_us]
sex_3[is.nan(rate_dis), rate_dis := 0] ##if sample_size is 0 can't calculate rate and standard error, but should both be 0
sex_3[is.nan(se_dismod), se_dismod := 0]
sex_3 <- unique(sex_3, by = c("age_group_id", "measure_id", "location_id"))
sex_3[, sex_id := 3]
age_pattern <- rbind(age_pattern, sex_3)

age_pattern[, region_id := location_id]
age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod, region_id)]


split_dt <- split_dt[sex == "Male", sex_id := 1]
split_dt <- split_dt[sex == "Female", sex_id := 2]
split_dt <- split_dt[sex == "Both", sex_id := 3]

split_dt <- merge(split_dt, age_pattern, by = c("sex_id", "age_group_id", "measure_id", "region_id"))


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
split_dt[measure == "continuous", total_consump := sum(rate_dis), by = "id"]

backup <- copy(split_dt)
split_dt <- copy(backup)
split_dt[measure != "continuous", sample_size := (population / total_pop) * sample_size]
split_dt[measure != "continuous", cases_dis := sample_size * rate_dis]

split_dt[measure == "continuous", sample_size := ((rate_dis)/ (total_consump)) * sample_size]
split_dt[measure == "continuous", cases_dis := ((rate_dis)/ (total_consump)) * cases]


split_dt[, total_cases_dis := sum(cases_dis), by = "id"]
split_dt[, total_sample_size := sum(sample_size), by = "id"]
split_dt[, all_age_rate := total_cases_dis/total_sample_size]
split_dt[, ratio := mean / all_age_rate]
split_dt[, global_mean := mean(rate_dis), by = "id"]


split_dt[measure != "continuous", mean := ratio * rate_dis]
split_dt[measure != "continuous", cases := mean * sample_size]

split_dt[measure == "continuous", mean := rate_dis*mean/global_mean]

split_dt[measure == "continuous", cases := cases_dis]

## FORMAT DATA TO FINISH----------------------------------------------------
split_dt[, group := 1]
split_dt[, specificity := "age,sex"]
split_dt[, group_review := 1]
split_dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
blank_vars <- c("lower", "upper", "effective_sample_size", "standard_error", "uncertainty_type", "uncertainty_type_value", "seq")
split_dt[, (blank_vars) := NA]
split_dt <- get_se(split_dt)
split_dt[, note_modeler := paste0(note_modeler, "| age split using the regional age pattern_", date)]
split_ids <- split_dt[, unique(id)]
split_dt$age_split <- 1
original_dt$age_split <- 0
split_dt <- rbind(original_dt[!id %in% split_ids], split_dt, fill = T)

split_dt <- split_dt[, c(names(original_dt)), with = F]
split_dt[, id := NULL]


## EXPORT FINAL SEX SPLIT DATA----------------------------------------------- 

write.csv(split_dt, 'FILEPATH')
