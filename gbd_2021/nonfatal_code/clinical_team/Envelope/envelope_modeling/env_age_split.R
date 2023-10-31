####################################################################################################
## ENV_AGE_SPLIT.R
## Age split inpatient utilization envelope data after crosswalking. Relies on age-pattern model.
####################################################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
}

## -----------------------------------------------------------------------------
## SETUP
pacman::p_load(data.table, openxlsx, ggplot2, magrittr, Hmisc, dplyr) # load libraries
functions_dir <- "FILEPATH" # source shared functions from here
draws <- paste0("draw_", 0:999) # set draws

## Specifics of age-pattern model (DisMod)
b_id <- 7889 # bundle_id
id <- 25219 # me_id
ver_id <- 544352 # DisMod version (model id)
measure_dismod <- 19 # continuous
round_id = 7 # GBD 2020 
step <- "iterative" 

## Set pattern you want to use for age-splitting
region_pattern <- F

## For shared functions
round_id_functions <- 7 
step_id_functions <- "iterative" 

## Read in data (post-crosswalk)
dt <- as.data.table(read.csv("FILEPATH"))

## Assorted cleanup/prep
dt$X.1 <- NULL
dt$standard_error <- dt$se_final
dt$mean <- dt$mean_final
dt$X <- NULL
dt$V1 <- NULL
dt$index <- NULL
dt$start_age_group_id <- dt$age_group_id
dt$age_group_id <- NULL


## -----------------------------------------------------------------------------
## FUNCTIONS NEEDED - SHARED AND USER-DEFINED/CUSTOM
## Get shared functions
functs <- c("get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))


## Calculate mean, cases, sample_size (if missing)
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

## Calculate standard error (if missing)
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  dt[is.na(standard_error) & measure == "continuous" & cases < 5, standard_error := (((5-mean*sample_size)/sample_size)+((mean*sample_size*(5/sample_size^2))/5))]
  dt[is.na(standard_error) & measure == "continuous" & cases >= 5, standard_error := sqrt(cases)/sample_size]
  return(dt)
}

## Calculate cases from standard error (if still missing)
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt$sample_size <- as.numeric(dt$sample_size)
  dt[is.na(cases) & is.na(sample_size) & measure == "continuous", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

## Format data - preps dataset to age-split based on whether age_group_id is present
format_data <- function(unformatted_dt, sex_dt){
  dt <- copy(unformatted_dt)
  dt[, `:=` (mean = as.numeric(mean), sample_size = as.numeric(sample_size), cases = as.numeric(cases),
             age_start = as.numeric(age_start), age_end = as.numeric(age_end), year_start = as.numeric(year_start))]
  dt <- dt[measure %in% c("continuous"),] 
  dt <- dt[is_outlier==0,] 
  dt <- dt[is.na(dt$start_age_group_id),] 
  dt <- dt[!mean == 0 & !cases == 0, ] 
  dt <- merge(dt, sex_dt, by = "sex")
  dt[measure == "continuous", measure_id := 19] 
  dt[, year_id := round((year_start + year_end)/2, 0)] 
  return(dt)
}

## Create new rows as needed based on current age_start, age_end for age-splitting
expand_age <- function(small_dt, age_dt = ages){
  dt <- copy(small_dt)
  dt <- dt[age_end > 99, age_end := 99] 

  ## This chunk for age_start = 0, age_end = 1
  dt01 <- subset(dt, (age_start == 0 & age_end == 1))
  dt01[, n.age := 4] 
  dt01[, age_start_floor:=age_start]
  dt01[, drop := cases/n.age] # drop the data points if cases/n.age is less than 1 
  dt01 <- dt01[!drop<1,] # comment this out if you want to retain all age splits regardless of small case number
  expanded01 <- rep(dt01$id, dt01$n.age) %>% data.table("id" = .)
  split01 <- merge(expanded01, dt01, by="id", all=T)
  split01[, age.rep := 1:.N - 1, by =.(id)]
  split01 <- split01 %>% 
    mutate(
      age_start = case_when(
        age.rep == 0 ~ 0,
        age.rep == 1 ~ 0.01917808,
        age.rep == 2 ~ 0.07671233,
        age.rep == 3 ~ 0.50136986,
      )
    ) 
  
  split01 <- split01 %>% 
    mutate(
      age_end = case_when(
        age.rep == 0 ~ 0.01917808,
        age.rep == 1 ~ 0.07671233,
        age.rep == 2 ~ 0.5,
        age.rep == 3 ~ 1,
      )
    )
  
  ## This chunk for age_start = 0, age_end >= 2
  dt0 <- subset(dt, (age_start == 0 & age_end >= 2))
  dt0[, n.age:=((age_end+1 - age_start)/5)+5] 
  dt0[, age_start_floor:=age_start]
  dt0[, drop := cases/n.age] # drop the data points if cases/n.age is less than 1 
  dt0 <- dt0[!drop<1,] # comment this out if you want to retain all age splits regardless of small case number
  expanded0 <- rep(dt0$id, dt0$n.age) %>% data.table("id" = .)
  split0 <- merge(expanded0, dt0, by="id", all=T)
  split0[, age.rep := 1:.N - 1, by =.(id)]
  split0 <- split0 %>% 
    mutate(
      age_start = case_when(
        age.rep == 0 ~ 0,
        age.rep == 1 ~ 0.01917808,
        age.rep == 2 ~ 0.07671233,
        age.rep == 3 ~ 0.5,
        age.rep == 4 ~ 1,
        age.rep == 5 ~ 2,
        TRUE         ~ age_start+(age.rep-5)*5
      )
    ) 
  
  split0 <- split0 %>% 
    mutate(
      age_end = case_when(
        age.rep == 0 ~ 0.01917808,
        age.rep == 1 ~ 0.07671233,
        age.rep == 2 ~ 0.5,
        age.rep == 3 ~ 1,
        age.rep == 4 ~ 2,
        age.rep == 5 ~ 4,
        TRUE         ~ age_start+4
      )
    )
  
  ## This chunk for age_start = 1
  dt1 <- subset(dt, age_start == 1)
  dt1[, n.age:=((age_end+1 - age_start)/5)+1] 
  dt1[, age_start_floor:=age_start]
  dt1[, drop := cases/n.age] # drop the data points if cases/n.age is less than 1 
  dt1 <- dt1[!drop<1,] # comment this out if you want to retain all age splits regardless of small case number
  expanded1 <- rep(dt1$id, dt1$n.age) %>% data.table("id" = .)
  split1 <- merge(expanded1, dt1, by="id", all=T)
  split1[, age.rep := 1:.N - 1, by =.(id)]
  split1 <- split1 %>% 
    mutate(
      age_start = case_when(
        age.rep == 0 ~ 1,
        age.rep == 1 ~ 2,
        TRUE         ~ (age.rep-1)*5
      )
    ) 
  
  split1 <- split1 %>% 
    mutate(
      age_end = case_when(
        age.rep == 0 ~ 2,
        age.rep == 1 ~ 4,
        TRUE         ~ age_start+4
      )
    )
  
  ## This chunk for age_start = 2, 3, or 4
  dt2 <- dt[dt$age_start %in% c(2,3,4)]
  dt2[, n.age:=((age_end+1 - age_start)/5)+1] 
  dt2[, age_start_floor:=age_start]
  dt2[, drop := cases/n.age] # drop the data points if cases/n.age is less than 1 
  dt2 <- dt2[!drop<1,] # comment this out if you want to retain all age splits regardless of small case number
  expanded2 <- rep(dt2$id, dt2$n.age) %>% data.table("id" = .)
  split2 <- merge(expanded2, dt2, by="id", all=T)
  split2[, age.rep := 1:.N - 1, by =.(id)]
  split2 <- split2 %>% 
    mutate(
      age_start = case_when(
        age.rep == 0 ~ 2,
        TRUE         ~ (age.rep)*5
      )
    ) 
  
  split2 <- split2 %>% 
    mutate(
      age_end = case_when(
        age.rep == 0 ~ 4,
        TRUE         ~ age_start+4
      )
    )
  
  ## This chunk for all other age_start values 
  dt3 <- dt[dt$age_start %nin% c(0,1,2,3,4)]
  dt3[, age_start := age_start - age_start %%5] # this seems harmless - unless becomes issue for under 1 ages
  dt3[, age_end := age_end - age_end %%5 + 4]
  dt3[, n.age:=((age_end+1 - age_start)/5)]
  dt3[, age_start_floor:=age_start]
  dt3[, drop := cases/n.age] # drop the data points if cases/n.age is less than 1 
  dt3 <- dt3[!drop<1,] # comment this out if you want to retain all age splits regardless of small case number
  expanded3 <- rep(dt3$id, dt3$n.age) %>% data.table("id" = .)
  split3 <- merge(expanded3, dt3, by="id", all=T)
  split3[, age.rep := 1:.N - 1, by =.(id)]
  split3[, age_start := age_start+age.rep*5] 
  split3[, age_end :=  age_start + 4] 
  
  ## Re-bind all age-split data back together 
  split <- rbind(split01, split0, split1, split2, split3)
  split <- merge(split, ages, by = c("age_start", "age_end"), all.x = T)
  return(split)
}

## Get DisMod age pattern
get_age_pattern <- function(locs, id, age_groups){
  age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = id, 
                           measure_id = measure_dismod, location_id = locs, source = "epi",
                           version_id = ver_id, sex_id = c(1,2), gbd_round_id = round_id, decomp_step = step, 
                           age_group_id = age_groups, year_id = 2010) 
  age_pattern_population <- get_population(location_id = locs, year_id = 2010, sex_id = c(1, 2),
                                           age_group_id = age_groups, gbd_round_id = round_id_functions, decomp_step = step_id_functions)
  age_pattern_population <- age_pattern_population[, .(age_group_id, sex_id, population, location_id)]
  age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
  age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
  age_pattern[, (draws) := NULL]
  age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]
  
  ## Get sample size and cases
  age_pattern[measure_id == 19, ss_age_pattern := (rate_dis*(1-rate_dis)/se_dismod^2)]
  age_pattern[, cases_age_pattern := ss_age_pattern * rate_dis]
  age_pattern[is.nan(ss_age_pattern), ss_age_pattern := 0] 
  age_pattern[is.nan(cases_age_pattern), cases_age_pattern := 0]
  
  age_pattern[, super_region_id := location_id]
  age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_age_pattern, ss_age_pattern, rate_dis, se_dismod, super_region_id)]
  return(age_pattern)
}

## Get population structure
get_pop_structure <- function(locs, years, age_groups){
  populations <- get_population(location_id = locs, year_id = years, gbd_round_id=round_id_functions, decomp_step = step_id_functions,
                                sex_id = c(1, 2, 3), age_group_id = age_groups)
  return(populations)
}

## Age-split the data that needs it
split_data <- function(raw_dt){
  dt_split <- copy(raw_dt)
  dt_split[, total_pop := sum(population), by = "id"]
  dt_split[, sample_size := (population / total_pop) * sample_size]
  dt_split[, cases_dis := sample_size * rate_dis]
  dt_split[, total_cases_dis := sum(cases_dis), by = "id"]
  dt_split[, total_sample_size := sum(sample_size), by = "id"]
  dt_split[, all_age_rate := total_cases_dis/total_sample_size]
  dt_split[, ratio := mean / all_age_rate]
  dt_split[, mean := ratio * rate_dis]
  dt_split[, cases := mean * sample_size]
  return(dt_split)
}

## Format age-split data 
format_data_forfinal <- function(unformatted_dt, location_split_id, region, original_dt){
  dt <- copy(unformatted_dt)
  
  dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
  blank_vars <- c("lower", "upper", "effective_sample_size", "standard_error", "uncertainty_type", "uncertainty_type_value", "seq")
  dt[, (blank_vars) := NA]
  dt <- get_se(dt)
  if (region == T) {
    dt[, note_modeler := paste0(note_modeler, "| age split using the super region age pattern", date)]
  } else {
    dt[, note_modeler := paste0(note_modeler, "| age split using the age pattern from location id: ")]
  }
  split_ids <- dt[, unique(id)]
  dt <- rbind(original_dt[!id %in% split_ids], dt, fill = T)
  dt <- dt[, c(names(df)), with = F]
  return(dt)
}

## -----------------------------------------------------------------------------
## SET UP TO ACTUALLY DO AGE-SPLITTING
## Get age metadata
ages <- get_age_metadata(19, gbd_round_id = round_id_functions)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age <- ages[age_start >= 0, age_group_id]

## Make copies to compare versions in case of debugging
df <- copy(dt)
gbd_id <- id

## -----------------------------------------------------------------------------
## DO AGE-SPLITTING
## Set location_pattern_id here
location_pattern_id <- 73 

## Get data needed and prep 
sex_names <- get_ids(table = "sex")
ages[, age_group_weight_value := NULL]
ages[age_start >= 2, age_end := age_end - 1] 
ages[age_end == 124, age_end := 99]
super_region_dt <- get_location_metadata(location_set_id = 22, gbd_round_id=round_id_functions)
super_region_dt <- super_region_dt[, .(location_id, super_region_id)]

## Some more copies
original <- copy(df)
original[, id := 1:.N]

## Use user-defined functions to do age-splitting
## Get cases and sample size
dt <- get_cases_sample_size(original)

## Get standard error
dt <- get_se(dt)

## Get these if they are STILL missing 
dt <- calculate_cases_fromse(dt)

## Format the data 
dt <- format_data(dt, sex_dt = sex_names) 

## Add rows to expand by age
split_dt <- expand_age(dt, age_dt = ages)

## Prep location pattern to use
if (region_pattern == T){
  split_dt <- merge(split_dt, super_region_dt, by = "location_id")
  super_regions <- unique(split_dt$super_region_id) 
  locations <- super_regions
} else {
  locations <- location_pattern_id
}

## Get locations and years in the data
pop_locs <- unique(split_dt$location_id)
pop_years <- unique(split_dt$year_id)

## Get age pattern from DisMod
print("Getting age pattern")
age_pattern <- get_age_pattern(locs = locations, id = gbd_id, age_groups = age) 

if (region_pattern == T) {
  age_pattern1 <- copy(age_pattern)
  split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id", "super_region_id"))
} else {
  age_pattern1 <- copy(age_pattern)
  split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id"))
}

## Get population information 
print("getting pop structure")
pop_structure <- get_pop_structure(locs = pop_locs, years = pop_years, age_group = age)
split_dt <- merge(split_dt, pop_structure, by = c("location_id", "sex_id", "year_id", "age_group_id"))

## Do the age-splitting - for real this time!
print("splitting data")
split_dt <- split_data(split_dt)
 
## -----------------------------------------------------------------------------
## FINAL CLEANUP AND SAVE
## Make sure this is reassigned - should be already 
split_dt$crosswalk_parent_seq <- split_dt$seq

## Format final data and bind to data that didn't need splitting 
final_dt <- format_data_forfinal(split_dt, location_split_id = location_pattern_id, region = region_pattern, original_dt = original)

## Clean up age_end mismatches
final_dt[age_end %in% c(4, 9, 14, 19, 24, 29, 34, 39, 44, 49, 54, 59, 64, 69, 74, 79, 84, 89, 94, 99, 124), age_end := age_end + 1]
final_dt[age_end == 0.50136986, age_end := 0.5]
final_dt[age_start == 0.50136986, age_start := 0.5]
ages_up <- ages
ages_up[age_end %in% c(4, 9, 14, 19, 24, 29, 34, 39, 44, 49, 54, 59, 64, 69, 74, 79, 84, 89, 94, 99, 124), age_end := age_end + 1]

## Make sure final dataset has correct age_start, age_end, age_group_id, etc. 
final_dt <- merge(final_dt, ages_up, by=c("age_start","age_end"), all.x = TRUE)
final_dt[age_start == 95 & age_end == 125, age_group_id := 235]

## Subset to only keep data that is in perfect age groups now - for ST-GPR
final_in_age_groups <- subset(final_dt, !is.na(age_group_id))

## Can check data not in perfect age groups here
final_no_ages <- subset(final_dt, is.na(age_group_id)) 

## Write csv to save for next step!
write.csv(final_in_age_groups, paste0("FILEPATH", format(Sys.time(), "%Y_%m_%d_%H_%M"), ".csv"))

