#####################################################################################
### This script age-split aggregated age data by using DisMod output
####################################################################################

pacman::p_load(data.table, openxlsx, ggplot2, magrittr)
date <- gsub("-", "_", Sys.Date())
date <- Sys.Date()

# GET OBJECTS -------------------------------------------------------------
b_id <- "BUNDLE_ID"  #this is bundle ID
a_cause <- "digest_pancreatitis"
name <- "Acute_pancreatitis"

date <- gsub("-", "_", date)
draws <- paste0("draw_", 0:999)

# SET FUNCTIONS ------------------------------------------------------------
library(mortdb, lib = "FILEPATH")
repo_dir <- "FILEPATH"
functions_dir <- "FILEPATH"
functs <- c("get_crosswalk_version.R", "save_crosswalk_version.R","get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))

# INPUT DATA -------------------------------------------------------------
dt <- copy("FILEPATH") #CROSSWALKED DATASET

dt$crosswalk_parent_seq <- dt$seq 
dt$group_review[is.na(dt$group_review)] <-1
dt$group_review[dt$group_review==0 & dt$is_outlier==0] <-1 #correct wrongly tagged

dt_inc <- subset(dt, measure=="incidence")

# CREATE FUNCTIONS -----------------------------------------------------------

## FILL OUT MEAN/CASES/SAMPLE SIZE
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

## GET CASES IF THEY ARE MISSING
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "proportion", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

## MAKE SURE DATA IS FORMATTED CORRECTLY
format_data <- function(unformatted_dt, sex_dt){
  dt <- copy(unformatted_dt)
  dt[, `:=` (mean = as.numeric(mean), sample_size = as.numeric(sample_size), cases = as.numeric(cases),
             age_start = as.numeric(age_start), age_end = as.numeric(age_end), year_start = as.numeric(year_start))]
  dt <- dt[measure %in% c("proportion", "prevalence", "incidence"),] 
  dt <- dt[!group_review==0 | is.na(group_review),] ##don't use group_review 0
  dt <- dt[is_outlier==0,] ##don't age split outliered data
  dt <- dt[(age_end-age_start)>25 & cv_literature==1 ,] #for prevelance, incidence, proportion
  dt <- dt[(!mean == 0 & !cases == 0) |(!mean == 0 & is.na(cases))  , ] 
  dt <- merge(dt, sex_dt, by = "sex")
  dt[measure == "proportion", measure_id := 18]
  dt[measure == "prevalence", measure_id := 5]
  dt[measure == "incidence", measure_id := 6]
  dt[, year_id := round((year_start + year_end)/2, 0)] 
  return(dt)
}

## CREATE NEW AGE ROWS
expand_age <- function(small_dt, age_dt = ages){
  dt <- copy(small_dt)
  
  ## ROUND AGE GROUPS
  dt[, age_start := age_start - age_start %%5]
  dt[, age_end := age_end - age_end %%5 + 4]
  dt <- dt[age_end > 99, age_end := 99]
  
  ## EXPAND FOR AGE
  dt[, n.age:=(age_end+1 - age_start)/5]
  dt[, age_start_floor:=age_start]
  dt[, drop := cases/n.age] ##drop the data points if cases/n.age is less than 1 
  expanded <- rep(dt$id, dt$n.age) %>% data.table("id" = .)
  split <- merge(expanded, dt, by="id", all=T)
  split[, age.rep := 1:.N - 1, by =.(id)]
  split[, age_start:= age_start+age.rep*5]
  split[, age_end :=  age_start + 4]
  split <- merge(split, age_dt, by = c("age_start", "age_end"), all.x = T)
  split[age_start == 0 & age_end == 4, age_group_id := 1]
  split <- split[age_group_id %in% age | age_group_id == 1] ##don't keep where age group id isn't estimated for cause
  return(split)
}
 
## GET DISMOD AGE PATTERN
get_age_pattern <- function(locs, id, age_groups){
  age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = id, ## USING 2010 AGE PATTERN BECAUSE LIKELY HAVE MORE DATA FOR 2010
                           measure_id = measure_id, location_id = locs, source = "epi", 
                           version_id = version_id,  sex_id = c(1,2), gbd_round_id = 6, decomp_step = "step2", #can replace version_id with status = "best" or "latest"
                           age_group_id = age_groups, year_id = 2010) ##imposing age pattern
  us_population <- get_population(location_id = locs, year_id = 2010, sex_id = c(1, 2),
                                  age_group_id = age_groups, decomp_step = "step2")
  us_population <- us_population[, .(age_group_id, sex_id, population, location_id)]
  age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
  age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
  age_pattern[, (draws) := NULL]
  age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]
  
  ## AGE GROUP 1 (SUM POPULATION WEIGHTED RATES)
  age_1 <- copy(age_pattern)
  age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5), ]
  se <- copy(age_1)
  se <- se[age_group_id==5, .(measure_id, sex_id, se_dismod, location_id)] 
  age_1 <- merge(age_1, us_population, by = c("age_group_id", "sex_id", "location_id"))
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
  
  ## CASES AND SAMPLE SIZE
  age_pattern[measure_id == 18, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
  age_pattern[, cases_us := sample_size_us * rate_dis]
  age_pattern[is.nan(sample_size_us), sample_size_us := 0] ##if all draws are 0 can't calculate cases and sample size b/c se = 0, but should both be 0
  age_pattern[is.nan(cases_us), cases_us := 0]
  
  ## GET SEX ID 3
  sex_3 <- copy(age_pattern)
  sex_3[, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
  sex_3[, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
  sex_3[, rate_dis := cases_us/sample_size_us]
  sex_3[measure_id == 18, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
  sex_3[is.nan(rate_dis), rate_dis := 0] ##if sample_size is 0 can't calculate rate and standard error, but should both be 0
  sex_3[is.nan(se_dismod), se_dismod := 0]
  sex_3 <- unique(sex_3, by = c("age_group_id", "measure_id", "location_id"))
  sex_3[, sex_id := 3]
  age_pattern <- rbind(age_pattern, sex_3)
  
  age_pattern[, super_region_id := location_id]
  age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]
  return(age_pattern)
}

## GET POPULATION STRUCTURE
get_pop_structure <- function(locs, years, age_groups){
  populations <- get_population(location_id = locs, year_id = years,decomp_step = "step2",
                                sex_id = c(1, 2, 3), age_group_id = age_groups)
  age_1 <- copy(populations) ##create age group id 1 by collapsing lower age groups
  age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5)]
  age_1[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
  age_1 <- unique(age_1, by = c("location_id", "year_id", "sex_id"))
  age_1[, age_group_id := 1]
  populations <- populations[!age_group_id %in% c(2, 3, 4, 5)]
  populations <- rbind(populations, age_1)  ##add age group id 1 back on
  return(populations)
}

## ACTUALLY SPLIT THE DATA
split_data <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[, total_pop := sum(population), by = "id"]
  dt[, sample_size := (population / total_pop) * sample_size]
  dt[, cases_dis := sample_size * rate_dis]
  dt[, total_cases_dis := sum(cases_dis), by = "id"]
  dt[, total_sample_size := sum(sample_size), by = "id"]
  dt[, all_age_rate := total_cases_dis/total_sample_size]
  dt[, ratio := mean / all_age_rate]
  dt[, mean := ratio * rate_dis ]
  dt <- dt[mean < 1, ]
  dt[, cases := mean * sample_size]
  return(dt)
}

## FORMAT DATA TO FINISH
format_data_forfinal <- function(unformatted_dt, location_split_id, region, original_dt){
  dt <- copy(unformatted_dt)
  dt[, group := 1]
  dt[, specificity := "age,sex"]
  dt[, group_review := 1]
  dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
  blank_vars <- c("lower", "upper", "effective_sample_size", "standard_error", "uncertainty_type", "uncertainty_type_value", "seq")
  dt[, (blank_vars) := NA]
  dt <- get_se(dt)
  if (region == T) {
    dt[, note_modeler := paste0(note_modeler, "| age split using the super region age pattern", date)]
  } else {
    dt[, note_modeler := paste0(note_modeler, "| age split using the age pattern from location id ", location_split_id, " ", date)]
  }
  split_ids <- dt[, unique(id)]
  dt <- rbind(original_dt[!id %in% split_ids], dt, fill = T)
  dt <- dt[, c(names(df)), with = F]
  return(dt)
}


###########################################################################################
## WE AGE-SPLIT INCIDENCE DATA
id <- "DISMOD_ID" ## this is the meid for iterative or wherever age split Dismod was run
version_id <- "DISMOD_VERSION_ID"
measure_id <- 6 ##Measure ID 5= prev, 6=incidence, 18=proportion
region_pattern <- FALSE


# RUN THESE CALLS ---------------------------------------------------------------------------
ages <- get_age_metadata(12)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age_groups <- ages[age_start >= 5, age_group_id]

df <- copy(dt_inc)
age <- age_groups
gbd_id <- id

location_pattern_id <- 1

# AGE SPLIT FUNCTION -----------------------------------------------------------------------
  ## GET TABLES
  sex_names <- get_ids(table = "sex")
  ages <- get_age_metadata(12)
  setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
  ages[, age_group_weight_value := NULL]
  ages[age_start >= 1, age_end := age_end - 1]
  ages[age_end == 124, age_end := 99]
  super_region_dt <- get_location_metadata(location_set_id = 22)
  super_region_dt <- super_region_dt[, .(location_id, super_region_id)]
  
  ## SAVE ORIGINAL DATA 
  original <- copy(df)
  original[, id := 1:.N]
  
  ## FORMAT DATA
  dt <- format_data(original, sex_dt = sex_names)
  dt <- get_cases_sample_size(dt)
  dt <- get_se(dt)
  dt <- calculate_cases_fromse(dt)
  
  ## EXPAND AGE
  split_dt <- expand_age(dt, age_dt = ages)
  
  ## GET PULL LOCATIONS
  if (region_pattern == T){
    split_dt <- merge(split_dt, super_region_dt, by = "location_id")
    super_regions <- unique(split_dt$super_region_id) ##get super regions for dismod results
    locations <- super_regions
  } else {
    locations <- location_pattern_id
  }
  
  ##GET LOCS AND POPS
  pop_locs <- unique(split_dt$location_id)
  pop_years <- unique(split_dt$year_id)
  
  ## GET AGE PATTERN
  print("getting age pattern")
  age_pattern <- get_age_pattern(locs = locations, id = gbd_id, age_groups = age) 
  
  if (region_pattern == T) {
    age_pattern1 <- copy(age_pattern)
    split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id", "super_region_id"))
  } else {
    age_pattern1 <- copy(age_pattern)
    split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id"))
  }
  
  ## GET POPULATION INFO
  print("getting pop structure")
  pop_structure <- get_pop_structure(locs = pop_locs, years = pop_years, age_group = age)
  split_dt <- merge(split_dt, pop_structure, by = c("location_id", "sex_id", "year_id", "age_group_id"))
  
  ## CREATE NEW POINTS
  print("splitting data")
  split_dt <- split_data(split_dt)
  
  
  final_dt <- format_data_forfinal(split_dt, location_split_id = location_pattern_id, region = region_pattern,
                                   original_dt = original)
  
  
###########################################################################################
## LASTLY, APPEND PREVALENCE AND INCIDENCE DATA AND SAVE
  
write.csv(final_dt, "FILEPATH")

