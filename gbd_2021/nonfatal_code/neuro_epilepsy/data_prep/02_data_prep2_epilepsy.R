##################################################################################################
## Purpose: Epilepsy data prep
## Creation Date: 07/06/2020
## Created by: USER
##################################################################################################
#This should be run after data has been sex split and the crosswalks applied.

rm(list=ls())

library(mortdb, lib = "FILEPATH")
library(Hmisc)
library(data.table)
library(msm)
library(plyr)
library(openxlsx)
library(magrittr)

##################################################################################################
date <- gsub("-", "_", Sys.Date())

functions_dir <- "FILEPATH"
functs <- c("get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids", "get_crosswalk_version", "save_crosswalk_version")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))
####################################################################################################
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
  dt <- dt[group_review == 1 | is.na(group_review)]
  dt <- dt[is_outlier==0 | is.na(is_outlier)] 
  dt <- dt[(age_end-age_start)>=25,] #split rows with greater than or equal to 25 year age range
  dt <- dt[!mean == 0 & !cases == 0, ] ##don't split rows with zero prevalence
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
  dt <- dt[!drop<1,] ##comment this out if you want to retain all age splits regardless of small case number
  expanded <- rep(dt$id, dt$n.age) %>% data.table("id" = .)
  split <- merge(expanded, dt, by="id", all=T)
  split[, age.rep := 1:.N - 1, by =.(id)]
  split[, age_start:= age_start+age.rep*5]
  split[, age_end :=  age_start + 4]
  split <- merge(split, age_dt, by = c("age_start", "age_end"), all.x = T)
  split[age_start == 0 & age_end == 4, age_group_id := 1]
  split <- split[age_group_id %in% age | age_group_id == 1]
  return(split)
}
## GET DISMOD AGE PATTERN
get_age_pattern <- function(locs, id, age_groups){
  age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", 
                  gbd_id = id, 
                  source = "epi", 
                  decomp_step = "iterative",
                  measure_id = c(5, 6),
                  sex_id = c(1,2), 
                  age_group_id = age_groups,
                  location_id = locs,
                  year_id = 2010)
  
  us_population <- get_population(location_id = locs, year_id = 2010, sex_id = c(1, 2),
                                  age_group_id = age_groups, decomp_step = "iterative")
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
  age_pattern[measure_id == 5, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
  age_pattern[measure_id == 6, sample_size_us := rate_dis/se_dismod^2]
  age_pattern[, cases_us := sample_size_us * rate_dis]
  age_pattern[is.nan(sample_size_us), sample_size_us := 0] 
  age_pattern[is.nan(cases_us), cases_us := 0]
  
  ## GET SEX ID 3
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
  
  age_pattern[, super_region_id := location_id]
  age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]
  return(age_pattern)
}
## GET POPULATION STRUCTURE
get_pop_structure <- function(locs, years, age_groups){
  populations <- get_population(location_id = locs, year_id = years, gbd_round_id=round_id_functions, decomp_step = step_id_functions,
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
## SPLIT THE DATA
split_data <- function(raw_dt){
  dt1 <- copy(raw_dt)
  dt1[, total_pop := sum(population), by = "id"]
  dt1[, sample_size := (population / total_pop) * sample_size]
  dt1[, cases_dis := sample_size * rate_dis]
  dt1[, total_cases_dis := sum(cases_dis), by = "id"]
  dt1[, total_sample_size := sum(sample_size), by = "id"]
  dt1[, all_age_rate := total_cases_dis/total_sample_size]
  dt1[, ratio := mean / all_age_rate]
  dt1[, mean := ratio * rate_dis ]
  dt1 <- dt1[mean < 1, ]
  dt1[, cases := mean * sample_size]
  return(dt1)
}
## FORMAT DATA TO FINISH
format_data_forfinal <- function(unformatted_dt, location_split_id, region, original_dt, tosplit = tosplit_dt){
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
  dt <- data.table(do.call(rbind.fill, list(dt, original_dt, tosplit)))
  dt <- dt[, c(names(df)), with = F] 
  return(dt)
}
# AGE SPLIT FUNCTION -----------------------------------------------------------------------
age_split <- function(gbd_id, df, age, region_pattern, location_pattern_id = 1){
  
  cols<- c(names(df)) 
  ## GET TABLES
  sex_names <- get_ids(table = "sex")
  ages <- age
  setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
  ages[, age_group_weight_value := NULL]
  ages[age_start >= 1, age_end := age_end - 1]
  ages[age_end == 124, age_end := 99]
  age <- ages[age_start >= 0, age_group_id]
  super_region_dt <- get_location_metadata(location_set_id = 22) 
  super_region_dt <- super_region_dt[, .(location_id, super_region_id)]
  
  ## SAVE ORIGINAL DATA (DOESN'T REQUIRE ALL DATA TO HAVE SEQUENCES)
  df[, id := 1:.N]
  original <- copy(df)
  
  Sys.sleep(1)
  ## FORMAT DATA
  df[, cases:= as.numeric(cases)]
  df[, sample_size:= as.numeric(sample_size)]
  df <- get_cases_sample_size(df)
  df <- get_se(df)
  df <- calculate_cases_fromse(df)
  
  tosplit_dt <- format_data(df, sex_dt = sex_names) 
  nosplit_dt <-copy(original[!id %in% tosplit_dt[, id]])
  ## EXPAND AGE
  split_dt <- expand_age(tosplit_dt, age_dt = ages)
  print(nrow(split_dt))
  tosplit_dt[, is_outlier := 1]
  
  ## GET PULL LOCATIONS
  if (region_pattern == T){
    super_regions <- unique(split_dt$super_region_id) ##get super regions for dismod results
    locations <- super_regions
  } else {
    locations <- location_pattern_id
  }
  
  ##GET LOCATIONS AND POPULATIONS
  pop_locs <- unique(split_dt$location_id)
  pop_years <- unique(split_dt$year_id)
  
  ## GET AGE PATTERN
  print("getting age pattern")
  age_pattern <- get_age_pattern(locs = locations, id = gbd_id, age_groups = age) #set desired locations here if you want to specify something other than super regions or global
  
  if (region_pattern == T) {
    age_pattern1 <- copy(age_pattern)
    split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id", "super_region_id"))
  } else {
    age_pattern1 <- copy(age_pattern)
    split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id"))
  }
  print(nrow(split_dt))
  ## GET POPULATION INFO
  print("getting pop structure")
  pop_structure <- get_pop_structure(locs = pop_locs, years = pop_years, age_groups = age)
  split_dt <- merge(split_dt, pop_structure, by = c("location_id", "sex_id", "year_id", "age_group_id"))
  print(nrow(split_dt))
  #####CALCULATE AGE SPLIT POINTS#######################################################################
  print("splitting data")
  split_dt <- split_data(split_dt)
  print(nrow(split_dt))
  ######################################################################################################
  split_dt$crosswalk_parent_seq <- split_dt$seq 
  final_dt <- format_data_forfinal(split_dt, location_split_id = location_pattern_id, region = region_pattern,
                                   original_dt = nosplit_dt, tosplit = tosplit_dt)
  print(nrow(split_dt))
  print(nrow(nosplit_dt))
  print(nrow(tosplit_dt))
  
  return(final_dt)
  
}



###################################################################################################
a_cause <- "MOD_ENV_"
id <- 26149 ## this is the model entity identifier for age split Dismod model entity identifier of cause
ver_id <- 512894 ## Dismod version used for age split
measure_dismod <- 5 #5 = prev, 6 = incidence, 18 = proportion
round_id = 7 #6 for a Dismod model from 2019, 7 for a model from 2020 This matches where we pulled our dismod model from.
step <- 'iterative' #step for Dismod version used for age split (usually iterative) This matches where we pulled our dismod model from.
region_pattern <- F #T if want to use super region pattern for age split, F to use global
draws <- paste0("draw_", 0:999)
round_id_functions <- 7 #this is for shared function calls
step_id_functions <- 'iterative' #this is for shared function calls


##################################################################################
date <- "2020_09_23"
flat_file_dir <- paste0("FILEPATH", date)

input_dt <- read.csv(paste0(flat_file_dir, "adjusted_data.csv"))

full_dt <- copy(data.table(input_dt))

gbd_id = id
df = full_dt
region_pattern = region_pattern
location_pattern_id = 1
region_pattern = F

age <- get_age_metadata(19, gbd_round_id = 7)
final_split <- age_split(gbd_id = id, age = age, df = full_dt, region_pattern = region_pattern, location_pattern_id = 1)
final_split <- copy(final_dt)
#Check age split was successful
check_dt <- final_split[(final_split$age_end-final_split$age_start>25) & final_split$mean!=0 & final_split$is_outlier!=1 & final_split$group_review!=0 &(final_split$measure=="prevalence" | final_split$measure=="incidence" | final_split$measure=="proportion"), ]

table(check_dt[, group_review]) 
sum(is.na(check_dt$mean)) 
table(check_dt$is_outlier, useNA = "ifany") 
table(check_dt$measure, useNA = "ifany") 

check_dt[, n.age:=(age_end+1 - age_start)/5]
check_dt[, drop := cases/n.age]
sum(check_dt[, drop > 1], na.rm = T) 

final_split[location_id == 95, group_review := 0]
final_split <- final_split[group_review == 1 | is.na(group_review)] 
final_split[standard_error > 1, standard_error := 1]

#outlier poland claims:
final_split[nid %in% c(397812, 397813, 397814), is_outlier := 1]

#outlier nid 125545 and 269696
final_split[nid %in% c(125545, 269696), is_outlier := 1]

#Outlier a Peru study
final_split[nid %in% c(124520), is_outlier := 1]

final_split <- data.table(apply(final_split, MARGIN = 2, FUN = function(x){stringi::stri_trans_general(x, "latin-ascii")}))
final_split <- data.table(final_split[,lapply(.SD,function(x){ifelse(is.na(x),"",x)})])
final_split <- data.table(apply(final_split, 2, FUN = function(x){gsub("\'", "", x)})) 
final_split <- data.table(apply(final_split, 2, FUN = function(x){gsub("\"", "", x)}))
final_split <- data.table(apply(final_split, 2, FUN = function(x){gsub("%", "percent", x)}))

final_split[,c("case_name", "case_diagnostics", "case_definition") := NULL]
final_split[,case_name := ""]
final_split[,case_definition := ""]
final_split[, case_diagnostics := ""]

final_split[, seq := as.character(seq)]
final_split[, seq := ""]

#Outlier marketscan 2000
final_split[nid %in% c(244369), is_outlier := 1]

path_to_data <- paste0(flat_file_dir, "adjusted_data_global_age_sex_split_logit_xwalks_mkt_scn_outlier", date,".xlsx")

write.xlsx(final_split, path_to_data, sheetName= 'extraction')
save_crosswalk_version(33689, path_to_data,
                       description = "crosswalked, global age, sex split data, 2020_09_24, NID 125385 and 15-44 and 45-59 for NID 125382 outlier for new xwalk, marketscan 2000 outlier")


#### Save crosswalk with marketscan 2000 outliered
final_split[cv_marketscan_all_2000 == 1, is_outlier := 1]
write.xlsx(final_split, paste0(flat_file_dir, "adjusted_data_global_age_sex_split_logit_xwalks_marketscan_2000_outlier.xlsx"), sheetName= 'extraction')

save_crosswalk_version(28946, paste0(flat_file_dir, "adjusted_data_global_age_sex_split_logit_xwalks_marketscan_2000_outlier.xlsx") ,
                       description = "crosswalked, global age, sex split data 2020_08_10 marketscan 2000 outliered, logit xwalks")

gbd_id = id
df = full_dt
location_pattern_id = 1
region_pattern <- T
region_split <- age_split(gbd_id = id, df = full_dt, region_pattern = region_pattern, location_pattern_id = 1)
#Check age split was successful 
check_dt <- region_split[(region_split$age_end-region_split$age_start>25) & region_split$mean!=0 & region_split$is_outlier!=1 & region_split$group_review!=0 &(region_split$measure=="prevalence" | region_split$measure=="incidence" | region_split$measure=="proportion"), ]

sum(is.na(check_dt$mean)) 
table(check_dt$is_outlier, useNA = "ifany")
table(check_dt$measure, useNA = "ifany") 

check_dt[, n.age:=(age_end+1 - age_start)/5]
check_dt[, drop := cases/n.age]
sum(check_dt[, drop > 1], na.rm = T) 

region_split[location_id == 95, group_review := 0]
region_split <- region_split[group_review == 1 | is.na(group_review)] 
region_split[standard_error > 1, standard_error := 1]

#outlier poland claims:
region_split[nid %in% c(397812, 397813, 397814), is_outlier := 1]

region_split[,c("case_name", "case_diagnostics", "case_definition") := NULL]
region_split[,case_name := ""]
region_split[,case_definition := ""]
region_split[, case_diagnostics := ""]

region_split[, seq := ""]

write.xlsx(region_split, paste0(flat_file_dir, "adjusted_data_super_region_age_sex_split_inc_marketscan_2000_poland_clin_outliered.xlsx"), sheetName= 'extraction')
save_crosswalk_version(28946, paste0(flat_file_dir, "adjusted_data_super_region_age_sex_split_inc_marketscan_2000_poland_clin_outliered.xlsx") ,
                       description = "crosswalked, super region age, sex split data, 2020_08_06 inc marketscan 2000 xwalk poland clinical outliered")

#### Save crosswalk with marketscan 2000 outliered
region_split[cv_marketscan_all_2000 == 1, is_outlier := 1]
write.xlsx(region_split, paste0(flat_file_dir, "adjusted_super_region_age_sex_split_marketscan_2000_outliered_poland_clin_outliered.xlsx"), sheetName= 'extraction')

save_crosswalk_version(28946, paste0(flat_file_dir, "adjusted_super_region_age_sex_split_marketscan_2000_outliered_poland_clin_outliered.xlsx") ,
                       description = "crosswalked, super region age, sex split data 2020_08_06 marketscan 2000 outliered, poland clinical outliered")
