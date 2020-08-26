####################################################################
## Proportion HIV due to Sex
## Purpose: Pull data from bundle, prepare crosswalk version, upload
####################################################################

# Clean up and initialize with the packages we need
rm(list = ls())
library(data.table)
library(mortdb, lib = "FILEPATH")
pacman::p_load(data.table, openxlsx, ggplot2, plyr, parallel, dplyr, RMySQL, stringr, msm)

date <- Sys.Date()
date <- gsub("-", "_", Sys.Date())

draws <- paste0("draw_", 0:999)

# Central functions
functs <- c("get_draws", "get_outputs", "get_population", "get_location_metadata", "get_model_results", "get_age_metadata", "get_ids", "get_bundle_data", "save_crosswalk_version", "save_bundle_version", "get_bundle_version")
invisible(lapply(functs, function(x) source(paste0("FILEPATH", x, ".R"))))

mrbrt_functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function",
                  "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function",
                  "load_mr_brt_preds_function", "plot_mr_brt_function")
invisible(lapply(mrbrt_functs, function(x) source(paste0("FILEPATH", x, ".R"))))

source("FILEPATH")

# Set objects
bid<-386
dstep<-"step2"

# Download data and export for manual review
df<-get_bundle_data(bundle_id = bid, decomp_step = dstep, gbd_round_id = 6)
drops<-copy(df)

# Only keep data that were used in the past
df<-df[is_outlier==1, drop:=1]
df<-df[note_modeler%like%"using the super region" & group_review==1, drop:=1]
df<-df[group_review==0 & !(note_modeler%like%"using the super region"), drop:=1]
df<-df[is.na(drop)]
df<-df[, drop:=NULL]

# There are duplicate data, identify and drop these from the bundle
df<-unique(df, by = c("location_id", "year_start", "year_end", "nid", "underlying_nid", "mean", "lower", "upper", "cases", "sample_size"))

drops<-unique(drops[,.(nid, location_id, year_start, year_end, group_review)])
drops<-drops[is.na(group_review), group_review:=1]
drops<-drops[, sum_gr:=sum(group_review), by = c("location_id", "nid", "year_start", "year_end")]
drops<-drops[, drop:=ifelse(sum_gr==0, 1, 0)]
drops<-unique(drops[drop==1, .(location_id, nid, year_start, year_end, drop)])
df<-merge(df, drops, by = c("location_id", "nid", "year_start", "year_end"), all.x=T)
df<-df[is.na(drop)]
df<-df[, drop:=NULL]

backup<-copy(df)

#### Age-sex split using the inverse age pattern from IDU risk factor
df<-df[age_end>99, age_end:=99]

## GET TABLES
sex_names <- get_ids(table = "sex")
ages <- get_age_metadata(12, gbd_round_id = 6)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
ages[, age_group_weight_value := NULL]
ages[age_start >= 1, age_end := age_end - 1]
ages[age_end == 124, age_end := 99]

## SAVE ORIGINAL DATA (DOESN'T REQUIRE ALL DATA TO HAVE SEQS)
original <- copy(df)
original[, id := 1:.N]

## FORMAT DATA
dt <- format_data(original, sex_dt = sex_names)
dt <- get_cases_sample_size(dt)
dt <- get_se(dt)
dt <- calculate_cases_fromse(dt)
dt<-dt[sample_size>25]

## EXPAND AGE
age<-unique(ages$age_group_id)
split_dt <- expand_age(dt, age_dt = ages)
m<-split_dt[sex=="Both"]
m<-m[, sex:="Male"]
m<-m[, sex_id:=1]
f<-split_dt[sex=="Both"]
f<-f[, sex:="Female"]
f<-f[, sex_id:=2]
split_dt<-split_dt[sex%in%c("Male", "Female")]
split_dt<-rbind(split_dt, m)
split_dt<-rbind(split_dt, f)

##Get super region information and merge on
super_region_dt <- get_location_metadata(location_set_id = 22, gbd_round_id = 6)
super_region_dt <- super_region_dt[, .(location_id, super_region_id)]
  split_dt <- merge(split_dt, super_region_dt, by = "location_id")
  super_regions <- unique(split_dt$super_region_id) ##get super regions for dismod results
  locations <- super_regions

##GET LOCS AND POPS
pop_locs <- unique(split_dt$location_id)
pop_years <- unique(split_dt$year_id)

## GET AGE PATTERN
print("getting age pattern")
age_pattern <- get_age_pattern(locs = locations, age_groups = age)
age_pattern<-age_pattern[, measure_id:=NULL]
split_dt <- merge(split_dt, age_pattern, by = c("sex_id", "age_group_id", "super_region_id"))

## GET POPULATION INFO
print("getting pop structure")
pop_structure <- get_pop_structure(locs = pop_locs, years = pop_years, age_group = age)
split_dt <- merge(split_dt, pop_structure, by = c("location_id", "sex_id", "year_id", "age_group_id"))

#####CALCULATE AGE SPLIT POINTS#######################################################################
## CREATE NEW POINTS
print("splitting data")
split_dt <- split_data(split_dt)
######################################################################################################

cap<-copy(split_dt)
cap<-cap[mean>.99, mean:=.99]

final_dt <- format_data_forfinal(cap, location_split_id = location_pattern_id, region = T,
                                       original_dt = original)

## Now we have to sex-split the data points with sample size less than 25 that were not age-sex split above
original <- copy(final_dt)
original[, id := 1:.N]

## FORMAT DATA
dt <- format_data(original, sex_dt = sex_names)
dt <- get_cases_sample_size(dt)
dt <- get_se(dt)
dt <- calculate_cases_fromse(dt)

m<-dt[sex=="Both"]
m<-m[, sex:="Male"]
m<-m[, sex_id:=1]
f<-dt[sex=="Both"]
f<-f[, sex:="Female"]
f<-f[, sex_id:=2]
split_dt<-rbind(m, f)

  split_dt <- merge(split_dt, super_region_dt, by = "location_id")
  super_regions <- unique(split_dt$super_region_id) ##get super regions for dismod results
  locations <- super_regions

##GET LOCS AND POPS
pop_locs <- unique(split_dt$location_id)
pop_years <- unique(split_dt$year_id)

## GET AGE PATTERN
print("getting age pattern")
source("FILEPATH")
## GET DISMOD AGE PATTERN
get_age_pattern <- function(locs){
  age_pattern <- as.data.table(get_outputs(topic = "rei", rei_id = c(103), cause_id = 402,
                                           location_id = locs, metric_id = 2, sex_id = c(1,2),
                                           gbd_round_id=5,  age_group_id = 22, measure_id = 3,
                                           year_id = 2010)) ##imposing age pattern 
  setnames(age_pattern, "val", "rate_dis")
  ##Reverse age pattern by inverting it
  age_pattern[, max_rate := max(rate_dis), by = c("location_id")]
  age_pattern[, max_rate := max_rate + 0.01] 
  age_pattern[, rate_dis := max_rate - rate_dis]
  age_pattern[, max_rate := NULL]
  age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, rate_dis)]
  age_pattern[, super_region_id := location_id]
  age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, rate_dis, super_region_id)]
  return(age_pattern)
}

age_pattern <- get_age_pattern(locs = locations)
age_pattern<-age_pattern[, measure_id:=NULL]
split_dt <- merge(split_dt, age_pattern, by = c("sex_id", "super_region_id"))

## GET POPULATION INFO
print("getting pop structure")
hiv <- get_model_results(gbd_team = "epi", gbd_id = 9368, age = 22,
                         status = "best", location_id = pop_locs, year_id = pop_years, measure_id = 5, gbd_round_id = 5)
hiv<-hiv[,.(location_id, year_id, age_group_id, sex_id, mean)]
populations <- get_population(location_id = pop_locs, year_id = pop_years, decomp_step = "step2",
                              sex_id = c(1, 2, 3), age_group_id = 22)
populations <- merge(hiv, populations, by = c("age_group_id", "location_id", "sex_id", "year_id"))
populations[, population := mean * population]
populations <- populations[, .(location_id, sex_id, year_id, age_group_id, population)]

split_dt <- merge(split_dt, populations, by = c("location_id", "sex_id", "year_id"))
split_dt<-split_dt[, age_group_id.x:=NULL]
split_dt<-split_dt[, age_group_id.y:=NULL]

#####CALCULATE AGE SPLIT POINTS#######################################################################
## CREATE NEW POINTS
print("splitting data")
split_dt<-split_dt[, total_pop := sum(population), by = "id"]
split_dt<-split_dt[, sample_size := (population / total_pop) * sample_size]
split_dt<-split_dt[, cases_dis := sample_size * rate_dis]
split_dt<-split_dt[, total_cases_dis := sum(cases_dis), by = "id"]
split_dt<-split_dt[, total_sample_size := sum(sample_size), by = "id"]
split_dt<-split_dt[, all_age_rate := total_cases_dis/total_sample_size]
split_dt<-split_dt[, ratio := mean / all_age_rate]
split_dt<-split_dt[, mean := ratio * rate_dis]
split_dt<-split_dt[, cases := mean * sample_size]
######################################################################################################

cap<-copy(split_dt)
cap<-cap[mean>.99, mean:=.99]
final_dt <- format_data_forfinal(cap, location_split_id = location_pattern_id, region = T,
                                 original_dt = original)

final_dt<-final_dt[mean!=0]
final_dt<-final_dt[!is.na(group), group_review:=1]
final_dt<-final_dt[standard_error>1, standard_error:=NA]
final_dt<-final_dt[sample_size>0 | is.na(sample_size)]

write.xlsx(final_dt, "FILEPATH", sheetName="extraction")

save_bundle_version(bundle_id = bid, decomp_step = "step2", include_clinical = F)
save_crosswalk_version(bundle_version_id = 10919, data_filepath = "FILEPATH", description = "Age-sex split using inverse IDU-HEP SR pattern, capped at 1, min SS split 25")

## Invert to 1-p
source("FILEPATH")
final_dt<-get_crosswalk_version(crosswalk_version_id = 7073, export = F)
final_dt<-final_dt[, mean:=1-mean]
final_dt<-final_dt[, lower:=NA]
final_dt<-final_dt[, upper:=NA]
final_dt<-final_dt[, uncertainty_type_value:=NA]
final_dt<-final_dt[standard_error>1, standard_error:=NA]
final_dt<-final_dt[!is.na(crosswalk_parent_seq), seq:=NA]

bid<-386

write.xlsx(final_dt, "FILEPATH", sheetName="extraction")


