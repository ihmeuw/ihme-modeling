############################################################################################################################################
# Project: RF: Lead Exposure
# Purpose: age-split input data
#############################################################################################################################################

rm(list=ls())

require(data.table)
require(dplyr)
require(stringr)
require(boot)
require(openxlsx)

## OS locals
os <- .Platform$OS.type
if (os == "windows") {
  jpath <- "J:/"
} else {
  jpath <- "/home/j/"
}

## Resources for pulling in central data
source("FILEPATH/get_population.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/save_crosswalk_version.R")

#Load locations and restrict to relevant vars
locations <- get_location_metadata(gbd_round_id = 7, decomp_step = "iterative", location_set_id = 22) # REMEMBER TO CHECK GBD ROUND ID
locs <- copy(locations[, c("location_id", "location_name", "region_name", "level", "region_id", "super_region_id"), with=F])

#Load populations from 1970 to present (change location_set_version_id, year_id, & decomp_step)
pops <- get_population(location_set_id = 22, year_id = c(1970:2022), sex_id = c(1,2), location_id = -1,
                       age_group_id = c(2,3,6:20,30:32,34,235,238,388,389), gbd_round_id = 7, decomp_step = "iterative") %>% as.data.table

#Load age_groups_ids
ages <- get_age_metadata(age_group_set_id = 19, gbd_round_id = 7)[, .(age_group_id, age_group_years_start, age_group_years_end)]
setnames(ages, c("age_group_years_start","age_group_years_end"), c("age_start","age_end"))
ages[age_group_id %in% c(6:20,30:32,34), age_end := age_end - 1]
ages[age_group_id == 2, age_end := 0.01917807]
ages[age_group_id == 3, age_end := 0.07671232]
ages[age_group_id == 388, age_end := 0.41666667]
ages[age_group_id == 389, age_end := 0.91666667]
ages[age_group_id == 238, age_end := 1.91666667]
ages[age_group_id == 235, age_end := 99]

#Merge ages and populations so pops has an age_start and age_end column
setkeyv(pops, "age_group_id")
setkeyv(ages, "age_group_id")
pops <- merge(pops, ages)

# read in data (output of lead_crosswalk.R)
df <- fread("FILEPATH/lead_xw_final.csv")

#renaming vars for use with code
location_id<-"location_id"
year_id<-"year_id"
age_start<-"age_start"
age_end<-"age_end"
sex<-"sex_id"
estimate<-"val"
sample_size<-"sample_size"

#make number vars numeric
all_vars <- c(location_id, year_id, age_start, age_end, sex, estimate, sample_size)
df[, (all_vars) := lapply(.SD, as.numeric), .SDcols=all_vars]

#Create split id
df[, split_id := 1:.N]

## Save original values
orig <- c(age_start, age_end, sex, estimate, sample_size)
orig.cols <- paste0("preagesplit.", orig)
df[, (orig.cols) := lapply(.SD, function(x) x), .SDcols=orig]

## Separate metadata from required variables
cols <- c(location_id, year_id, age_start, age_end, sex, estimate, sample_size, "seq", "crosswalk_parent_seq")
meta.cols <- setdiff(names(df), cols)
metadata <- df[, meta.cols, with=F]
dt <- df[, c("split_id", cols), with=F]

## Round age groups to the nearest age-group boundary
dt[age_start < 0.01917808, age_start := 0]
dt[age_start >= 0.01917808 & age_start < 0.07671233, age_start := 0.01917808]
dt[age_start >= 0.07671233 & age_start < 0.5, age_start := 0.07671233]
dt[age_start >= 0.5 & age_start < 1, age_start := 0.5]
dt[age_start >= 1 & age_start < 2, age_start := 1]
dt[age_start >= 2 & age_start < 5, age_start := 2]
dt[age_start >= 5, age_start := age_start - age_start %%5]
dt[age_start > 95, age_start := 95]
dt[age_end <= 0.01917808, age_end := 0.01917807]
dt[age_end > 0.01917808 & age_end <= 0.07671233, age_end := 0.07671232]
dt[age_end > 0.07671233 & age_end < 0.5, age_end := 0.41666667]
dt[age_end >= 0.5 & age_end < 1, age_end := 0.91666667]
dt[age_end >= 1 & age_end < 2, age_end := 1.91666667]
dt[age_end >= 2 & age_end < 5, age_end := 4]
dt[age_end >= 5, age_end := age_end - age_end %%5 + 4]
dt[age_end > 95, age_end := 99]

# can run into issues if age start and age end are both the same number under 1 that
# happens to be a cutoff value
stopifnot(dt$age_end > dt$age_start)

#Merge in region and super-region info
dt <- merge(dt, locs, by="location_id",all.x=T)

## Split into training and split set (but only working with split for lead)
dt[, need_split := 1]
dt[age_start == 0 & age_end == 0.01917807, need_split := 0]
dt[age_start == 0.01917808 & age_end == 0.07671232, need_split := 0]
dt[age_start == 0.07671233 & age_end == 0.41666667, need_split := 0]
dt[age_start == 0.5 & age_end == 0.91666667, need_split := 0]
dt[age_start == 1 & age_end == 1.91666667, need_split := 0]
dt[age_start == 2 & age_end == 4, need_split := 0]
dt[age_start > 0 & (age_end - age_start) == 4, need_split := 0]
dt[age_start == 95 & age_end == 99, need_split := 0]

training <- dt[need_split == 0]
split <- dt[need_split == 1]


##########################
## Expand rows for splits
##########################

split[age_start == 0 & age_end %ni% c(0.07671232,0.41666667,0.91666667,1.91666667,125), n.age := ((age_end + 1)/5) + 5]
split[age_start == 0 & age_end == 0.07671232, n.age := 2]
split[age_start == 0 & age_end == 0.41666667, n.age := 3]
split[age_start == 0 & age_end == 0.91666667, n.age := 4]
split[age_start == 0 & age_end == 1.91666667, n.age := 5]
split[age_start == 0 & age_end == 125, n.age := 25]
split[age_start == 0.01917808 & age_end %ni% c(0.41666667,0.91666667,1.91666667), n.age := ((age_end + 1)/5) + 4]
split[age_start == 0.01917808 & age_end == 0.41666667, n.age := 2]
split[age_start == 0.01917808 & age_end == 0.91666667, n.age := 3]
split[age_start == 0.01917808 & age_end == 1.91666667, n.age := 4]
split[age_start == 0.07671233 & age_end %ni% c(0.91666667,1.91666667), n.age := ((age_end + 1)/5) + 3]
split[age_start == 0.07671233 & age_end == 0.91666667, n.age := 2]
split[age_start == 0.07671233 & age_end == 1.91666667, n.age := 3]
split[age_start == 0.5 & age_end != 1.91666667, n.age := ((age_end + 1)/5) + 2]
split[age_start == 0.5 & age_end == 1.91666667, n.age := 2]
split[age_start == 1, n.age := ((age_end + 1)/5) + 1]
split[age_start == 2, n.age := (age_end + 1)/5]
split[age_start > 2, n.age := (age_end + 1 - age_start)/5]

## Expand for age
split[, age_start_floor := age_start]
expanded <- rep(split$split_id, split$n.age) %>% data.table("split_id" = .)
split <- merge(expanded, split, by="split_id", all=T)
split[, age.rep := 1:.N - 1, by=.(split_id)]

split[age_start_floor == 0 & age.rep == 1, age_start := 0.01917808]
split[age_start_floor == 0 & age.rep == 2, age_start := 0.07671233]
split[age_start_floor == 0 & age.rep == 3, age_start := 0.5]
split[age_start_floor == 0 & age.rep == 4, age_start := 1]
split[age_start_floor == 0 & age.rep == 5, age_start := 2]
split[age_start_floor == 0 & age.rep > 5, age_start := (age.rep - 5) * 5]
split[age_start_floor == 0.01917808 & age.rep == 1, age_start := 0.07671233]
split[age_start_floor == 0.01917808 & age.rep == 2, age_start := 0.5]
split[age_start_floor == 0.01917808 & age.rep == 3, age_start := 1]
split[age_start_floor == 0.01917808 & age.rep == 4, age_start := 2]
split[age_start_floor == 0.01917808 & age.rep > 4, age_start := (age.rep - 4) * 5]
split[age_start_floor == 0.07671233 & age.rep == 1, age_start := 0.5]
split[age_start_floor == 0.07671233 & age.rep == 2, age_start := 1]
split[age_start_floor == 0.07671233 & age.rep == 3, age_start := 2]
split[age_start_floor == 0.07671233 & age.rep > 3, age_start := (age.rep - 3) * 5]
split[age_start_floor == 0.5 & age.rep == 1, age_start := 1]
split[age_start_floor == 0.5 & age.rep == 2, age_start := 2]
split[age_start_floor == 0.5 & age.rep > 2, age_start := (age.rep - 2) * 5]
split[age_start_floor == 1 & age.rep == 1, age_start := 2]
split[age_start_floor == 1 & age.rep > 1, age_start := (age.rep - 1) * 5]
split[age_start_floor == 2 & age.rep > 0, age_start := age.rep * 5]
split[age_start_floor > 2, age_start := age_start + age.rep * 5]

split[age_start == 0, age_end := 0.01917807]
split[age_start == 0.01917808, age_end := 0.07671232]
split[age_start == 0.07671233, age_end := 0.41666667]
split[age_start == 0.5, age_end := 0.91666667]
split[age_start == 1, age_end := 1.91666667]
split[age_start == 2, age_end := 4]
split[age_start >= 5, age_end := age_start + 4]

# read in bradmod output and create estimates for every age and sex group, then merge onto data to be split
estimates <- fread("FILEPATH/pred_out.csv")[, .(age_lower, pred_lower, pred_median, pred_upper)]
setnames(estimates, c("age_lower", "pred_median"), c("age_start", "est"))
estimates <- merge(ages[, .(age_start, age_end)], estimates, by = "age_start")

split <- merge(split, estimates, by = c("age_start", "age_end"))

#Merge in populations
#first, check to make sure there's no column named 'population' already
if ("population" %in% names(split)) {
  split[, population:=NULL]
}

split <- merge(split, pops[, c(all_vars[1:5], "population"), with=F], by = c(all_vars[1:5]),all.x=T)

#find ratio between estimate and original (unsplit) prevalence
split[, R:=est*population]
split[, R_group:=sum(R), by="split_id"]

#calculate final estimates and split sample size based on population
split[, pop_group:=sum(population), by = "split_id"]
split[, (estimate):=(get(estimate)*(R/R_group)*(pop_group/population))]
split[, sample_size:= sample_size*(population/pop_group)]

## Mark as split
split[, age_split := 1]

## adjust seqs
split[!is.na(seq), crosswalk_parent_seq := seq]
split[, seq := NA]

##############################################
## Append training, merge back metadata, clean
##############################################

## Append training, mark age_split
out <- rbind(split, training, fill=T)
out <- out[is.na(age_split), age_split := 0]

## Append on metadata
metadata <- metadata[,-("location_name"),with=F]
out <- merge(out, metadata, by="split_id", all.x=T)

## Clean
out <- out[, c(meta.cols, cols, "age_split", "n.age", "region_id", "super_region_id"), with=F]
out[, split_id := NULL]

# Add age_group_id back in (particularly helpful to re-do since NHANES data was tabulated to have
# incorrect id for highest age group)
out[, age_group_id := NULL]
out <- merge(ages, out, by = c("age_start","age_end"))

# save data
out <- out[order(nid, location_id, year_id, age_start, sex_id)]
out[, c("note_SR","note_modeler") := NULL]
out[, unit_value_as_published := 1]
setcolorder(out, c("nid","underlying_nid","page_num","table_num","source_type","ihme_loc_id","location_name","location_id","smaller_site_unit","site_memo",
                   "year_start","year_end","year_id","age_start","age_end","age_group_id","sex","sex_id","measure","val","variance","standard_error",
                   "standard_deviation","sample_size","seq","origin_seq","crosswalk_parent_seq","unit_value_as_published"))
write.xlsx(out, "FILEPATH/lead_age_split.xlsx", sheetName = "extraction")

###########################
## Upload crosswalk version
###########################

bundle_info <- data.table(read.xlsx("FILEPATH/versioning.xlsx", sheet = "GBD20"))[me_name == "envir_lead_exp", .(me_name, best_bundle_version)]
filepath <- "FILEPATH/lead_age_split.xlsx"
description <- "fixed variance calculation in crosswalks"

save_crosswalk_version(bundle_version_id = bundle_info$best_bundle_version, data_filepath = filepath, description = description)
