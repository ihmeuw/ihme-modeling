############################################################################################################################################
# Project: RF: Lead Exposure
# Purpose: Perform age-sex splitting using a space-time method to match training data (sources that reported results
#         with IHME's five-year age groups and separated by sex) with split data (sources in need of age-sex splitting)
#
# Inputs: Input dataframe (df) with columns specifying location_id, age_start and age_end (ex. age_group_id 7: age_start = 10,
#         age_end = 14), year_id, sex_id, estimate and sample size for each data source.
#
#         wspace = the input to calculate space weight,
#         where space weight = (1 - (distance*wspace)), distance = {0 if same location_id, 1 if same region_id, 2 if same super_region_id}
#
#         wtime = input to calculate time weight, where time weight = 1 - abs(year_split - year_train)*wtime
#
#         pspace = weight given to the spatial component in calculating total space-time weight,
#                  wheere total weight = ((pspace*spacewt) + (ptime*timewt))
#         NOTE: ptime = 1 - pspace
#
#         nkeep = # of training sources to use to estimate each source to be split (keeps those with the highest space-time weights)
# -------------------------------------------------------------------------------------------------------------------------------------------
# Notes: As of GBD 2019, sex-splitting is done in MR-BRT, so this code has been adjusted to remove all the sex-splitting stuff.
#############################################################################################################################################

rm(list=ls())

require(data.table)
require(dplyr)
require(stringr)
require(boot)
require(openxlsx)

## Resources for pulling in central data
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

#Load locations and restrict to relevant vars
locations <- get_location_metadata(gbd_round_id = 6, location_set_id = 22) # REMEMBER TO CHECK GBD ROUND ID
locs <- copy(locations[, c("location_id", "location_name", "region_name", "level", "region_id", "super_region_id"), with=F])

#Load populations from 1970 to present (change location_set_version_id, year_id, & decomp_step)
pops <- get_population(location_set_version_id = 443, year_id = c(1970:2019), sex_id = c(1,2), location_id = -1,
                       age_group_id = c(2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235), decomp_step = "step2") %>% as.data.table

#Load age_groups_ids
ages <- get_ids(table = "age_group")

#Split age_group_name to get age_start and age_end, hardcode non-standard age groups, and restrict to GBD-modeled age groups
ages[, age_start:=str_split_fixed(unique(ages$age_group_name), " to ", 2)[,1]]
ages[, age_end:=str_split_fixed(unique(ages$age_group_name), " to ", 2)[,2]]
ages[age_group_id == 2, c("age_start", "age_end") := .(0,0.019)]
ages[age_group_id == 3, c("age_start", "age_end") := .(0.02,0.082)]
ages[age_group_id == 4, c("age_start", "age_end") := .(0.083,0.999)]
ages[age_group_id == 235, c("age_start", "age_end") := .(95,99)]
ages <- ages[(age_group_id > 1 & age_group_id < 21) | (age_group_id > 29 & age_group_id < 33) | age_group_id == 235,]
ages[, age_group_name:=NULL]
ages <- ages[, lapply(.SD, as.numeric)]

#Merge ages and populations so pops has an age_start and age_end column
setkeyv(pops, "age_group_id")
setkeyv(ages, "age_group_id")
pops <- merge(pops, ages)

# read in data that has already been crosswalked for urbanicity
df <- read.xlsx("FILEPATH") %>% as.data.table
df[sex == "Male", sex_id := 1]
df[sex == "Female", sex_id := 2]
df[, level := NULL] # will be merging this column in later, removing now to avoid duplicating

#renaming vars for use with code
location_id<-"location_id"
year_id<-"year_id"
age_start<-"age_start"
age_end<-"age_end"
sex<-"sex_id"
estimate<-"mean"
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
cols <- c(location_id, year_id, age_start, age_end, sex, estimate, sample_size)
meta.cols <- setdiff(names(df), cols)
metadata <- df[, meta.cols, with=F]
dt <- df[, c("split_id", cols), with=F]

## Round age groups to the nearest age-group boundary
dt[age_start < 0.02, age_start := 0]
dt[age_start >= 0.02 & age_start < 0.083, age_start := 0.02]
dt[age_start >= 0.083 & age_start < 1, age_start := 0.083]
dt[age_start >= 1 & age_start < 5, age_start := 1]
dt[age_start >= 5, age_start := age_start - age_start %%5]
dt[age_start > 95, age_start := 95]
dt[age_end <= 0.02, age_end := 0.019]
dt[age_end > 0.02 & age_end <= 0.083, age_end := 0.082]
dt[age_end > 0.083 & age_end < 1, age_end := 0.999]
dt[age_end >= 1 & age_end < 5, age_end := 4]
dt[age_end >= 5, age_end := age_end - age_end %%5 + 4]
dt[age_end > 95, age_end := 99]

# can run into issues if age start and age end are both the same number under 1 that happens to be a cutoff value
stopifnot(dt$age_end > dt$age_start)

#Merge in region and super-region info
dt <- merge(dt, locs, by="location_id",all.x=T)

## Split into training and split set (but only working with split for lead)
dt[, need_split := 1]
dt[age_start == 0 & age_end == 0.019, need_split := 0]
dt[age_start == 0.02 & age_end == 0.082, need_split := 0]
dt[age_start == 0.083 & age_end == 0.999, need_split := 0]
dt[age_start == 1 & age_end == 4, need_split := 0]
dt[age_start > 0 & (age_end - age_start) == 4, need_split := 0]

training <- dt[need_split == 0]
split <- dt[need_split == 1]


##########################
## Expand rows for splits
##########################

split[age_start > 1, n.age := (age_end + 1 - age_start)/5]
split[age_start == 1, n.age := (age_end + 1)/5]
split[age_start == 0.083 & age_end != 0.999, n.age := ((age_end + 1)/5) + 1]
split[age_start == 0.083 & age_end == 0.999, n.age := 1]
split[age_start == 0.02 & age_end != 0.999 & age_end != 0.082, n.age := ((age_end + 1)/5) + 2]
split[age_start == 0.02 & age_end == 0.082, n.age := 1]
split[age_start == 0.02 & age_end == 0.999, n.age := 2]
split[age_start == 0 & age_end != 0.082 & age_end != 0.999 & age_end != 0.019, n.age := ((age_end + 1)/5) + 3]
split[age_start == 0 & age_end == 0.019, n.age := 1]
split[age_start == 0 & age_end == 0.082, n.age := 2]
split[age_start == 0 & age_end == 0.999, n.age := 3]

## Expand for age
split[, age_start_floor := age_start]
expanded <- rep(split$split_id, split$n.age) %>% data.table("split_id" = .)
split <- merge(expanded, split, by="split_id", all=T)
split[, age.rep := 1:.N - 1, by=.(split_id)]

split[age_start_floor > 1, (age_start):= age_start + age.rep * 5 ]
split[age_start_floor == 1 & age.rep > 0, (age_start):= age.rep * 5]
split[age_start_floor == 0.083 & age.rep == 1, (age_start):= 1]
split[age_start_floor == 0.083 & age.rep > 1, (age_start):= (age.rep - 1) * 5]
split[age_start_floor == 0.02 & age.rep == 1, (age_start):= 0.083]
split[age_start_floor == 0.02 & age.rep == 2, (age_start):= 1]
split[age_start_floor == 0.02 & age.rep > 2, (age_start):= (age.rep - 2) * 5]
split[age_start_floor == 0 & age.rep == 1, (age_start):= 0.02]
split[age_start_floor == 0 & age.rep == 2, (age_start):= 0.083]
split[age_start_floor == 0 & age.rep == 3, (age_start):= 1]
split[age_start_floor == 0 & age.rep > 3, (age_start):= (age.rep - 3) * 5]

split[age_start > 1, (age_end) :=  age_start + 4]
split[age_start == 1, (age_end) := 4]
split[age_start == 0.083, (age_end) := 0.999]
split[age_start == 0.02, (age_end) := 0.082]
split[age_start == 0, (age_end) := 0.019]

# read in bradmod output and create estimates for every age and sex group, then merge onto data to be split
estimates <- fread("FILEPATH")
estimates <- estimates[,list(age_lower, age_upper, pred_lower, pred_median, pred_upper)]
setnames(estimates, c("age_lower", "age_upper", "pred_median"), c("age_start", "age_end", "est"))
estimates[age_end > 1,age_end := age_end - 1]
estimates[age_end <= 1,age_end := age_end - 0.001]

split <- merge(split, estimates, by = c("age_start", "age_end"))

#Merge in populations
#first, check to make sure there's no column named 'population' already
if ("population" %in% names(split)) {
  split[, population:=NULL]
}

split <-  merge(split, pops[, c(all_vars[1:5], "population"), with=F], by = c(all_vars[1:5]),all.x=T)

#find ratio between estimate and original (unsplit) prevalence
split[, R:=est*population]
split[, R_group:=sum(R), by="split_id"]

#calculate final estimates and split sample size based on population
split[, pop_group:=sum(population), by = "split_id"]
split[, (estimate):=(get(estimate)*(R/R_group)*(pop_group/population))]
split[, sample_size:= sample_size*(population/pop_group)]

## Mark as split
split[, cv_split := 1]

#############################################
## Append training, merge back metadata, clean
#############################################

## Append training, mark cv_split
out <- rbind(split, training, fill=T)
out <- out[is.na(cv_split), cv_split := 0]

## Append on metadata
metadata <- metadata[,-("location_name"),with=F]
out <- merge(out, metadata, by="split_id", all.x=T)

## Clean
out <- out[, c(meta.cols, cols, "cv_split", "n.age", "region_id", "super_region_id"), with=F]
out[, split_id := NULL]

# Add age_group_id back in (particularly helpful to re-do since NHANES data was tabulated to have
# incorrect id for highest age group)
out[, age_group_id := NA]
out[, age_group_id := as.numeric(age_group_id)]
out[age_start == 0, age_group_id := 2]
out[age_start == 0.02, age_group_id := 3]
out[age_start == 0.083, age_group_id := 4]
out[age_start == 1, age_group_id := 5]
out[age_start > 1 & age_start < 80, age_group_id := (age_start/5) + 5]
out[age_start >= 80, age_group_id := (age_start/5) + 14]
out[age_start == 95, age_group_id := 235]

#save data
write.csv(out, "FILEPATH")
