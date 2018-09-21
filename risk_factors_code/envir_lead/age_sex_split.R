
#################### Space-Time Age-Sex Split ####################################################################################
##Purpose: Perform age-sex splitting using a template age-sex trend 
##################################################################################################################################

require(data.table)
require(dplyr)
require(stringr)
require(boot)

## OS locals
os <- .Platform$OS.type
if (os == "windows") {
  jpath <- "FILEPATH"
} else {
  jpath <- "FILEPATH"
}

## Resources for pulling in central data
source(paste0(jpath, "FILEPATH"))
source(paste0(jpath, "FILEPATH"))
source(paste0(jpath, "FILEPATH"))
source(paste0(jpath, "FILEPATH"))

#Load locations and restrict to relevant vars
locations <- get_location_metadata(gbd_round_id = 4, location_set_id = 22) #REMEMBER TO CHANGE THE GBD_ROUND_ID
locs <- copy(locations[, c("location_id", "location_name", "region_name", "level", "region_id", "super_region_id"), with=F])

#Load populations from 1970 to present
pops <- get_population(location_set_version_id = 149, year_id = c(1970:2016), sex_id = c(1,2), location_id = -1, 
                       age_group_id = c(2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235)) %>% as.data.table
pops[, process_version_map_id:=NULL]

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

#Load covariates relevant to lead
sdi <- get_covariate_estimates(covariate_name_short = "sdi")
setnames(sdi,"mean_value","sdi")
sdi <- sdi[,.(location_id, year_id, sdi)]

vehicles_pc <- get_covariate_estimates(covariate_name_short = "vehicles_2_plus_4wheels_pc")
setnames(vehicles_pc,"mean_value","vehicles_pc")
vehicles_pc <- vehicles_pc[,.(location_id, year_id, vehicles_pc)]

prop_urban <- get_covariate_estimates(covariate_name_short = "prop_urban")
#off-set values of 0 or 1 (for locations like Singapore) because they will be logit inputs
prop_urban[mean_value == 1, mean_value := 0.99999]
prop_urban[mean_value == 0, mean_value := 0.00001]
prop_urban[,lt_prop_urban := logit(mean_value)]
prop_urban <- prop_urban[,.(location_id, year_id, lt_prop_urban)]

outphase_smooth <- get_covariate_estimates(covariate_name_short = "lead_gas_smooth")
setnames(outphase_smooth, "mean_value", "outphase_smooth")
outphase_smooth <- outphase_smooth[,.(location_id, year_id, outphase_smooth)]

#Merge ages and populations so pops has an age_start and age_end column
setkeyv(pops, "age_group_id")
setkeyv(ages, "age_group_id")
pops <- merge(pops, ages)


# read in lead data and fill in year_id and location_id
df <- fread(paste0(jpath, "FILEPATH"), header=T)
df[,year_id := floor((as.numeric(year_end)+as.numeric(year_start))/2)]
fill_loc <- copy(locations[, c("location_id", "ihme_loc_id"), with=F])
df <- merge(df[,-("location_id"),with=F], fill_loc, by="ihme_loc_id")

#renaming vars for use with Hayley's code
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

#Varlists
location_ids <- c(location_id, "region_id", "super_region_id")
age_sex <- c(age_start, age_end, sex)
id_vars <- c(location_id, year_id)

#Create split id 
df[, split_id := 1:.N]

## Save original values
orig <- c(age_start, age_end, sex, estimate, sample_size)
orig.cols <- paste0("orig.", orig)
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

stopifnot(dt$age_end > dt$age_start)

#Merge in region and super-region info
dt <- merge(dt, locs, by="location_id",all.x=T)

## Split into training and split set
dt[, need_split := 1]
dt[age_start == 0 & age_end == 0.019 & (get(sex) %in% c(1,2)), need_split := 0]
dt[age_start == 0.02 & age_end == 0.082 & (get(sex) %in% c(1,2)), need_split := 0]
dt[age_start == 0.083 & age_end == 0.999 & (get(sex) %in% c(1,2)), need_split := 0]
dt[age_start == 1 & age_end == 4 & (get(sex) %in% c(1,2)), need_split := 0]
dt[age_start > 0 & (age_end - age_start) == 4 & (get(sex) %in% c(1,2)), need_split := 0]

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
split[, n.sex := ifelse(get(sex)==3, 2, 1)]

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

## Expand for sex
split[, sex_split_id := paste0(split_id, "_", age_start)]
expanded <- rep(split$sex_split_id, split$n.sex) %>% data.table("sex_split_id" = .)
split <- merge(expanded, split, by="sex_split_id", all=T)
split[get(sex)==3, (sex) := as.numeric(1:.N), by=sex_split_id]


# read in bradmod output and create estimates for every age and sex group, then merge onto data to be split
estimatesx <- fread(paste0(jpath,"FILEPATH"), header = T)
estimatesx <- estimatesx[,list(age_lower, age_upper, pred_lower, pred_median, pred_upper)]
estimatesx[,sex_id := 1]
estimatesy <- copy(estimatesx) 
estimatesy[,sex_id := 2]
sex_beta <- fread(paste0(jpath,"FILEPATH"), header = T)
sex_beta <- sex_beta[name=="beta_incidence_x_sex"]
adjust <- c("pred_lower","pred_median","pred_upper")
estimatesx[,(adjust) := lapply(.SD, function(x) x*exp(.5*sex_beta[,mean])), .SDcols=adjust]
estimatesy[,(adjust) := lapply(.SD, function(x) x*exp(-.5*sex_beta[,mean])), .SDcols=adjust]
estimatesx <- rbind(estimatesx, estimatesy)
setnames(estimatesx, c("age_lower", "age_upper", "pred_median"), c("age_start", "age_end", "est"))
estimatesx[age_end > 1,age_end := age_end - 1]
estimatesx[age_end <= 1,age_end := age_end - 0.001]

split <- merge(split, estimatesx, by = c("age_start", "age_end", "sex_id"))

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
out <- out[, c(meta.cols, cols, "cv_split", "n.sex", "n.age", "region_id", "super_region_id"), with=F]
out[, split_id := NULL]

# Add age_group_id back in
out[, age_group_id := as.numeric(age_group_id)]
out[age_start == 0, age_group_id := 2]
out[age_start == 0.02, age_group_id := 3]
out[age_start == 0.083, age_group_id := 4]
out[age_start == 1, age_group_id := 5]
out[age_start > 1 & age_start < 80, age_group_id := (age_start/5) + 5]
out[age_start >= 80, age_group_id := (age_start/5) + 14]
out[age_start == 95, age_group_id := 235]

# merge on lead covariates
out <- merge(out, sdi,by=c("location_id","year_id"),all.x=T)
out <- merge(out, prop_urban,by=c("location_id","year_id"),all.x=T)
out <- merge(out, outphase_smooth,by=c("location_id","year_id"),all.x=T)
out <- merge(out, vehicles_pc,by=c("location_id","year_id"),all.x=T)

# When point data is representative, copy over location's value for prop_urban, otherwise use the data to infer urbanicity
# When urbanicity unknown, copy over location's prop_urban as well
out[representative_name == "Nationally representative only" | representative_name == "Representative for subnational location only",
    urbanicity_type := "0"]
#added three decimal places
out[urbanicity_type == "2" | urbanicity_type == "Urban", lt_urban := 0.99999]
out[urbanicity_type == "3" | urbanicity_type == "Rural", lt_urban := 0.00001]
out[urbanicity_type == "Mixed/both" | urbanicity_type == "Suburban", lt_urban := 0.5]
out[grepl("IND_",ihme_loc_id),lt_urban := NA]

# take logit of prop_urban
out[, lt_urban := logit(lt_urban)]
out[is.na(lt_urban), lt_urban := lt_prop_urban]

#add in vars that will be needed for crosswalking
out[, me_name := "envir_lead_exp"]
out[, standard_error := as.numeric(standard_error)]
out[, variance := standard_error**2]
setnames(out, "mean", "data")

#mark representativeness in cv_subgeo
out[representative_name == "Nationally representative only" | 
      representative_name == "Representative for subnational location only" | 
      national_type == 1, cv_subgeo := 0]
out[is.na(cv_subgeo),cv_subgeo:=1]

#save data
write.csv(out, paste0(jpath, "FILEPATH"),row.names=F)
