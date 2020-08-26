####################################################################
## Proportion HIV due to IDU
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
functs <- c("get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids", "get_bundle_data", "save_crosswalk_version", "save_bundle_version", "get_bundle_version")
invisible(lapply(functs, function(x) source(paste0("FILEPATH", x, ".R"))))

mrbrt_functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function",
                  "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function",
                  "load_mr_brt_preds_function", "plot_mr_brt_function")
invisible(lapply(mrbrt_functs, function(x) source(paste0("FILEPATH", x, ".R"))))

source("FILEPATH")

# Set objects
bid<-385
dstep<-"step2"

# Download data and export for manual review
df<-get_bundle_data(bundle_id = bid, decomp_step = dstep)
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

#### Age-sex split using dismod pattern
ages <- get_age_metadata(12)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age_groups <- ages[age_start >= 10, age_group_id]

df<-df[age_end>99, age_end:=99]

## GET TABLES
sex_names <- get_ids(table = "sex")
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

region_pattern<-T
##Get super region information and merge on
super_region_dt <- get_location_metadata(location_set_id = 22)
super_region_dt <- super_region_dt[, .(location_id, super_region_id)]
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
age_pattern <- get_age_pattern(locs = locations, age_groups = age)
age_pattern<-age_pattern[, measure_id:=NULL]

if (region_pattern == T) {
  age_pattern1 <- copy(age_pattern)
  split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "super_region_id"))
} else {
  age_pattern1 <- copy(age_pattern)
  split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id"))
}

## GET POPULATION INFO
print("getting pop structure")
pop_structure <- get_pop_structure(locs = pop_locs, years = pop_years, age_group = age)
split_dt <- merge(split_dt, pop_structure, by = c("location_id", "sex_id", "year_id", "age_group_id"))

#####CALCULATE AGE SPLIT POINTS#######################################################################
## CREATE NEW POINTS
print("splitting data")
split_dt <- split_data(split_dt)
######################################################################################################

split_dt<-split_dt[mean>.99, mean:=.99]
dt <- copy(split_dt)
dt[, group := 1]
dt[, specificity := "age,sex"]
dt[, group_review := 1]
dt[, crosswalk_parent_seq := seq]
blank_vars <- c("lower", "upper", "effective_sample_size", "standard_error", "uncertainty_type", "uncertainty_type_value", "seq")
dt[, (blank_vars) := NA]
dt <- get_se(dt)
#dt <- col_order(dt)
dt[, note_modeler := paste0(note_modeler, "| age split using the super region age pattern", date)]
split_ids <- dt[, unique(id)]
dt <- rbind(original[!id %in% split_ids], dt, fill = T)
dt <- dt[, c(names(df), "crosswalk_parent_seq"), with = F]

m<-dt[sex=="Both"]
m<-m[, sex:="Male"]
f<-dt[sex=="Both"]
f<-f[, sex:="Female"]
split_dt<-rbind(m, f)
split_dt<-split_dt[, crosswalk_parent_seq:=seq]
split_dt<-split_dt[, seq:=NA]

dt<-dt[sex!="Both"]
final_dt<-rbind(dt, split_dt)

final_dt<-final_dt[!is.na(group), group_review:=1]
final_dt<-final_dt[sample_size<cases, cases:=NA]
final_dt<-final_dt[standard_error>1, standard_error:=NA]
final_dt<-final_dt[mean==.99, standard_error:=NA]

temp<-final_dt[age_start==0]
out<-final_dt[age_start>0]
temp<-temp[, crosswalk_parent_seq:=seq]
temp<-temp[, seq:=NA]

for (a in seq(15, 95, 5)) {
  temp2<-copy(temp)
  temp2<-temp2[, age_start:=a]
  temp2<-temp2[, age_end:=a+4]
  out<-rbind(out, temp2)
}

write.xlsx(out, "FILEPATH", sheetName="extraction")

bund<-save_bundle_version(bundle_id = bid, decomp_step = "step4", include_clinical = F)
save_crosswalk_version(bundle_version_id = bund$bundle_version_id, data_filepath = "FILEPATH", description = "Retain 0s, cap1, no min SS for split, no Wislon SE")


