##############################
## Purpose: First stage regression, second stage space-time smoothing. Data Processing and Bias Adjustment. 
##############################
sessionInfo()
rm(list=ls())
library(data.table)
library(haven)
library(lme4)
library(ggplot2)
library(splines)
library(parallel)
library(magrittr)


if (Sys.info()[1] == 'Windows') {
  username <- USERNAME
  root <- FILEPATH
  model_age <- 
  version <- 
  loop <- 
} else if (interactive()){
  
  username <- USERNAME
  root <- FILEPATH
  model_age <- 15
  version <- 
  loop <- 
  super_reg <- 
  
} else {
  username <- USERNAME
  root <- FILEPATH
  model_age <- commandArgs(trailingOnly = T)[1]
  version <- commandArgs(trailingOnly = T)[2]
  loop <- commandArgs(trailingOnly = T)[3]
  super_reg <- commandArgs(trailingOnly = T)[4]
}
print(model_age)
print(version)
print(loop)
print(super_reg)

if(super_reg == "Africa") super_reg <- "Sub-Saharan Africa"
if(super_reg == "central_europe_eastern_europe_central_asia") super_reg <- "Central Europe, Eastern Europe, and Central Asia"
super_reg1 <- super_reg
if(super_reg == "others") super_reg <- c("Southeast Asia, East Asia, and Oceania", "Latin America and Caribbean", "North Africa and Middle East", "South Asia")


## set global options
year_start <- 1950
year_end <- 2017


## set directories
mort_function_dir <- FILEPATH
central_function_dir <- FILEPATH
fert_function_dir <- FILEPATH
if (Sys.info()[1] == 'Windows') fert_function_dir <- FILEPATH

base_dir <- FILEPATH

jbase <- FILEPATH
data_dir <- FILEPATH
out_dir <- FILEPATH
param_dir <- FILEPATH
j_data_dir <- FILEPATH
diagnostic_dir <- FILEPATH
j_results_dir <- FILEPATH

  # in_dir <- paste0("J:/temp/xrkulik/")

## run functions
source(paste0(mort_function_dir, "get_age_map.r"))
source(paste0(mort_function_dir, "get_locations.r" ))
source(paste0(central_function_dir, "get_covariate_estimates.R"))
source(paste0(mort_function_dir, "get_spacetime_loc_hierarchy.R"))
source(paste0(fert_function_dir, "space_time.r"))
source(paste0(mort_function_dir, "get_proc_version.R"))
source(paste0(mort_function_dir, "get_best_versions.R"))


inv_logit <- function(x){
  return(exp(x)/(1+exp(x)))
}


## set model locs
model_locs <- data.table(get_locations(level="estimate", gbd_year = 2017))
model_locs <- model_locs[!grepl("SAU_", ihme_loc_id)]
model_locs <- model_locs[super_region_name %in% super_reg]

parents <- unique(substr(grep("_", model_locs$ihme_loc_id, value = T),1 ,3))

model_locs <- model_locs$location_id

## get DDM version for completeness designation
ddm_version <- get_proc_version(model_name = "ddm", model_type = "estimate", 
                                run_id = "best", gbd_year = 2017)

## read in data/mapping files
if(loop==1){
  input_data <- readRDS(paste0(j_data_dir, "asfr.RDs"))
}


## adding SBH split data for the second loop
if(loop == 2){
  input_data <- readRDS(paste0(j_data_dir, "asfr.RDs"))
  sbh <- rbindlist(mclapply(list.files(paste0(data_dir, 'sbh'), full.names = T), readRDS, mc.cores = 10))
  setnames(sbh, 'source', 'id')
  input_data <- rbind(input_data, sbh, fill = T, use.names = T)
  
  total_births <- rbindlist(mclapply(list.files(paste0(data_dir, "total_births"), full.names = T), fread, mc.cores = 10))
  input_data <- rbind(input_data, total_births, use.names = T, fill = T)
  
  split_locs <- rbindlist(mclapply(list.files(paste0(data_dir, "historic_locs"), full.names = T), fread, mc.cores = 10))
  input_data <- rbind(input_data, split_locs, use.names = T, fill = T)
}

input_data[ihme_loc_id == "MUS", val := max(val), by= c("ihme_loc_id", "year_id", "id", "nid", "age_group_id")] 
input_data[grepl("USA", ihme_loc_id) & source_type == "vr",source_type := "vr_complete"]

## read in and format a bunch of age and loc mapping files
age_map <- data.table(get_age_map())[,.(age_group_id, age_group_name_short)]
setnames(age_map, "age_group_name_short", "age")
loc_map <- data.table(get_locations(level="estimate", gbd_year = 2017))
loc_map <- loc_map[!grepl("SAU_", ihme_loc_id)]
nat_locs <- copy(loc_map)
reg_map <- copy(loc_map)
loc_map <- loc_map[,.(location_id, ihme_loc_id)]
nat_locs <- nat_locs[level==3| ihme_loc_id %in% c("CHN_44533", "CHN_361", "CHN_354",
                                                  "GBR_433", "GBR_434", "GBR_4636", "GBR_4749")]
nat_locs <- nat_locs[!ihme_loc_id %in% c("CHN", "GBR")]
nat_locs <- nat_locs$ihme_loc_id
reg_map <- reg_map[,.(location_id, ihme_loc_id, region_name, super_region_name)]

## read in popualtion and education and outliers
pop <- data.table(read.csv(paste0(jbase, "pop_input.csv")))
pop[,pvid := NULL]
female_edu <- as.data.table(read.csv(paste0(jbase, "fedu_cov_input.csv")))
female_edu[,pvid := NULL]
births <- data.table(read.csv(paste0(jbase, "dd_births.csv")))
births[,pvid := NULL]
outliers <- data.table(read.csv(paste0(j_data_dir, "outliered_sources.csv")))

conflict <- fread(paste0(jbase, "conflict_cov_input.csv"))

## data cleaning/formatting
input_data <- input_data[,.(nid, id, source_type, location_id, ihme_loc_id, year_id, age_group_id, asfr_data = val)]
input_data[is.na(nid), nid := paste0(id, '_', year_id)]
input_data <- merge(input_data, age_map, by="age_group_id", all.x=T)
input_data[, age_group_id := NULL]

## subset to relevant age
input_data <- input_data[age==model_age]
input_data <- input_data[year_id >=1950]

## outliering
outliers[,outlier := 1]
outliers <- outliers[,.(nid, ihme_loc_id, age, year_id, entire_timeseries, drop, outlier)]
outliers[,age := as.character(age)]

yearly <- outliers[!is.na(year_id)]
setnames(yearly, "outlier", "outlier1")
input_data[,nid := as.character(nid)]
input_data <- merge(input_data, yearly, by= c("ihme_loc_id", "nid", "year_id", "age"), all.x=T)
input_data[, entire_timeseries := NULL]
input_data[is.na(drop), drop := 0]
input_data <- input_data[drop != 1]
input_data <- input_data[,drop := NULL]

timeseries <- outliers[entire_timeseries ==1]
timeseries[,year_id := NULL]

input_data <- merge(input_data, timeseries, by = c("ihme_loc_id", "nid", "age"), all.x = T)
input_data[outlier1==1, outlier := 1]
input_data[,outlier1 := NULL]
input_data[is.na(outlier), outlier :=0]
input_data[is.na(drop), drop := 0]
input_data <- input_data[drop != 1]
input_data[,entire_timeseries := NULL]
input_data <- input_data[,drop := NULL]

input_data[asfr_data == 0, outlier := 1]

## model in scaled logit space
upper_bound <- quantile(input_data$asfr_data, 0.993)
lower_bound <- min(input_data$asfr_data)
input_data[asfr_data >= upper_bound, asfr_data := upper_bound - 0.000001]
input_data[asfr_data <= lower_bound, asfr_data := lower_bound + 0.000001]
input_data[,logit_asfr_data := logit((asfr_data - lower_bound) / (upper_bound - lower_bound))]

input_data[logit_asfr_data > 10 | logit_asfr_data < -12, outlier := 1]
input_data[asfr_data > 0.5, outlier := 1]

input_data[ihme_loc_id %in% c("IND_4842", "IND_4851", "IND_4865", "IND_4872") & grepl("vr", source_type) & year_id < 1990, outlier := 1]
input_data[ihme_loc_id %in% c("IND_4843", "IND_4844", "IND_4868") & grepl("vr", source_type) & year_id < 2010, outlier := 1]
input_data[ihme_loc_id %in% c("IND_4855", "IND_4862") & grepl("vr", source_type), outlier := 1]
input_data[ihme_loc_id %in% c("IND_4853", "IND_4856", "IND_4860", "IND_4861", "IND_4864", "IND_4875") & 
             grepl("vr", source_type) & year_id < 2010, outlier := 1]
input_data[ihme_loc_id == "IND" & source_type == "vr_incomplete_tb", outlier := 1]
input_data[ihme_loc_id == "BGD" & grepl("sbh", source_type), outlier := 1]
input_data[ihme_loc_id == "USA_531" & year_id >= 2004 & grepl("vr", source_type), outlier := 1]
input_data[ihme_loc_id == "KEN" & year_id > 2000 & source_type == "vr_incomplete_tb", outlier := 1]
input_data <- input_data[!(ihme_loc_id == "PER" & source_type == "vr_incomplete_tb")]
input_data[grepl("USA_", ihme_loc_id) & nid == 35021 & year_id == 1975, outlier := 1]
input_data[grepl("USA_", ihme_loc_id) & year_id == 1976 & nid == 350606, outlier := 1]

logit_bounds <- data.table(age = model_age, upper_logit = upper_bound, lower_logit = lower_bound)
write.csv(logit_bounds, paste0(out_dir, "logit_bounds_", model_age, super_reg1, ".csv"), row.names = F)

## merge on covarites
female_edu <- female_edu[,.(location_id, year_id, sex_id, age_group_id, mean_value)]
setnames(female_edu, c("mean_value"), c("female_edu_yrs"))
female_edu <- female_edu[sex_id==2]
female_edu <- merge(female_edu, age_map, by="age_group_id")
input_data <- merge(input_data, female_edu, by=c("location_id", "year_id", "age"), all.x=T)

## merge on asfr 20 and conflict death rate
if(model_age !=20){
  ## been having some issues reading this file in, going to implement a trycatch
  asfr20 <-  tryCatch({
    fread(paste0(base_dir, "results/gpr/compiled_summary_gpr.csv")) 
  }, error = function(e) {
    print("Re-trying file with 30 second rest")
    Sys.sleep(30)
    fread(paste0(base_dir, "results/gpr/compiled_summary_gpr.csv"))
  })


  asfr20 <- asfr20[age == 20]
  asfr20[,year_id := floor(year)]
  asfr20 <- merge(asfr20, loc_map, all.x=T, by="ihme_loc_id")
  asfr20 <- asfr20[,.(location_id, year_id, mean)]
  asfr20[,mean := logit(mean)]
  
  if(nrow(asfr20[is.na(mean)])>0) stop("Missing covariate values for ASFR age 20")
  
  setnames(asfr20, "mean", "asfr20")
  input_data <- merge(input_data, asfr20, all.x=T, by=c("location_id", "year_id"))
  
} else if (loop == 2 & model_age == 20){
  setnames(conflict, "mean_value", "conflict")
  
  input_data <- merge(input_data, conflict, by = c("ihme_loc_id", "year_id", "location_id"), all.x =T)
  input_data[is.na(conflict), conflict := 0]
}


fix_locs <- unique(input_data$location_id[input_data$source_type == "vr_complete"])
n <- nrow(input_data)

fix <- input_data[location_id %in% fix_locs]
fix[source_type %in% c("stat", "report_tabs", "dyb_other"), source_type := "vr_complete"]

input_data <- input_data[!location_id %in% fix_locs]
input_data <- rbind(input_data, fix)

if(nrow(input_data) != n) stop("you lost or gained rows in your vr source fix")

## do the same in incomplete vr locations
fix_locs <- unique(input_data$location_id[input_data$source_type == "vr_incomplete"])
n <- nrow(input_data)

fix <- input_data[location_id %in% fix_locs]
fix[source_type %in% c("stat", "report_tabs", "re"), source_type := "vr_incomplete"]

input_data <- input_data[!location_id %in% fix_locs]
input_data <- rbind(input_data, fix)

if(nrow(input_data) != n) stop("you lost or gained rows in your vr source fix")


input_data <- input_data[!(ihme_loc_id == "AUS" & source_type == "unknown")]

input_data <- input_data[!(ihme_loc_id == "DJI" & id %like% "papfam")]

input_data[ihme_loc_id == "SRB" & year_id %in% c(2006,2008:2014) & source_type == "vr_complete", source_type := "vr_incomplete"]

input_data[ihme_loc_id == "NGA" & nid == 33188, source_type := "vr_incomplete"]
input_data[ihme_loc_id=="PHL" & source_type == "vr_incomplete", id := "report_tabs_and_vendors"]

input_data <- unique(input_data)

write.csv(input_data, paste0(data_dir, "input_data_", super_reg1, model_age, ".csv"), row.names = F)

#################
## Stage 1 Model
################


## designate the internal "reference source" for the regression aka it's absorbed in the intercept
other_sources <- unique(input_data$source_type[input_data$source_type != "vr_complete"])
input_data[,source_type:=factor(source_type,levels=c("vr_complete", other_sources),ordered=FALSE)]

## concatenate to get a loc_source RE
input_data[,loc_source := paste(id, location_id, sep="_")]

knotsdf <- data.table(region_name = c(rep("High-income", 5), rep("Sub-Saharan Africa", 5), rep("others", 5), rep("Central Europe, Eastern Europe, and Central Asia", 5)), age = rep(seq(25, 45, 5), 4), 
                      knot = c(NA, -2.25, -2, -2.25, -2.25, -1.75, -1.25, -1.3, -1.5, -1.75, -1.5, -1.3, -1.3, -2, -2.5, -1.5, -2, -1.75, -1.75, -2))
knots <- knotsdf[age == model_age & region_name == super_reg1, knot]

if(length(knots) == 0) knots <- NULL
if(length(knots) != 0){
  if(is.na(knots)) knots <- NULL
}  


input_data <- merge(input_data ,reg_map, by=c("ihme_loc_id", "location_id"), all.x=T)
input_data <- input_data[super_region_name %in% super_reg]


if(model_age == 20){
  form <-  paste0("logit_asfr_data ~ female_edu_yrs  + (1|loc_source)")
} else if (model_age != 20 & super_reg== "High-income" ) {
  form <-  paste0("logit_asfr_data ~  bs(asfr20, degree = 1, knots = ", paste(knots, collapse = ", "), ") + (1|loc_source)")
} else {
  form <-  paste0("logit_asfr_data ~ female_edu_yrs +  bs(asfr20, degree = 1, knots = ", paste(knots, collapse = ", "), ") + (1|loc_source)")
}


mod <- lmer(formula=as.formula(form), data=input_data[outlier == 0])
summary(mod)

spline_mod <- grepl('bs', form)


######################
## Data Adjustment
######################

## now extract REs by loc_source
loc_source_re <- data.frame(ranef(mod)$loc_source)
loc_source_re$loc_source <- rownames(loc_source_re)
loc_source_re <- data.table(loc_source_re)
setnames(loc_source_re, "X.Intercept.", "loc_source_re")

ref_loc_source_re <- copy(loc_source_re)
setnames(ref_loc_source_re, c("loc_source_re"), c("ref_loc_source_re"))

input_data[ihme_loc_id == "PHL" & grepl("vr", source_type), source_type := "vr_incomplete"]
input_data[ihme_loc_id == "PAN" & source_type == "vr_incomplete" & year_id > 1975, source_type := "vr_complete"]
input_data[ihme_loc_id == "THA" & source_type == "vr_incomplete" & year_id > 1966, source_type := "vr_complete"]
input_data[ihme_loc_id == "COL" & source_type == "vr_complete", source_type := "vr_incomplete"]
input_data[ihme_loc_id == "PRY" & year_id > 2010 & source_type == "vr_incomplete", source_type := "vr_complete"]

## designate reference sources
all_sources <- unique(input_data[,.(ihme_loc_id, location_id, source_type, outlier)])
all_sources <- all_sources[outlier == 0]
ref_source_list <- copy(all_sources)
complete_vr_locs <- ref_source_list[grepl("vr_complete", source_type)]$ihme_loc_id

## choose vr_complete when it's there
ref_source_list <- ref_source_list[(grepl("vr_complete", source_type) & ihme_loc_id %in% complete_vr_locs) | !ihme_loc_id %in% complete_vr_locs]

## chose all CBH sources by default if no vr_complete
ref_source_list <- ref_source_list[grepl("vr_complete", source_type) | grepl("cbh", source_type)]

## 1/27/18 tracking sheet, make refernece agnostic
ref_source_list <- ref_source_list[!ihme_loc_id %in% c("BFA", "BWA", "KEN", "IDN", "CIV", "COG")]
if(model_age == 45) ref_source_list <- ref_source_list[!ihme_loc_id %in% c("TUR")]

## if locations that have data don't have a reference source designation from above, make all sources 
## "reference", i.e. agnostic by reference source
data_locs <- unique(input_data$ihme_loc_id[!is.na(input_data$asfr_data)])
missing_locs <- data_locs[!data_locs %in% unique(ref_source_list$ihme_loc_id)]
agnostic_locs <- all_sources[ihme_loc_id %in% missing_locs]
ref_source_list <- rbind(ref_source_list, agnostic_locs)

##########################################
#### custom reference source designations
##########################################

# 
# ref_source_list[location_id == 44533, source_type := "census_tab"]
ref_source_list[location_id == 139, source_type := "other_survey_cbh"]
# as per Rafael
if(loop == 1) ref_source_list[ihme_loc_id == "MEX", source_type := "dhs_cbh_tab"]
if(loop == 2){
  ref_source_list[ihme_loc_id == "MEX", source_type := "mics_sbh"]
  ref_source_list[grepl("MEX_", source_type), source_type := "other_survey_sbh"]
} 
ref_source_list[ihme_loc_id == "CIV", source_type := "dhs_cbh"]
ref_source_list[ihme_loc_id == "BGD", source_type := "dhs_cbh"]

## conveninece function to add reference sources
add_ref_source <- function(data, ihme_loc_ids, source_types, subnat = F){
  if(subnat == F) {
    add <- unique(data[ihme_loc_id %in% ihme_loc_ids & source_type %in% source_types, .(ihme_loc_id, location_id, source_type, outlier)])
  } else if (subnat == T){
    add <- unique(data[grepl(ihme_loc_ids, ihme_loc_id) & source_type %in% source_types, .(ihme_loc_id, location_id, source_type, outlier)])
  }
  return(rbind(ref_source_list, add))
}

ref_source_list <- add_ref_source(input_data, c("BFA", "GIN", "NER", "SEN", "SWZ"), "census_tab")
ref_source_list <- add_ref_source(input_data, c("BDI", "BWA", "CAF", "CIV", "GAB", "GMB", "LBR", "MOZ", "NAM", "STP", "SWZ", "ZMB"), 
                                  "census_tab_sbh")

ref_source_list <- add_ref_source(input_data, "AZE", c("vr_incomplete", "vr_incomplete_tb"))
ref_source_list <- add_ref_source(input_data, "ARM", c("vr_incomplete", "vr_incomplete_tb"))
ref_source_list <- add_ref_source(input_data, "BEN", c("dhs_sbh", "other_survey_sbh", "wsf_sbh"))
ref_source_list <- add_ref_source(input_data, "BGD", c("vr_incomplete", "vr_incomplete_tb", "wfs_cbh"))
ref_source_list <- add_ref_source(input_data, "BRA", c("census_sbh", "dhs_sbh", "other_survey_sbh"))
ref_source_list <- add_ref_source(input_data, "CAF", c("census_sbh", "census_tab_sbh"))
ref_source_list <- add_ref_source(input_data, "CHN_44533", c("census_tab", "survey_unknown_recall_tab"))
ref_source_list <- add_ref_source(input_data, "COD", "mics_sbh")
ref_source_list <- add_ref_source(input_data, "CMR", "wfs_sbh")
ref_source_list <- add_ref_source(input_data, "CPV", "dhs_sbh")
ref_source_list <- ref_source_list[!(ihme_loc_id == "DJI" & source_type == "vr_incomplete_tb")]
ref_source_list[ihme_loc_id == "DZA", source_type := "mics_cbh"]
ref_source_list <- add_ref_source(input_data, "DZA", "dyb_other")
ref_source_list <- add_ref_source(input_data, "ECU", c("census_sbh", "rhs_sbh", "dhs_sbh"))
ref_source_list <- add_ref_source(input_data, "FJI", c("vr_incomplete", "vr_incomplete_tb"))
ref_source_list <- add_ref_source(input_data, "GEO", c("vr_incomplete", "vr_incomplete_tb"))
ref_source_list <- add_ref_source(input_data, "IND", c("sample_registration"), subnat = T)
ref_source_list <- ref_source_list[!(grepl("IND", ihme_loc_id) & source_type == "other_survey_cbh")]
ref_source_list <- add_ref_source(input_data, c("IND_44538", "IND_44539","IND_44540"), "other_survey_cbh")
if(loop == 2) ref_source_list[ihme_loc_id == "IND_4849", source_type := "vr_incomplete_tb"]
ref_source_list <- add_ref_source(input_data, "KAZ", c("vr_incomplete", "vr_incomplete_tb"))
ref_source_list <- ref_source_list[!(ihme_loc_id == "KEN" & source_type == "vr_incomplete_tb")]
ref_source_list <- add_ref_source(input_data, "KOR", c("other_survey_recall", "wfs_cbh", "wfs_sbh"))
ref_source_list[ihme_loc_id == "MAR", source_type := "dhs_cbh"]
ref_source_list[ihme_loc_id == "MDG", source_type := "dhs_cbh"]
ref_source_list <- add_ref_source(input_data, "MDA", c("vr_incomplete", "vr_incomplete_tb"))
ref_source_list <- add_ref_source(input_data, "MDV", c("dhs_cbh", "dhs_sbh"))
ref_source_list <- add_ref_source(input_data, "MNG", c("stat", "vr_incomplete", "vr_incomplete_tb"))
if(model_age == 45) ref_source_list <- add_ref_source(input_data, "MNG", c("vr_incomplete","vr_incomplete_tb"))
ref_source_list <- add_ref_source(input_data, "JAM",c("vr_incomplete","vr_incomplete_tb"))
if(model_age >= 20) ref_source_list <- add_ref_source(input_data, "LKA", "dyb_other")
ref_source_list <- add_ref_source(input_data, "NIC", c("census_sbh", "dhs_sbh", "other_survey_sbh", "rhs_sbh"))
ref_source_list <- add_ref_source(input_data, "NPL", "mics_sbh")
ref_source_list <- ref_source_list[!(ihme_loc_id == "NGA" & source_type == "mis_cbh")]
ref_source_list <- ref_source_list[!(ihme_loc_id == "PAK" & source_type == "wfs_cbh" )]
ref_source_list <- add_ref_source(input_data, "PAN", c("vr_incomplete","vr_incomplete_tb"))
ref_source_list <- ref_source_list[!(ihme_loc_id == "PER" & source_type == "other_survey_cbh")]
ref_source_list <- ref_source_list[!(ihme_loc_id == "PHL" & source_type == "vr_complete")]
ref_source_list <- add_ref_source(input_data, "PHL", c("dhs_cbh", "survey_unknown_recall_tab"))
if(loop ==2) ref_source_list <- ref_source_list[!(ihme_loc_id == "PRY" & source_type == "vr_incomplete")]
ref_source_list <- add_ref_source(input_data, "RWA", c("mis_sbh", "dhs_sbh", "wfs_sbh"))
ref_source_list <- add_ref_source(input_data, "SDN", "dhs_sbh")
ref_source_list <- add_ref_source(input_data, "SLV", c("vr_incomplete","vr_incomplete_tb"))
ref_source_list <- add_ref_source(input_data, "STP", c("dhs_cbh", "mics_cbh"))
ref_source_list <- add_ref_source(input_data, "TCD", "mics_sbh")
ref_source_list <- add_ref_source(input_data, "TGO", "dhs_sbh")
ref_source_list <- add_ref_source(input_data, "TLS", "dhs_sbh")
ref_source_list <- add_ref_source(input_data, "TUN", c("vr_incomplete","vr_incomplete_tb"))
ref_source_list <- ref_source_list[!(ihme_loc_id == "TUR" & source_type == "other_survey_cbh")]
ref_source_list <- add_ref_source(input_data, "TUR", c("vr_incomplete","vr_incomplete_tb"))
ref_source_list <- add_ref_source(input_data, "TTO", c("vr_incomplete","vr_incomplete_tb", "other_survey_sbh"))
ref_source_list <- add_ref_source(input_data, "UKR", c("vr_incomplete","vr_incomplete_tb"))
ref_source_list <- ref_source_list[!(ihme_loc_id == "WSM" & source_type %in% c("vr_incomplete", "vr_incomplete_tb"))]
ref_source_list <- add_ref_source(input_data, "YEM", "dhs_sbh")
ref_source_list <- add_ref_source(input_data, "VNM", "mics_sbh")
ref_source_list <- add_ref_source(input_data, "KGZ", c("vr_incomplete", "vr_incomplete_tb"))
ref_source_list <- add_ref_source(input_data, "PRY", c("dhs_cbh", "mics_cbh", "rhs_cbh", "wfs_cbh")) 
ref_source_list <- add_ref_source(input_data, "LBY", c("vr_incomplete_tb"))
ref_source_list <- ref_source_list[!(ihme_loc_id == "PHL" & source_type == "wfs_cbh")]
idn_sub <- paste0("IDN_", c(4710,4711,4712,4714,4716,4720,4721,4722,4723,4724,4726,4727,4728,4730,4731,4732,4735,4736,4739,4740,4741,4742))
ref_source_list <- ref_source_list[!(ihme_loc_id %in% idn_sub & source_type != "dhs_cbh")]

## in case before the custom designations you had more than one row/source_type, you don't want completely duplicate rows
ref_source_list <- unique(ref_source_list)

## reference sources might be different by age
ref_source_list[,age := model_age]

## save the reference source list for loop 2 different regions for graphing purposes
if(loop == 2) write.csv(ref_source_list, paste0(out_dir, "reference_sources_", super_reg1, model_age, ".csv"), row.names = F)

ref_source_list[,age := NULL]
#####################################
## end custom ref source designations
######################################

## get reference loc-source random effects
ref_re <- merge(ref_source_list, unique(input_data[,.(ihme_loc_id, source_type, loc_source, location_id)]), all.x = T, by=c("ihme_loc_id", "source_type", "location_id"))
ref_re <- merge(ref_re, loc_source_re, by = "loc_source", all.x=T)
## because of really strange error on cluster
ref_re <- ref_re[!is.na(loc_source_re)]
ref_re <- ref_re[,.(ref_loc_source_re = base::mean(loc_source_re, na.rm =T)), by =c("ihme_loc_id", "location_id")]

## merging on reference loc-source REs
input_data <- merge(input_data, ref_re, by = c("ihme_loc_id", "location_id"), all.x = T)

## merging on the loc-source REs and source_type FEs of the actual data point
input_data <- merge(input_data, loc_source_re, by = "loc_source", all.x=T)

## making the complete vr adjustment 0, to fix/revisit
input_data[grepl("vr_complete", source_type), ref_loc_source_re := 0]
input_data[grepl("vr_complete", source_type), loc_source_re := 0]


## making sure nothing is missing
if(nrow(input_data[is.na(ref_loc_source_re) & outlier == 0]) > 0) {
  test <- input_data[is.na(ref_loc_source_re) & outlier == 0]
  print(head(test))
  stop("missing reference loc source random effects")
}
if(nrow(input_data[is.na(loc_source_re) & outlier == 0]) > 0) stop("missing loc source random effects")


## doing the actual adjustment
input_data[outlier == 1, adjusted_logit_asfr_data := logit_asfr_data]
input_data[outlier == 0, adjustment_factor :=  (ref_loc_source_re - loc_source_re)]

## making incomplete_vr 0 if it's being adjusted down
input_data[grepl("vr_incomplete", source_type) & adjustment_factor <0, adjustment_factor := 0]

## 1/25/18 change per Chris-- if there's more than one reference source, instead of adjusting both to the average,
## adjust neither 
ref_source_list[,outlier := NULL]
ref_source_list[, ref_source := 1]
ref_source_list <- unique(ref_source_list)
input_data <- merge(input_data, ref_source_list, by= c("ihme_loc_id", "location_id", "source_type"), all.x=T)
input_data[ref_source ==1, adjustment_factor := 0]

input_data[outlier == 0, adjusted_logit_asfr_data := logit_asfr_data + adjustment_factor]

if(nrow(input_data[is.na(adjusted_logit_asfr_data)]) > 0) stop("missing adjusted data")


####################
## Make Pred
####################


### create initial pred template
compare_manual_pred <- F
### merge on standard covariates needed for predictions
pred <- data.table(expand.grid(year_id = year_start:year_end, location_id = model_locs, age = model_age))
input_data[,age := as.numeric(as.character(age))]
pred[,age := as.numeric(as.character(age))]
female_edu[,age := as.numeric(age)]
pred <- merge(pred, loc_map, all.x=T, by="location_id")
pred <- merge(pred, female_edu, by=c("age", "year_id", "location_id"), all.x=T)
if (model_age == 20 & loop == 2){
  pred <- merge(pred, conflict, by = c("ihme_loc_id", "year_id", "location_id"), all.x =T)
  pred[is.na(conflict) & year_id < 1970, conflict := 0] ## not uplaoded before 1970
}
if(model_age != 20) pred <- merge(pred, asfr20, by=c("location_id", "year_id"), all.x=T)
#pred[, grep("bs|Intercept", names(fixef(mod)), invert = T, value = T) := 0] # predict as if VR
pred[, loc_source := 'pred']
pred[, stage1_pred_no_re := predict(mod, newdata = pred, re.form = ~0, allow.new.levels = T)] # predict without random effects
pred[, loc_source := NULL]



if(compare_manual_pred){
  
  
  ### extract model_type_specific betas
  model_vars <- tstrsplit(form, "~")[[2]] %>% tstrsplit(., '\\+') %>% unlist 
  model_vars <- gsub(" ", "", model_vars) %>% grep("source", ., invert = T, value = T)
  
  if (spline_mod) {
    
    extract_spline_coef <- function(mod, varname, knots, input_data) {
      
      ## extracting spline basis coeffs
      coefs <- fixef(mod)[grep("bs|Intercept", names(fixef(mod)))]
      coefs <- data.table(coef = names(coefs), value = coefs)
      coefs[, coef := gsub('bs\\(.+\\)', paste0(varname, '_'), coef)]
      coefs[, coef := gsub("\\(Intercept\\)", "int", coef)]
      coefs <- dcast(coefs, ...~coef, value.var = 'value')
      
      betas <- data.table(varname = varname)
      
      minx <- min(input_data[outlier == 0, get(varname)])
      maxx <- max(input_data[outlier == 0, get(varname)])

      
      ## converting to conventional betas
      if(is.null(knots)) {
        
        betas[, paste0(varname, "_beta_") := (coefs[, paste0(varname, "_"), with = F] - coefs[, int])/(maxx - minx)]
        betas[, paste0(varname, "_int_") := coefs[, int] - get(paste0(varname, "_beta_"))*minx]
        
      } else {
        
        for(piece in 1:(length(knots)+1)) {
          
          if(piece == 1) {
            ## discuss with rachel whether should be using range of cov in input data versus range of cov in pred template to calculate betas
            betas[, paste0(varname, "_beta_", 1) := (coefs[, paste0(varname, "_1"), with = F] - coefs[, int])/(knots[1]-minx)]
            betas[, paste0(varname, "_beta_", 0) := get(paste0(varname, "_beta_1"))]
            betas[, paste0(varname, "_int_", 0:1) := coefs[, int] - (get(paste0(varname, "_beta_1"))*minx)]
            
          } else if (piece == length(knots) + 1) {
            betas[, paste0(varname, "_beta_", piece) := (coefs[, paste0(varname, "_", piece), with = F] - (coefs[, paste0(varname, "_", piece-1), with = F]))/(maxx - knots[piece - 1])]
            betas[, paste0(varname, "_beta_", piece + 1) := get(paste0(varname, "_beta_", piece))]
            betas[, paste0(varname, "_int_", piece:(piece+1)) := coefs[, get(paste0(varname, "_", (piece-1)))] - (get(paste0(varname, "_beta_", piece)) * knots[piece - 1])]
            
          } else {
            betas[, paste0(varname, "_beta_", piece) := (coefs[, paste0(varname, "_", piece), with = F] - coefs[, paste0(varname, "_", piece-1), with = F])/(knots[piece]-knots[piece - 1])]
            betas[, paste0(varname, "_int_", piece) := get(paste0(varname, "_beta_", piece)) * (minx - knots[piece - 1])]
            
          } 
        }
      }
      
      
      
      betas <- melt(betas, id.vars = 'varname', measure = patterns("beta" ,"int"), variable.name = 'coeff', value.name = c(paste0(varname, "_beta"), paste0(varname, "_int")), variable.factor = F) %>% .[order(varname, coeff)]
      bins <- cut(seq(-1000, 1000, 1), breaks = c(-1e4, min(input_data[outlier == 0, varname, with = F]), knots, max(input_data[outlier == 0, varname, with = F]), 1e4)) %>% unique %>% sort
      comps <- c(rep(min(input_data[outlier == 0, get(varname)]), 2), knots, max(input_data[outlier == 0, get(varname)])) %>% sort
      
      betas <- betas[order(coeff)]
      
      if(is.null(knots)){
        
        betas[, mergid := 1]
        
        tempbins <- data.table(mergid = rep(1, length(bins)))
        tempbins[, tbins := bins]
        setnames(tempbins, "tbins", paste0(varname, "_bin"))
        
        betas <- merge(betas, tempbins, by = 'mergid')
        
      } else {
        
        betas[, paste0(varname, "_bin") := bins]
        betas[, paste0(varname, "_comp")  := comps]
        
      }
      
      return(betas[, grep("beta|bin|comp|int", names(betas), value = T), with = F])
      
    }
    spline_vars <- grep("bs", model_vars, value = T) %>% tstrsplit(., ",") %>% .[[1]] %>% gsub("bs\\(", "", .)
    spline_coefs <- lapply(spline_vars, extract_spline_coef, mod = mod, knots = knots, input_data = input_data) %>% rbindlist(., use.names = T, fill = T)
  
    
    for (spline_var in spline_vars) pred[, paste0(spline_var, "_bin") := cut(get(spline_var), breaks = c(-1e4, min(input_data[outlier == 0, get(spline_var)]), 
                                                                                                         knots, max(input_data[outlier == 0, get(spline_var)]),
                                                                                                                    1e4))]
    pred <- merge(pred, spline_coefs, by = grep('bin', names(spline_coefs), value = T), all.x = T)
   # pred[, grep('bin', names(pred), value = T) := NULL]
    
    for(spline_var in spline_vars) pred[, paste0(spline_var, "_eff") := asfr20_int + asfr20_beta*asfr20]
    
  }
  
  if(exists("spline_vars")) linear_vars <- grep("bs", model_vars, invert = T, value = T) else linear_vars <- model_vars
  
  if(length(linear_vars) > 0) {
    
    fixed_effects <- fixef(mod) %>% .[grep("source|bs", names(.), invert = T, value = T)]
    names(fixed_effects) <- gsub("\\(Intercept\\)", "int", paste0(names(fixed_effects), "_beta"))
    fixed_effects <- data.frame(fixed_effects)
    fixed_effects$varname <- rownames(fixed_effects)
    fixed_effects <- dcast(data.table(fixed_effects), 1~varname, value.var = 'fixed_effects') %>% .[, grep("\\.",names(.), invert = T, value = T), with = F]
    
    fe_merge <- data.table(ihme_loc_id = unique(pred$ihme_loc_id))
    for(col in names(fixed_effects)) fe_merge[, (col) := fixed_effects[, col, with = F]]
    
    pred <- merge(pred, fe_merge, by = 'ihme_loc_id', all.x = T)
    for(linear_var in linear_vars) pred[, paste0(linear_var, '_eff') := get(linear_var)*get(paste0(linear_var, '_beta'))]
    
  }
  
  ## making the actual prediction-- complete_vr is the absorbed source, so by 
  ## not including a source_type intercept we are predicting as if everything is complete vr
  
  pred[, manual_stage1_pred_no_re := rowSums(.SD), .SDcols = c(grep("eff|int_beta", names(pred), value = T))]
  #pred[, grep("eff|int|beta|comp", names(pred), value = T) := NULL] # clearing intermediate columns
  
  coefs <- copy(pred)
  coefs <- unique(coefs[,.(asfr20_bin, asfr20_beta, asfr20_int, female_edu_yrs_beta, int_beta,age, region_name = super_reg1)])
  setnames(coefs, c("int_beta"), c("intercept"))
  
  write.csv(coefs, paste0(diagnostic_dir, super_reg1, model_age, "_coefs.csv"), row.names=F)
  
  
}


 
pred <- merge(pred, input_data[,.(location_id, year_id, age, nid, asfr_data, logit_asfr_data,id, outlier, source_type, loc_source, adjusted_logit_asfr_data, adjustment_factor)],
              by=c("year_id", "age", "location_id"), all = T)


if(nrow(pred[is.na(stage1_pred_no_re)]) >0 ) stop("missing stage 1 predictions")


#####################################################
## Set Parameters
## based on data density
## values from age-sex model, maybe should be changed
####################################################

params <- copy(pred)
params <- params[!is.na(asfr_data) & outlier ==0]
params <- params[,.(ihme_loc_id, age, year_id, nid, asfr_data, outlier, id, source_type, location_id, age_group_id)]

vr <- copy(params)
vr <- vr[grepl("vr", source_type) | source_type == "sample_registration"]

vr <- merge(vr, births, all.x = T, by= c("location_id", "age_group_id", "year_id"))
vr[,dd := births /100]
vr[dd >= 1, dd := 1]
vr <- vr[,.(dd = sum(dd)), by =c("ihme_loc_id", "age", "source_type")]

vr[,dd_source := source_type]
vr[dd_source == "sample_registration", dd_source := "vr_incomplete"]
vr[,source_type := NULL]

nonvr <- copy(params)
nonvr <- nonvr[!grepl("vr", source_type) & source_type != "sample_registration"]
nonvr[grepl("cbh", source_type),dd_source := "cbh"]
nonvr[grepl("sbh", source_type),dd_source := "sbh"]

nonvr[is.na(dd_source), dd_source := "other"]
nonvr <- unique(nonvr, by= c("ihme_loc_id", "age", "dd_source", "nid"))
nonvr <- subset(nonvr)[,.(dd = .N), by = c("ihme_loc_id", "age", "dd_source")]

dd <- rbind(vr, nonvr)

write.csv(dd, paste0(diagnostic_dir, "data_density_by_source", model_age, "_", super_reg1, ".csv"), row.names=F)


## use this equation: 
## data_density = complete_vr_deaths + (2 * cbh_sources) + (0.25 * sbh_sources) + (0.5 * incomplete_vr_deaths)
## weight "other" as 1 for the moment 
dd[dd_source == "vr_incomplete", dd := dd * 0.5]
dd[dd_source == "cbh", dd := dd * 2]
dd[dd_source == "sbh", dd := dd * 0.25]

setkey(dd, ihme_loc_id, age)
dd <- dd[,.(dd = sum(dd)), by=key(dd)]

## dealing with data density 0
locs <- data.table(get_locations(level="estimate", gbd_year=2017))
locs <- locs[super_region_name %in% super_reg]
locs <- locs[,.(ihme_loc_id)]
dd <- merge(dd, locs, all = T, by=c("ihme_loc_id"))
dd[,age := model_age]
dd[is.na(dd), dd := 0]

params <- copy(dd)

params[dd>=50, lambda := 0.2]
params[dd>=50, zeta := 0.99]
params[dd>=50, scale := 5]

params[dd<50 & dd>=30, lambda := 0.4]
params[dd<50 & dd>=30, zeta := 0.9]
params[dd<50 & dd>=30, scale := 10]

params[dd<30 & dd>=20, lambda := 0.6]
params[dd<30 & dd>=20, zeta := 0.8]
params[dd<30 & dd>=20, scale := 15]

params[dd<20 & dd>=10, lambda := 0.8]
params[dd<20 & dd>=10, zeta := 0.7]
params[dd<20 & dd>=10, scale := 15]

params[dd<=10, lambda := 1]
params[dd<=10, zeta := 0.6]
params[dd<=10, scale := 15]

params[,amp2x := 1]
params[,best:=1]

params <- params[,.(ihme_loc_id, scale, amp2x, lambda, zeta, best, dd)]
params <- unique(params, by=c())

#############################
## Custom parameter choosing
##############################

## make changes as per Haidong to test theory about weird stage 2
params[ihme_loc_id %in% c("BMU", "TKM", "GEO"), lambda := 0.2]
params[ihme_loc_id %in% c("BMU", "TKM", "GEO"), zeta := 0.9]

## as per Chris 1/14/2018
params[ihme_loc_id == "CAF", lambda := 0.4]
params[ihme_loc_id == "COM", lambda := 0.5]

## 1/27 from population tracking sheet
params[ihme_loc_id == "BEN", lambda := 0.5]
ceb_locs <- c("BDI", "BWA", "CAF", "CIV", "GAB", "GMB", "LBR", "MOZ", "NAM", "SEN", "STP", "SWZ", "ZMB","BFA", "GIN", "NER", "SEN", "SWZ", "CMR", "TZA", "MRT")
if(model_age == 20) params[ihme_loc_id %in% ceb_locs, lambda := 0.4]
if(model_age == 20) params[ihme_loc_id %in% c("COM"), lambda := 0.2]
params[ihme_loc_id == "IND", lambda := 0.2]

params[ihme_loc_id == "TON", zeta := 0.8]
params[ihme_loc_id == "SGP", scale := 1]
params[ihme_loc_id == "JPN", scale := 1]
params[ihme_loc_id == "ETH", lambda := 0.3]
params[ihme_loc_id == "JAM", lambda := 0.2]
params[ihme_loc_id == "YEM", lambda := 0.4]
params[ihme_loc_id == "CHN_44533", lambda := 0.2]
params[ihme_loc_id == "CHN_44533", zeta := 0.95]
params[ihme_loc_id == "CHN_44533", scale := 2]

if(model_age >=40) params[ihme_loc_id %in% c("BGR", "CHE"), lambda := 0.1]

write.csv(params, paste0(param_dir, "asfr_params_age_", model_age,"_", super_reg1, ".txt"), row.names=F)
write.csv(params, paste0(j_results_dir, "asfr_params_age_", model_age, "_", super_reg1, ".txt"), row.names=F)


################################
## Stage 2: Space-Time Smoothing
################################
if(model_age != 20){
  pred[,resid := stage1_pred_no_re - logit_asfr_data]
  pdf(paste0(diagnostic_dir, "resid_to_asfr20", model_age, "_loop_", loop, ".pdf"))
  p <- ggplot(data = pred, aes(x = asfr20, y=resid)) + geom_point(alpha = 0.3, size = 0.5) +
    labs(title = paste0("Loop ", loop, " Age ", model_age))
  print(p)
  dev.off()
  pred[,resid := NULL]
}


## calculate residual
st_data <- copy(pred)
st_data[,resid := stage1_pred_no_re - adjusted_logit_asfr_data]
st_data[outlier == 1, resid := NA]

## merge on fake regions
regs <- get_spacetime_loc_hierarchy(old_ap = F, gbd_year = 2017)
regs <- regs[!grepl("SAU_", ihme_loc_id)]
st_data <- merge(st_data, regs, by=c("location_id", "ihme_loc_id"), all=T,allow.cartesian=TRUE)

## one residual for each country-year-age with data for space-time, so as not to give years with more data more weight
setkey(st_data, ihme_loc_id, year_id, region_name, age, keep)
st_data <- st_data[,.(resid = mean(resid, na.rm=T)), by=key(st_data)]

temp <- unique(reg_map$region_name[reg_map$super_region_name %in% super_reg])
st_regions <- list()
for (rr in temp) st_regions[[paste0(rr)]] <-  unique(regs$region_name[grepl(rr, regs$region_name)])
st_regions <- unlist(st_regions)

st_pred <- resid_space_time(data=st_data, min_year=year_start, max_year =year_end,
                            use_super_regs = "East Asia_PRK", param_path = paste0(param_dir, "asfr_params_age_", model_age, "_", super_reg1, ".txt"), regression = F, region = st_regions)

## process spacetime output
st_pred[,year:=floor(year)]
st_pred[,"weight":=NULL]
st_pred <- st_pred[keep==1]
st_pred[,"keep":=NULL]
setnames(st_pred, "year", "year_id")

st_pred <- merge(st_pred, pred, all=T, by=c("ihme_loc_id", "year_id"))
st_pred[,stage2_pred:=stage1_pred_no_re-pred.2.resid]

write.csv(st_pred, paste0(out_dir, "stage2_results_age", model_age, "_reg_", super_reg1, ".csv"), row.names=F)
write.csv(dd, paste0(out_dir, "data_den_age", model_age, "_reg_", super_reg1, ".csv"), row.names=F)



