# Purpose: pull together all HAP extractions for bundle upload

#------------------Set-up--------------------------------------------------
# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
  } else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
  }

# load packages


library(data.table)
library(magrittr)
library(openxlsx)
library(msm)
library(gtools)

"%ni%" <- Negate("%in%")

# Directories -------------------------------------------------------------
home_dir <- "FILEPATH"

bundle_dir <- file.path(home_dir, "FILEPATH")
xwalk_dir <- file.path(home_dir, "FILEPATH")
report_dir <- file.path(home_dir, "FILEPATH")

#we have multiple collapse folders:
extraction_dir <- file.path(home_dir, "FILEPATH")

#list all of the microdata files
microdata_files <- c(list.files(path = paste0(extraction_dir, "FILEPATH"), pattern = ".csv", full.names=T))

# Functions
source("FILEPATH/get_bundle_data.R")
source(file.path(central_lib, "FILEPATH/get_bundle_data.R"))
source(file.path(central_lib, "FILEPATH/upload_bundle_data.R"))
source(file.path(central_lib, "FILEPATH/validate_input_sheet.R"))
source(file.path(central_lib, "FILEPATH/validate_crosswalk_input_sheet.R"))
source(file.path(central_lib, "FILEPATH/save_bundle_version.R"))
source(file.path(central_lib, "FILEPATH/save_crosswalk_version.R"))
source(file.path(central_lib, "FILEPATH/get_crosswalk_version.R"))
source(file.path(central_lib, "FILEPATH/get_bundle_version.R"))
source(file.path(central_lib, "FILEPATH/get_location_metadata.R"))

# values ##########################################################
# decomp <- "iterative"
release <- 16  #G BD2023 release id
loc_set <- 35  # location set: model results
description <- "GBD2023 first run"
hap_bundle_id <- 4736
clean_bundle_id <- 6164
coal_bundle_id <- 6167
crop_bundle_id <- 6173
dung_bundle_id <- 6176
wood_bundle_id <- 6170

# generate log to track problems
log <- data.table(nid=integer(), note=character())

locs <- get_location_metadata(loc_set, release_id=release)

# Read in reports and format ----------------------------------------------
reports <- rbindlist(lapply(list.files(path=report_dir, full.names=T, pattern=".csv"),
                            fread), use.names=T, fill=T)

## Edits ##############################################
# remove all of the blank rows
reports <- reports[!is.na(nid)]

# fill in empty columns

# this report file does not have an n column. n is suppose to be the number of people using a type of fuel
reports[, n:=(as.numeric(sample_size_num_hh)*as.numeric(avg_hh_size))]

# fill in sample size. Sample size is the total number of people in the population that was sampled.
reports$sample_size<-as.numeric(reports$sample_size)

reports[is.na(sample_size) & !is.na(n), sample_size:=sum(n), by=c("site_memo","nid")]  # they sampled 2 different locations, so setting the sample size based on this
reports[is.na(mean), mean:=n/sample_size]

# change some columns to be numeric
cols<-c("mean", "sample_size")
reports[, (cols):=lapply(.SD,function(x) as.numeric(x)), .SDcols=cols]

# calculate the standard error
reports[, standard_error:=sqrt(((n+.5)/(sample_size+1)*(1-(n+.5)/(sample_size+1)))/(sample_size+1))]

# for the reports, we need
reports <- reports[, .(underlying_nid,nid,survey_name,location_id,ihme_loc_id,site_memo,year_start,year_end,file_path,cv_HH,n,sample_size,hap_var,mean,standard_error,note_SR)]

# isolate to needed variables for GBD2020: solid, crop, coal, dung, wood, missing
# we're going to take out hap_electricity, hap_kerosene, hap_gas, and hap_none ("none" means you don't cook in your house or use any fuel)
reports <- reports[hap_var %in% c("hap_solid","hap_crop", "hap_coal", "hap_dung", "hap_wood",  # dirty categories
                              "missing_cooking_fuel_mapped")]  # missing

# Assume those who don't publish missing have missingness of zero
reports[hap_var == "missing_cooking_fuel_mapped" & is.na(mean), mean:=0]

# rename cv_hh variable for sensible column names after melting
reports[cv_HH==1, HH:="household"]
reports[cv_HH==0, HH:="individual"]


## melt ##########################
# melt each study wide
reports <- dcast.data.table(reports,survey_name + nid + underlying_nid + ihme_loc_id + year_start + year_end + file_path + site_memo + note_SR + location_id ~ HH + hap_var,
                         value.var = c("mean","standard_error","sample_size"), fun.aggregate= mean, fill=NA)

# indicate that it isn't microdata
reports[, cv_microdata:=0]

setnames(reports, names(reports)[names(reports) %like% "hap"], gsub("_hap","",names(reports)[names(reports) %like% "hap"]))
setnames(reports, names(reports)[names(reports) %like% "missing"], gsub("_cooking_fuel_mapped", "", names(reports)[names(reports) %like% "missing"]))

# if the missingness columns don't exist add them here
reports[!"missing" %in% colnames(reports), ':='(mean_individual_missing=0, mean_household_missing=0)]

## outliers #########################
# outlier studies with missingness > 15% in accordance with the Shaddick paper
reports[mean_individual_missing>=.15 | mean_household_missing>=.15, c("is_outlier","outlier_reason"):=.(1,"missingness>=15%")]
reports[is.na(is_outlier),is_outlier:=0]

# Microdata #########################################################################
## Read in ########################

read_and_date <- function(name){
  this_dt <- fread(name)
  this_dt[, date:=file.info(name)$mtime]
  return(this_dt)
}

microdata <- rbindlist(lapply(microdata_files,read_and_date), fill=T, use.names=T)

## edits ############################
### add locs #############
microdata<-merge(microdata, locs[,.(location_id,ihme_loc_id,location_name)], by="ihme_loc_id", all.x=T)

### other edits ##########################
# none of the microdata rows have outlier indicates so, mark them as 0 for now
microdata[, is_outlier:=0]

# we don't need some variables
microdata[, c("nclust","nstrata","design_effect","standard_deviation","standard_deviation_se"):=NULL]

# isolate to needed variables for GBD2020+: solid, crop, coal, dung, wood, missing
microdata <- microdata[var %in% c("hap_solid","hap_crop", "hap_dung", "hap_wood", "hap_coal", "missing_cooking_fuel_mapped","missing_hh_size")]

# delete outright duplicates
microdata <- unique(microdata)

# drop data with tiny sample size
microdata[sample_size <= 5, c("is_outlier", "outlier_reason"):=.(1, "tiny sample size")]

# add outliering to log
log <- rbind(log, data.table(nid=microdata[outlier_reason=="tiny sample size", unique(nid)],
                            note="check extraction, really small sample size for some locations"))

# rename cv_hh variable for sensible column names after melting
microdata[cv_HH==1, HH:="household"]
microdata[cv_HH==0, HH:="individual"]


## Standard errors ##############################
# fix standard error of zero (or really small) using formula sqrt(p*(1-p)/n) but we add half a case to the numerator and 1 case to the denominator to prevent standard error of 0
# do this for each of the categories individually

se_cutoff <- min(c(reports$standard_error_household_solid, reports$standard_error_individual_solid), na.rm=T)
log <- rbind(log,data.table(nid=microdata[var=="hap_solid" & standard_error<se_cutoff, unique(nid)],
                            note="check extraction, really small standard_error"))
microdata[var=="hap_solid" & standard_error<se_cutoff, standard_error:=sqrt((((mean*sample_size)+.5)/(sample_size+1)*(1-((mean*sample_size)+.5)/(sample_size+1)))/(sample_size+1))]

se_cutoff <- min(c(reports$standard_error_household_coal,reports$standard_error_individual_coal), na.rm=T)
log <- rbind(log,data.table(nid=microdata[var=="hap_coal" & standard_error<se_cutoff,unique(nid)],
                            note="check extraction, really small standard_error"))
microdata[var=="hap_coal" & standard_error<se_cutoff, standard_error:=sqrt((((mean*sample_size)+.5)/(sample_size+1)*(1-((mean*sample_size)+.5)/(sample_size+1)))/(sample_size+1))]

se_cutoff <- min(c(reports$standard_error_household_crop,reports$standard_error_individual_crop), na.rm=T)
log <- rbind(log,data.table(nid=microdata[var=="hap_crop" & standard_error<se_cutoff,unique(nid)],
                            note="check extraction, really small standard_error"))
microdata[var=="hap_crop" & standard_error<se_cutoff, standard_error:=sqrt((((mean*sample_size)+.5)/(sample_size+1)*(1-((mean*sample_size)+.5)/(sample_size+1)))/(sample_size+1))]

se_cutoff <- min(c(reports$standard_error_household_dung,reports$standard_error_individual_dung), na.rm=T)
log <- rbind(log,data.table(nid=microdata[var=="hap_dung" & standard_error<se_cutoff,unique(nid)],
                            note="check extraction, really small standard_error"))
microdata[var=="hap_dung" & standard_error<se_cutoff, standard_error:=sqrt((((mean*sample_size)+.5)/(sample_size+1)*(1-((mean*sample_size)+.5)/(sample_size+1)))/(sample_size+1))]

se_cutoff <- min(c(reports$standard_error_household_wood,reports$standard_error_individual_wood), na.rm=T)
log <- rbind(log,data.table(nid=microdata[var=="hap_wood" & standard_error<se_cutoff,unique(nid)],
                            note="check extraction, really small standard_error"))
microdata[var=="hap_wood" & standard_error<se_cutoff, standard_error:=sqrt((((mean*sample_size)+.5)/(sample_size+1)*(1-((mean*sample_size)+.5)/(sample_size+1)))/(sample_size+1))]

## keep most recent #######################
# only take the most recent
microdata[order(date), N:=.N:1, by=c("nid","ihme_loc_id","year_start","year_end","var","cv_HH")]

microdata <- microdata[N==1]

## Melt ################################
# melt each study wide
microdata <- dcast.data.table(microdata,survey_name + nid + ihme_loc_id + year_start + year_end + file_path + survey_module + location_id ~ HH + var,
                         value.var = c("mean","standard_error","sample_size"))

setnames(microdata, names(microdata)[names(microdata) %like% "hap"], gsub("_hap","",names(microdata)[names(microdata) %like% "hap"]))
setnames(microdata, names(microdata)[names(microdata) %like% "missing"], gsub("_cooking_fuel_mapped","", names(microdata)[names(microdata) %like% "missing"]))

## clean up ###################
# only keep these columns
microdata <-  microdata[, c("survey_name","nid","ihme_loc_id","year_start","year_end","file_path", "survey_module","location_id",
                           "mean_household_coal",
                           "mean_household_crop",
                           "mean_household_dung",
                           "mean_household_solid",
                           "mean_household_wood",
                           "mean_household_missing",
                           "mean_individual_coal",
                           "mean_individual_crop",
                           "mean_individual_dung",
                           "mean_individual_solid",
                           "mean_individual_wood",
                           "mean_individual_missing",
                           "standard_error_household_coal",
                           "standard_error_household_crop",
                           "standard_error_household_dung",
                           "standard_error_household_solid",
                           "standard_error_household_wood",
                           "standard_error_individual_coal",
                           "standard_error_individual_crop",
                           "standard_error_individual_dung",
                           "standard_error_individual_solid",
                           "standard_error_individual_wood",
                           "sample_size_household_coal",
                           "sample_size_household_crop",
                           "sample_size_household_dung",
                           "sample_size_household_solid",
                           "sample_size_household_wood",
                           "sample_size_individual_coal",
                           "sample_size_individual_crop",
                           "sample_size_individual_dung",
                           "sample_size_individual_solid",
                           "sample_size_individual_wood"), with=F]

## outliers #############################
# outlier studies with missingness > 15% in accordance with the Shaddick paper
microdata[, is_outlier:=0]
microdata[mean_individual_missing>=.15 | mean_household_missing>=.15, c("is_outlier","outlier_reason"):=.(1,"missingness>=15%")]

# if the mean household estimate is equivalent to the mean individual measurement, and they are not 1 or zero
# we assume it's actually a HH measurement and delete all the individual measures
# do this for each category individually

log <- rbind(log,data.table(nid=microdata[mean_household_solid==mean_individual_solid & mean_household_solid!=1 & mean_household_solid!=0, unique(nid)], note="HH estimate equal to individual, check extraction"))
microdata[mean_household_solid==mean_individual_solid & mean_household_solid!=1 & mean_household_solid!=0, grep("individual",names(microdata),value=T):=NA]

log <- rbind(log,data.table(nid=microdata[mean_household_coal==mean_individual_coal & mean_household_coal!=1 & mean_household_coal!=0, unique(nid)], note="HH estimate equal to individual, check extraction"))
microdata[mean_household_coal==mean_individual_coal & mean_household_coal!=1 & mean_household_coal!=0, grep("individual",names(microdata),value=T):=NA]

log <- rbind(log,data.table(nid=microdata[mean_household_crop==mean_individual_crop & mean_household_crop!=1 & mean_household_crop!=0, unique(nid)], note="HH estimate equal to individual, check extraction"))
microdata[mean_household_crop==mean_individual_crop & mean_household_crop!=1 & mean_household_crop!=0, grep("individual",names(microdata),value=T):=NA]

log <- rbind(log,data.table(nid=microdata[mean_household_dung==mean_individual_dung & mean_household_dung!=1 & mean_household_dung!=0, unique(nid)], note="HH estimate equal to individual, check extraction"))
microdata[mean_household_dung==mean_individual_dung & mean_household_dung!=1 & mean_household_dung!=0, grep("individual",names(microdata),value=T):=NA]

log <- rbind(log,data.table(nid=microdata[mean_household_wood==mean_individual_wood & mean_household_wood!=1 & mean_household_wood!=0, unique(nid)], note="HH estimate equal to individual, check extraction"))
microdata[mean_household_wood==mean_individual_wood & mean_household_wood!=1 & mean_household_wood!=0, grep("individual",names(microdata),value=T):=NA]

log <- log[!is.na(nid)]
log <- unique(log)

#indicate it is microdata
microdata[, cv_microdata:=1]

# Bind reports and microdata ##############################################################
data <- rbind(reports, microdata, use.names=T, fill=T)

# format year_id so we can merge on outliers
data[, year_id:=round((as.numeric(year_start)+as.numeric(year_end))/2)]

# Bundle prep ####################################################
# Split into fuel categories, bundle prep
# bundle prep (prepping the columns that don't need to be specialized for each "out" datatable)
data[, ':='(sex="Both", age_group_id=22, age_start= 0, age_end=125, measure="proportion", seq=NA)]


data[nid %in% c(59275,93804,163066), year_id:=year_start]

# rename year_start and year_end for epi uploader
setnames(data, c("year_start","year_end"), c("orig_year_start","orig_year_end"))
data <- data[, c("year_start", "year_end"):=year_id]

# change some columns to be numeric
cols <- c("year_start","year_end")
data[, (cols):=lapply(.SD,function(x) as.numeric(x)),.SDcols=cols]

# now, split the larger datatable into "out" datatables unique to fuel types
## solid ######################
data_solid <- data[,c("seq","survey_name","nid","underlying_nid","ihme_loc_id","year_start","year_end","file_path","note_SR","location_id",
                      "mean_household_solid","mean_individual_solid",
                      "standard_error_household_solid","standard_error_individual_solid",
                      "sample_size_household_solid","sample_size_individual_solid",
                      "is_outlier","outlier_reason","cv_microdata","survey_module", "sex", "age_group_id", "age_start",
                      "age_end", "measure", "year_id","site_memo")]

data_solid[!is.na(mean_individual_solid), c("cv_hh","mean","standard_error","sample_size"):=.(0,mean_individual_solid,standard_error_individual_solid,sample_size_individual_solid)]

data_solid[is.na(mean), c("cv_hh","mean","standard_error","sample_size"):=.(1,mean_household_solid,standard_error_household_solid,sample_size_household_solid)]

setnames(data_solid, c("mean_household_solid","standard_error_household_solid"), c("mean_household","standard_error_household"))

out_solid <- data_solid[, .(seq,underlying_nid,nid,survey_name,ihme_loc_id,year_id,year_start,year_end,location_id,
                           sex,age_group_id,age_start,age_end,measure,
                           file_path,mean,standard_error,sample_size,
                           cv_hh,mean_household,standard_error_household,
                           note_SR,is_outlier,outlier_reason,cv_microdata,site_memo)]

## coal ###########################
data_coal <- data[, c("seq","survey_name","nid","underlying_nid","ihme_loc_id","year_start","year_end","file_path","note_SR","location_id",
                      "mean_household_coal","mean_individual_coal",
                      "standard_error_household_coal","standard_error_individual_coal",
                      "sample_size_household_coal","sample_size_individual_coal",
                      "is_outlier","outlier_reason","cv_microdata","survey_module", "sex", "age_group_id", "age_start",
                      "age_end", "measure", "year_id", "site_memo")]

data_coal[!is.na(mean_individual_coal), c("cv_hh", "mean", "standard_error", "sample_size"):=.(0,mean_individual_coal,standard_error_individual_coal,sample_size_individual_coal)]

data_coal[is.na(mean), c("cv_hh", "mean", "standard_error", "sample_size"):=.(1,mean_household_coal,standard_error_household_coal,sample_size_household_coal)]

setnames(data_coal, c("mean_household_coal", "standard_error_household_coal"), c("mean_household","standard_error_household"))

out_coal <- data_coal[, .(seq,underlying_nid,nid,survey_name,ihme_loc_id,year_id,year_start,year_end,location_id,
                           sex,age_group_id,age_start,age_end,measure,
                           file_path,mean,standard_error,sample_size,
                           cv_hh,mean_household,standard_error_household,
                           note_SR,is_outlier,outlier_reason,cv_microdata,site_memo)]


## crop ############################
data_crop <- data[,c("seq","survey_name","nid","underlying_nid","ihme_loc_id","year_start","year_end","file_path","note_SR","location_id",
                      "mean_household_crop","mean_individual_crop",
                      "standard_error_household_crop","standard_error_individual_crop",
                      "sample_size_household_crop","sample_size_individual_crop",
                      "is_outlier","outlier_reason","cv_microdata","survey_module","sex", "age_group_id", "age_start",
                      "age_end", "measure", "year_id", "site_memo")]

data_crop[!is.na(mean_individual_crop), c("cv_hh","mean","standard_error","sample_size"):=.(0,mean_individual_crop,standard_error_individual_crop,sample_size_individual_crop)]

data_crop[is.na(mean), c("cv_hh","mean","standard_error","sample_size"):=.(1,mean_household_crop,standard_error_household_crop,sample_size_household_crop)]

setnames(data_crop, c("mean_household_crop","standard_error_household_crop"), c("mean_household","standard_error_household"))

out_crop <- data_crop[, .(seq,underlying_nid,nid,survey_name,ihme_loc_id,year_id,year_start,year_end,location_id,
                           sex,age_group_id,age_start,age_end,measure,
                           file_path,mean,standard_error,sample_size,
                           cv_hh,mean_household,standard_error_household,
                           note_SR,is_outlier,outlier_reason,cv_microdata,site_memo)]

## dung #######################
data_dung <- data[, c("seq","survey_name","nid","underlying_nid","ihme_loc_id","year_start","year_end","file_path","note_SR","location_id",
                      "mean_household_dung","mean_individual_dung",
                      "standard_error_household_dung","standard_error_individual_dung",
                      "sample_size_household_dung","sample_size_individual_dung",
                      "is_outlier","outlier_reason","cv_microdata","survey_module","sex", "age_group_id", "age_start",
                      "age_end", "measure", "year_id", "site_memo")]

data_dung[!is.na(mean_individual_dung), c("cv_hh","mean","standard_error","sample_size"):=.(0,mean_individual_dung,standard_error_individual_dung,sample_size_individual_dung)]

data_dung[is.na(mean), c("cv_hh","mean","standard_error","sample_size"):=.(1,mean_household_dung,standard_error_household_dung,sample_size_household_dung)]

setnames(data_dung, c("mean_household_dung","standard_error_household_dung"), c("mean_household","standard_error_household"))

out_dung <- data_dung[, .(seq,underlying_nid,nid,survey_name,ihme_loc_id,year_id,year_start,year_end,location_id,
                           sex,age_group_id,age_start,age_end,measure,
                           file_path,mean,standard_error,sample_size,
                           cv_hh,mean_household,standard_error_household,
                           note_SR,is_outlier,outlier_reason,cv_microdata,site_memo)]

## wood ########################
data_wood <- data[, c("seq","survey_name","nid","underlying_nid","ihme_loc_id","year_start","year_end","file_path","note_SR","location_id",
                      "mean_household_wood","mean_individual_wood",
                      "standard_error_household_wood","standard_error_individual_wood",
                      "sample_size_household_wood","sample_size_individual_wood",
                      "is_outlier","outlier_reason","cv_microdata","survey_module","sex", "age_group_id", "age_start",
                      "age_end", "measure", "year_id", "site_memo")]

data_wood[!is.na(mean_individual_wood), c("cv_hh","mean","standard_error","sample_size"):=.(0,mean_individual_wood,standard_error_individual_wood,sample_size_individual_wood)]

data_wood[is.na(mean), c("cv_hh","mean","standard_error","sample_size"):=.(1,mean_household_wood,standard_error_household_wood,sample_size_household_wood)]

setnames(data_wood, c("mean_household_wood","standard_error_household_wood"), c("mean_household","standard_error_household"))

out_wood <- data_wood[, .(seq,underlying_nid,nid,survey_name,ihme_loc_id,year_id,year_start,year_end,location_id,
                           sex,age_group_id,age_start,age_end,measure,
                           file_path,mean,standard_error,sample_size,
                           cv_hh,mean_household,standard_error_household,
                           note_SR,is_outlier,outlier_reason,cv_microdata,site_memo)]

## bundle prep #############################
# now, continue with bundle prep (add variance and format "mean" name to "val")
out_solid[, variance:=standard_error^2]
out_coal[, variance:=standard_error^2]
out_crop[, variance:=standard_error^2]
out_dung[, variance:=standard_error^2]
out_wood[, variance:=standard_error^2]

setnames(out_solid, "mean", "val")
setnames(out_coal, "mean", "val")
setnames(out_crop, "mean", "val")
setnames(out_dung, "mean", "val")
setnames(out_wood, "mean", "val")


# get rid of all the rows in the fuel-type-specific bundles that don't actually have observations for that fuel type
out_solid <- out_solid[!is.na(val)]
out_coal <- out_coal[!is.na(val)]
out_crop <- out_crop[!is.na(val)]
out_dung <- out_dung[!is.na(val)]
out_wood <- out_wood[!is.na(val)]


# Upload to Bundle --------------------------------------------------------

write.xlsx(out_solid, file.path(bundle_dir, "HAP_solid_IND_new_extractions.xlsx"), sheetName="extraction",rowNames=F)
write.xlsx(out_coal, file.path(bundle_dir, "HAP_coal_IND_new_extractions.xlsx"), sheetName="extraction", rowNames=F)
write.xlsx(out_crop, file.path(bundle_dir, "HAP_crop_IND_new_extractions.xlsx"), sheetName="extraction", rowNames=F)
write.xlsx(out_dung, file.path(bundle_dir, "HAP_dung_IND_new_extractions.xlsx"), sheetName="extraction", rowNames=F)
write.xlsx(out_wood, file.path(bundle_dir, "HAP_wood_IND_new_extractions.xlsx"), sheetName="extraction", rowNames=F)