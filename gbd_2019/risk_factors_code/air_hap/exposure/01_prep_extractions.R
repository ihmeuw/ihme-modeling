
#-------------------Header------------------------------------------------
# Author: NAME
# Date: 08/23/2019
# Purpose: pull together all HAP extractions for bundle upload
#          
# source("FILEPATH.R", echo=T)
#***************************************************************************

#------------------SET-UP--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
  } else {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
  }


# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","openxlsx","msm","gtools")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

# Directories -------------------------------------------------------------

home_dir <- "FILEPATH"
extraction_dir <- file.path(home_dir,"FILEPATH")
bundle_dir <- file.path(home_dir,"FILEPATH")
xwalk_dir <- file.path(home_dir,"FILEPATH")

#functions
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))

source(file.path(central_lib,"FILEPATH.R"))
locs <- get_location_metadata(35)

decomp <- "step4"
description <- " " 
bundle_id <- 4736


#generate log to track problems
log <- data.table(nid=integer(),note=character())


# Read in reports and format ----------------------------------------------

reports <- read.xlsx(file.path(home_dir,"FILEPATH.xlsx"),sheetIndex = 1) %>% as.data.table()

# if not availabe, calculate mean and standard error from n and sample size
# we add half a case to the numerator and 1 case to the denominator to prevent standard error of 0
reports[is.na(mean),mean:=n/sample_size]
reports[is.na(n),n:=sample_size*mean]

reports[, standard_error:=sqrt(((n+.5)/(sample_size+1)*(1-(n+.5)/(sample_size+1)))/(sample_size+1))]

# for the reports, we need 

reports <- reports[,.(underlying_nid,nid,survey_name,ihme_loc_id,year_start,year_end,file_path,cv_HH,n,sample_size,var,mean,standard_error,note_SR)]

# isolate to needed variable for GBD2019
reports <- reports[var %in% c("hap_solid","missing_cooking_fuel_mapped")]

# assume those who don't publish missing have missingness of zero
reports[var=="missing_cooking_fuel_mapped" & is.na(mean), mean:=0]

# rename cv_hh variable for sensible column names after melting
reports[cv_HH==1, HH:="household"]
reports[cv_HH==0, HH:="individual"]

# reports that give summer and winter fuel usage separately; right now this is just nid 565
reports[grep("summer",note_SR,ignore.case=T),survey_name:=paste0(survey_name,"_summer")]
reports[grep("winter",note_SR,ignore.case=T),survey_name:=paste0(survey_name,"_winter")]

# melt each study wide

reports <- dcast.data.table(reports,survey_name + nid + underlying_nid + ihme_loc_id + year_start + year_end + file_path ~ HH + var,
                         value.var = c("mean","standard_error","sample_size","note_SR"))

# create one note_SR column by pasting together unique values from all note_SR columns
reports[, note_SR := paste(na.omit(unique(unlist(.SD))), collapse=", ") , by=1:nrow(reports), .SDcols=c(grep("note_SR",names(reports),value = T))]
reports[,c(grep("note_SR_",names(reports),value = T)):=NULL]

setnames(reports,c("mean_household_hap_solid",
                "mean_individual_hap_solid",
                "standard_error_household_hap_solid",
                "standard_error_individual_hap_solid",
                "mean_household_missing_cooking_fuel_mapped",
                "mean_individual_missing_cooking_fuel_mapped",
                "sample_size_household_hap_solid",
                "sample_size_individual_hap_solid"),
         c("mean_household",
           "mean_individual",
           "standard_error_household",
           "standard_error_individual",
           "missing_household",
           "missing_individual",
           "sample_size_household",
           "sample_size_individual"))

reports <- reports[,c("survey_name","nid","underlying_nid","ihme_loc_id","year_start","year_end","file_path","note_SR",
                 "mean_household",
                 "mean_individual",
                 "standard_error_household",
                 "standard_error_individual",
                 "missing_household",
                 "missing_individual",
                 "sample_size_household",
                 "sample_size_individual"),with=F]

# outlier studies with missingness > 15% in accordance with the Shaddick paper
reports[,is_outlier:=0]
reports[missing_individual>=.15 | missing_household>=.15, c("is_outlier","outlier_reason"):=.(1,"missingness>=15%")]

# In ubcov, "other" or "missing" are naturally redistributed to the other categories. For reports we have to do this manually.
# We assume the missing or other categories are evenly distributed amongst the other groups (unless it causes prop to be > 1).
reports[,mean_individual:=min(mean_individual+missing_individual*(mean_individual/(1-missing_individual)),1),by=1:nrow(reports)]
reports[,mean_household:=min(mean_household+missing_household*(mean_household/(1-missing_household)),1),by=1:nrow(reports)]

reports[,cv_microdata:=0]


# Read in microdata extractions and format --------------------------------

read_and_date <- function(name){
  this_dt <- fread(name)
  this_dt[,date:=file.info(name)$mtime]
  return(this_dt)
}

microdata <- rbindlist(lapply(grep("FILEPATH",list.files(extraction_dir,full.names=T),value=T),read_and_date), fill=TRUE, use.names=TRUE)
microdata[,is_outlier:=0]

# we don't need some variables
microdata[,c("nclust","nstrata","design_effect","standard_deviation","standard_deviation_se"):=NULL]

# isolate to needed variable for GBD2019
microdata <- microdata[var %in% c("hap_solid","missing_cooking_fuel_mapped","missing_hh_size")]

# delete outright duplicates
microdata <- unique(microdata)

# drop data with tiny sample size
microdata[sample_size <= 5, c("is_outlier","outlier_reason"):=.(1,"tiny sample size")]

log <- rbind(log,data.table(nid=microdata[outlier_reason=="tiny sample size",unique(nid)],
                            note="check extraction, really small sample size for some locations"))

# fix standard error of zero (or really small) using formula sqrt(p*(1-p)/n)
# we add half a case to the numerator and 1 case to the denominator to prevent standard error of 0
se_cutoff <- min(c(reports$standard_error_household,reports$standard_error_individual),na.rm=T)
log <- rbind(log,data.table(nid=microdata[var=="hap_solid" & standard_error<se_cutoff,unique(nid)],
                            note="check extraction, really small standard_error"))

microdata[var=="hap_solid" & standard_error<se_cutoff, standard_error:=sqrt((((mean*sample_size)+.5)/(sample_size+1)*(1-((mean*sample_size)+.5)/(sample_size+1)))/(sample_size+1))]

# only take the most recent
microdata[order(date),N:=.N:1,by=c("nid","ihme_loc_id","year_start","year_end","var","cv_HH")]

microdata <- microdata[N==1]

# rename cv_hh variable for sensible column names after melting
microdata[cv_HH==1, HH:="household"]
microdata[cv_HH==0, HH:="individual"]

# melt each study wide

microdata <- dcast.data.table(microdata,survey_name + nid + ihme_loc_id + year_start + year_end + file_path + survey_module ~ HH + var,
                         value.var = c("mean","standard_error","sample_size"))
setnames(microdata,c("mean_household_hap_solid",
                "mean_individual_hap_solid",
                "standard_error_household_hap_solid",
                "standard_error_individual_hap_solid",
                "mean_household_missing_cooking_fuel_mapped",
                "mean_individual_missing_cooking_fuel_mapped",
                "sample_size_household_hap_solid",
                "sample_size_individual_hap_solid"),
         c("mean_household",
           "mean_individual",
           "standard_error_household",
           "standard_error_individual",
           "missing_household",
           "missing_individual",
           "sample_size_household",
           "sample_size_individual"))

microdata <-  microdata[,c("survey_name","nid","ihme_loc_id","year_start","year_end","file_path", "survey_module",
           "mean_household",
           "mean_individual",
           "standard_error_household",
           "standard_error_individual",
           "missing_household",
           "missing_individual",
           "sample_size_household",
           "sample_size_individual"),with=F]

# outlier studies with missingness > 15% in accordance with the Shaddick paper
microdata[,is_outlier:=0]
microdata[missing_individual>=.15 | missing_household>=.15, c("is_outlier","outlier_reason"):=.(1,"missingness>=15%")]

# if the mean household estimate is equivalent to the mean individual measurement, and they are not 1 or 0
# we assume it's actually a HH measurement and delete all the individual measures
log <- rbind(log,data.table(nid=microdata[mean_household==mean_individual & mean_household!=1 & mean_household!=0, unique(nid)], note="HH estimate equal to individual, check extraction"))

microdata[mean_household==mean_individual & mean_household!=1 & mean_household!=0, grep("individual",names(microdata),value=T):=NA]

microdata[,cv_microdata:=1]


data <- rbind(reports,microdata,use.names=T,fill=T)

data[!is.na(mean_individual),c("cv_hh","mean","standard_error","sample_size"):=.(0,mean_individual,standard_error_individual,sample_size_individual)]
data[is.na(mean),c("cv_hh","mean","standard_error","sample_size"):=.(1,mean_household,standard_error_household,sample_size_household)]

out <- data[,.(underlying_nid,nid,survey_name,ihme_loc_id,year_start,year_end,
        file_path,mean,standard_error,sample_size,
        cv_hh,mean_household,standard_error_household,
        note_SR,is_outlier,outlier_reason,cv_microdata)]


# Bundle prep -----------------------------------------------------------

out[,sex:="Both"]
out[,age_group_id:=22]
out[,measure:="proportion"]
out[,year_id:=round((year_start+year_end)/2)]

# fix anomalous india survey years
out[nid %in% c(59275,93804,163066), year_id:=year_start]

out[,variance:=standard_error^2]

setnames(out,"mean","val")

# rename year_start and year_end for epi uploader
setnames(out,c("year_start","year_end"),c("orig_year_start","orig_year_end"))

# additional outliers (not excluded for missingness or small sample sizes) saved in .csv

outliers <- fread(file.path(bundle_dir,"outliers.csv"))
setnames(outliers,"outlier_reason","outlier_reason2")

out <- merge(out,outliers,by=c("ihme_loc_id","underlying_nid","nid","file_path","year_id"),all.x=T,all.y=T)
out[!is.na(outlier_reason2),is_outlier:=1]
out[!is.na(outlier_reason2),outlier_reason:=outlier_reason2]
out[,outlier_reason2:=NULL]

# merge on location ids
out <- merge(out,locs[,.(ihme_loc_id,location_id)],by="ihme_loc_id")


# Upload to bundle --------------------------------------------------------

write.xlsx(out,file.path(bundle_dir,"upload.xlsx"),sheetName="extraction",row.names=F)
upload_bundle_data(bundle_id,decomp,filepath=file.path(bundle_dir,"upload.xlsx"))