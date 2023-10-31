
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

"%ni%" <- Negate("%in%")

# Directories -------------------------------------------------------------
home_dir <- "FILEPATH"
extraction_dir <- file.path(home_dir,"FILEPATH")
bundle_dir <- file.path(home_dir,"FILEPATH")
xwalk_dir <- file.path(home_dir,"FILEPATHk")

# Functions
source(file.path(central_lib,"FILEPATH/get_bundle_data.R"))
source(file.path(central_lib,"FILEPATH/upload_bundle_data.R"))
source(file.path(central_lib,"FILEPATH/validate_input_sheet.R"))
source(file.path(central_lib,"FILEPATH/validate_crosswalk_input_sheet.R"))
source(file.path(central_lib,"FILEPATH/save_bundle_version.R"))
source(file.path(central_lib,"FILEPATH/save_crosswalk_version.R"))
source(file.path(central_lib,"FILEPATH/get_crosswalk_version.R"))
source(file.path(central_lib,"FILEPATH/get_bundle_version.R"))

source(file.path(central_lib,"FILEPATH/get_location_metadata.R"))
locs <- get_location_metadata(35)

decomp <- "iterative"
description <- "DESCRIPTION" 
hap_bundle_id <- 4736
clean_bundle_id <- 6164
coal_bundle_id <- 6167
crop_bundle_id <- 6173
dung_bundle_id <- 6176
wood_bundle_id <- 6170

#generate log to track problems
log <- data.table(nid=integer(),note=character())


# Read in reports and format ----------------------------------------------
reports <- read.xlsx(file.path(home_dir,"FILEPATH","FILEPATH.xlsx")) %>% as.data.table()
# these names are not needed
reports[,"location_name":=NULL]
reports[,"smaller_site_unit":=NULL]
reports[,"site_memo":=NULL]
reports <- rbindlist(list(reports,read.xlsx("FILEPATH.xlsx")),use.names = T) %>% as.data.table
reports <- reports[!is.na(nid)]

# if not available, calculate mean and standard error from n and sample size
# we add half a case to the numerator and 1 case to the denominator to prevent standard error of 0
reports[is.na(mean),mean:=n/sample_size]
reports[is.na(n),n:=sample_size*mean]

reports[, standard_error:=sqrt(((n+.5)/(sample_size+1)*(1-(n+.5)/(sample_size+1)))/(sample_size+1))]

# for the reports, we need 
reports <- reports[,.(underlying_nid,nid,survey_name,ihme_loc_id,year_start,year_end,file_path,cv_HH,n,sample_size,var,mean,standard_error,note_SR)]

# isolate to needed variables for GBD2020: solid, crop, coal, dung, wood, missing
# we're going to take out hap_electricity, hap_kerosene, hap_gas, and hap_none ("none" means you don't cook in your house or use any fuel)
reports <- reports[var %in% c("hap_solid","hap_crop", "hap_coal", "hap_dung", "hap_wood", # dirty categories
                              "hap_electricity", "hap_kerosene", "hap_none", "hap_gas", # clean categories
                              "missing_cooking_fuel_mapped")] # missing

# Assume those who don't publish missing have missingness of zero
reports[var=="missing_cooking_fuel_mapped" & is.na(mean), mean:=0]

# rename cv_hh variable for sensible column names after melting
reports[cv_HH==1, HH:="household"]
reports[cv_HH==0, HH:="individual"]

# melt each study wide
reports <- dcast.data.table(reports,survey_name + nid + underlying_nid + ihme_loc_id + year_start + year_end + file_path ~ HH + var,
                         value.var = c("mean","standard_error","sample_size","note_SR"), fun.aggregate= mean, fill=NA) 

# create one note_SR column by pasting together unique values from all note_SR columns
reports[, note_SR := paste(na.omit(unique(unlist(.SD))), collapse=", ") , by=1:nrow(reports), .SDcols=c(grep("note_SR",names(reports),value = T))]
reports[,c(grep("note_SR_",names(reports),value = T)):=NULL]

setnames(reports, names(reports)[names(reports) %like% "hap"], gsub("_hap","",names(reports)[names(reports) %like% "hap"]))
setnames(reports, names(reports)[names(reports) %like% "missing"], gsub("_cooking_fuel_mapped","",names(reports)[names(reports) %like% "missing"]))

# outlier studies with missingness > 15%
reports[,is_outlier:=0]
reports[mean_individual_missing>=.15 | mean_household_missing>=.15, c("is_outlier","outlier_reason"):=.(1,"missingness>=15%")]

# in ubcov, "other" or missing are naturally redistributed to the other categories. For reports we have to do this manually
# We assume the missing or other categories are evenly distributed amongst the other groups. (unless it causes it to be > 1)

# redistribute missing for hap_solid first
reports[,mean_individual_solid:=min(mean_individual_solid+mean_individual_missing*(mean_individual_solid/(1-mean_individual_missing)),1),by=1:nrow(reports)]
reports[,mean_household_solid:=min(mean_household_solid+mean_household_missing*(mean_household_solid/(1-mean_household_missing)),1),by=1:nrow(reports)]

# redistribute missing for disaggregated categories
reports[,mean_individual_coal:=min(mean_individual_coal+mean_individual_missing*(mean_individual_coal/(1-mean_individual_missing)),1),by=1:nrow(reports)]
reports[,mean_household_coal:=min(mean_household_coal+mean_household_missing*(mean_household_coal/(1-mean_household_missing)),1),by=1:nrow(reports)]
reports[,mean_individual_crop:=min(mean_individual_crop+mean_individual_missing*(mean_individual_crop/(1-mean_individual_missing)),1),by=1:nrow(reports)]
reports[,mean_household_crop:=min(mean_household_crop+mean_household_missing*(mean_household_crop/(1-mean_household_missing)),1),by=1:nrow(reports)]
reports[,mean_individual_dung:=min(mean_individual_dung+mean_individual_missing*(mean_individual_dung/(1-mean_individual_missing)),1),by=1:nrow(reports)]
reports[,mean_household_dung:=min(mean_household_dung+mean_household_missing*(mean_household_dung/(1-mean_household_missing)),1),by=1:nrow(reports)]
reports[,mean_individual_wood:=min(mean_individual_wood+mean_individual_missing*(mean_individual_wood/(1-mean_individual_missing)),1),by=1:nrow(reports)]
reports[,mean_household_wood:=min(mean_household_wood+mean_household_missing*(mean_household_wood/(1-mean_household_missing)),1),by=1:nrow(reports)]

reports[,cv_microdata:=0]


# Read in microdata extractions and format --------------------------------

read_and_date <- function(name){
  this_dt <- fread(name)
  this_dt[,date:=file.info(name)$mtime]
  return(this_dt)
}

microdata <- rbindlist(lapply(grep("/collapse",list.files(extraction_dir,full.names=T),value=T),read_and_date), fill=TRUE, use.names=TRUE)
microdata_2019 <- rbindlist(lapply(grep("/collapse",list.files("FILEPATH",full.names=T),value=T),read_and_date),fill=T,use.names=T)
microdata <- rbind(microdata,microdata_2019,fill=T,use.names=T)
microdata[,is_outlier:=0]

# we don't need some variables
microdata[,c("nclust","nstrata","design_effect","standard_deviation","standard_deviation_se"):=NULL]

# isolate to needed variables for GBD2020: solid, crop, coal, dung, wood, missing
microdata <- microdata[var %in% c("hap_solid","hap_crop", "hap_dung", "hap_wood", "hap_coal", "missing_cooking_fuel_mapped","missing_hh_size")]

#delete outright duplicates
microdata <- unique(microdata)

# drop data with tiny sample size
microdata[sample_size <= 5, c("is_outlier","outlier_reason"):=.(1,"tiny sample size")]

log <- rbind(log,data.table(nid=microdata[outlier_reason=="tiny sample size",unique(nid)],
                            note="check extraction, really small sample size for some locations"))

# fix standard error of zero (or really small) using formula sqrt(p*(1-p)/n) but we add half a case to the numerator and 1 case to the denominator to prevent standard error of 0
# do this for each of the categories individually to use unique se_cutoffs and check what is flagged/logged
se_cutoff <- min(c(reports$standard_error_household_solid,reports$standard_error_individual_solid),na.rm=T)
log <- rbind(log,data.table(nid=microdata[var=="hap_solid" & standard_error<se_cutoff,unique(nid)],
                            note="check extraction, really small standard_error"))
microdata[var=="hap_solid" & standard_error<se_cutoff, standard_error:=sqrt((((mean*sample_size)+.5)/(sample_size+1)*(1-((mean*sample_size)+.5)/(sample_size+1)))/(sample_size+1))]

se_cutoff <- min(c(reports$standard_error_household_coal,reports$standard_error_individual_coal),na.rm=T)
log <- rbind(log,data.table(nid=microdata[var=="hap_coal" & standard_error<se_cutoff,unique(nid)],
                            note="check extraction, really small standard_error"))
microdata[var=="hap_coal" & standard_error<se_cutoff, standard_error:=sqrt((((mean*sample_size)+.5)/(sample_size+1)*(1-((mean*sample_size)+.5)/(sample_size+1)))/(sample_size+1))]

se_cutoff <- min(c(reports$standard_error_household_crop,reports$standard_error_individual_crop),na.rm=T)
log <- rbind(log,data.table(nid=microdata[var=="hap_crop" & standard_error<se_cutoff,unique(nid)],
                            note="check extraction, really small standard_error"))
microdata[var=="hap_crop" & standard_error<se_cutoff, standard_error:=sqrt((((mean*sample_size)+.5)/(sample_size+1)*(1-((mean*sample_size)+.5)/(sample_size+1)))/(sample_size+1))]

se_cutoff <- min(c(reports$standard_error_household_dung,reports$standard_error_individual_dung),na.rm=T)
log <- rbind(log,data.table(nid=microdata[var=="hap_dung" & standard_error<se_cutoff,unique(nid)],
                            note="check extraction, really small standard_error"))
microdata[var=="hap_dung" & standard_error<se_cutoff, standard_error:=sqrt((((mean*sample_size)+.5)/(sample_size+1)*(1-((mean*sample_size)+.5)/(sample_size+1)))/(sample_size+1))]

se_cutoff <- min(c(reports$standard_error_household_wood,reports$standard_error_individual_wood),na.rm=T)
log <- rbind(log,data.table(nid=microdata[var=="hap_wood" & standard_error<se_cutoff,unique(nid)],
                            note="check extraction, really small standard_error"))
microdata[var=="hap_wood" & standard_error<se_cutoff, standard_error:=sqrt((((mean*sample_size)+.5)/(sample_size+1)*(1-((mean*sample_size)+.5)/(sample_size+1)))/(sample_size+1))]

# only take the most recent
microdata[order(date),N:=.N:1,by=c("nid","ihme_loc_id","year_start","year_end","var","cv_HH")]


microdata <- microdata[N==1]

# rename cv_hh variable for sensible column names after melting
microdata[cv_HH==1, HH:="household"]
microdata[cv_HH==0, HH:="individual"]

# melt each study wide
microdata <- dcast.data.table(microdata,survey_name + nid + ihme_loc_id + year_start + year_end + file_path + survey_module ~ HH + var,
                         value.var = c("mean","standard_error","sample_size"))

setnames(microdata, names(microdata)[names(microdata) %like% "hap"], gsub("_hap","",names(microdata)[names(microdata) %like% "hap"]))
setnames(microdata, names(microdata)[names(microdata) %like% "missing"], gsub("_cooking_fuel_mapped","",names(microdata)[names(microdata) %like% "missing"]))

microdata <-  microdata[,c("survey_name","nid","ihme_loc_id","year_start","year_end","file_path", "survey_module",
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
                           "sample_size_individual_wood"),with=F]

# outlier studies with missingness > 15%
microdata[,is_outlier:=0]
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

microdata[,cv_microdata:=1]


# Bind reports and microdata ----------------------------------------------

data <- rbind(reports,microdata,use.names=T,fill=T)

# format year_id so we can merge on outliers
data[,year_id:=round((year_start+year_end)/2)]

# Identify and outlier implausible data------------------------------------

# Additional outliers (not excluded for missingness or small sample sizes) saved in .csv
outliers <- fread(file.path(bundle_dir,"outliers.csv"))
setnames(outliers,"outlier_reason","outlier_reason2")

data <- merge(data,outliers,by=c("ihme_loc_id","underlying_nid","nid","file_path","year_id"),all.x=T,all.y=F)
data[!is.na(outlier_reason2),is_outlier:=1]
data[!is.na(outlier_reason2),outlier_reason:=outlier_reason2]
data[,outlier_reason2:=NULL]

# Mixed-fuel strings: outlier the mixed-fuel strings for fuel types
mixed_fuel_outliers <- read.xlsx(xlsxFile = paste0(j_root,"FILEPATH.xlsx"),sheet = "solid fuels")

mixed_fuel_outliers <- mixed_fuel_outliers$nid

# fuel-type specific outliers
fuel_type_outliers <- fread(file.path(bundle_dir,"FILEPATH.csv"))

# Split into fuel categories, bundle prep -----------------------------------------------------------
data[,sex:="Both"]
data[,age_group_id:=22]
data[,age_start:= 0]
data[,age_end:=125]
data[,measure:="proportion"]

# rename year_start and year_end for epi uploader
setnames(data,c("year_start","year_end"),c("orig_year_start","orig_year_end"))
data <- data[,c("year_start", "year_end"):=year_id] #all of 3 of these columns need to be present & identical for epi uploader

# now, split the larger datatable into "out" datatables unique to fuel types
# solid first
data_solid <- data[,c("survey_name","nid","underlying_nid","ihme_loc_id","year_start","year_end","file_path","note_SR",
                      "mean_household_solid","mean_individual_solid",
                      "standard_error_household_solid","standard_error_individual_solid",
                      "sample_size_household_solid","sample_size_individual_solid",
                      "is_outlier","outlier_reason","cv_microdata","survey_module", "sex", "age_group_id", "age_start",
                      "age_end", "measure", "year_id", "orig_year_start", "orig_year_end")]
data_solid[!is.na(mean_individual_solid),c("cv_hh","mean","standard_error","sample_size"):=.(0,mean_individual_solid,standard_error_individual_solid,sample_size_individual_solid)]
data_solid[is.na(mean),c("cv_hh","mean","standard_error","sample_size"):=.(1,mean_household_solid,standard_error_household_solid,sample_size_household_solid)]
setnames(data_solid, c("mean_household_solid","standard_error_household_solid"),c("mean_household","standard_error_household"))

out_solid <- data_solid[,.(underlying_nid,nid,survey_name,ihme_loc_id,year_id,year_start,year_end,
                           orig_year_start,orig_year_end,sex,age_group_id,age_start,age_end,measure,
                          file_path,mean,standard_error,sample_size,
                          cv_hh,mean_household,standard_error_household,
                          note_SR,is_outlier,outlier_reason,cv_microdata)]

# coal
data_coal <- data[,c("survey_name","nid","underlying_nid","ihme_loc_id","year_start","year_end","file_path","note_SR",
                      "mean_household_coal","mean_individual_coal",
                      "standard_error_household_coal","standard_error_individual_coal",
                      "sample_size_household_coal","sample_size_individual_coal",
                      "is_outlier","outlier_reason","cv_microdata","survey_module", "sex", "age_group_id", "age_start",
                     "age_end", "measure", "year_id", "orig_year_start", "orig_year_end")]
data_coal[!is.na(mean_individual_coal),c("cv_hh","mean","standard_error","sample_size"):=.(0,mean_individual_coal,standard_error_individual_coal,sample_size_individual_coal)]
data_coal[is.na(mean),c("cv_hh","mean","standard_error","sample_size"):=.(1,mean_household_coal,standard_error_household_coal,sample_size_household_coal)]
setnames(data_coal, c("mean_household_coal","standard_error_household_coal"),c("mean_household","standard_error_household"))

data_coal[nid%in%mixed_fuel_outliers, `:=` (is_outlier=1, outlier_reason="mixed fuel strings cause incorrect values")]
data_coal[nid%in%fuel_type_outliers[fuel_type%in%c("all","coal"),nid],is_outlier:=1]

out_coal <- data_coal[,.(underlying_nid,nid,survey_name,ihme_loc_id,year_id,year_start,year_end,
                           orig_year_start,orig_year_end,sex,age_group_id,age_start,age_end,measure,
                           file_path,mean,standard_error,sample_size,
                           cv_hh,mean_household,standard_error_household,
                           note_SR,is_outlier,outlier_reason,cv_microdata)]


# crop
data_crop <- data[,c("survey_name","nid","underlying_nid","ihme_loc_id","year_start","year_end","file_path","note_SR",
                      "mean_household_crop","mean_individual_crop",
                      "standard_error_household_crop","standard_error_individual_crop",
                      "sample_size_household_crop","sample_size_individual_crop",
                      "is_outlier","outlier_reason","cv_microdata","survey_module","sex", "age_group_id", "age_start",
                     "age_end", "measure", "year_id", "orig_year_start", "orig_year_end")]
data_crop[!is.na(mean_individual_crop),c("cv_hh","mean","standard_error","sample_size"):=.(0,mean_individual_crop,standard_error_individual_crop,sample_size_individual_crop)]
data_crop[is.na(mean),c("cv_hh","mean","standard_error","sample_size"):=.(1,mean_household_crop,standard_error_household_crop,sample_size_household_crop)]
setnames(data_crop, c("mean_household_crop","standard_error_household_crop"),c("mean_household","standard_error_household"))

data_crop[nid%in%mixed_fuel_outliers, `:=` (is_outlier=1, outlier_reason="mixed fuel strings cause incorrect values")]
data_crop[nid%in%fuel_type_outliers[fuel_type%in%c("all","crop"),nid],is_outlier:=1]

out_crop <- data_crop[,.(underlying_nid,nid,survey_name,ihme_loc_id,year_id,year_start,year_end,
                           orig_year_start,orig_year_end,sex,age_group_id,age_start,age_end,measure,
                           file_path,mean,standard_error,sample_size,
                           cv_hh,mean_household,standard_error_household,
                           note_SR,is_outlier,outlier_reason,cv_microdata)]

# dung
data_dung <- data[,c("survey_name","nid","underlying_nid","ihme_loc_id","year_start","year_end","file_path","note_SR",
                      "mean_household_dung","mean_individual_dung",
                      "standard_error_household_dung","standard_error_individual_dung",
                      "sample_size_household_dung","sample_size_individual_dung",
                      "is_outlier","outlier_reason","cv_microdata","survey_module","sex", "age_group_id", "age_start",
                     "age_end", "measure", "year_id", "orig_year_start", "orig_year_end")]
data_dung[!is.na(mean_individual_dung),c("cv_hh","mean","standard_error","sample_size"):=.(0,mean_individual_dung,standard_error_individual_dung,sample_size_individual_dung)]
data_dung[is.na(mean),c("cv_hh","mean","standard_error","sample_size"):=.(1,mean_household_dung,standard_error_household_dung,sample_size_household_dung)]
setnames(data_dung, c("mean_household_dung","standard_error_household_dung"),c("mean_household","standard_error_household"))

data_dung[nid%in%mixed_fuel_outliers, `:=` (is_outlier=1, outlier_reason="mixed fuel strings cause incorrect values")]
data_dung[nid%in%fuel_type_outliers[fuel_type%in%c("all","dung"),nid],is_outlier:=1]

out_dung <- data_dung[,.(underlying_nid,nid,survey_name,ihme_loc_id,year_id,year_start,year_end,
                           orig_year_start,orig_year_end,sex,age_group_id,age_start,age_end,measure,
                           file_path,mean,standard_error,sample_size,
                           cv_hh,mean_household,standard_error_household,
                           note_SR,is_outlier,outlier_reason,cv_microdata)]

# wood
data_wood <- data[,c("survey_name","nid","underlying_nid","ihme_loc_id","year_start","year_end","file_path","note_SR",
                      "mean_household_wood","mean_individual_wood",
                      "standard_error_household_wood","standard_error_individual_wood",
                      "sample_size_household_wood","sample_size_individual_wood",
                      "is_outlier","outlier_reason","cv_microdata","survey_module","sex", "age_group_id", "age_start",
                     "age_end", "measure", "year_id", "orig_year_start", "orig_year_end")]
data_wood[!is.na(mean_individual_wood),c("cv_hh","mean","standard_error","sample_size"):=.(0,mean_individual_wood,standard_error_individual_wood,sample_size_individual_wood)]
data_wood[is.na(mean),c("cv_hh","mean","standard_error","sample_size"):=.(1,mean_household_wood,standard_error_household_wood,sample_size_household_wood)]
setnames(data_wood, c("mean_household_wood","standard_error_household_wood"),c("mean_household","standard_error_household"))

data_wood[nid%in%mixed_fuel_outliers, `:=` (is_outlier=1, outlier_reason="mixed fuel strings cause incorrect values")]
data_wood[nid%in%fuel_type_outliers[fuel_type%in%c("all","wood"),nid],is_outlier:=1]

out_wood <- data_wood[,.(underlying_nid,nid,survey_name,ihme_loc_id,year_id,year_start,year_end,
                           orig_year_start,orig_year_end,sex,age_group_id,age_start,age_end,measure,
                           file_path,mean,standard_error,sample_size,
                           cv_hh,mean_household,standard_error_household,
                           note_SR,is_outlier,outlier_reason,cv_microdata)]

# now, continue with bundle prep (add variance and format "mean" name to "val")
out_solid[,variance:=standard_error^2]
out_coal[,variance:=standard_error^2]
out_crop[,variance:=standard_error^2]
out_dung[,variance:=standard_error^2]
out_wood[,variance:=standard_error^2]

setnames(out_solid,"mean","val")
setnames(out_coal,"mean","val")
setnames(out_crop,"mean","val")
setnames(out_dung,"mean","val")
setnames(out_wood,"mean","val")

# append old bundle data
old_bundle_solid <- get_bundle_data(hap_bundle_id, decomp_step="step4", gbd_round_id=6) %>% as.data.table
old_bundle_solid[,location_id:=NULL]
out_solid <- rbindlist(list(old_bundle_solid,out_solid),use.names=T,fill=T)
out_solid[,seq:=NULL]
out_solid <- unique(out_solid)

old_bundle_coal <- get_bundle_data(coal_bundle_id, decomp_step="iterative", gbd_round_id=6) %>% as.data.table
old_bundle_coal[,location_id:=NULL]
out_coal <- rbindlist(list(old_bundle_coal,out_coal),use.names=T,fill=T)
out_coal[,seq:=NULL]
out_coal <- unique(out_coal)

old_bundle_crop <- get_bundle_data(crop_bundle_id, decomp_step="iterative", gbd_round_id=6) %>% as.data.table
old_bundle_crop[,location_id:=NULL]
out_crop <- rbindlist(list(old_bundle_crop,out_crop),use.names=T,fill=T)
out_crop[,seq:=NULL]
out_crop <- unique(out_crop)

old_bundle_dung <- get_bundle_data(dung_bundle_id, decomp_step="iterative", gbd_round_id=6) %>% as.data.table
old_bundle_dung[,location_id:=NULL]
out_dung <- rbindlist(list(old_bundle_dung,out_dung),use.names=T,fill=T)
out_dung[,seq:=NULL]
out_dung <- unique(out_dung)

old_bundle_wood <- get_bundle_data(wood_bundle_id, decomp_step="iterative", gbd_round_id=6) %>% as.data.table
old_bundle_wood[,location_id:=NULL]
out_wood <- rbindlist(list(old_bundle_wood,out_wood),use.names=T,fill=T)
out_wood[,seq:=NULL]
out_wood <- unique(out_wood)

# get rid of rows in the fuel-type-specific bundles that don't actually have observations for that fuel type
out_solid <- out_solid[!is.na(val)]
out_coal <- out_coal[!is.na(val)]
out_crop <- out_crop[!is.na(val)]
out_dung <- out_dung[!is.na(val)]
out_wood <- out_wood[!is.na(val)]

# merge on location ids
out_solid <- merge(out_solid,locs[,.(ihme_loc_id,location_id)],by="ihme_loc_id")
out_coal <- merge(out_coal,locs[,.(ihme_loc_id,location_id)],by="ihme_loc_id")
out_crop <- merge(out_crop,locs[,.(ihme_loc_id,location_id)],by="ihme_loc_id")
out_dung <- merge(out_dung,locs[,.(ihme_loc_id,location_id)],by="ihme_loc_id")
out_wood <- merge(out_wood,locs[,.(ihme_loc_id,location_id)],by="ihme_loc_id")

# add SEQs for bundle upload
out_solid[,seq:=1:.N]
out_coal[,seq:=1:.N]
out_crop[,seq:=1:.N]
out_dung[,seq:=1:.N]
out_wood[,seq:=1:.N]

# Epi uploader doesn't like the " character
out_solid[,note_SR:=gsub('"',"",note_SR)]
out_coal[,note_SR:=gsub('"',"",note_SR)]
out_crop[,note_SR:=gsub('"',"",note_SR)]
out_dung[,note_SR:=gsub('"',"",note_SR)]
out_wood[,note_SR:=gsub('"',"",note_SR)]


# Upload to Bundle --------------------------------------------------------

write.xlsx(out_solid,file.path(bundle_dir,"upload_iterative_solid_DATE.xlsx"),sheetName="extraction",row.names=F)
write.xlsx(out_coal,file.path(bundle_dir,"upload_iterative_coal_DATE.xlsx"),sheetName="extraction",row.names=F)
write.xlsx(out_crop,file.path(bundle_dir,"upload_iterative_crop_DATE.xlsx"),sheetName="extraction",row.names=F)
write.xlsx(out_dung,file.path(bundle_dir,"upload_iterative_dung_DATE.xlsx"),sheetName="extraction",row.names=F)
write.xlsx(out_wood,file.path(bundle_dir,"upload_iterative_wood_DATE.xlsx"),sheetName="extraction",row.names=F)

upload_bundle_data(hap_bundle_id,decomp,filepath=file.path(bundle_dir,"upload_iterative_solid_DATE.xlsx"),gbd_round_id=7)
upload_bundle_data(coal_bundle_id,decomp,filepath=file.path(bundle_dir,"upload_iterative_coal_DATE.xlsx"),gbd_round_id=7)
upload_bundle_data(crop_bundle_id,decomp,filepath=file.path(bundle_dir,"upload_iterative_crop_DATE.xlsx"),gbd_round_id=7)
upload_bundle_data(dung_bundle_id,decomp,filepath=file.path(bundle_dir,"upload_iterative_dung_DATE.xlsx"),gbd_round_id=7)
upload_bundle_data(wood_bundle_id,decomp,filepath=file.path(bundle_dir,"upload_iterative_wood_DATE.xlsx"),gbd_round_id=7)

