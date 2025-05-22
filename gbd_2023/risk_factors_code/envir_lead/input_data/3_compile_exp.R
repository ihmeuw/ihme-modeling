# compile lead exposure data #######################################

rm(list=ls())

# Libraries ######################################
rm(list=ls())
library(data.table)
library(magrittr)
library(xlsx)
library(readr)

source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_age_metadata.R")

# values #####################################
gbd<-"GBD2022" #for filepaths
dir<-paste0("FILEPATH",gbd,"FILEPATH") #in and out dir
release<-16 

# Import ###############################################
# load in data
data<-read.xlsx(paste0(dir,"FILEPATH/2021_lead_SR_extractions_plus_2024_lead_extractions_lessthan2000edit.xlsx"),sheetName = "extraction")
data2<-read.xlsx(paste0(dir,"FILEPATH/NHANES_2015-2018_tabulation_extraction.xlsx"),sheetName = "extraction")

#remove the description column
data<-setDT(data)[-1,]
data2<-setDT(data2)[-1,]

#rbind the 2 together
data<-rbind(data,data2)

# remove all NAs in the nid column
data<-data[!is.na(nid),]

# edits #######################################
#add a variance column
data[,variance:=NA]

#change mean to be val
setnames(data,"mean","val")

#delete the current is_outlier column
data$is_outlier<-NULL

#change outlier to be is_outlier
setnames(data,"outlier","is_outlier")

#fill in the outlier column
data[is.na(is_outlier),is_outlier:=0]

#add year_id
data[,year_id:=year_end]

#change year start and end
setnames(data,c("year_start","year_end"),c("year_start_og","year_end_og"))

#make a year_start and end column
data[,':='(year_start=year_id,
           year_end=year_id)]

#remove the NA. column
data$NA.<-NULL

# add the age_group_id and age start and age end columns
data[,':='(age_group_id=22,
           age_start=0,
           age_end=125)]

#change mean_unit to be a character and change N/A to be NA
data$mean_unit<-as.character(data$mean_unit)
data[mean_unit=="N/A",mean_unit:=NA]
data[mean_unit=="μg/dL;",mean_unit:="μg/dL"]

#fill in IQR flag column
data[is.na(IQR_flag),IQR_flag:=0]


# covariate edits #################################
#Change the cv_plasma columns to be binary
data[cv_plasma==1,cv_plasma_type:=1] #yes they used plasma
data[cv_plasma!=0,cv_plasma_type:=0] #no they did not use plasma
data[cv_plasma==3, cv_serum:=1] #yes they used serum
data[cv_plasma!=3, cv_serum:=0] #no they did not use serum
data[cv_plasma==2, cv_b_type_known:=0] #blood type is unknown
data[cv_plasma!=2, cv_b_type_known:=1] #blood type was known

data$cv_plasma<-NULL

#change the cv_mean_type column to be binary
data[cv_mean_type==0,cv_geomean:=1]
data[cv_mean_type==1,cv_arth_mean:=1]
data[cv_mean_type==2,cv_mean_unk:=1]
data[cv_mean_type==3,cv_median:=1]

data$cv_mean_type<-NULL

#fill in the cv columns with 0s if NA
data[is.na(cv_plasma_type),cv_plasma_type:=0]
data[is.na(cv_serum),cv_serum:=0]
data[is.na(cv_b_type_known),cv_b_type_known:=0]

data[is.na(cv_geomean),cv_geomean:=0]
data[is.na(cv_arth_mean),cv_arth_mean:=0]
data[is.na(cv_mean_unk),cv_mean_unk:=0]
data[is.na(cv_median),cv_median:=0]

# add age data ########################
data[,':='(age_group_id=22,
           age_start=0,
           age_end=125)]

# Units #############################
#some of the rows are in ug/L rather than ug/dL. We need to convert those to dL
data[,(c("standard_deviation","variance","sample_size","standard_error","upper","lower","val")):=lapply(.SD,as.numeric),
     .SDcols = c("standard_deviation","variance","sample_size","standard_error","upper","lower","val")]

#first lets make OG columns
data[,':='(val_og=val,
           upper_og=upper,
           lower_og=lower,
           standard_error_og=standard_error,
           standard_deviation_og=standard_deviation,
           variance_og=variance,
           mean_unit_og=mean_unit)]



# ug/L and pbb (ppb is 1 ug/L)
data[mean_unit_og=="μg/L" | mean_unit_og==" μg/L" | mean_unit_og=="µg/L" | mean_unit=="ppb" | mean_unit_og=="ng/ml" | mean_unit_og=="ng/mL",
     ':='(mean_unit="μg/dL",
                            val=val_og/10,
                            upper=upper_og/10,
                            lower=lower_og/10,
                            standard_error=standard_error_og/10,
                            standard_deviation=standard_deviation_og/10,
                            variance=variance_og/10)]

# mg/L
data[mean_unit_og=="mg/L",
     ':='(mean_unit="μg/dL",
          val=val_og*10,
          upper=upper_og*10,
          lower=lower_og*10,
          standard_error=standard_error_og*10,
          standard_deviation=standard_deviation_og*10,
          variance=variance_og*10)]



# variance ##############################

# variance is standard deviation^2 so, calculate that here
data[is.na(variance) & !is.na(standard_deviation), variance:=((standard_deviation)^2)]

#now calcualte variance using standard error
data[is.na(variance) & !is.na(standard_error) & !is.na(sample_size), variance:=((sqrt(sample_size)*standard_error)^2)]

#calculate variance using the UIs
data[is.na(variance) & !is.na(upper) & !is.na(lower) & !is.na(sample_size),variance:=((sqrt(sample_size)*(upper-lower))/3.92)]

## regression #########################
#if a row still does not have a variance value, then we need to predict what the variance would be. 

#first run a linear regression between mean, sample size and variance
model<-lm(standard_deviation ~ val, data[!is.na(standard_deviation)])

# now predict out the variance
data[is.na(variance),standard_deviation:=predict(model,.SD),.SDcols=c("val")]
data[is.na(variance),variance:=(standard_deviation^2)]


#write to excel to upload in epiuploader
write.xlsx(data,paste0(dir,"lead_exp_bundle_upload.xlsx"),sheetName = "extraction", row.names = F)










