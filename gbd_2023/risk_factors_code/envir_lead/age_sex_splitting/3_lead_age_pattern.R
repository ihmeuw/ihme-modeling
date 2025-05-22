# age splitting for lead
#this happens after we do sex splitting

rm(list=ls())

# Libraries #################################
library(reticulate)
library(readr)
library(ggplot2)

#MRBRT
reticulate::use_python("FILEPATH")
mr <- import("mrtool")
cw <- import("crosswalk")

#shared functions
source("FILEPATH/get_age_metadata.R")

# values ####################################
release<-16 #release_id
gbd<-"GBD2022" #for filepath

# Import ##################################
#read in the data that was sex split
data<-fread(paste0("FILEPATH/",gbd,"/FILEPATH/lead_sex_split.csv"))

#only keep the columns that we need
data <- data[, .(seq,crosswalk_parent_seq,nid, underlying_nid, year_start, year_end, age_end_orig, age_start_orig, age_group_id,val, standard_error, sex_id, location_id, year_id,upper,lower,
                 sample_size,standard_deviation,is_outlier)]

#age metadata
ages<-get_age_metadata(release_id = 16)[,c("age_group_id","age_group_name","age_group_years_start","age_group_years_end")]

#duplicate the age data to contain male and female
ages<-rbindlist(list(ages[, sex_id := 1], copy(ages)[, sex_id := 2]))

# data edits #############################
#calculate the age midpoint to be used in the MRBBRT model
data[,age_midpoint:=(age_start_orig + 1 + age_end_orig)/2]

# merge on ages ##########################
# Only keep the data that does not already have an age group id
data_age_wo<-data[is.na(age_group_id),]

data_age_wo$age_group_id<-NULL

#merge on ages
data_age_wo<-merge(data_age_wo,ages[,-c("sex_id")],all.x=T, by.x=c("age_start_orig","age_end_orig"), by.y=c("age_group_years_start","age_group_years_end"))

# merge back with the original dataset ###############################
#merge the data that did not have age group ids originally with the data that did
data<-data[!is.na(age_group_id)]

data<-rbind(data,data_age_wo,fill=T)

# Only keep data for model #############################
# we only want to keep the data that is already age-specific so we can run a model on it
non_split<-data[!is.na(age_group_id) & age_group_id!=22 & is_outlier==0,]

# calculate the log val and SE
non_split[,c("ln_val","ln_val_se")]<-cw$utils$linear_to_log(mean=array(non_split$val),
                                                       sd=array(non_split$standard_error))

#make sure everything is numeric
non_split$age_midpoint<-as.numeric(non_split$age_midpoint)
non_split$sex_id<-as.numeric(non_split$sex_id)

non_split[,age_midpoint:=age_midpoint]

# MRBRT #########################################
#load in dataframe
data_model<-mr$MRData()

data_model$load_df(non_split,
                   col_obs="ln_val",
                   col_obs_se="ln_val_se",
                   col_covs=list("age_midpoint","sex_id"), #we are not including sex_id as the age metadata does not include sex
                   col_study_id="nid")

#prep the model
model<-mr$MRBRT(
  data=data_model,
  cov_models=list(
    mr$LinearCovModel("intercept", use_re = TRUE),
    mr$LinearCovModel("age_midpoint"),
    mr$LinearCovModel("sex_id")
  )
)

#run the model
model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L) 

# prediction and draws ###########################################
#grab the age metadata and calculate the age midpoint to make predictions for
ages[,age_midpoint:=(age_group_years_start + 1 + age_group_years_end)/2]

#make sure the covariates are numeric
ages$age_midpoint<-as.numeric(ages$age_midpoint)
ages$sex_id<-as.numeric(ages$sex_id)


#make a mrbrt object for the values to predict on
age_pred_data<-mr$MRData()

#fill it in with the age metadata
age_pred_data$load_df(
  data=ages,
  col_covs=list("age_midpoint","sex_id")
)

#use the model to predict
ages$pred<-model$predict(data=age_pred_data)

#graph
ggplot(ages,aes(x=age_midpoint,y=pred))+
  geom_line()+
  facet_wrap(~sex_id)


#set up the number of samples we want
age_samples <- model$sample_soln(sample_size = 1000L)

#make an empty table
age_draws<-data.table()

#do this for each age group id
for(sex in unique(ages$sex_id)){

for (age in unique(ages$age_midpoint)) {
  print(paste0("sex: ",sex,", age: ",age))
  

  #make a mrbrt object for the values to predict on
  age_pred_data<-mr$MRData()
  
  #fill it in with the age metadata
  age_pred_data$load_df(
    data=ages[age_midpoint==age & sex_id==sex,],
    col_covs=list("age_midpoint","sex_id")
  )

  
  #use the model to predict
  age_pred<-model$predict(data=age_pred_data)

temp <- model$create_draws(
  data = age_pred_data,
  beta_samples = age_samples[[1]],
  gamma_samples = age_samples[[2]],
  random_study = TRUE
)
temp <- data.table(temp)

# rename columns
setnames(temp, paste0("V",1:1000), paste0("draw_",0:999)) 

#the draws are in log space, transform them into linear
temp[, (names(temp)) := lapply(.SD, exp)]

#add age group column
temp[,':='(age_midpoint=age,
           sex_id=sex)]

#rbind with age_draws
age_draws<-rbind(age_draws,temp)

}
}

#calculate the mean to double check
draw_columns <- grep("^draw_", names(age_draws), value = TRUE)
age_draws[,mean:= rowMeans(.SD, na.rm = TRUE), .SDcols = draw_columns]


# add age group ###########################################
#add the age group ids with the age patterns
age_draws<-merge(age_draws,ages[,-c("age_group_name")],by=c("age_midpoint","sex_id"))

#remove columns that we don't need anymore
age_draws<-age_draws[,-c("age_midpoint","mean")]

# Export ###########################################
write_excel_csv(age_draws,paste0("FILEPATH/",gbd,"/FILEPATH/age_split_pattern.csv"))






