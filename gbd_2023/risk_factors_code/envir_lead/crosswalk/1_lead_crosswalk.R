## crosswalks for lead exposure (crosswalking to arithmetic mean and to national-level urbanicity)

rm(list=ls())

################ Libraries #################################
library(data.table)
library(magrittr)
library(openxlsx)
library(readr)
library(reticulate)
reticulate::use_python("FILEPATH")
cw <- import("crosswalk")

# values
release<-16 #release id
loc_set<-22 #location_set_id
gbd<-"GBD2022" #for filepaths
input_filepath<-paste0("FILEPATH/",gbd,"/FILEPATH/age_split.csv") #filepath to age & sex split data
output_filepath<-paste0("FILEPATH/",gbd,"/FILEPATH/lead_cw_final_new_urb.xlsx")


############### Import basics ######################################
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_location_metadata.R")
locs <- get_location_metadata(location_set_id = loc_set, release_id=release)

########## Import Lead sex split ###########################
# read in dataset (post sex split)
dt<-fread(input_filepath)

setnames(dt,"val","mean")

# CROSSWALK 1 - mean type ##########################################################################################
#make a copy of dt, in a new dataset called mean_xw
mean_xw <- copy(dt)

## Establish means ##############################
# add reference/alternative dummy vars (mean type)
#Arithmetic mean
mean_xw[is.na(am_mean) & (mean_type == "am_mean"| cv_mean_unk==1), am_mean := 1] 
mean_xw[is.na(am_mean), am_mean := 0] 

#Geometric mean
mean_xw[is.na(gm_mean) & mean_type == "gm_mean", gm_mean := 1]
mean_xw[is.na(gm_mean), gm_mean := 0]

#median
mean_xw[is.na(median) & mean_type == "median", median := 1]
mean_xw[is.na(median), median := 0]

## Set up matching data frame #################################
#Only keep the listed columns
mean_xw_clean <- mean_xw[, .(nid, location_id, ihme_loc_id, location_name, sex, sex_id, age_start, age_end, age_group_id, year_start, year_end, year_id,
                   mean, standard_error, standard_deviation, variance, sample_size, measure, mean_type, am_mean, gm_mean, median,is_outlier)]

## Match am_mean to gm_mean and median ###################################
#Currently there are multiple rows for the same location and sex. One row for each mean type: am_mean, gm_mean, and median.
#Now we want to pair each am_mean with its respective gm_mean and median. So instead of 3 rows, there will be 2 rows for each location-sex combo.
#This also calculates the ratio and the standard error of the ratio (ratio: alt/ref)

# #create a column that we will be matching on location, sex, and year
locs_level<-locs[,.(location_id,level)]

mean_xw_clean<-merge(mean_xw_clean,locs_level,by="location_id",all.x=T)

mean_xw_clean<-mean_xw_clean[level==3]

mean_xw_clean[,key:=paste0(location_id,"_",sex_id,"_",year_id)]

ref<-copy(mean_xw_clean)[am_mean==1]
alt<-copy(mean_xw_clean)[gm_mean==1 | median==1,]

#ref and alt to all of the column names
colnames(ref)<-paste(colnames(ref),"ref",sep="_")
colnames(alt)<-paste(colnames(alt),"alt",sep="_")

#Now make the matched pairs
data_combo<-merge(ref,alt,by.x="key_ref", by.y="key_alt",allow.cartesian = T)


## Log and Diff #################################
#first we need to do a delta transformation to convert the means and se's from linear to log space
data_combo[,c("log_mean_ref","log_se_ref")]<-cw$utils$linear_to_log(mean=array(data_combo$mean_ref),
                                                                    sd=array(data_combo$standard_error_ref))

data_combo[,c("log_mean_alt","log_se_alt")]<-cw$utils$linear_to_log(mean=array(data_combo$mean_alt),
                                                                    sd=array(data_combo$standard_error_alt))

#now calculate the difference
data_combo[,':='(diff_log_mean=log_mean_alt-log_mean_ref,
                 diff_log_se=sqrt(log_se_alt^2+log_se_ref^2))]

## Clean Up ###################################

mean_xw_matched <- merge(locs[, .(super_region_name, location_id)], data_combo, by.y = "location_id_ref",by.x="location_id") # add super region

# add identifier for each unique super-region/sex combo (to be used as a random effect)
mean_xw_matched[, sr_sex := .GRP, by = c("super_region_name","sex_ref")] 

#add a column that identify the alt and ref definitions
mean_xw_matched[,':='(alt_def=mean_type_alt,
                      ref_def=mean_type_ref)]

#remove outliers
mean_xw_matched<-mean_xw_matched[is_outlier_ref==0 & is_outlier_alt==0,]

## Prepare model ###########################################
#Place your data in CWdata to put it in the correct format to be used in the model
#when you use CWdata, you just need to indicate what each variable is
lead_mean_data <- cw$CWData(
  df = mean_xw_matched, #Your dataframe
  obs = "diff_log_mean", #which one is your observation
  obs_se = "diff_log_se", #which one is the se of your observation
  alt_dorms = "alt_def", #column that defines your alternate (geomean and median)
  ref_dorms = "ref_def", #column that defines your reference (arithmetic mean)
  study_id = "sr_sex" # random effect, super-region/sex combos
)

## Run the model ##############################
#set the parameters
lead_mean_model <- cw$CWModel(
  cwdata = lead_mean_data,
  obs_type = "diff_log", #Can choose between diff_log or diff_logit. 
  cov_models = list(
    cw$CovModel("intercept")#This makes it so the regression has an intercept
  ),
  gold_dorm = "am_mean", #Indicated which of the ref_dorms is the gold standard. This is useful if you have multiple
  use_random_intercept = TRUE 
)

#run the model
lead_mean_model$fit()

#get the coefficients of the model
mean_coef<-lead_mean_model$create_result_df()

## Adjust data #################################

to_adjust <- mean_xw[mean_type != "am_mean"] #only keep the rows that are not the am_mean type
#remember that mean_xw is the old data table, before we cleaned it up

#"adjust_orig_vals adjusts biased observations in the original dataset using the meta-regression model to predict the degree of bias"
#So, whats actually happening is that the mr_brt_model (above) is finding what the bias conversion is for each study (for example 0.8)
#and then adjust_orig_vals is used to replace the study's values with new values by multiplying the original value by the bias conversion factor
lead_mean_pred <- lead_mean_model$adjust_orig_vals(
  df = to_adjust,
  orig_dorms = "mean_type", #which column defines the alt
  orig_vals_mean = "mean", 
  orig_vals_se = "standard_error"
)
#This gives you mean_adj, se_adj and pred_log, pred_se_log.

setDT(lead_mean_pred) #Make it into a data.table

setnames(lead_mean_pred, names(lead_mean_pred), c("mean_adj","se_adj","pred_log","pred_se_log","data_id")) #Change all of the column names in this order

to_adjust <- cbind(to_adjust, lead_mean_pred) #Combined the columns of to_adjust and lead_mean_pred.
#Essentially we are combining the adjusted and predicted columns with the original data (excluding the am_mean rows)

to_adjust[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq] #If the crosswalk_parent_seq is NA, then make it equal to the seq column

## Combine crosswalked data with non-crosswalked data #############################################
#Combine the rows that are am_mean from mean_xw to the rows of to_adjust. If one of them do not have the same columns, Fill in the empty cells with NAs
mean_xw_final <- rbind(mean_xw[mean_type == "am_mean"], to_adjust, fill = TRUE)
#We are doing this because to_adjust are all of the rows from mean_xw EXCEPT for the am_mean rows. So, here we are adding them back in

# clean things up
mean_xw_final[is.na(mean_adj), mean_adj := mean] #If mean_adj is NA, make it the same as the mean column. This will be true for any rows that were am_mean,
#since they were not used in the adjusted model

#Change some column names to be more informative
setnames(mean_xw_final, c("mean","standard_error","mean_adj"), c("mean_og","se_og","val"))

#add in a standard error column and fill it in
mean_xw_final[mean_type == "am_mean", standard_error := se_og] # for unadjusted data points, use original (post sex split) SE

mean_xw_final[is.na(standard_error), standard_error := se_adj] # for adjusted data points, use adjusted SE

# CROSSWALK 2 - urbanicity ##########################################################################################
# Adjust data urbanicity to the national average
# here, the variable urban_study represents the urbanicity of the datapoint
# whereas the variable urban_loc represents the average urbanicity in that location (taken from the GBD urbanicity covariate)
# we want to crosswalk data point urbanicity to what we would expect it to be if representative of its location

## Retrieve urbanicity values ########################
# get urbanicity covariate
prop_urban <- get_covariate_estimates(covariate_id = 854, release_id = release)

## Clean up urbanicity values #############################
#off-set values of 0 or 1 because they will be logit inputs
#We are doing this because logit will go to infinity if the input value is 0 or 1
prop_urban[mean_value >= 1, mean_value := 0.99999]
prop_urban[mean_value <= 0, mean_value := 0.00001]

#Change mean_value to urban_loc
setnames(prop_urban, "mean_value", "urban_loc")

#Only keep the following 3 columns
prop_urban <- prop_urban[, .(location_id, year_id, urban_loc)]
#Again, urban_loc represents the average urbanicity in that location

## Merge mean_xw_final and prop_urban #############################
#Merge mean_xw_final, minus 3 columns, and prop_urban by the columns location_id and year_id. Keep all rows
urb_xw <- merge(mean_xw_final[, -c("se_adj","pred_log","pred_se_log","data_id")], prop_urban, by = c("location_id","year_id"), all.x=T)

# fix typo
urb_xw[representative_name == "Nationall representative", representative_name := "Nationally representative"]

## urban_study col #########################
#Again, urban_study represents the urbanicity of the datapoint while urban_loc represents the average urbanicity in that location
# When point data is representative, copy over location's prop_urban, otherwise use the data to infer urbanicity
# When urbanicity unknown, copy over location's prop_urban as well

#If "Nationally representative only", "Nationally representative", or "Representative for subnational location only" is in representative_name OR
#urbanicity_type=0 then make the urban_study column equal to the urban_loc column
#Essentially this means that if the urban_loc column is already nationally representative them make urban_study equal to it
urb_xw[representative_name %in% c("Nationally representative only","Nationally representative","Representative for subnational location only") |
                urbanicity_type == "0", urban_study := urban_loc] # for representative data points, use the urbanicity from the GBD covariate
#urbanicity_type=0 means that it is unknown. But since it is nationally representative, we can just use urban_loc

urb_xw[is.na(urban_study) & urbanicity_type %in% c("2","Urban"), urban_study := 0.99999] # for urban data points, set to 0.99999

urb_xw[is.na(urban_study) & urbanicity_type %in% c("3","Rural"), urban_study := 0.00001] # for rural data points, set to 0.00001

urb_xw[is.na(urban_study) & urbanicity_type %in% c("Mixed/both","Suburban"), urban_study := 0.5] # for suburban, set to 0.5

urb_xw[is.na(urban_study) & urbanicity_type %in% c("1","Unknown"), urban_study := urban_loc] # for data points missing urbanicity type, use GBD covariate
#Since it is "unknown" and 1 (mixed/both) we will just use the urban_loc for urban_study

urb_xw[grepl("IND_",ihme_loc_id), urban_study := urban_loc] # use GBD covariate for all of India's subnationals
#if the first part of ihme_loc_id starts with IND_ then fill urban_study with urban_loc

#one of the studies did not have the urbanicity_type col filled out. I looked into it and said it was conducted in a highly urban capital, so marking it as urban
urb_xw[nid==496537, urban_study:=0.99999]

## ref and alt col ###########################
# reference data points are the ones where the study urbanicity (urban_study) equals the location average urbanicity (urban_loc)
urb_xw[urban_loc == urban_study, ':='(urb_ref = 1, urb_alt=0)]
urb_xw[urban_loc != urban_study, ':='(urb_ref = 0,urb_alt=1)]
#why are the ref points the ones that are equal to the national average for urbanicity?
#This is because the other studies are site-specific, which is NOT what we want. We want the studies to be representative of the
#entire country. SO to convert them to national-type data we will be use crosswalk to model the bias

## urban_type #############################
# summary column
urb_xw[urb_ref == 1, urban_type := "urb_ref"]
urb_xw[urb_alt == 1, urban_type := "urb_alt"]

## Clean up columns of urb_xw #########################
# set up matching data frame
urb_xw_clean <- urb_xw[, .(nid, location_id, ihme_loc_id, location_name, sex, sex_id, age_start, age_end, age_group_id, year_start, year_end, year_id,
                   val, standard_error, standard_deviation, variance, sample_size, measure,urban_type,is_outlier)]

## Match urb_ref to urb_alt #######################
#reminder: reference data points are the ones where the study urbanicity (urban_study) equals the location average urbanicity (urban_loc), alt is everything else

#create a column that we will be matching on, for this we will match on location and sex
urb_xw_clean[,key:=paste0(location_id,"_",sex_id,"_",year_id)]


ref<-copy(urb_xw_clean)[urban_type=="urb_ref"]
alt<-copy(urb_xw_clean)[urban_type=="urb_alt"]

#ref and alt to all of the column names
colnames(ref)<-paste(colnames(ref),"ref",sep="_")
colnames(alt)<-paste(colnames(alt),"alt",sep="_")

#Now make the matched pairs
data_combo<-merge(ref,alt,by.x="key_ref", by.y="key_alt",allow.cartesian = T)

## Log and Diff ##########################
data_combo[,c("log_mean_ref","log_se_ref")]<-cw$utils$linear_to_log(mean=array(data_combo$val_ref),
                                                                    sd=array(data_combo$standard_error_ref))

data_combo[,c("log_mean_alt","log_se_alt")]<-cw$utils$linear_to_log(mean=array(data_combo$val_alt),
                                                                    sd=array(data_combo$standard_error_alt))

#now calculate the difference
data_combo[,':='(diff_log_mean=log_mean_alt-log_mean_ref,
                 diff_log_se=sqrt(log_se_alt^2+log_se_ref^2))]

## Clean up ###############################
#Merge locs and urb_xw_matched by location_id
urb_xw_matched <- merge(locs[, .(super_region_name, location_id)], data_combo, by.x = "location_id",by.y="location_id_ref") # add super region (to be used as a random effect)

# add identifier for each unique super-region/sex combo (to be used as a random effect)
urb_xw_matched[, sr_sex := .GRP, by = c("super_region_name","sex_ref")] 

#remove outliers
urb_xw_matched<-urb_xw_matched[is_outlier_ref==0 & is_outlier_alt==0,]

## Prepare model #####################################
lead_urb_data <- cw$CWData(
  df = urb_xw_matched, #dataframe
  obs = "diff_log_mean", #ratio
  obs_se = "diff_log_se", #ratio_se
  alt_dorms = "urban_type_alt", #alternative definition
  ref_dorms = "urban_type_ref", #reference definition
  study_id = "sr_sex" # random effect
)

##Run Model ####################################
#set the parameters
lead_urb_model <- cw$CWModel(
  cwdata = lead_urb_data, #prepared data
  obs_type = "diff_log", #Can choose between diff_log or diff_logit
  cov_models = list(
    cw$CovModel("intercept")#This makes it so the regression has an intercept
  ),
  gold_dorm = "urb_ref", #This is the gold standard
  use_random_intercept = TRUE #use a random intercept
)

#run the model
lead_urb_model$fit()

#get the coefficients
urb_coef<-lead_urb_model$create_result_df()

## Adjust data ################
#only keep the rows that have the alternate urbanicity
to_adjust <- urb_xw[urban_type == "urb_alt"]

#adjust the values
lead_urb_pred <- lead_urb_model$adjust_orig_vals(
  df = to_adjust, #dataframe that needs adjusted
  orig_dorms = "urban_type", #based the adjustments on this column
  orig_vals_mean = "val", 
  orig_vals_se = "standard_error"
)

#Change lead_urb_pred into a data.table
setDT(lead_urb_pred)

#Change all of the column names of lead_urb_pred to be the following
setnames(lead_urb_pred, names(lead_urb_pred), c("mean_adj","se_adj","pred_log","pred_se_log","data_id"))

#Combine to_adjust and lead_urb_pred
to_adjust <- cbind(to_adjust, lead_urb_pred)

#Update the crosswalk_parent_seq column
to_adjust[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]

####### combine crosswalked data with non-crosswalked data #############################
#Combine the urb_ref rows with to_adjust. This is because urb_ref was removed from to_adjust prior to it being adjusted
#since it did not need to be adjusted. This is us adding them back in
urb_xw_final <- rbind(urb_xw[urban_type == "urb_ref"], to_adjust, fill = TRUE)

######## Clean up ##################################
#If mean_adj is NA then fill it in with val. This will be true for all rows that are not urb_ref
urb_xw_final[is.na(mean_adj), mean_adj := val]

#change the following 3 columns
setnames(urb_xw_final, c("val","standard_error","mean_adj"), c("mean_post_mean_type_xw", 
                                                               "se_post_mean_type_xw", 
                                                               "val"))

#Update standard error columns
urb_xw_final[urban_type == "urb_ref", standard_error := se_post_mean_type_xw] # for unadjusted data points, use original (post-mean type crosswalk) SE

urb_xw_final[is.na(standard_error), standard_error := se_adj] # for adjusted data points, use adjusted SE

####### Finalize ###############################
#If crosswalk_parent_seq is not NA, make it NA
lead_xw_final<-urb_xw_final[!is.na(crosswalk_parent_seq), seq := NA]

#for some reason this note_SR keeps changing it, so it is longer than 2000 characters. Updating it here
lead_xw_final[nid==131657,note_SR:="Some of the residents lived in urban areas, mostly rural. However, the code currently classifies combined settings as nationally representative. This should be something we change in the future."]
lead_xw_final[nid==122592,note_SR:="Children were primary school age I estimated their age based on looking online to see avg start/end age for Egyptian primary schools"]
lead_xw_final[nid==355342,note_SR:="Study gives no information about age range of participants, other than workers, so set to 18-65 based on that information and mean/SD of age"]

#remove the group_review column, no longer needed
lead_xw_final$group_review<-NULL

#update upper and lower values
lead_xw_final[,':='(upper=val+(1.96*standard_error),
                    lower=val-(1.96*standard_error))]

# Save ########################
write.xlsx(lead_xw_final, output_filepath,sheetName="extraction",rowNames=F)


