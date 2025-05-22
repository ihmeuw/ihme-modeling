#######################################################
# Purpose: read in the RR data for the specified cause, prepare for MR-BERT, establish covariates, and run
# July 29, 2019
######################################################

library(msm)
library(dplyr)
library(data.table)
library(parallel)
library(readr)
library(ggplot2)
library(stringr)
library(digest)
library(googledrive)

source(FILEPATH) # lots of helpful functions in here!
locs<-get_location_metadata(location_set_id = 22, gbd_round_id = 7)

date <- gsub(" |:","-",Sys.time())

# get age metadata to use for age adjustments:
age_dt <- get_age_metadata(19,gbd_round_id = 7)
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_id == 235, age_group_years_end := 99]
age_dt[,age_group_identity := paste(age_group_years_start,age_group_years_end,sep=":")]

### booleans:
recreate_ref <- F # if you want to recreate the map of which terms should be mapped to what
new_upload <- F # if you want to create a new upload to the google drive
adjust_for_confounders <- F # if you want to adjust the model by confounders, instead of just simply predicting using exposure

######################################################################################## 
############################ DATA PREP #####################################

## read in the data:
cause_choice_c <- "asthma"

risk_reduction <- T # if you want to run a risk reduction curve, versus just a normal exposure curve
expand <- F

ref_path <- FILEPATH
other_confounders_map <- fread(FILEPATH)
map <- fread(FILEPATH)
unexp_map <- fread(FILEPATH)
cause_choice_formal_c <- map[cause_choice == cause_choice_c,cause_choice_formal]
cause_choice <- cause_choice_c
cause_choice_formal <- cause_choice_formal_c

ref_data <- fread(ref_path)

risk_choice <- "Smoking"
if (cause_choice %like% "cancer" | cause_choice == "leukemia"){
  cancer <- T
} else{
  cancer <- F
}

# find the best sheet for 2019:
setwd(FILEPATH)
path_metadata <- drive_download(as_id(ID), type="csv", overwrite = T, path=paste0(FILEPATH))
path_metadata <- as.data.table(fread(path_metadata$local_path))

# read in data:
data <- read_in_data(data_path,path_metadata,cause_choice_formal,cause_choice_c,cancer,risk_reduction)
######################################################################################## 


######################################################################################## 
############################ MAPPING AND CLEANING #####################################
# study design
bad_designs <- c("Retrospective cohort")
data <- data[!(design %in% bad_designs)]

# only limit to exposure ranges when we aren't doing a risk reduction analysis
# because for risk reduction we want RRs for all current smokers to represent the YSQ = 0 values
if(!risk_reduction){
  data <- data[(risk_mapping == "aggregate" & outcome == "Fractures") |
                 (!is.na(custom_exp_level_lower) | !is.na(custom_exp_level_upper) | !is.na(cc_exp_level_rr) | !is.na(cohort_exp_level_rr))]
  
}


# make a few fixes to the data so that some pieces are included that otherwise would not have the continuous data we would expect
data[(is.na(custom_exp_level_lower) & is.na(custom_exp_level_upper)) & (!is.na(cohort_exp_level_rr)), c("custom_exp_level_upper","custom_exp_level_lower") := list(cohort_exp_level_rr,cohort_exp_level_rr)]
data[(is.na(custom_exp_level_lower) & is.na(custom_exp_level_upper)) & (!is.na(cc_exp_level_rr)), c("custom_exp_level_upper","custom_exp_level_lower") := list(cc_exp_level_rr,cc_exp_level_rr)]

# fix a particular NID:
data[nid==338782, cc_exp_unit_rr := cc_exp_level_rr]

# map on the reference file and return a cleaned dataset
data <- create_reference(ref_path,data)


# add unit variables to the reference data if they did not get mapped
#### this code will error because ref_data is in a function now. but, that is fine because we need to fix the definition anyway
units_to_map <- data[is.na(map) & (!is.na(cohort_exp_unit_rr) | !is.na(cc_exp_unit_rr))]
for (j in c(units_to_map[!is.na(cohort_exp_unit_rr),cohort_exp_unit_rr],units_to_map[!is.na(cc_exp_unit_rr),cc_exp_unit_rr])){
  print(j)
  row <- data.table(exposure_units = j, map = "NEEDS MAP")
  ref_data <- rbind(ref_data, row,fill=T)
}
# add exposure definitions to the reference data if they did not get mapped
def_to_map <- data[is.na(map_defn) & (!is.na(cohort_exposed_def) | !is.na(cc_exposed_def))]
for (j in unique(c(def_to_map[!is.na(cohort_exposed_def),cohort_exposed_def],def_to_map[!is.na(cc_exposed_def),cc_exposed_def]))){
  print(j)
  row <- data.table(exposure_defns = j, map_defn = "NEEDS MAP")
  ref_data <- rbind(ref_data, row,fill=T)
}

if (nrow(def_to_map) > 0 | nrow(units_to_map) > 0){
  write.csv(ref_data,gsub(".csv","_to_map.csv",ref_data))
  message("Some definitions of units were not mapped!! PLEASE edit reference file and re-save with the appropriate name.")
}


# then take out the rows where there are errors
data <- data[(map != "error" | is.na(map)) & (map_defn != "error" | is.na(map_defn))] # these illogical maps take out about 7000 rows

# subset to the effects we can actually measure:
data <- data[!(effect_size_measure %in% c("Direct PAF"))]


## DETERMINE EXPOSURE UNITS AND DEFINITIONS TO USE (DEPENDS ON IF IT IS A CURRENT OR FORMER SMOKING ANALYSIS)
data <- select_units_and_defs(data,risk_reduction)

## CLEAN LOTS OF MISTAKES AND INDICATORS
data <- clean_up_time(data,risk_reduction)

######################################################################################## 



######################################################################################## 
############################ PREPARE CONFOUNDERS #####################################

data[percent_male==1 | percent_male==0, confounders_sex := "1"]

# map the other confounders for the data that have them
# fix a specific issue with some confounders:
data$confounders_other <- gsub("\"area\"","area",data$confounders_other)

## IF THERE ARE VALUES IN THE CONFOUNDERS_OTHER COLUMN, THEN MAP THEM
if(nrow(data[!is.na(confounders_other) & confounders_other != ""]) > 0){
  data_oth_confound <- other_confounders_reshape(df=data[!is.na(confounders_other) & confounders_other != ""],map=other_confounders_map)
  if(F){
    # run this again in order to run the above function (once the mapping file is updated)
    other_confounders_map <- fread(FILEPATH)
  }
  
  # if this errors out, you need to edit the file above
  nrow(data_oth_confound)
  
  data_no_oth_confound <- data[is.na(confounders_other) | confounders_other == ""]
  data_no_oth_confound$confounders_other <- NULL
  data_no_oth_confound$adjustment <- 0
  
  # combine the data with and without other confounders
  data <- rbind(data_oth_confound, data_no_oth_confound,fill=T)
} else(
  data[,adjustment := 0]
)

nrow(data)
# fill in missing data with 0's:
missing_cols <- setdiff(names(data_oth_confound),names(data_no_oth_confound))
data[, (missing_cols) := lapply(.SD, function(x){x[is.na(x)] <- 0; x}), .SDcols = missing_cols]

# add to the adjustment by counting up the number of standard confounders as well:
# also subtract off the count for to_map for now since these are not trustworthy mappings
stand_confounders <- names(data)[names(data) %like% "confounders_" & !(names(data) %like% "confounders_oth_")]
data[,(stand_confounders):= lapply(.SD, as.numeric), .SDcols = stand_confounders]

data$add_adju <- rowSums(data[,(stand_confounders),with=F],na.rm = T)
data[is.na(add_adju), add_adju := 0]
if ("confounders_oth_to_map" %in% names(data)){
  data[confounders_oth_to_map == 1, add_adju := add_adju - 1]
}

data[,adjustment := adjustment + add_adju]

# we can take this adjustment to determine the range of adjustments, and thus fill out the bias covariates on confounders
# take NID into consideration here so that one study that just adjusted for a bunch of stuff cannot dicatate these divisions

######################################################################################## 



######################################################################################## 
############################ CREATE BIAS COVARIATES #####################################

data_covs <- create_bias_covs(data) # uses info in columns to create bias covariates
data <- copy(data_covs)

################### create cv_confounding_controlled ####################
# this varies depending on the cause. so, although we create a basic covariate in create_bias_covariates(), we want to make this more nuanced
# we have to specify what confounders are important
data <- outcome_spec_confounders(data,cause_choice_c,date)

######################################################################################## 


######################################################################################## 
########################## UPLOAD TO AND DOWNLOAD FROM GOOGLE DRIVE ##########################

## LOG TRANSFORM THE EFFECT SIZE AND THE STANDARD ERROR OF THE EFFECT SIZE USING THE DELTA TRANSFORM
data <- log_transform_mrbrt_effect(data)

## CHECK IF ANY GBD 2017 NIDS ARE MISSING FROM THE DATA:
compare_nids_gbd2017(data,metric_name)

# copy the data so we can merge the confounders back on later
full_data <- copy(data)


## SUBSET TO COLUMNS THAT BEST DEFINE EACH ROW; USED FOR UNIQUE HASH CREATION AND UPLOAD TO GOOGLE DRIVE
data <- unique(data[,.(nid,effect_size,lower,upper,cc_exposed_def,cc_unexposed_def,cohort_exposed_def,cohort_unexp_def,outcome_def,mean_exp,se_effect,
                       age_start,age_end,
                       percent_male,custom_exp_level_lower,custom_exp_level_upper,custom_unexp_level_lower,
                       custom_unexp_level_upper,note_sr,adjustment)])



### create a hash using the digest function
data_coded <- data %>%
  rowwise() %>% 
  do(data.frame(., hash = digest(.)))
data_coded <- as.data.table(data_coded)


#### write out the table so we can upload to a google sheet:
# sort the data by NID and mean exposure unit
setorderv(data_coded,c("nid","mean_exp","effect_size"))

## DOWNLOAD THE REFERENCE FILE THAT TELLS US THE LOCATION OF THE OUTCOME-SPECIFIC DIRECTORY WITH ALL OF THE FILES
drive_key <- download_ref_dir(data_coded,date,cause_choice_c)

# if you want to upload an entirely new sheet instead of appending to a new one
if(new_upload){
  upload_new(data_coded,cause_choice_c,date,drive_key)
  stop("Edit the newly uploaded file AND change the hash ID in the reference file.")
}

## DOWNLOAD THE FILE FROM GOOGLE DRIVE THAT IS READY TO BE MERGED ONTO YOUR DATA
df_drive_data <- download_file_to_use(cause_choice_c,date,risk_reduction)

######## then, read in the coded data from the google drive and merge onto the data so that we know which rows we should use:
data <- copy(data_coded)
# see if there are any hash codes that don't match up
message(paste0("Number of hashes in data but not in reference file: ",length(setdiff(data$hash, df_drive_data$hash))))
message(paste0("Number of hashes in reference file but not data: ",length(setdiff(df_drive_data$hash,data$hash))))

data <- as.data.table(data)

# if there are hashes missing, we want to add these new rows onto the existing reference file
# don't worry about the hashes that are in the reference file but not the data, since these are always being updated
if(length(setdiff(data$hash, df_drive_data$hash)) > 0){
  new_hashes <- setdiff(data$hash, df_drive_data$hash)
  new_data <- copy(data[hash %in% new_hashes])
  new_reference <- rbind(df_drive_data,new_data,fill=T)
  
  # re-save the new reference file
  write.csv(new_reference, paste0(FILEPATH,cause_choice_c,"/",cause_choice_c,"_",date,".csv"))
  
  # this will upload to a new key value (because not sure how to overwrite the file), so the reference sheet needs to be updated and then re-run
  drive_upload(media=paste0(FILEPATH,cause_choice_c,"/",
                            cause_choice_c,"_",date,".csv"), path=as_id(drive_key),type="spreadsheet")
  
  
  stop("update the new sheet online!")
  
  ## ONCE NEW SHEET IS FILLED OUT, DOWNLOAD AND USE
  df_drive_data <- download_file_to_use(cause_choice_c,date,risk_reduction)
  
  # read out the stats again so that we know if we are comfortable going forward:
  message(paste0("Number of hashes in data but not in reference file: ",length(setdiff(data$hash, df_drive_data$hash))))
  message(paste0("Number of hashes in reference file but not data: ",length(setdiff(df_drive_data$hash,data$hash))))
}

######################################################################################## 


######################################################################################## 
############################# PREPARE FOR MODELING #############################

# if there are no missing hashes, then merge on the reference file to determine which rows to keep
drive_cols <- c("hash","include_in_model")
if(risk_reduction){
  drive_cols <- c(drive_cols,"former_smok_match")
}

df_drive_data <- df_drive_data[,(drive_cols),with=F]
data <- merge(data,df_drive_data,by="hash")

# and then only keep the specified rows
data_to_use <- data[include_in_model == 1]
covs <- c("mean_exp")


# merge back on the confounders so we know what was controlled for
nrow(data)
data_int <- copy(data)

confounder_columns <- names(full_data)[names(full_data) %like% "cv_" | names(full_data) %like% "confounder"]
all_cols <- c(confounder_columns,
              "nid","effect_size", "lower", "upper", "percent_male", "custom_exp_level_lower",
              "custom_exp_level_upper","study_name",
              "effect_size_log", "se_effect_log","map","map_defn")
if(risk_reduction){
  all_cols <- c(all_cols,"map_unexp")
}

data <- merge(data,unique(full_data[,(all_cols),with=F]),by=c("nid","effect_size","lower","upper","percent_male","custom_exp_level_lower","custom_exp_level_upper"))

## GET RID OF DUPLICATES THAT HAVE BEEN MERGED ON
## we will use the cv_confounding_uncontrolled for this, since this takes into consideration the confounders that we care about for each cause
## if there are rows with the same number for cv_confounding_uncontrolled, we will just pick one of them
### we need to remember that this decision is being made so that we can best interpret the final results

# get rid of replicates, because there are some models that have the same exact values and confounders because of multiple sites being measured
data <- unique(data)
# now, figure out which of these studies still have replicates!
data[,rep := .N, by=c("nid","effect_size","lower","upper","percent_male","custom_exp_level_lower","custom_exp_level_upper")]
data[rep>1]

# get the minimum value for cv_confounding_uncontrolled
data[,min_confounding := min(cv_confounding_uncontrolled), by=c("nid","effect_size","lower","upper","percent_male","custom_exp_level_lower","custom_exp_level_upper")]
data <- data[cv_confounding_uncontrolled == min_confounding]

# for the rows that are still repeated, just pick one
data[,index := .I]
data[,max_index_dup := max(index), by=c("nid","effect_size","lower","upper","percent_male","custom_exp_level_lower","custom_exp_level_upper")]
data <- data[rep == 1 | (rep > 1 & index == max_index_dup)]

# and then plot to see what the data landscape looks like
ggplot(data,aes(mean_exp,effect_size,color=nid)) + geom_point()



## write out data for experimentation purposes in the python version of mr-brt
write.csv(data,paste0(FILEPATH,cause_choice_c,"_",metric,"_",date,"_test.csv"))
message(paste0(FILEPATH,cause_choice_c,"_",metric,"_",date,"_test.csv"))