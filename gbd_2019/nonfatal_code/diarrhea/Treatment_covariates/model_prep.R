###################################################################################
#####################################
## Date: 05/01/2018 | Updated: 01/03/2019
## Project: Child Care Covariates
## Purpose: Process collapsed data, add to existing data, and prep for model upload
########################################################################################################################

message("Setting up environment...")
rm(list=ls())
#library(testit)
library(data.table)

os <- .Platform$OS.type
if (os == "windows") {
 source("FILEPATH")
} else {
  source("FILEPATH")
}

# set arguments
input_dir <- paste0(fix_path("FILEPATH"))
output_dir <- paste0(fix_path("FILEPATH")) #path to model dataset directory
me <- "lri_antibiotics" #me name you want to collapse (diarrhea_ors, diarrhea_zinc, diarrhea_antibiotics, lri_antibiotics)
me_short <- "lri" #ors, zinc, lri, antibiotics (for diarrhea specifically)

# load libraries & functions
library(data.table)
source(fix_path("FILEPATH"))

# location hierarchy
locs <- get_location_metadata(gbd_round_id=6, location_set_id=22)[level>=3,.(ihme_loc_id,location_id)]

############################################## MODEL PREP #############################################################
message("Prepping collapsed data...")
message("Loading input dataset...")
df_new <- data.table(read.csv(input_dir))
message(paste0("New collapsed data has ",nrow(df_new)," rows."))
if("mean" %in% names(df_new)){setnames(df_new,"mean","data")}
if("var" %in% names(df_new)){setnames(df_new,"var","me_name")}
if(!("me_name" %in% names(df_new))){
  df_new$me_name <- me_short
}
df_new <- subset(df_new,me_name==me_short)
if(!("year_id" %in% names(df_new))){
  df_new$year_id <- sapply(1:nrow(df_new),function(x){
    m <- floor(mean(c(df_new$year_start[x],df_new$year_end[x])))
  })
}
df_new$missing_design_vars <- NA
if("age_start" %in% names(df_new)){setnames(df_new,"age_start","age_year")}
df_new$age_end <- NULL
df_new$is_outlier <- 0
df_new$bundle <- "maternal"
df_new$dhs_data <- NA
df_new$cv_admin <- 0
df_new$cv_survey <- 1
df_new$file_nid <- NA
df_new$file_location <- NA
df_new$sex_id <- 3
df_new$age_group_id <- 22
df_new$var <- NULL
df_new$measure <- "proportion"
if("variance" %not in% names(df_new)){
  df_new$variance <- df_new$standard_error^2
}
df_new <- merge(df_new,locs,by="ihme_loc_id")
message("Collapsed data prepped!")
                                                                #this is where the extra columns are getting added
df_old <- data.table(read.csv(paste0(output_dir,"/",me,".csv")))
write.csv(df_old, paste0(output_dir,"maternal_archive/",me,".csv"),row.names=F)
message("Removing any duplicate NIDs from old data")
nids <- df_new$nid
df_old <- df_old[! df_old$nid %in% nids,]
df <- rbind(df_old,df_new,fill=TRUE)

message("New data successfully added!")
message(paste0("Final dataset has ",nrow(df)," rows."))
message("Exporting final dataset for model upload...")
write.csv(df, paste0(output_dir,"/",me,".csv"), row.names=F)

message("Success! New data ready for upload.")

### END ###
