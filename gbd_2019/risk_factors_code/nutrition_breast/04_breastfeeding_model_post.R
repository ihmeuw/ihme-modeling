########################################################################################################################
## Project: RF: Suboptimal Breastfeeding
## Purpose: Process modeled breastfeeding data post-ST-GPR, adjust to match GBD age groups, and prep for upload
########################################################################################################################

message("Setting up environment...")
rm(list=ls())

os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "FILEPATH"
} else {
  j <- "FILEPATH"
  user <- Sys.info()[["user"]]
  h <- paste0("FILEPATH", user)
  k <- "FILEPATH"
}

# load libraries & functions 
library(data.table)
library(dplyr)
source(paste0(k,"FILEPATH/get_location_metadata.R"))
source(paste0(k,"FILEPATH/get_covariate_estimates.R"))
source("FILEPATH/save_results_stgpr.R")
source("FILEPATH/save_results_epi.R")

# set directories and vectors
gbd_round <- "2020"
decomp_step <- "step1"
gpr_dir <- "FILEPATH"
data_dir <- paste0("FILEPATH"); if (!dir.exists(data_dir)) dir.create(data_dir, recursive=TRUE)
ids <- c("location_id","year_id","age_group_id","sex_id")
log_path <- paste0("FILEPATH/bf_run_log.csv")


# location hierarchy
locs <- get_location_metadata(gbd_round_id=7, location_set_id=22)[level>=3,.(ihme_loc_id,location_id)]

#########################################################################################################################
############################################## GET RUN IDS ##############################################################
#########################################################################################################################

# Pull run-ids from the current 'best' models
log_file <- fread(log_path)[is_best==1]
abfrate0to5 <- log_file[me_name=="abfrate0to5"]$run_id
abfrate6to11 <- log_file[me_name=="abfrate6to11"]$run_id
abfrate12to23 <- log_file[me_name=="abfrate12to23"]$run_id
ebfrate0to5 <- log_file[me_name=="ebfrate0to5"]$run_id
predbfrate0to5 <- log_file[me_name=="predbfrate0to5"]$run_id
partbfrate0to5 <- log_file[me_name=="partbfrate0to5"]$run_id

#########################################################################################################################
####################################### LAUNCH SAVE_RESULTS_STGPR #######################################################
#########################################################################################################################

save_stgpr(abfrate0to5,ages="3,388")
save_stgpr(abfrate6to11,ages="389")
save_stgpr(abfrate12to23,ages="238")
save_stgpr(ebfrate0to5,ages="3,388")
save_stgpr(predbfrate0to5,ages="3,388")
save_stgpr(partbfrate0to5,ages="3,388")

#########################################################################################################################
############################################## APPEND DRAWS #############################################################
#########################################################################################################################

# abfrate0to5
setwd(paste0("FILEPATH"))
files <- list.files(paste0(gpr_dir,abfrate0to5,"/draws_temp_0"))
abf0_5 <- rbindlist(lapply(files,fread),use.names=TRUE)
write.csv(abf0_5,paste0(data_dir,"abfrate0to5.csv"),row.names=F)
# abfrate6to11
setwd(paste0("FILEPATH"))
files <- list.files(paste0(gpr_dir,abfrate6to11,"/draws_temp_0"))
abf6_11 <- rbindlist(lapply(files,fread),use.names=TRUE)
write.csv(abf6_11,paste0(data_dir,"abfrate6to11.csv"),row.names=F)
# abfrate12to23
setwd(paste0("FILEPATH"))
files <- list.files(paste0(gpr_dir,abfrate12to23,"/draws_temp_0"))
abf12_23 <- rbindlist(lapply(files,fread),use.names=TRUE)
write.csv(abf12_23,paste0(data_dir,"abfrate12to23.csv"),row.names=F)
# ebfrate0to5
setwd(paste0("FILEPATH"))
files <- list.files(paste0(gpr_dir,ebfrate0to5,"/draws_temp_0"))
ebf0_5 <- rbindlist(lapply(files,fread),use.names=TRUE)
write.csv(ebf0_5,paste0(data_dir,"ebfrate0to5.csv"),row.names=F)
# predbfrate0to5
setwd(paste0("FILEPATH"))
files <- list.files(paste0(gpr_dir,predbfrate0to5,"/draws_temp_0"))
predbf0_5 <- rbindlist(lapply(files,fread),use.names=TRUE)
write.csv(predbf0_5,paste0(data_dir,"predbfrate0to5.csv"),row.names=F)
# partbfrate0to5
setwd(paste0("FILEPATH"))
files <- list.files(paste0(gpr_dir,partbfrate0to5,"/draws_temp_0"))
partbf0_5 <- rbindlist(lapply(files,fread),use.names=TRUE)
write.csv(partbf0_5,paste0(data_dir,"partbfrate0to5.csv"),row.names=F)

########################################################################################################################
################################ FORMAT AND MERGE DRAWS FOR AGE SPLITTING ##############################################
########################################################################################################################

## Non-Exclusive Breastfeeding ##

# Set draw names needed in this process
draws <- paste0("draw_",0:999)
ebf_draws <- paste0("exp_cat4_",0:999)
predbf_draws <- paste0("exp_cat3_",0:999)
partbf_draws <- paste0("exp_cat2_",0:999)
nobf0_5_draws <- paste0("exp_cat1_",0:999)
abf0_5_draws <- paste0("abf_",0:999)

# Exclusive Breastfeeding
ebf0_5 <- ebf0_5[!duplicated(ebf0_5, by=ids),]
setnames(ebf0_5, draws, ebf_draws)

# Predominant Breastfeeding
predbf0_5 <- predbf0_5[!duplicated(predbf0_5, by=ids),]
setnames(predbf0_5, draws, predbf_draws)

# Partial Breastfeeding
partbf0_5 <- partbf0_5[!duplicated(partbf0_5, by=ids),]
setnames(partbf0_5, draws, partbf_draws)

# Any Breastfeeding 0-5 Months
abf0_5 <- abf0_5[!duplicated(abf0_5, by=ids),]
setnames(abf0_5, draws, abf0_5_draws)

# Merge Data Together
l <- list(partbf0_5,predbf0_5,ebf0_5)
non_exc <- abf0_5
for (i in l) {
  non_exc <- merge(non_exc,i,by=ids,all=T)
}

# Rescale Draws to Proportion of Any Breastfeeding 0-5 Months
non_exc[,(nobf0_5_draws) := lapply(.SD,function(x) 1-x),.SDcols=abf0_5_draws]
non_exc <- melt.data.table(non_exc, id.vars = ids, measure =patterns('abf', paste0('exp_cat', seq(1,4))), 
                           value.name = c('abf', paste0('exp_cat', seq(1,4))), variable.factor = FALSE)
non_exc$variable <- as.numeric(non_exc$variable)-1
non_exc$scale <- non_exc$abf/(non_exc$exp_cat2+non_exc$exp_cat3+non_exc$exp_cat4)
non_exc$exp_cat2 <- non_exc$exp_cat2 * non_exc$scale
non_exc$exp_cat3 <- non_exc$exp_cat3 * non_exc$scale
non_exc$exp_cat4 <- non_exc$exp_cat4 * non_exc$scale
non_exc[,c("abf","scale")] <- NULL

# Dcast wide for saving
non_exc_wide <- dcast.data.table(non_exc, as.formula(paste0(paste(ids, collapse=" + "), " ~ variable")), 
                                 value.var=paste0("exp_cat",1:4))
write.csv(non_exc_wide, paste0(data_dir,"/all_cats_0to5.csv"),row.names=F)

## Discontinued Breastfeeding ##

# Set draw names needed in this process
abf6_11_draws <- paste0("exp_cat2_",0:999)
nobf6_11_draws <- paste0("exp_cat1_",0:999)
abf12_23_draws <- paste0("exp_cat2_",0:999)
nobf12_23_draws <- paste0("exp_cat1_",0:999)

# Any Breastfeeding 6-11 Months
abf6_11 <- abf6_11[!duplicated(abf6_11, by=ids),]
setnames(abf6_11, draws, abf6_11_draws)

# Discontinued Breastfeeding 6-11 Months
disc6_11 <- copy(abf6_11)
disc6_11[,(nobf6_11_draws) := lapply(.SD,function(x) 1-x),.SDcols=abf6_11_draws]
write.csv(disc6_11,paste0(data_dir,"/all_cats_6to11.csv"),row.names=F)

# Any Breastfeeding 12-23 Months
abf12_23 <- abf12_23[!duplicated(abf12_23, by=ids),]
setnames(abf12_23, draws, abf12_23_draws)

# Discontinued Breastfeeding 12-23 Months
disc12_23 <- copy(abf12_23)
disc12_23[,(nobf12_23_draws) := lapply(.SD,function(x) 1-x),.SDcols=abf12_23_draws]
write.csv(disc12_23,paste0(data_dir,"/all_cats_12to23.csv"),row.names=F)


########################################################################################################################
################################### BREASTFEEDING AGE GROUP POPULATIONS ################################################
########################################################################################################################

duplicate.draws <- function(df, ages=c(3,388), sex_ids=c(1, 2)) {
  demo <- expand.grid(age_group_id=ages, sex_id=sex_ids) %>% data.table
  df.i <- df %>% copy
  ## Duplicate
  df <- lapply(1:nrow(demo), function(x) {
    age <- demo[x]$age_group_id
    sex <- demo[x]$sex_id
    cf <- df.i[, `:=` (age_group_id=age, sex_id=sex)] %>% copy
    return(cf)
  }) %>% rbindlist
  return(df)
}

duplicate.sex <- function(df, sex_ids=c(1, 2)) {
  demo <- expand.grid(sex_id=sex_ids) %>% data.table
  df.i <- df %>% copy
  ## Duplicate
  df <- lapply(1:nrow(demo), function(x) {
    sex <- demo[x]$sex_id
    cf <- df.i[, `:=` (sex_id=sex)] %>% copy
    return(cf)
  }) %>% rbindlist
  return(df)
}

###########################################################
## Non-Exclusive Breastfeeding
###########################################################

# duplicate draws for the two GBD age groups and both sex ids
non_exc <- duplicate.draws(non_exc)
# reformat, cast wide and save
ids2 <- c("location_id","year_id","sex_id","age_group_id")
non_exc_wide <- dcast.data.table(non_exc, as.formula(paste0(paste(ids2, collapse=" + "), " ~ variable")), 
                                 value.var=paste0("exp_cat",1:4))
write.csv(non_exc_wide, paste0(data_dir,"/bf_exclusive_exposure.csv"),row.names=F)

###########################################################
## Discontinued Breastfeeding
###########################################################

# generate age ids and duplicate sexes
disc6_11[,age_group_id := 389]
disc12_23[,age_group_id := 238]
disc_bf <- rbind(disc6_11,disc12_23)
disc_bf <- duplicate.sex(disc_bf)
# Write out and save
write.csv(disc_bf, paste0(data_dir,"/bf_discontinued_exposure.csv"),row.names=F)


########################################################################################################################
################################### BREASTFEEDING PREP FOR UPLOAD TO DB ################################################
########################################################################################################################


###########################################################
## Non-Exclusive Breastfeeding
###########################################################

# Export data for each category
cats <- c("exp_cat1_","exp_cat2_","exp_cat3_","exp_cat4_")
for (cat in cats){ 
  names <- paste0(cat,0:999)
  df <- non_exc_wide[,c(ids,names),with=F]
  setnames(df,names,draws)
  write.csv(df,paste0(data_dir,"/",cat,"draws.csv"),row.names=F)
}

# Save Results!
save_epi(input_dir=data_dir,cat="exp_cat1",me_id=9598,step=decomp_step,bundle_id=log_file[me_name=="abfrate0to5",bundle_id],crosswalk_id=log_file[me_name=="abfrate0to5",crosswalk_version_id]) #No Breastfeeding
save_epi(input_dir=data_dir,cat="exp_cat2",me_id=8904,step=decomp_step,bundle_id=log_file[me_name=="partbfrate0to5",bundle_id],crosswalk_id=log_file[me_name=="partbfrate0to5",crosswalk_version_id]) #Partial Breastfeeding
save_epi(input_dir=data_dir,cat="exp_cat3",me_id=8905,step=decomp_step,bundle_id=log_file[me_name=="predbfrate0to5",bundle_id],crosswalk_id=log_file[me_name=="predbfrate0to5",crosswalk_version_id]) #Predominant Breastfeeding
save_epi(input_dir=data_dir,cat="exp_cat4",me_id=20417,step=decomp_step,bundle_id=log_file[me_name=="ebfrate0to5",bundle_id],crosswalk_id=log_file[me_name=="ebfrate0to5",crosswalk_version_id]) #Exclusive Breastfeeding

###########################################################
## Discontinued Breastfeeding
###########################################################

# Export data for each category
cats <- c("exp_cat1_","exp_cat2_")
for (cat in cats) {
  names <- paste0(cat,0:999)
  df <- disc_bf[,c(ids,names),with=F]
  setnames(df,names,draws)
  write.csv(df,paste0(data_dir,"/disc_",cat,"draws.csv"),row.names=F)
}

# Save Results!
save_epi(input_dir=data_dir,cat="disc_exp_cat1",me_id=8907,step=decomp_step,bundle_id=log_file[me_name=="abfrate6to11",bundle_id],crosswalk_id=log_file[me_name=="abfrate6to11",crosswalk_version_id]) #Discontinued Breastfeeding
