### NTDs Dengue
#Data cleaning
####DOWNLOAD AND PREP DENGUE DATA FOR CROSSWALK #####

#rm(list=ls())

library(dplyr)

source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_bundle_data.R")
##############
# Dengue all age data crosswalking
################

dengue_bundle<-read.csv("FILEPATH")

#targetting those rows where specificity is missing - these are all age data 
#drop group_review==0, drop if is_outlier==1

data<-subset(dengue_bundle, group_review!=0)
data<-subset(data, is_outlier==0)

data$sex_id<- 0

data$sex_id[data$sex=="Both"] <- 3
data$sex_id[data$sex=="Female"] <- 2
data$sex_id[data$sex=="Male"] <- 1

data$unique_id <- 1:nrow(data)


data_1<- subset(data, specificity=="")

#drop 2
#making sure that there is only one source per demographic cohort (nid not taken into account)
data_2<- data_1 %>% distinct(location_id, sex_id, year_start, year_end, age_start, age_end, cases, sample_size, .keep_all = TRUE)

dropped_2 <- data_1[!(data_1$unique_id %in% data_2$unique_id),]
#1 observations dropped - most of these were indian locations, where same case numbers and sample sizes with different sources


#####moving on to those rows where specificity is not equal to missing so we can use unique group to do this entire process!

data_with_UI<- subset(data, specificity!="")

table(data_with_UI$specificity)
table(data_with_UI$group)

# drop 1

data_spec<-subset(data_with_UI,specificity=="all")

#observing overall duplicates based on nid, location_id, sex, year_start, year_end, age_start, age_end, cases, sample_size, measure 
data_with_UI_1<-data_spec %>% distinct(nid, location_id, sex_id, year_start, year_end, age_start, age_end, cases, sample_size, measure, .keep_all = TRUE)

#no drops
dropped_wUI_1 <- data_spec[!(data_spec$unique_id %in% data_with_UI_1$unique_id),]


#drop 2
#making sure that there is only one source per demographic cohort (nid not taken into account)
data_with_UI_2<- data_with_UI_1 %>% distinct(location_id, sex_id, year_start, year_end, age_start, age_end, cases, sample_size, .keep_all = TRUE)
#none dropped
dropped_wUI_2 <- data_with_UI_1[!(data_with_UI_1$unique_id %in% data_with_UI_2$unique_id),]


# drop 3
#checking if there is a difference by sample size
data_with_UI_3<- data_with_UI_2 %>% distinct(location_id, sex_id, year_start, year_end, age_start, age_end, cases, .keep_all = TRUE)
#765
dropped_wUI_3 <- data_with_UI_2[!(data_with_UI_2$unique_id %in% data_with_UI_3$unique_id),]



#append two datasets

dedup_dengue_data<-rbind(data_2,data_with_UI_3)

#clean
dedup_dengue_data<- subset(dedup_dengue_data, !is.na(cases))
dedup_dengue_data<- subset(dedup_dengue_data, !is.na(sample_size))
dedup_dengue_data<- subset(dedup_dengue_data, sample_size!=0)
dedup_dengue_data<- subset(dedup_dengue_data, sample_size!=cases)
dedup_dengue_data<- subset(dedup_dengue_data, sample_size>cases)


#recalculating mean and SE
dedup_dengue_data$mean<- dedup_dengue_data$cases/dedup_dengue_data$sample_size
dedup_dengue_data$standard_error<- sqrt(((dedup_dengue_data$mean*(1-dedup_dengue_data$mean))/dedup_dengue_data$sample_size))

#cleaning
dedup_dengue_data$mean[dedup_dengue_data$cases==0] <- 0


location_set<- get_location_metadata(location_set_id = 35, gbd_round_id = ADDRESS, decomp_step="ADDRESS")
dedup_dengue_data = subset(dedup_dengue_data, select = -c(ihme_loc_id))
dedup_dengue_data <- merge(dedup_dengue_data, location_set[, c('location_id', 'ihme_loc_id' )], by=c('location_id'), all.x = TRUE)


#all-age dengue cases data
write.csv(dedup_dengue_data,"FILEPATH")



#################################################################################################

##########
## crosswalkking dengue age bundle 
############

dengue_age<- get_bundle_data(bundle_id = ADDRESS, decomp_step = ADDRESS)
dengue_age_bv<- save_bundle_version(bundle_id=ADDRESS, decomp_step=ADDRESS, include_clinical = FALSE) 


dengue_age_bundle<- get_bundle_version(bundle_version_id = ADDRESS, export=TRUE)

dengue_age_bundle <- dengue_age_bundle[, age_width := age_end - age_start]
dengue_age_bundle <- dengue_age_bundle[age_width <= 25, ]
dengue_age_bundle[, crosswalk_parent_seq := seq]



dengue_age_bundle_v1<- subset(dengue_age_bundle, sex=="Male" | sex=="Female")

#removing group review =0 and keeping group revoew =1 or =missings
dengue_age_bundle_v2<- subset(dengue_age_bundle_v1, group_review !=0 | is.na(group_review))

description <- "dengue_age_specific_data"
openxlsx::write.xlsx(dengue_age_bundle_v2, "FILEPATH/age_specific_crosswalk.xlsx",  sheetName = "extraction")


cw_dengue_age_specific_bundle <- save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = "FILEPATH/age_specific_crosswalk.xlsx", description = description)

####################################################################################
###saving crosswalk for MAIN BUNDLE! dengue age split data that goes into dismod####
######################################################################################

dengue_total_bv<- save_bundle_version(bundle_id=ADDRESS, decomp_step=ADDRESS, include_clinical = FALSE) # returns a bundle_version_id


description <- "sex_and_age_split_dengue_data"
dengue_cw_v <- save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = "FILEPATH/age_split_dengue.xlsx", description = description)
#CW version:9014

#Attempt2, uploading cases>=0.5
description <- "cases>=0.5, sex_and_age_split_dengue_data"
dengue_cw_v <- save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = "FILEPATH/age_split_dengue.xlsx", description = description)

###END####