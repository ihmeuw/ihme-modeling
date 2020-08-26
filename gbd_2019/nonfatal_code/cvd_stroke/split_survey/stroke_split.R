##############################
# Stroke code for spliting unspecified/hemorrhagic stroke into sub-types
# Author: USERNAME
# Date  : 9/9/2019
##############################


####### Load functions #######
source('FILEPATH/get_bundle_data.R')
source('FILEPATH/upload_bundle_data.R')
source('FILEPATH/get_bundle_version.R')
source('FILEPATH/save_bundle_version.R')
source('FILEPATH/get_location_metadata.R')
source('FILEPATH/get_age_metadata.R')
if (Sys.info()[1] == "Linux") jpath <- "FILEPATH"
if (Sys.info()[1] == "Windows") jpath <- "FILEPATH"
rm(list = setdiff(ls(), c(lsf.str(), "jpath", "hpath", "os")))
date<-gsub("-", "_", Sys.Date())
logs <- paste0(jpath, "FILEPATH")


library(openxlsx)
library(ggplot2)
'%ni%' <- Negate('%in%')

#Get region information for graphing later
locations <- data.frame(get_location_metadata(location_set_id=35))
ages <- get_age_metadata(age_group_set_id=19)

#Drop Ecuador, Georgia, Mexico, Kenya, Turkey, India, Philippines, Indonesia, Chile, low SDI areas of China, Brazil
todrop_names <- locations[locations$location_name %in% c("Ecuador", "Tibet", "Turkey","Kyrgyzstan", "Philippines", "Chile", "Nepal", "Kenya",'Botswana',"Karnataka, Urban"), c("location_name", "location_id")]
todrop_utla <- locations[which(locations$parent_id==4749), c("location_name", "location_id")]
todrop_ids <- locations[locations$parent_id %in% c(135, 130, 163, 11, unlist(todrop_utla$location_id, use.names=F)),c("location_id", "location_name"),] #drops Brazil, India, Indonesia, UTLAs, Mexico
todrop <- rbind(todrop_names, todrop_ids)

cf.map <- read.csv("FILEPATH/cvd_cf2.csv") #clinical data adjustment factors.
#Confidence and quantile for uncertainty
confidence <- 0.95
quantile <- 1 - (1-confidence)/2

####### STEP 1 #######
# Grab clinical data from bundle 116 (I-62) and 371 (I-64)

# Get I62
i62 <- get_bundle_version(bundle_version_id = 13466, export=FALSE) #all the new clinical data.
i62 <- subset(i62, clinical_data_type=="inpatient")
i62 <- merge(i62, locations[,c("location_id", "parent_id")], by="location_id", all.x=T)
i62 <- subset(i62, location_id %ni% unlist(todrop$location_id, use.names=F))

i62$parent_id <- NULL
setnames(i62, "mean", "mean.i62")
setnames(i62, "standard_error", "se.i62")

#Get i64
i64 <- get_bundle_version(bundle_version_id=13469, export=FALSE) #bundle_version_id associated with bundle_id=371
i64 <- subset(i64, clinical_data_type=="inpatient")
i64 <- merge(i64, locations[,c("location_id", "parent_id")], by="location_id", all.x=T)
i64 <- subset(i64, location_id %ni% unlist(todrop$location_id, use.names=F))
i64$parent_id <- NULL
setnames(i64, "mean", "mean.i64")
setnames(i64, "standard_error", "se.i64")


#SUBTYPE SPECIFI: #Read in first ever acute subtype-specific stroke
#Ischemic stroke
fe.is <- get_bundle_version(bundle_version_id=14081, export=FALSE)     #bundle_version_id associated with bundle_id=454
fe.is <- subset(fe.is, clinical_data_type=="inpatient") #subset to inpatient data only
fe.is <- merge(fe.is, locations[,c("location_id", "parent_id")], by="location_id", all.x=T)
fe.is <- subset(fe.is, location_id %ni% unlist(todrop$location_id, use.names=F)) #drop locations not being used
fe.is$parent_id <- NULL
setnames(fe.is, "mean", "mean.is")
setnames(fe.is, "standard_error", "se.is")

#Intracerebral hemorrhage
fe.cerhem <- get_bundle_version(bundle_version_id=14084) #bundle_version_id associated with bundle_id=455
fe.cerhem <- subset(fe.cerhem, clinical_data_type=="inpatient")
fe.cerhem <- merge(fe.cerhem, locations[,c("location_id", "parent_id")], by="location_id", all.x=T)
fe.cerhem <- subset(fe.cerhem, location_id %ni% unlist(todrop$location_id, use.names=F)) #drop locations not being used
fe.cerhem$parent_id <- NULL
setnames(fe.cerhem, "mean", "mean.cerhem")
setnames(fe.cerhem, "standard_error", "se.cerhem")

##Subarachnoid hemorrhage
fe.subhem <- get_bundle_version(bundle_version_id=14087) #bundle_version_id associated with bundle_id=2996
fe.subhem <- subset(fe.subhem, clinical_data_type=="inpatient")
fe.subhem <- merge(fe.subhem, locations[,c("location_id", "parent_id")], by="location_id", all.x=T)
fe.subhem <- subset(fe.subhem, location_id %ni% unlist(todrop$location_id, use.names=F))
fe.subhem$parent_id <- NULL
setnames(fe.subhem, "mean", "mean.subhem")
setnames(fe.subhem, "standard_error", "se.subhem")

#Merge all together
merged <- merge(fe.is[,c("location_id", "year_start", "year_end", "age_start", "age_end", "sex", "mean.is", "se.is")],
                fe.cerhem[,c("location_id", "year_start", "year_end", "age_start", "age_end", "sex", "mean.cerhem", "se.cerhem")],
                by=c("location_id", "year_start", "year_end", "age_start", "age_end", "sex"), all=T)
merged <- merge(merged, fe.subhem[,c("location_id", "year_start", "year_end", "age_start", "age_end", "sex", "mean.subhem", "se.subhem")],
                by=c("location_id", "year_start", "year_end", "age_start", "age_end", "sex"), all=T)
merged <- merge(merged, i64[,c("location_id", "year_start", "year_end", "age_start", "age_end", "sex", "mean.i64", "se.i64")],
                by=c("location_id", "year_start", "year_end", "age_start", "age_end", "sex"), all=T)
merged <- merge(merged, i62[,c("location_id", "year_start", "year_end", "age_start", "age_end", "sex", "mean.i62", "se.i62")],
                by=c("location_id", "year_start", "year_end", "age_start", "age_end", "sex"), all=T)

merged <- subset(merged, age_start>=40)                                      #only take ages greater than 40
merged$mean.i64 <- with(merged, ifelse(is.na(mean.i64)==T, 0, mean.i64))     #replace NA's
merged$mean.i62 <- with(merged, ifelse(is.na(mean.i62)==T, 0, mean.i62))
merged$mean.subhem <- with(merged, ifelse(is.na(mean.subhem)==T, 0, mean.subhem))
merged$se.i62 <- with(merged, ifelse(is.na(se.i62)==T, 0, se.i62))
merged$se.i64 <- with(merged, ifelse(is.na(se.i64)==T, 0, se.i64))

#i64 produce proportion of ischemic, cerhem, and subhem strokes of all recorded strokes.
merged$prop.is <- with(merged, ifelse(is.na(mean.subhem)==F, mean.is/(mean.is+mean.cerhem+mean.subhem), mean.is/(mean.is+mean.cerhem)))
merged$prop.cerhem <- with(merged, ifelse(is.na(mean.subhem)==F, mean.cerhem/(mean.is+mean.cerhem+mean.subhem), mean.cerhem/(mean.is+mean.cerhem)))
merged$prop.subhem <- with(merged, ifelse(is.na(mean.subhem)==F, mean.subhem/(mean.is+mean.cerhem+mean.subhem), 0))

merged$prop.is <- with(merged, ifelse(mean.is==0 & mean.cerhem==0 & mean.subhem==0, 0, prop.is))
merged$prop.cerhem <- with(merged, ifelse(mean.is==0 & mean.cerhem==0 & mean.subhem==0, 0, prop.cerhem))
merged$prop.subhem <- with(merged, ifelse(mean.is==0 & mean.cerhem==0 & mean.subhem==0, 0, prop.subhem))

#i62
merged$prop.cerhem2 <- with(merged, ifelse(is.na(mean.subhem)==F, mean.cerhem/(mean.cerhem+mean.subhem), 1))
merged$prop.subhem2 <- with(merged, ifelse(is.na(mean.subhem)==F, mean.subhem/(mean.cerhem+mean.subhem), 0))
merged$prop.cerhem2 <- with(merged, ifelse(mean.cerhem==0 & mean.subhem==0, 0, prop.cerhem2))
merged$prop.subhem2 <- with(merged, ifelse(mean.cerhem==0 & mean.subhem==0, 0, prop.subhem2))

#i64
merged$mean.i64.is <- with(merged, mean.i64*prop.is)
merged$mean.i64.cerhem <- with(merged, mean.i64 * prop.cerhem)
merged$mean.i64.subhem <- with(merged, mean.i64 * prop.subhem)

#i62
merged$mean.i62.cerhem <- with(merged, mean.i62 * prop.cerhem2)
merged$mean.i62.subhem <- with(merged, mean.i62 * prop.subhem2)

merged$mean.is.new <- with(merged, mean.is + mean.i64.is)
#merged$se.is.new <- with(merged, ((mean.is**2)*(se.i64**2) + (mean.i64**2)*(se.is**2) +(se.i64**2)*(se.is**2))**(1/2))
merged$se.is.new <- with(merged, se.is + se.i64)
merged$mean.is.new <- with(merged, ifelse(is.na(mean.i64)==T, mean.is, mean.is.new))
merged$se.is.new <- with(merged, ifelse(is.na(se.i64)==T, se.is, se.is.new))

merged$mean.cerhem.new <- with(merged, mean.cerhem + mean.i64.cerhem + mean.i62.cerhem)
#merged$se.cerhem.new <- with(merged, ((mean.cerhem**2)*(se.i64**2) + (mean.i64**2)*(se.cerhem**2) +(se.i64**2)*(se.cerhem**2))**(1/2))
merged$se.cerhem.new <- with(merged, se.cerhem + se.i64 + se.i62)
merged$mean.cerhem.new <- with(merged, ifelse(is.na(mean.i64)==T & is.na(mean.i62)==T, mean.cerhem, mean.cerhem.new))
merged$se.cerhem.new <- with(merged, ifelse(is.na(se.i64)==T & is.na(se.i62)==T, se.cerhem, se.cerhem.new))

merged$mean.subhem.new <- with(merged, mean.subhem + mean.i64.subhem + mean.i62.subhem)
#merged$se.cerhem.new <- with(merged, ((mean.cerhem**2)*(se.i64**2) + (mean.i64**2)*(se.cerhem**2) +(se.i64**2)*(se.cerhem**2))**(1/2))
merged$se.subhem.new <- with(merged, se.subhem + se.i64 + se.i62)
merged$mean.subhem.new <- with(merged, ifelse(is.na(mean.i64)==T & is.na(mean.i62)==T, mean.subhem, mean.subhem.new))
merged$se.subhem.new <- with(merged, ifelse(is.na(se.i64)==T & is.na(se.i62)==T, se.subhem, se.subhem.new))


#Make files for upload
fe.is.upload <- merge(fe.is, merged[,c("location_id", "year_start", "year_end", "age_start", "age_end", "sex", "mean.is.new", "se.is.new")],  #here is where you lose some data, you are merging onto fe.is
                      by=c("location_id", "year_start", "year_end", "age_start", "age_end", "sex"), all.x=T)                                  #fe.is does not have the new clinical data.
fe.is.upload <- subset(fe.is.upload, age_start>=40)
fe.is.upload$sex_id <- with(fe.is.upload, ifelse(sex=="Male", 1, 2))
fe.is.upload <- merge(fe.is.upload, ages[,c("age_group_id", "age_group_years_start")], by.x="age_start", by.y="age_group_years_start")
fe.is.upload <- merge(fe.is.upload, unique(cf.map[cf.map$bundle_id==454 & cf.map$age_group_id>=13, c("sex_id", "age_group_id", "Y_mean")]),
                      by=c("age_group_id", "sex_id"), all.x=T, allow.cartesian=T)
fe.is.upload$bundle_id <- 7112
fe.is.upload$bundle_name <- "First ever acute ischemic stroke with CSMR - EMR Comparison"
fe.is.upload$mean <- with(fe.is.upload, mean.is.new*Y_mean)
fe.is.upload$standard_error <- fe.is.upload$se.is.new
fe.is.upload$uncertainty_type_value <- NA
fe.is.upload$note_modeler <- "first ever acute ischemic stroke + split of I64"
fe.is.upload$cv_inpatient <- 1
fe.is.upload$cv_acute_first_ever_stroke <- 0
fe.is.upload$cv_all_stroke <- 0
fe.is.upload$cv_first_ever_stroke <- 0
fe.is.upload$cv_outpatient <- 0
fe.is.upload <- subset(fe.is.upload, is.na(mean)==F)
fe.is.upload$lower <- NA
fe.is.upload$upper <- NA
fe.is.upload$extractor <- "USERNAME"

fe.is.upload <- fe.is.upload[,c("location_id", "year_start", "year_end", "age_start", "age_end", "sex", "bundle_id", "bundle_name", "measure", "location_name", "nid",
                                "representative_name", "mean", "lower", "upper", "sample_size", "source_type", "urbanicity_type", "recall_type", "unit_type", "unit_value_as_published",
                                "cases", "is_outlier", "seq", "underlying_nid", "sampling_type", "recall_type_value", "uncertainty_type", "uncertainty_type_value", "input_type",
                                "standard_error", "effective_sample_size", "design_effect", "extractor", "note_modeler", "cv_inpatient",'cv_acute_first_ever_stroke',
                                "cv_all_stroke","cv_first_ever_stroke","cv_outpatient")]
fe.is.upload$response_rate <- NA
filename.is <- paste0(jpath, "FILEPATH/7112_hospital_split_", date, ".xlsx") #data storage
write.xlsx(fe.is.upload, filename.is, sheetName="extraction", na="")


#ICH
fe.cerhem.upload <- merge(fe.cerhem, merged[,c("location_id", "year_start", "year_end", "age_start", "age_end", "sex", "mean.cerhem.new", "se.cerhem.new")],
                          by=c("location_id", "year_start", "year_end", "age_start", "age_end", "sex"), all.x=T)
fe.cerhem.upload <- subset(fe.cerhem.upload, age_start>=40)
fe.cerhem.upload$sex_id <- with(fe.cerhem.upload, ifelse(sex=="Male", 1, 2))
fe.cerhem.upload <- merge(fe.cerhem.upload, ages[,c("age_group_id", "age_group_years_start")], by.x="age_start", by.y="age_group_years_start")
fe.cerhem.upload <- merge(fe.cerhem.upload, unique(cf.map[cf.map$bundle_id==455 & cf.map$age_group_id>=13, c("sex_id", "age_group_id", "Y_mean")]),
                          by=c("age_group_id", "sex_id"), all.x=T, allow.cartesian=T)
fe.cerhem.upload$bundle_id <- 7088
fe.cerhem.upload$bundle_name <- "First ever acute hemorrhagic stroke with CSMR - EMR Comparison"
fe.cerhem.upload$mean <- with(fe.cerhem.upload, mean.cerhem.new*Y_mean)
fe.cerhem.upload$standard_error <- fe.cerhem.upload$se.cerhem.new
fe.cerhem.upload$uncertainty_type_value <- NA
fe.cerhem.upload$note_modeler <- "first ever acute intracerebral hemorrhage stroke + split of I64"
fe.cerhem.upload$cv_inpatient <- 1
fe.cerhem.upload$cv_acute_first_ever_stroke <- 0
fe.cerhem.upload$cv_all_stroke <- 0
fe.cerhem.upload$cv_first_ever_stroke <- 0
fe.cerhem.upload$cv_outpatient <- 0
fe.cerhem.upload <- subset(fe.cerhem.upload, is.na(mean)==F)
fe.cerhem.upload$lower <- NA
fe.cerhem.upload$upper <- NA
fe.cerhem.upload$response_rate <- NA
fe.cerhem.upload$extractor <-'USERNAME'
fe.cerhem.upload <- fe.cerhem.upload[,c("location_id", "year_start", "year_end", "age_start", "age_end", "sex", "bundle_id", "bundle_name", "measure", "location_name", "nid",
                                        "representative_name", "mean", "lower", "upper", "sample_size", "source_type", "urbanicity_type", "recall_type", "unit_type", "unit_value_as_published",
                                        "cases", "is_outlier", "seq", "underlying_nid", "sampling_type", "recall_type_value", "uncertainty_type", "uncertainty_type_value", "input_type",
                                        "standard_error", "effective_sample_size", "design_effect", "response_rate", "extractor", "note_modeler", "cv_inpatient",
                                        "cv_acute_first_ever_stroke","cv_all_stroke","cv_first_ever_stroke", "cv_outpatient")]

filename.ich <- paste0(jpath, "FILEPATH/7088_plus_", date, ".xlsx")
write.xlsx(fe.cerhem.upload, filename.ich, sheetName="extraction", na="")


#SAH
fe.subhem.upload <- merge(fe.subhem, merged[,c("location_id", "year_start", "year_end", "age_start", "age_end", "sex", "mean.subhem.new", "se.subhem.new")],
                          by=c("location_id", "year_start", "year_end", "age_start", "age_end", "sex"), all.x=T)
fe.subhem.upload <- subset(fe.subhem.upload, age_start>=40)
fe.subhem.upload$sex_id <- with(fe.subhem.upload, ifelse(sex=="Male", 1, 2))
fe.subhem.upload <- merge(fe.subhem.upload, ages[,c("age_group_id", "age_group_years_start")], by.x="age_start", by.y="age_group_years_start")
fe.subhem.upload <- merge(fe.subhem.upload, unique(cf.map[cf.map$bundle_id==2996 & cf.map$age_group_id>=13, c("sex_id", "age_group_id", "Y_mean")]),
                          by=c("age_group_id", "sex_id"), all.x=T, allow.cartesian=T)
fe.subhem.upload$bundle_id <- 7100
fe.subhem.upload$bundle_name <- "First ever acute subarachnoid hemorrhage with CSMR"
fe.subhem.upload$mean <- with(fe.subhem.upload, mean.subhem.new*Y_mean)
fe.subhem.upload$standard_error <- fe.subhem.upload$se.subhem.new
fe.subhem.upload$uncertainty_type_value <- NA
fe.subhem.upload$note_modeler <- "first ever acute ischemic stroke + split of I64"
fe.subhem.upload$cv_inpatient <- 1
#fe.subhem.upload <- subset(fe.subhem.upload, age_start>=40)
fe.subhem.upload <- subset(fe.subhem.upload, is.na(mean)==F)
fe.subhem.upload$lower <- NA
fe.subhem.upload$upper <- NA
fe.subhem.upload$response_rate <- NA
fe.subhem.upload$extractor <- 'USERNAME'
fe.subhem.upload <- fe.subhem.upload[,c("location_id", "year_start", "year_end", "age_start", "age_end", "sex", "bundle_id", "bundle_name", "measure", "location_name", "nid",
                                        "representative_name", "mean", "lower", "upper", "sample_size", "source_type", "urbanicity_type", "recall_type", "unit_type", "unit_value_as_published",
                                        "cases", "is_outlier", "seq", "underlying_nid", "sampling_type", "recall_type_value", "uncertainty_type", "uncertainty_type_value", "input_type",
                                        "standard_error", "effective_sample_size", "design_effect", "response_rate", "extractor", "note_modeler", "cv_inpatient")]

filename <- paste0(jpath, "FILEPATH/7100_plus_", date, ".xlsx")
write.xlsx(fe.subhem.upload, filename, sheetName="extraction", na="")

#Save bundle_versions
ischemic <- save_bundle_version(bundle_id=7112, decomp_step="step4")
ich <- save_bundle_version(bundle_id=7088, decomp_step="step4")
sah <- save_bundle_version(bundle_id=7100, decomp_step="step4")



