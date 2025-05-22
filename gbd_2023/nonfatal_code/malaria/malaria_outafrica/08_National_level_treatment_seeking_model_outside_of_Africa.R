
# 1.admin0

library(gtools)
library(VGAM)
library(survey) 
library(plotrix)
library(MASS)
library(dplyr)
library(mgcv)
library(splitstackshape)
library(MuMIn)
library(nortest)
library(xtable)
library(plotrix)
library(itsadug)
library(stringr)
library(ggplot2)
library(grid)
library(gridExtra)
library(class)
library(TMB)
library(sparseMVN)
library(tidyverse)

# 1 Data_Preparation.R

rm(list = ls())
error_mssgs<-list()

GBD<-'ADDRESS'
 directory in which the covariates and the parameter file will be saved for version control purposes
# FOR NEW RUNS

Run <- 'ADDRESS'

output.path<-paste0('FILEPATH')
graphics.path.part <- paste0(FILEPATH)
graphics.path <- paste0(FILEPATH)
data.path <- paste0(FILEPATH)
table.path<-paste0(FILEPATH)
dir.create(output.path)
dir.create(graphics.path.part)
dir.create(graphics.path)
dir.create(data.path)
dir.create(table.path)

#================================SPECIFY INPUT PATHS AND MAKE A PARAMETER FILE======================
#1. Inputs for Admin Units
config.path<-paste0("FILEPATH/Combined_Config_Data.csv")
subnat.path<-paste0("FILEPATH/location_set_22.csv")
#2. Covariates
healthcare.accessibility.path<-paste0("FILEPATH/pop_weighted_healthcare_accessibility_1km.csv")
accessibility.path<-paste0("FILEPATH/pop_weighted_accessibility_1km.csv")
VIIRS.path<-paste0("FILEPATH/viirs_v2_nighttime_lights_pop_weighted.csv")

#--IHME
IHME.cov.path<-paste0('FILEPATH/ts_covariates.RData')
#3. Treatment Seeking data from DHS
DHS.path<-paste0("FILEPATH/subnational_TS.csv")

#4. Input years to model:
year.start<-ADDRESS

year.end<-ADDRESS

#MAKE PARAM FILE
params<-rbind(config.path,healthcare.accessibility.path,accessibility.path,VIIRS.path,IHME.cov.path,DHS.path,year.start,year.end)
write.table(params, file = paste0(output.path,"/parameters.txt"), row.names=TRUE,col.names=FALSE, quote=FALSE)

#============================================LOAD INPUT DATA========================================
#1. Inputs for Admin Units
config_file<-read.csv(config.path, stringsAsFactors = FALSE )
subnational_config<-read.csv(subnat.path)

#2. Covariates
#--MAP (static accessibility and night time lights covariates)
#Population averaged -> average travel time to city per capita.
healthcare.accessibility<-read.csv(healthcare.accessibility.path)
accessibility <- read.csv(accessibility.path)
VIIRS_nighttime <- read.csv(VIIRS.path)

##filter VIIRS to just 2012
VIIRS_nighttime<-VIIRS_nighttime[VIIRS_nighttime$year==2012,]
VIIRS_nighttime<-dplyr::select(VIIRS_nighttime, -"year")
#--IHME
load(IHME.cov.path)
#3. Treatment Seeking data from DHS
subnational_data<-read.csv(DHS.path) #ADMIN1

#4. Input years to model:
years <- year.start:year.end 

#===========================================CLEAN INPUT DATA========================================
#==================1. ADMIN UNITS===================================================================

sub.countries<-unique(config_file$IHME_location_id[config_file$MAP_Pf_Model_Method=="Reconstitute"])
sub.countries<-sub.countries[!is.na(sub.countries)]
subnational_config_1<-subnational_config[subnational_config$parent_id %in% sub.countries,]
subnational_config_2<-subnational_config[subnational_config$parent_id %in% subnational_config_1$location_id & subnational_config$location_type %in% c("admin1", "subnational"),]

admin1<-rbind(subnational_config_1, subnational_config_2)
rm(subnational_config_1, subnational_config_2)
# Count number of countries to be modelled for treatment seeking: 
length(sub.countries) 

admin0<-config_file[config_file$IHME_location_id %in% sub.countries & !is.na(config_file$IHME_location_id) ,]
#add admin0 names and ISO3 codes
a0.config<-admin0[,c("MAP_Country_Name", "IHME_location_id", "ISO3", "ISO2", "GAUL_Code","WHO_Region", "WHO_Subregion","IHME_Super_Region_ID", "IHME_Super_Region_Name", "IHME_Region_ID","IHME_Region_Name")]
names(a0.config)[1:2]<-c("CountryName", "parent_id")


a0.config<-admin0[,c("MAP_Country_Name", "IHME_location_id", "ISO3", "ISO2", "GAUL_Code","WHO_Region", "WHO_Subregion","IHME_Super_Region_ID", "IHME_Super_Region_Name", "IHME_Region_ID","IHME_Region_Name")]
names(a0.config)[1:2]<-c("CountryName", "parent_id")
admin1<-left_join(admin1, a0.config, by="parent_id")
nas<-admin1[is.na(admin1$CountryName),]
for(i in 1:nrow(nas)){
  id<-nas$location_id[i]
  parent_id<-nas$parent_id[i]
  admin1$CountryName[admin1$location_id==id]<-admin1$CountryName[admin1$location_id==parent_id]
  admin1$ISO3[admin1$location_id==id]<-admin1$ISO3[admin1$location_id==parent_id]
  admin1$ISO2[admin1$location_id==id]<-admin1$ISO2[admin1$location_id==parent_id]
  admin1$GAUL_Code[admin1$location_id==id]<-admin1$GAUL_Code[admin1$location_id==parent_id]
  admin1$WHO_Region[admin1$location_id==id]<-admin1$WHO_Region[admin1$location_id==parent_id]
  admin1$WHO_Subregion[admin1$location_id==id]<-admin1$WHO_Subregion[admin1$location_id==parent_id]
  admin1$IHME_Super_Region_ID[admin1$location_id==id]<-admin1$IHME_Super_Region_ID[admin1$location_id==parent_id]
  admin1$IHME_Super_Region_Name[admin1$location_id==id]<-admin1$IHME_Super_Region_Name[admin1$location_id==parent_id]
  admin1$IHME_Region_ID[admin1$location_id==id]<-admin1$IHME_Region_ID[admin1$location_id==parent_id]
  admin1$IHME_Region_Name[admin1$location_id==id]<-admin1$IHME_Region_Name[admin1$location_id==parent_id]
}

admin1 %>% 
  group_by_all() %>%
  filter(n() > 1) %>%
  ungroup()

nas %>% 
  group_by_all() %>%
  filter(n() > 1) %>%
  ungroup()


subnational_list<-unique(admin1$location_id)

which(is.na(admin1$ISO3))

# Remove Six Minor Territories  (aka Union Territories other than Delhi) and add them individually 
SMT_IDs<-c(4840,4858,4845,4847,4848,4866) #The order of the IDs matches the order of SMT names
subnational_list <- subnational_list[-which(subnational_list == ID)] #Remove The Six Minor Territories
six_minor_terr <- c("Andaman & Nicobar Islands", "Lakshadweep", "Chandigarh", "Dadra & Nagar Haveli", "Daman & Diu", "Puducherry")
SMT_list <- data.frame("ISO3" = "IND","ISO2"="IN","Gaul_Code"=115, "country_name" = "India", "Admin_Unit_Name" = six_minor_terr, "IHME_location_id" = SMT_IDs)


subnational_list <- c(subnational_list, SMT_IDs)

#Also, remove China(without Hong Kond and Macao) and other units which are not necessary
units_to_remove<-subnational_list[!subnational_list %in% c(VIIRS_nighttime$loc_id, SMT_IDs)]
subnational_list <- subnational_list[-which(subnational_list %in% units_to_remove)] 

admin1<-admin1[admin1$location_id %in% c(subnational_list),]



admin1$location_type<- "ADMIN1"

####FINISHED ADDING ADMIN1 here - continue below.

#==================2. COVARIATES (IHME) ===================================================================
# Choose only the covariates we wil use in the model:
cov_names <- rep(NA, length(ts_covs))
for (i in 1:length(cov_names)){
  cov_names[i] <- unique(ts_covs[[i]]$covariate_name_short)
}
names(ts_covs) <- cov_names
ts_covs <- ts_covs[which(cov_names %in% c("ANC1_coverage_prop", "ANC4_coverage_prop","DTP3_coverage_prop","hospital_beds_per1000","IFD_coverage_prop","LDI_pc","measles_vacc_cov_prop", "SBA_coverage_prop","GDPpc_id_b2010", "prop_urban", "he_cap", "measles_vacc_cov_prop_2", "ind_health", "education_all_ages_and_sexes_pc", "log_the_pc" ))] 
cov_names <- rep(NA, length(ts_covs))
for (i in 1:length(cov_names)){
  cov_names[i] <- unique(as.character(ts_covs[[i]]$covariate_name_short))
}
names(ts_covs) <- cov_names

save(ts_covs,file=paste0(output.path,"/ts_covariates_clean.RData"))

#==================3. TREATMENT SEEKING DATA FROM DHS===================================================================
subnational_data$CountryName <- as.character(subnational_data$CountryName)
config_file<-filter(config_file,!is.na(ISO3))

########## ---------------------- 1. CREATE FULL ORIGINAL DATASETS -------------------- ############
admin1<-dplyr::select(admin1, c("ISO3", "CountryName", "location_id", "location_type", "location_ascii_name", "IHME_Super_Region_Name", "IHME_Region_Name"))

names(admin1)<-c("ISO3", "Country_Name", "IHME_location_id", "Admin_Unit_Level", "Admin_Unit_Name", "IHME_Super_Region_Name", "IHME_Region_Name")


#---Make a full metadata set
metadb<-admin1
metadb$Year<-years[1]
metadb$SMT_Factor<-"Non_SMT"
full_metadb<-metadb


for(y in 2:length(years)){
  metadb$Year<-years[y]
  full_metadb<-rbind(full_metadb, metadb)
}

SMT_rows<-full_metadb[full_metadb$Admin_Unit_Name=="Assam",]
SMT_rows$IHME_location_id<-44538

for(ids in SMT_IDs){
  SMT_rows$Admin_Unit_Name<-unique(SMT_list$Admin_Unit_Name[SMT_list$IHME_location_id==ids])
  SMT_rows$SMT_Factor<-"SMT"
  full_metadb<-rbind(full_metadb,SMT_rows)
}



nrow(full_metadb)==(nrow(admin1)+6)*length(years)

#--- Now add the IHME covariates:

full_TreatSeek<-full_metadb

for(i in 1:length(ts_covs)){
  ts_i<-ts_covs[[i]]
  ts_i<-dplyr::select(ts_i, c("location_id", "year_id", "mean_value"))
  names(ts_i)<-c("IHME_location_id","Year",names(ts_covs)[i])
  full_TreatSeek<-dplyr::left_join(full_TreatSeek, ts_i, by=c("IHME_location_id","Year"))
}

#--- Now add MAP covars:
#healthcare accessibility
healthcare.accessibility<-dplyr::select(healthcare.accessibility, c("loc_id","weighted_rate"))
names(healthcare.accessibility)<-c("IHME_location_id","healthcare_accessibility")
full_TreatSeek<-left_join(full_TreatSeek, healthcare.accessibility, by=c("IHME_location_id"))

#accessibility to cities
accessibility<-dplyr::select(accessibility, c("loc_id","weighted_rate"))
names(accessibility)<-c("IHME_location_id","accessibility")
full_TreatSeek<-left_join(full_TreatSeek, accessibility, by=c("IHME_location_id"))

# ____________________
# No NA
full_TreatSeek %>% 
  filter(is.na(accessibility))
full_TreatSeek %>% 
  filter(is.na(healthcare_accessibility))
full_TreatSeek %>% 
  filter(accessibility == 0) %>% 
  distinct(Country_Name)
# Indo, China
full_TreatSeek %>% 
  filter(healthcare_accessibility == 0) %>% 
  distinct(Country_Name)
# China

# ____________________


VIIRS_nighttime<-dplyr::select(VIIRS_nighttime, c("loc_id","weighted_rate"))

names(VIIRS_nighttime)<-c("IHME_location_id","VIIRS_nighttime")
full_TreatSeek<-left_join(full_TreatSeek, VIIRS_nighttime, by=c("IHME_location_id"))

nrow(full_TreatSeek)

nrow(full_TreatSeek)==(nrow(admin1)+6)*length(years)


#Change SMT ids back
for(i in 1:length(six_minor_terr)){
  full_TreatSeek$IHME_location_id[full_TreatSeek$Admin_Unit_Name==six_minor_terr[i]]<-SMT_IDs[i]
  
}


write.csv(full_TreatSeek, paste(data.path, "full_TreatSeek.csv", sep = ""),row.names=FALSE)

########## ---------------------- 2. Normalise the Covariates -------------------- ############

full_TreatSeek_n <- full_TreatSeek

# Check the distributions of the covariates before normalisation:

full_TreatSeek %>%
  filter(healthcare_accessibility == 0) %>% 
  distinct(Country_Name)

typeof(full_TreatSeek$healthcare_accessibility)
class(full_TreatSeek$healthcare_accessibility)

subset(full_TreatSeek, healthcare_accessibility < 0)
subset(full_TreatSeek, healthcare_accessibility < 0.0001)

full_TreatSeek %>%
  filter(accessibility == 0) %>% 
  distinct(Country_Name)

full_TreatSeek %>% 
  filter(healthcare_accessibility < 0)

pdf(paste(graphics.path, 'covariate_transformations_prior_norm.pdf', sep = ''), width = 12, height = 8)

hist(full_TreatSeek$he_cap)

par(mfrow = c(2, 3))
hist(full_TreatSeek$ANC1_coverage_prop^2)
hist(full_TreatSeek$ANC4_coverage_prop)
hist(full_TreatSeek$DTP3_coverage_prop)
hist(log(full_TreatSeek$hospital_beds_per1000))
hist(full_TreatSeek$IFD_coverage_prop)
hist(log(full_TreatSeek$LDI_pc))
hist(full_TreatSeek$measles_vacc_cov_prop^2)
hist(full_TreatSeek$SBA_coverage_prop)
hist(log(full_TreatSeek$GDPpc_id_b2010))
hist(full_TreatSeek$prop_urban)
hist(log(full_TreatSeek$he_cap))

hist(log(full_TreatSeek$measles_vacc_cov_prop_2 + 0.0001))# Response variable must be positive.
hist(log(full_TreatSeek$ind_health))
hist(full_TreatSeek$education_all_ages_and_sexes_pc)
hist(full_TreatSeek$log_the_pc) 
hist(log(full_TreatSeek$healthcare_accessibility))
hist(log(full_TreatSeek$accessibility + 0.0001))
hist(log(full_TreatSeek$VIIRS_nighttime))

dev.off()

# Transform prior normalisation:
full_TreatSeek_n$ANC1_coverage_prop <- full_TreatSeek_n$ANC1_coverage_prop^2
full_TreatSeek_n$hospital_beds_per1000 <- log(full_TreatSeek_n$hospital_beds_per1000)
full_TreatSeek_n$LDI_pc <- log(full_TreatSeek_n$LDI_pc)
full_TreatSeek_n$measles_vacc_cov_prop <- full_TreatSeek_n$measles_vacc_cov_prop^2
full_TreatSeek_n$measles_vacc_cov_prop_2 <- log(full_TreatSeek_n$measles_vacc_cov_prop_2 + 0.0001)
full_TreatSeek_n$GDPpc_id_b2010 <- log(full_TreatSeek_n$GDPpc_id_b2010)
full_TreatSeek_n$he_cap <- log(full_TreatSeek_n$he_cap)
full_TreatSeek_n$ind_health <- log(full_TreatSeek_n$ind_health)
full_TreatSeek_n$healthcare_accessibility <- log(full_TreatSeek_n$healthcare_accessibility)
full_TreatSeek_n$accessibility <- log(full_TreatSeek_n$accessibility + 0.0001)
full_TreatSeek_n$VIIRS_nighttime <- log(full_TreatSeek_n$VIIRS_nighttime)

# _________________________

full_TreatSeek_n %>% 
  filter(is.na(healthcare_accessibility))

# _________________________


#Specify the covariate column numbers
first_covar<-names(ts_covs[1])
col.start<-which(colnames(full_TreatSeek)==first_covar)
col.end<-col.start+length(ts_covs)+2 #IHME covariates plus 3 MAP covariates

#MARGIN=2 indicates that the function will be applied accross the specified columns
full_TreatSeek_n[, col.start:col.end] <- apply(full_TreatSeek_n[, col.start:col.end], MARGIN = 2, FUN = function(x){(x - mean(x))/sd(x)}) 

#check if the covariates are in fact normalised (all should have mean 0)
summary(full_TreatSeek_n)

full_TreatSeek_n$IHME_Region_Name <- droplevels(as.factor(full_TreatSeek_n$IHME_Region_Name)) #I added "as.factor() as otherwise got an error applying the droplevels function on character variables.

write.csv(full_TreatSeek_n, paste(data.path, "full_TreatSeek_n.csv", sep = ""),row.names=FALSE)

########## ---------------------- 3. CREATE CLEAN SUBSETS WITH TS DATA FOR MODELLING -------------------- ############
# ------------- Check number of IHME  regions in TS data -------- #
names(subnational_data)

subnational_data$Outlier_Any<-"NO"
subnational_data$Outlier_HMIS<-"NO"
subnational_data$Outlier_Any[subnational_data$iso3=="NGA" & subnational_data$SurveyYear==2008]<-"YES"
subnational_data$Outlier_HMIS[subnational_data$iso3=="NGA" & subnational_data$SurveyYear==2008]<-"YES"

subnational_data<-dplyr::select(subnational_data, c("location_id","SurveyYear","SurveyType","HMIS_treat","HMIS_treat_lower","HMIS_treat_upper","Any_treat","Any_treat_lower","Any_treat_upper","Outlier_HMIS","Outlier_Any"))
names(subnational_data)<-c("IHME_location_id","Year","SurveyType","HMIS_treat","HMIS_treat_low","HMIS_treat_high","Any_treat","Any_treat_low","Any_treat_high","Outlier_HMIS","Outlier_Any")

dhs_data<-subnational_data

dhs_data %>% 
  group_by_all() %>%
  filter(n() > 1) %>%
  ungroup()

clean_TreatSeek<-left_join(full_TreatSeek_n, dhs_data, by= c("IHME_location_id","Year"))

clean_TreatSeek %>% 
  group_by_all() %>%
  filter(n() > 1) %>%
  ungroup()

clean_TreatSeek <- distinct(clean_TreatSeek)


#remove NAs and Outliers 
clean_TreatSeek_Any <- clean_TreatSeek[!is.na(clean_TreatSeek$Any_treat), ]
clean_TreatSeek_Any<-clean_TreatSeek_Any[clean_TreatSeek_Any$Outlier_Any=="NO",]

clean_TreatSeek_HMISfrac <- clean_TreatSeek_Any[!is.na(clean_TreatSeek_Any$HMIS_treat/clean_TreatSeek_Any$Any_treat)  & (clean_TreatSeek_Any$HMIS_treat != clean_TreatSeek_Any$Any_treat), ] # To be able to do logit transform later. logit has a nice interpretation and leads to values between 0 and 1. Non-normality could be due to underlying covariates.\
clean_TreatSeek_HMISfrac<-clean_TreatSeek_HMISfrac[clean_TreatSeek_HMISfrac$Outlier_HMIS=="NO",]
clean_TreatSeek_HMISfrac$HMIS_frac <- clean_TreatSeek_HMISfrac$HMIS_treat/clean_TreatSeek_HMISfrac$Any_treat

clean_TreatSeek_Any$logit_Any <- logitlink(clean_TreatSeek_Any$Any_treat)
clean_TreatSeek_HMISfrac$logit_HMIS <- logitlink(clean_TreatSeek_HMISfrac$HMIS_frac)

clean_TreatSeek_Any <- clean_TreatSeek_Any[is.finite(clean_TreatSeek_Any$logit_Any), ]
clean_TreatSeek_HMISfrac <- clean_TreatSeek_HMISfrac[is.finite(clean_TreatSeek_HMISfrac$logit_HMIS), ]

summary(clean_TreatSeek_Any$IHME_Region_Name)
summary(clean_TreatSeek_HMISfrac$IHME_Region_Name)

clean_TreatSeek_Any %>% 
  group_by_all() %>%
  filter(n() > 1) %>%
  ungroup()

names(clean_TreatSeek_Any)

clean_TreatSeek_Any<-dplyr::select(clean_TreatSeek_Any, -c("Outlier_Any","Outlier_HMIS"))
clean_TreatSeek_HMISfrac<-dplyr::select(clean_TreatSeek_HMISfrac, -c("Outlier_Any","Outlier_HMIS"))

write.csv(clean_TreatSeek_Any, paste(data.path, 'clean_TreatSeek_Any.csv', sep = ''))
write.csv(clean_TreatSeek_HMISfrac, paste(data.path, 'clean_TreatSeek_HMISfrac.csv', sep = ''))
######### ---------------------- 4. CREATE TRAINING AND TEST SETS FOR CHOOSING MODELS -------------------- ###########

# ------------------- Remove pre-1990 data (and outliers) and create training/test sets -----------------

clean_TreatSeek_Any <- read.csv(paste(data.path, 'clean_TreatSeek_Any.csv', sep = ''))
clean_TreatSeek_HMISfrac <- read.csv(paste(data.path, 'clean_TreatSeek_HMISfrac.csv', sep = ''))

master.country.list <- unique(clean_TreatSeek_Any$Country_Name)


#First we make the training and test (validating) sets for the model of treatment seeking at any facility (public, private, NGO)
set.seed(1)
train.id <- numeric()
for (i in 1:length(master.country.list)){
  region.id <- which(clean_TreatSeek_Any$Country_Name == master.country.list[i])
  years<-clean_TreatSeek_Any$Year[region.id]
  early.y<-region.id[which(years<2000)] #Make sure the 1990-2000 surveys don't get lost in random sampling.
  late.y<-region.id[which(years>=2000)]
  if (length(region.id) > 8){
    no.sample.early <- ceiling(length(early.y)*0.7)
    no.sample.late <- ceiling(length(late.y)*0.7)
    temp.id.e <- sample(early.y, no.sample.early)
    temp.id.l <- sample(late.y, no.sample.late)
  }else{temp.id <- region.id}
  train.id <- c(train.id, temp.id.e, temp.id.l)
}

length(train.id)/nrow(clean_TreatSeek_Any) # 0.7060302 - this is the proportion of the treatment seeking data that was used to inform the model. the remaining ~30% will be used to validate the model

train_data_Any <- clean_TreatSeek_Any[train.id, ]

train_data_Any %>% 
  group_by_all() %>%
  filter(n() > 1) %>%
  ungroup()

summary(train_data_Any$Country_Name)

nrow(train_data_Any) 
train_data_Any <- as.data.frame(train_data_Any) #training set (~70% survey DHS data)

for (i in 1:length(six_minor_terr)){
  if (!(six_minor_terr[i] %in% train_data_Any$Admin_Unit_Name)){
    train_data_Any <- rbind(train_data_Any, clean_TreatSeek_Any[clean_TreatSeek_Any$Admin_Unit_Name == six_minor_terr[i], ]) # Only have one data_Any point per SMT at the moment.
  }
}

test_data_Any <- clean_TreatSeek_Any[!(clean_TreatSeek_Any$X %in% train_data_Any$X), ] #model validation set (~30% DHS survey data)
nrow(test_data_Any)

#Second, we make the same training and testing sets for the model of treatment seeking at public facilities
set.seed(1)
train.id <- numeric()
for (i in 1:length(master.country.list)){
  region.id <- which(clean_TreatSeek_HMISfrac$Country_Name == master.country.list[i])
  years<-clean_TreatSeek_HMISfrac$Year[region.id]
  early.y<-region.id[which(years<2000)] #Make sure the 1990-2000 surveys don't get lost in random sampling.
  late.y<-region.id[which(years>=2000)]
  if (length(region.id) > 8){
    no.sample.early <- ceiling(length(early.y)*0.7)
    no.sample.late <- ceiling(length(late.y)*0.7)
    temp.id.e <- sample(early.y, no.sample.early)
    temp.id.l <- sample(late.y, no.sample.late)
  }else{temp.id <- region.id}
  train.id <- c(train.id, temp.id.e, temp.id.l)
}



train_data_HMISfrac <- clean_TreatSeek_HMISfrac[train.id, ]

train_data_HMISfrac <- as.data.frame(train_data_HMISfrac)

for (i in 1:length(six_minor_terr)){
  if (!(six_minor_terr[i] %in% train_data_HMISfrac$Admin_Unit_Name)){
    train_data_HMISfrac <- rbind(train_data_HMISfrac, clean_TreatSeek_HMISfrac[(clean_TreatSeek_HMISfrac$Admin_Unit_Name == six_minor_terr[i]), ]) # Only have one data_HMISfrac point per SMT at the moment.
  }
}

test_data_HMISfrac <- clean_TreatSeek_HMISfrac[!(clean_TreatSeek_HMISfrac$X %in% train_data_HMISfrac$X), ]

# Save the training and test sets:
write.csv(train_data_Any, paste(data.path, 'train_data_Any.csv', sep = ''), row.names=FALSE)
write.csv(train_data_HMISfrac, paste(data.path, 'train_data_HMISfrac.csv', sep = ''), row.names=FALSE)
write.csv(test_data_Any, paste(data.path, 'test_data_Any.csv', sep = ''), row.names=FALSE)
write.csv(test_data_HMISfrac, paste(data.path, 'test_data_HMISfrac.csv', sep = ''), row.names=FALSE)



########## ---------------------- 4. CREATE 100 FULL DATASETS TO ACCOUNT FOR TS AND IHME COVARIATE VARIABILITY -------------------- ############

# These are alternative full_TreatSeek_n datasets for fitting and predicting.

N_TS <- 100

TS_datasets <- rep(list(NA), N_TS)
TS_clean_datasets_Any<- rep(list(NA), N_TS)
TS_clean_datasets_HMIS<- rep(list(NA), N_TS)

names(TS_datasets) <- paste("Dataset", 1:N_TS, sep = "_")
names(TS_clean_datasets_Any) <- paste("Dataset", 1:N_TS, sep = "_")
names(TS_clean_datasets_HMIS) <- paste("Dataset", 1:N_TS, sep = "_")

temptime3 <- proc.time()[3]

for (TS_i in 1:N_TS){
  set.seed(TS_i)
  
  
  full_TreatSeek_i<-full_metadb
  
  #--- Now add the IHME covariates:
  full_TreatSeek_i$IHME_location_id[full_TreatSeek$Admin_Unit_Name %in% six_minor_terr]<-ID
  
  
  
  for(i in 1:length(ts_covs)){
    ts_i<-ts_covs[[i]]
    ts_i$rls_value<-ts_i$mean_value
    
    indx<-which(ts_i$lower_value != ts_i$upper_value)
    for(x in indx){
      ts_i$rls_value[x]<-rnorm(1, mean=ts_i$mean_value[x], sd=((ts_i$upper_value[x]-ts_i$lower_value[x])/(2*qnorm(0.975))))
    }
    
    ts_i<-dplyr::select(ts_i, c("location_id", "year_id", "rls_value"))
    names(ts_i)<-c("IHME_location_id","Year",names(ts_covs)[i])
    full_TreatSeek_i<-dplyr::left_join(full_TreatSeek_i, ts_i, by=c("IHME_location_id","Year"))
  }
  
 
  #--- Now add MAP covars:
  #healthcare accessibility
  full_TreatSeek_i<-left_join(full_TreatSeek_i, healthcare.accessibility, by=c("IHME_location_id"))
  
  #accessibility to cities
  full_TreatSeek_i<-left_join(full_TreatSeek_i, accessibility, by=c("IHME_location_id"))
  
  #VIIRS_nighttime
  full_TreatSeek_i<-left_join(full_TreatSeek_i, VIIRS_nighttime, by=c("IHME_location_id"))
  
  
  #Change SMT ids back
  for(i in 1:length(six_minor_terr)){
    full_TreatSeek_i$IHME_location_id[full_TreatSeek_i$Admin_Unit_Name==six_minor_terr[i]]<-SMT_IDs[i]
    
  }
  
  # Can check values before taking log transforms etc. 
  full_TreatSeek_i$ANC1_coverage_prop <-  sapply(full_TreatSeek_i$ANC1_coverage_prop, function(x){min(max(x, 0), 1)})
  full_TreatSeek_i$ANC4_coverage_prop <-  sapply(full_TreatSeek_i$ANC4_coverage_prop, function(x){min(max(x, 0), 1)})
  full_TreatSeek_i$DTP3_coverage_prop <-  sapply(full_TreatSeek_i$DTP3, function(x){min(max(x, 0), 1)})
  full_TreatSeek_i$IFD_coverage_prop <- sapply(full_TreatSeek_i$IFD_coverage_prop, function(x){min(max(x, 0), 1)})
  full_TreatSeek_i$measles_vacc_cov_prop <-  sapply(full_TreatSeek_i$measles_vacc_cov_prop, function(x){min(max(x, 0), 1)})
  full_TreatSeek_i$measles_vacc_cov_prop_2 <-  sapply(full_TreatSeek_i$measles_vacc_cov_prop_2, function(x){min(max(x, 0), 1)})
  full_TreatSeek_i$SBA_coverage_prop <-  sapply(full_TreatSeek_i$SBA_coverage_prop, function(x){min(max(x, 0), 1)})
  full_TreatSeek_i$prop_urban <-  sapply(full_TreatSeek_i$prop_urban, function(x){min(max(x, 0), 1)})
  full_TreatSeek_i$education_all_ages_and_sexes_pc <-  sapply(full_TreatSeek_i$education_all_ages_and_sexes_pc, function(x){min(max(x, 0), 99)})
  full_TreatSeek_i$log_the_pc <- sapply(full_TreatSeek_i$log_the_pc, function(x){max(x, 0)})
  full_TreatSeek_i$hospital_beds_per1000 <-  sapply(full_TreatSeek_i$education_all_ages_and_sexes_pc, function(x){min(max(x, 0.0001), 1000)})
  full_TreatSeek_i$LDI_pc <- sapply(full_TreatSeek_i$LDI_pc, function(x){max(x, 0.0001)})
  full_TreatSeek_i$GDPpc_id_b2010 <- sapply(full_TreatSeek_i$GDPpc_id_b2010, function(x){max(x, 0.0001)})
  full_TreatSeek_i$he_cap <- sapply(full_TreatSeek_i$he_cap, function(x){max(x, 0.0001)})
  full_TreatSeek_i$ind_health <- sapply(full_TreatSeek_i$ind_health, function(x){min(max(x, 0.0001), 1)})
  
  # Transform prior normalisation:
  full_TreatSeek_i$ANC1_coverage_prop <- full_TreatSeek_i$ANC1_coverage_prop^2
  full_TreatSeek_i$hospital_beds_per1000 <- log(full_TreatSeek_i$hospital_beds_per1000)
  full_TreatSeek_i$LDI_pc <- log(full_TreatSeek_i$LDI_pc)
  full_TreatSeek_i$measles_vacc_cov_prop <- full_TreatSeek_i$measles_vacc_cov_prop^2
  full_TreatSeek_i$measles_vacc_cov_prop_2 <- log(full_TreatSeek_i$measles_vacc_cov_prop_2 + 0.0001)
  full_TreatSeek_i$GDPpc_id_b2010 <- log(full_TreatSeek_i$GDPpc_id_b2010)
  full_TreatSeek_i$he_cap <- log(full_TreatSeek_i$he_cap)
  full_TreatSeek_i$ind_health <- log(full_TreatSeek_i$ind_health)
  full_TreatSeek_i$healthcare_accessibility <- log(full_TreatSeek_i$healthcare_accessibility)
  full_TreatSeek_i$VIIRS_nighttime <- log(full_TreatSeek_i$VIIRS_nighttime)
  full_TreatSeek_i$accessibility <- log(full_TreatSeek_i$accessibility + 0.0001)
  
  full_TreatSeek_i_copy<-full_TreatSeek_i
  
  full_TreatSeek_i[, col.start:col.end] <- apply(full_TreatSeek_i[, col.start:col.end], MARGIN = 2, FUN = function(x){(x - mean(x))/sd(x)}) 
  
  
  
  str(full_TreatSeek_i)
  
  clean_TreatSeek_Any_i<-dplyr::select(clean_TreatSeek_Any, c("IHME_location_id","Admin_Unit_Name", "Year","Any_treat","Any_treat_low","Any_treat_high"))
  clean_TreatSeek_HMIS_i<-dplyr::select(clean_TreatSeek_HMISfrac, c("IHME_location_id","Admin_Unit_Name", "Year","HMIS_treat","HMIS_treat_low","HMIS_treat_high"))
  
  for(r in 1: nrow(clean_TreatSeek_Any_i)){
    clean_TreatSeek_Any_i$rls_value[r]<-rnorm(1, mean=clean_TreatSeek_Any_i$Any_treat[r], sd=((clean_TreatSeek_Any_i$Any_treat_high[r]-clean_TreatSeek_Any_i$Any_treat_low[r])/(2*qnorm(0.975))))
  }
  
  for(r in 1: nrow(clean_TreatSeek_HMIS_i)){
    clean_TreatSeek_HMIS_i$rls_value[r]<-rnorm(1, mean=clean_TreatSeek_HMIS_i$HMIS_treat[r], sd=((clean_TreatSeek_HMIS_i$HMIS_treat_high[r]-clean_TreatSeek_HMIS_i$HMIS_treat_low[r])/(2*qnorm(0.975))))
  }
  
  
  clean_TreatSeek_Any_i$Any_treat<-clean_TreatSeek_Any_i$rls_value
  clean_TreatSeek_Any_i$Any_treat[clean_TreatSeek_Any_i$Any_treat>1]<-0.999
  clean_TreatSeek_Any_i$Any_treat[clean_TreatSeek_Any_i$Any_treat<0.001] <- 0.001
  clean_TreatSeek_Any_i<-dplyr::select(clean_TreatSeek_Any_i, -c("Any_treat_low", "Any_treat_high", "rls_value"))
  
  clean_TreatSeek_HMIS_i$HMIS_treat<-clean_TreatSeek_HMIS_i$rls_value
  clean_TreatSeek_HMIS_i$HMIS_treat[clean_TreatSeek_HMIS_i$HMIS_treat>1]<-0.999
  clean_TreatSeek_HMIS_i$HMIS_treat[clean_TreatSeek_HMIS_i$HMIS_treat<0.001] <- 0.001
  clean_TreatSeek_HMIS_i<-dplyr::select(clean_TreatSeek_HMIS_i, -c("HMIS_treat_low", "HMIS_treat_high", "rls_value"))
  

  clean_TreatSeek_Any_i$logit_Fev <- logitlink(clean_TreatSeek_Any_i$Any_treat)
  clean_TreatSeek_HMIS_i$logit_Fev <- logitlink(clean_TreatSeek_HMIS_i$HMIS_treat)
  
  TS_datasets[[TS_i]] <- full_TreatSeek_i 
  TS_clean_datasets_Any[[TS_i]]<-clean_TreatSeek_Any_i
  TS_clean_datasets_HMIS[[TS_i]]<-clean_TreatSeek_HMIS_i
  
  print(TS_i)
  
}

timetaken3 <- proc.time()[3] - temptime3

save(TS_datasets, file = paste(data.path, 'TS_datasets_for_prediction.RData', sep = ''))
save(TS_clean_datasets_Any, file = paste(data.path, 'TS_datasets_input_Any.RData', sep = ''))
save(TS_clean_datasets_HMIS, file = paste(data.path, 'TS_datasets_input_HMIS.RData', sep = ''))


load(paste(data.path, 'TS_datasets_for_prediction.RData', sep = ''))
full_TreatSeek_i <- TS_datasets[[3]]
summary(full_TreatSeek_i)

summary(as.factor(full_TreatSeek_i$IHME_Region_Name))
summary(as.factor(full_TreatSeek_n$IHME_Region_Name))

########## ---------------------- 5. CHECK IHME COVARIATE VARIABILITY -------------------- ############

#--- inspect IHME covariates:
#temporarily change GUF and MYT loc ids
cov_variability<-full_metadb
cov_variability$IHME_location_id[cov_variability$ISO3=="GUF"]<-unique(admin0$IHME_location_id[admin0$ISO3 == "SUR"])
cov_variability$IHME_location_id[cov_variability$ISO3=="MYT"]<-  unique(admin0$IHME_location_id[admin0$ISO3 == "COM"])


for(i in 1:length(ts_covs)){
  ts_i<-ts_covs[[i]]
  ts_i<-dplyr::select(ts_i, c("location_id", "year_id", "mean_value","lower_value","upper_value" ))
  names(ts_i)[1:2]<-c("IHME_location_id","Year")
  covariate_i<-left_join(cov_variability, ts_i, by=c("IHME_location_id","Year"))
  
  pdf(paste(graphics.path, names(ts_covs)[i], '.pdf', sep = ''),width=8.7,height = 11.2)
  
  par(mfrow=c(3,2))
  
  for (l in 1:length(master.country.list)){
    region.data <- covariate_i[covariate_i$Country_Name == master.country.list[l], ]
    countries <- unique(region.data$ISO3)
    for (m in 1:length(countries)){
      unit.list <- unique(region.data[region.data$ISO3 == countries[m], c("Admin_Unit_Name", "Admin_Unit_Level")])
      for (n in 1:nrow(unit.list)){
        unit.row <- region.data[region.data$ISO3 == countries[m] & region.data$Admin_Unit_Name == unit.list$Admin_Unit_Name[n] & region.data$Admin_Unit_Level == unit.list$Admin_Unit_Level[n], ]
        plot(0,0,type='n',c(min(covariate_i$lower_value, na.rm = TRUE),max(covariate_i$upper_value, na.rm = TRUE)),xlim=c(year.start, year.end),main=paste(master.country.list[l], ': ', unit.list$Admin_Unit_Name[n] , ", ", countries[m], sep = ''), ylab='IHME covariate', xlab='Year')
        
        plotCI(year.start:year.end,unit.row$mean_value,ui=unit.row$upper_value,li=unit.row$lower_value,ylim=c(min(covariate_i$lower_value, na.rm = TRUE),max(covariate_i$upper_value, na.rm = TRUE)),add=T)
      }
    }
  }
  
  dev.off()
  print(names(ts_covs)[i])
}

# 2-Model)Selection_RTTIR.R 

rm(list = ls())

#===========================================SET OUTPUT DIRECTORIES==================================
output.path<-paste0('FILEPATH')
graphics.path <- paste0(FILEPATH)
dir.create(graphics.path) #The graphics are split by model selection steps
data.path <- paste0(FILEPATH)
table.path<-paste0(FILEPATH)

#================================SPECIFY INPUT PATHS======================
# Years to model:
years <- 1980:2025 

train_data_Any <- read.csv(paste(data.path, 'train_data_Any.csv', sep = ''))
train_data_HMISfrac <- read.csv(paste(data.path, 'train_data_HMISfrac.csv', sep = ''))

train_data_Any<-train_data_Any[train_data_Any$Year %in% years,]
train_data_HMISfrac<-train_data_HMISfrac[train_data_HMISfrac$Year %in% years,]

train_data_Any$IHME_Region_Name<-as.factor(train_data_Any$IHME_Region_Name) #Need to transform IHME_Region_Name to factor
train_data_HMISfrac$IHME_Region_Name<-as.factor(train_data_HMISfrac$IHME_Region_Name)

# Models to be tested:

model_no <- "RTTIR_" # 1. RTTIR: IHME region temporal trend and factors; 2. PTTIR: Pruned regions temporal trend and IHME regional factors; 3. PTTPF: Pruned regions temporal trend and factors; 4. OC: Other considerations.


########## ---------------------- 1. CHOOSE SUBSETS OF COVARIATES TO CONSIDER IN MODEL WHICH HAVE LOW COLLINEARITY -------------------- ############

## Correlation between covariates:
# Get columns with covariates. 
cov_col_vector<-names(train_data_Any)[(which(names(train_data_Any)=="Year")+1):(which(names(train_data_Any)=="SurveyType")-1)]

cov_col <- train_data_Any[, cov_col_vector]
#Specify the covariate column numbers
first_covar<-cov_col_vector[1]
col.start<-which(colnames(train_data_Any)==first_covar)
col.end<-col.start+length(cov_col_vector)-1 

# Fit univariate GAMs on the covariates and compare AIC, then select covariates to include in final model depending on their relative AIC and correlation with each other:

cov_ind <- c(col.start:col.end) # check covariate names.
AIC_Any <- rep(NA, length(cov_ind))
names(AIC_Any) <- names(train_data_Any)[cov_ind]

AIC_HMIS <- rep(NA, length(cov_ind))
names(AIC_HMIS) <- names(train_data_HMISfrac)[cov_ind]

summary(train_data_Any)
table(train_data_Any$IHME_Region_Name)
table(train_data_Any$Year)



for (i in 1:length(cov_ind)){
  var_values_Any <- train_data_Any[, cov_ind[i]] 
  gam_Any <- gam(logit_Any ~ -1 + IHME_Region_Name + s(Year, by = IHME_Region_Name) + s(X), data = data.frame("logit_Any" = train_data_Any$logit_Any, "X" = var_values_Any, 'Year' = train_data_Any$Year, 'IHME_Region_Name' = train_data_Any$IHME_Region_Name))
  var_values_HMIS <- train_data_HMISfrac[, cov_ind[i]]
  gam_HMIS <- gam(logit_HMIS ~ -1 + IHME_Region_Name + s(Year, by = IHME_Region_Name) + s(X), data =  data.frame("logit_HMIS" = train_data_HMISfrac$logit_HMIS, "X" = var_values_HMIS, 'Year' = train_data_HMISfrac$Year, 'IHME_Region_Name' = train_data_HMISfrac$IHME_Region_Name))
  AIC_Any[i] <- gam_Any$aic
  AIC_HMIS[i] <- gam_HMIS$aic
}

AIC_Any_order <- names(sort(AIC_Any[!(names(AIC_Any) %in% c("measles_vacc_cov_prop_2", "oop_hexp_cap", "ind_health"))]))

test_cov_mat <- cor(cov_col[, AIC_Any_order])
abs_mat <- abs(test_cov_mat) >0.6 # For checking which covariates are strongly correlated to those of increasing AIC.

test_cov_Any <- AIC_Any_order[1]

for (i in AIC_Any_order[-1]){
  # If candidate covariate has < 0.6 correlation with existing covariates.
  if (sum(abs_mat[i, test_cov_Any])< 1){
    test_cov_Any <- c(test_cov_Any, i)
  }
}

AIC_HMIS_order <- names(sort(AIC_HMIS[!(names(AIC_HMIS) %in% c("measles_vacc_cov_prop_2", "education_all_ages_and_sexes_pc", "ind_health"))]))

test_cov_mat <- cor(cov_col[, AIC_HMIS_order])
abs_mat <- abs(test_cov_mat) >0.6 # For checking which covariates are strongly correlated to those of increasing AIC.

test_cov_HMIS <- AIC_HMIS_order[1]

for (i in AIC_HMIS_order[-1]){
  # If candidate covariate has < 0.6 correlation with existing covariates.
  if (sum(abs_mat[i, test_cov_HMIS])< 1){
    test_cov_HMIS <- c(test_cov_HMIS, i)
  }
}

########## ---------------------- 2. FIT THE PRELIMINARY MODELS ON TS MEANS OF TRAINING DATA -------------------- ############

# Check for regional trends to omit

#  Use covariates identified in test_cov_Any:

formula_any <-  logit_Any ~ -1 + IHME_Region_Name + s(Year, by = IHME_Region_Name, k = 5)+s(ANC4_coverage_prop, k=3)+s(hospital_beds_per1000, k=3)+s(healthcare_accessibility,k=3)+s(measles_vacc_cov_prop, k=3)+s(prop_urban, k=3)

test.model.any <- gam(formula_any, data = train_data_Any)

summary(test.model.any) 

 
formula_HMIS <- logit_HMIS ~ -1 + IHME_Region_Name + s(Year, by = IHME_Region_Name, k = 5)+s(healthcare_accessibility, k=3)+s(DTP3_coverage_prop, k=3)+s(ANC1_coverage_prop, k=3)+s(hospital_beds_per1000, k=3)+s(prop_urban, k=3)+s(LDI_pc, k=3)

test.model.hmis <- gam(formula_HMIS, data = train_data_HMISfrac)

summary(test.model.hmis) 

########## ---------------------- 3. EXAMINE THE FINAL MODELS -------------------- ############

# To be changed for different models:

gamtabs(test.model.any, caption = "Summary of first any TS model with IHME region temporal trends and factors")
# Look at the plots of the smooths 
pdf(paste(graphics.path, model_no, "Any_model_smooths.pdf", sep = ""), width = 12, height = 8)
par(mfrow = c(2, 3))
for(i in 1:30){
  plot(test.model.any, select = i, ylim = c(-5, 5))
  abline(h=0, col="red", lty=2)
}

dev.off()


gamtabs(test.model.hmis, caption = "Summary of first public fraction model with IHME region temporal trends and factors")

pdf(paste(graphics.path, model_no, "HMIS_model_smooths.pdf", sep = ""), width = 12, height = 8)
par(mfrow = c(2, 3))
for(i in 1:30){
  plot(test.model.hmis, select = i, ylim = c(-5, 5))
  abline(h=0, col="red", lty=2)
}

dev.off()


# 3-Model_Selection_PTTIR.R


rm(list = ls())
#===========================================SET OUTPUT DIRECTORIES==================================
output.path<-paste0('FILEPATH') 
graphics.path <- paste0(FILEPATH)
dir.create(graphics.path)
data.path <- paste0(FILEPATH)
table.path<-paste0(FILEPATH)

#================================SPECIFY INPUT PATHS======================
# Years to model:
years <- 1980:2025 

# Read in training and test sets:

train_data_Any <- read.csv(paste(data.path, 'train_data_Any.csv', sep = ''))
train_data_HMISfrac <- read.csv(paste(data.path, 'train_data_HMISfrac.csv', sep = ''))



train_data_Any$IHME_Region_Name<-as.factor(train_data_Any$IHME_Region_Name) #added conversion to factors


summary(train_data_Any$IHME_Region_Name)

# Models to be tested:

model_no <- "PTTIR_" # 1. RTTIR: IHME region temporal trend and factors; 2. PTTIR: Pruned regions temporal trend and IHME regional factors; 3. PTTPF: Pruned regions temporal trend and factors; 4. OC: Other considerations.

#=================================REVIEW REGION-SPECIFIC TEMPORAL TRENDS IDENTIFIED IN RTTIR================

# For PTTIR onwards (regional temporal trends) - This has been built based on the significance from model summaries and smooths created at the end of RTTIR step: (plots in 2.RTTIR)

sig.temp.trends.any<-c("Caribbean","Central Asia","Western Sub-Saharan Africa","Eastern Sub-Saharan Africa","Southeast Asia")

sig.temp.trends.HMIS<- c("Central Asia","North Africa and Middle East","Southern Sub-Saharan Africa","Western Sub-Saharan Africa") 

train_data_Any$Time_Factor <- as.character(train_data_Any$IHME_Region_Name)
train_data_Any$Time_Factor[!(train_data_Any$IHME_Region_Name %in% sig.temp.trends.any)] <- 'Other' #not a specific trend (where weird regional trends would go)
train_data_Any$Time_Factor <- as.factor(train_data_Any$Time_Factor)
unique(train_data_Any$Time_Factor)
train_data_Any$Time_Factor <- relevel(train_data_Any$Time_Factor, ref = 'Other')

train_data_HMISfrac$Time_Factor_2 <- as.character(train_data_HMISfrac$IHME_Region_Name)
train_data_HMISfrac$Time_Factor_2[!(train_data_HMISfrac$IHME_Region_Name %in% sig.temp.trends.HMIS)] <- 'Other'

train_data_HMISfrac$Time_Factor_2 <- as.factor(train_data_HMISfrac$Time_Factor_2)
unique(train_data_HMISfrac$Time_Factor_2)
train_data_HMISfrac$Time_Factor_2 <- relevel(train_data_HMISfrac$Time_Factor_2, ref = 'Other')

train_data_Any$Reg_Factor<-as.character(train_data_Any$IHME_Region_Name)
train_data_Any$Reg_Factor<-as.factor(train_data_Any$Reg_Factor)


########## ---------------------- 1. CHOOSE SUBSETS OF COVARIATES TO CONSIDER IN MODEL WHICH HAVE LOW COLLINEARITY -------------------- ############

## Correlation between covariates:

# Get columns with covariates. 
cov_col_vector<-names(train_data_Any)[(which(names(train_data_Any)=="Year")+1):(which(names(train_data_Any)=="SurveyType")-1)]
cov_col <- train_data_Any[, cov_col_vector]# Fit univariate GAMs on the covariates and compare AIC, then select covariate to include in final model depending on their relative AIC and correlation with each other:
#Specify the covariate column numbers
first_covar<-cov_col_vector[1]
col.start<-which(colnames(train_data_Any)==first_covar)
col.end<-col.start+length(cov_col_vector)-1 

cov_ind <- c(col.start:col.end)
AIC_Any <- rep(NA, length(cov_ind))
names(AIC_Any) <- names(train_data_Any)[cov_ind] # Check names

AIC_HMIS <- rep(NA, length(cov_ind))
names(AIC_HMIS) <- names(train_data_HMISfrac)[cov_ind]

for (i in 1:length(cov_ind)){
  var_values_Any <- train_data_Any[, cov_ind[i]]
  gam_Any <- gam(logit_Any ~ -1 + IHME_Region_Name + s(Year, by = Time_Factor) + s(X), data = data.frame("logit_Any" = train_data_Any$logit_Any, "X" = var_values_Any, 'Year' = train_data_Any$Year, 'IHME_Region_Name' = train_data_Any$IHME_Region_Name, 'Time_Factor' = train_data_Any$Time_Factor))
  var_values_HMIS <- train_data_HMISfrac[, cov_ind[i]]
  gam_HMIS <- gam(logit_HMIS ~ -1 + IHME_Region_Name + s(Year, by = Time_Factor_2) + s(X), data =  data.frame("logit_HMIS" = train_data_HMISfrac$logit_HMIS, "X" = var_values_HMIS, 'Year' = train_data_HMISfrac$Year, 'IHME_Region_Name' = train_data_HMISfrac$IHME_Region_Name, 'Time_Factor_2' = train_data_HMISfrac$Time_Factor_2))
  AIC_Any[i] <- gam_Any$aic
  AIC_HMIS[i] <- gam_HMIS$aic
}

AIC_Any_order <- names(sort(AIC_Any[!(names(AIC_Any) %in% c("measles_vacc_cov_prop_2", "oop_hexp_cap", "ind_health"))]))# Don't use frac_oop_hexp, oop_hexp_cap for any TS. measles_vacc_cov_prop_2 has extreme outliers. accessibility (travel time) etc. has strange relations.)]))

test_cov_mat <- cor(cov_col[, AIC_Any_order])
abs_mat <- abs(test_cov_mat) >0.6 # For checking which covariates are strongly correlated to those of increasing AIC.

test_cov_Any <- AIC_Any_order[1]

for (i in AIC_Any_order[-1]){
  # If candidate covariate has < 0.6 correlation with existing covariates.
  if (sum(abs_mat[i, test_cov_Any])< 1){
    test_cov_Any <- c(test_cov_Any, i)
  }
}

AIC_HMIS_order <- names(sort(AIC_HMIS[!(names(AIC_HMIS) %in% c("measles_vacc_cov_prop_2", "education_all_ages_and_sexes_pc", "ind_health"))]))
test_cov_mat <- cor(cov_col[, AIC_HMIS_order])
abs_mat <- abs(test_cov_mat) >0.6 # For checking which covariates are strongly correlated to those of increasing AIC.

test_cov_HMIS <- AIC_HMIS_order[1]

for (i in AIC_HMIS_order[-1]){
  # If candidate covariate has < 0.6 correlation with existing covariates.
  if (sum(abs_mat[i, test_cov_HMIS])< 1){
    test_cov_HMIS <- c(test_cov_HMIS, i)
  }
}

test_cov_Any
test_cov_HMIS


########## ---------------------- 2. FIT THE PRELIMINARY MODELS ON TS MEANS OF TRAINING DATA -------------------- ############

#  Use covariates identified in test_cov_Any:

formula_any <-  logit_Any ~ -1 + Reg_Factor + s(Year, by = Time_Factor, k = 5)+s(ANC4_coverage_prop, k=3)+s(hospital_beds_per1000, k=3)+s(healthcare_accessibility, k=3)

test.model.any<-gam(formula_any, data = train_data_Any)

train_data_HMISfrac$IHME_Super_Region_Name<-as.factor(train_data_HMISfrac$IHME_Super_Region_Name)

formula_HMIS <- logit_HMIS ~ -1 + IHME_Region_Name + s(Year, by = Time_Factor_2, k = 5)+s(hospital_beds_per1000, k=3)+s(DTP3_coverage_prop, k=3)

test.model.hmis <- gam(formula_HMIS, data = train_data_HMISfrac)

summary(test.model.hmis) 


########## ---------------------- 3. EXAMINE THE FINAL MODELS -------------------- ############

# Examine components of chosen models:

model_any_1 <- gam(formula_any, data = train_data_Any)
sum_any_1 <- summary(model_any_1) # PTTIR: 49.9% PD 55.4%

sum_any_1

reg_coeff <- sum_any_1$p.coeff
col.id <- rep(1, length(reg_coeff))
# Put regional intercepts that are significant
# These plots check if remaining insignif are similar
sig.reg.Any<-names(reg_coeff)[names(reg_coeff) %in% c("Reg_FactorAndean Latin America","Reg_FactorCentral Asia","Reg_FactorCentral Sub-Saharan Africa","Reg_FactorEastern Sub-Saharan Africa","Reg_FactorNorth Africa and Middle East","Reg_FactorOceania","Reg_FactorSouth Asia","Reg_FactorSoutheast Asia","Reg_FactorSouthern Sub-Saharan Africa","Reg_FactorWestern Sub-Saharan Africa")]

col.id[(names(reg_coeff) %in% sig.reg.Any)] <- 2


pdf(paste(graphics.path, model_no, "Any_intercepts_1.pdf", sep = ""), width = 12, height = 4)
plotCI(1:length(reg_coeff), reg_coeff, ui = reg_coeff + qnorm(0.975)*sum_any_1$se[grep("Reg_Factor", names(sum_any_1$se))], li = reg_coeff - qnorm(0.975)*sum_any_1$se[grep("Reg_Factor", names(sum_any_1$se))], xlab = 'IHME Regions', ylab = 'Intercept estimates', xaxt = 'n', col = col.id)
abline(h = 0, lty = 2)
abline(h = mean(reg_coeff[!(names(reg_coeff) %in% sig.reg.Any)]), col = 2)
axis(side=1, at=1:length(reg_coeff), labels = c("ALA", 'Car.', 'C.Asia', 'CLA', 'CSA', 'ESA',"Oc.", 'NAME','S.Asia', 'SE.Asia', 'SSA','TLA', 'WSA'))
legend('topright', lty = 1, col = 2, legend = c('Mean of base regions'))
dev.off()

# To be changed for different models:

gamtabs(model_any_1, caption = "Summary of best any TS model with pruned region temporal trends and IHME region factors")

pdf(paste(graphics.path, model_no, "Any_model_smooths.pdf", sep = ""), width = 12, height = 8)
par(mfrow = c(2, 3))
for(i in 1:30){
  plot(model_any_1, select = i, ylim = c(-5, 5))
  abline(h=0, col="red", lty=2)
}

dev.off()

model_hmis_1 <- gam(formula_HMIS, data = train_data_HMISfrac)
sum_hmis_1 <- summary(model_hmis_1) # 51.3%. PD 53.1%

sum_hmis_1

gamtabs(model_hmis_1, caption = "Summary of best public fraction model with pruned region temporal trends and IHME region factors")

reg_coeff_2 <- sum_hmis_1$p.coeff

col.id_2 <- rep(1, length(reg_coeff_2))

sig.reg.HMIS<-names(reg_coeff_2)[!names(reg_coeff_2) %in% c("IHME_Region_NameNorth Africa and Middle East","IHME_Region_NameTropical Latin America","IHME_Region_NameSoutheast Asia")]
col.id_2[names(reg_coeff_2) %in% sig.reg.HMIS ] <- 2


pdf(paste(graphics.path, model_no, "HMIS_intercepts_1.pdf", sep = ""), width = 12, height = 4)
plotCI(1:length(reg_coeff_2), reg_coeff_2, ui = reg_coeff_2 + qnorm(0.975)*sum_hmis_1$se[grep("IHME_Region_Name", names(sum_hmis_1$se))], li = reg_coeff_2 - qnorm(0.975)*sum_hmis_1$se[grep("IHME_Region_Name", names(sum_hmis_1$se))], xlab = 'IHME Regions', ylab = 'Intercept estimates', xaxt = 'n', col = col.id_2)
abline(h = 0, lty = 2)
abline(h = mean(reg_coeff_2[!(names(reg_coeff_2) %in%  sig.reg.HMIS)]), col = 2)
axis(side=1, at=1:length(reg_coeff_2), labels = c("ALA", 'Car.', 'C.Asia', 'CLA', 'CSA', 'ESA', 'NAME','Oc.', 'S.Asia', 'SE.Asia', 'SSA','TLA', 'WSA'))
legend('topright', lty = 1, col = 2, legend = c('Mean of base regions'))
dev.off()

pdf(paste(graphics.path, model_no, "HMIS_model1_smooths.pdf", sep = ""), width = 12, height = 8)
par(mfrow = c(2, 3))
for(i in 1:30){
  plot(model_hmis_1, select = i, ylim = c(-5, 5))
  abline(h=0, col="red", lty=2)
}

dev.off()


# 5-Modele_Selection_PTTFR.R


rm(list = ls())
#===========================================SET OUTPUT DIRECTORIES==================================
output.path<-paste0('FILEPATH')
graphics.path <- paste0(FILEPATH)
dir.create(graphics.path)
data.path <- paste0(FILEPATH)
table.path<-paste0(FILEPATH)

# Years to model:
years <- 1980:2025 

train_data_Any <- read.csv(paste(data.path, 'train_data_Any.csv', sep = ''))
train_data_HMISfrac <- read.csv(paste(data.path, 'train_data_HMISfrac.csv', sep = ''))


train_data_Any$IHME_Region_Name<-as.factor(train_data_Any$IHME_Region_Name) #added conversion to factors

train_data_HMISfrac$IHME_Region_Name<-as.factor(train_data_HMISfrac$IHME_Region_Name) #added conversion to factors

summary(train_data_Any$IHME_Region_Name)

# Models to be tested:

model_no <- "PTTFR_" # 1. RTTIR: IHME region temporal trend and factors; 2. PTTIR: Pruned regions temporal trend and IHME regional factors; 3. PTTPF: Pruned regions temporal trend and factors; 4. OC: Other considerations.

# From RTTIR step (regional temporal trends):

sig.temp.trends.any<-c("Caribbean","Central Asia","Western Sub-Saharan Africa","Eastern Sub-Saharan Africa","Southeast Asia")
sig.temp.trends.HMIS<- c("Central Asia","North Africa and Middle East","Southern Sub-Saharan Africa","Western Sub-Saharan Africa") 


train_data_Any$Time_Factor <- as.character(train_data_Any$IHME_Region_Name)
train_data_Any$Time_Factor[!(train_data_Any$IHME_Region_Name %in% sig.temp.trends.any)] <- 'Other' #not a specific trend (where weird regional trends would go)

train_data_Any$Time_Factor <- as.factor(train_data_Any$Time_Factor)
unique(train_data_Any$Time_Factor)
train_data_Any$Time_Factor <- relevel(train_data_Any$Time_Factor, ref = 'Other')


train_data_HMISfrac$Time_Factor_2 <- as.character(train_data_HMISfrac$IHME_Region_Name)
train_data_HMISfrac$Time_Factor_2[!(train_data_HMISfrac$IHME_Region_Name %in% sig.temp.trends.HMIS)] <- 'Other'

train_data_HMISfrac$Time_Factor_2 <- as.factor(train_data_HMISfrac$Time_Factor_2)
unique(train_data_HMISfrac$Time_Factor_2)
train_data_HMISfrac$Time_Factor_2 <- relevel(train_data_HMISfrac$Time_Factor_2, ref = 'Other')

#From PTTIR step - relative values of intercepts (coarser regional factors):
sig.reg.Any<-unique(train_data_Any$IHME_Region_Name)[unique(train_data_Any$IHME_Region_Name) %in% c("Andean Latin America","Central Asia","Central Sub-Saharan Africa","Eastern Sub-Saharan Africa","North Africa and Middle East","Oceania","South Asia","Southeast Asia","Southern Sub-Saharan Africa","Western Sub-Saharan Africa")]
sig.reg.HMIS<-unique(train_data_HMISfrac$IHME_Region_Name)[!unique(train_data_HMISfrac$IHME_Region_Name) %in% c("Southeast Asia","North Africa and Middle East","Tropical Latin America")]


train_data_Any$Reg_Factor <- as.character(train_data_Any$IHME_Region_Name)
train_data_Any$Reg_Factor[!(train_data_Any$IHME_Region_Name %in% sig.reg.Any)] <- 'Other'
train_data_Any$Reg_Factor <- as.factor(train_data_Any$Reg_Factor)
unique(train_data_Any$Reg_Factor)
train_data_Any$Reg_Factor <- relevel(train_data_Any$Reg_Factor, ref = 'Other')


train_data_HMISfrac$Reg_Factor_2 <- as.character(train_data_HMISfrac$IHME_Region_Name)
train_data_HMISfrac$Reg_Factor_2[!(train_data_HMISfrac$IHME_Region_Name %in% sig.reg.HMIS)] <- 'Other'
train_data_HMISfrac$Reg_Factor_2 <- as.factor(train_data_HMISfrac$Reg_Factor_2)
unique(train_data_HMISfrac$Reg_Factor_2)
train_data_HMISfrac$Reg_Factor_2 <- relevel(train_data_HMISfrac$Reg_Factor_2, ref = 'Other')


########## ---------------------- 1. CHOOSE SUBSETS OF COVARIATES TO CONSIDER IN MODEL WHICH HAVE LOW COLLINEARITY -------------------- ############

## Correlation between covariates:

# Get columns with covariates. 
cov_col_vector<-names(train_data_Any)[(which(names(train_data_Any)=="Year")+1):(which(names(train_data_Any)=="SurveyType")-1)]
cov_col <- train_data_Any[, cov_col_vector]# Fit univariate GAMs on the covariates and compare AIC, then select covariate to include in final model depending on their relative AIC and correlation with each other:
#Specify the covariate column numbers
first_covar<-cov_col_vector[1]
col.start<-which(colnames(train_data_Any)==first_covar)
col.end<-col.start+length(cov_col_vector)-1 # Fit univariate GAMs on the covariates and compare AIC, then select covariate to include in final model depending on their relative AIC and correlation with each other:

cov_ind <- c(col.start:col.end) # check covariate names.
AIC_Any <- rep(NA, length(cov_ind))
names(AIC_Any) <- names(train_data_HMISfrac)[cov_ind]

AIC_HMIS <- rep(NA, length(cov_ind))
names(AIC_HMIS) <- names(train_data_HMISfrac)[cov_ind]

for (i in 1:length(cov_ind)){
  var_values_Any <- train_data_Any[, cov_ind[i]]
  gam_Any <- gam(logit_Any ~ -1 + Reg_Factor + s(Year, by = Time_Factor) + s(X), data = data.frame("logit_Any" = train_data_Any$logit_Any, "X" = var_values_Any, 'Year' = train_data_Any$Year, 'Reg_Factor' = train_data_Any$Reg_Factor, 'Time_Factor' = train_data_Any$Time_Factor))
  var_values_HMIS <- train_data_HMISfrac[, cov_ind[i]]
  gam_HMIS <- gam(logit_HMIS ~ -1 + Reg_Factor_2 + s(Year, by = Time_Factor_2) + s(X), data =  data.frame("logit_HMIS" = train_data_HMISfrac$logit_HMIS, "X" = var_values_HMIS, 'Year' = train_data_HMISfrac$Year, 'Reg_Factor_2' = train_data_HMISfrac$Reg_Factor_2, 'Time_Factor_2' = train_data_HMISfrac$Time_Factor_2))
  AIC_Any[i] <- gam_Any$aic
  AIC_HMIS[i] <- gam_HMIS$aic
}

AIC_Any_order <- names(sort(AIC_Any[!(names(AIC_Any) %in% c("measles_vacc_cov_prop_2", "oop_hexp_cap", "ind_health"))]))# Don't use frac_oop_hexp, oop_hexp_cap for any TS. measles_vacc_cov_prop_2 has extreme outliers. accessibility (travel time) etc. has strange relations.)]))

test_cov_mat <- cor(cov_col[, AIC_Any_order])
abs_mat <- abs(test_cov_mat) >0.6 # For checking which covariates are strongly correlated to those of increasing AIC.

test_cov_Any <- AIC_Any_order[1]

for (i in AIC_Any_order[-1]){
  # If candidate covariate has < 0.6 correlation with existing covariates.
  if (sum(abs_mat[i, test_cov_Any])< 1){
    test_cov_Any <- c(test_cov_Any, i)
  }
}

AIC_HMIS_order <- names(sort(AIC_HMIS[!(names(AIC_HMIS) %in% c("measles_vacc_cov_prop_2", "education_all_ages_and_sexes_pc", "ind_health"))]))

test_cov_mat <- cor(cov_col[, AIC_HMIS_order])
abs_mat <- abs(test_cov_mat) >0.6 # For checking which covariates are strongly correlated to those of increasing AIC.

test_cov_HMIS <- AIC_HMIS_order[1]

for (i in AIC_HMIS_order[-1]){
  # If candidate covariate has < 0.6 correlation with existing covariates.
  if (sum(abs_mat[i, test_cov_HMIS])< 1){
    test_cov_HMIS <- c(test_cov_HMIS, i)
  }
}

test_cov_Any
test_cov_HMIS

########## ---------------------- 2. EXAMINE THE FINAL MODELS -------------------- ############

# Examine components of chosen models:

formula_any <-  logit_Any ~ -1 + Reg_Factor + s(Year, by = Time_Factor, k = 5) + s(ANC4_coverage_prop, k = 3)+s(hospital_beds_per1000, k=3)+s(healthcare_accessibility, k=3)


model_any_1 <- gam(formula_any, data = train_data_Any, random=list(IHME_location_id = ~ 1))
summary(model_any_1) 

gamtabs(model_any_1, caption = "Summary of best any TS model with pruned region temporal trends and region factors")

pdf(paste(graphics.path, model_no, "Any_model_smooths_1.pdf", sep = ""), width = 12, height = 8)
par(mfrow = c(2, 3))
for(i in 1:30){
  plot(model_any_1, select = i, ylim = c(-5, 5))
  abline(h=0, col="red", lty=2)
}

dev.off()


formula_HMIS <- logit_HMIS ~ -1 + Reg_Factor_2 + s(Year, by = Time_Factor_2, k = 5)+s(DTP3_coverage_prop, k=3)+s(hospital_beds_per1000, k=3)+s(ANC1_coverage_prop, k=3)


model_hmis_1 <- gam(formula_HMIS, data = train_data_HMISfrac, random=list(IHME_location_id = ~ 1))
summary(model_hmis_1) 

gamtabs(model_hmis_1, caption = "Summary of best public fraction model with pruned region temporal trends and region factors")


pdf(paste(graphics.path, model_no, "HMIS_model_smooths.pdf", sep = ""), width = 12, height = 8)
par(mfrow = c(2, 3))
for(i in 1:30){
  plot(model_hmis_1, select = i, ylim = c(-5, 5))
  abline(h=0, col="red", lty=2)
}

dev.off()

# 6-Model_Selection_PREF.R

rm(list = ls())
#===========================================SET OUTPUT DIRECTORIES==================================
output.path<-paste0('FILEPATH')
graphics.path <- paste0(FILEPATH)
dir.create(graphics.path)
data.path <- paste0(FILEPATH)
table.path<-paste0(FILEPATH)

# Years to model:
years <- 1980:2025 

# Read in full datasets, training and test sets:

train_data_Any <- read.csv(paste(data.path, 'train_data_Any.csv', sep = ''))
train_data_HMISfrac <- read.csv(paste(data.path, 'train_data_HMISfrac.csv', sep = ''))


train_data_Any$IHME_Region_Name<-as.factor(train_data_Any$IHME_Region_Name) #added conversion to factors


summary(train_data_Any$IHME_Region_Name)

# Models to be tested:

model_no <- "PRF_" # 1. RF (random effect for region and units)

# For PTTIR onwards (regional temporal trends):

sig.temp.trends.any<-c("Caribbean","Central Asia","Western Sub-Saharan Africa","Southeast Asia")
sig.temp.trends.HMIS<- c("Central Asia","North Africa and Middle East","Southern Sub-Saharan Africa","Western Sub-Saharan Africa") 

train_data_Any$Time_Factor <- as.character(train_data_Any$IHME_Region_Name)
train_data_Any$Time_Factor[!(train_data_Any$IHME_Region_Name %in% sig.temp.trends.any)] <- 'Other'

train_data_Any$Time_Factor <- as.factor(train_data_Any$Time_Factor)
unique(train_data_Any$Time_Factor)
train_data_Any$Time_Factor <- relevel(train_data_Any$Time_Factor, ref = 'Other')


train_data_HMISfrac$Time_Factor_2 <- as.character(train_data_HMISfrac$IHME_Region_Name)
train_data_HMISfrac$Time_Factor_2[!(train_data_HMISfrac$IHME_Region_Name %in% sig.temp.trends.HMIS)] <- 'Other'
train_data_HMISfrac$Time_Factor_2 <- as.factor(train_data_HMISfrac$Time_Factor_2)
unique(train_data_HMISfrac$Time_Factor_2)
train_data_HMISfrac$Time_Factor_2 <- relevel(train_data_HMISfrac$Time_Factor_2, ref = 'Other')



# For PTTFR onwards (coarser regional factors):

sig.reg.Any<-unique(train_data_Any$IHME_Region_Name)[unique(train_data_Any$IHME_Region_Name) %in% c("Andean Latin America","Central Asia","Central Sub-Saharan Africa","Eastern Sub-Saharan Africa","Oceania","South Asia","Southeast Asia")]
sig.reg.HMIS<-unique(train_data_HMISfrac$IHME_Region_Name)[!unique(train_data_HMISfrac$IHME_Region_Name) %in% c("Central Latin America","North Africa and Middle East","Tropical Latin America")]



train_data_Any$Reg_Factor <- as.character(train_data_Any$IHME_Region_Name)
train_data_Any$Reg_Factor[!(train_data_Any$IHME_Region_Name %in% sig.reg.Any)] <- 'Other'

train_data_Any$Reg_Factor <- as.factor(train_data_Any$Reg_Factor)
unique(train_data_Any$Reg_Factor)
train_data_Any$Reg_Factor <- relevel(train_data_Any$Reg_Factor, ref = 'Other')

train_data_HMISfrac$Reg_Factor_2 <- as.character(train_data_HMISfrac$IHME_Region_Name)
train_data_HMISfrac$Reg_Factor_2[!(train_data_HMISfrac$IHME_Region_Name %in% sig.reg.HMIS)] <- 'Other'
train_data_HMISfrac$Reg_Factor_2 <- as.factor(train_data_HMISfrac$Reg_Factor_2)
unique(train_data_HMISfrac$Reg_Factor_2)
train_data_HMISfrac$Reg_Factor_2 <- relevel(train_data_HMISfrac$Reg_Factor_2, ref = 'Other')


# NA: Use location id as factor instead of admin unit name so as to account for 2 Punjabs and 2 Mexicos (eg Mexico city, Mexico):
train_data_Any$IHME_location_id <- as.factor(train_data_Any$IHME_location_id)
train_data_HMISfrac$IHME_location_id <- as.factor(train_data_HMISfrac$IHME_location_id)


########## ---------------------- 1. CHOOSE SUBSETS OF COVARIATES TO CONSIDER IN MODEL WHICH HAVE LOW COLLINEARITY -------------------- ############

# Get columns with covariates. 
cov_col_vector<-names(train_data_Any)[(which(names(train_data_Any)=="Year")+1):(which(names(train_data_Any)=="SurveyType")-1)]
cov_col <- train_data_Any[, cov_col_vector]# Fit univariate GAMs on the covariates and compare AIC, then select covariate to include in final model depending on their relative AIC and correlation with each other:
#Specify the covariate column numbers
first_covar<-cov_col_vector[1]
col.start<-which(colnames(train_data_Any)==first_covar)
col.end<-col.start+length(cov_col_vector)-1 # Fit univariate GAMs on the covariates and compare AIC, then select covariate to include in final model depending on their relative AIC and correlation with each other:

cov_ind <- c(col.start:col.end) # check covariate names.
AIC_Any <- rep(NA, length(cov_ind))
names(AIC_Any) <- names(train_data_HMISfrac)[cov_ind]

AIC_HMIS <- rep(NA, length(cov_ind))
names(AIC_HMIS) <- names(train_data_HMISfrac)[cov_ind]

for (i in 1:length(cov_ind)){
  var_values_Any <- train_data_Any[, cov_ind[i]]
  gam_Any <- gamm(logit_Any ~ -1 + Reg_Factor + s(Year, by = Time_Factor) + s(X), data = data.frame("logit_Any" = train_data_Any$logit_Any, "X" = var_values_Any, 'Year' = train_data_Any$Year, 'Time_Factor' = train_data_Any$Time_Factor, 'Reg_Factor' = train_data_Any$Reg_Factor, 'IHME_location_id' = train_data_Any$IHME_location_id), random = list(IHME_location_id = ~ 1))
  AIC_Any[i] <- AIC(gam_Any$lme)

  var_values_HMIS <- train_data_HMISfrac[, cov_ind[i]]
  gam_HMIS <- gamm(logit_HMIS ~ -1 + Reg_Factor_2 + s(Year, by = Time_Factor_2) + s(X), data =  data.frame("logit_HMIS" = train_data_HMISfrac$logit_HMIS, "X" = var_values_HMIS, 'Year' = train_data_HMISfrac$Year, 'Time_Factor_2' = train_data_HMISfrac$Time_Factor_2, 'Reg_Factor_2' = train_data_HMISfrac$Reg_Factor_2, 'IHME_location_id' = train_data_HMISfrac$IHME_location_id), random = list(IHME_location_id = ~ 1))
  AIC_HMIS[i] <- AIC(gam_HMIS$lme)

}
AIC_Any_order <- names(sort(AIC_Any[!(names(AIC_Any) %in% c("measles_vacc_cov_prop_2", "oop_hexp_cap", "education_all_ages_and_sexes_pc", "ind_health","GDPpc_id_b2010"))]))

test_cov_mat <- cor(cov_col[, AIC_Any_order])
abs_mat <- abs(test_cov_mat) >0.6 # For checking which covariates are strongly correlated to those of increasing AIC.

test_cov_Any <- AIC_Any_order[1]

for (i in AIC_Any_order[-1]){
  # If candidate covariate has < 0.6 correlation with existing covariates.
  if (sum(abs_mat[i, test_cov_Any])< 1){
    test_cov_Any <- c(test_cov_Any, i)
  }
}

AIC_HMIS_order <- names(sort(AIC_HMIS[!(names(AIC_HMIS) %in% c("measles_vacc_cov_prop_2", "education_all_ages_and_sexes_pc", "ind_health"))])) #these have strange relations

test_cov_mat <- cor(cov_col[, AIC_HMIS_order])
abs_mat <- abs(test_cov_mat) >0.6 # For checking which covariates are strongly correlated to those of increasing AIC.

test_cov_HMIS <- AIC_HMIS_order[1]

for (i in AIC_HMIS_order[-1]){
  # If candidate covariate has < 0.6 correlation with existing covariates.
  if (sum(abs_mat[i, test_cov_HMIS])< 1){
    test_cov_HMIS <- c(test_cov_HMIS, i)
  }
}

test_cov_Any
test_cov_HMIS

########## ---------------------- 2. FIT THE PRELIMINARY MODELS ON TS MEANS OF TRAINING DATA -------------------- ############

#  Use covariates identified in test_cov_Any:(check test_cov_Any) 

#Any
formula_any <-  logit_Any ~ -1 + Reg_Factor + s(Year, by = Time_Factor, k = 5)+s(ANC1_coverage_prop, k=3)+s(he_cap, k=3)+s(healthcare_accessibility, k=3)

test.model <- gamm(formula_any, data = train_data_Any, random = list(IHME_location_id = ~ 1), control = list(opt='optim'))

#HMIS

train_data_HMISfrac$IHME_Region_Name<-as.factor(train_data_HMISfrac$IHME_Region_Name)

formula_HMIS <- logit_HMIS ~ -1 +Reg_Factor_2 + s(Year, by = Time_Factor_2, k = 5)+s(IFD_coverage_prop, k=3)+s(accessibility, k=3)+s(he_cap, k=3)


test.model <- gamm(formula_HMIS, data = train_data_HMISfrac, random = list(IHME_location_id = ~ 1), control = list(opt='optim'))

summary(test.model$gam)

itsadug::gamtabs(summary(test.model$gam))

########## ---------------------- 3. EXAMINE THE FINAL MODELS -------------------- ############
#input the formula of the model chosen as best in best.any
#formula_any <-  logit_Any ~ -1 + Reg_Factor + s(Year, by = Time_Factor, k = 5) + s(ANC1_coverage_prop, k = 3) + s(DMSP_nighttime, k = 3) + s(GDPpc_id_b2010, k = 3) 

model_any_1 <- gamm(formula_any, data = train_data_Any, random = list(IHME_location_id = ~ 1), control = list(opt='optim'))
refRegion <- ranef(model_any_1$lme)

pdf(paste(graphics.path, model_no, "Any_model_smooths.pdf", sep = ""), width =8 , height = 12)
par(mfrow = c(4, 3))
for(i in 1:30){
  plot(model_any_1$gam, select = i, ylim = c(-5, 5))
  abline(h=0, col="red", lty=2)
}

dev.off()


jpeg(paste(graphics.path, model_no, "Any_model_smooths.jpeg", sep = ""), res=1200,width=10000, height=14000)
par(mfrow = c(4, 3))
for(i in 1:30){
  plot(model_any_1$gam, select = i, ylim = c(-5, 5))
  abline(h=0, col="red", lty=2)
}

dev.off()



model_hmis_1 <- gamm(formula_HMIS, data = train_data_HMISfrac, random = list(IHME_location_id = ~ 1), control = list(opt='optim'))
refRegion_HMIS <- ranef(model_hmis_1$lme)

pdf(paste(graphics.path, model_no, "Unit_QQ.pdf", sep = ""), width = 9, height = 4.5)
par(mfrow = c(1, 2))
qqnorm(refRegion$IHME_location_id[, ], main = 'Normal Q-Q plot (Any)')
qqline(refRegion$IHME_location_id[, ])
qqnorm(refRegion_HMIS$IHME_location_id[, ], main = 'Normal Q-Q plot (Pub. fraction)')
qqline(refRegion_HMIS$IHME_location_id[, ])
dev.off()

pdf(paste(graphics.path, model_no, "Any_model_REF_QQ.pdf", sep = ""), width = 4, height = 4)
qqnorm(refRegion$IHME_location_id[, ], main = 'Normal Q-Q plot (Any)')
qqline(refRegion$IHME_location_id[, ])
dev.off()

gamtabs(model_hmis_1$gam, caption = "Summary of best public fraction model with random effects for the units.")

pdf(paste(graphics.path, model_no, "HMIS_model_smooths.pdf", sep = ""), width = 12, height = 8)
par(mfrow = c(2, 3))
for(i in 1:30){
  plot(model_hmis_1$gam, select = i, ylim = c(-5, 5))
  abline(h=0, col="red", lty=2)
}

dev.off()



jpeg(paste(graphics.path, model_no, "HMIS_model_smooths.jpeg", sep = ""), res=1200,width=10000, height=14000)
par(mfrow = c(4, 3))
for(i in 1:30){
  plot(model_hmis_1$gam, select = i, ylim = c(-5, 5))
  abline(h=0, col="red", lty=2)
}

dev.off()

# 7-Model_validation.R 


rm(list = ls())
#===========================================SET OUTPUT DIRECTORIES==================================
output.path<-paste0('FILEPATH')

graphics.path <- paste0(FILEPATH)
dir.create(graphics.path)
data.path <- paste0(FILEPATH)
table.path<-paste0(FILEPATH)

# Years to model:
years <- 1980:2025 

# Read in training and test sets:

train_data_Any <- read.csv(paste(data.path, 'train_data_Any.csv', sep = ''))
train_data_HMISfrac <- read.csv(paste(data.path, 'train_data_HMISfrac.csv', sep = ''))
test_data_Any <- read.csv(paste(data.path, 'test_data_Any.csv', sep = ''))
test_data_HMISfrac <- read.csv(paste(data.path, 'test_data_HMISfrac.csv', sep = ''))

train_data_Any$IHME_Region_Name<-as.factor(train_data_Any$IHME_Region_Name) #added conversion to factors
test_data_Any$IHME_Region_Name<-as.factor(test_data_Any$IHME_Region_Name)#added conversion to factors



# Models to be tested:

model_no <- "Final_" 

# For PTTIR onwards (regional temporal trends):

sig.temp.trends.any<-c("Caribbean","Central Asia","Western Sub-Saharan Africa","Southeast Asia")
sig.temp.trends.HMIS<- c("Central Asia","North Africa and Middle East","Southern Sub-Saharan Africa","Western Sub-Saharan Africa") 

train_data_Any$Time_Factor <- as.character(train_data_Any$IHME_Region_Name)
train_data_Any$Time_Factor[!(train_data_Any$IHME_Region_Name %in% sig.temp.trends.any)] <- 'Other'

train_data_Any$Time_Factor <- as.factor(train_data_Any$Time_Factor)
unique(train_data_Any$Time_Factor)
train_data_Any$Time_Factor <- relevel(train_data_Any$Time_Factor, ref = 'Other')

test_data_Any$Time_Factor <- as.character(test_data_Any$IHME_Region_Name)
test_data_Any$Time_Factor[!(test_data_Any$IHME_Region_Name %in% sig.temp.trends.any)] <- 'Other'
test_data_Any$Time_Factor <- as.factor(test_data_Any$Time_Factor)
unique(test_data_Any$Time_Factor)
test_data_Any$Time_Factor <- relevel(test_data_Any$Time_Factor, ref = 'Other')

train_data_HMISfrac$Time_Factor_2 <- as.character(train_data_HMISfrac$IHME_Region_Name)
train_data_HMISfrac$Time_Factor_2[!(train_data_HMISfrac$IHME_Region_Name %in% sig.temp.trends.HMIS)] <- 'Other'
train_data_HMISfrac$Time_Factor_2 <- as.factor(train_data_HMISfrac$Time_Factor_2)
unique(train_data_HMISfrac$Time_Factor_2)
train_data_HMISfrac$Time_Factor_2 <- relevel(train_data_HMISfrac$Time_Factor_2, ref = 'Other')

test_data_HMISfrac$Time_Factor_2 <- as.character(test_data_HMISfrac$IHME_Region_Name)
test_data_HMISfrac$Time_Factor_2[!(test_data_HMISfrac$IHME_Region_Name %in% sig.temp.trends.HMIS)] <- 'Other'
test_data_HMISfrac$Time_Factor_2 <- as.factor(test_data_HMISfrac$Time_Factor_2)
unique(test_data_HMISfrac$Time_Factor_2)
test_data_HMISfrac$Time_Factor_2 <- relevel(test_data_HMISfrac$Time_Factor_2, ref = 'Other')

# For PTTFR onwards (coarser regional factors):
sig.reg.Any<-unique(train_data_Any$IHME_Region_Name)[unique(train_data_Any$IHME_Region_Name) %in% c("Andean Latin America","Central Asia","Central Sub-Saharan Africa","Eastern Sub-Saharan Africa","Oceania","South Asia","Southeast Asia")]
sig.reg.HMIS<-unique(train_data_HMISfrac$IHME_Region_Name)[!unique(train_data_HMISfrac$IHME_Region_Name) %in% c("Central Latin America","North Africa and Middle East","Tropical Latin America")]

train_data_Any$Reg_Factor <- as.character(train_data_Any$IHME_Region_Name)
train_data_Any$Reg_Factor[!(train_data_Any$IHME_Region_Name %in% sig.reg.Any)] <- 'Other'
train_data_Any$Reg_Factor <- as.factor(train_data_Any$Reg_Factor)
unique(train_data_Any$Reg_Factor)
train_data_Any$Reg_Factor <- relevel(train_data_Any$Reg_Factor, ref = 'Other')

test_data_Any$Reg_Factor <- as.character(test_data_Any$IHME_Region_Name)
test_data_Any$Reg_Factor[!(test_data_Any$IHME_Region_Name %in% sig.reg.Any)] <- 'Other'
test_data_Any$Reg_Factor <- as.factor(test_data_Any$Reg_Factor)
unique(test_data_Any$Reg_Factor)
test_data_Any$Reg_Factor <- relevel(test_data_Any$Reg_Factor, ref = 'Other')

train_data_HMISfrac$Reg_Factor_2 <- as.character(train_data_HMISfrac$IHME_Region_Name)
train_data_HMISfrac$Reg_Factor_2[!(train_data_HMISfrac$IHME_Region_Name %in% sig.reg.HMIS)] <- 'Other'
train_data_HMISfrac$Reg_Factor_2 <- as.factor(train_data_HMISfrac$Reg_Factor_2)
unique(train_data_HMISfrac$Reg_Factor_2)
train_data_HMISfrac$Reg_Factor_2 <- relevel(train_data_HMISfrac$Reg_Factor_2, ref = 'Other')

test_data_HMISfrac$Reg_Factor_2 <- as.character(test_data_HMISfrac$IHME_Region_Name)
test_data_HMISfrac$Reg_Factor_2[!(test_data_HMISfrac$IHME_Region_Name %in% sig.reg.HMIS)] <- 'Other'
test_data_HMISfrac$Reg_Factor_2 <- as.factor(test_data_HMISfrac$Reg_Factor_2)
unique(test_data_HMISfrac$Reg_Factor_2)
test_data_HMISfrac$Reg_Factor_2 <- relevel(test_data_HMISfrac$Reg_Factor_2, ref = 'Other')

# Use location id as factor instead of admin unit name so as to account for 2 Punjabs and 2 Mexicos:
train_data_Any$IHME_location_id <- as.factor(train_data_Any$IHME_location_id)
train_data_HMISfrac$IHME_location_id <- as.factor(train_data_HMISfrac$IHME_location_id)
test_data_Any$IHME_location_id <- as.factor(test_data_Any$IHME_location_id)
test_data_HMISfrac$IHME_location_id <- as.factor(test_data_HMISfrac$IHME_location_id)

#here's the final formulas from PREF

formula_any <-  logit_Any ~ -1 + Reg_Factor + s(Year, by = Time_Factor, k = 5)+s(ANC1_coverage_prop, k=3)+s(he_cap, k=3)+s(healthcare_accessibility, k=3)

formula_HMIS <- logit_HMIS ~ -1 +Reg_Factor_2 + s(Year, by = Time_Factor_2, k = 5)+s(IFD_coverage_prop, k=3)+s(accessibility, k=3)+s(he_cap, k=3)

# Use best models:
Any_model <-  gamm(formula_any, data = train_data_Any, random = list(IHME_location_id = ~ 1), control = list(opt='optim'))
HMIS_model <- gamm(formula_HMIS, data = train_data_HMISfrac, random = list(IHME_location_id = ~ 1), control = list(opt='optim'))

train.predict.any <- predict(Any_model)
train.predict.HMIS <- predict(HMIS_model)

master.region.list <- unique(train_data_Any$IHME_Region_Name)
master.unit.list <- unique(train_data_Any$IHME_location_id)

Any_rf <- rep(NA, nrow(train_data_Any))

for (i in 1:length(master.unit.list)){
  Any_rf[train_data_Any$IHME_location_id == as.character(master.unit.list[i])] <-ifelse(is.na(ranef(Any_model$lme,level= length(ranef(Any_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(Any_model$lme))-1))), master.unit.list[i], sep = ""), ]), 0, ranef(Any_model$lme,level= length(ranef(Any_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(Any_model$lme))-1))), master.unit.list[i], sep = ""), ])
} 


Any_rf[is.na(Any_rf)] <- 0

HMIS_rf <- rep(NA, nrow(train_data_HMISfrac))



master.region.list <- unique(train_data_HMISfrac$IHME_Region_Name)
master.unit.list <- unique(train_data_HMISfrac$IHME_location_id)

for (i in 1:length(master.unit.list)){
  HMIS_rf[train_data_HMISfrac$IHME_location_id == as.character(master.unit.list[i])] <- ifelse(is.na(ranef(HMIS_model$lme,level= length(ranef(HMIS_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(HMIS_model$lme))-1))), master.unit.list[i], sep = ""), ]), 0, ranef(HMIS_model$lme,level= length(ranef(HMIS_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(HMIS_model$lme))-1))), master.unit.list[i], sep = ""), ])
  
}

HMIS_rf[is.na(HMIS_rf)] <- 0

(sum(Any_rf == 0) + sum(HMIS_rf == 0))==0

train.predict.any <- predict(Any_model, newdata = train_data_Any) + Any_rf
train.predict.HMIS <- predict(HMIS_model, newdata = train_data_HMISfrac) + HMIS_rf

master.region.list <- unique(test_data_Any$IHME_Region_Name)
master.unit.list <- unique(test_data_Any$IHME_location_id)

Any_rf_2 <- rep(NA, nrow(test_data_Any))

for (i in 1:length(master.unit.list)){
  # for (i in 1:length(units.in.region)){
  # 1. REF:
  Any_rf_2[test_data_Any$IHME_location_id == as.character(master.unit.list[i])] <-ifelse(is.na(ranef(Any_model$lme,level= length(ranef(Any_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(Any_model$lme))-1))), master.unit.list[i], sep = ""), ]), 0, ranef(Any_model$lme,level= length(ranef(Any_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(Any_model$lme))-1))), master.unit.list[i], sep = ""), ])
}
# }

Any_rf_2[is.na(Any_rf_2)] <- 0

master.region.list <- unique(test_data_HMISfrac$IHME_Region_Name)
master.unit.list <- unique(test_data_HMISfrac$IHME_location_id)

HMIS_rf_2 <- rep(NA, nrow(test_data_HMISfrac))

for (i in 1:length(master.unit.list)){
  # for (i in 1:length(units.in.region)){
  # 1. REF:
  # HMIS_rf_2[test_data_HMISfrac$IHME_location_id == as.character(units.in.region[i])] <- ranef(HMIS_model$lme,level= 14)[paste("1/1/1/1/1/1/1/1/1/1/1/1/1/", master.region.list[j], sep = ""), ] + ifelse(is.na(ranef(HMIS_model$lme,level= 15)[paste("1/1/1/1/1/1/1/1/1/1/1/1/1/", master.region.list[j], "/", units.in.region[i], sep = ""), ]), 0, ranef(HMIS_model$lme,level= 15)[paste("1/1/1/1/1/1/1/1/1/1/1/1/1/", master.region.list[j], "/", units.in.region[i], sep = ""), ])
  # 2. REF:
  HMIS_rf_2[test_data_HMISfrac$IHME_location_id == as.character(master.unit.list[i])] <- ifelse(is.na(ranef(HMIS_model$lme,level= length(ranef(HMIS_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(HMIS_model$lme))-1))), master.unit.list[i], sep = ""), ]), 0, ranef(HMIS_model$lme,level= length(ranef(HMIS_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(HMIS_model$lme))-1))), master.unit.list[i], sep = ""), ])
}
# }

HMIS_rf_2[is.na(HMIS_rf_2)] <- 0

(sum(Any_rf_2 == 0) + sum(HMIS_rf_2 == 0))==0

test.predict.any <- predict(Any_model, newdata = test_data_Any) + Any_rf_2
test.predict.HMIS <- predict(HMIS_model, newdata = test_data_HMISfrac) + HMIS_rf_2


# Model validation:

pdf(paste(graphics.path, "Model_validation_plots.pdf", sep = ""), width = 9, height = 9)
par(mfrow = c(2, 2), mai = rep(0.4, 4))
plot(train.predict.any, train.predict.any - train_data_Any$logit_Any, main = "Residuals versus fitted values (logit any)", ylim = c(-2.5, 2.5))
abline(h = 0)
plot(train.predict.HMIS, train.predict.HMIS - train_data_HMISfrac$logit_HMIS, main = "Residuals versus fitted values (logit public fraction)", ylim = c(-2.5, 2.5))
abline(h = 0)
hist(train.predict.any - train_data_Any$logit_Any, main = 'Histogram of residuals (logit any)')
hist(train.predict.HMIS - train_data_HMISfrac$logit_HMIS, main = 'Histogram of residuals (logit public fraction)')
dev.off()

pdf(paste(graphics.path, "Fitted_vs_observed.pdf", sep = ""), width = 9, height = 4.5)
par(mfrow = c(1, 2), mai = rep(0.4, 4))
plot(train_data_Any$logit_Any, train.predict.any, main = "Fitted versus observed values (logit any)", ylim = c(min(train_data_Any$logit_Any), max(train_data_Any$logit_Any)), xlim = c(min(train_data_Any$logit_Any), max(train_data_Any$logit_Any)))
lines(train_data_Any$logit_Any, train_data_Any$logit_Any)
plot(train_data_HMISfrac$logit_HMIS, train.predict.HMIS, main = "Fitted versus observed values (logit public fraction)", ylim = c(min(train_data_Any$logit_Any), max(train_data_Any$logit_Any)), xlim = c(min(train_data_HMISfrac$logit_HMIS), max(train_data_HMISfrac$logit_HMIS)))
lines(train_data_HMISfrac$logit_HMIS, train_data_HMISfrac$logit_HMIS)
dev.off()

pdf(paste(graphics.path, "Fitted_vs_observed_rawscale.pdf", sep = ""), width = 9, height = 4.5)
par(mfrow = c(1, 2), mai = rep(0.4, 4))
plot(train_data_Any$Any_treat, inv.logit(train.predict.any), main = "Fitted versus observed values (any)", ylim = c(0, 1), xlim = c(0, 1))
lines(c(0, 1), c(0, 1))
plot(train_data_HMISfrac$HMIS_frac, inv.logit(train.predict.HMIS), main = "Fitted versus observed values (public fraction)", ylim = c(0, 1), xlim = c(0, 1))
lines(c(0, 1), c(0, 1))
dev.off()

# Compare training and test RMSE - amount of overfitting.

train.error.any <- sqrt(mean((train.predict.any - train_data_Any$logit_Any)^2)) 
train.error.hmis <- sqrt(mean((train.predict.HMIS - train_data_HMISfrac$logit_HMIS)^2)) 

test.error.any <- sqrt(mean((test.predict.any - test_data_Any$logit_Any)^2)) 
test.error.hmis <- sqrt(mean((test.predict.HMIS - test_data_HMISfrac$logit_HMIS)^2))

train.error.any.raw <- sqrt(mean((train_data_Any$Any_treat - inv.logit(train.predict.any))^2)) 
train.error.hmis.raw <- sqrt(mean((train_data_HMISfrac$HMIS_frac - inv.logit(train.predict.HMIS))^2)) 

test.error.any.raw <- sqrt(mean((test_data_Any$Any_treat - inv.logit(test.predict.any))^2)) 
test.error.hmis.raw <- sqrt(mean((test_data_HMISfrac$HMIS_frac - inv.logit(test.predict.HMIS))^2)) 

# Training and test error are comparable.


# 8-Prediction_experiment.R


rm(list = ls())
set.seed(8)
#===========================================SET OUTPUT DIRECTORIES==================================
output.path<-paste0('FILEPATH')

graphics.path <- paste0(FILEPATH)
dir.create(graphics.path)
data.path <- paste0(FILEPATH)
table.path<-paste0(FILEPATH)
source("FILEPATH/multiplot.R")
#============================================LOAD INPUT DATA========================================
# Years to model:
years <- 1980:2025 
# IHME populations:

# Read in IHME covariates:
load(paste0(output.path,"ts_covariates_clean.RData")) #ts_covars cleaned in Data_Preparation step
# Read in full datasets, training and test sets:
full_TreatSeek <- read.csv(paste(data.path, "full_TreatSeek.csv", sep = ""))
full_TreatSeek_n <- read.csv(paste(data.path, "full_TreatSeek_n.csv", sep = ""))

clean_TreatSeek_Any <- read.csv(paste(data.path, "clean_TreatSeek_Any.csv", sep = ""))
clean_TreatSeek_HMISfrac <- read.csv(paste(data.path, "clean_TreatSeek_HMISfrac.csv", sep = ""))
clean_TreatSeek_Any$Admin_Unit_Name <- as.character(clean_TreatSeek_Any$Admin_Unit_Name)
clean_TreatSeek_HMISfrac$Admin_Unit_Name <- as.character(clean_TreatSeek_HMISfrac$Admin_Unit_Name)

full_TreatSeek_n$IHME_Region_Name[full_TreatSeek_n$ISO3=="BHR"]<-"North Africa and Middle East"

#=====================================TRANSFORM DATA================================================
# Use location id as factor for regional trends instead of admin unit name so as to account for 2 Punjabs and 2 Mexicos:
clean_TreatSeek_Any$IHME_location_id <- as.factor(clean_TreatSeek_Any$IHME_location_id)
clean_TreatSeek_HMISfrac$IHME_location_id <- as.factor(clean_TreatSeek_HMISfrac$IHME_location_id)
full_TreatSeek_n$IHME_location_id <- as.factor(full_TreatSeek_n$IHME_location_id)
full_TreatSeek$IHME_location_id <- as.factor(full_TreatSeek$IHME_location_id)

#=====================================FIND ADMIN UNITS WITHOUT DHS SURVEY DATA====================================
master.region.list <- unique(full_TreatSeek_n$IHME_Region_Name) # This has grouped Oceania with SE Asia etc. Use full_TreatSeek if want true regions.
master.unit.list <- unique(full_TreatSeek_n$IHME_location_id)
# Use superregion for matching units without data with those with data based on covariates:
full_TreatSeek_n$IHME_Super_Region_Name[full_TreatSeek_n$IHME_Super_Region_Name=="Southeast Asia_ East Asia_ and Oceania"]<-"Southeast Asia, East Asia, and Oceania"
full_TreatSeek_n$IHME_Super_Region_Name[full_TreatSeek_n$Admin_Unit_Name %in% c("South Korea")] <- "Southeast Asia, East Asia, and Oceania"
full_TreatSeek_n$IHME_Super_Region_Name[full_TreatSeek_n$Admin_Unit_Name %in% c("Argentina", "French Guiana")] <- "Latin America and Caribbean"
full_TreatSeek_n$IHME_Super_Region_Name[full_TreatSeek_n$Admin_Unit_Name %in% c("Mayotte")] <- "Sub-Saharan Africa"

clean_TreatSeek_Any$IHME_Super_Region_Name[clean_TreatSeek_Any$IHME_Super_Region_Name=="Southeast Asia_ East Asia_ and Oceania"]<-"Southeast Asia, East Asia, and Oceania"
clean_TreatSeek_Any$IHME_Super_Region_Name[clean_TreatSeek_Any$Admin_Unit_Name %in% c("South Korea")] <- "Southeast Asia, East Asia, and Oceania"
clean_TreatSeek_Any$IHME_Super_Region_Name[clean_TreatSeek_Any$Admin_Unit_Name %in% c("Argentina", "French Guiana")] <- "Latin America and Caribbean"
clean_TreatSeek_Any$IHME_Super_Region_Name[clean_TreatSeek_Any$Admin_Unit_Name %in% c("Mayotte")] <- "Sub-Saharan Africa"

clean_TreatSeek_HMISfrac$IHME_Super_Region_Name[clean_TreatSeek_HMISfrac$IHME_Super_Region_Name=="Southeast Asia_ East Asia_ and Oceania"]<-"Southeast Asia, East Asia, and Oceania"
clean_TreatSeek_HMISfrac$IHME_Super_Region_Name[clean_TreatSeek_HMISfrac$Admin_Unit_Name %in% c("South Korea")] <- "Southeast Asia, East Asia, and Oceania"
clean_TreatSeek_HMISfrac$IHME_Super_Region_Name[clean_TreatSeek_HMISfrac$Admin_Unit_Name %in% c("Argentina", "French Guiana")] <- "Latin America and Caribbean"
clean_TreatSeek_HMISfrac$IHME_Super_Region_Name[clean_TreatSeek_HMISfrac$Admin_Unit_Name %in% c("Mayotte")] <- "Sub-Saharan Africa"


#Make a list of all admin units and super regions included in the model
master.superregion.list <- as.factor(unique(full_TreatSeek_n$IHME_Super_Region_Name))
master.superregion.list <-droplevels(master.superregion.list)
#Initialize the lists of units with/without test/training data
units_no_Any_data <- c(); units_with_Any_data <- c(); units_1_Any_data <- c()
units_no_HMISfrac_data <- c(); units_with_HMISfrac_data <- c(); units_1_HMISfrac_data <- c()
master.unit.list <- as.character(master.unit.list)
#Add admin units to the lists
for (i in 1:length(master.unit.list)){
  Any_pts <- sum(as.character(clean_TreatSeek_Any$IHME_location_id) == as.character(master.unit.list[i]))
  HMISfrac_pts <- sum(as.character(clean_TreatSeek_HMISfrac$IHME_location_id) == as.character(master.unit.list[i]))
  HMISfrac_span<-0
  Any_span<-0
  if (Any_pts == 0){units_no_Any_data <- c(units_no_Any_data, master.unit.list[i])}
  if (Any_pts == 1){units_1_Any_data <- c(units_1_Any_data, master.unit.list[i])}
  if (Any_pts > 0){units_with_Any_data <- c(units_with_Any_data, master.unit.list[i])}
  if (HMISfrac_pts == 0){units_no_HMISfrac_data <- c(units_no_HMISfrac_data, master.unit.list[i])}
  if (HMISfrac_pts > 0){units_with_HMISfrac_data <- c(units_with_HMISfrac_data, master.unit.list[i])}
  
  if(HMISfrac_pts>0){
    HMISfrac_span<-unique(clean_TreatSeek_HMISfrac$Year[as.character(clean_TreatSeek_HMISfrac$IHME_location_id)==as.character((master.unit.list[i]))])
    if(max(HMISfrac_span)-min(HMISfrac_span)<5){
      units_1_HMISfrac_data<-c(units_1_HMISfrac_data, master.unit.list[i])
    }
  }
  
  if(Any_pts>0){
    Any_span<-unique(clean_TreatSeek_Any$Year[as.character(clean_TreatSeek_Any$IHME_location_id)==as.character((master.unit.list[i]))])
    if(max(Any_span)-min(Any_span)<5){
      units_1_Any_data<-c(units_1_Any_data, master.unit.list[i])
    }
  }
  
  
  
}
#=============================================REGROUP IHME SUPER REGIONS=============================

# ===========For PTTIR onwards (regional temporal trends):
sig.temp.trends.any<-c("Caribbean","Central Asia","North Africa and Middle East","Tropical Latin America","Western Sub-Saharan Africa")
sig.temp.trends.HMIS<- c("Central Asia","North Africa and Middle East","Southern Sub-Saharan Africa","Western Sub-Saharan Africa") 

#Grouping Super Regions -Time Factor for Any treatment seeking
full_TreatSeek_n$Time_Factor <- as.character(full_TreatSeek_n$IHME_Region_Name)
full_TreatSeek_n$Time_Factor[!(full_TreatSeek_n$IHME_Region_Name %in% sig.temp.trends.any)] <- 'Other'
full_TreatSeek_n$Time_Factor[full_TreatSeek_n$ISO3 %in% c("BFA", "MLI")] <- 'WSA2'
full_TreatSeek_n$Time_Factor[full_TreatSeek_n$ISO3 %in% c("NER")] <- 'WSA3'


full_TreatSeek_n$Time_Factor <- as.factor(full_TreatSeek_n$Time_Factor)
full_TreatSeek_n$Time_Factor <- relevel(full_TreatSeek_n$Time_Factor, ref = 'Other')

#Apply grouping 1 to the DHS dataset
reg_config<-unique(dplyr::select(full_TreatSeek_n, c(IHME_Super_Region_Name,IHME_Region_Name, Time_Factor, Admin_Unit_Name)))
clean_TreatSeek_Any$IHME_Super_Region_Name[clean_TreatSeek_Any$IHME_Super_Region_Name=="Southeast Asia_ East Asia_ and Oceania"]<-"Southeast Asia, East Asia, and Oceania"
clean_TreatSeek_Any<-dplyr::left_join(clean_TreatSeek_Any, reg_config, by=c("IHME_Super_Region_Name","IHME_Region_Name","Admin_Unit_Name"))

#Grouping Super Regions -Time Factor 2 for Public treatment seeking
full_TreatSeek_n$Time_Factor_2 <- as.character(full_TreatSeek_n$IHME_Region_Name)
full_TreatSeek_n$Time_Factor_2[!(full_TreatSeek_n$IHME_Region_Name %in% sig.temp.trends.HMIS)] <- 'Other'

full_TreatSeek_n$Time_Factor_2 <- as.factor(full_TreatSeek_n$Time_Factor_2)
full_TreatSeek_n$Time_Factor_2 <- relevel(full_TreatSeek_n$Time_Factor_2, ref = 'Other')
#Apply grouping 2 to the DHS dataset
reg_config<-unique(dplyr::select(full_TreatSeek_n, c(IHME_Super_Region_Name,IHME_Region_Name, Time_Factor_2, Admin_Unit_Name)))
clean_TreatSeek_HMISfrac$IHME_Super_Region_Name[clean_TreatSeek_HMISfrac$IHME_Super_Region_Name=="Southeast Asia_ East Asia_ and Oceania"]<-"Southeast Asia, East Asia, and Oceania"
clean_TreatSeek_HMISfrac<-dplyr::left_join(clean_TreatSeek_HMISfrac, reg_config, by=c("IHME_Super_Region_Name","IHME_Region_Name","Admin_Unit_Name"))


#============For PTTFR onwards (coarser regional factors):
sig.reg.Any<-c("Andean Latin America","Central Asia","Central Latin America","Central Sub-Saharan Africa","Eastern Sub-Saharan Africa","North Africa and Middle East","Oceania","South Asia","Southeast Asia")
sig.reg.HMIS<-unique(full_TreatSeek_n$IHME_Region_Name)[!unique(full_TreatSeek_n$IHME_Region_Name) %in% c("Central Latin America","North Africa and Middle East","Tropical Latin America")]

#Regional Factor for Any treatment seeking
full_TreatSeek_n$Reg_Factor <- as.character(full_TreatSeek_n$IHME_Region_Name)
full_TreatSeek_n$Reg_Factor[!(full_TreatSeek_n$IHME_Region_Name %in% sig.reg.Any)] <- 'Other'
full_TreatSeek_n$Reg_Factor <- as.factor(full_TreatSeek_n$Reg_Factor)
unique(full_TreatSeek_n$Reg_Factor)
full_TreatSeek_n$Reg_Factor <- relevel(full_TreatSeek_n$Reg_Factor, ref = 'Other')
#Apply grouping 1 to the DHS dataset
reg_config<-unique(dplyr::select(full_TreatSeek_n, c(IHME_Super_Region_Name, IHME_Region_Name, Reg_Factor, Admin_Unit_Name)))
clean_TreatSeek_Any<-dplyr::left_join(clean_TreatSeek_Any, reg_config, by=c("IHME_Super_Region_Name","IHME_Region_Name","Admin_Unit_Name"))

#Regional Factor 2 for Public treatment seeking
full_TreatSeek_n$Reg_Factor_2 <- as.character(full_TreatSeek_n$IHME_Region_Name)
full_TreatSeek_n$Reg_Factor_2[!(full_TreatSeek_n$IHME_Region_Name %in% sig.reg.HMIS)] <- 'Other'

full_TreatSeek_n$Reg_Factor_2 <- as.factor(full_TreatSeek_n$Reg_Factor_2)
unique(full_TreatSeek_n$Reg_Factor_2)
full_TreatSeek_n$Reg_Factor_2 <- relevel(full_TreatSeek_n$Reg_Factor_2, ref = 'Other')
#Apply grouping 2 to the DHS dataset
reg_config<-unique(dplyr::select(full_TreatSeek_n, c(IHME_Super_Region_Name,IHME_Region_Name, Reg_Factor_2, Admin_Unit_Name)))
clean_TreatSeek_HMISfrac<-dplyr::left_join(clean_TreatSeek_HMISfrac, reg_config, by=c("IHME_Super_Region_Name","IHME_Region_Name","Admin_Unit_Name"))

#Draft the formulas for GAMM
Any_covars<-c("ANC4_coverage_prop","he_cap","healthcare_accessibility")
HMIS_covars<-c("IFD_coverage_prop","accessibility","he_cap")

formula_any <-  logit_Any ~ -1 + Reg_Factor + s(Year, by = Time_Factor, k = 5)+s(ANC4_coverage_prop, k=3)+s(he_cap, k=3)+s(healthcare_accessibility, k=3)

formula_HMIS <- logit_HMIS ~ -1 +Reg_Factor_2 + s(Year, by = Time_Factor_2, k = 5)+s(IFD_coverage_prop, k=3)+s(accessibility, k=3)+s(he_cap, k=3)

# Insert dummy true Year for plotting:
full_TreatSeek_n$t.Year <- full_TreatSeek_n$Year


#Freeze non-linear temporal trend according to last available datapoint per Time Factor:
for(tf in unique(full_TreatSeek_n$Time_Factor)){
  ym<-min(clean_TreatSeek_Any$Year[clean_TreatSeek_Any$Time_Factor==tf])
  full_TreatSeek_n$Year[full_TreatSeek_n$Year<ym & full_TreatSeek_n$Time_Factor==tf]<-ym
}
# Freeze non-linear temporal trend pre 1995 for Central Asia:
full_TreatSeek_n$Year[full_TreatSeek_n$Year < 1995 & full_TreatSeek_n$IHME_Region_Name=="Central Asia"] <- 1995
full_TreatSeek_n$Year[full_TreatSeek_n$Year > 2022] <- 2022

# ------------ 1a. Explore the similarity of covariates for units without data ----------

# Function to check proportion of variance explained by significant covariates:

prop_var_plot <- function(prin_comp = region.pca1, plot.title = "Any TS Regional Trend Covariates"){
  std_dev <- prin_comp$sdev
  #compute variance
  pr_var <- std_dev^2
  #proportion of variance explained
  prop_varex <- pr_var/sum(pr_var)
  #scree plot
  plot(prop_varex, xlab = "Principal Component",
       ylab = "Proportion of Variance Explained",
       type = "b")
  title(plot.title)
}

full_TreatSeek_n$rf.unit <- full_TreatSeek_n$IHME_location_id
full_TreatSeek_n$rf.unit.pf <- full_TreatSeek_n$IHME_location_id
full_TreatSeek_n$trend.unit <- full_TreatSeek_n$IHME_location_id
full_TreatSeek_n$trend.unit.pf <- full_TreatSeek_n$IHME_location_id

ref_year_1 <- 1980; ref_year_2 <- 2000; ref_year_3 <- 2020
ETimor_id <- as.character(unique(full_TreatSeek_n$IHME_location_id[full_TreatSeek_n$Admin_Unit_Name == "East Timor"]))


#For some Southeast Asia, Oceania regions, use super regions
master.region.list.1<-c(master.region.list, "Southeast Asia, East Asia, and Oceania", "Latin America and Caribbean")
master.region.list.1<-master.region.list.1[!(master.region.list.1 %in% c("Caribbean","Southeast Asia","Andean Latin America", "Tropical Latin America", "Oceania","Central Latin America"))]




for (j in 1:length(master.region.list.1)){
  
  if(master.region.list.1[j] %in% c("Southeast Asia, East Asia, and Oceania","Latin America and Caribbean")){
    temp_region_cov <- full_TreatSeek_n[full_TreatSeek_n$IHME_Super_Region_Name == as.character(master.region.list.1[j]), ]
    
  }else{ temp_region_cov <- full_TreatSeek_n[full_TreatSeek_n$IHME_Region_Name == as.character(master.region.list.1[j]), ]
  }
  
  
  # Use ADMIN0 with data for matching with ADMIN0 and ADMIN1 without data:
  
  # a) Trend: Use changes between dynamic covariates for years 2000-2010 and 2010-2022.
  
  t_Any_region_cov <- temp_region_cov[temp_region_cov$Admin_Unit_Level == "ADMIN0" | (temp_region_cov$Admin_Unit_Level == "ADMIN1"), ]
  t_Any_region_cov_1 <- t_Any_region_cov[t_Any_region_cov$t.Year == ref_year_1, ]
  t_Any_region_cov_2 <- t_Any_region_cov[t_Any_region_cov$t.Year == ref_year_2, ]
  t_Any_region_cov_3 <- t_Any_region_cov[t_Any_region_cov$t.Year == ref_year_3, ]
  t_Any_region_no_data <- unique(t_Any_region_cov$IHME_location_id[t_Any_region_cov$IHME_location_id %in% c(units_no_Any_data, units_1_Any_data)])
  
  t_HMISfrac_region_cov <- temp_region_cov[temp_region_cov$Admin_Unit_Level == "ADMIN0" | (temp_region_cov$Admin_Unit_Level == "ADMIN1"), ]
  t_HMISfrac_region_cov_1 <- t_HMISfrac_region_cov[t_HMISfrac_region_cov$t.Year == ref_year_1, ]
  t_HMISfrac_region_cov_2 <- t_HMISfrac_region_cov[t_HMISfrac_region_cov$t.Year == ref_year_2, ]
  t_HMISfrac_region_cov_3 <- t_HMISfrac_region_cov[t_HMISfrac_region_cov$t.Year == ref_year_3, ]
  t_HMISfrac_region_no_data <- unique(t_HMISfrac_region_cov$IHME_location_id[t_HMISfrac_region_cov$IHME_location_id %in% c(units_no_HMISfrac_data, units_1_HMISfrac_data)])
  
  # Omit "DMSP_nighttime" because static.
  Any_covars_1<-Any_covars[!(Any_covars %in% c("DMSP_nighttime","accessibility","VIIRS_nighttime","healthcare_accessibility"))]
  temp_df1 <- cbind(t_Any_region_cov_2[, Any_covars_1] - t_Any_region_cov_1[, Any_covars_1], t_Any_region_cov_3[, Any_covars_1] - t_Any_region_cov_2[, Any_covars_1])
  region.pca1 <- prcomp(temp_df1, center = TRUE, scale. = TRUE)
  
  npc_1<-min(3, length(Any_covars_1))
  
  
  query_df1 <- region.pca1$x[t_Any_region_cov_1$IHME_location_id %in% c(units_no_Any_data, units_1_Any_data), 1:npc_1]
  ref_df1 <- region.pca1$x[!(t_Any_region_cov_1$IHME_location_id %in% c(units_no_Any_data, units_1_Any_data)), 1:npc_1]
  
  # Omit "VIIRS_nighttime" because static.
  HMIS_covars_1<-HMIS_covars[!(HMIS_covars %in% c("DMSP_nighttime","accessibility","VIIRS_nighttime","healthcare_accessibility"))]
  temp_df2 <- cbind(t_HMISfrac_region_cov_2[,HMIS_covars_1] - t_HMISfrac_region_cov_1[, HMIS_covars_1], t_HMISfrac_region_cov_3[, HMIS_covars_1] - t_HMISfrac_region_cov_2[, HMIS_covars_1])
  region.pca2 <- prcomp(temp_df2, center = TRUE, scale. = TRUE)
  
  npc_2<-min(3, length(HMIS_covars_1))
  
  query_df2 <- region.pca2$x[t_HMISfrac_region_cov_1$IHME_location_id %in% c(units_no_HMISfrac_data, units_1_HMISfrac_data), 1:npc_2]
  ref_df2 <- region.pca2$x[!(t_HMISfrac_region_cov_1$IHME_location_id %in% c(units_no_HMISfrac_data, units_1_HMISfrac_data)), 1:npc_2]
  
  # b) Random effect: Use significant covariates for years 2000, 2010 and 2019.
  
  rf_Any_region_cov <- temp_region_cov[temp_region_cov$Admin_Unit_Level == "ADMIN0" | (temp_region_cov$Admin_Unit_Level == "ADMIN1" & temp_region_cov$IHME_location_id %in% units_no_Any_data), ]
  
  
  rf_Any_region_cov_1 <- rf_Any_region_cov[rf_Any_region_cov$t.Year == ref_year_1, ]
  rf_Any_region_cov_2 <- rf_Any_region_cov[rf_Any_region_cov$t.Year == ref_year_2, ]
  rf_Any_region_cov_3 <- rf_Any_region_cov[rf_Any_region_cov$t.Year == ref_year_3, ]
  rf_Any_region_no_data <- unique(rf_Any_region_cov$IHME_location_id[rf_Any_region_cov$IHME_location_id %in% units_no_Any_data])
  
  rf_HMISfrac_region_cov <- temp_region_cov[temp_region_cov$Admin_Unit_Level == "ADMIN0" | (temp_region_cov$Admin_Unit_Level == "ADMIN1" & temp_region_cov$IHME_location_id %in% units_no_HMISfrac_data), ]
  
  
  rf_HMISfrac_region_cov_1 <- rf_HMISfrac_region_cov[rf_HMISfrac_region_cov$t.Year == ref_year_1, ]
  rf_HMISfrac_region_cov_2 <- rf_HMISfrac_region_cov[rf_HMISfrac_region_cov$t.Year == ref_year_2, ]
  rf_HMISfrac_region_cov_3 <- rf_HMISfrac_region_cov[rf_HMISfrac_region_cov$t.Year == ref_year_3, ]
  rf_HMISfrac_region_no_data <- unique(rf_HMISfrac_region_cov$IHME_location_id[rf_HMISfrac_region_cov$IHME_location_id %in% units_no_HMISfrac_data])
  
  temp_df3 <- cbind(rf_Any_region_cov_1[, Any_covars], rf_Any_region_cov_2[, Any_covars], rf_Any_region_cov_3[, Any_covars])
  region.pca3 <- prcomp(temp_df3, center = TRUE, scale. = TRUE)
  
  
  query_df3 <- region.pca3$x[rf_Any_region_cov_1$IHME_location_id %in% units_no_Any_data, 1:npc_1]
  ref_df3 <- region.pca3$x[!(rf_Any_region_cov_1$IHME_location_id %in% units_no_Any_data), 1:npc_1]
  
  temp_df4 <- cbind(rf_HMISfrac_region_cov_1[, HMIS_covars], rf_HMISfrac_region_cov_2[, HMIS_covars], rf_HMISfrac_region_cov_3[, HMIS_covars])
  region.pca4 <- prcomp(temp_df4, center = TRUE, scale. = TRUE)
  
  
  query_df4 <- region.pca4$x[rf_HMISfrac_region_cov_1$IHME_location_id %in% units_no_HMISfrac_data, 1:npc_2]
  ref_df4 <- region.pca4$x[!(rf_HMISfrac_region_cov_1$IHME_location_id %in% units_no_HMISfrac_data), 1:npc_2]
  
  ref_class_1 <- t_Any_region_cov_1[!(t_Any_region_cov_1$IHME_location_id %in% t_Any_region_no_data), c("IHME_location_id")]
  ref_class_2 <- t_HMISfrac_region_cov_1[!(t_HMISfrac_region_cov_1$IHME_location_id %in% t_HMISfrac_region_no_data), c("IHME_location_id")]
  ref_class_3 <- rf_Any_region_cov_1[!(rf_Any_region_cov_1$IHME_location_id %in% rf_Any_region_no_data), c("IHME_location_id")]
  ref_class_4 <- rf_HMISfrac_region_cov_1[!(rf_HMISfrac_region_cov_1$IHME_location_id %in% rf_HMISfrac_region_no_data), c("IHME_location_id")]
  
  
  
  neighbour_result_t_any <- knn1(test = query_df1, train = ref_df1, cl = ref_class_1)
  neighbour_result_t_pf <- knn1(test = query_df2, train = ref_df2, cl = ref_class_2)
  neighbour_result_rf_any <- knn1(test = query_df3, train = ref_df3, cl = ref_class_3)
  neighbour_result_rf_pf <- knn1(test = query_df4, train = ref_df4, cl = ref_class_4)
  
  for (i in 1:length(t_Any_region_no_data)){
    full_TreatSeek_n$trend.unit[full_TreatSeek_n$IHME_location_id == t_Any_region_no_data[i]] <- neighbour_result_t_any[i]
  }
  for (i in 1:length(t_HMISfrac_region_no_data)){
    full_TreatSeek_n$trend.unit.pf[full_TreatSeek_n$IHME_location_id == t_HMISfrac_region_no_data[i]] <- neighbour_result_t_pf[i]
  }
  for (i in 1:length(rf_Any_region_no_data)){
    full_TreatSeek_n$rf.unit[full_TreatSeek_n$IHME_location_id == rf_Any_region_no_data[i]] <- neighbour_result_rf_any[i]
  }
  for (i in 1:length(rf_HMISfrac_region_no_data)){
    full_TreatSeek_n$rf.unit.pf[full_TreatSeek_n$IHME_location_id == rf_HMISfrac_region_no_data[i]] <- neighbour_result_rf_pf[i]
  }
  
  pdf(paste(graphics.path, master.region.list.1[j], '.trend.rf.covariates.PCA_NN.pdf', sep = ""),width=12, height = 12)
  
  par(mfrow=c(2,2))
  
  plot(region.pca1$x[, 1:2], col = "white", main = "(a) Trend: Any Treatment Seeking", xlim = c(min(region.pca1$x[, 1])*1.5, max(region.pca1$x[, 1])*1.5), ylim = c(c(min(region.pca1$x[, 2])*1.5, max(region.pca1$x[, 2])*1.5)))
  text(region.pca1$x[, 1:2], labels = t_Any_region_cov_1$Admin_Unit_Name, col = ifelse(t_Any_region_cov_1$IHME_location_id %in% t_Any_region_no_data, "blue", "black"), cex = 0.7)
  
  plot(region.pca2$x[, 1:2], col = "white", main = "(b) Trend: Public Fractions", xlim = c(min(region.pca2$x[, 1])*1.5, max(region.pca2$x[, 1])*1.5), ylim = c(c(min(region.pca2$x[, 2])*1.5, max(region.pca2$x[, 2])*1.5)))
  text(region.pca2$x[, 1:2], labels = t_HMISfrac_region_cov_1$Admin_Unit_Name, col = ifelse(t_HMISfrac_region_cov_1$IHME_location_id %in% t_HMISfrac_region_no_data, "blue", "black"), cex = 0.7)
  
  plot(region.pca3$x[, 1:2], col = "white", main = "(c) Random Effect: Any Treatment Seeking", xlim = c(min(region.pca3$x[, 1])*1.5, max(region.pca3$x[, 1])*1.5), ylim = c(c(min(region.pca3$x[, 2])*1.5, max(region.pca3$x[, 2])*1.5)))
  text(region.pca3$x[, 1:2], labels = rf_Any_region_cov_1$Admin_Unit_Name, col = ifelse(rf_Any_region_cov_1$IHME_location_id %in% rf_Any_region_no_data, "blue", "black"), cex = 0.7)
  
  plot(region.pca4$x[, 1:2], col = "white", main = "(d) Random Effect: Public Fractions", xlim = c(min(region.pca4$x[, 1])*1.5, max(region.pca4$x[, 1])*1.5), ylim = c(c(min(region.pca4$x[, 2])*1.5, max(region.pca4$x[, 2])*1.5)))
  text(region.pca4$x[, 1:2], labels = rf_HMISfrac_region_cov_1$Admin_Unit_Name, col = ifelse(rf_HMISfrac_region_cov_1$IHME_location_id %in% rf_HMISfrac_region_no_data, "blue", "black"), cex = 0.7)
  
  dev.off()
  
  pdf(paste(graphics.path, master.region.list.1[j], '.trend.rf.covariates.PCA_propvar.pdf', sep = ""),width=12, height = 12)
  
  par(mfrow=c(2,2))
  
  prop_var_plot(prin_comp = region.pca1, plot.title = "(a) Trend: Any Treatment Seeking")
  prop_var_plot(prin_comp = region.pca2, plot.title = "(b) Trend: Public Fractions")
  prop_var_plot(prin_comp = region.pca3, plot.title = "(c) Random Effect: Any Treatment Seeking")
  prop_var_plot(prin_comp = region.pca4, plot.title = "(d) Random Effect: Public Fractions")
  
  dev.off()
  
  
  
}


sum(full_TreatSeek_n$rf.unit %in% units_no_Any_data)
sum(full_TreatSeek_n$rf.unit.pf %in% units_no_HMISfrac_data)

NN_results <- full_TreatSeek_n[full_TreatSeek_n$IHME_location_id %in% c(units_no_Any_data, units_no_HMISfrac_data, units_1_Any_data, units_1_HMISfrac_data) & full_TreatSeek_n$t.Year == 2019, c("ISO3", "Country_Name", "Admin_Unit_Name", "IHME_Super_Region_Name", "IHME_Region_Name", "trend.unit", "trend.unit.pf", "rf.unit", "rf.unit.pf")]

col.names <- c("trend.unit", "trend.unit.pf", "rf.unit", "rf.unit.pf")

for (j in col.names){
  NN_results[, j] <- as.character(NN_results[, j])
  temp.trend.unit <- as.character(unique(NN_results[, j]))
  for (i in 1:length(temp.trend.unit)){
    temp.name <- as.character(unique(full_TreatSeek_n$Admin_Unit_Name[full_TreatSeek_n$IHME_location_id == temp.trend.unit[i]]))
    NN_results[na.omit(NN_results[, j] == temp.trend.unit[i]), j] <- temp.name
  }  
}

head(NN_results)

write.csv(NN_results, file = paste(data.path, "trend_rf_NN.csv", sep = ""),row.names=FALSE)

# Implement matched trends:

switch_units <-unique(full_TreatSeek_n$IHME_location_id[full_TreatSeek_n$IHME_location_id != full_TreatSeek_n$trend.unit])
for (loc in switch_units){
  loc.trend<-as.character(unique(full_TreatSeek_n$trend.unit[full_TreatSeek_n$IHME_location_id==loc]))
  full_TreatSeek_n$Time_Factor[full_TreatSeek_n$IHME_location_id==loc] <-  unique(full_TreatSeek_n$Time_Factor[full_TreatSeek_n$IHME_location_id==loc.trend])
  clean_TreatSeek_Any$Time_Factor[clean_TreatSeek_Any$IHME_location_id==loc] <-  unique(clean_TreatSeek_Any$Time_Factor[clean_TreatSeek_Any$IHME_location_id==loc.trend])
  
}

switch_units_2 <-unique(full_TreatSeek_n$IHME_location_id[full_TreatSeek_n$IHME_location_id != full_TreatSeek_n$trend.unit.pf])
for (loc in switch_units_2){
  loc.trend<-as.character(unique(full_TreatSeek_n$trend.unit.pf[full_TreatSeek_n$IHME_location_id==loc]))
  full_TreatSeek_n$Time_Factor_2[full_TreatSeek_n$IHME_location_id==loc] <-  unique(full_TreatSeek_n$Time_Factor_2[full_TreatSeek_n$IHME_location_id==loc.trend])
  clean_TreatSeek_HMISfrac$Time_Factor_2[clean_TreatSeek_HMISfrac$IHME_location_id==loc] <-  unique(clean_TreatSeek_HMISfrac$Time_Factor_2[clean_TreatSeek_HMISfrac$IHME_location_id==loc.trend])
  
}

# ----------------------------------------------- Fit the models...

# Apply data outliering and post-1990 restriction:

Any_data <- clean_TreatSeek_Any
HMIS_data <- clean_TreatSeek_HMISfrac

Any_data$IHME_location_id <- as.factor(Any_data$IHME_location_id)
HMIS_data$IHME_location_id <- as.factor(HMIS_data$IHME_location_id)

Any_data[Any_data$ISO3=="BRA",]
# 3. PREF:
Any_model <-  gamm(formula_any, data = Any_data, random = list(IHME_location_id = ~ 1), control=list(niterEM=1, opt='optim', maxit = 500)) #, control = list(niterEM=1, opt='optim', maxit = 500))
HMIS_model <- gamm(formula_HMIS, data = HMIS_data, random = list(IHME_location_id = ~ 1),control=list(niterEM=1, opt='optim', maxit = 500)) #, control = list(niterEM=1, opt='optim', maxit = 500))
#control function - uncertainty in the covariates
summary(Any_model$gam)
summary(HMIS_model$gam)

# ------------ 1b. Ignore the variability in Any_Treat, HMIS_Treat and the IHME covariates -----------

Any_rf <- rep(NA, nrow(full_TreatSeek_n))

full_TreatSeek_n$rf.unit<-as.character(full_TreatSeek_n$rf.unit)

for (i in 1:length(units_with_Any_data)){
  Any_rf[full_TreatSeek_n$rf.unit == as.character(units_with_Any_data[i])] <-ifelse(is.na(ranef(Any_model$lme,level= length(ranef(Any_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(Any_model$lme))-1))), units_with_Any_data[i], sep = ""), ]), 0, ranef(Any_model$lme,level= length(ranef(Any_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(Any_model$lme))-1))), units_with_Any_data[i], sep = ""), ])
} 


Any_rf[is.na(Any_rf)] <- 0
# check: Storing random effect values (!)
sum(Any_rf == 0) 


HMIS_rf <- rep(NA, nrow(full_TreatSeek_n))

for (i in 1:length(units_with_HMISfrac_data)){
  HMIS_rf[full_TreatSeek_n$rf.unit.pf == as.character(units_with_HMISfrac_data[i])] <-ifelse(is.na(ranef(HMIS_model$lme,level= length(ranef(HMIS_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(HMIS_model$lme))-1))), units_with_HMISfrac_data[i], sep = ""), ]), 0, ranef(HMIS_model$lme,level= length(ranef(HMIS_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(HMIS_model$lme))-1))),units_with_HMISfrac_data[i], sep = ""), ])
} 


HMIS_rf[is.na(HMIS_rf)] <- 0

# check: Storing random effect values (!)
sum(HMIS_rf == 0) 

Any_pred_initial <- predict(Any_model, newdata = full_TreatSeek_n, se.fit = TRUE)
HMIS_pred_initial <- predict(HMIS_model, newdata = full_TreatSeek_n, se.fit = TRUE)

full_TreatSeek_n$Any_pred_initial <- logitlink(Any_pred_initial$fit + Any_rf, inverse=TRUE)
full_TreatSeek_n$Any_pred_initial_low <- logitlink(Any_pred_initial$fit + Any_rf + qnorm(0.025)*Any_pred_initial$se.fit,inverse=TRUE)
full_TreatSeek_n$Any_pred_initial_high <- logitlink(Any_pred_initial$fit + Any_rf + qnorm(0.975)*Any_pred_initial$se.fit,inverse=TRUE)

full_TreatSeek_n$HMIS_pred_initial <- full_TreatSeek_n$Any_pred_initial*logitlink(HMIS_pred_initial$fit + HMIS_rf,inverse=TRUE)
full_TreatSeek_n$HMIS_pred_initial_low <- full_TreatSeek_n$Any_pred_initial_low*logitlink(HMIS_pred_initial$fit + HMIS_rf + qnorm(0.025)*HMIS_pred_initial$se.fit,inverse=TRUE)
full_TreatSeek_n$HMIS_pred_initial_high <- full_TreatSeek_n$Any_pred_initial_high*logitlink(HMIS_pred_initial$fit + HMIS_rf + qnorm(0.975)*HMIS_pred_initial$se.fit,inverse=TRUE)

full_TreatSeek_n$HMISfrac_pred_initial<-logitlink(HMIS_pred_initial$fit + HMIS_rf,inverse=TRUE)
full_TreatSeek_n$HMISfrac_pred_initial_low<-logitlink(HMIS_pred_initial$fit + HMIS_rf+ qnorm(0.025)*HMIS_pred_initial$se.fit,inverse=TRUE)
full_TreatSeek_n$HMISfrac_pred_initial_high<-logitlink(HMIS_pred_initial$fit + HMIS_rf+ qnorm(0.975)*HMIS_pred_initial$se.fit,inverse=TRUE)


# ------------ 2. Consider the variability in Any_Treat, HMIS_Treat and the IHME covariates -----------

load(file = paste(data.path, 'TS_datasets_for_prediction.RData', sep = ''))
load(file = paste(data.path, 'TS_datasets_input_Any.RData', sep = ''))
load(file = paste(data.path, 'TS_datasets_input_HMIS.RData', sep = ''))
N_TS <- 100

## For each of the 100 datasets, fit the chosen models and generate 100 prediction sets.

Any_pred_list <- rep(list(NA), N_TS)
HMIS_pred_list <- rep(list(NA), N_TS)

names(Any_pred_list) <- paste("Dataset", 1:N_TS, sep = "_")
names(HMIS_pred_list) <- paste("Dataset", 1:N_TS, sep = "_")

ref_error <- NA

temp_time <- proc.time()[3]

TS_groups<-unique(dplyr::select(full_TreatSeek_n, c("IHME_location_id", "Time_Factor", "Time_Factor_2", "Reg_Factor","Reg_Factor_2")))
TS_groups$IHME_location_id<-as.numeric(as.character(TS_groups$IHME_location_id))

## Apply lower and upper bounds for the numeric covariates which were simulated.
for (TS_j in 1:N_TS){
  prediction_dataset<-dataset<-TS_datasets[[TS_j]]
  dataset_Any <- TS_clean_datasets_Any[[TS_j]]
  dataset_HMIS <- TS_clean_datasets_HMIS[[TS_j]]
  
  prediction_dataset$IHME_Region_Name[prediction_dataset$ISO3=="BHR"]<-"North Africa and Middle East"
  
  dataset_Any<-dplyr::select(dataset_Any,-c(logit_Fev))
  dataset_HMIS<-dplyr::select(dataset_HMIS, -c(logit_Fev))
  
  dataset<-left_join(dataset_Any,dataset_HMIS, by=c("IHME_location_id", "Year", "Admin_Unit_Name"))
  
  dataset<-left_join(dataset, prediction_dataset, by=c("IHME_location_id", "Year", "Admin_Unit_Name"))
  
  dataset$Any_treat[dataset$Any_treat>1] <- 0.999 # If Any TS > 1 due to random simulations, set upper bound of public frac to be 0.999.
  dataset$Any_treat[dataset$Any_treat<0.001] <- 0.001 # If Any TS <0 due to random simulations, set it to 0.001.
  dataset$logit_Any <- logitlink(dataset$Any_treat,inverse=FALSE)
  
  
  dataset$HMIS_treat[dataset$HMIS_treat>1] <- 0.999 # If HMIS TS > 1 due to random simulations, set upper bound of public frac to be 0.999.
  dataset$HMIS_treat[dataset$HMIS_treat<0.001] <- 0.001 # If HMIS TS <0 due to random simulations, set it to 0.001.
  dataset$logit_HMIS <- logitlink(dataset$HMIS_treat,inverse=FALSE)
  
  
  temp_HMIS_frac <- dataset$HMIS_treat/dataset$Any_treat
  temp_HMIS_frac[temp_HMIS_frac>1] <- 0.999 # If public TS > Any TS due to random simulations, set upper bound of public frac to be 0.999.
  temp_HMIS_frac[temp_HMIS_frac<0.001] <- 0.001
  dataset$logit_HMIS <- logitlink(temp_HMIS_frac,inverse=FALSE)
  
  #  Remove name attributes and set Year as a numeric covariate:
  dataset$Year <- as.numeric(dataset$Year)
  prediction_dataset$Year<-as.numeric(prediction_dataset$Year)
  
  # For PTTIR onwards (regional temporal trends):
  dataset<-dplyr::left_join(dataset, TS_groups, by="IHME_location_id")
  prediction_dataset<-dplyr::left_join(prediction_dataset, TS_groups, by="IHME_location_id")
  
  # For PTTFR onwards (coarser regional factors):
  
  dataset$IHME_Region_Name  <- as.factor(dataset$IHME_Region_Name)
  dataset$IHME_location_id  <- as.factor(dataset$IHME_location_id)
  
  prediction_dataset$IHME_Region_Name  <- as.factor(prediction_dataset$IHME_Region_Name)
  prediction_dataset$IHME_location_id  <- as.factor(prediction_dataset$IHME_location_id)
  
  #  Option 1: Fit models on cleaned data and store 100 predictions for full data set:
  
  clean_Any <- dataset[!is.na(dataset$logit_Any) & is.finite(dataset$logit_Any), ]
  
  clean_HMIS <- dataset[!is.na(dataset$logit_HMIS) & is.finite(dataset$logit_HMIS), ]
  
  Any_data<-clean_Any
  HMIS_data<-clean_HMIS
  
  Any_data$Admin_Unit_Name <- as.factor(Any_data$Admin_Unit_Name)
  HMIS_data$Admin_Unit_Name <- as.factor(HMIS_data$Admin_Unit_Name)
  
  # Refit the models on simulated datasets:
  
  Any_fit <- try(gamm(formula_any, data = Any_data, random = list(IHME_location_id = ~ 1)))
  if("try-error" %in% class(Any_fit)){
    Any_fit <-  gamm(formula_any, data = Any_data, random = list(IHME_location_id = ~ 1), control = list(niterEM=1,opt='optim', maxit = 500)) # list(niterEM=1, opt='optim', maxit = 500)
  }
  
  HMIS_fit <- try(gamm(formula_HMIS, data = HMIS_data, random = list(IHME_location_id = ~ 1)))
  if("try-error" %in% class(HMIS_fit)){
    HMIS_fit <- try(gamm(formula_HMIS, data = HMIS_data, random = list(IHME_location_id = ~ 1), control = list(opt='optim', maxit = 500)))
  }
  
  Any_model<-Any_fit
  HMIS_model<-HMIS_fit
  
  Any_pred <- matrix(NA, nrow = nrow(prediction_dataset), ncol = 100)
  HMIS_pred <- matrix(NA, nrow = nrow(prediction_dataset), ncol = 100)
  
  

  # Freeze non-linear temporal trend pre 1995 for Central Asia and post 2020 for all regions::
  prediction_dataset$Year[prediction_dataset$Year < 1995 & prediction_dataset$IHME_Region_Name=="Central Asia"] <- 1995
  prediction_dataset$Year[prediction_dataset$Year > 2020] <- 2020
  
  
  Any_pred_distr <- predict(Any_fit, newdata = prediction_dataset, se.fit = TRUE)
  HMIS_pred_distr <- predict(HMIS_fit, newdata = prediction_dataset, se.fit = TRUE)
  
  Any_rf <- rep(NA, nrow(full_TreatSeek_n))
  
  for (i in 1:length(units_with_Any_data)){
    Any_rf[full_TreatSeek_n$rf.unit == as.character(units_with_Any_data[i])] <-ifelse(is.na(ranef(Any_model$lme,level= length(ranef(Any_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(Any_model$lme))-1))), units_with_Any_data[i], sep = ""), ]), 0, ranef(Any_model$lme,level= length(ranef(Any_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(Any_model$lme))-1))), units_with_Any_data[i], sep = ""), ])
  } 

  Any_rf[is.na(Any_rf)] <- 0
  
  check<-full_TreatSeek_n[which(Any_rf==0),]
  
  HMIS_rf <- rep(NA, nrow(full_TreatSeek_n))
  
  for (i in 1:length(units_with_HMISfrac_data)){
    HMIS_rf[full_TreatSeek_n$rf.unit.pf == as.character(units_with_HMISfrac_data[i])] <-ifelse(is.na(ranef(HMIS_model$lme,level= length(ranef(HMIS_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(HMIS_model$lme))-1))), units_with_HMISfrac_data[i], sep = ""), ]), 0, ranef(HMIS_model$lme,level= length(ranef(HMIS_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(HMIS_model$lme))-1))),units_with_HMISfrac_data[i], sep = ""), ])
  } 
  
  HMIS_rf[is.na(HMIS_rf)] <- 0
  
  for (i in 1:100){
    set.seed(i)
    Any_pred[, i] <- rnorm(nrow(prediction_dataset), mean = (Any_pred_distr$fit + Any_rf), sd = Any_pred_distr$se.fit)
    HMIS_pred[, i] <- rnorm(nrow(prediction_dataset), mean = (HMIS_pred_distr$fit + HMIS_rf), sd = HMIS_pred_distr$se.fit)
  }
  Any_pred_list[[TS_j]] <- Any_pred
  HMIS_pred_list[[TS_j]] <- HMIS_pred
  
  if((sum(Any_rf == 0) + sum(HMIS_rf == 0))==0){
    print(paste("Dataset ", TS_j, " done.", sep = ""))
  }else{
    ref_error <- c(ref_error, TS_j)
    print(paste("Dataset ", TS_j, ": Random effect matching error.", sep = ""))}
  
}

time.taken <- proc.time()[3] - temp_time

full_Any_pred <- do.call(cbind, Any_pred_list)
full_HMIS_pred <- do.call(cbind, HMIS_pred_list)

full_Any_pred_raw <- logitlink(full_Any_pred,inverse=TRUE)
full_HMIS_pred_raw <- logitlink(full_HMIS_pred,inverse=TRUE)

# Compute mean and 95% confidence intervals:

full_TreatSeek_n$Any_pred <- rowMeans(full_Any_pred_raw)
full_TreatSeek_n$Any_pred_low <- apply(full_Any_pred_raw, MARGIN = 1, function(x){quantile(x, probs = 0.025)})
full_TreatSeek_n$Any_pred_high <- apply(full_Any_pred_raw, MARGIN = 1, function(x){quantile(x, probs = 0.975)})

full_TreatSeek_n$HMIS_pred <- rowMeans(full_HMIS_pred_raw*full_Any_pred_raw)
full_TreatSeek_n$HMIS_pred_low <- apply(full_HMIS_pred_raw*full_Any_pred_raw, MARGIN = 1, function(x){quantile(x, probs = 0.025)})
full_TreatSeek_n$HMIS_pred_high <- apply(full_HMIS_pred_raw*full_Any_pred_raw, MARGIN = 1, function(x){quantile(x, probs = 0.975)})

full_TreatSeek_n$HMISfrac_pred <- rowMeans(full_HMIS_pred_raw)
full_TreatSeek_n$HMISfrac_pred_low <- apply(full_HMIS_pred_raw, MARGIN = 1, function(x){quantile(x, probs = 0.025)})
full_TreatSeek_n$HMISfrac_pred_high <- apply(full_HMIS_pred_raw, MARGIN = 1, function(x){quantile(x, probs = 0.975)})


head(full_TreatSeek_n)

master.region.list <- unique(full_TreatSeek_n$IHME_Region_Name)
master.unit.list <- unique(full_TreatSeek_n$Admin_Unit_Name)


full_TreatSeek_n
full_TreatSeek_n$Admin_Unit_Name[full_TreatSeek_n$ISO3=="GUF"]<-"French Guiana"
full_TreatSeek_n$Admin_Unit_Name[full_TreatSeek_n$ISO3=="SWZ"]<-"Eswatini"


for(i in 1:nrow(full_TreatSeek_n)){
  mean_any<-full_TreatSeek_n$Any_pred[i]
  lower_any<-full_TreatSeek_n$Any_pred_low[i]
  upper_any<-full_TreatSeek_n$Any_pred_high[i]
  
  mean_hmis<-full_TreatSeek_n$HMIS_pred[i]
  lower_hmis<-full_TreatSeek_n$HMIS_pred_low[i]
  upper_hmis<-full_TreatSeek_n$HMIS_pred_high[i]
  
  rany<-rnorm(100000, mean=mean_any, sd=(upper_any-lower_any)/3.92) # assuming that 95% CI spans 1.96*2 sd
  rhmis<-rnorm(100000, mean=mean_hmis, sd=(upper_hmis-lower_hmis)/3.92)
  
  priv<-rany-rhmis
  
  mean_priv<-mean(priv)
  upper_priv<-mean_priv+1.96*sd(priv)
  lower_priv<-mean_priv-1.96*sd(priv)
  
  full_TreatSeek_n$Private_pred[i]<-mean_priv
  full_TreatSeek_n$Private_pred_high[i]<-upper_priv
  full_TreatSeek_n$Private_pred_low[i]<-lower_priv
  
}

#  Save the results:

write.csv(full_TreatSeek_n, file = paste(data.path, 'TS_predictions_GAMkNN.csv', sep = ''),row.names=FALSE) # For Option 1: With the uncertainty in the model fit considered.



# 1.admin1

# 1-Data_Preparation.R

rm(list = ls())
error_mssgs<-list()

GBD<-'ADDRESS'


#===========================================MAKE OUTPUT DIRECTORIES==================================

#Create a dated directory in which the covariates and the parameter file will be saved for version control purposes
# FOR NEW RUNS

Run <- 'ADDRESS'

output.path<-paste0('FILEPATH')
graphics.path.part <- paste0(FILEPATH)
graphics.path <- paste0(FILEPATH)
data.path <- paste0(FILEPATH)
table.path<-paste0(FILEPATH)
dir.create(output.path)
dir.create(graphics.path.part)
dir.create(graphics.path)
dir.create(data.path)
dir.create(table.path)

#================================SPECIFY INPUT PATHS AND MAKE A PARAMETER FILE======================
#1. Inputs for Admin Units
config.path<-paste0("FILEPATH/Combined_Config_Data.csv")
subnat.path<-paste0("FILEPATH/location_set_22.csv")
#2. Covariates
#--MAP (static accessibility and night time lights covariates)
healthcare.accessibility.path<-paste0("FILEPATH/pop_weighted_healthcare_accessibility_1km.csv")
accessibility.path<-paste0("FILEPATH/pop_weighted_accessibility_1km.csv")
VIIRS.path<-paste0("FILEPATH/viirs_v2_nighttime_lights_pop_weighted.csv")

#--IHME
IHME.cov.path<-paste0('FILEPATH/ts_covariates.RData')
#3. Treatment Seeking data from DHS
DHS.path<-paste0("FILEPATH/subnational_TS.csv")

#4. Input years to model:
year.start<-1980

year.end<-2025 

#MAKE PARAM FILE
params<-rbind(config.path,healthcare.accessibility.path,accessibility.path,VIIRS.path,IHME.cov.path,DHS.path,year.start,year.end)
write.table(params, file = paste0(output.path,"/parameters.txt"), row.names=TRUE,col.names=FALSE, quote=FALSE)

#============================================LOAD INPUT DATA========================================
#1. Inputs for Admin Units
config_file<-read.csv(config.path, stringsAsFactors = FALSE )
subnational_config<-read.csv(subnat.path)

#2. Covariates
#--MAP (static accessibility and night time lights covariates)
#Population averaged -> average travel time to city per capita.
healthcare.accessibility<-read.csv(healthcare.accessibility.path)
accessibility <- read.csv(accessibility.path)
VIIRS_nighttime <- read.csv(VIIRS.path)

##filter VIIRS to just 2012
VIIRS_nighttime<-VIIRS_nighttime[VIIRS_nighttime$year==2012,]
VIIRS_nighttime<-dplyr::select(VIIRS_nighttime, -"year")
#--IHME
load(IHME.cov.path)

#3. Treatment Seeking data from DHS
subnational_data<-read.csv(DHS.path) #ADMIN1

#4. Input years to model:
years <- year.start:year.end 

#===========================================CLEAN INPUT DATA========================================
#==================1. ADMIN UNITS===================================================================

sub.countries<-unique(config_file$IHME_location_id[config_file$MAP_Pf_Model_Method=="Reconstitute"])
sub.countries<-sub.countries[!is.na(sub.countries)]
subnational_config_1<-subnational_config[subnational_config$parent_id %in% sub.countries,]
subnational_config_2<-subnational_config[subnational_config$parent_id %in% subnational_config_1$location_id & subnational_config$location_type %in% c("admin1", "subnational"),]

admin1<-rbind(subnational_config_1, subnational_config_2)
rm(subnational_config_1, subnational_config_2)
# Count number of countries to be modelled for treatment seeking: 
length(sub.countries) #12

admin0<-config_file[config_file$IHME_location_id %in% sub.countries & !is.na(config_file$IHME_location_id) ,]
#add admin0 names and ISO3 codes
a0.config<-admin0[,c("MAP_Country_Name", "IHME_location_id", "ISO3", "ISO2", "GAUL_Code","WHO_Region", "WHO_Subregion","IHME_Super_Region_ID", "IHME_Super_Region_Name", "IHME_Region_ID","IHME_Region_Name")]
names(a0.config)[1:2]<-c("CountryName", "parent_id")


a0.config<-admin0[,c("MAP_Country_Name", "IHME_location_id", "ISO3", "ISO2", "GAUL_Code","WHO_Region", "WHO_Subregion","IHME_Super_Region_ID", "IHME_Super_Region_Name", "IHME_Region_ID","IHME_Region_Name")]
names(a0.config)[1:2]<-c("CountryName", "parent_id")
admin1<-left_join(admin1, a0.config, by="parent_id")
nas<-admin1[is.na(admin1$CountryName),]
for(i in 1:nrow(nas)){
  id<-nas$location_id[i]
  parent_id<-nas$parent_id[i]
  admin1$CountryName[admin1$location_id==id]<-admin1$CountryName[admin1$location_id==parent_id]
  admin1$ISO3[admin1$location_id==id]<-admin1$ISO3[admin1$location_id==parent_id]
  admin1$ISO2[admin1$location_id==id]<-admin1$ISO2[admin1$location_id==parent_id]
  admin1$GAUL_Code[admin1$location_id==id]<-admin1$GAUL_Code[admin1$location_id==parent_id]
  admin1$WHO_Region[admin1$location_id==id]<-admin1$WHO_Region[admin1$location_id==parent_id]
  admin1$WHO_Subregion[admin1$location_id==id]<-admin1$WHO_Subregion[admin1$location_id==parent_id]
  admin1$IHME_Super_Region_ID[admin1$location_id==id]<-admin1$IHME_Super_Region_ID[admin1$location_id==parent_id]
  admin1$IHME_Super_Region_Name[admin1$location_id==id]<-admin1$IHME_Super_Region_Name[admin1$location_id==parent_id]
  admin1$IHME_Region_ID[admin1$location_id==id]<-admin1$IHME_Region_ID[admin1$location_id==parent_id]
  admin1$IHME_Region_Name[admin1$location_id==id]<-admin1$IHME_Region_Name[admin1$location_id==parent_id]
}

admin1 %>% 
  group_by_all() %>%
  filter(n() > 1) %>%
  ungroup()

nas %>% 
  group_by_all() %>%
  filter(n() > 1) %>%
  ungroup()


subnational_list<-unique(admin1$location_id)

which(is.na(admin1$ISO3))

# Remove Six Minor Territories  (aka Union Territories other than Delhi) and add them individually 
SMT_IDs<-c(4840,4858,4845,4847,4848,4866)\
subnational_list <- subnational_list[-which(subnational_list == 44538)] #Remove The Six Minor Territories
six_minor_terr <- c("Andaman & Nicobar Islands", "Lakshadweep", "Chandigarh", "Dadra & Nagar Haveli", "Daman & Diu", "Puducherry")
SMT_list <- data.frame("ISO3" = "IND","ISO2"="IN","Gaul_Code"=115, "country_name" = "India", "Admin_Unit_Name" = six_minor_terr, "IHME_location_id" = SMT_IDs)


subnational_list <- c(subnational_list, SMT_IDs)

#Also, remove China(without Hong Kond and Macao) and other units which are not necessary
units_to_remove<-subnational_list[!subnational_list %in% c(VIIRS_nighttime$loc_id, SMT_IDs)]
subnational_list <- subnational_list[-which(subnational_list %in% units_to_remove)] 

admin1<-admin1[admin1$location_id %in% c(subnational_list),]



admin1$location_type<- "ADMIN1"

####FINISHED ADDING ADMIN1 here - continue below.

#==================2. COVARIATES (IHME) ===================================================================
# Choose only the covariates we wil use in the model:
cov_names <- rep(NA, length(ts_covs))
for (i in 1:length(cov_names)){
  cov_names[i] <- unique(ts_covs[[i]]$covariate_name_short)
}
names(ts_covs) <- cov_names
ts_covs <- ts_covs[which(cov_names %in% c("ANC1_coverage_prop", "ANC4_coverage_prop","DTP3_coverage_prop","hospital_beds_per1000","IFD_coverage_prop","LDI_pc","measles_vacc_cov_prop", "SBA_coverage_prop","GDPpc_id_b2010", "prop_urban", "he_cap", "measles_vacc_cov_prop_2", "ind_health", "education_all_ages_and_sexes_pc", "log_the_pc" ))] 
cov_names <- rep(NA, length(ts_covs))
for (i in 1:length(cov_names)){
  cov_names[i] <- unique(as.character(ts_covs[[i]]$covariate_name_short))
}
names(ts_covs) <- cov_names

save(ts_covs,file=paste0(output.path,"/ts_covariates_clean.RData"))

#==================3. TREATMENT SEEKING DATA FROM DHS===================================================================
subnational_data$CountryName <- as.character(subnational_data$CountryName)
config_file<-filter(config_file,!is.na(ISO3))

########## ---------------------- 1. CREATE FULL ORIGINAL DATASETS -------------------- ############
admin1<-dplyr::select(admin1, c("ISO3", "CountryName", "location_id", "location_type", "location_ascii_name", "IHME_Super_Region_Name", "IHME_Region_Name"))

names(admin1)<-c("ISO3", "Country_Name", "IHME_location_id", "Admin_Unit_Level", "Admin_Unit_Name", "IHME_Super_Region_Name", "IHME_Region_Name")


#---Make a full metadata set
metadb<-admin1
metadb$Year<-years[1]
metadb$SMT_Factor<-"Non_SMT"
full_metadb<-metadb


for(y in 2:length(years)){
  metadb$Year<-years[y]
  full_metadb<-rbind(full_metadb, metadb)
}

SMT_rows<-full_metadb[full_metadb$Admin_Unit_Name=="Assam",]
SMT_rows$IHME_location_id<-44538

for(ids in SMT_IDs){
  SMT_rows$Admin_Unit_Name<-unique(SMT_list$Admin_Unit_Name[SMT_list$IHME_location_id==ids])
  SMT_rows$SMT_Factor<-"SMT"
  full_metadb<-rbind(full_metadb,SMT_rows)
}



nrow(full_metadb)==(nrow(admin1)+6)*length(years)

#--- Now add the IHME covariates:

full_TreatSeek<-full_metadb


for(i in 1:length(ts_covs)){
  ts_i<-ts_covs[[i]]
  ts_i<-dplyr::select(ts_i, c("location_id", "year_id", "mean_value"))
  names(ts_i)<-c("IHME_location_id","Year",names(ts_covs)[i])
  full_TreatSeek<-dplyr::left_join(full_TreatSeek, ts_i, by=c("IHME_location_id","Year"))
}

#--- Now add MAP covars:
#healthcare accessibility
healthcare.accessibility<-dplyr::select(healthcare.accessibility, c("loc_id","weighted_rate"))
names(healthcare.accessibility)<-c("IHME_location_id","healthcare_accessibility")
full_TreatSeek<-left_join(full_TreatSeek, healthcare.accessibility, by=c("IHME_location_id"))

#accessibility to cities
accessibility<-dplyr::select(accessibility, c("loc_id","weighted_rate"))
names(accessibility)<-c("IHME_location_id","accessibility")
full_TreatSeek<-left_join(full_TreatSeek, accessibility, by=c("IHME_location_id"))

#VIIRS_nighttime
VIIRS_nighttime<-dplyr::select(VIIRS_nighttime, c("loc_id","weighted_rate"))

names(VIIRS_nighttime)<-c("IHME_location_id","VIIRS_nighttime")
full_TreatSeek<-left_join(full_TreatSeek, VIIRS_nighttime, by=c("IHME_location_id"))

nrow(full_TreatSeek)

nrow(full_TreatSeek)==(nrow(admin1)+6)*length(years)


#Change SMT ids back
for(i in 1:length(six_minor_terr)){
  full_TreatSeek$IHME_location_id[full_TreatSeek$Admin_Unit_Name==six_minor_terr[i]]<-SMT_IDs[i]
  
}


write.csv(full_TreatSeek, paste(data.path, "full_TreatSeek.csv", sep = ""),row.names=FALSE)

########## ---------------------- 2. Normalise the Covariates -------------------- ############

full_TreatSeek_n <- full_TreatSeek

# Check the distributions of the covariates before normalisation:

# Transform prior normalisation:
full_TreatSeek_n$ANC1_coverage_prop <- full_TreatSeek_n$ANC1_coverage_prop^2
full_TreatSeek_n$hospital_beds_per1000 <- log(full_TreatSeek_n$hospital_beds_per1000)
full_TreatSeek_n$LDI_pc <- log(full_TreatSeek_n$LDI_pc)
full_TreatSeek_n$measles_vacc_cov_prop <- full_TreatSeek_n$measles_vacc_cov_prop^2
full_TreatSeek_n$measles_vacc_cov_prop_2 <- log(full_TreatSeek_n$measles_vacc_cov_prop_2 + 0.0001)
full_TreatSeek_n$GDPpc_id_b2010 <- log(full_TreatSeek_n$GDPpc_id_b2010)
full_TreatSeek_n$he_cap <- log(full_TreatSeek_n$he_cap)
full_TreatSeek_n$ind_health <- log(full_TreatSeek_n$ind_health)
full_TreatSeek_n$healthcare_accessibility <- log(full_TreatSeek_n$healthcare_accessibility)
full_TreatSeek_n$accessibility <- log(full_TreatSeek_n$accessibility + 0.0001)
full_TreatSeek_n$VIIRS_nighttime <- log(full_TreatSeek_n$VIIRS_nighttime)

#Specify the covariate column numbers
first_covar<-names(ts_covs[1])
col.start<-which(colnames(full_TreatSeek)==first_covar)
col.end<-col.start+length(ts_covs)+2 #IHME covariates plus 3 MAP covariates

#MARGIN=2 indicates that the function will be applied accross the specified columns
full_TreatSeek_n[, col.start:col.end] <- apply(full_TreatSeek_n[, col.start:col.end], MARGIN = 2, FUN = function(x){(x - mean(x))/sd(x)}) 

#check if the covariates are in fact normalised (all should have mean 0)
summary(full_TreatSeek_n)

full_TreatSeek_n$IHME_Region_Name <- droplevels(as.factor(full_TreatSeek_n$IHME_Region_Name)) 

write.csv(full_TreatSeek_n, paste(data.path, "full_TreatSeek_n.csv", sep = ""),row.names=FALSE)

########## ---------------------- 3. CREATE CLEAN SUBSETS WITH TS DATA FOR MODELLING -------------------- ############
# ------------- Check number of IHME  regions in TS data -------- #
names(subnational_data)

subnational_data$Outlier_Any<-"NO"
subnational_data$Outlier_HMIS<-"NO"
subnational_data$Outlier_Any[subnational_data$iso3=="NGA" & subnational_data$SurveyYear==2008]<-"YES"
subnational_data$Outlier_HMIS[subnational_data$iso3=="NGA" & subnational_data$SurveyYear==2008]<-"YES"

subnational_data<-dplyr::select(subnational_data, c("location_id","SurveyYear","SurveyType","HMIS_treat","HMIS_treat_lower","HMIS_treat_upper","Any_treat","Any_treat_lower","Any_treat_upper","Outlier_HMIS","Outlier_Any"))
names(subnational_data)<-c("IHME_location_id","Year","SurveyType","HMIS_treat","HMIS_treat_low","HMIS_treat_high","Any_treat","Any_treat_low","Any_treat_high","Outlier_HMIS","Outlier_Any")

dhs_data<-subnational_data

clean_TreatSeek<-left_join(full_TreatSeek_n, dhs_data, by= c("IHME_location_id","Year"))

clean_TreatSeek %>% 
  group_by_all() %>%
  filter(n() > 1) %>%
  ungroup()

clean_TreatSeek <- distinct(clean_TreatSeek)


#remove NAs and Outliers 
clean_TreatSeek_Any <- clean_TreatSeek[!is.na(clean_TreatSeek$Any_treat), ]
clean_TreatSeek_Any<-clean_TreatSeek_Any[clean_TreatSeek_Any$Outlier_Any=="NO",]

clean_TreatSeek_HMISfrac <- clean_TreatSeek_Any[!is.na(clean_TreatSeek_Any$HMIS_treat/clean_TreatSeek_Any$Any_treat)  & (clean_TreatSeek_Any$HMIS_treat != clean_TreatSeek_Any$Any_treat), ] # To be able to do logit transform later. logit has a nice interpretation and leads to values between 0 and 1. Non-normality could be due to underlying covariates.\
clean_TreatSeek_HMISfrac<-clean_TreatSeek_HMISfrac[clean_TreatSeek_HMISfrac$Outlier_HMIS=="NO",]
clean_TreatSeek_HMISfrac$HMIS_frac <- clean_TreatSeek_HMISfrac$HMIS_treat/clean_TreatSeek_HMISfrac$Any_treat

clean_TreatSeek_Any$logit_Any <- logitlink(clean_TreatSeek_Any$Any_treat)
clean_TreatSeek_HMISfrac$logit_HMIS <- logitlink(clean_TreatSeek_HMISfrac$HMIS_frac)

clean_TreatSeek_Any <- clean_TreatSeek_Any[is.finite(clean_TreatSeek_Any$logit_Any), ]
clean_TreatSeek_HMISfrac <- clean_TreatSeek_HMISfrac[is.finite(clean_TreatSeek_HMISfrac$logit_HMIS), ]

summary(clean_TreatSeek_Any$IHME_Region_Name)
summary(clean_TreatSeek_HMISfrac$IHME_Region_Name)

clean_TreatSeek_Any %>% 
  group_by_all() %>%
  filter(n() > 1) %>%
  ungroup()

names(clean_TreatSeek_Any)

clean_TreatSeek_Any<-dplyr::select(clean_TreatSeek_Any, -c("Outlier_Any","Outlier_HMIS"))
clean_TreatSeek_HMISfrac<-dplyr::select(clean_TreatSeek_HMISfrac, -c("Outlier_Any","Outlier_HMIS"))

write.csv(clean_TreatSeek_Any, paste(data.path, 'clean_TreatSeek_Any.csv', sep = ''))
write.csv(clean_TreatSeek_HMISfrac, paste(data.path, 'clean_TreatSeek_HMISfrac.csv', sep = ''))
######### ---------------------- 4. CREATE TRAINING AND TEST SETS FOR CHOOSING MODELS -------------------- ###########

# ------------------- Remove pre-1990 data (and outliers) and create training/test sets -----------------

clean_TreatSeek_Any <- read.csv(paste(data.path, 'clean_TreatSeek_Any.csv', sep = ''))
clean_TreatSeek_HMISfrac <- read.csv(paste(data.path, 'clean_TreatSeek_HMISfrac.csv', sep = ''))

master.country.list <- unique(clean_TreatSeek_Any$Country_Name)

#First we make the training and test (validating) sets for the model of treatment seeking at any facility (public, private, NGO)
set.seed(1)
train.id <- numeric()
for (i in 1:length(master.country.list)){
  region.id <- which(clean_TreatSeek_Any$Country_Name == master.country.list[i])
  years<-clean_TreatSeek_Any$Year[region.id]
  early.y<-region.id[which(years<2000)] #Make sure the 1990-2000 surveys don't get lost in random sampling.
  late.y<-region.id[which(years>=2000)]
  if (length(region.id) > 8){
    no.sample.early <- ceiling(length(early.y)*0.7)
    no.sample.late <- ceiling(length(late.y)*0.7)
    temp.id.e <- sample(early.y, no.sample.early)
    temp.id.l <- sample(late.y, no.sample.late)
  }else{temp.id <- region.id}
  train.id <- c(train.id, temp.id.e, temp.id.l)
}

length(train.id)/nrow(clean_TreatSeek_Any) 
train_data_Any <- clean_TreatSeek_Any[train.id, ]

train_data_Any %>% 
  group_by_all() %>%
  filter(n() > 1) %>%
  ungroup()

summary(train_data_Any$Country_Name)

nrow(train_data_Any) 
train_data_Any <- as.data.frame(train_data_Any) #training set (~70% survey DHS data)


for (i in 1:length(six_minor_terr)){
  if (!(six_minor_terr[i] %in% train_data_Any$Admin_Unit_Name)){
    train_data_Any <- rbind(train_data_Any, clean_TreatSeek_Any[clean_TreatSeek_Any$Admin_Unit_Name == six_minor_terr[i], ]) # Only have one data_Any point per SMT at the moment.
  }
}

test_data_Any <- clean_TreatSeek_Any[!(clean_TreatSeek_Any$X %in% train_data_Any$X), ] #model validation set (~30% DHS survey data)
nrow(test_data_Any) 


#Second, we make the same training and testing sets for the model of treatment seeking at public facilities
set.seed(1)
train.id <- numeric()
for (i in 1:length(master.country.list)){
  region.id <- which(clean_TreatSeek_HMISfrac$Country_Name == master.country.list[i])
  years<-clean_TreatSeek_HMISfrac$Year[region.id]
  early.y<-region.id[which(years<2000)] #Make sure the 1990-2000 surveys don't get lost in random sampling.
  late.y<-region.id[which(years>=2000)]
  if (length(region.id) > 8){
    no.sample.early <- ceiling(length(early.y)*0.7)
    no.sample.late <- ceiling(length(late.y)*0.7)
    temp.id.e <- sample(early.y, no.sample.early)
    temp.id.l <- sample(late.y, no.sample.late)
  }else{temp.id <- region.id}
  train.id <- c(train.id, temp.id.e, temp.id.l)
}


train_data_HMISfrac <- clean_TreatSeek_HMISfrac[train.id, ]

train_data_HMISfrac <- as.data.frame(train_data_HMISfrac)

for (i in 1:length(six_minor_terr)){
  if (!(six_minor_terr[i] %in% train_data_HMISfrac$Admin_Unit_Name)){
    train_data_HMISfrac <- rbind(train_data_HMISfrac, clean_TreatSeek_HMISfrac[(clean_TreatSeek_HMISfrac$Admin_Unit_Name == six_minor_terr[i]), ]) # Only have one data_HMISfrac point per SMT at the moment.
  }
}

test_data_HMISfrac <- clean_TreatSeek_HMISfrac[!(clean_TreatSeek_HMISfrac$X %in% train_data_HMISfrac$X), ]

# Save the training and test sets:
write.csv(train_data_Any, paste(data.path, 'train_data_Any.csv', sep = ''), row.names=FALSE)
write.csv(train_data_HMISfrac, paste(data.path, 'train_data_HMISfrac.csv', sep = ''), row.names=FALSE)
write.csv(test_data_Any, paste(data.path, 'test_data_Any.csv', sep = ''), row.names=FALSE)
write.csv(test_data_HMISfrac, paste(data.path, 'test_data_HMISfrac.csv', sep = ''), row.names=FALSE)



########## ---------------------- 4. CREATE 100 FULL DATASETS TO ACCOUNT FOR TS AND IHME COVARIATE VARIABILITY -------------------- ############

# These are alternative full_TreatSeek_n datasets for fitting and predicting.

N_TS <- 100

TS_datasets <- rep(list(NA), N_TS)
TS_clean_datasets_Any<- rep(list(NA), N_TS)
TS_clean_datasets_HMIS<- rep(list(NA), N_TS)

names(TS_datasets) <- paste("Dataset", 1:N_TS, sep = "_")
names(TS_clean_datasets_Any) <- paste("Dataset", 1:N_TS, sep = "_")
names(TS_clean_datasets_HMIS) <- paste("Dataset", 1:N_TS, sep = "_")

temptime3 <- proc.time()[3]

for (TS_i in 1:N_TS){
  set.seed(TS_i)
  
  
  full_TreatSeek_i<-full_metadb
  
  #--- Now add the IHME covariates:
  full_TreatSeek_i$IHME_location_id[full_TreatSeek$Admin_Unit_Name %in% six_minor_terr]<-44538
  
  
  
  for(i in 1:length(ts_covs)){
    ts_i<-ts_covs[[i]]
    ts_i$rls_value<-ts_i$mean_value
    
    indx<-which(ts_i$lower_value != ts_i$upper_value)
    for(x in indx){
      ts_i$rls_value[x]<-rnorm(1, mean=ts_i$mean_value[x], sd=((ts_i$upper_value[x]-ts_i$lower_value[x])/(2*qnorm(0.975))))
    }
    
    ts_i<-dplyr::select(ts_i, c("location_id", "year_id", "rls_value"))
    names(ts_i)<-c("IHME_location_id","Year",names(ts_covs)[i])
    full_TreatSeek_i<-dplyr::left_join(full_TreatSeek_i, ts_i, by=c("IHME_location_id","Year"))
  }
  

  #--- Now add MAP covars:
  #healthcare accessibility
  full_TreatSeek_i<-left_join(full_TreatSeek_i, healthcare.accessibility, by=c("IHME_location_id"))
  
  #accessibility to cities
  full_TreatSeek_i<-left_join(full_TreatSeek_i, accessibility, by=c("IHME_location_id"))
  
  #VIIRS_nighttime
  full_TreatSeek_i<-left_join(full_TreatSeek_i, VIIRS_nighttime, by=c("IHME_location_id"))
  
  
  #Change SMT ids back
  for(i in 1:length(six_minor_terr)){
    full_TreatSeek_i$IHME_location_id[full_TreatSeek_i$Admin_Unit_Name==six_minor_terr[i]]<-SMT_IDs[i]
    
  }
  
  # Can check values before taking log transforms etc. 
  full_TreatSeek_i$ANC1_coverage_prop <-  sapply(full_TreatSeek_i$ANC1_coverage_prop, function(x){min(max(x, 0), 1)})
  full_TreatSeek_i$ANC4_coverage_prop <-  sapply(full_TreatSeek_i$ANC4_coverage_prop, function(x){min(max(x, 0), 1)})
  full_TreatSeek_i$DTP3_coverage_prop <-  sapply(full_TreatSeek_i$DTP3, function(x){min(max(x, 0), 1)})
  full_TreatSeek_i$IFD_coverage_prop <- sapply(full_TreatSeek_i$IFD_coverage_prop, function(x){min(max(x, 0), 1)})
  full_TreatSeek_i$measles_vacc_cov_prop <-  sapply(full_TreatSeek_i$measles_vacc_cov_prop, function(x){min(max(x, 0), 1)})
  full_TreatSeek_i$measles_vacc_cov_prop_2 <-  sapply(full_TreatSeek_i$measles_vacc_cov_prop_2, function(x){min(max(x, 0), 1)})
  full_TreatSeek_i$SBA_coverage_prop <-  sapply(full_TreatSeek_i$SBA_coverage_prop, function(x){min(max(x, 0), 1)})
  full_TreatSeek_i$prop_urban <-  sapply(full_TreatSeek_i$prop_urban, function(x){min(max(x, 0), 1)})
  full_TreatSeek_i$education_all_ages_and_sexes_pc <-  sapply(full_TreatSeek_i$education_all_ages_and_sexes_pc, function(x){min(max(x, 0), 99)})
  full_TreatSeek_i$log_the_pc <- sapply(full_TreatSeek_i$log_the_pc, function(x){max(x, 0)})
  full_TreatSeek_i$hospital_beds_per1000 <-  sapply(full_TreatSeek_i$education_all_ages_and_sexes_pc, function(x){min(max(x, 0.0001), 1000)})
  full_TreatSeek_i$LDI_pc <- sapply(full_TreatSeek_i$LDI_pc, function(x){max(x, 0.0001)})
  full_TreatSeek_i$GDPpc_id_b2010 <- sapply(full_TreatSeek_i$GDPpc_id_b2010, function(x){max(x, 0.0001)})
  full_TreatSeek_i$he_cap <- sapply(full_TreatSeek_i$he_cap, function(x){max(x, 0.0001)})
  full_TreatSeek_i$ind_health <- sapply(full_TreatSeek_i$ind_health, function(x){min(max(x, 0.0001), 1)})
  
  # Transform prior normalisation:
  full_TreatSeek_i$ANC1_coverage_prop <- full_TreatSeek_i$ANC1_coverage_prop^2
  full_TreatSeek_i$hospital_beds_per1000 <- log(full_TreatSeek_i$hospital_beds_per1000)
  full_TreatSeek_i$LDI_pc <- log(full_TreatSeek_i$LDI_pc)
  full_TreatSeek_i$measles_vacc_cov_prop <- full_TreatSeek_i$measles_vacc_cov_prop^2
  full_TreatSeek_i$measles_vacc_cov_prop_2 <- log(full_TreatSeek_i$measles_vacc_cov_prop_2 + 0.0001)
  full_TreatSeek_i$GDPpc_id_b2010 <- log(full_TreatSeek_i$GDPpc_id_b2010)
  full_TreatSeek_i$he_cap <- log(full_TreatSeek_i$he_cap)
  full_TreatSeek_i$ind_health <- log(full_TreatSeek_i$ind_health)
  full_TreatSeek_i$healthcare_accessibility <- log(full_TreatSeek_i$healthcare_accessibility)
  full_TreatSeek_i$VIIRS_nighttime <- log(full_TreatSeek_i$VIIRS_nighttime)
  full_TreatSeek_i$accessibility <- log(full_TreatSeek_i$accessibility + 0.0001)
  
  full_TreatSeek_i_copy<-full_TreatSeek_i
  
  full_TreatSeek_i[, col.start:col.end] <- apply(full_TreatSeek_i[, col.start:col.end], MARGIN = 2, FUN = function(x){(x - mean(x))/sd(x)}) 
  
  
  
  str(full_TreatSeek_i)
  
  clean_TreatSeek_Any_i<-dplyr::select(clean_TreatSeek_Any, c("IHME_location_id","Admin_Unit_Name", "Year","Any_treat","Any_treat_low","Any_treat_high"))
  clean_TreatSeek_HMIS_i<-dplyr::select(clean_TreatSeek_HMISfrac, c("IHME_location_id","Admin_Unit_Name", "Year","HMIS_treat","HMIS_treat_low","HMIS_treat_high"))
  
  for(r in 1: nrow(clean_TreatSeek_Any_i)){
    clean_TreatSeek_Any_i$rls_value[r]<-rnorm(1, mean=clean_TreatSeek_Any_i$Any_treat[r], sd=((clean_TreatSeek_Any_i$Any_treat_high[r]-clean_TreatSeek_Any_i$Any_treat_low[r])/(2*qnorm(0.975))))
  }
  
  for(r in 1: nrow(clean_TreatSeek_HMIS_i)){
    clean_TreatSeek_HMIS_i$rls_value[r]<-rnorm(1, mean=clean_TreatSeek_HMIS_i$HMIS_treat[r], sd=((clean_TreatSeek_HMIS_i$HMIS_treat_high[r]-clean_TreatSeek_HMIS_i$HMIS_treat_low[r])/(2*qnorm(0.975))))
  }
  
  
  clean_TreatSeek_Any_i$Any_treat<-clean_TreatSeek_Any_i$rls_value
  clean_TreatSeek_Any_i$Any_treat[clean_TreatSeek_Any_i$Any_treat>1]<-0.999
  clean_TreatSeek_Any_i$Any_treat[clean_TreatSeek_Any_i$Any_treat<0.001] <- 0.001
  clean_TreatSeek_Any_i<-dplyr::select(clean_TreatSeek_Any_i, -c("Any_treat_low", "Any_treat_high", "rls_value"))
  
  clean_TreatSeek_HMIS_i$HMIS_treat<-clean_TreatSeek_HMIS_i$rls_value
  clean_TreatSeek_HMIS_i$HMIS_treat[clean_TreatSeek_HMIS_i$HMIS_treat>1]<-0.999
  clean_TreatSeek_HMIS_i$HMIS_treat[clean_TreatSeek_HMIS_i$HMIS_treat<0.001] <- 0.001
  clean_TreatSeek_HMIS_i<-dplyr::select(clean_TreatSeek_HMIS_i, -c("HMIS_treat_low", "HMIS_treat_high", "rls_value"))
  
  clean_TreatSeek_Any_i$logit_Fev <- logitlink(clean_TreatSeek_Any_i$Any_treat)
  clean_TreatSeek_HMIS_i$logit_Fev <- logitlink(clean_TreatSeek_HMIS_i$HMIS_treat)
  
  TS_datasets[[TS_i]] <- full_TreatSeek_i 
  TS_clean_datasets_Any[[TS_i]]<-clean_TreatSeek_Any_i
  TS_clean_datasets_HMIS[[TS_i]]<-clean_TreatSeek_HMIS_i
  
  print(TS_i)
  
}

timetaken3 <- proc.time()[3] - temptime3

# About an hour (used to be over 24h)
save(TS_datasets, file = paste(data.path, 'TS_datasets_for_prediction.RData', sep = ''))
save(TS_clean_datasets_Any, file = paste(data.path, 'TS_datasets_input_Any.RData', sep = ''))
save(TS_clean_datasets_HMIS, file = paste(data.path, 'TS_datasets_input_HMIS.RData', sep = ''))
# Check:

load(paste(data.path, 'TS_datasets_for_prediction.RData', sep = ''))
full_TreatSeek_i <- TS_datasets[[3]]
summary(full_TreatSeek_i)

summary(as.factor(full_TreatSeek_i$IHME_Region_Name))
summary(as.factor(full_TreatSeek_n$IHME_Region_Name))

########## ---------------------- 5. CHECK IHME COVARIATE VARIABILITY -------------------- ############

#--- inspect IHME covariates:
#temporarily change GUF and MYT loc ids
cov_variability<-full_metadb
cov_variability$IHME_location_id[cov_variability$ISO3=="GUF"]<-unique(admin0$IHME_location_id[admin0$ISO3 == "SUR"])
cov_variability$IHME_location_id[cov_variability$ISO3=="MYT"]<-  unique(admin0$IHME_location_id[admin0$ISO3 == "COM"])


for(i in 1:length(ts_covs)){
  ts_i<-ts_covs[[i]]
  ts_i<-dplyr::select(ts_i, c("location_id", "year_id", "mean_value","lower_value","upper_value" ))
  names(ts_i)[1:2]<-c("IHME_location_id","Year")
  covariate_i<-left_join(cov_variability, ts_i, by=c("IHME_location_id","Year"))
  
  pdf(paste(graphics.path, names(ts_covs)[i], '.pdf', sep = ''),width=8.7,height = 11.2)
  
  par(mfrow=c(3,2))
  
  for (l in 1:length(master.country.list)){
    region.data <- covariate_i[covariate_i$Country_Name == master.country.list[l], ]
    countries <- unique(region.data$ISO3)
    for (m in 1:length(countries)){
      unit.list <- unique(region.data[region.data$ISO3 == countries[m], c("Admin_Unit_Name", "Admin_Unit_Level")])
      for (n in 1:nrow(unit.list)){
        unit.row <- region.data[region.data$ISO3 == countries[m] & region.data$Admin_Unit_Name == unit.list$Admin_Unit_Name[n] & region.data$Admin_Unit_Level == unit.list$Admin_Unit_Level[n], ]
        plot(0,0,type='n',c(min(covariate_i$lower_value, na.rm = TRUE),max(covariate_i$upper_value, na.rm = TRUE)),xlim=c(year.start, year.end),main=paste(master.country.list[l], ': ', unit.list$Admin_Unit_Name[n] , ", ", countries[m], sep = ''), ylab='IHME covariate', xlab='Year')
        
        plotCI(year.start:year.end,unit.row$mean_value,ui=unit.row$upper_value,li=unit.row$lower_value,ylim=c(min(covariate_i$lower_value, na.rm = TRUE),max(covariate_i$upper_value, na.rm = TRUE)),add=T)
      }
    }
  }
  
  dev.off()
  print(names(ts_covs)[i])
}


# 2-Model_Selection.R

rm(list = ls())

#===========================================SET OUTPUT DIRECTORIES==================================
output.path<-paste0('FILEPATH')

graphics.path <- paste0(FILEPATH)
dir.create(graphics.path) 
data.path <- paste0(FILEPATH)
table.path<-paste0(FILEPATH)

years <- 1980:2025
# Read in training and test sets:

train_data_Any <- read.csv(paste(data.path, 'train_data_Any.csv', sep = ''))
train_data_HMISfrac <- read.csv(paste(data.path, 'train_data_HMISfrac.csv', sep = ''))

train_data_Any<-train_data_Any[train_data_Any$Year %in% years,]
train_data_HMISfrac<-train_data_HMISfrac[train_data_HMISfrac$Year %in% years,]

train_data_Any$Country_Name<-as.factor(train_data_Any$Country_Name) #Need to transform Country_Name to factor
train_data_HMISfrac$Country_Name<-as.factor(train_data_HMISfrac$Country_Name)

##############################################################################################
#MODEL SELECTION
##############################################################################################

####################################################
#   1. IDENTIFY SIGNIFICANT TEMPORAL TRENDS
####################################################

model_no<-"01_temporal_"

### Fit the first models - note that here we use the covariates chosen in the ADMIN0 model

train_data_Any %>% 
  filter(is.na(healthcare_accessibility)) %>% 
  distinct(Country_Name)
train_data_HMISfrac %>% 
  filter(is.na(healthcare_accessibility)) %>% 
  distinct(Country_Name)

#Any
formula_any <-  logit_Any ~ -1 + Country_Name + s(Year, by = Country_Name, k = 5)+s(ANC1_coverage_prop, k=3)+s(he_cap, k=3)+s(healthcare_accessibility, k=3)
test.model.any <- gam(formula_any, data = train_data_Any)

summary(train_data_Any) #


#Public
formula_HMIS <- logit_HMIS ~ -1 + Country_Name + s(Year, by = Country_Name, k = 5)+s(IFD_coverage_prop, k=3)+s(accessibility, k=3)+s(he_cap, k=3)

test.model.hmis <- gam(formula_HMIS, data = train_data_HMISfrac)

summary(test.model.hmis) #

####Plot the splines of the temporal trends

gamtabs(test.model.any, caption = "Summary of first any TS model with country temporal trends and factors")
# Look at the plots of the smooths !
pdf(paste(graphics.path, model_no, "Any_model_smooths.pdf", sep = ""), width = 12, height = 8)
par(mfrow = c(2, 3))
for(i in 1:30){
  plot(test.model.any, select = i, ylim = c(-5, 5))
  abline(h=0, col="red", lty=2)
}

dev.off()


gamtabs(test.model.hmis, caption = "Summary of first public fraction model with country temporal trends and factors")

pdf(paste(graphics.path, model_no, "HMIS_model_smooths.pdf", sep = ""), width = 12, height = 8)
par(mfrow = c(2, 3))
for(i in 1:30){
  plot(test.model.hmis, select = i, ylim = c(-5, 5))
  abline(h=0, col="red", lty=2)
}

dev.off()

#### Examine the p-values from model summaries and the spline plots to choose significant temporal trends.


####################################################
#   2. IDENTIFY SIGNIFICANT INTERCEPTS
####################################################

model_no<-"02_intercepts_"

### update these to the countries with significant (non-flat) temporal trends 
sig.temp.trends.any<-c("Indonesia","Kenya","Nigeria","Philippines")
sig.temp.trends.HMIS<- c("India","Indonesia","Kenya","Nigeria","Philippines") 

train_data_Any$Time_Factor <- as.character(train_data_Any$Country_Name)
train_data_Any$Time_Factor[!(train_data_Any$Country_Name %in% sig.temp.trends.any)] <- 'Other' #not a specific trend (where weird regional trends would go)
train_data_Any$Time_Factor[train_data_Any$Country_Name=="Ethiopia"]<-"Kenya" #group Kenya and Ethiopia together

train_data_Any$Time_Factor <- as.factor(train_data_Any$Time_Factor)
unique(train_data_Any$Time_Factor)
train_data_Any$Time_Factor <- relevel(train_data_Any$Time_Factor, ref = 'Other')
##
train_data_HMISfrac$Time_Factor_2 <- as.character(train_data_HMISfrac$Country_Name)
train_data_HMISfrac$Time_Factor_2[!(train_data_HMISfrac$Country_Name %in% sig.temp.trends.HMIS)] <- 'Other'
train_data_HMISfrac$Time_Factor_2[train_data_HMISfrac$Country_Name=="Pakistan"]<-"India" 


train_data_HMISfrac$Time_Factor_2 <- as.factor(train_data_HMISfrac$Time_Factor_2)
unique(train_data_HMISfrac$Time_Factor_2)
train_data_HMISfrac$Time_Factor_2 <- relevel(train_data_HMISfrac$Time_Factor_2, ref = 'Other')

#Any
formula_any <-  logit_Any ~ -1 + Country_Name + s(Year, by = Time_Factor, k = 5)+s(ANC1_coverage_prop, k=3)+s(he_cap, k=3)+s(healthcare_accessibility, k=3)
test.model.any<-gam(formula_any, data = train_data_Any)


#Public
formula_HMIS <- logit_HMIS ~ -1 + Country_Name + s(Year, by = Time_Factor_2, k = 5)+s(IFD_coverage_prop, k=3)+s(accessibility, k=3)+s(he_cap, k=3)
test.model.hmis <- gam(formula_HMIS, data = train_data_HMISfrac)

###Plot the intercepts

#Any
sum_any_1<-summary(test.model.any)
reg_coeff <- sum_any_1$p.coeff

pdf(paste(graphics.path, model_no, "Any_intercepts_1.pdf", sep = ""), width = 12, height = 4)
plotCI(1:length(reg_coeff), reg_coeff, ui = reg_coeff + qnorm(0.975)*sum_any_1$se[grep("Country_Name", names(sum_any_1$se))], li = reg_coeff - qnorm(0.975)*sum_any_1$se[grep("Country_Name", names(sum_any_1$se))], xlab = 'Country_Name', ylab = 'Intercept estimates', xaxt = 'n')
abline(h = 0, lty = 2)
axis(side=1, at=1:length(reg_coeff), labels = c("BRA", 'ETH', 'IND', 'IDN',"KEN", 'MEX','NGA', 'PAK', 'PHL','ZWE'))
dev.off()

#Public

sum_hmis_1 <- summary(test.model.hmis)
reg_coeff_2 <- sum_hmis_1$p.coeff


pdf(paste(graphics.path, model_no, "HMIS_intercepts_1.pdf", sep = ""), width = 12, height = 4)
plotCI(1:length(reg_coeff_2), reg_coeff_2, ui = reg_coeff_2 + qnorm(0.975)*sum_hmis_1$se[grep("Country_Name", names(sum_hmis_1$se))], li = reg_coeff_2 - qnorm(0.975)*sum_hmis_1$se[grep("Country_Name", names(sum_hmis_1$se))], xlab = 'Country_Name', ylab = 'Intercept estimates', xaxt = 'n')
abline(h = 0, lty = 2)
axis(side=1, at=1:length(reg_coeff_2), labels=c("BRA", 'ETH', 'IND', 'IDN',"KEN", 'MEX','NGA', 'PAK', 'PHL','ZWE'))
dev.off()


#Choose the significant intercepts based on the p-values in the summary and the intercept plots (consider the differences from 0 and the differences from each other)
##update these to the countries with significant intercepts 
sig.reg.Any<-unique(train_data_Any$Country_Name)
sig.reg.HMIS<-unique(train_data_HMISfrac$Country_Name)[!unique(train_data_HMISfrac$Country_Name)%in% c("South Africa", "Brazil", "Mexico")]


train_data_Any$Reg_Factor <- as.character(train_data_Any$Country_Name)
train_data_Any$Reg_Factor[!(train_data_Any$Country_Name %in% sig.reg.Any)] <- 'Other'
train_data_Any$Reg_Factor <- as.factor(train_data_Any$Reg_Factor)

train_data_HMISfrac$Reg_Factor_2 <- as.character(train_data_HMISfrac$Country_Name)
train_data_HMISfrac$Reg_Factor_2[!(train_data_HMISfrac$Country_Name %in% sig.reg.HMIS)] <- 'Other'
train_data_HMISfrac$Reg_Factor_2 <- as.factor(train_data_HMISfrac$Reg_Factor_2)
train_data_HMISfrac$Reg_Factor_2 <- relevel(train_data_HMISfrac$Reg_Factor_2, ref = 'Other')

# Use location id for the unit specific effect.
train_data_Any$IHME_location_id <- as.factor(train_data_Any$IHME_location_id)
train_data_HMISfrac$IHME_location_id <- as.factor(train_data_HMISfrac$IHME_location_id)


unique(train_data_HMISfrac$Country_Name[train_data_HMISfrac$Time_Factor_2=="Other"])
#Prune the intercept and temporal trends
#Any
formula_any <-  logit_Any ~ -1 + Reg_Factor + s(Year, by = Time_Factor, k = 5)+s(ANC1_coverage_prop, k=3)+s(he_cap, k=3)+s(healthcare_accessibility, k=3)
test.model.any<- gam(formula_any, data = train_data_Any)
summary(test.model.any) #

check<-gamm(formula_any, data = train_data_Any,random = list(IHME_location_id = ~ 1)) 

#Public
formula_HMIS <- logit_HMIS ~ -1 + Reg_Factor_2 + s(Year, by = Time_Factor_2, k = 5)+s(VIIRS_nighttime, k=3)+s(healthcare_accessibility, k=3)+s(hospital_beds_per1000, k=3)+s(DTP3_coverage_prop, k=3)
test.model.hmis <- gam(formula_HMIS, data = train_data_HMISfrac)

summary(test.model.hmis) #

check<-gamm(formula_HMIS, data = train_data_HMISfrac,random = list(IHME_location_id = ~ 1)) 


####################################################
#   IDENTIFY SIGNIFICANT COVARIATES
####################################################


###### Choose covariates based on AIC
cov_col_vector<-names(train_data_Any)[(which(names(train_data_Any)=="Year")+2):(which(names(train_data_Any)=="SurveyType")-1)]
cov_col <- train_data_Any[, cov_col_vector]# Fit univariate GAMs on the covariates and compare AIC, then select covariate to include in final model depending on their relative AIC and correlation with each other:
#Specify the covariate column numbers
first_covar<-cov_col_vector[1]
col.start<-which(colnames(train_data_Any)==first_covar)
col.end<-col.start+length(cov_col_vector)-1 # Fit univariate GAMs on the covariates and compare AIC, then select covariate to include in final model depending on their relative AIC and correlation with each other:

cov_ind <- c(col.start:col.end) # check covariate names.
AIC_Any <- rep(NA, length(cov_ind))
names(AIC_Any) <- names(train_data_HMISfrac)[cov_ind]

AIC_HMIS <- rep(NA, length(cov_ind))
names(AIC_HMIS) <- names(train_data_HMISfrac)[cov_ind]

for (i in 1:length(cov_ind)){
  var_values_Any <- train_data_Any[, cov_ind[i]]
  gam_Any <- gamm(logit_Any ~ -1 + Reg_Factor + s(Year, by = Time_Factor) + s(X), data = data.frame("logit_Any" = train_data_Any$logit_Any, "X" = var_values_Any, 'Year' = train_data_Any$Year, 'Time_Factor' = train_data_Any$Time_Factor, 'Reg_Factor' = train_data_Any$Reg_Factor, 'IHME_location_id' = train_data_Any$IHME_location_id), random = list(IHME_location_id = ~ 1))
  AIC_Any[i] <- AIC(gam_Any$lme)
  var_values_HMIS <- train_data_HMISfrac[, cov_ind[i]]
  gam_HMIS <- gamm(logit_HMIS ~ -1 + Reg_Factor_2 + s(Year, by = Time_Factor_2) + s(X), data =  data.frame("logit_HMIS" = train_data_HMISfrac$logit_HMIS, "X" = var_values_HMIS, 'Year' = train_data_HMISfrac$Year, 'Time_Factor_2' = train_data_HMISfrac$Time_Factor_2, 'Reg_Factor_2' = train_data_HMISfrac$Reg_Factor_2, 'IHME_location_id' = train_data_HMISfrac$IHME_location_id), random = list(IHME_location_id = ~ 1))
  AIC_HMIS[i] <- AIC(gam_HMIS$lme)
}
AIC_Any_order <- names(sort(AIC_Any[!(names(AIC_Any) %in% c("measles_vacc_cov_prop_2", "oop_hexp_cap", "education_all_ages_and_sexes_pc", "ind_health","GDPpc_id_b2010"))]))# Don't use frac_oop_hexp, oop_hexp_cap for any TS. measles_vacc_cov_prop_2 has extreme outliers. accessibility (travel time) etc. has strange relations.)]))


test_cov_mat <- cor(cov_col[, AIC_Any_order])
abs_mat <- abs(test_cov_mat) >0.6 # For checking which covariates are strongly correlated to those of increasing AIC.

test_cov_Any <- AIC_Any_order[1]

for (i in AIC_Any_order[-1]){
  # If candidate covariate has < 0.6 correlation with existing covariates.
  if (sum(abs_mat[i, test_cov_Any])< 1){
    test_cov_Any <- c(test_cov_Any, i)
  }
}

AIC_HMIS_order <- names(sort(AIC_HMIS[!(names(AIC_HMIS) %in% c("measles_vacc_cov_prop_2", "education_all_ages_and_sexes_pc", "ind_health"))])) #these have strange relations

test_cov_mat <- cor(cov_col[, AIC_HMIS_order])
abs_mat <- abs(test_cov_mat) >0.6 # For checking which covariates are strongly correlated to those of increasing AIC.

test_cov_HMIS <- AIC_HMIS_order[1]

for (i in AIC_HMIS_order[-1]){
  # If candidate covariate has < 0.6 correlation with existing covariates.
  if (sum(abs_mat[i, test_cov_HMIS])< 1){
    test_cov_HMIS <- c(test_cov_HMIS, i)
  }
}


#note these covariates in the model formulas below and prune as needed.
test_cov_Any
test_cov_HMIS

####################################################
#   3.  FINAL MODEL
####################################################

model_no<-"03_final_"

#Any
train_data_Any$IHME_location_id<-as.factor(train_data_Any$IHME_location_id)
formula_any <-  logit_Any ~ -1 + Reg_Factor + s(Year, by = Time_Factor, k = 5)+s(LDI_pc, k=3)+s(VIIRS_nighttime, k=3)+s(IFD_coverage_prop, k=3)

model_any_1 <- gamm(formula_any, data = train_data_Any,random = list(IHME_location_id = ~ 1), control = list(opt='optim'))

summary(model_any_1$gam) #all covars significant
refRegion <- ranef(model_any_1$lme) # for QQ plots

#Public
formula_HMIS <- logit_HMIS ~ -1 + Reg_Factor_2 + s(Year, by = Time_Factor_2, k = 5)+s(VIIRS_nighttime, k=3)+s(healthcare_accessibility, k=3)+s(hospital_beds_per1000, k=3)+s(he_cap, k=3)
model_hmis_1 <- gamm(formula_HMIS, data = train_data_HMISfrac, random = list(IHME_location_id = ~ 1), control = list(opt='optim'))

summary(model_hmis_1$gam)
refRegion_HMIS <- ranef(model_hmis_1$lme) #for QQ plots

# 7-Model_validation.R


rm(list = ls())
#===========================================SET OUTPUT DIRECTORIES==================================
output.path<-paste0('FILEPATH')
graphics.path <- paste0(FILEPATH)
dir.create(graphics.path)
data.path <- paste0(FILEPATH)
table.path<-paste0(FILEPATH)

# Years to model:
years <- 1980:2025 
# Read in training and test sets:

train_data_Any <- read.csv(paste(data.path, 'train_data_Any.csv', sep = ''))
train_data_HMISfrac <- read.csv(paste(data.path, 'train_data_HMISfrac.csv', sep = ''))
test_data_Any <- read.csv(paste(data.path, 'test_data_Any.csv', sep = ''))
test_data_HMISfrac <- read.csv(paste(data.path, 'test_data_HMISfrac.csv', sep = ''))

train_data_Any$Country_Name<-as.factor(train_data_Any$Country_Name) #added conversion to factors
test_data_Any$Country_Name<-as.factor(test_data_Any$Country_Name)#added conversion to factors

# Models to be tested:

model_no <- "Final_" 

# For PTTIR onwards (regional temporal trends):

sig.temp.trends.any<-c("Indonesia","Nigeria","Philippines")
sig.temp.trends.HMIS<- c("India","Indonesia","Kenya","Nigeria","Pakistan","Philippines" ) 
train_data_Any$Time_Factor <- as.character(train_data_Any$Country_Name)
train_data_Any$Time_Factor[!(train_data_Any$Country_Name %in% sig.temp.trends.any)] <- 'Other'
train_data_Any$Time_Factor <- as.factor(train_data_Any$Time_Factor)
unique(train_data_Any$Time_Factor)
train_data_Any$Time_Factor <- relevel(train_data_Any$Time_Factor, ref = 'Other')

test_data_Any$Time_Factor <- as.character(test_data_Any$Country_Name)
test_data_Any$Time_Factor[!(test_data_Any$Country_Name %in% sig.temp.trends.any)] <- 'Other'
test_data_Any$Time_Factor <- as.factor(test_data_Any$Time_Factor)
unique(test_data_Any$Time_Factor)
test_data_Any$Time_Factor <- relevel(test_data_Any$Time_Factor, ref = 'Other')

train_data_HMISfrac$Time_Factor_2 <- as.character(train_data_HMISfrac$Country_Name)
train_data_HMISfrac$Time_Factor_2[!(train_data_HMISfrac$Country_Name %in% sig.temp.trends.HMIS)] <- 'Other'
train_data_HMISfrac$Time_Factor_2 <- as.factor(train_data_HMISfrac$Time_Factor_2)
unique(train_data_HMISfrac$Time_Factor_2)
train_data_HMISfrac$Time_Factor_2 <- relevel(train_data_HMISfrac$Time_Factor_2, ref = 'Other')

test_data_HMISfrac$Time_Factor_2 <- as.character(test_data_HMISfrac$Country_Name)
test_data_HMISfrac$Time_Factor_2[!(test_data_HMISfrac$Country_Name %in% sig.temp.trends.HMIS)] <- 'Other'
test_data_HMISfrac$Time_Factor_2 <- as.factor(test_data_HMISfrac$Time_Factor_2)
unique(test_data_HMISfrac$Time_Factor_2)
test_data_HMISfrac$Time_Factor_2 <- relevel(test_data_HMISfrac$Time_Factor_2, ref = 'Other')

# For PTTFR onwards (coarser regional factors):
sig.reg.Any<-unique(train_data_Any$Country_Name)[!unique(train_data_Any$Country_Name) %in% c("Brazil", "Ethiopia", "South Africa")]
sig.reg.HMIS<-unique(train_data_HMISfrac$Country_Name)[!(unique(train_data_HMISfrac$Country_Name)%in% c("South Africa", "Brazil", "Mexico"))]

train_data_Any$Reg_Factor <- as.character(train_data_Any$Country_Name)
train_data_Any$Reg_Factor[!(train_data_Any$Country_Name %in% sig.reg.Any)] <- 'Other'
train_data_Any$Reg_Factor <- as.factor(train_data_Any$Reg_Factor)
unique(train_data_Any$Reg_Factor)
train_data_Any$Reg_Factor <- relevel(train_data_Any$Reg_Factor, ref = 'Other')

test_data_Any$Reg_Factor <- as.character(test_data_Any$Country_Name)
test_data_Any$Reg_Factor[!(test_data_Any$Country_Name %in% sig.reg.Any)] <- 'Other'
test_data_Any$Reg_Factor <- as.factor(test_data_Any$Reg_Factor)
unique(test_data_Any$Reg_Factor)
test_data_Any$Reg_Factor <- relevel(test_data_Any$Reg_Factor, ref = 'Other')

train_data_HMISfrac$Reg_Factor_2 <- as.character(train_data_HMISfrac$Country_Name)
train_data_HMISfrac$Reg_Factor_2[!(train_data_HMISfrac$Country_Name %in% sig.reg.HMIS)] <- 'Other'
train_data_HMISfrac$Reg_Factor_2 <- as.factor(train_data_HMISfrac$Reg_Factor_2)
unique(train_data_HMISfrac$Reg_Factor_2)
train_data_HMISfrac$Reg_Factor_2 <- relevel(train_data_HMISfrac$Reg_Factor_2, ref = 'Other')

test_data_HMISfrac$Reg_Factor_2 <- as.character(test_data_HMISfrac$Country_Name)
test_data_HMISfrac$Reg_Factor_2[!(test_data_HMISfrac$Country_Name %in% sig.reg.HMIS)] <- 'Other'
test_data_HMISfrac$Reg_Factor_2 <- as.factor(test_data_HMISfrac$Reg_Factor_2)
unique(test_data_HMISfrac$Reg_Factor_2)
test_data_HMISfrac$Reg_Factor_2 <- relevel(test_data_HMISfrac$Reg_Factor_2, ref = 'Other')

# Use location id as factor instead of admin unit name so as to account for 2 Punjabs and 2 Mexicos:
train_data_Any$IHME_location_id <- as.factor(train_data_Any$IHME_location_id)
train_data_HMISfrac$IHME_location_id <- as.factor(train_data_HMISfrac$IHME_location_id)
test_data_Any$IHME_location_id <- as.factor(test_data_Any$IHME_location_id)
test_data_HMISfrac$IHME_location_id <- as.factor(test_data_HMISfrac$IHME_location_id)

#here's the final formulas from PREF

formula_any <-  logit_Any ~ -1 + Reg_Factor + s(Year, by = Time_Factor, k = 5)+s(ANC1_coverage_prop, k=3)+s(he_cap, k=3)+s(healthcare_accessibility, k=3)

formula_HMIS <- logit_HMIS ~ -1 + Reg_Factor_2 + s(Year, by = Time_Factor_2, k = 5)+s(VIIRS_nighttime, k=3)+s(healthcare_accessibility, k=3)+s(hospital_beds_per1000, k=3)+s(DTP3_coverage_prop, k=3)

# Use best models:
Any_model <-  gamm(formula_any, data = train_data_Any, random = list(IHME_location_id = ~ 1), control = list(opt='optim'))
HMIS_model <- gamm(formula_HMIS, data = train_data_HMISfrac, random = list(IHME_location_id = ~ 1), control = list(opt='optim'))

train.predict.any <- predict(Any_model)
train.predict.HMIS <- predict(HMIS_model)

master.region.list <- unique(train_data_Any$Country_Name)
master.unit.list <- unique(train_data_Any$IHME_location_id)

Any_rf <- rep(NA, nrow(train_data_Any))

for (i in 1:length(master.unit.list)){
  Any_rf[train_data_Any$IHME_location_id == as.character(master.unit.list[i])] <-ifelse(is.na(ranef(Any_model$lme,level= length(ranef(Any_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(Any_model$lme))-1))), master.unit.list[i], sep = ""), ]), 0, ranef(Any_model$lme,level= length(ranef(Any_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(Any_model$lme))-1))), master.unit.list[i], sep = ""), ])
} 


Any_rf[is.na(Any_rf)] <- 0

HMIS_rf <- rep(NA, nrow(train_data_HMISfrac))

master.region.list <- unique(train_data_HMISfrac$Country_Name)
master.unit.list <- unique(train_data_HMISfrac$IHME_location_id)

for (i in 1:length(master.unit.list)){
  HMIS_rf[train_data_HMISfrac$IHME_location_id == as.character(master.unit.list[i])] <- ifelse(is.na(ranef(HMIS_model$lme,level= length(ranef(HMIS_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(HMIS_model$lme))-1))), master.unit.list[i], sep = ""), ]), 0, ranef(HMIS_model$lme,level= length(ranef(HMIS_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(HMIS_model$lme))-1))), master.unit.list[i], sep = ""), ])
  
}

HMIS_rf[is.na(HMIS_rf)] <- 0

(sum(Any_rf == 0) + sum(HMIS_rf == 0))==0

train.predict.any <- predict(Any_model, newdata = train_data_Any) + Any_rf
train.predict.HMIS <- predict(HMIS_model, newdata = train_data_HMISfrac) + HMIS_rf

master.region.list <- unique(test_data_Any$Country_Name)
master.unit.list <- unique(test_data_Any$IHME_location_id)

Any_rf_2 <- rep(NA, nrow(test_data_Any))

for (i in 1:length(master.unit.list)){
  Any_rf_2[test_data_Any$IHME_location_id == as.character(master.unit.list[i])] <-ifelse(is.na(ranef(Any_model$lme,level= length(ranef(Any_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(Any_model$lme))-1))), master.unit.list[i], sep = ""), ]), 0, ranef(Any_model$lme,level= length(ranef(Any_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(Any_model$lme))-1))), master.unit.list[i], sep = ""), ])
}
Any_rf_2[is.na(Any_rf_2)] <- 0

master.region.list <- unique(test_data_HMISfrac$Country_Name)
master.unit.list <- unique(test_data_HMISfrac$IHME_location_id)

HMIS_rf_2 <- rep(NA, nrow(test_data_HMISfrac))
for (i in 1:length(master.unit.list)){
  HMIS_rf_2[test_data_HMISfrac$IHME_location_id == as.character(master.unit.list[i])] <- ifelse(is.na(ranef(HMIS_model$lme,level= length(ranef(HMIS_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(HMIS_model$lme))-1))), master.unit.list[i], sep = ""), ]), 0, ranef(HMIS_model$lme,level= length(ranef(HMIS_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(HMIS_model$lme))-1))), master.unit.list[i], sep = ""), ])
}

HMIS_rf_2[is.na(HMIS_rf_2)] <- 0

(sum(Any_rf_2 == 0) + sum(HMIS_rf_2 == 0))==0 # There are test units which have no data in training set.

test.predict.any <- predict(Any_model, newdata = test_data_Any) + Any_rf_2
test.predict.HMIS <- predict(HMIS_model, newdata = test_data_HMISfrac) + HMIS_rf_2


# Model validation:

pdf(paste(graphics.path, "Model_validation_plots.pdf", sep = ""), width = 9, height = 9)
par(mfrow = c(2, 2), mai = rep(0.4, 4))
plot(train.predict.any, train.predict.any - train_data_Any$logit_Any, main = "Residuals versus fitted values (logit any)", ylim = c(-2.5, 2.5))
abline(h = 0)
plot(train.predict.HMIS, train.predict.HMIS - train_data_HMISfrac$logit_HMIS, main = "Residuals versus fitted values (logit public fraction)", ylim = c(-2.5, 2.5))
abline(h = 0)
hist(train.predict.any - train_data_Any$logit_Any, main = 'Histogram of residuals (logit any)')
hist(train.predict.HMIS - train_data_HMISfrac$logit_HMIS, main = 'Histogram of residuals (logit public fraction)')
dev.off()

pdf(paste(graphics.path, "Fitted_vs_observed.pdf", sep = ""), width = 9, height = 4.5)
par(mfrow = c(1, 2), mai = rep(0.4, 4))
plot(train_data_Any$logit_Any, train.predict.any, main = "Fitted versus observed values (logit any)", ylim = c(min(train_data_Any$logit_Any), max(train_data_Any$logit_Any)), xlim = c(min(train_data_Any$logit_Any), max(train_data_Any$logit_Any)))
lines(train_data_Any$logit_Any, train_data_Any$logit_Any)
plot(train_data_HMISfrac$logit_HMIS, train.predict.HMIS, main = "Fitted versus observed values (logit public fraction)", ylim = c(min(train_data_Any$logit_Any), max(train_data_Any$logit_Any)), xlim = c(min(train_data_HMISfrac$logit_HMIS), max(train_data_HMISfrac$logit_HMIS)))
lines(train_data_HMISfrac$logit_HMIS, train_data_HMISfrac$logit_HMIS)
dev.off()

pdf(paste(graphics.path, "Fitted_vs_observed_rawscale.pdf", sep = ""), width = 9, height = 4.5)
par(mfrow = c(1, 2), mai = rep(0.4, 4))
plot(train_data_Any$Any_treat, inv.logit(train.predict.any), main = "Fitted versus observed values (any)", ylim = c(0, 1), xlim = c(0, 1))
lines(c(0, 1), c(0, 1))
plot(train_data_HMISfrac$HMIS_frac, inv.logit(train.predict.HMIS), main = "Fitted versus observed values (public fraction)", ylim = c(0, 1), xlim = c(0, 1))
lines(c(0, 1), c(0, 1))
dev.off()


# Compare training and test RMSE - amount of overfitting.

train.error.any <- sqrt(mean((train.predict.any - train_data_Any$logit_Any)^2))
train.error.hmis <- sqrt(mean((train.predict.HMIS - train_data_HMISfrac$logit_HMIS)^2))

test.error.any <- sqrt(mean((test.predict.any - test_data_Any$logit_Any)^2))
test.error.hmis <- sqrt(mean((test.predict.HMIS - test_data_HMISfrac$logit_HMIS)^2))

train.error.any.raw <- sqrt(mean((train_data_Any$Any_treat - inv.logit(train.predict.any))^2))
train.error.hmis.raw <- sqrt(mean((train_data_HMISfrac$HMIS_frac - inv.logit(train.predict.HMIS))^2))

test.error.any.raw <- sqrt(mean((test_data_Any$Any_treat - inv.logit(test.predict.any))^2))
test.error.hmis.raw <- sqrt(mean((test_data_HMISfrac$HMIS_frac - inv.logit(test.predict.HMIS))^2))

# 8-Prediction_experiment.R

rm(list = ls())
set.seed(8)
#===========================================SET OUTPUT DIRECTORIES==================================
output.path<-paste0('FILEPATH')
graphics.path <- paste0(FILEPATH)
dir.create(graphics.path)
data.path <- paste0(FILEPATH)
table.path<-paste0(FILEPATH)

source("FILEPATH/multiplot.R")
#============================================LOAD INPUT DATA========================================
# Years to model:
years <- 1980:2025

# IHME populations:
# Read in IHME covariates:
load(paste0(output.path,"ts_covariates_clean.RData")) #ts_covars cleaned in Data_Preparation step
# Read in full datasets, training and test sets:
full_TreatSeek <- read.csv(paste(data.path, "full_TreatSeek.csv", sep = ""))
full_TreatSeek_n <- read.csv(paste(data.path, "full_TreatSeek_n.csv", sep = ""))


clean_TreatSeek_Any <- read.csv(paste(data.path, "clean_TreatSeek_Any.csv", sep = ""))
clean_TreatSeek_HMISfrac <- read.csv(paste(data.path, "clean_TreatSeek_HMISfrac.csv", sep = ""))
clean_TreatSeek_Any$Admin_Unit_Name <- as.character(clean_TreatSeek_Any$Admin_Unit_Name)
clean_TreatSeek_HMISfrac$Admin_Unit_Name <- as.character(clean_TreatSeek_HMISfrac$Admin_Unit_Name)


unique(full_TreatSeek$Country_Name[is.na(full_TreatSeek$Admin_Unit_Level)])


#=====================================TRANSFORM DATA================================================
# Use location id as factor for regional trends instead of admin unit name so as to account for 2 Punjabs and 2 Mexicos:
clean_TreatSeek_Any$IHME_location_id <- as.factor(clean_TreatSeek_Any$IHME_location_id)
clean_TreatSeek_HMISfrac$IHME_location_id <- as.factor(clean_TreatSeek_HMISfrac$IHME_location_id)
full_TreatSeek_n$IHME_location_id <- as.factor(full_TreatSeek_n$IHME_location_id)
full_TreatSeek$IHME_location_id <- as.factor(full_TreatSeek$IHME_location_id)


#=====================================REMOVE CHINA AND IRAN BECAUSE THEY HAVE NO DATA - THEY WILL JUST INHERIT NATIONAL VALUES================================================
full_TreatSeek_n<-full_TreatSeek_n[!full_TreatSeek_n$Country_Name %in% c("Iran", "China"),]
full_TreatSeek<-full_TreatSeek[!full_TreatSeek_n$Country_Name %in% c("Iran", "China"),]


#=====================================FIND ADMIN UNITS WITHOUT DHS SURVEY DATA====================================
master.region.list <- unique(full_TreatSeek_n$Country_Name) # This has grouped Oceania with SE Asia etc. Use full_TreatSeek if want true regions.
master.unit.list <- unique(full_TreatSeek_n$IHME_location_id)

#Initialize the lists of units with/without test/training data
units_no_Any_data <- c(); units_with_Any_data <- c()
units_no_HMISfrac_data <- c(); units_with_HMISfrac_data <- c()
master.unit.list <- as.character(master.unit.list)
#Add admin units to the lists
for (i in 1:length(master.unit.list)){
  Any_pts <- sum(as.character(clean_TreatSeek_Any$IHME_location_id) == as.character(master.unit.list[i]))
  HMISfrac_pts <- sum(as.character(clean_TreatSeek_HMISfrac$IHME_location_id) == as.character(master.unit.list[i]))
  HMISfrac_span<-0
  Any_span<-0
  if (Any_pts == 0){units_no_Any_data <- c(units_no_Any_data, master.unit.list[i])}
  if (Any_pts > 0){units_with_Any_data <- c(units_with_Any_data, master.unit.list[i])}
  if (HMISfrac_pts == 0){units_no_HMISfrac_data <- c(units_no_HMISfrac_data, master.unit.list[i])}
  if (HMISfrac_pts > 0){units_with_HMISfrac_data <- c(units_with_HMISfrac_data, master.unit.list[i])}
}
#=============================================REGROUP IHME SUPER REGIONS=============================

# ===========For PTTIR onwards (regional temporal trends):
sig.temp.trends.any<-c("Indonesia","Kenya","Nigeria","Philippines")
sig.temp.trends.HMIS<- c("India","Indonesia","Kenya","Nigeria","Philippines") 

#Grouping Super Regions -Time Factor for Any treatment seeking
full_TreatSeek_n$Time_Factor <- as.character(full_TreatSeek_n$Country_Name)
full_TreatSeek_n$Time_Factor[!(full_TreatSeek_n$Country_Name %in% sig.temp.trends.any)] <- 'Other'
full_TreatSeek_n$Time_Factor[full_TreatSeek_n$Admin_Unit_Name %in% c("Gujarat","Arunachal Pradesh","Chhattisgarh","Yogyakarta","Karnataka","Nagaland")]<-"India"
full_TreatSeek_n$Time_Factor[full_TreatSeek_n$Admin_Unit_Name %in% c("Batangas","Cavite", "Laguna","Occidental Mindoro","Quezon","Rizal")]<-"Philippines2"
full_TreatSeek_n$Time_Factor[full_TreatSeek_n$Admin_Unit_Name %in% c("Balochistan")]<-"Other"
full_TreatSeek_n$Time_Factor[full_TreatSeek_n$Admin_Unit_Name %in% c("Bangka-Belitung Islands","Banten", "South Kalimantan","Central Kalimantan","East Kalimantan","Riau Islands", "East Java", "North Kalimantan", "Lampung", "Riau", "South Sumatra", "North Sumatra")]<-"Indonesia2"
full_TreatSeek_n$Time_Factor[full_TreatSeek_n$Country_Name %in%c("Ethiopia")] <- "Kenya"
full_TreatSeek_n$Time_Factor <- as.factor(full_TreatSeek_n$Time_Factor)
full_TreatSeek_n$Time_Factor <- relevel(full_TreatSeek_n$Time_Factor, ref = 'Other')

#Apply grouping 1 to the DHS dataset
reg_config<-unique(dplyr::select(full_TreatSeek_n, c(Country_Name, Time_Factor, Admin_Unit_Name)))
clean_TreatSeek_Any<-dplyr::left_join(clean_TreatSeek_Any, reg_config, by=c("Country_Name","Admin_Unit_Name"))

#Grouping Super Regions -Time Factor 2 for Public treatment seeking
full_TreatSeek_n$Time_Factor_2 <- as.character(full_TreatSeek_n$Country_Name)
full_TreatSeek_n$Time_Factor_2[!(full_TreatSeek_n$Country_Name %in% sig.temp.trends.HMIS)] <- 'Other'
full_TreatSeek_n$Time_Factor_2[full_TreatSeek_n$Country_Name=="Pakistan"]<-"India" #
full_TreatSeek_n$Time_Factor_2[full_TreatSeek_n$Admin_Unit_Name %in% c("Tarlac", "Pampanga", "Nueva Ecija", "Bulacan", "Bataan", "Aurora", "West Papua","Zambales")]<- "Philippines2"
full_TreatSeek_n$Time_Factor_2 <- as.factor(full_TreatSeek_n$Time_Factor_2)
full_TreatSeek_n$Time_Factor_2 <- relevel(full_TreatSeek_n$Time_Factor_2, ref = 'Other')
#Apply grouping 2 to the DHS dataset
reg_config<-unique(dplyr::select(full_TreatSeek_n, c(Country_Name, Time_Factor_2, Admin_Unit_Name)))
clean_TreatSeek_HMISfrac<-dplyr::left_join(clean_TreatSeek_HMISfrac, reg_config, by=c("Country_Name","Admin_Unit_Name"))


#============For PTTFR onwards (coarser regional factors):
sig.reg.Any<-unique(full_TreatSeek_n$Country_Name)
sig.reg.HMIS<-unique(full_TreatSeek_n$Country_Name)[!(unique(full_TreatSeek_n$Country_Name)%in% c("Mexico", "South Africa", "Brazil"))]

#Regional Factor for Any treatment seeking
full_TreatSeek_n$Reg_Factor <- as.character(full_TreatSeek_n$Country_Name)
full_TreatSeek_n$Reg_Factor[!(full_TreatSeek_n$Country_Name %in% sig.reg.Any)] <- 'Other'
full_TreatSeek_n$Reg_Factor[full_TreatSeek_n$Admin_Unit_Name %in% c("Mizoram","Assam","Nagaland", "Arunachal Pradesh", "Manipur", "Sikkim" ,"West Bengal","Vihiga")] <- "Other"
full_TreatSeek_n$Reg_Factor[full_TreatSeek_n$Admin_Unit_Name %in% c("Jharkhand","Telangana", "Tripura")] <- "India2"
full_TreatSeek_n$Reg_Factor <- as.factor(full_TreatSeek_n$Reg_Factor)
unique(full_TreatSeek_n$Reg_Factor)
full_TreatSeek_n$Reg_Factor <- relevel(full_TreatSeek_n$Reg_Factor, ref = 'Other')
#Apply grouping 1 to the DHS dataset
reg_config<-unique(dplyr::select(full_TreatSeek_n, c(Country_Name, Reg_Factor, Admin_Unit_Name)))
clean_TreatSeek_Any<-dplyr::left_join(clean_TreatSeek_Any, reg_config, by=c("Country_Name","Admin_Unit_Name"))

#Regional Factor 2 for Public treatment seeking
full_TreatSeek_n$Reg_Factor_2 <- as.character(full_TreatSeek_n$Country_Name)
full_TreatSeek_n$Reg_Factor_2[!(full_TreatSeek_n$Country_Name %in% sig.reg.HMIS)] <- 'Other'
full_TreatSeek_n$Reg_Factor_2 <- as.factor(full_TreatSeek_n$Reg_Factor_2)
unique(full_TreatSeek_n$Reg_Factor_2)
full_TreatSeek_n$Reg_Factor_2 <- relevel(full_TreatSeek_n$Reg_Factor_2, ref = 'Other')
#Apply grouping 2 to the DHS dataset
reg_config<-unique(dplyr::select(full_TreatSeek_n, c(Country_Name, Reg_Factor_2, Admin_Unit_Name)))
clean_TreatSeek_HMISfrac<-dplyr::left_join(clean_TreatSeek_HMISfrac, reg_config, by=c("Country_Name","Admin_Unit_Name"))

#Draft the formulas for GAMM
Any_covars<-c("LDI_pc","VIIRS_nighttime","IFD_coverage_prop")
HMIS_covars<-c("VIIRS_nighttime","healthcare_accessibility","hospital_beds_per1000","he_cap")

formula_any <-  logit_Any ~ -1 + Reg_Factor + s(Year, by = Time_Factor, k = 5)+s(LDI_pc, k=3)+s(VIIRS_nighttime, k=3)+s(IFD_coverage_prop, k=3)

formula_HMIS <- logit_HMIS ~ -1 + Reg_Factor_2 + s(Year, by = Time_Factor_2, k = 5)+s(VIIRS_nighttime, k=3)+s(healthcare_accessibility, k=3)+s(hospital_beds_per1000, k=3)+s(he_cap, k=3)

# Insert dummy true Year for plotting:
full_TreatSeek_n$t.Year <- full_TreatSeek_n$Year

# **

#Freeze non-linear temporal trend according to last available datapoint per Time Factor:
for(tf in unique(full_TreatSeek_n$Time_Factor)){
  ym<-min(clean_TreatSeek_Any$Year[clean_TreatSeek_Any$Time_Factor==tf])
  full_TreatSeek_n$Year[full_TreatSeek_n$Year<ym & full_TreatSeek_n$Time_Factor==tf]<-ym
}

full_TreatSeek_n$Year[full_TreatSeek_n$Year > 2019] <- 2019

# ------------ 1a. Explore the similarity of covariates for units without data ----------

# Function to check proportion of variance explained by significant covariates:

prop_var_plot <- function(prin_comp = region.pca1, plot.title = "Any TS Regional Trend Covariates"){
  std_dev <- prin_comp$sdev
  #compute variance
  pr_var <- std_dev^2
  #proportion of variance explained
  prop_varex <- pr_var/sum(pr_var)
  #scree plot
  plot(prop_varex, xlab = "Principal Component",
       ylab = "Proportion of Variance Explained",
       type = "b")
  title(plot.title)
}

full_TreatSeek_n$rf.unit <- full_TreatSeek_n$IHME_location_id
full_TreatSeek_n$rf.unit.pf <- full_TreatSeek_n$IHME_location_id
full_TreatSeek_n$trend.unit <- full_TreatSeek_n$IHME_location_id
full_TreatSeek_n$trend.unit.pf <- full_TreatSeek_n$IHME_location_id

ref_year_1 <- 1990; ref_year_2 <- 2005; ref_year_3 <- 2020




for (j in 1:length(master.region.list)){
  
  temp_region_cov <- full_TreatSeek_n[full_TreatSeek_n$Country_Name == as.character(master.region.list[j]), ]
  
  
  # a) Trend: Use changes between dynamic covariates for years 2000-2010 and 2010-2022.
  
  t_Any_region_cov <- temp_region_cov[temp_region_cov$Admin_Unit_Level == "ADMIN0" | (temp_region_cov$Admin_Unit_Level == "ADMIN1"), ]
  t_Any_region_cov_1 <- t_Any_region_cov[t_Any_region_cov$t.Year == ref_year_1, ]
  t_Any_region_cov_2 <- t_Any_region_cov[t_Any_region_cov$t.Year == ref_year_2, ]
  t_Any_region_cov_3 <- t_Any_region_cov[t_Any_region_cov$t.Year == ref_year_3, ]
  t_Any_region_no_data <- unique(t_Any_region_cov$IHME_location_id[t_Any_region_cov$IHME_location_id %in% c(units_no_Any_data)])
  
  t_HMISfrac_region_cov <- temp_region_cov[temp_region_cov$Admin_Unit_Level == "ADMIN0" | (temp_region_cov$Admin_Unit_Level == "ADMIN1"), ]
  t_HMISfrac_region_cov_1 <- t_HMISfrac_region_cov[t_HMISfrac_region_cov$t.Year == ref_year_1, ]
  t_HMISfrac_region_cov_2 <- t_HMISfrac_region_cov[t_HMISfrac_region_cov$t.Year == ref_year_2, ]
  t_HMISfrac_region_cov_3 <- t_HMISfrac_region_cov[t_HMISfrac_region_cov$t.Year == ref_year_3, ]
  t_HMISfrac_region_no_data <- unique(t_HMISfrac_region_cov$IHME_location_id[t_HMISfrac_region_cov$IHME_location_id %in% c(units_no_HMISfrac_data)])
  
  # Omit "DMSP_nighttime" because static.
  Any_covars_1<-Any_covars[!(Any_covars %in% c("DMSP_nighttime","accessibility","VIIRS_nighttime","healthcare_accessibility"))]
  temp_df1 <- cbind(t_Any_region_cov_2[, Any_covars_1] - t_Any_region_cov_1[, Any_covars_1], t_Any_region_cov_3[, Any_covars_1] - t_Any_region_cov_2[, Any_covars_1])
  region.pca1 <- prcomp(temp_df1, center = TRUE, scale. = TRUE)
  
  npc_1<-min(3, length(Any_covars_1))
  
  
  query_df1 <- region.pca1$x[t_Any_region_cov_1$IHME_location_id %in% c(units_no_Any_data), 1:npc_1]
  ref_df1 <- region.pca1$x[!(t_Any_region_cov_1$IHME_location_id %in% c(units_no_Any_data)), 1:npc_1]
  
  # Omit "VIIRS_nighttime" because static.
  HMIS_covars_1<-HMIS_covars[!(HMIS_covars %in% c("DMSP_nighttime","accessibility","VIIRS_nighttime","healthcare_accessibility"))]
  temp_df2 <- cbind(t_HMISfrac_region_cov_2[,HMIS_covars_1] - t_HMISfrac_region_cov_1[, HMIS_covars_1], t_HMISfrac_region_cov_3[, HMIS_covars_1] - t_HMISfrac_region_cov_2[, HMIS_covars_1])
  region.pca2 <- prcomp(temp_df2, center = TRUE, scale. = TRUE)
  
  npc_2<-min(3, length(HMIS_covars_1))
  
  query_df2 <- region.pca2$x[t_HMISfrac_region_cov_1$IHME_location_id %in% c(units_no_HMISfrac_data), 1:npc_2]
  ref_df2 <- region.pca2$x[!(t_HMISfrac_region_cov_1$IHME_location_id %in% c(units_no_HMISfrac_data)), 1:npc_2]
  
  # b) Random effect: Use significant covariates for years 2000, 2010 and 2019.
  
  rf_Any_region_cov <- temp_region_cov[temp_region_cov$Admin_Unit_Level == "ADMIN1", ]
   
  rf_Any_region_cov_1 <- rf_Any_region_cov[rf_Any_region_cov$t.Year == ref_year_1, ]
  rf_Any_region_cov_2 <- rf_Any_region_cov[rf_Any_region_cov$t.Year == ref_year_2, ]
  rf_Any_region_cov_3 <- rf_Any_region_cov[rf_Any_region_cov$t.Year == ref_year_3, ]
  rf_Any_region_no_data <- unique(rf_Any_region_cov$IHME_location_id[rf_Any_region_cov$IHME_location_id %in% units_no_Any_data])
  
  rf_HMISfrac_region_cov <- temp_region_cov[temp_region_cov$Admin_Unit_Level == "ADMIN1", ]
   
  rf_HMISfrac_region_cov_1 <- rf_HMISfrac_region_cov[rf_HMISfrac_region_cov$t.Year == ref_year_1, ]
  rf_HMISfrac_region_cov_2 <- rf_HMISfrac_region_cov[rf_HMISfrac_region_cov$t.Year == ref_year_2, ]
  rf_HMISfrac_region_cov_3 <- rf_HMISfrac_region_cov[rf_HMISfrac_region_cov$t.Year == ref_year_3, ]
  rf_HMISfrac_region_no_data <- unique(rf_HMISfrac_region_cov$IHME_location_id[rf_HMISfrac_region_cov$IHME_location_id %in% units_no_HMISfrac_data])
  
  temp_df3 <- cbind(rf_Any_region_cov_1[, Any_covars], rf_Any_region_cov_2[, Any_covars], rf_Any_region_cov_3[, Any_covars])
  region.pca3 <- prcomp(temp_df3, center = TRUE, scale. = TRUE)
  
  
  query_df3 <- region.pca3$x[rf_Any_region_cov_1$IHME_location_id %in% units_no_Any_data, 1:npc_1]
  ref_df3 <- region.pca3$x[!(rf_Any_region_cov_1$IHME_location_id %in% units_no_Any_data), 1:npc_1]
  
  temp_df4 <- cbind(rf_HMISfrac_region_cov_1[, HMIS_covars], rf_HMISfrac_region_cov_2[, HMIS_covars], rf_HMISfrac_region_cov_3[, HMIS_covars])
  region.pca4 <- prcomp(temp_df4, center = TRUE, scale. = TRUE)
  
  
  query_df4 <- region.pca4$x[rf_HMISfrac_region_cov_1$IHME_location_id %in% units_no_HMISfrac_data, 1:npc_2]
  ref_df4 <- region.pca4$x[!(rf_HMISfrac_region_cov_1$IHME_location_id %in% units_no_HMISfrac_data), 1:npc_2]
  
  ref_class_1 <- t_Any_region_cov_1[!(t_Any_region_cov_1$IHME_location_id %in% t_Any_region_no_data), c("IHME_location_id")]
  ref_class_2 <- t_HMISfrac_region_cov_1[!(t_HMISfrac_region_cov_1$IHME_location_id %in% t_HMISfrac_region_no_data), c("IHME_location_id")]
  ref_class_3 <- rf_Any_region_cov_1[!(rf_Any_region_cov_1$IHME_location_id %in% rf_Any_region_no_data), c("IHME_location_id")]
  ref_class_4 <- rf_HMISfrac_region_cov_1[!(rf_HMISfrac_region_cov_1$IHME_location_id %in% rf_HMISfrac_region_no_data), c("IHME_location_id")]
  
  
  
  neighbour_result_t_any <- knn1(test = query_df1, train = ref_df1, cl = ref_class_1)
  neighbour_result_t_pf <- knn1(test = query_df2, train = ref_df2, cl = ref_class_2)
  neighbour_result_rf_any <- knn1(test = query_df3, train = ref_df3, cl = ref_class_3)
  neighbour_result_rf_pf <- knn1(test = query_df4, train = ref_df4, cl = ref_class_4)
  
  for (i in 1:length(t_Any_region_no_data)){
    full_TreatSeek_n$trend.unit[full_TreatSeek_n$IHME_location_id == t_Any_region_no_data[i]] <- neighbour_result_t_any[i]
  }
  for (i in 1:length(t_HMISfrac_region_no_data)){
    full_TreatSeek_n$trend.unit.pf[full_TreatSeek_n$IHME_location_id == t_HMISfrac_region_no_data[i]] <- neighbour_result_t_pf[i]
  }
  for (i in 1:length(rf_Any_region_no_data)){
    full_TreatSeek_n$rf.unit[full_TreatSeek_n$IHME_location_id == rf_Any_region_no_data[i]] <- neighbour_result_rf_any[i]
  }
  for (i in 1:length(rf_HMISfrac_region_no_data)){
    full_TreatSeek_n$rf.unit.pf[full_TreatSeek_n$IHME_location_id == rf_HMISfrac_region_no_data[i]] <- neighbour_result_rf_pf[i]
  }
  
  pdf(paste(graphics.path, master.region.list[j], '.trend.rf.covariates.PCA_NN.pdf', sep = ""),width=12, height = 12)
  
  par(mfrow=c(2,2))
  
  plot(region.pca1$x[, 1:2], col = "white", main = "(a) Trend: Any Treatment Seeking", xlim = c(min(region.pca1$x[, 1])*1.5, max(region.pca1$x[, 1])*1.5), ylim = c(c(min(region.pca1$x[, 2])*1.5, max(region.pca1$x[, 2])*1.5)))
  text(region.pca1$x[, 1:2], labels = t_Any_region_cov_1$Admin_Unit_Name, col = ifelse(t_Any_region_cov_1$IHME_location_id %in% t_Any_region_no_data, "blue", "black"), cex = 0.7)
  
  plot(region.pca2$x[, 1:2], col = "white", main = "(b) Trend: Public Fractions", xlim = c(min(region.pca2$x[, 1])*1.5, max(region.pca2$x[, 1])*1.5), ylim = c(c(min(region.pca2$x[, 2])*1.5, max(region.pca2$x[, 2])*1.5)))
  text(region.pca2$x[, 1:2], labels = t_HMISfrac_region_cov_1$Admin_Unit_Name, col = ifelse(t_HMISfrac_region_cov_1$IHME_location_id %in% t_HMISfrac_region_no_data, "blue", "black"), cex = 0.7)
  
  plot(region.pca3$x[, 1:2], col = "white", main = "(c) Random Effect: Any Treatment Seeking", xlim = c(min(region.pca3$x[, 1])*1.5, max(region.pca3$x[, 1])*1.5), ylim = c(c(min(region.pca3$x[, 2])*1.5, max(region.pca3$x[, 2])*1.5)))
  text(region.pca3$x[, 1:2], labels = rf_Any_region_cov_1$Admin_Unit_Name, col = ifelse(rf_Any_region_cov_1$IHME_location_id %in% rf_Any_region_no_data, "blue", "black"), cex = 0.7)
  
  plot(region.pca4$x[, 1:2], col = "white", main = "(d) Random Effect: Public Fractions", xlim = c(min(region.pca4$x[, 1])*1.5, max(region.pca4$x[, 1])*1.5), ylim = c(c(min(region.pca4$x[, 2])*1.5, max(region.pca4$x[, 2])*1.5)))
  text(region.pca4$x[, 1:2], labels = rf_HMISfrac_region_cov_1$Admin_Unit_Name, col = ifelse(rf_HMISfrac_region_cov_1$IHME_location_id %in% rf_HMISfrac_region_no_data, "blue", "black"), cex = 0.7)
  
  dev.off()
  
  pdf(paste(graphics.path, master.region.list[j], '.trend.rf.covariates.PCA_propvar.pdf', sep = ""),width=12, height = 12)
  
  par(mfrow=c(2,2))
  
  prop_var_plot(prin_comp = region.pca1, plot.title = "(a) Trend: Any Treatment Seeking")
  prop_var_plot(prin_comp = region.pca2, plot.title = "(b) Trend: Public Fractions")
  prop_var_plot(prin_comp = region.pca3, plot.title = "(c) Random Effect: Any Treatment Seeking")
  prop_var_plot(prin_comp = region.pca4, plot.title = "(d) Random Effect: Public Fractions")
  
  dev.off()
  
  
  
}


# Check:

NN_results <- full_TreatSeek_n[full_TreatSeek_n$IHME_location_id %in% c(units_no_Any_data, units_no_HMISfrac_data) & full_TreatSeek_n$t.Year == 2019, c("ISO3", "Country_Name", "Admin_Unit_Name", "IHME_Super_Region_Name", "IHME_Region_Name", "trend.unit", "trend.unit.pf", "rf.unit", "rf.unit.pf")]

col.names <- c("trend.unit", "trend.unit.pf", "rf.unit", "rf.unit.pf")

for (j in col.names){
  NN_results[, j] <- as.character(NN_results[, j])
  temp.trend.unit <- as.character(unique(NN_results[, j]))
  for (i in 1:length(temp.trend.unit)){
    temp.name <- as.character(unique(full_TreatSeek_n$Admin_Unit_Name[full_TreatSeek_n$IHME_location_id == temp.trend.unit[i]]))
    NN_results[na.omit(NN_results[, j] == temp.trend.unit[i]), j] <- temp.name
  }  
}

head(NN_results)

write.csv(NN_results, file = paste(data.path, "trend_rf_NN.csv", sep = ""),row.names=FALSE)

# Implement matched trends:

switch_units <-unique(full_TreatSeek_n$IHME_location_id[full_TreatSeek_n$IHME_location_id != full_TreatSeek_n$trend.unit])
for (loc in switch_units){
  loc.trend<-as.character(unique(full_TreatSeek_n$trend.unit[full_TreatSeek_n$IHME_location_id==loc]))
  full_TreatSeek_n$Time_Factor[full_TreatSeek_n$IHME_location_id==loc] <-  unique(full_TreatSeek_n$Time_Factor[full_TreatSeek_n$IHME_location_id==loc.trend])
  clean_TreatSeek_Any$Time_Factor[clean_TreatSeek_Any$IHME_location_id==loc] <-  unique(clean_TreatSeek_Any$Time_Factor[clean_TreatSeek_Any$IHME_location_id==loc.trend])
  
}

switch_units_2 <-unique(full_TreatSeek_n$IHME_location_id[full_TreatSeek_n$IHME_location_id != full_TreatSeek_n$trend.unit.pf])
for (loc in switch_units_2){
  loc.trend<-as.character(unique(full_TreatSeek_n$trend.unit.pf[full_TreatSeek_n$IHME_location_id==loc]))
  full_TreatSeek_n$Time_Factor_2[full_TreatSeek_n$IHME_location_id==loc] <-  unique(full_TreatSeek_n$Time_Factor_2[full_TreatSeek_n$IHME_location_id==loc.trend])
  clean_TreatSeek_HMISfrac$Time_Factor_2[clean_TreatSeek_HMISfrac$IHME_location_id==loc] <-  unique(clean_TreatSeek_HMISfrac$Time_Factor_2[clean_TreatSeek_HMISfrac$IHME_location_id==loc.trend])
  
}





# ----------------------------------------------- Fit the models...
# Apply data outliering and post-1990 restriction:

Any_data <- clean_TreatSeek_Any
HMIS_data <- clean_TreatSeek_HMISfrac
Any_data <- Any_data[Any_data$Year > 1989, ]
HMIS_data <- HMIS_data[HMIS_data$Year > 1989, ]

Any_data$IHME_location_id <- as.factor(Any_data$IHME_location_id)
HMIS_data$IHME_location_id <- as.factor(HMIS_data$IHME_location_id)

Any_data[Any_data$ISO3=="BRA",]
# 3. PREF:
Any_model <-  gamm(formula_any, data = Any_data, random = list(IHME_location_id = ~ 1)) 
HMIS_model <- gamm(formula_HMIS, data = HMIS_data, random = list(IHME_location_id = ~ 1)) 
#control function - uncertainty in the covariates
summary(Any_model$gam)
summary(HMIS_model$gam)

# ------------ 1b. Ignore the variability in Any_Treat, HMIS_Treat and the IHME covariates -----------

Any_rf <- rep(NA, nrow(full_TreatSeek_n))

full_TreatSeek_n$rf.unit<-as.character(full_TreatSeek_n$rf.unit)

for (i in 1:length(units_with_Any_data)){
  Any_rf[full_TreatSeek_n$rf.unit == as.character(units_with_Any_data[i])] <-ifelse(is.na(ranef(Any_model$lme,level= length(ranef(Any_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(Any_model$lme))-1))), units_with_Any_data[i], sep = ""), ]), 0, ranef(Any_model$lme,level= length(ranef(Any_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(Any_model$lme))-1))), units_with_Any_data[i], sep = ""), ])
} 


Any_rf[is.na(Any_rf)] <- 0
# check: Storing random effect values 
sum(Any_rf == 0) 


HMIS_rf <- rep(NA, nrow(full_TreatSeek_n))

for (i in 1:length(units_with_HMISfrac_data)){
  HMIS_rf[full_TreatSeek_n$rf.unit.pf == as.character(units_with_HMISfrac_data[i])] <-ifelse(is.na(ranef(HMIS_model$lme,level= length(ranef(HMIS_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(HMIS_model$lme))-1))), units_with_HMISfrac_data[i], sep = ""), ]), 0, ranef(HMIS_model$lme,level= length(ranef(HMIS_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(HMIS_model$lme))-1))),units_with_HMISfrac_data[i], sep = ""), ])
} 


HMIS_rf[is.na(HMIS_rf)] <- 0

# check: Storing random effect values 
sum(HMIS_rf == 0) 

Any_pred_initial <- predict(Any_model, newdata = full_TreatSeek_n, se.fit = TRUE)
HMIS_pred_initial <- predict(HMIS_model, newdata = full_TreatSeek_n, se.fit = TRUE)

full_TreatSeek_n$Any_pred_initial <- logitlink(Any_pred_initial$fit + Any_rf, inverse=TRUE)
full_TreatSeek_n$Any_pred_initial_low <- logitlink(Any_pred_initial$fit + Any_rf + qnorm(0.025)*Any_pred_initial$se.fit,inverse=TRUE)
full_TreatSeek_n$Any_pred_initial_high <- logitlink(Any_pred_initial$fit + Any_rf + qnorm(0.975)*Any_pred_initial$se.fit,inverse=TRUE)

full_TreatSeek_n$HMIS_pred_initial <- full_TreatSeek_n$Any_pred_initial*logitlink(HMIS_pred_initial$fit + HMIS_rf,inverse=TRUE)
full_TreatSeek_n$HMIS_pred_initial_low <- full_TreatSeek_n$Any_pred_initial_low*logitlink(HMIS_pred_initial$fit + HMIS_rf + qnorm(0.025)*HMIS_pred_initial$se.fit,inverse=TRUE)
full_TreatSeek_n$HMIS_pred_initial_high <- full_TreatSeek_n$Any_pred_initial_high*logitlink(HMIS_pred_initial$fit + HMIS_rf + qnorm(0.975)*HMIS_pred_initial$se.fit,inverse=TRUE)

full_TreatSeek_n$HMISfrac_pred_initial<-logitlink(HMIS_pred_initial$fit + HMIS_rf,inverse=TRUE)
full_TreatSeek_n$HMISfrac_pred_initial_low<-logitlink(HMIS_pred_initial$fit + HMIS_rf+ qnorm(0.025)*HMIS_pred_initial$se.fit,inverse=TRUE)
full_TreatSeek_n$HMISfrac_pred_initial_high<-logitlink(HMIS_pred_initial$fit + HMIS_rf+ qnorm(0.975)*HMIS_pred_initial$se.fit,inverse=TRUE)

# ------------ 2. Consider the variability in Any_Treat, HMIS_Treat and the IHME covariates -----------

load(file = paste(data.path, 'TS_datasets_for_prediction.RData', sep = ''))
load(file = paste(data.path, 'TS_datasets_input_Any.RData', sep = ''))
load(file = paste(data.path, 'TS_datasets_input_HMIS.RData', sep = ''))
N_TS <- 100

## For each of the 100 datasets, fit the chosen models and generate 100 prediction sets.

Any_pred_list <- rep(list(NA), N_TS)
HMIS_pred_list <- rep(list(NA), N_TS)

names(Any_pred_list) <- paste("Dataset", 1:N_TS, sep = "_")
names(HMIS_pred_list) <- paste("Dataset", 1:N_TS, sep = "_")

ref_error <- NA

temp_time <- proc.time()[3]

TS_groups<-unique(dplyr::select(full_TreatSeek_n, c("IHME_location_id", "Time_Factor", "Time_Factor_2", "Reg_Factor","Reg_Factor_2")))
TS_groups$IHME_location_id<-as.numeric(as.character(TS_groups$IHME_location_id))


## Apply lower and upper bounds for the numeric covariates which were simulated.
for (TS_j in 1:N_TS){
  prediction_dataset<-dataset<-TS_datasets[[TS_j]]
  dataset_Any <- TS_clean_datasets_Any[[TS_j]]
  dataset_HMIS <- TS_clean_datasets_HMIS[[TS_j]]
  
  prediction_dataset<-prediction_dataset[!prediction_dataset$Country_Name %in% c("Iran", "China"),]
  
  ###how are the datasets different
  
  dataset<-left_join(dataset_Any,dataset_HMIS, by=c("IHME_location_id", "Year", "Admin_Unit_Name"))
  
  check<-dataset[is.na(dataset$HMIS_treat),]
  
  dataset<-left_join(dataset, prediction_dataset, by=c("IHME_location_id", "Year", "Admin_Unit_Name"))
  
  
  dataset$Any_treat[dataset$Any_treat>1] <- 0.999 # If Any TS > 1 due to random simulations, set upper bound of public frac to be 0.999.
  dataset$Any_treat[dataset$Any_treat<0.001] <- 0.001 # If Any TS <0 due to random simulations, set it to 0.001.
  dataset$logit_Any <- logitlink(dataset$Any_treat,inverse=FALSE)
  
  
  dataset$HMIS_treat[dataset$HMIS_treat>1] <- 0.999 # If HMIS TS > 1 due to random simulations, set upper bound of public frac to be 0.999.
  dataset$HMIS_treat[dataset$HMIS_treat<0.001] <- 0.001 # If HMIS TS <0 due to random simulations, set it to 0.001.
  dataset$logit_HMIS <- logitlink(dataset$HMIS_treat,inverse=FALSE)
  
  
  temp_HMIS_frac <- dataset$HMIS_treat/dataset$Any_treat
  temp_HMIS_frac[temp_HMIS_frac>1] <- 0.999 # If public TS > Any TS due to random simulations, set upper bound of public frac to be 0.999.
  temp_HMIS_frac[temp_HMIS_frac<0.001] <- 0.001
  dataset$logit_HMIS <- logitlink(temp_HMIS_frac,inverse=FALSE)
  
  #  Remove name attributes and set Year as a numeric covariate:
  dataset$Year <- as.numeric(dataset$Year)
  prediction_dataset$Year<-as.numeric(prediction_dataset$Year)
  
  
  
  # For PTTIR onwards (regional temporal trends):
  dataset<-dplyr::left_join(dataset, TS_groups, by="IHME_location_id")
  prediction_dataset<-dplyr::left_join(prediction_dataset, TS_groups, by="IHME_location_id")
  
  
  # For PTTFR onwards (coarser regional factors):
  
  dataset$Country_Name  <- as.factor(dataset$Country_Name)
  dataset$IHME_location_id  <- as.factor(dataset$IHME_location_id)
  
  prediction_dataset$Country_Name  <- as.factor(prediction_dataset$Country_Name)
  prediction_dataset$IHME_location_id  <- as.factor(prediction_dataset$IHME_location_id)
  
  #  Option 1: Fit models on cleaned data and store 100 predictions for full data set:
  
  clean_Any <- dataset[!is.na(dataset$logit_Any) & is.finite(dataset$logit_Any), ]
  
  clean_HMIS <- dataset[!is.na(dataset$logit_HMIS) & is.finite(dataset$logit_HMIS), ]

  Any_outliers <- c(which(clean_Any$ISO3 == "NGA" & clean_Any$Year == 2008))
  HMIS_outliers <- c(which(clean_HMIS$ISO3 == "NGA" & clean_HMIS$Year == 2008)) 
  
  Any_data <- clean_Any[-Any_outliers, ]
  HMIS_data <- clean_HMIS[-HMIS_outliers, ]
  
  Any_data<-clean_Any
  HMIS_data<-clean_HMIS
  
  Any_data <- Any_data[Any_data$Year > 1989, ]
  HMIS_data <- HMIS_data[HMIS_data$Year > 1989, ]

  
  Any_data$Admin_Unit_Name <- as.factor(Any_data$Admin_Unit_Name)
  HMIS_data$Admin_Unit_Name <- as.factor(HMIS_data$Admin_Unit_Name)
  
  # Refit the models on simulated datasets:
  
  Any_fit <- try(gamm(formula_any, data = Any_data, random = list(IHME_location_id = ~ 1)))
  if("try-error" %in% class(Any_fit)){
    Any_fit <-  gamm(formula_any, data = Any_data, random = list(IHME_location_id = ~ 1), control = list(niterEM=1,opt='optim', maxit = 500)) # list(niterEM=1, opt='optim', maxit = 500)
  }
  
  HMIS_fit <- try(gamm(formula_HMIS, data = HMIS_data, random = list(IHME_location_id = ~ 1)))
  if("try-error" %in% class(HMIS_fit)){
    HMIS_fit <- try(gamm(formula_HMIS, data = HMIS_data, random = list(IHME_location_id = ~ 1), control = list(opt='optim', maxit = 500)))
  }
  
  # Option 2: Use original fitted model:

  Any_model<-Any_fit
  HMIS_model<-HMIS_fit
  
  Any_pred <- matrix(NA, nrow = nrow(prediction_dataset), ncol = 100)
  HMIS_pred <- matrix(NA, nrow = nrow(prediction_dataset), ncol = 100)
  
  
  #Freeze non-linear temporal trend according to last available datapoint per Time Factor:
  for(tf in unique(prediction_dataset$Time_Factor)){
    ym<-min(dataset$Year[dataset$Time_Factor==tf])
    prediction_dataset$Year[prediction_dataset$Year<ym & prediction_dataset$Time_Factor==tf]<-ym
  }
  # Freeze non-linear temporal trend post 2019::
  prediction_dataset$Year[prediction_dataset$Year > 2019] <- 2019
  
  
  Any_pred_distr <- predict(Any_fit, newdata = prediction_dataset, se.fit = TRUE)
  HMIS_pred_distr <- predict(HMIS_fit, newdata = prediction_dataset, se.fit = TRUE)
  
  Any_rf <- rep(NA, nrow(full_TreatSeek_n))

  for (i in 1:length(units_with_Any_data)){
    Any_rf[full_TreatSeek_n$rf.unit == as.character(units_with_Any_data[i])] <-ifelse(is.na(ranef(Any_model$lme,level= length(ranef(Any_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(Any_model$lme))-1))), units_with_Any_data[i], sep = ""), ]), 0, ranef(Any_model$lme,level= length(ranef(Any_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(Any_model$lme))-1))), units_with_Any_data[i], sep = ""), ])
  } 

  Any_rf[is.na(Any_rf)] <- 0
  
  check<-full_TreatSeek_n[which(Any_rf==0),]
  
  HMIS_rf <- rep(NA, nrow(full_TreatSeek_n))

  for (i in 1:length(units_with_HMISfrac_data)){
    HMIS_rf[full_TreatSeek_n$rf.unit.pf == as.character(units_with_HMISfrac_data[i])] <-ifelse(is.na(ranef(HMIS_model$lme,level= length(ranef(HMIS_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(HMIS_model$lme))-1))), units_with_HMISfrac_data[i], sep = ""), ]), 0, ranef(HMIS_model$lme,level= length(ranef(HMIS_model$lme)))[paste(str_flatten(rep("1/", (length(ranef(HMIS_model$lme))-1))),units_with_HMISfrac_data[i], sep = ""), ])
  } 

  HMIS_rf[is.na(HMIS_rf)] <- 0
  
  check2<-full_TreatSeek_n[which(HMIS_rf==0),]
  
  for (i in 1:100){
    set.seed(i)
    Any_pred[, i] <- rnorm(nrow(prediction_dataset), mean = (Any_pred_distr$fit + Any_rf), sd = Any_pred_distr$se.fit)
    HMIS_pred[, i] <- rnorm(nrow(prediction_dataset), mean = (HMIS_pred_distr$fit + HMIS_rf), sd = HMIS_pred_distr$se.fit)
  }
  Any_pred_list[[TS_j]] <- Any_pred
  HMIS_pred_list[[TS_j]] <- HMIS_pred
  
  if((sum(Any_rf == 0) + sum(HMIS_rf == 0))==0){
    print(paste("Dataset ", TS_j, " done.", sep = ""))
  }else{
    ref_error <- c(ref_error, TS_j)
    print(paste("Dataset ", TS_j, ": Random effect matching error.", sep = ""))}
  
}

time.taken <- proc.time()[3] - temp_time

#finished editing here.

full_Any_pred <- do.call(cbind, Any_pred_list)
full_HMIS_pred <- do.call(cbind, HMIS_pred_list)

full_Any_pred_raw <- logitlink(full_Any_pred,inverse=TRUE)
full_HMIS_pred_raw <- logitlink(full_HMIS_pred,inverse=TRUE)

# Compute mean and 95% confidence intervals:

full_TreatSeek_n$Any_pred <- rowMeans(full_Any_pred_raw)
full_TreatSeek_n$Any_pred_low <- apply(full_Any_pred_raw, MARGIN = 1, function(x){quantile(x, probs = 0.025)})
full_TreatSeek_n$Any_pred_high <- apply(full_Any_pred_raw, MARGIN = 1, function(x){quantile(x, probs = 0.975)})

full_TreatSeek_n$HMIS_pred <- rowMeans(full_HMIS_pred_raw*full_Any_pred_raw)
full_TreatSeek_n$HMIS_pred_low <- apply(full_HMIS_pred_raw*full_Any_pred_raw, MARGIN = 1, function(x){quantile(x, probs = 0.025)})
full_TreatSeek_n$HMIS_pred_high <- apply(full_HMIS_pred_raw*full_Any_pred_raw, MARGIN = 1, function(x){quantile(x, probs = 0.975)})

full_TreatSeek_n$HMISfrac_pred <- rowMeans(full_HMIS_pred_raw)
full_TreatSeek_n$HMISfrac_pred_low <- apply(full_HMIS_pred_raw, MARGIN = 1, function(x){quantile(x, probs = 0.025)})
full_TreatSeek_n$HMISfrac_pred_high <- apply(full_HMIS_pred_raw, MARGIN = 1, function(x){quantile(x, probs = 0.975)})


head(full_TreatSeek_n)

master.region.list <- unique(full_TreatSeek_n$Country_Name) 
master.unit.list <- unique(full_TreatSeek_n$Admin_Unit_Name)

#add private TS


for(i in 1:nrow(full_TreatSeek_n)){
  mean_any<-full_TreatSeek_n$Any_pred[i]
  lower_any<-full_TreatSeek_n$Any_pred_low[i]
  upper_any<-full_TreatSeek_n$Any_pred_high[i]
  
  mean_hmis<-full_TreatSeek_n$HMIS_pred[i]
  lower_hmis<-full_TreatSeek_n$HMIS_pred_low[i]
  upper_hmis<-full_TreatSeek_n$HMIS_pred_high[i]
  
  rany<-rnorm(100000, mean=mean_any, sd=(upper_any-lower_any)/3.92) # assuming that 95% CI spans 1.96*2 sd
  rhmis<-rnorm(100000, mean=mean_hmis, sd=(upper_hmis-lower_hmis)/3.92)
  
  priv<-rany-rhmis
  
  mean_priv<-mean(priv)
  upper_priv<-mean_priv+1.96*sd(priv)
  lower_priv<-mean_priv-1.96*sd(priv)
  
  full_TreatSeek_n$Private_pred[i]<-mean_priv
  full_TreatSeek_n$Private_pred_high[i]<-upper_priv
  full_TreatSeek_n$Private_pred_low[i]<-lower_priv
  
}

#  Save the results:

write.csv(full_TreatSeek_n, file = paste(data.path, 'TS_predictions_GAMkNN.csv', sep = ''),row.names=FALSE) # For Option 1: With the uncertainty in the model fit considered.


# 9-Rake_subnational_over_national-Final_predictions.R


rm(list=ls())
gc()

run <- '20240807'
=======
nat<-read.csv("FILEPATH/TS_predictions_GAMkNN.csv")
sub<-read.csv("FILEPATH/TS_predictions_GAMkNN.csv")
sub<-sub[sub$t.Year %in% unique(nat$t.Year),]
pop<-read.csv("FILEPATH/ihme_populations.csv")

##filter pop to 12 subnationally modelled countries, minus China and Iran (they have no data so we just take the national estimates for the subnational units.)
pop_add<-pop[pop$admin_unit_level=="ADMIN1" & pop$iso3 %in% c("HKG", "MAC","CHN","IRN") & pop$age_bin=="All_Ages" & pop$year %in% unique(nat$t.Year), ]
pop<-pop[pop$admin_unit_level=="ADMIN1" & !(pop$iso3 %in% c("HKG", "MAC","CHN","IRN"))& pop$age_bin=="All_Ages" & pop$year %in% unique(nat$t.Year) ,]

SMT_ID<-unique(pop$ihme_id[pop$admin_unit_name=="The Six Minor Territories"])

length(unique(pop$iso3))==10
pop<-dplyr::select(pop, c("ihme_id", "year", "total_pop"))

names(pop)[1:2]<-c("IHME_location_id", "t.Year")

nat.vals<-dplyr::select(nat, c("ISO3","t.Year", "Any_pred", "HMIS_pred", "Private_pred"))

sub<-left_join(sub, pop, by=c("IHME_location_id", "t.Year"))

sub<-sub[order(sub$t.Year),]

iso_list<-unique(sub$ISO3)
years<-unique(nat$t.Year)

#assume the population for the SMT units is the total SMT pop divided by 6.
sub$total_pop[sub$SMT_Factor=="SMT"]<-SMT_ID/6


for(iso in iso_list){
  for(y in years){
    #first, get all the national estimates
    nat.iy<-nat[nat$ISO3==iso & nat$t.Year==y, ]
    
    nat.val.Any<-nat.iy$Any_pred
    nat.val.Any.low<-nat.iy$Any_pred_low
    nat.val.Any.high<-nat.iy$Any_pred_high
    
    nat.val.HMIS<-nat.iy$HMIS_pred
    nat.val.HMIS.low<-nat.iy$HMIS_pred_low
    nat.val.HMIS.high<-nat.iy$HMIS_pred_high
    
    nat.val.Private<-nat.iy$Private_pred
    nat.val.Private.low<-nat.iy$Private_pred_low
    nat.val.Private.high<-nat.iy$Private_pred_high
    
    #Get the national combined values from the subnational estimates
    sub.iy<-sub[sub$ISO3==iso & sub$t.Year==y, ]
    tot.pop.iy<-sum(sub.iy$total_pop)
    
    nat.val.Any.2<-sum(sub.iy$total_pop*sub.iy$Any_pred)/tot.pop.iy
    nat.val.Any.low.2<-sum(sub.iy$total_pop*sub.iy$Any_pred_low)/tot.pop.iy
    nat.val.Any.high.2<-sum(sub.iy$total_pop*sub.iy$Any_pred_high)/tot.pop.iy
    
    nat.val.HMIS.2<-sum(sub.iy$total_pop*sub.iy$HMIS_pred)/tot.pop.iy
    nat.val.HMIS.low.2<-sum(sub.iy$total_pop*sub.iy$HMIS_pred_low)/tot.pop.iy
    nat.val.HMIS.high.2<-sum(sub.iy$total_pop*sub.iy$HMIS_pred_high)/tot.pop.iy
    
    nat.val.Private.2<-sum(sub.iy$total_pop*sub.iy$Private_pred)/tot.pop.iy
    nat.val.Private.low.2<-sum(sub.iy$total_pop*sub.iy$Private_pred_low)/tot.pop.iy
    nat.val.Private.high.2<-sum(sub.iy$total_pop*sub.iy$Private_pred_high)/tot.pop.iy
    
    #calculate the scaling factors
    sf.Any<-nat.val.Any/nat.val.Any.2
    ci.Any<-(nat.val.Any.high-nat.val.Any.low)/2
    
    sf.HMIS<-nat.val.HMIS/nat.val.HMIS.2
    ci.HMIS<-(nat.val.HMIS.high-nat.val.HMIS.low)/2
    
    sf.Private<-nat.val.Private/nat.val.Private.2
    ci.Private<-(nat.val.Private.high-nat.val.Private.low)/2
    
    #apply the scaling factor to subnational results
    sub$Any_pred[sub$ISO3==iso & sub$t.Year==y]<-sub$Any_pred[sub$ISO3==iso & sub$t.Year==y]*sf.Any
    sub$Any_pred_low[sub$ISO3==iso & sub$t.Year==y]<-sub$Any_pred[sub$ISO3==iso & sub$t.Year==y]-ci.Any
    sub$Any_pred_high[sub$ISO3==iso & sub$t.Year==y]<-sub$Any_pred[sub$ISO3==iso & sub$t.Year==y]+ci.Any
    
    
    sub$HMIS_pred[sub$ISO3==iso & sub$t.Year==y]<-sub$HMIS_pred[sub$ISO3==iso & sub$t.Year==y]*sf.HMIS
    sub$HMIS_pred_low[sub$ISO3==iso & sub$t.Year==y]<-sub$HMIS_pred[sub$ISO3==iso & sub$t.Year==y]-ci.HMIS
    sub$HMIS_pred_high[sub$ISO3==iso & sub$t.Year==y]<-sub$HMIS_pred[sub$ISO3==iso & sub$t.Year==y]+ci.HMIS
    
    sub$Private_pred[sub$ISO3==iso & sub$t.Year==y]<-sub$Private_pred[sub$ISO3==iso & sub$t.Year==y]*sf.Private
    sub$Private_pred_low[sub$ISO3==iso & sub$t.Year==y]<-sub$Private_pred[sub$ISO3==iso & sub$t.Year==y]-ci.Private
    sub$Private_pred_high[sub$ISO3==iso & sub$t.Year==y]<-sub$Private_pred[sub$ISO3==iso & sub$t.Year==y]+ci.Private
    
  }
}


##check for errors
check1<-sub[is.na(sub$HMIS_pred),]
check2<-sub[sub$HMIS_pred>sub$HMIS_pred_high | sub$HMIS_pred<sub$HMIS_pred_low | sub$Any_pred>sub$Any_pred_high | sub$Any_pred<sub$Any_pred_low,]


#Combine the national and subnational table
sub<-dplyr::select(sub, -c("total_pop","SMT_Factor"))

TS<-rbind(nat, sub)

#Add China and Iran sub-national units
add_units<-unique(dplyr::select(pop_add, c("iso3", "country_name","admin_unit_level","ihme_id", "admin_unit_name")))

for(i in 1:nrow(add_units)){
  unit<-add_units[i,]
  iso<-unit$iso3
  if(iso %in% c("HKG", "MAC")){iso<-"CHN"}
  new.rows<-nat[nat$ISO3==iso,]
  new.rows$IHME_location_id<-unit$ihme_id
  new.rows$Admin_Unit_Level<-unit$admin_unit_level
  new.rows$Admin_Unit_Name<-unit$admin_unit_name
  
  TS<-rbind(TS, new.rows)
  
}


###sort by year
TS.fin<-TS[order(TS$t.Year),]

TS.fin<-dplyr::select(TS.fin, -"Year")

names(TS.fin)[which(names(TS.fin)=="t.Year")]<-"Year"

#Save

Output_Path <- paste0(FILEPATH)
dir.create(Output_Path)
Output_Path <- paste0(Output_Path, '/TS_predictions_GAMkNN.csv')
write.csv(TS.fin, Output_Path, row.names=FALSE)
write.csv(TS.fin, "FILEPATH/TS_predictions_GAMkNN.csv", row.names=FALSE)




