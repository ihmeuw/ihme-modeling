##########################################################################################
### Project: Neonatal Sepsis and Other Neonatal Infections
### Purpose: Process and prep sepsis data, conduct age/sex splitting, and apply crosswalks
###########################################################################################

rm(list=ls())
os <- .Platform$OS.type
if (os=="windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "FILEPATH"
}

pacman::p_load(ggplot2, data.table, magrittr, dplyr, splitstackshape, msm, openxlsx, tidyr, matrixStats)
library(crosswalk, lib.loc = "FILEPATH")
locs <- fread("FILEPATH/locs.csv")


## Set options
bundle <- "neonatal_sepsis"  
decomp_step <- "iterative"
new_bundle_version <- T 
xwalk_version_description <- "" 

## Source central functions
setwd("FILEPATH")
functions <- list.files()
lapply(functions, source)


## User-defined functions
setwd("FILEPATH")
# Prep clinical data from prevalence to incidence
source("02_prep_clinical_data.R")
# Sex-Split
source("03_sex_split.R")
# Age-Split
source("04_age_split.R")
# Crosswalk
source("05_crosswalk.R")

###############
##### RUN #####
###############

## Save bundle version
if (new_bundle_version){
  # load bundle data
  bundle_version <- save_bundle_version(92, decomp_step, 7, include_clinical=c("inpatient","claims, inpatient only"))  
  # write out bundle version id and save
  df <- fread("FILEPATH/bundle_versions.csv")
  df[bundle==bundle]$is_best <- 0
  dt   <- data.frame("bundle"            = bundle,
                     "bundle_version_id" = bundle_version$bundle_version_id,
                     "note"              = xwalk_version_description,
                     "is_best"           = 1)
  df <- rbind(df,dt,fill=T)
  write.csv(df,"FILEPATH/bundle_versions.csv", row.names=F)
  # get bundle version
  data <- get_bundle_version(bundle_version$bundle_version_id,fetch="all") 
} else {
  bundle_version <- fread("FILEPATH/bundle_versions.csv")[bundle=="neonatal_sepsis"&is_best==1] 
  data <- get_bundle_version(bundle_version$bundle_version_id,fetch="all") 
}

## Hospital data prep
emr_data <- data[measure=="mtexcess"]
data <- data[measure!="mtexcess"]
data <- prep_clinical_data(data, aggregate_under1=TRUE)

## Sex Split
data <- sex_split(data)

## Write-out for xwalk upload
write.xlsx(data,paste0("FILEPATH.xlsx"), sheetName="extraction")

#upload data to CW version in DB
xwalk <- save_crosswalk_version(bundle_version$bundle_version_id, 
                       data_filepath=paste0("FILEPATH.xlsx"), 
                       description="DESC")

# Pull xwalk version and prep for ST-GPR Bundle Upload
data <- get_crosswalk_version(xwalk$crosswalk_version_id)

## Age split data
data <- age_split(data)
data[age_start==0.019, age_start:=0.01917808]
data[age_end==0.019, age_end:=0.01917808]
data[age_end==0.076, age_end:=0.07671233]

#identify crosswalk matches
df <- copy(data)
df <- prep_data(df)
df <- match_data(df)

#run MRBRT for crosswalk
data <- run_mrbrt(matched_df=df, data=data)

# Set Age Group IDs/Prep ST-GPR Bundle Upload
data[age_start==0, age_group_id:=2]
data[age_start==0.01917808, age_group_id:=3]
data[,measure:="continuous"]
setnames(data,"mean","val")
data[,variance:=standard_error^2]

# Clear existing ST-GPR Bundle
clear <- get_bundle_data(8060,"iterative",7)
clear <- clear[,.(seq)]
write.xlsx(clear,"FILEPATH.xlsx",sheetName="extraction")
upload_bundle_data(8060,"iterative","FILEPATH.xlsx",7)

# Upload ST-GPR Bundle
data[,`:=`(seq="", crosswalk_parent_seq="",year_id=floor((year_start+year_end)/2))]
data[,`:=`(year_start=year_id, year_end=year_id)]
write.xlsx(data,"FILEPATH.xlsx",sheetName="extraction")
upload_bundle_data(8060,"iterative","FILEPATH.xlsx",7)

# Save Bundle Version
bvid <- save_bundle_version(8060,"iterative",7)

# Pull Bundle Version and Process
df <- get_bundle_version(bvid$bundle_version_id,fetch="all")

# Outliering
df[,is_outlier:=0]

# Remove ANISA
df <- df[!nid==447741]
# Outlier 2018 Poland subnat data
df[ihme_loc_id %like% "POL" & year_id==2018, is_outlier:=1]
# Outlier Lithuania
df[ihme_loc_id=="LTU", is_outlier:=1]
# Outlier USA National
df[ihme_loc_id=="USA", is_outlier:=1]
# Outlier USA subnats in 2015 and 2005
df[ihme_loc_id %like% "USA" & year_id==2005, is_outlier:=1]
df[ihme_loc_id %like% "USA" & year_id==2015, is_outlier:=1]
# Outlier BRA data
df[ihme_loc_id %like% "BRA", is_outlier:=1]
# Outlier Portugal and Spain
df[ihme_loc_id=="PRT", is_outlier:=1]
df[ihme_loc_id=="ESP", is_outlier:=1]
# Un-Outier PHL and VTN
df[ihme_loc_id %like% "PHL", is_outlier:=0]
df[ihme_loc_id=="VNM", is_outlier:=0]
# Unoutlier JPN
df[ihme_loc_id %like% "JPN", is_outlier:=0]
# Unoutlier NOR
df[ihme_loc_id %like% "NOR", is_outlier:=0]
# Unoutlier ITA
df[ihme_loc_id %like% "ITA", is_outlier:=0]
# Unoutlier GBR
df[ihme_loc_id %like% "GBR", is_outlier:=0]
# Outlier Hawaii 2016 females late neonatal
df[location_id==534 & year_id==2016 & sex=="Female" & age_group_id==3, is_outlier:=1]
# Outlier Vermont female 2015 ENN and LNN
df[location_id==568 & year_id==2015 & sex=="Female", is_outlier:=1]
# Outlier Wyoming 2015 males and females ENN and LNN
df[location_id==573 & year_id==2015, is_outlier:=1]
# Other outliers
df[ihme_loc_id=="ITA_35508" & sex=="Male" & age_group_id==3 & year_id==2020, is_outlier:=1]
df[ihme_loc_id=="ITA_35495" & sex=="Male" & age_group_id==3 & year_id==2005, is_outlier:=1]
df[ihme_loc_id=="ITA_35495" & sex=="Female" & age_group_id==2 & year_id==2015, is_outlier:=1]
df[ihme_loc_id=="GBR_4636" & sex=="Male" & year_id==2009 & age_group_id==2, is_outlier:=1]
df[ihme_loc_id=="POL", is_outlier:=1]
df[ihme_loc_id=="ISL", is_outlier:=1]
df[ihme_loc_id %like% "MEX" & year_id==2000, is_outlier:=1]
df[ihme_loc_id=="HRV", is_outlier:=1]
df[ihme_loc_id=="FIN", is_outlier:=1]
df[ihme_loc_id=="SWE", is_outlier:=1]
df[ihme_loc_id=="NPL", is_outlier:=1]
df[ihme_loc_id=="PHL" & cv_hospital==1, is_outlier:=1]
df[ihme_loc_id=="TUR", is_outlier:=1]

# Outlier NAME
df$location_name <- NULL
df <- merge(df,locs[,.(location_id,location_name,super_region_name)],by="location_id",all.x=TRUE)
unique(df[super_region_name=="North Africa and Middle East",location_name])
df[super_region_name=="North Africa and Middle East",is_outlier:=1]
# Outlier based on SE
df[, standard_error := sqrt(variance)]
df[standard_error > 20*val, is_outlier:=1]

df[,`:=`(upper=NA,lower=NA)]
df[,crosswalk_parent_seq:=seq]
df[,seq:=""]
write.xlsx(df,"FILEPATH.xlsx",sheetName="extraction")

# Upload to Crosswalk Version
xwalk <- save_crosswalk_version(bvid$bundle_version_id,
                                data_filepath="FILEPATH.xlsx",
                                description=xwalk_version_description)
