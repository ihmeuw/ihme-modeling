### NTDs Dengue
#import all-age total case notifications and append age-specific data (extracted as age-specific)


library(dplyr)

source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_location_metadata.R")


all_age<-read.csv("FILEPATH/dengue_allage_data20.csv")
all_age$X<-NULL


aa_loc_years<-select(all_age,location_id, year_start)
#create counter of grouped observations
aa_loc_years<-aa_loc_years %>% group_by(location_id) %>% count(year_start)
aa_loc_years$in_all_age<-1

################# pull agespecific data
dengue_age <- get_bundle_version(ADDRESS, fetch="all",)

#table location-year combinations
da_loc_years<-select(dengue_age,location_id, year_start)
#create counter of grouped observations
da_loc_years<-da_loc_years %>% group_by(location_id) %>% count(year_start)
da_loc_years$in_age_spec<-1
#################
#compare year/locs combinations of all-age data

check_locs<-merge(da_loc_years, aa_loc_years, by=c("location_id","year_start"), all=TRUE)

table(check_locs$in_all_age, check_locs$in_age_spec)

age_id<-check_locs[check_locs$in_age_spec==1 & is.na(check_locs$in_all_age),]
age_id$flag<-1
age_id$n.x<-NULL
age_id$n.y<-NULL
age_id$in_all_age<-NULL


age_recs<-merge(dengue_age,age_id,by=c("location_id","year_start"),all=TRUE)
age_recs<-age_recs[age_recs$flag==1,]

##append age_specific records that are not included in all age dataset
all_dengue_xwalk_prep<-rbind(age_recs,all_age, fill=TRUE)

write.csv(all_dengue_xwalk_prep,"FILEPATH/prep_URxwalk.csv")
