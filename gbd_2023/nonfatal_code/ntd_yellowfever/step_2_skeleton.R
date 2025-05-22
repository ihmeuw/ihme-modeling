# Purpose: Create skeleton dataset of every combination of iso, all age, sex, & year

### ========================= BOILER PLATE ========================= ###
os <- .Platform$OS.type
if (os == ADDRESS) {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}		

source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/save_results_epi.R")
source("FILEPATH/get_bundle_data.R")


# Using cov to get national numbers for places where we would usually do subnational estimation (e.g., China)
AllPlaces <- get_demographics(gbd_team = ADDRESS)
release_id <- ID
AllPop <- get_population(age_group_id = 22, location_id = AllPlaces$location_id, year_id = AllPlaces$year_id, sex_id = 3, release_id = release_id)

AllLoc <- get_location_metadata(location_set_id = 35, gbd_round_id=ADDRESS, decomp=ADDRESS)
AllLoc <- AllLoc[,c( "location_set_version_id","location_set_id","location_id","parent_id","is_estimate","location_name","location_type","super_region_id","super_region_name","region_id",
                     "region_name","ihme_loc_id")]


AllOut <- merge(AllPop,AllLoc, by = "location_id", all = TRUE)

Keep <- which(AllOut$is_estimate == 1 | AllOut$location_type == "admin0")
AllOut <- AllOut[Keep,]
AllOut$countryIso <- substr(AllOut$ihme_loc_id,1,3)

library(foreign) 
write.dta(AllOut,"FILEPATH")
write.csv(AllOut,"FILEPATH")

###-------------AGE-SPECIFIC SKELETON ----------------

#save all location-age specific populations for second skeleton-this is for age_groups
AllPlaces <- get_demographics(gbd_team = ADDRESS)
ages<-unique(AllPlaces$age_group_id)
years<-unique(AllPlaces$year_id)

AllPop <- get_population(age_group_id = ages, location_id = AllPlaces$location_id, year_id = years, sex_id = c(1,2), decomp_step = ADDRESS, gbd_round_id=ADDRESS)

AllLoc <- get_location_metadata(location_set_id = 35, release_id = release_id)
AllLoc <- AllLoc[,c( "location_set_version_id","location_set_id","location_id","parent_id","is_estimate","location_name","location_type","super_region_id","super_region_name","region_id",
                     "region_name","ihme_loc_id")]


AllOut <- merge(AllPop,AllLoc, by = "location_id", all = TRUE)

Keep <- which(AllOut$is_estimate == 1 | AllOut$location_type == "admin0")
AllOut <- AllOut[Keep,]
AllOut$countryIso <- substr(AllOut$ihme_loc_id,1,3)
write.dta(AllOut,"FILEPATH")