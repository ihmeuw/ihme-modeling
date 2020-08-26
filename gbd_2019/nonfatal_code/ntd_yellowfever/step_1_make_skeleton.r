
# Create skeleton dataset of every combination of iso, age, sex, & year

# Using cov to get national numbers for places where we would usually do subnational estimation (e.g., China)
AllPlaces <- get_demographics(gbd_team = "cov")
AllPop <- get_population(age_group_id = c(AllPlaces$age_group_id,22), location_id = AllPlaces$location_id, year_id = AllPlaces$year_id, sex_id = 1:3, decomp_step = 'step4')

AllLoc <- get_location_metadata(location_set_id = 35)
AllLoc <- AllLoc[,c( "location_set_version_id", "location_set_id",
                     "location_id","parent_id","is_estimate","location_name",
                     "location_type", "super_region_id", "super_region_name",
                     "region_id", "region_name", "ihme_loc_id")]

AllOut <- merge(AllPop,AllLoc, by = "location_id", all = TRUE)

Keep <- which(AllOut$is_estimate == 1 | AllOut$location_type == "admin0")
AllOut <- AllOut[Keep,]
AllOut$countryIso <- substr(AllOut$ihme_loc_id,1,3)

library(foreign) 

write.dta(AllOut,"FILEPATH")
