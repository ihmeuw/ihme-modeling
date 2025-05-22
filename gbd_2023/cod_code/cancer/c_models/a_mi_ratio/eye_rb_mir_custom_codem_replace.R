


## Details: 1. Loads in the four staged files (outputs of keep best for MIRs) which include all ages CR, all ages VR, peds CR and peds VR. 

##          3. Merges together the codem outputs with staged datasets and replaces CR mor in HICs for 0-4 and 5-9 age groups with codem mor. 
##          4. Saves out out this "fixed" file with the same 

## REDACTED


# Libraries
library(data.table)


# Shared functions 
source("FILEPATH/get_draws.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_location_metadata.R")

# Set globals 
GBD_YEAR <-  'GBD2022' 
INDIR <- paste0('FILEPATH', GBD_YEAR, 'FILEPATH')
CANCER <- 'neo_eye_rb'
CANCER_ID <- REDACTED 
VERSION_ID_M <- REDACTED # male codem run of interest (current best)
VERSION_ID_F <-  REDACTED # female codem run of interest (current best)
STAGED_INCIDENCE_VERSION_ID <- REDACTED # staged_incidence_version_id from staging pipeline associated with best mir_model_version (can find this in the mir_model_version sql table)
DATE_GENERATED <- "REDACTED" # date_generated from staging pipeline associated with best mir_model_version (can find this in the mir_model_version sql table)
RELEASE_ID <- REDACTED 
under_five_ages <- c(2, 3, 34, 238, 388, 389)
NEW_STAGED_INCIDENCE_VERSION_ID <- REDACTED 
TODAY <- "REDACTED" 

# get location metadata 
locs <- get_location_metadata(location_set_id = 22, release_id = RELEASE_ID)


mir_all_ages <- fread(paste0(INDIR, "/MI_ratio_model_input_all_ages_CR_v",STAGED_INCIDENCE_VERSION_ID, "_", DATE_GENERATED, ".csv")) # all ages CR 
mir_all_ages <- merge(mir_all_ages, locs[,.(location_id, super_region_name)], by="location_id") # merge in loc info so we can subset to HICs 

mir_all_ages_vr <- fread(paste0(INDIR, "/MI_ratio_model_input_all_ages_VR_v",STAGED_INCIDENCE_VERSION_ID, "_", DATE_GENERATED, ".csv")) # all ages VR 
mir_all_ages_vr <- merge(mir_all_ages_vr, locs[,.(location_id, super_region_name)], by="location_id") # merge in loc info so we can subset to HICs 

mir_peds <- fread(paste0(INDIR, "/MI_ratio_model_input_pediatric_CR_v",STAGED_INCIDENCE_VERSION_ID, "_", DATE_GENERATED, ".csv")) 
mir_peds <- merge(mir_peds, locs[,.(location_id, super_region_name)], by="location_id") # merge in loc info so we can subset to HICs 

mir_peds_vr <- fread(paste0(INDIR, "/MI_ratio_model_input_pediatric_VR_v",STAGED_INCIDENCE_VERSION_ID, "_", DATE_GENERATED, ".csv")) 
mir_peds_vr <- merge(mir_peds_vr, locs[,.(location_id, super_region_name)], by="location_id")# merge in loc info so we can subset to HICs  


dt1 <- mir_all_ages[super_region_name=="High-income", .(location_id)]
dt2 <- mir_all_ages_vr[super_region_name=="High-income", .(location_id)]
dt3 <- mir_peds[super_region_name=="High-income", .(location_id)]
dt4 <- mir_peds_vr[super_region_name=="High-income", .(location_id)]
locations_to_pull <- rbindlist(list(dt1, dt2, dt3, dt4)) # bind them all together 
locations_to_pull <- unique(locations_to_pull$location_id) # keep just unique locs 



codem_temp <-  get_model_results(gbd_team = "cod", gbd_id = CANCER_ID, measure_id = 1, location_id = locations_to_pull, sex_id = c(1,2), age_group_id = c(under_five_ages,6), release_id = RELEASE_ID)
tmp_0_4 <- codem_temp[age_group_id %in% under_five_ages, .(codem_deaths=sum(mean_death), age_group_id=1, acause="neo_eye_rb", sex_id = 3), by=c("location_id", "year_id")]
tmp_5_9 <- codem_temp[age_group_id==6, .(codem_deaths = sum(mean_death), acause="neo_eye_rb", sex_id = 3), by=c("location_id", "year_id", "age_group_id")]
codem_deaths_df <- rbind(tmp_0_4, tmp_5_9)


mir_all_ages_fixed <- merge(mir_all_ages,codem_deaths_df, by=c("location_id", "sex_id", "year_id", "age_group_id", "acause"), all.x=T) # all ages CR 
mir_all_ages_fixed <- mir_all_ages_fixed[acause=="neo_eye_rb"&super_region_name=="High-income"&(age_group_id==1|age_group_id==6)&!is.na(codem_deaths), deaths := codem_deaths]
mir_all_ages_fixed$codem_deaths <- NULL; mir_all_ages_fixed$super_region_name <- NULL
 
mir_all_ages_vr_fixed <- merge(mir_all_ages_vr,codem_deaths_df, by=c("location_id", "sex_id", "year_id", "age_group_id", "acause"), all.x=T) # all ages VR 
mir_all_ages_vr_fixed <- mir_all_ages_vr_fixed[acause=="neo_eye_rb"&super_region_name=="High-income"&(age_group_id==1|age_group_id==6)&!is.na(codem_deaths), deaths := codem_deaths]
mir_all_ages_vr_fixed$codem_deaths <- NULL; mir_all_ages_vr_fixed$super_region_name <- NULL

mir_peds_fixed <- merge(mir_peds,codem_deaths_df, by=c("location_id", "sex_id", "year_id", "age_group_id", "acause"), all.x=T) 
mir_peds_fixed <- mir_peds_fixed[acause=="neo_eye_rb"&super_region_name=="High-income"&(age_group_id==1|age_group_id==6)&!is.na(codem_deaths), deaths := codem_deaths]
mir_peds_fixed$codem_deaths <- NULL; mir_peds_fixed$super_region_name <- NULL

mir_peds_vr_fixed <- merge(mir_peds_vr,codem_deaths_df, by=c("location_id", "sex_id", "year_id", "age_group_id", "acause"), all.x=T) 
mir_peds_vr_fixed <- mir_peds_vr_fixed[acause=="neo_eye_rb"&super_region_name=="High-income"&(age_group_id==1|age_group_id==6)&!is.na(codem_deaths), deaths := codem_deaths]
mir_peds_vr_fixed$codem_deaths <- NULL; mir_peds_vr_fixed$super_region_name <- NULL

# confirm there are do NA values for deaths column since that will fail in MIR prep step 
if(nrow((mir_all_ages_fixed[is.na(deaths)]))>0){print("REDACTED")}else{print("PASS :)")} 
if(nrow((mir_all_ages_vr_fixed[is.na(deaths)]))>0){print("REDACTED")}else{print("PASS :)")} 
if(nrow((mir_peds_fixed[is.na(deaths)]))>0){print("REDACTED")}else{print("PASS :)")} 
if(nrow((mir_peds_vr_fixed[is.na(deaths)]))>0){print("REDACTED")}else{print("PASS :)")} 

# write out these four files 
fwrite(mir_all_ages_fixed, paste0(INDIR, "/MI_ratio_model_input_all_ages_CR_v",NEW_STAGED_INCIDENCE_VERSION_ID, "_", TODAY, ".csv"))  # all ages CR 
fwrite(mir_all_ages_vr_fixed, paste0(INDIR, "/MI_ratio_model_input_all_ages_VR_v",NEW_STAGED_INCIDENCE_VERSION_ID, "_", TODAY, ".csv")) # all ages VR 
fwrite(mir_peds_fixed, paste0(INDIR, "/MI_ratio_model_input_pediatric_CR_v",NEW_STAGED_INCIDENCE_VERSION_ID, "_", TODAY, ".csv")) 
fwrite(mir_peds_vr_fixed, paste0(INDIR, "/MI_ratio_model_input_pediatric_VR_v",NEW_STAGED_INCIDENCE_VERSION_ID, "_", TODAY, ".csv")) 



# REDACTED




