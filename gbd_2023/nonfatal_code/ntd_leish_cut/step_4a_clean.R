##############################################################################################
# Purpose: Create dataset for covariates. Use draws in order to recreate the distribution

source("/FILEPATH/get_age_metadata.R")
source("/FILEPATH/interpolate.R")
dir.create(paste0(interms_dir, 'FILEPATH/'))


###' [1) Interpolate age pattern to split all-age, both-sex incidence
## Create variables to index location ids and age groups
leish_endemic_cl <- fread(paste0(params_dir, "/FILEPATH"))
unique_locs <- unique(leish_endemic_cl[value_endemicity == 1 & most_detailed == 1, location_id])

ages_meta <- get_age_metadata(age_group_set_id = 19)
ages <- unique(ages_meta$age_group_id)

## Pull in age pattern from DisMod, interpolate for each year
agesex_interp <- interpolate(gbd_id_type = "modelable_entity_id",
                             gbd_id = ID,
                             location_id = unique_locs,
                             measure_id = 6,
                             source = "epi",
                             sex_id = c(1,2),
                             age_group_id = ages,
                             reporting_year_start = 1980,
                             reporting_year_end = 2024,
                             release_id = release_id)

## Save intermediate file for each location                          
setnames(agesex_interp, old = paste0("draw_", 0:999), new = paste0("ageCurve_", 0:999))
agesex_interp <- left_join(agesex_interp, ages_meta, by="age_group_id")

for(i in unique_locs){
  loc_agesex <- subset(agesex_interp, location_id == i)
  write.csv(loc_agesex, file = (paste0(interms_dir, "FILEPATH/", i, ".csv")), row.names = F)
  
  cat("\n Writing", i) 
}


###' [2) Pull in UHC data 
uhc_data <- fread('/FILEPATH')
draw_cols_uhc <- paste0("draw_uhc_", 0:999) 
setnames(uhc_data, old = paste0("draw_", 0:999), new = draw_cols_uhc)

# create uhc dataset for subnationals and merge
uhc_data_subn <- uhc_data
uhc_data_subn$location_merge <- uhc_data_subn$location_id
uhc_data_subn$location_id <- NULL

subn_locs <- leish_endemic_cl[value_endemicity == 1 & year_start == 2019 & most_detailed == 1 & level > 3 ,]
subn_locs <- subn_locs[, c("location_id", "parent_id", "level")]
subn_locs$location_merge <- ifelse(subn_locs$level == 5, ID, subn_locs$parent_id) 
subn_locs_uhc <- merge(subn_locs, uhc_data_subn, by = 'location_merge')
subn_locs_uhc[, c("location_merge", "parent_id", "level") := NULL]

uhc_data_cl <- rbind(subn_locs_uhc, uhc_data)  
uhc_data_cl <- subset(uhc_data_cl, location_id %in% unique_locs)

write.csv(uhc_data_cl, paste0(params_dir, "/FILEPATH"), row.names = FALSE)
