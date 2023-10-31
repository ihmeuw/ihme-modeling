rm(list=ls())
####################################################################################
## This file launches Clostridium difficile PAF calculation, parallel by location_id ##
####################################################################################
source("/PATH/get_location_metadata.R")
source("/PATH/get_draws.R")
source("/PATH/get_age_metadata.R")
source("/PATH/get_population.R")

## FUNCTIONS

# Check if locations are missing in population and codcorrect files.
check_missing_locs <- function(dir, keyword) {
  unfinished_locations <- c()
  
  files <- list.files(dir)
  checks <- files[which(files %like% keyword)]
  finished <- lapply(checks, function(file) substr(file, nchar(keyword) + 2, nchar(file)-4)) # Assumes file extensions are .csv for the -4
  unfinished_locations <- c(unfinished_locations, setdiff(locations, finished))

  return(unfinished_locations)
}

###############################################
locations <- get_location_metadata(location_set_id=35, gbd_round_id = 7, decomp_step = 'iterative')
locations <- locations$location_id[locations$is_estimate==1]

# Test
#locations <- 33

#Get population and export to avoid overwhelming the database by running in parallel
year_id <- c(1990,1995,2000,2005,2010,2015,2019,2020, 2021, 2022) #estimation years
sex_id <- c(1,2)
age_map <- get_age_metadata(age_group_set = 19, gbd_round_id = 7)
age_group_id <- age_map$age_group_id

pop_dir <- "/PATH/pop/"
pop <- get_population(location_id=locations, age_group_id=age_group_id, year_id=year_id, 
                      sex_id=sex_id, release_id=9) # release_id 9 = gbd 2020 release 1

for (i in 1:length(locations)) {
  subset <- pop[pop$location_id == locations[i],]
  write.csv(subset, paste0(pop_dir,"pop_",locations[i],".csv"), row.names=FALSE)
}

missing_locs_pop <- check_missing_locs(pop_dir, "pop") # if length = 0, no missing locs
message(paste0(as.character(length(missing_locs_pop))), " missing locations in population files.")


#Get Cod draws, export each location draw file#
source_draws <- "codcorrect"
version_id <- ID # Update to latest version of codcorrect

cod_draws <- get_draws(source=source_draws,
                       gbd_id_type="cause_id",
                       gbd_id=302, sex_id=sex_id,
                       version_id=version_id,
                       gbd_round_id=7,
                       location_id=locations,
                       year_id=year_id,
                       age_group_id=age_group_id,
                       decomp_step="iterative")

cod_draws <- cod_draws[measure_id==1]
cod_dir <- "/PATH/cod/"
for (i in 1:length(locations)) {
  subset <- cod_draws[cod_draws$location_id == locations[i],]
  write.csv(subset, paste0(cod_dir,"cod_",locations[i],".csv"), row.names=FALSE)
}

missing_locs_cod <- check_missing_locs(cod_dir, "cod") # if length = 0, no missing locs
message(paste0(as.character(length(missing_locs_cod))), " missing locations in codcorrect files.")
###

###############################################
# loop over locations, submit a job
for(l in locations) {
  args <- paste(l)
  # store qsub command
  qsub = paste0(args)

  # submit job
  system(qsub)
}

