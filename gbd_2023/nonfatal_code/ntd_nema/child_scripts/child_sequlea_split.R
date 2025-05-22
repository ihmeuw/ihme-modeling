# Purpose: calculate heavy and mild sequela
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())

## Load functions and packages
code_root <-"FILEPATH"
data_root <- "FILEPATH"

params_dir <- "FILEPATH"
draws_dir <- "FILEPATH"
interms_dir <- "FILEPATH"
logs_dir <- "FILEPATH"
location_id <- 214

source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_population.R")
library(argparse)
source(paste0(code_root, 'FILEPATH'))
library(data.table)

# set run dir
run_file <- fread(paste0(data_root, "FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "FILEPATH")
code_dir <- paste0(code_root, 'FILEPATH')

#############################################################################################
###                           Processes for restricted locations                          ###
#############################################################################################

task_id <- as.integer(Sys.getenv("ADDRESS"))
params  <- fread(param_path)
my_loc <- params[task_id, location_ids]
is_heavy <- params[task_id, is_heavy]
my_worm <- params[task_id, my_worm]
release_id <- params[task_id, release_id]

meids_directory     <- paste0(params_dir, "FILEPATH")
meids     <- fread(meids_directory)
meids     <- meids[worm == my_worm]
all_cases <- meids[model == "all"]$meid
heavy <- meids[model == "heavy"]$meid
mild <- meids[model == "mild"]$meid
if (is_heavy == 1){ meid <- heavy} else {meid <- mild}

draws_directory <- paste0(draws_dir, my_worm,  "FILEPATH")
prop_directory = ("FILEPATH")
meids_directory <- paste0(params_dir, "FILEPATH")

## Set up geographic restrictions first:

if (my_worm == "ascariasis") {short <- "ascar"}
if (my_worm == "trichuriasis") {short <- "trichur"}
if (my_worm == "hookworm") {short <- "hook"}

restrict_loc <- fread(paste0(params_dir, "FILEPATH"))
restrict_loc <- unique(restrict_loc[value_endemicity == 0, location_id])

if (my_loc %in% restrict_loc){
  
  data <- gen_zero_draws(model_id= meid, location_id = NA, measure_id = 5, release_id = release_id, team = 'ADDRESS')
  data[, metric_id := 3]
  data[, model_id:= meid]
  write.csv(data, file = paste0(draws_directory, my_loc, "FILEPATH"), row.names = F)
  
}

#############################################################################################
###                           Processes for unrestricted locations                        ###
#############################################################################################

## Condition
if (!(my_loc %in% restrict_loc)){
  
  prop <- fread(prop_directory)
  prop <- prop[location_id == my_loc]
  
  ## Get draws
  worm_draws <- get_draws(gbd_id_type = "model_id", gbd_id = all_cases, source = "ADDRESS",
                          location_id = my_loc, release_id = release_id,  status = "best") 
  
  pops <- get_population(age_group_id = unique(worm_draws$age_group_id),
                         location_id  = unique(worm_draws$location_id),
                         year_id      = unique(worm_draws$year_id), 
                         sex_id = 1:2,
                         release_id = release_id)
  
  pops[, run_id := NULL]
  worm_draws <- merge(worm_draws, pops, by = setdiff(names(pops), "population"))
  
  ## Convert to case space
  for (draw in grep("draw", names(worm_draws), value = T))
    set(worm_draws, j = draw, value = worm_draws[[draw]]*worm_draws[, population])
  
  ## Loop through years
  for (year in unique(worm_draws$year_id)){
    cat(paste0("Applying proportion for year id ", year, "\n"))
    temp <- copy(worm_draws)
    temp <- temp[year_id == year]
    
    ## Here we save the proportions of infestation. If the location is missing
    ## we apply the regional average.
    if (my_loc %in% unique(prop$location_id)){
      
      if (is_heavy == T){
        cat("\tUsing heavy location proportions \n")
        prop_age0_4   <- prop[year_id == year & age == "0to4"]$prop_heavy
        prop_age5_9   <- prop[year_id == year & age == "5to9"]$prop_heavy
        prop_age10_14 <- prop[year_id == year & age == "10to14"]$prop_heavy
        prop_age15    <- prop[year_id == year & age == "15plus"]$prop_heavy
      } else if (is_heavy == F){
        cat("\tUsing medium location proportions \n")
        prop_age0_4   <- prop[year_id == year & age == "0to4"]$prop_med
        prop_age5_9   <- prop[year_id == year & age == "5to9"]$prop_med
        prop_age10_14 <- prop[year_id == year & age == "10to14"]$prop_med
        prop_age15    <- prop[year_id == year & age == "15plus"]$prop_med
      }
      
    } else if (!(my_loc %in% unique(prop$location_id))){
      cat("\tapplying regional average proportion \n")
      locs <- get_location_metadata(location_set_id = 35, gbd_round_id = gbd_round_id)
      locs <- locs[location_id == my_loc]
      
      prop <- fread(prop_directory)
      prop <- prop[region_name == locs$region_name]
      
      if (nrow(prop) == 0) {
        cat(paste0("Missing region for location ", my_loc, " -- now breaking"))
        write.csv(my_loc, file = paste0(draws_directory, my_loc, ".csv"), row.names = F)
        break
      }
      
      if (is_heavy == T){
        cat("\tUsing heavy regional proportions \n")
        prop_age0_4   <- unique(prop[year_id == year & age == "0to4"]$region_heavy_ave)
        prop_age5_9   <- unique(prop[year_id == year & age == "5to9"]$region_heavy_ave)
        prop_age10_14 <- unique(prop[year_id == year & age == "10to14"]$region_heavy_ave)
        prop_age15    <- unique(prop[year_id == year & age == "15plus"]$region_heavy_ave)
      } else {
        cat("\tUsing medium regional proportions \n")
      prop_age0_4   <- unique(prop[year_id == year & age == "0to4"]$region_med_ave)
      prop_age5_9   <- unique(prop[year_id == year & age == "5to9"]$region_med_ave)
      prop_age10_14 <- unique(prop[year_id == year & age == "10to14"]$region_med_ave)
      prop_age15    <- unique(prop[year_id == year & age == "15plus"]$region_med_ave)
      }
    }
    
    ## Merge proportion onto dataframe
    temp[age_group_id <= 5, heavy_prop := prop_age0_4]
    temp[age_group_id == 388, heavy_prop := prop_age0_4]
    temp[age_group_id == 389, heavy_prop := prop_age0_4]
    temp[age_group_id == 238, heavy_prop := prop_age0_4]
    temp[age_group_id == 34, heavy_prop := prop_age0_4]
    temp[age_group_id == 6, heavy_prop := prop_age5_9]
    temp[age_group_id == 7, heavy_prop := prop_age10_14]
    temp[age_group_id %in% c(8:33), heavy_prop := prop_age15]
    temp[age_group_id == 235, heavy_prop := prop_age15]
    
    ## Apply proportion by age group
    for (draw in grep("draw", names(temp), value = T))
      set(temp, j = draw, value = temp[[draw]]*temp[, heavy_prop])
    
    if (year == 1990) data <- copy(temp)
    if (year != 1990) data <- rbind(data, temp)
    
  }
  
  ## Convert back to prevalence space for upload
  for (draw in grep("draw", names(data), value = T))
    set(data, j = draw, value = data[[draw]] / data[, population])
  
  data[, c("model_id", "metric_id", "version_id", "population", "heavy_prop") := NULL]
  data[, metric_id := 3]
  data[, model_id:= meid]
  
  cat(paste("Finished location", my_loc, "\n"))
  write.csv(data, file = paste0(draws_directory, my_loc, "FILEPATH"), row.names = F)
  
}

