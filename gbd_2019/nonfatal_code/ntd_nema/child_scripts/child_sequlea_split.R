# Unable to estimate for Poland / Poland subnationals since expert groups did not consider Europe
# As done last cycle, assuming to zero out

## Empty the environment
rm(list = ls())

## Set up focal drives
os <- .Platform$OS.type
if (os=="windows") {
  ADDRESS <- FILEPATH
  ADDRESS <- FILEPATH
} else {
  ADDRESS <- FILEPATH
  ADDRESS <- paste0(FILEPATH, Sys.info()[7], "/")
}

## Load functions and packages
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
library(argparse, lib.loc= FILEPATH)
library(data.table)

## Parse Arguments
print(commandArgs())
parser <- ArgumentParser()
parser$add_argument("--param_path", help = "param_path", default = "", type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
sessionInfo()

gbd_round_id <- 6

#############################################################################################
###                           Processes for restricted locations                          ###
#############################################################################################

task_id <- as.integer(Sys.getenv(ADDRESS))
params  <- fread(param_path)
my_loc <- params[task_id, location_ids]
is_heavy <- params[task_id, is_heavy]
draws_directory <- params[task_id, draws_directory]
prop_directory <- params[task_id, prop_directory]
my_worm <- params[task_id, my_worm]
meids_directory <- params[task_id, meids_directory]
params_dir <- params[task_id, params_directory]

meids     <- fread(meids_directory)
meids     <- meids[worm == my_worm]
all_cases <- meids[model == "all"]$meid

## Set up geographic restrictions first:

# GR's  
if (my_worm == "ascariasis") {short <- "ascar"}
if (my_worm == "trichuriasis") {short <- "trichur"}
if (my_worm == "hookworm") {short <- "hook"}

restrict_loc <- fread(paste0(params_dir, FILEPATH, short, FILEPATH))
restrict_loc <- unique(restrict_loc[value_endemicity == 0, location_id])


if (my_loc %in% restrict_loc){
  
  cat("location has geographic restrictions. Zeroing everything out. \n")
  
  # Canada has zero cases, and is the skeleton for all 0 draws 
  worm_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = all_cases, source = "epi",
                          location_id = 101, decomp_step = "step1",  version_id = ADDRESS)
  # ^^ do not edit -- need static generic zero draw file
  
  worm_draws[, c("modelable_entity_id", "metric_id", "model_version_id") := NULL]
  worm_draws[, location_id := my_loc]
  
  write.csv(worm_draws, file = paste0(draws_directory, my_loc, ".csv"), row.names = F)
  cat(paste("Finished location", my_loc, "\n"))
}


#############################################################################################
###                           Processes for unrestricted locations                        ###
#############################################################################################

## Conditon
if (!(my_loc %in% restrict_loc)){
  
  prop <- fread(prop_directory)
  prop <- prop[location_id == my_loc]
  
  ## Get draws
  worm_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = all_cases, source = "epi",
                          location_id = my_loc, decomp_step = "step4",  status = "best", 
                          gbd_round_id = gbd_round_id) 
  
  pops <- get_population(age_group_id = unique(worm_draws$age_group_id),
                         location_id  = unique(worm_draws$location_id),
                         year_id      = unique(worm_draws$year_id), 
                         sex_id = 1:2,
                         decomp_step = "step4",
                         gbd_round_id = gbd_round_id)
  
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
        if (locs$region_name %in% c("Western Europe", "Central Europe")){
          cat("location has geographic restrictions. Zeroing everything out. \n")
          
          worm_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = all_cases, source = "epi",
                                  location_id = 101, decomp_step = "step1",  version_id = ADDRESS)
          # ^^ do not edit -- need static generic zero draw file
          
          worm_draws[, c("modelable_entity_id", "metric_id", "model_version_id") := NULL]
          worm_draws[, location_id := my_loc]
          
          write.csv(worm_draws, file = paste0(draws_directory, my_loc, ".csv"), row.names = F)
          cat(paste("Finished location", my_loc, "\n"))
          break
        }
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
    temp[age_group_id == 6, heavy_prop := prop_age5_9]
    temp[age_group_id == 7, heavy_prop := prop_age10_14]
    temp[age_group_id >= 8, heavy_prop := prop_age15]
    
    ## Apply proportion by age group
    for (draw in grep("draw", names(temp), value = T))
      set(temp, j = draw, value = temp[[draw]]*temp[, heavy_prop])
    
    if (year == 1990) data <- copy(temp)
    if (year != 1990) data <- rbind(data, temp)
    
  }
  
  ## Convert back to prevalence space for upload
  for (draw in grep("draw", names(data), value = T))
    set(data, j = draw, value = data[[draw]] / data[, population])
  
  data[, c("modelable_entity_id", "metric_id", "model_version_id", "population", "heavy_prop") := NULL]
  
  cat(paste("Finished location", my_loc, "\n"))
  write.csv(data, file = paste0(draws_directory, my_loc, ".csv"), row.names = F)
  
}

