##########################################################################################
# Date: 11/20/2018
# Purpose: Generate Exposure Values
##########################################################################################
rm(list = ls())

print("starting execution")
source("FILEPATH")
library(data.table)

# Read in arguments, first batch refer to what estimates to produce,
#print('command args:')
#print(commandArgs())
args <- commandArgs(trailingOnly = TRUE)
print('args:')
print(args)

debug <- args[1]
debug <- ifelse(is.na(debug) == FALSE, debug, TRUE) # take debug value from parent file

param_map <- fread(paste0("FILEPATH"))
task_id <- Sys.getenv("SLURM_ARRAY_TASK_ID") |> as.integer()

if (debug == TRUE) { # Debug via cluster submission
  location <- param_map[task_id, location_id]
  ages <- c(8:20, 30:32, 235)
  sexes <- c(1, 2)
  years <- c(1980:2024)

  output_dir        <- args[2]
  collapse          <- args[3] |> as.logical()
  gday_mod          <- args[4]
  binary_mod        <- args[5]
  lpc_mod           <- args[6]
  former_cap        <- args[7] |> as.integer()
  include_former    <- args[8] |> as.logical()
  include_unrecorded<- args[9] |> as.logical()
  
  print("inputs:")
  print(location)
  print(ages)
  print(sexes)
  print(years)
  print('args:')
  
  print(paste(output_dir,collapse,gday_mod,binary_mod,lpc_mod,former_cap,include_former,include_unrecorded))
  
} else { # Regular submission
  location <- param_map[task_id, location_id]
  ages <- c(8:20, 30:32, 235)
  sexes <- c(1, 2)
  years <- c(1980:2024)

  #input_dir         <- args[2]
  output_dir        <- args[2]
  collapse          <- args[3] |> as.logical()
  gday_mod          <- args[4]
  binary_mod        <- args[5]
  lpc_mod           <- args[6]
  former_cap        <- args[7] |> as.integer()
  include_former    <- args[8] |> as.logical()
  include_unrecorded<- args[9] |> as.logical()
  
  print("inputs:")
  print(location)
  print(ages)
  print(sexes)
  print(years)
  print('args:')
  
  print(paste(output_dir,collapse,gday_mod,binary_mod,lpc_mod,former_cap,include_former,include_unrecorded))
} 

#function to make sure upper age groups have the right values
cp_age_21 <- function(data){
  temp <- data[age_group_id == 21]
  for(c.age in c(30,  31,  32, 235)){
    temp[,age_group_id := c.age]
    data <- rbind(data, temp)
  }
  data <- data[age_group_id!=21]
  return(data)
}
# Load datasets and reduce to required columns. Reshape long.
current_drinkers <- fread(paste0('FILEPATH'))
current_drinkers <- cp_age_21(current_drinkers)

gday <- fread(paste0('FILEPATH'))
gday <- cp_age_21(gday)

lpc <- fread(paste0('FILEPATH'))

if (include_unrecorded == TRUE) {
  unrecorded <- fread('FILEPATH')
  unrecorded <- dplyr::rename(unrecorded, unrecorded_rate = unrecorded)
}

populations  <- get_population(
  age_group_id = ages, 
  location_id = location, 
  year_id = years, 
  sex_id = sexes, 
  release_id = 16
)

alc <- list(current_drinkers = current_drinkers, gday = gday)

alc <- lapply(alc, "[", TRUE, -c("measure_id", "modelable_entity_id")) |>
  lapply(
    data.table::melt, 
    id.vars = c("age_group_id", "location_id", "sex_id", "year_id"), 
    variable.name = "draw", 
    value.name = "key"
  )

list2env(alc, .GlobalEnv)
rm(alc)

current_drinkers <- dplyr::rename(current_drinkers, current_drinkers = key)
gday             <- dplyr::rename(gday, gday = key)

alc <- dplyr::left_join(current_drinkers, gday, by = c("location_id", "year_id", "age_group_id", "sex_id", "draw"))

alc <- alc[year_id %in% years, ]
alc <- alc[age_group_id %in% ages, ]

rm(current_drinkers, gday)

#Make final dataset from all of the merged datasets, merge on lpc and populations
alc <- dplyr::left_join(alc, lpc, by = c("year_id", "location_id", "draw"))

if (include_unrecorded == TRUE) {
  alc <- dplyr::left_join(alc, unrecorded, by = c("location_id", "draw"))
  alc$unrecorded_rate <- ifelse(is.na(alc$unrecorded_rate), 1, alc$unrecorded_rate) #If no data on unrecorded, assume unrecorded is non-existent (i.e. adjustment factor = 1)
  rm(unrecorded)
}

alc <- dplyr::left_join(alc, populations, by = c("year_id", "location_id", "age_group_id", "sex_id"))

rm(lpc, populations)

#Scale current_drinkers, abstainers, and former drinkers to 1. Place cap on former drinkers and reapportion to abstainers


alc$drinker_population     <- alc$current_drinkers * alc$population
alc$population_consumption <- alc$drinker_population * alc$gday

#Calculate total stock overall from summing across age_groups for each sex

total_gday <- alc |> 
  dplyr::group_by(location_id, year_id, draw) |> 
  dplyr::summarise(total_gday = sum(population_consumption))
alc <- dplyr::left_join(alc, total_gday, by = c("location_id", "year_id", "draw"))
rm(total_gday)

#Calculate percentage of total stock each invidual-level population consumes
# Age sex pattern
alc$percentage_total_consumption <- alc$population_consumption / alc$total_gday

#Now, using LPC data, calculate total stock and use above calculated percentages to split this 
#stock into lpc drinkers for each population

alc$population_stock_consumption <- alc$population * alc$alc_lpc

if (include_unrecorded == TRUE) {
  alc$population_stock_consumption <- alc$population_stock_consumption * alc$unrecorded_rate
}

total_consumption <- alc |> 
  dplyr::group_by(location_id, year_id, draw) |> 
  dplyr::summarise(total_consumption = sum(population_stock_consumption)) 
alc <- dplyr::left_join(alc, total_consumption, by = c("location_id", "year_id", "draw"))
rm(total_consumption)

alc$drinker_lpc <- (alc$percentage_total_consumption * alc$total_consumption) / alc$drinker_population
  
#Convert lpc/year to g/day
alc$drink_gday <- (alc$drinker_lpc * 1000 * 0.789) / 365
  
#Hold onto relevant columns and export
alc = alc[, c("location_id", "sex_id", "age_group_id", "year_id", "draw", "current_drinkers", 
              "alc_lpc", "drink_gday", "population")]

#save
l <- location
fwrite(alc,  paste0('FILEPATH'))
  

#Convert to collapsed, if option chosen. Regardless, save results
if (collapse == TRUE) {
  alc <- melt(alc, id.vars= c("location_id", "sex_id", "age_group_id", "year_id", "draw"))
  alc <- alc[,.(mean = mean(value),
                lower = quantile(value, 0.05, na.rm = TRUE),
                upper = quantile(value, 0.95, na.rm = TRUE)),
             by = c("age_group_id", "location_id", "sex_id", "year_id", "variable")]
  path <- paste0('FILEPATH')
  print(path)
  fwrite(alc,path)
} 

print("Finished!")

