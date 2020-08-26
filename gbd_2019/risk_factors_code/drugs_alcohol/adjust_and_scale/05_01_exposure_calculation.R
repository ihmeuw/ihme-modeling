##################
# Purpose: Generate Exposure Values
##################

print("starting execution")
lib.packages  <- paste0('FILEPATH')

source('FILEPATH')
source('FILEPATH')
source('FILEPATH')
source('FILEPATH')
source('FILEPATH')

library(data.table      , lib.loc = lib.packages)
library(plyr            , lib.loc = lib.packages)
library(dplyr)
library(magrittr        , lib.loc = lib.packages)

# Read in arguments, first batch refer to what estimates to produce,
# second batch refer to where to find everything.
# Third batch are alcohol and GBD specific options

args <- commandArgs(trailingOnly = TRUE)

debug <- ifelse(!is.na(args[1]),args[1], T) # take debug value from parent file

param_map <- fread('FILEPATH')
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))

if (debug == F) {
  
  location   <- param_map[task_id, location]
  years      <- as.numeric(unlist(strsplit(args[3], ",")))
  ages       <- as.numeric(unlist(strsplit(args[4], ",")))
  sexes      <- as.numeric(unlist(strsplit(args[5], ",")))
  
  version    <- as.numeric(args[6])
  input_dir  <- as.character(args[7])
  output_dir <- as.character(args[8])
  collapse   <- as.logical(args[9])

  include_unrecorded <- as.logical(args[12])
  
} else {  
  
  input_dir <- 'FILEPATH'
  location  <- 101
  ages      <- 13
  sexes     <- 1
  years <- c(1990, 2000, 2017)
  include_unrecorded     <- T
  
} 

#Load datasets and reduce to required columns. Reshape long.

lpc <- fread('FILEPATH')

if (include_unrecorded == T){
  unrecorded <- read.csv('FILEPATH')
  unrecorded[,c("location_name", "region_name", "super_region_name")] <- NULL
  unrecorded <- setnames(unrecorded, old = "unrecorded", new = "unrecorded_rate")
}

current_drinkers <- fread(paste0('FILEPATH'))
gday             <- fread(paste0('FILEPATH'))

populations      <- get_population(age_group_id = ages, location_id = location, year_id = years, sex_id = sexes, decomp_step= "step4")
populations      <- populations[,c('location_id', 'year_id', 'sex_id', 'age_group_id', 'population')]

alc <- list(current_drinkers = current_drinkers, 
            gday = gday)

alc <- lapply(alc, "[", T, -c("measure_id", "modelable_entity_id")) %>%
  lapply(data.table::melt, id.vars = c("age_group_id", "location_id", "sex_id", "year_id"), 
         variable.name = "draw", value.name = "key")

list2env(alc, .GlobalEnv)
rm(alc)


current_drinkers <- plyr::rename(current_drinkers, replace = c("key" = "current_drinkers"))
gday             <- plyr::rename(gday, replace = c("key" = "gday"))

alc <- join(current_drinkers, gday, by = c("location_id","year_id", "age_group_id", "sex_id", "draw"), type = "left")
alc <- alc[year_id %in% years]
alc <- alc[age_group_id %in% ages]

rm(current_drinkers, gday)


#Make final dataset from all of the merged datasets, merge on lpc and populations

alc <- join(alc, lpc, by = c('year_id', 'location_id', 'draw'), type = "left")

if (include_unrecorded == T) {
  alc <- join(alc, unrecorded, by = c('location_id', 'draw'), type = "left")
  rm(unrecorded)
} 

alc <- join(alc, populations, by = c('year_id', 'location_id', 'age_group_id', 'sex_id'), type = "left")
rm(lpc, populations)


#Determine percentage of total liters per capita to distribute to each group, based on gday relationships and population.

#Calculate total stock in a given population, based on survey g/day

alc$drinker_population     <- alc$current_drinkers * alc$population
alc$population_consumption <- alc$drinker_population * alc$gday

#Calculate total stock overall from summing across age_groups for each sex
#This doesn't group by sex- should it?

total_gday <- alc %>% dplyr::group_by(location_id, year_id, draw) %>% dplyr::summarise(total_gday = sum(population_consumption))
alc <- join(alc, total_gday, by = c('location_id', 'year_id', 'draw'), type = "left")
rm(total_gday)

#Calculate percentage of total stock each invidual-level population consumes
# Age sex pattern
alc$percentage_total_consumption <- alc$population_consumption / alc$total_gday

#Now, using LPC data, calculate total stock and use above calculated percentages to split this 
#stock into lpc drinkers for each population

alc$population_consumption = (alc$population * alc$alc_lpc)

if (include_unrecorded == T){
  alc$population_consumption <- alc$population_consumption * alc$unrecorded_rate
}

total_consumption <- alc %>% dplyr::group_by(location_id, year_id, draw) %>% dplyr::summarise(total_consumption = sum(population_consumption)) 
alc <- plyr::join(alc, total_consumption, by = c('location_id', 'year_id', 'draw'), type = "left")
rm(total_consumption)

alc$drinker_lpc <- (alc$percentage_total_consumption * alc$total_consumption) / alc$drinker_population

#Convert lpc/year to g/day
alc$drink_gday <- (alc$drinker_lpc * 1000)/365

#Hold onto relevant columns and export
alc = alc[,c('location_id', 'sex_id', 'age_group_id', 'year_id', 'draw', 'current_drinkers', 
             'alc_lpc', 'drink_gday', 'population')]


#Convert to collapsed, if option chosen. Regardless, save results

if (collapse == T){
  
  alc <- melt(alc, id.vars= c("location_id", "sex_id", "age_group_id", "year_id", "draw"))
  
  alc <- alc[, mean:=mean(value), by = c("age_group_id", "location_id", "sex_id", "year_id", 'variable')]
  alc <- alc[, lower:=quantile(value, .05, na.rm = T), by = c("age_group_id", "location_id", "sex_id", "year_id", 'variable')]
  alc <- alc[, upper:=quantile(value, .95, na.rm = T), by = c("age_group_id", "location_id", "sex_id", "year_id", 'variable')]
  
  alc[,c("draw","value")] <- NULL
  alc <- unique(alc)
  
  l <- location
  write.csv(alc, 'FILEPATH', row.names = F)
  
}else{
  
  l <- location
  write.csv(alc, 'FILEPATH', row.names = F)
  print("Finished!")
  
}
