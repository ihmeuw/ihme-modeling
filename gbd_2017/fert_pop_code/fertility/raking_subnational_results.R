##############################
#Raking Subnational Results
##############################

rm(list=ls())
library(data.table)
library(haven)
library(lme4)
library(ggplot2)
library(splines)
library(mortcore, lib = FILEPATH)

if (Sys.info()[1] == 'Windows') {
  username <- USER
  root <- FILEPATH
  model_age <- 
  version <- 
  loop <- 
} else if (interactive()){
  username <- Sys.getenv("USER")
  root <- FILEPATH
  version <- 
  loop <- 
  model_age <- 
  parent_ihme <- 
} else {
  username <- Sys.getenv("USER")
  root <- FILEPATH
  version <- commandArgs(trailingOnly = T)[1]
  loop <- commandArgs(trailingOnly = T)[2]
  model_age <- commandArgs(trailingOnly = T)[3]
  parent_ihme <- commandArgs(trailingOnly = T)[4]
  
}

## setting directories
unraked_dir       <- FILEPATH
jbase             <- FILEPATH
mort_function_dir <- FILEPATH
raked_dir         <- FILEPATH

## running functions
source(paste0(mort_function_dir, "get_locations.r"))
source(paste0(mort_function_dir, "get_age_map.r"))

## setting options

## getting relevant locations
children <- data.table(get_locations(level = "estimate"))


if(parent_ihme == "CHN_44533"){
  children <- children[grepl("CHN_", ihme_loc_id) & (!ihme_loc_id %in% c("CHN_354", "CHN_361"))] 
} else{
  children <- children[grepl(parent_ihme, ihme_loc_id)]
} 

loc_map <- children[,.(ihme_loc_id, location_id)]

age_map <- data.table(get_age_map())[,.(age_group_id, age_group_name_short)]
setnames(age_map, "age_group_name_short", "age")
age_map[,age := as.numeric(age)]
age_map <- age_map[!is.na(age)]

## reading in and appending parent and child files
missing_files <- c()
unraked <- list()
for (loc in children$ihme_loc_id){
  cat(paste0(loc, model_age, "\n")); flush.console()
  file <- paste0(unraked_dir,  "/gpr_", loc, "_", model_age, "_sim.csv")
  if(file.exists(file)){ 
    unraked[[paste0(file)]] <- fread(file)
  } else {
    missing_files <- c(missing_files, file)
  }
}

if(length(missing_files) > 0) stop("Missing files")

unraked <- rbindlist(unraked)
unraked[,year_id := floor(year)]
unraked[,age := as.numeric(model_age)]

## reading in population
pop <- fread(paste0(jbase, "pop_input.csv"))
pop[,pvid := NULL]
pop <- merge(pop, age_map, all.x = T, by= c("age_group_id"))
pop <- merge(pop, loc_map, by = c("location_id"))
pop <- pop[age == model_age & ihme_loc_id %in% children$ihme_loc_id]
pop[,age := as.numeric(age)]

unraked <- merge(unraked, pop, by=c("age", "ihme_loc_id", "year_id"), all.x=T)

if(nrow(unraked[is.na(population)]) > 0) stop("missing population needed for raking")

## actually is ASFR 
unraked[,val := val * population]

if(parent_ihme == "CHN_44533"){
  raked <- scale_results(unraked, id_vars = c("location_id", "year_id", "age_group_id", "sim"), value_var = "val", location_set_id = 21, gbd_round_id = 5, exclude_parent = "CHN")
} else if(parent_ihme == "GBR") {
  raked <- scale_results(unraked, id_vars = c("location_id", "year_id", "age_group_id", "sim"), value_var = "val", location_set_id = 21, gbd_round_id = 5)
} else {
  raked <- scale_results(unraked, id_vars = c("location_id", "year_id", "age_group_id", "sim"), value_var = "val", location_set_id = 21, gbd_round_id = 5)
}

raked[,val := val / population]

## pruning down to the variables the files can in with
raked <- raked[,.(ihme_loc_id, year, sim, val)]

## collpasing to the summary file
summary <- copy(raked)

setkey(summary, ihme_loc_id, year)
summary <- summary[,.(mean = mean(val),lower=quantile(val,probs=c(.025)),upper = quantile(val,probs=c(.975))),by=key(summary)]

## writing output files
## draw files
for(loc in unique(raked$ihme_loc_id)){
  temp <- raked[ihme_loc_id == loc]
  write.csv(temp, paste0(raked_dir, "gpr_", loc, "_", model_age, "_sim.csv"), row.names = F)
}

## summary level files
for(loc in unique(summary$ihme_loc_id)){
  temp <- summary[ihme_loc_id == loc]
  write.csv(temp, paste0(raked_dir, "gpr_", loc, "_", model_age, ".csv"), row.names = F)
}







