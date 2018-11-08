# Computes correction factors to scale household deaths to nationally-representative space

rm(list=ls())
library(data.table); library(haven); library(readr); library(readstata13); library(assertable); library(plyr); library(DBI); library(foreign); library(mortdb, lib = "FIELPATH"); library(mortcore, lib = "FILEPATH");

if (Sys.info()[1] == "Linux") {
  root <- "FILEPATH" 
  user <- Sys.getenv("USER")
  version_id <- as.numeric(commandArgs(trailingOnly = T)[1])
  gbd_year <- as.numeric(commandArgs(trailingOnly = T)[2])
} else {
  root <- "J:"
  user <- Sys.getenv("USERNAME")
  code_dir <- paste0("FILEPATH")
}

source("FILEPATH/get_population.R")
source("FILEPATH/get_location_metadata.R")

out_dir <- paste0("FILEPATH", version_id, "FILEPATH")

save_pop_version <- function(version, out_dir){
  fileConn<-file(paste0(out_dir, "/best_pop_version.txt"))
  writeLines(as.character(version), fileConn)
  close(fileConn)
  print(paste0("Best population version ", version, " saved."))
}


best_pop_version <- get_proc_version("population", "estimate", run_id = "best")
best_death_version <- get_best_versions(c("death number empirical data"), gbd_year = gbd_year, hostname = "modeling-mortality-db")
save_pop_version(best_pop_version, out_dir)


gbd_pop <- get_population(status = "recent", age_group_id=22, location_id='all', year_id='all', sex_id='all', location_set_id = 82)


old_ap_pops <- gbd_pop[location_id %in% c(4841, 4871)]
old_ap_pops <- old_ap_pops[, list(population = sum(population)), by = c("age_group_id", "year_id", "sex_id", "run_id")]
old_ap_pops[, location_id := 44849]
gbd_pop <- rbindlist(list(gbd_pop, old_ap_pops), use.names = T)


locations <- get_location_metadata(location_set_id=82)[,c("location_id", "ihme_loc_id")]
gbd_pop <- merge(gbd_pop, locations, by = "location_id", all.x = TRUE)
gbd_pop[,sex_id:= as.factor(sex_id)]
levels(gbd_pop$sex_id) <- c("male", "female", "both")
setnames(gbd_pop, c("sex_id", "year_id"), c("sex", "year"))

pop <- fread(paste0(out_dir, "d00_compiled_population_pre_scaled.csv"))
pop <- pop[!is.na(mean)]
pop <- pop[year >= 1950]
pop <- pop[!grepl("CENS|VR", source_type)]


age_map <- data.table(get_age_map("all"))
id_vars <- c("ihme_loc_id", "source_type", "pop_source", "sex", "pop_nid", "underlying_pop_nid", "year")
aggregated_pop <- agg_granular_age_data(pop, id_vars, age_map, agg_all_ages = T, agg_u5 = F, agg_15_60 = F)


aggregated_pop <- merge(aggregated_pop, gbd_pop[, c("ihme_loc_id", "year", "sex", "population")], 
                                                  by = c("ihme_loc_id", "year", "sex"), all.x = TRUE)
aggregated_pop[, correction_factor := population/mean]
aggregated_pop <- aggregated_pop[!is.na(correction_factor)]

save_path <- paste0(out_dir, "correction_factors.csv")
write_csv(aggregated_pop, save_path)
print(paste0("correction factors saved here: ", save_path))



aggregated_pop$exists <- 1
merged <- merge(pop, 
                aggregated_pop[, c("ihme_loc_id", "source_type", "pop_source", "sex", "pop_nid", "underlying_pop_nid", "year", "exists")], 
                by = c("ihme_loc_id", "source_type", "pop_source", "sex", "pop_nid", "underlying_pop_nid", "year"), 
                all.x=TRUE)
no_scalars <- merged[is.na(exists)]
no_scalars$exists <- NULL
no_scalars <- unique(no_scalars[, c("ihme_loc_id", "source_type", "pop_source", "sex", "pop_nid", "underlying_pop_nid", "year")])
save_path_no_scalars <- paste0(out_dir, "no_correction_factors.csv")
write_csv(no_scalars, save_path_no_scalars)
print(paste0("missing correction factors list saved here: ", save_path_no_scalars))

#DONE