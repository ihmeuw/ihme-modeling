
### saving 0 for incidence for comp cirrhosis due to HBV and HCV

# set up environment
rm(list=ls())
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- '/home/j/' 
  h_root <- '~/'
} else { 
  j_root <- 'J:/'
  h_root <- 'H:/'
}

shell <- "FILEPATH" 
draws <- paste0("draw_", 0:999)


# source central functions
source("FILEPATH/get_draws.R")

# define objects

## objects for causes, locations, directories to send draws to
args<-commandArgs(trailingOnly = TRUE)
meid <- args[1]
location<-args[2]
output_dir <-args[3] 

##---------------------------------------------------------------------------------------------------------------------------

## objects for getting draws
age_groups <- c(2:3, 6:20, 30:32, 34, 388, 389, 238, 235)

#pulls in prevalence and incidence from MEID
comp_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=meid, source="epi", location_id=location, measure_id=c(5, 6), age_group_id=age_groups, release_id = 16)
final_draws <- copy(comp_draws)
final_draws[(measure_id == 6), (draws) := lapply(.SD, function(x) x*0), .SDcols = draws]

final_draws$crosswalk_version_id <-OBJECT 

## save draws 
write.csv(final_draws, file.path(output_dir, paste0(location, ".csv")), row.names = F)

