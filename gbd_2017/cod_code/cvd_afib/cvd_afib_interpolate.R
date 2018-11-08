.libPaths(paste0(hpath, "Rlibrary"))
library(R.utils)

#Parallelization
argue <- commandArgs(trailingOnly = T)
location <- as.numeric(argue[1])

#Source shared functions
suppressMessages(sourceDirectory("FILEPATH/r/"))

#Set paths
filedir <- "FILEPATH/cvd_afib/"

#Interpolate!
df <- data.frame(interpolate(gbd_id_type='modelable_entity_id', gbd_id=9366, source='epi', measure_id=15, location_id=location, 
							 gbd_round_id=5, reporting_year_start=1980, reporting_year_end=2017))
df <- subset(df, age_group_id %ni% c(27,164))
df$measure_id <- 1
write.csv(df, file=paste0(filedir, "interpolated_", location, ".csv"))
