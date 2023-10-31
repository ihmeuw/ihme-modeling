library(haven)
source("/FILEPATH/get_location_metadata.R")

locations <- get_location_metadata(location_set_id=35, gbd_round_id = 7, decomp_step = 'iterative')
locations <- locations$location_id[locations$is_estimate==1]

cod_draws <- read_dta("/FILEPATH/death_draws_codcorrect.dta")
cod_draws <- subset(cod_draws, measure_id==1)
cod_draws <- cod_draws[cod_draws$location_id %in% locations,]

dir <- paste0(getwd(), "/FILEPATH/")
for (i in 1:length(locations)) {
  loc <- locations[i]
  subset <- cod_draws[cod_draws$location_id == loc,]
  write.csv(subset, paste0(dir, loc, ".csv"), row.names=FALSE)
}

