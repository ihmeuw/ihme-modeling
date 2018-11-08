########################################################
# Overwrite zeroes for Americas with last year's results#
########################################################

# load packages
library(data.table)

# source central functions
source("FILEPATH/get_draws.R")
source("FILEPATH/get_demographics.R")


# create vector of meids need files for
meids <- c(1494, 1495, 2620, 1496, 2515, 2621, 1497, 1498, 1499)


demographics <- get_demographics(gbd_team = "ADDRESS")
# vector only locations need to overwrite for Americas estimates that need to be run
gbdlocs <-  c(130, 133, 135, 125, 122, 128)
gbdyears <- demographics$year_id # get gbd years for this year
gbdsexes <- demographics$sex_id # get sex_ids needed
gbdages <- demographics$age_group_id # get age groups needed


for (meid in meids) {
  print(meid)
  # get_draws from last years model
  draws_2016 <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = meid, source = "ADDRESS", measure_id = 5, year_id = c(1990, 1995, 2000, 2005, 2010, 2016),
                            age_group_id = gbdages, gbd_round_id = 4, location_id = gbdlocs)

  draws_2017 <- draws_2016[year_id == 2016, year_id := 2017]

  draws_2017[, measure_id := 5]
  draws_2017[, modelable_entity_id := meid]


  # delete two extra columns we do not need for uploading results
  # metric_id model_version_id to be deleted now...
  draws_2017[, metric_id := NULL]
  draws_2017[, model_version_id := NULL]

  # order columns
  draw_names <- grep("draw", names(draws_2017), value = TRUE)
  other_cols <- c("modelable_entity_id", "measure_id", "location_id", "year_id", "age_group_id", "sex_id")
  setcolorder(draws_2017, append(other_cols, draw_names))

  for (loc in unique(draws_2017$location_id)) {

    draw <- draws_2017[location_id == loc]
    draw <- draw[modelable_entity_id == meid]
    # Output
    if (file.exists(paste0("FILEPATH", meid, "/", loc, ".csv"))) {
      file.remove(paste0("FILEPATH", meid, "/", loc, ".csv"))
    }
    fwrite(draw, paste0("FILEPATH", meid, "/", loc, ".csv"))
  }


}
