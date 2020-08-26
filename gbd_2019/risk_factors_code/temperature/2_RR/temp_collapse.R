# Summary: This function loops over all year of VR data, imports the corresponding temperature data, merges the two,
# and then collapses the result to produce a dataset of cause-specific death counts and population totals (person-days)
# by location, date, age, sex, and temperature category
#
# Requires: data.table (should have already been loaded by the master script)




temp_collapse <- function(dt, paths, iso3, collapse = F) {

  vr <- copy(dt)

  cat("Merging in temperature data & collapsing data by location, year, age, sex, and daily temperature: ", "\n")
  vr_append <- data.table()

  # loop through all years present in the VR dataset
  for (y in unique(vr[, year])) {

    # restrict data to only the year being processed
    vr_temp <- copy(vr)
    vr_temp <- vr_temp[year==y, ]

    cat(y)

    # load temperature data for the year and format for merging
    temperature <- fread(paste0(paths$tempPath, iso3, "_temperature_", year, ".csv"))[, .(date, zonecode, temp, population)]

    temperature[, date := as.Date(date)]

    # collapse to get sums by location and date 
    vr_temp <- vr_temp[, lapply(.SD, sum), by = .(location_id, zonecode, year, date), .SDcols = c(grep("^n_", names(vr_temp), value = T))]
    
    # merge temperature and VR data
    vr_temp <- merge(vr_temp, temperature, by = c('zonecode', 'date'), all.x = T)

    if (collapse==T) {
      # create daily temperature category variable
      vr_temp[, daily_temp_cat := as.integer(round(temp))]

      # collapse to sum deaths and population by location, age, sex, and temperature category
      vr_temp <- vr_temp[, lapply(.SD, sum), by = .(location_id, zonecode, year, age, sex, daily_temp_cat), .SDcols = c(grep("^n_", names(vr_temp), value = T), "population")]
    }

    # append to data for other years (if any)
    vr_append <- rbind(vr_append, vr_temp)
  }

  cat("\n", "Done", "\n", "\n")

  return(vr_append)
}
