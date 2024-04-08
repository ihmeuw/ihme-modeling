get_current_cc_draws <- function(file_results,
                                 cause_id,
                                 run_id_oprm,
                                 loc_id,
                                 current_year,
                                 loc_map,
                                 dir_cc_covid = "FILEPATH") {

  # balancing equation outputs only contain lowest level locations
  # so we need to aggregate up to the current location if it has subnationals
  # function loads data and subsets to current location

  dt <- fread(fs::path(dir_cc_covid, run_id_oprm, "pipeline_draws", file_results))

  dt_current <- dt[location_id == loc_id & year_id == current_year]

  # if current location has no data, aggregate up to it
  if (nrow(dt_current) == 0) {

    # find the lowest level locations which can be used to aggregate up to
    # the current location
    parent_locs <- loc_id

    while (nrow(dt_current) == 0) {

      # get children of parent locs
      child_locs <- loc_map[parent_id %in% parent_locs, location_id]

      # check if these locations are present in results
      dt_current <- dt[location_id %in% child_locs]

      # set parent_locs to current child locs
      parent_locs <- child_locs
    }

    dt_current <- dt_current[
      year_id == current_year,
      lapply(.SD, sum),
      by = c("year_id", "sex_id", "age_group_id")
    ]
    dt_current[, location_id := loc_id]

  }

  # add cause_id
  dt_current[, cause_id := cause_id]

  # also melt to long
  dt_current <- melt(
    data = dt_current,
    id.vars = c(id_cols, "cause_id"),
    measure.vars = patterns("draw"),
    variable.name = "draw",
    variable.factor = FALSE,
    value.name = "deaths"
  )
  dt_current[, draw := substr(draw, 6, nchar(draw)) |> as.integer()]


  return(dt_current)
}