#' @title Subtract shocks from a VR dataset
#'
#' @description Subtract deaths attributed to shocks from mortality VR data
#'
#' @param dataset \[`data.table()`\]\cr
#'   VR dataset containing standard id columns (including source type) and deaths
#'
#' @param shocks \[`data.table()`\]\cr
#'   shocks dataset containing standard id columns and "numkilled"
#'
#' @return \[`data.table()`\]\cr
#'   Dataset where deaths have had shocks subtracted if possible
#'

vr_shocks_subtraction <- function(dataset, shocks) {

  dt <- copy(dataset)

  # only subtract from VR where we are sure the data is complete enough
  dt_vr <- dt[source_type == "VR"]
  dt_other <- dt[!source_type == "VR"]

  # shocks subtraction
  dt_vr <- merge(
    dt_vr,
    shocks,
    by = c("location_id", "year_id", "age_group_id", "sex_id"),
    all.x = TRUE
  )
  dt_vr[deaths >= numkilled, deaths_noshock := deaths - numkilled]
  dt_vr[((numkilled - 1) <= deaths) & (deaths < numkilled), deaths_noshock := 0]
  dt_vr[((0.6*numkilled) <= deaths) & (deaths < (numkilled - 1)), deaths_noshock := deaths - (0.6*numkilled)]
  dt_vr[deaths < (0.6*numkilled) | is.na(numkilled), deaths_noshock := deaths]

  # format
  dt_vr[, ':=' (deaths = deaths_noshock, numkilled = NULL, deaths_noshock = NULL)]

  # combine
  dt <- rbind(dt_vr, dt_other)

  # checks
  assertable::assert_values(dt, "deaths", test = "gte", test_val = 0)
  demUtils::assert_is_unique_dt(dt, names(dt))

  return(dt)

}

#' @title Aggregate shocks from shocks aggregator
#'
#' @description CoD passes off shocks as draws so we need to aggregate ourselves
#'
#' @param base_dir \[`data.table()`\]\cr
#'   base directory to save new shocks file
#'
#' @param process_name \[`data.table()`\]\cr
#'   identifies specific process folder to save new shocks file
#'
#' @param shocks_version \[`data.table()`\]\cr
#'   version_id for shocks
#'
#' @param shock_years \[`data.table()`\]\cr
#'   years to calculate shocks for
#'
#' @param locs \[`data.table()`\]\cr
#'   locations to calculate shocks for
#'
#' @return \[`data.table()`\]\cr
#'   dataset of aggregated shocks by location-year-age-sex
#'
#' @details

aggregate_shocks <- function(base_dir, process_name, shocks_version, shock_years, locs) {

  id_vars <- c("location_id", "sex_id", "year_id", "age_group_id")

  dir.create(fs::path("FILEPATH"))

  shocks <- lapply(locs$location_id, function(loc) {

    print(loc)

    shocks_file <- paste0("FILEPATH")
    shocks <- lapply(shocks_file, fread) |> rbindlist()
    shocks <- shocks[cause_id == 294]

    shocks <- melt(
      shocks,
      id.vars = id_vars,
      variable.name = "draw",
      variable.factor = FALSE,
      measure.vars = grep("draw", names(shocks), value = TRUE),
      value.name = "shock_deaths"
    )

    shocks[, .(shock_deaths = mean(shock_deaths)), by = id_vars]

  }) |> rbindlist()

  readr::write_csv(
    shocks,
    fs::path("FILEPATH")
  )

}
