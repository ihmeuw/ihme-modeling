#' Get terminal age for life tables from empirical deaths output
#' 
#' @param death_number_empirical_version integer, version_id for empirical deaths
#'
#' @return returns data.table with columns ihme_loc_id, year, sex, source_type, terminal_age
#' @export
#'
#' @examples
#' @import data.table
#' @import mortdb

get_unsplit_terminal_age <- function(death_number_empirical_version){

  dt <- "FILEPATH"

  # Map age_group_id to age
  age_map <- get_age_map("all")
  dt <- merge(dt, age_map[, .(age_group_id, age_group_name)], by = "age_group_id", all.x = T)

  # Map location_id to ihme_loc_id
  loc_map <- get_locations(gbd_year = 2020)
  dt <- merge(dt, loc_map[, .(location_id, ihme_loc_id)], by = "location_id", all.x = T)

  # Map source_type_id to source_type
  source_map <- get_mort_ids("source_type")
  dt <- merge(dt, source_map[, .(source_type_id, type_short)] , by = "source_type_id", all.x = T)
  setnames(dt, "type_short", "source_type")
  dt[source_type %like% "VR" , source_type := "VR"]
  dt[source_type %like% "SRS", source_type := "SRS"]
  dt[source_type %like% "DSP", source_type := "DSP"]
  dt[source_type %like% "Civil Registration", source_type := "Civil Registration"]
  dt <- dt[source_type %in% c("VR", "SRS", "DSP", "Civil Registration")]

  # Map sex_id to sex
  dt[sex_id == 1, sex := "male"]
  dt[sex_id == 2, sex := "female"]

  # Map year_id to year
  setnames(dt, "year_id", "year")

  # Keep only terminal age
  dt <- dt[grep("plus", age_group_name)]

  # Subset columns and format
  dt <- dt[, c("ihme_loc_id", "year", "sex", "source_type", "age_group_name")]
  setnames(dt, "age_group_name", "terminal_age")
  dt[, terminal_age := as.numeric(gsub(" plus", "", terminal_age))]

  # return dt with ihme_loc_id, year, sex, source_type, terminal_age
  return(dt)

}

#' Aggregate age-split empirical deaths. For extension of ELTs we only want to use pre-split data.
#' 
#' @param dt data.table with columns ihme_loc_id, year, sex, source_type, age, age_group_name, deaths
#' @param unsplit_terminal_ages data.table specifying the terminal ages of pre-age-split data with columns ihme_loc_id, year, sex, source_type, terminal_age
#'
#' @return returns dt but with any age-split data as specified in unsplit_terminal_ages re-aggregated
#' @export
#'
#' @examples
#' @import data.table
#' @import mortdb

agg_age_split_deaths <- function(dt, unsplit_terminal_ages){

  byvars <- c("ihme_loc_id", "year", "sex", "source_type")

  dt <- merge(dt, unsplit_terminal_ages, by = byvars, all.x = T)

  over <- dt[age >= terminal_age, deaths := sum(deaths), by = byvars]
  over <- unique(over, by = byvars)
  over[, age_group_name := paste0(terminal_age, " plus")]

  dt <- rbind(dt, over)
  dt <- dt[order(byvars, age)]

  return(dt)

}
