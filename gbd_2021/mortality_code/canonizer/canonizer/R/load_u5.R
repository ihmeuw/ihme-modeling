#' Load Under-5 envelope estimates
#'
#' @param loc_id Location to load
#' @param age_map table mapping age group name abbreviations to age group IDs
#'   and age group starts.
#' @param dir_u5 Directory of estimate files.
#'
#' @return Table of u5 estimates.
#'
#' @export
load_u5 <- function(loc_id, age_map, dir_u5) {

  dt_u5 <- data.table::fread(fs::path(dir_u5, "draws", loc_id, ext = "csv"))

  data.table::setnames(dt_u5, c("year", "sim"), c("year_id", "draw"))
  dt_u5[, sex_id := fifelse(sex == "male", 1, 2)]
  dt_u5[, sex := NULL]

  death_cols <- paste0("deaths", age_map$age_group_name_short)
  stopifnot(all(death_cols %in% names(dt_u5)))

  dt_u5_prep <- melt(
    dt_u5,
    id.vars = c("year_id", "draw", "sex_id"),
    measure.vars = paste0("deaths", age_map$age_group_name_short),
    variable.name = "age_group_name_short",
    variable.factor = FALSE,
    value.name = "deaths"
  )

  dt_u5_prep[, age_group_name_short := gsub("^deaths", "", age_group_name_short)]
  dt_u5_prep[
    age_map,
    `:=`(
      age_group_id = i.age_group_id,
      age_start = i.age_start
    ),
    on = "age_group_name_short"
  ]

  stopifnot(!anyNA(dt_u5_prep))

  dt_u5_prep[, age_group_name_short := NULL]
  dt_u5_prep[, location_id := loc_id]

  data.table::setcolorder(
    dt_u5_prep,
    c("location_id", "year_id", "sex_id", "draw", "age_group_id", "age_start")
  )

}
