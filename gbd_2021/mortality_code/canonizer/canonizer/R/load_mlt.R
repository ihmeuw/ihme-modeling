#' Load MLT estimates
#'
#' @param loc_id Location to load
#' @param lt_type Life table type to load, either `"hiv_free"` or `"with_hiv"`
#' @param year_range Years to load
#' @param age_map Age mapping table with required ages
#' @param dir_mlt Main directory of MLT output files
#'
#' @return Table of MLT estimates.
#'
#' @export
load_mlt <- function(loc_id, lt_type, year_range, age_map, dir_mlt) {

  stopifnot(isTRUE(lt_type %in% c("hiv_free", "with_hiv")))

  dir_input <- fs::path(dir_mlt, paste0("lt_", lt_type), "scaled")
  file_input <- paste0(lt_type, "_lt_", year_range, ".h5")
  path_input <- fs::path(dir_input, file_input)

  stopifnot(all(fs::file_exists(path_input)))

  dt_mlt <- path_input |>
    lapply(\(x) mortcore::load_hdf(x, by_val = loc_id)) |>
    data.table::rbindlist(use.names = TRUE)

  data.table::setnames(dt_mlt, c("sim", "year"), c("draw", "year_id"))
  dt_mlt <- dt_mlt[sex != "both"]
  dt_mlt[, sex_id := ifelse(sex == "male", 1, 2)]
  dt_mlt[, sex := NULL]

  stopifnot(all(sort(unique(dt_mlt$age)) == sort(age_map$age_start)))

  dt_mlt[
    age_map,
    age_length := i.age_end - i.age_start,
    on = list(age = age_start)
  ]

  dt_mlt[age == age_map[is.infinite(age_end), age_start], qx := 1]
  dt_mlt[, px := 1 - qx]

  data.table::setcolorder(
    dt_mlt,
    c("location_id", "year_id", "sex_id", "age", "age_length")
  )

  return(dt_mlt)

}
