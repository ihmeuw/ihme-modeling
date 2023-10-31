#' Get the data of the first confirmed case for each location
#' @param locs Vector of locations to check for in the data
#' @param data_version Version to pull from model-inputs
#' @return A data.table with a date of first case for each location
get_first_case_date <- function(locs, data_version) {
  dt_cases <- fread(file.path(data_version, "full_data_unscaled.csv"))
  # dt_cases <- dt_cases[, .(location_id, admin_1 = get("Province/State"), admin_0 = get("Country/Region"), date = as.Date(Date), Confirmed)]
  dt_cases <- dt_cases[, .(location_id, location_name, date = as.Date(Date), Confirmed)]
  dt_cases <- dt_cases[!is.na(Confirmed) & Confirmed > 0]
  dt_cases <- merge(dt_cases, hierarchy[,c("location_id","parent_id","level")], by = "location_id")
  out <- dt_cases[, .(first_case_date = min(date)), by = c("location_id")]

  # Create admin 0 for aggregate locs
  # out_admin_0 <- dt_cases[admin_0 %in% hierarchy[, location_name], .(first_case_date = min(date)), by = "admin_0"]
  # out_admin_0 <- merge(out_admin_0, hierarchy[, .(location_id, location_name)], by.x = "admin_0", by.y = "location_name", all.x = T)
  # out_admin_0 <- out_admin_0[location_id %in% setdiff(hierarchy$location_id, out$location_id)]
  # out_admin_0[, admin_0 := NULL]
  out_admin_0 <- dt_cases[parent_id %in% hierarchy[, location_id], .(first_case_date = min(date)), by = "parent_id"]
  setnames(out_admin_0, "parent_id", "location_id")
  # out_admin_0 <- merge(out_admin_0, hierarchy[, .(location_id, location_name)], by = "location_id",  all.x = T)
  out_admin_0 <- out_admin_0[location_id %in% setdiff(hierarchy$location_id, out$location_id)]
  out_admin_0[, admin_0 := NULL]

  # Create washington
  # dt_cases <- fread(file.path(data_version, "full_data.csv"))
  # dt_cases <- dt_cases[, .(location_id, admin_1 = get("Province/State"), admin_0 = get("Country/Region"), date = as.Date(Date), Confirmed)]
  out_wa <- data.table(location_id = 570, first_case_date = dt_cases[location_id %in% c(60886, 3539, 60887), min(date)])

  out <- rbindlist(list(out, out_admin_0, out_wa), use.names = T)

  if (length(setdiff(locs, unique(out$location_id))) > 1) {
    warning(paste0(
      "Missing the following locations from case data:",
      paste(hierarchy[location_id %in% setdiff(locs, unique(out$location_id)), location_name], collapse = ", "),
      " for location_set_version_id: ", lsvid
    ))
  }

  return(out)
}
