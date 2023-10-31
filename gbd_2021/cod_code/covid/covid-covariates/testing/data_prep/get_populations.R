#' Pull population data 
#' @param hierarchy Location hierarchy to use for aggregation
#' @param data_version Version to pull from model-inputs
#' @return data.table of populations for each location including aggregates
get_populations <- function(hierarchy, data_version) {
  dt_pop <- unique(fread(file.path(data_version, "age_pop.csv")))
  dt_pop <- dt_pop[, .(pop = sum(population)), by = "location_id"]
  missing_pop_aggregates <- setdiff(hierarchy[most_detailed == 0, location_id], dt_pop$location_id)
  for (parent_loc in missing_pop_aggregates) {
    parent_ids <- strsplit(hierarchy[most_detailed == 1, path_to_top_parent], ",")
    children_ids <- hierarchy[most_detailed == 1][which(unlist(lapply(parent_ids, function(x) as.character(parent_loc) %in% x)))][, location_id]
    tmp_pop <- copy(dt_pop[location_id %in% children_ids])
    tmp_pop <- tmp_pop[, .(pop = sum(pop), location_id = parent_loc)]
    dt_pop <- rbind(dt_pop, tmp_pop)
  }
  if (length(setdiff(hierarchy$location_id, unique(dt_pop$location_id))) > 1) {
    warning(paste0(
      "Missing the following locations from population data:",
      paste(hierarchy[location_id %in% setdiff(hierarchy$location_id, unique(dt_pop$location_id)), location_name], collapse = ", "),
      " for location_set_version_id: ", lsvid
    ))
  }
  return(dt_pop)
}
