#' @title Correct old source type names
#'
#' @description Uses DDM method to correct old source type names to more standard versions
#'
#' @param dataset \[`data.table()`\]\cr
#'   VR dataset containing standard id columns including source_type_id
#'
#' @return \[`data.table()`\]\cr
#'   Dataset where old source type ids have been replaced with new ones
#'

source_type_id_correction <- function(dataset) {

  loc_map <- demInternal::get_locations()
  source_map <- demInternal::get_dem_ids("source_type")

  dt <- copy(dataset)

  dt[loc_map, ihme_loc_id := i.ihme_loc_id, on = "location_id"]
  dt[source_map, source_type_name := i.source_type_name, on = "source_type_id"]

  # use short source type name
  dt[, source_type := source_type_name]

  # adjust source type to simplify
  dt[source_type_id == 16, ':=' (source_type_id = 58, source_type = "Survey")]
  dt[source_type_id %in% c(18:19, 24:33, 53:54, 59:60), ':=' (source_type_id = 1, source_type = "VR")]
  dt[source_type_id %in% 20:23, ':=' (source_type_id = 3, source_type = "DSP")]
  dt[source_type_id %in% 44:49, ':=' (source_type_id = 2, source_type = "SRS")]

  # re-categorize BWA
  dt[source_type_id == 35 & ihme_loc_id == "BWA", ':=' (source_type_id = 1, source_type = "VR")]

  dt[, ':=' (ihme_loc_id = NULL, source_type_name = NULL)]

  return(dt)

}
