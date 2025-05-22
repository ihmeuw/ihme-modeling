# CHECK FOR AND REMOVE NON-DISMOD LOCATIONS
find_nondismod_locs <- function(raw_dt) {
  dt <- copy(raw_dt)
  locs <- get_location_metadata(9, gbd_round_id = gbd_round_id, decomp_step = ds)
  nondismod_locs <- dt[!(location_id %in% locs$location_id) & (group_review == 1 | is.na(group_review))]
  if (nrow(nondismod_locs) > 0) {
    print(paste0("The following non-DisMod location rows are being removed: ", unique(nondismod_locs$location_name)))
    dt <- dt[!location_id %in% nondismod_locs$location_id]
  } else {
    print(paste0("No non-DisMod locations exist"))
    dt <- dt
  }
  return(dt)
}