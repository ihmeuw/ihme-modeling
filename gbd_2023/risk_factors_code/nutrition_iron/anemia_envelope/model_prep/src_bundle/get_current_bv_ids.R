
# get most recent bundle version IDs --------------------------------------

get_current_bv_ids <- function(input_bundle_map){
  bundle_map <- copy(input_bundle_map)
  bundle_map$bv_id <- NA_integer_
  for(r in seq_len(nrow(bundle_map))){
    id_df <- ihme::get_elmo_ids(bundle_id = bundle_map$id[r])
    bv_id <- max(id_df$bundle_version_id)
    bundle_map$bv_id[r] <- bv_id
  }
  return(bundle_map)
}

