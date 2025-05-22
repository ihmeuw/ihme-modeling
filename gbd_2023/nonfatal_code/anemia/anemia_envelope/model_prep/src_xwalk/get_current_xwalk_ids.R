
# get most recent xwalk IDs -----------------------------------------------

get_current_xwalk_ids <- function(input_bundle_map){
  bundle_map <- copy(input_bundle_map)
  bundle_map$xwalk_id <- NA_integer_
  for(r in seq_len(nrow(bundle_map))){
    id_df <- ihme::get_elmo_ids(bundle_id = bundle_map$id[r])
    if(!(all(is.na(id_df$crosswalk_version_id)))){
      xwalk_id <- max(id_df$crosswalk_version_id, na.rm = TRUE)
      bundle_map$xwalk_id[r] <- xwalk_id
    }
  }
  return(bundle_map)
}
