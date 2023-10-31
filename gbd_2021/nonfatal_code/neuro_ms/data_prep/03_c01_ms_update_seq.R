update_seq <- function(dt) {
  
  if ("crosswalk_parent_seq" %in% names(dt)) {
    dt[is.na(crosswalk_parent_seq), `:=` (crosswalk_parent_seq = seq, seq = NA)]
    dt[!is.na(crosswalk_parent_seq), seq := NA]
  } else {
    dt[ , `:=` (crosswalk_parent_seq = seq, seq = NA)]
  }
  
  return(dt)
  
}