# Sum selected columns (agg_cols) to the national level

aggregate_to_national <- function(dt, hierarchy=NULL, agg_cols, id_cols){
  if(is.null(hierarchy)) hierarchy <-  get_location_metadata(35, gbd_round_id = 7, decomp_step = "iterative")
  loc_estimate <- hierarchy[most_detailed == 1]
  l <- strsplit(as.character(loc_estimate$path_to_top_parent), ',')
  # get just the countries
  l <- lapply(l, function(str){str <- as.integer(str[4])})
  df1new <- data.frame(country_id = as.integer(unlist(l)), 
                       path_to_top_parent = rep(loc_estimate$path_to_top_parent, lengths(l)),
                       location_id = rep(loc_estimate$location_id, lengths(l)))
  dt <- merge(dt, df1new, by = "location_id", all.x = TRUE, allow.cartesian = T)
  # Sum relevant columns
  dt <- dt[, lapply(.SD, sum), by = c(id_cols, "country_id"), .SDcols = agg_cols]
  setnames(dt, "country_id", "location_id")
  return(dt)
}