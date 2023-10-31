rake_vals <- function(out_dt, child_locs, parent_loc, var) {
  child_dt <- out_dt[location_id %in% child_locs] # fix to use a location hierarchy
  child_min <- max(child_dt[, .(min_date = min(date)), by = .(location_id)]$min_date)
  parent_dt <- out_dt[location_id == parent_loc]
  # Subset to dates of alignment
  intersect_dates <- sort(intersect(unique(child_dt[date >= child_min]$date), parent_dt$date))
  child_dt <- child_dt[date %in% intersect_dates]

  # Get a matrix of sub-national proportions over time (location X time period)
  child_cast <- dcast(child_dt[, c("location_id", "date", var), with = F],
    location_id ~ date,
    value.var = var
  )
  child_mat <- as.matrix(child_cast[, 2:ncol(child_cast)])
  child_prop <- sweep(child_mat, 2, colSums(child_mat, na.rm = T), "/")
  # colSums(child_prop, na.rm =T)

  # Get a vector of national counts
  parent_dt <- parent_dt[date %in% intersect_dates]
  parent_count <- parent_dt[[var]]

  # Calculate raked counts
  raked_mat <- sweep(child_prop, 2, parent_count, "*")
  raked_vec <- c(t(raked_mat))

  out_dt[
    location_id %in% child_locs & date %in% intersect_dates,
    paste0("raked_", var) := raked_vec
  ]

  return(out_dt[])
}
