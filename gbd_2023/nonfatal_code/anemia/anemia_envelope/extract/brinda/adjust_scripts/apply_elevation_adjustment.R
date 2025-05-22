# source libraries --------------------------------------------------------

library(data.table)

# elevation adjustment functions ------------------------------------------

apply_elevation_adjustment <- function(input_df) {
  df <- copy(input_df)

  i_vec <- which(!(is.na(df$cluster_altitude)) &
    !(is.na(df$hemoglobin_raw)))

  hb_adj_cols <- c("brinda_adj_hemog", "who_adj_hemog")
  for (h in hb_adj_cols) {
    set(
      x = df,
      i = i_vec,
      j = h,
      value = df$hemoglobin_raw[i_vec] - 
        adj_brinda_func(
          elevation = df$cluster_altitude[i_vec], 
          adj_function_name = h
        )
    )
  }

  return(df)
}

adj_brinda_func <- function(elevation, adj_function_name) {
  val <- switch(adj_function_name,
    "brinda_adj_hemog" = 0.0000003 * elevation**2 + 0.0056384 * elevation,
    "who_adj_hemog" = .00000257 * elevation**2 - .00105 * elevation
  )
  return(val)
}
