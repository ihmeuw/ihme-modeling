
# assign year id ----------------------------------------------------------

assign_year_id <- function(input_df){
  df <- copy(input_df)
  if(all(df$year_start <= df$year_end)){
    df$orig_year_start <- df$year_start
    df$orig_year_end <- df$year_end
    df$year_id <- df$year_end <- df$year_start
    return(df)
  }else{
    stop("Not all year_start are less than or equal to year_end.")
  }
}

# assign elevation year ---------------------------------------------------

assign_elevation_year <- function(year_vec){
  i_vec <- which(year_vec < 1980)
  year_vec[i_vec] <- 1980
  return(year_vec)
}