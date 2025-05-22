
# assign mean column ------------------------------------------------------

mean_hot_fix <- function(input_df){
  df <- copy(input_df)
  i_vec <- is.na(df$mean) |
    (
      df$measure == 'continuous' &
        (df$mean < 25 | df$mean > 250)
    ) |
    (
      df$measure == 'proportion' &
        (df$mean < 0 | df$mean > 1)
    )
  i_vec <- which(!i_vec)
  
  return(df[i_vec, ])
}

assign_mean <- function(mean_vec, measure_vec){
  i_vec <- is.na(mean_vec) |
    (
      measure_vec == 'continuous' &
        (mean_vec < 25 | mean_vec > 250)
    ) |
    (
      measure_vec == 'proportion' &
        (mean_vec < 0 | mean_vec > 1)
    )
  validation_flag <- any(i_vec)
  
  if(!validation_flag){
    return(mean_vec)
  }else{
    assign("invalid_mean_index", which(i_vec), envir = .GlobalEnv)
    stop("Mean is undefined or or has invalid bounds. Please check and rerun.")
  }
}