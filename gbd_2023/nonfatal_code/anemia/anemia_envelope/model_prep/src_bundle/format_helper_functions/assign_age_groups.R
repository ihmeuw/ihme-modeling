
# assign age group ids ----------------------------------------------------

assign_age_group_ids <- function(input_df){
  df <- copy(input_df)
  df <- get_original_ages(df)
  
  validation_flag <- any(
    is.na(df$age_start) |
      is.na(df$age_end) |
      df$age_start > df$age_end
  )
  
  if(!validation_flag){
    df$orig_age_start <- df$age_start
    df$orig_age_end <- df$age_end
    
    df$age_start <- 999
    df$age_end <- 999
    df$age_group_id <- 999
  }else{
    stop("age_start, age_end not defined or age_start>age_end. Please check and rerun.")
  }
  
  return(df)
}

get_original_ages <- function(input_df){
  df <- copy(input_df)
  
  i_vec <- which((df$age_group_id == 999 |
                    df$age_end == 999 |
                    df$age_start == 999) &
                   !(is.na(df$orig_age_start)) &
                   !(is.na(df$orig_age_end)))
  df$age_start[i_vec] <- df$orig_age_start[i_vec]
  df$age_end[i_vec] <- df$orig_age_end[i_vec]
  
  return(df)
}