
# source libraries --------------------------------------------------------

library(data.table)

# functions to impute missing mean, se, and cases -------------------------

impute_missing_statistics <- function(input_df){
  df <- copy(input_df)
  
  df <- impute_cases(df)
  df <- impute_standard_error(df)
  df <- impute_upper_lower(df)
  
  return(df)
}

impute_cases <- function(input_df){
  df <- copy(input_df)
  
  i_vec <- which(
    df$measure == "prevalence" &
      is.na(df$cases)
  )
  
  df$cases[i_vec] <- df$mean[i_vec] * df$sample_size[i_vec]
  
  return(df)
}

impute_standard_error <- function(input_df){
  df <- copy(input_df)
  i_vec <- which(
    df$measure == "prevalence" &
      !(is.na(df$mean)) &
      !(is.na(df$sample_size)) &
      df$sample_size > 0 &
      is.na(df$standard_error)
  )
  
  df$standard_error[i_vec] <- sqrt(
    df$mean[i_vec] * (1 - df$mean[i_vec]) / df$sample_size[i_vec]
  )
  
  i_vec <- which(
    !(is.na(df$upper)) &
      !(is.na(df$lower)) &
      is.na(df$standard_error)
  )
  
  df$standard_error[i_vec] <- (df$upper[i_vec] - df$lower[i_vec]) / (2 * qnorm(0.975))
  
  return(df)
}

impute_upper_lower <- function(input_df){
  df <- copy(input_df)
  
  bound_vec <- c("lower", "upper")
  for(b in 1:length(bound_vec)){
    i_vec <- which(
      !(is.na(df$mean)) &
        !(is.na(df$standard_error)) &
        is.na(df[[bound_vec[b]]])
    )
    
    df[[b]][i_vec] <- df$mean[i_vec] + df$standard_error[i_vec] * qnorm(0.975) * (-1 ^ b)
  }
  
  return(df)
}