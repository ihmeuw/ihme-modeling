library(data.table)

# impute missing stats ----------------------------------------------------

impute_missing_statistics <- function(input_df, se_hot_fix){
  df <- copy(input_df)
  
  df <- impute_cases(df)
  df <- impute_standard_error(df, se_hot_fix)
  df <- impute_upper_lower(df)
  df$variance <- calculate_variance(df$standard_error)
  
  return(df)
}

# impute cases for proportion measures ------------------------------------

impute_cases <- function(input_df){
  df <- copy(input_df)
  
  i_vec <- which(
    df$measure == "proportion" &
      is.na(df$cases)
  )
  
  df$cases[i_vec] <- df$val[i_vec] * df$sample_size[i_vec]
  
  return(df)
}

# impute standard errors --------------------------------------------------

impute_standard_error <- function(input_df, se_hot_fix){
  df <- copy(input_df)
  i_vec <- which(
    df$measure == "proportion" &
      !(is.na(df$val)) &
      !(is.na(df$sample_size)) &
      df$sample_size > 0 &
      is.na(df$standard_error)
  )
  
  df$standard_error[i_vec] <- sqrt(
    df$val[i_vec] * (1 - df$val[i_vec]) / df$sample_size[i_vec]
  )
  
  i_vec <- which(
    !(is.na(df$upper)) &
      !(is.na(df$lower)) &
      is.na(df$standard_error)
  )
  
  df$standard_error[i_vec] <- (df$upper[i_vec] - df$lower[i_vec]) / (2 * qnorm(0.975))
  
  if(se_hot_fix){
    i_vec <- which(
      is.na(df$standard_error) &
        df$sample_size < 1000
    )
    df$standard_error[i_vec] <- 2.5
    
    i_vec <- which(
      is.na(df$standard_error) &
        df$sample_size >= 1000
    )
    df$standard_error[i_vec] <- 1.25
  }
  
  return(df)
}

# impute upper and lower using SE with a 95% CI ---------------------------

impute_upper_lower <- function(input_df){
  df <- copy(input_df)
  
  bound_vec <- c("lower", "upper")
  for(b in 1:length(bound_vec)){
    i_vec <- which(
      !(is.na(df$val)) &
        !(is.na(df$standard_error)) &
        is.na(df[[bound_vec[b]]])
    )
    
    df[[bound_vec[b]]][i_vec] <- df$val[i_vec] + df$standard_error[i_vec] * qnorm(0.975) * ((-1) ^ b)
  }
  
  if(all(df$upper <= df$lower)){
    stop("All upper and lower values match. Please check and rerun.")
  }
  
  return(df)
}

# calculate variance from SE ----------------------------------------------

calculate_variance <- function(se_vec){
  if(any(is.na(se_vec))){
    stop("Standard error not defined. Please check and rerun.")
  }
  
  i_vec <- which(
    se_vec == 0
  )
  se_vec[i_vec] <- 0.01
  
  var_vec <- se_vec ^ 2
  
  validation_flag <- any(
    is.na(var_vec) | var_vec < 0
  )
  
  if(validation_flag){
    stop("Variance missing or less than 0. Please check and rerun.")
  }
  
  return(var_vec)
}