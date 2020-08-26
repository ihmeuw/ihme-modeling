################################################################################
### Project: Anemia 
### Purpose: Estimate variance of data missing variance/standard deviation by 
###          using global coefficient of variation approach
################################################################################

prep_variance <- function(df){
  
  message(paste0("Prepping variance for bundle ",me))
  ## generate global mean of coefficient of variation
  # Compute standard deviation
  df[,standard_deviation := NA]
  df[!is.na(variance)&cv_var_impute==0, standard_deviation := sqrt(sample_size * variance)]
  # Take a global mean of the coefficient of variation
  df[, cv.mean := mean(standard_deviation/val, na.rm=T)]
  # Estimate missing standard_deviation using global average cv
  df[is.na(standard_deviation), standard_deviation := val * cv.mean]
  # Impute sample size using 5th percentile sample_size
  sample_size_lower <- quantile(df[sample_size > 0, sample_size], 0.05, na.rm=T)
  df[is.na(sample_size) | sample_size==0, sample_size := sample_size_lower]
  # Estimate variance where missing
  df[is.na(variance), variance := standard_deviation^2/sample_size]
  df[is.na(standard_error), standard_error := sqrt(variance)]
  message("Variance successfully prepped!")
  
  return(df)
}