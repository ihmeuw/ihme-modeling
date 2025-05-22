### Quantile functions for USA self-report adjustment

################################################################################
# Calculate quantiles & pairs
################################################################################

# global variables
strat_vars <- c("age20", "sex_id")
num_quantiles <- 15

# calculated empirical weighted quantile based on self-reported BMI

# Create wrapper around Hmisc::wtd.Ecdf() b/c this function
# returns the ecdf, but it's not evaluated for each row.
# In this wrapper, calculate ecdf using Hmisc, turn it into
# a function so that we can calculate the quantile for each strata
calc_quantile <- function(.x, .weights, .normwt, .na.rm) {
  # error if there are NAs in .x or .weights
  if(any(is.na(.x)) | any(is.na(.weights))){
    stop("There are NAs in the BMI or weights")
  }
  # print(length(unique(.x)))
  if(length(unique(.x))>1){
    # calculate ecdf with Hmisc -- this returns the unique X values and the 
    # CDF for each of those values.
    ecdf <- Hmisc::wtd.Ecdf(
      x = .x, 
      weights = .weights, 
      normwt = .normwt, 
      na.rm = .na.rm,
      type = "i/n") 
    
    # Turn ecdf into a function by interpolation
    ecdf_fun <- approxfun(
      x = ecdf$x, 
      y = ecdf$ecdf, 
      method = "linear", 
      yleft = 0, 
      yright = 1,
      ties = "mean" 
    )
    # calculate the quantile for each observation
    return(ecdf_fun(.x))
  } else{
    print("<2 obs, skip to next")
  }
}

ushd_quantile <- function(data) {
  # Loop through each subset of the data.
  # Within that subset and any additional stratifications (age, sex, race, ...),
  # calculate each respondents' quantile in the empirical, weighted self-reported 
  # BMI distribution for that source and strata.
  
  ## create 20 year age groups (<20, 20-39, 40-59, 60+)
  data[, age20 := ""][age_group_id %in% c(9:12), age20 := "20_39"][age_group_id %in% c(13:16), age20 := "40_59"][age_group_id %in% c(17:20,30:32,235), age20 := "60+"]

  tmp <- data[!is.na(bmi_rep) & diagnostic=="self-report"]
  if(nrow(tmp)==0) return(tmp)
    
    ## set consistent weight for all
    tmp[, wt:=pweight][is.na(wt), wt := 1]
    
    tmp[, bmi_report_percentile := calc_quantile(
      .x = bmi_rep,
      .weights = wt,
      .normwt = TRUE,
      .na.rm = FALSE), # shouldn't have NA's
      by = c(strat_vars, "nid")]
    
    # error if there are NAs in bmi_report_percentile
    tmp <- tmp[!is.na(bmi_report_percentile)]
    stopifnot(!any(is.na(tmp$bmi_report_percentile)))
    
    # collapse the data within each bin of self-reported quantiles
    # Create bins of percentiles, based on the number of quantiles specified in settings
    tmp[, bmi_report_percentile_bin := cut(
      bmi_report_percentile, 
      breaks = seq(0, 1, length.out = num_quantiles + 1),
      include.lowest = T,
      labels = FALSE)
    ]
    tmp[, bmi_report_percentile_bin := as.integer(as.character(bmi_report_percentile_bin))]
return(tmp)
}
