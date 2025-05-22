#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Function for collapsing draws into mean, lower, and upper (95% UI) quantiles
# Inputs:  data - data containing draws
#***********************************************************************************************************************


#----COLLAPSE FUNCTION--------------------------------------------------------------------------------------------------
# write function for collapsing draws into mean-lower-upper
collapse_point <- function(input_file, draws_name="draw", variance=FALSE, keep_draws=FALSE) {
  
  # get data to collapse
  collapsed       <- copy(input_file)
  
  # columns to collapse
  collapse_cols   <- c(colnames(collapsed)[grep(draws_name, colnames(collapsed))])
  
  # column names to keep
  cols <- c(colnames(collapsed)[colnames(collapsed) %in% c("global_id", "super_region_id", "region_id", "location_id", "ihme_loc_id", "year_id", "age_group_id", 
                                                           "sex_id", "scenario", "wb_region", "wb_income", "region_name", "super_region_name", "run_id")], 
            "mean", "lower", "upper")
  if (keep_draws) cols <- c(cols, collapse_cols)
  
  # check for missingness
  if (sapply(collapsed[, collapse_cols, with=F], function(x) sum(is.na(x))) %>% sum > 0) print (
    paste0("Warning - NAs detected in draw columns. Looks like ", (sapply(collapsed[, collapse_cols, with=F], function(x) sum(is.na(x))) > 0) %>% sum, " draws are missing values for an average of ", 
           sapply(collapsed[, collapse_cols, with=F], function(x) sum(is.na(x))) %>% sum / (sapply(collapsed[, collapse_cols, with=F], function(x) sum(is.na(x))) > 0) %>% sum, " rows each.")
  )
  
  # calculate mean, lower, and upper
  collapsed$mean  <- rowMeans(subset(collapsed, select=collapse_cols), na.rm=TRUE)
  collapsed$lower <- apply(subset(collapsed, select=collapse_cols), 1, function(x) quantile(x, 0.025, na.rm=TRUE))
  collapsed$upper <- apply(subset(collapsed, select=collapse_cols), 1, function(x) quantile(x, 0.975, na.rm=TRUE))
  
  # calculate variance
  if (variance) { collapsed$variance <- apply(subset(collapsed, select=collapse_cols), 1, function(x) var(x))
                  cols <- c(cols, "variance") }
  
  # just keep needed columns
  collapsed <- subset(collapsed, select=cols)

  return(collapsed)
}
#***********************************************************************************************************************