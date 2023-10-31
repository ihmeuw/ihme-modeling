###########################################################
### Date: 5/28/2019
### Description: Accepts data as data.table argument, identifies data-series (defined by sex, location, year and NID) for which all observed values are 0 and those for which the age-adjusted mean is more than xMADs above or below median, marks them as outliers, returns data.table that can be uploaded to Epi DB.  Should be applied to data that have already had sex and age patterns imposed and have been cross-walked toward reference study design, if any of those transformations are required.
### Based on: GBS crosswalk code of Emma Nichols from 12/2017
###########################################################

# To source this function from another script: source("/FILEPATH/code/outlierbyMAD.R")

# Requires the following packages 
# pacman::p_load(data.table, ggplot2, readr, RMySQL, openxlsx, readxl, stringr, tidyr, plyr, dplyr, dbplyr)
# Also requires
## data that are in a standard set of mutually exclusive and collectively exhaustive age-groups
## an age_dt table of global population age-weights for the set of mutually exclusive and collectively exhaustive age-groups in the data

auto_outlier <- function(transformed_data, numb_mad=2, standardized = TRUE){

  dt <- copy(transformed_data)
  
  if (standardized == TRUE) {
  
    byvars <- c("location_id", "sex", "year_start", "year_end", "nid")
    
    ##merge age table map and merge on to dataset
    dt <- merge(dt, age_dt, by = c("age_start", "age_end"), all.x = T)
    
    #calculate age-standardized prevalence/incidence
    
    ##create new age-weights for each data source
    dt <- dt[, sum := sum(age_group_weight_value), by = byvars] #sum of standard age-weights for all the ages we are using, by location-age-sex-nid, sum will be same for all age-groups and possibly less than one 
    dt <- dt[, new_weight := age_group_weight_value/sum, by = byvars] #divide each age-group's standard weight by the sum of all the weights in their locaiton-age-sex-nid group 
    
    ##age standardizing per location-year by sex
    #add a column titled "age_std_mean" with the age-standardized mean for the location-year-sex-nid
    dt[, as_mean := mean * new_weight] #initially just the weighted mean for that AGE-location-year-sex-nid
    dt[, as_mean := sum(as_mean), by = byvars] #sum across ages within the location-year-sex-nid group, you now have age-standardized mean for that series
    
    ##mark as outlier if age standardized mean is 0 (either biased data or rare disease in small ppln)
    dt[as_mean == 0, is_outlier := 1] 
    
    if (numb_mad != 0) {
      ## log-transform to pick up low outliers
      dt[as_mean != 0, as_mean := log(as_mean)]
      
      # calculate median absolute deviation
      dt[as_mean == 0, as_mean := NA] ## don't count zeros in median calculations
      dt[,mad:=mad(as_mean,na.rm = T),by=c("sex")]
      dt[,median:=median(as_mean,na.rm = T),by=c("sex")]
      
      #assign outlier status if # MAD above or below median exceeds number in fxn argument
      dt[as_mean>((numb_mad*mad)+median), is_outlier := 1]
      dt[as_mean<(median-(numb_mad*mad)), is_outlier := 1]
    }  
    
    dt[, c("sum", "new_weight", "as_mean", "median", "mad", "age_group_weight_value", "age_group_id") := NULL]
  
  } else {
    
    dt[mean != 0, comp_mean := log(mean)]
    dt[mean == 0, comp_mean := NA] ## don't count zeros in median calculations
    dt[ , age_mid:=age_end-age_start]
    
    # calculate median absolute deviation
    dt[,mad:=mad(comp_mean,na.rm = T),by=c("sex", "age_mid")]
    dt[,median:=median(comp_mean,na.rm = T),by=c("sex", "age_mid")]
    
    #assign outlier status if # MAD above or below median exceeds number in fxn argument
    dt[comp_mean>((numb_mad*mad)+median), is_outlier := 1]
    dt[comp_mean<(median-(numb_mad*mad)), is_outlier := 1]
    
  }
  
  return(dt)

}


