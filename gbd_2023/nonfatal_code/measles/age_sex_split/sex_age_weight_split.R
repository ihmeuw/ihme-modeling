#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:  REDACTED 
# Date:    2017, 2020, 2023
# Purpose: Function to age-sex split using COD weights
# NB this script was previously called age_sex_split.R It is sourced by the measles and pertussis 01 scripts
#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
if (Sys.info()["sysname"]=="REDACTED"){
  REDACTED
}

#***********************************************************************************************************************


#----FUNCTION-----------------------------------------------------------------------------------------------------------
### age splitting function by weights from CoD database

age_sex_split <- function(cause_id, input_file, measure) {
  
  # pull in weights
  weights <- read.csv("/FILEPATH/age_sex_weights_gbd2022.csv") %>% as.data.table
  cause_val <- cause_id
  
  #subset to specified cause
  weights <- weights[weights$cause_id == cause_val,]

  # apply population
  split_pop <- population[sex_id %in% 1:2 & year_id %in% 1980:year_end, .(location_id, year_id, age_group_id, sex_id, population)]
  split_pop_weights <- merge(split_pop, weights[, .(age_group_id, sex_id, weight)], by=c("age_group_id", "sex_id"), all.x=TRUE)
  
  split_measure <- merge(split_pop_weights, input_file[, c("location_id", "year_id", paste0(measure, "_draw_", 0:999)), with=FALSE], 
                         by=c("location_id", "year_id"), allow.cartesian=TRUE, all.x=TRUE)
  
  # multiply weights by split population to get expected cases per age-sex group (weight = global mortality rate)
  split_measure[, split_factor := population * weight]
  
  # calculate expected cases for age group aggregate
  split_measure[, split_factor_sum := sum(split_factor), by = c("location_id", "year_id")]
  
  # apply age-sex weight equation to calculate splits
  draw_nums_gbd <- 0:999
  split_draws <- paste0("split_", measure, "_draw_", draw_nums_gbd)
  invisible(
    split_measure[, (split_draws) := lapply( draw_nums_gbd, function(x) split_factor * (get(paste0(measure, "_draw_", x)) / split_factor_sum) )]
  )
  
  # return output
  return(split_measure[, c("location_id", "year_id", "age_group_id", "sex_id", "population", split_draws), with=FALSE])
  
}
