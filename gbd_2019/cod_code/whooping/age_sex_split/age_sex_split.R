#----HEADER-------------------------------------------------------------------------------------------------------------
# Author: "USERNAME"
# Purpose: Function to age-sex split using COD weights
#          Inputs:
#          1. acause - any acause in COD weight db (i.e. measles, whooping)
#          2. input_file - must have location_id, year_id, and 1000 draws of paste0(measure, "_draw_", 0:999)
#          3. measure - can be, for example, death or case
#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
if (Sys.info()["sysname"]=="Linux") {
  j_root <- "FILEPATH" 
} else if (Sys.info()["sysname"]=="Darwin") {
  j_root <- ""FILEPATH""
} else { 
  j_root <- ""FILEPATH""
}
#***********************************************************************************************************************


#----FUNCTION-----------------------------------------------------------------------------------------------------------
### agesplitting function by weights from CoD database
age_sex_split <- function(acause, input_file, measure) {
  
  # pull in weights
  weights <- foreign::read.dta(file.path(""FILEPATH"weights", 
              paste0("acause_age_weight_", acause, ".dta"))) %>% as.data.table
  age_map <- data.table(age=c(91, 93, 94, 1, seq(5, 95, 5)), age_group_id=c(2:20, 30:32, 235))
  weights <- merge(weights, age_map, by="age", all.x=TRUE)
  setnames(weights, "sex", "sex_id")
  
  # apply population
  split_pop <- population[sex_id %in% 1:2 & year_id %in% 1980:year_end, .(location_id, year_id, age_group_id, sex_id, population)]
  split_pop_weights <- merge(split_pop, weights[, .(age_group_id, sex_id, weight)], by=c("age_group_id", "sex_id"), all.x=TRUE)
  split_measure <- merge(split_pop_weights, input_file[, c("location_id", "year_id", paste0(measure, "_draw_", 0:999)), with=FALSE], 
                         by=c("location_id", "year_id"), allow.cartesian=TRUE, all.x=TRUE)
  
  # multiply weights by split population to get expected cases per age-sex group
  split_measure[, split_factor := population * weight]
  
  # calculate expected cases for age group aggregate
  split_factor_sums <- plyr::ddply(split_measure, c("location_id", "year_id"), summarise, split_factor_sum=sum(split_factor) )
  split_measure <- merge(split_measure, split_factor_sums, by=c("location_id", "year_id"), allow.cartesian=TRUE, all.x=TRUE)
  
  # apply age-sex weight equation to calculate splits
  draw_nums_gbd <- 0:999
  split_draws <- paste0("split_", measure, "_draw_", draw_nums_gbd)
  invisible(
    split_measure[, (split_draws) := lapply( draw_nums_gbd, function(x) split_factor * (get(paste0(measure, "_draw_", x)) / split_factor_sum) )]
  )
  
  # return output
  return(split_measure[, c("location_id", "year_id", "age_group_id", "sex_id", "population", split_draws), with=FALSE])
  
}
#***********************************************************************************************************************