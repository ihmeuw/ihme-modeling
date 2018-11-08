#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Function for rescaling subnational units to fit national envelope
#          Need to already have locations loaded
#          Can only rake in count space 
# Inputs:  measure - whatever your draws are called (default measure is "case_draw_", for columns named "case_draw_{x}", where x is 0 through 999)
#          data - dataset to rake by 1,000 draws
#***********************************************************************************************************************


#----FUNCTION-----------------------------------------------------------------------------------------------------------
### call libraries 
library(data.table)

### function
rake <- function(data, measure="case_draw_") {
  
  ### set options
  draw_nums <- 0:999
  parent_cols <- paste0("parent_draw_", draw_nums)
  rake_cols <- paste0("rake_draw_", draw_nums)
  tot_cols <- paste0("tot_draw_", draw_nums)
  draw_cols <- paste0(measure, draw_nums)
  replace_cols <- paste0("replace_draw_", draw_nums)
  
  ### pull locations
  locations_rake <- locations[, .(location_id, level, parent_id)]
  # list locations that are parents of subnational units
  parents <- locations_rake[level %in% c(3:(max(locations_rake$level) - 1)), location_id]
  parents <- parents[parents %in% c(locations_rake$parent_id)]
  
  ### prep dataset
  # make sure is data.table
  if (!"data.table" %in% class(data)) data <- data %>% as.data.table
  # merge on locations
  data <- merge(data, locations_rake, by="location_id", all.x=TRUE)
  # results from parents to use for rescaling
  parent_rake <- data[location_id %in% parents, ] %>% .[, parent_id := NULL]
  setnames(parent_rake, draw_cols, parent_cols)
  setnames(parent_rake, "location_id", "parent_id")
  # merge on parent draws to subnational draws
  data_rake <- merge(data, parent_rake[, c("parent_id", "year_id", parent_cols), with=FALSE], by=c("parent_id", "year_id"), all.x=TRUE)
  
  for (LEVEL in 4:max(locations_rake$level)) {
    
    ### sum subnationals
    data_rake[level==LEVEL, (tot_cols) := lapply(draw_nums, function(x) { sum(get(paste0(measure, x))) }), by=c("parent_id", "year_id")]
    
    ### calculate raked cases as the proportion of that subnational-year of sum of subnationals-year, multiply that proportion by the modeled cases of the parent
    data_rake[level==LEVEL, (rake_cols) := lapply(draw_nums, function(x) { ( get(paste0(measure, x)) / get(paste0("tot_draw_", x)) ) * get(paste0("parent_draw_", x)) } )]
    
    if (LEVEL != max(locations_rake$level)) {
      
      ### prep next level for raking by replacing model estimates for parents with raked estimates (for countries with multiple levels of subnationals)
      data_replace <- data_rake[level==LEVEL, ]
      setnames(data_replace, rake_cols, replace_cols)
      data_replace <- data_replace[, c("location_id", "year_id", replace_cols), with=FALSE]
      setnames(data_replace, "location_id", "parent_id")
      data_rake <- merge(data_rake, data_replace, by=c("parent_id", "year_id"), all.x=TRUE)
      # rake next level using raked estimates from one level above
      data_rake[!is.na(get(replace_cols[1])), (parent_cols) := lapply(replace_cols, function(x) get(x))]
      data_rake[, (replace_cols) := NULL]
      
    }
    
  }
  
  ### just keep raked estimates for output
  data_rake <- data_rake[level != 3, ]
  data_rake <- data_rake[, c("location_id", "year_id", rake_cols), with=FALSE]
  setnames(data_rake, rake_cols, draw_cols)
  
  ### add on national model estimates (which were used for raking but not raked themselves)
  data_parents <- data[level==3, c("location_id", "year_id", draw_cols), with=FALSE]
  data_final <- rbind(data_parents, data_rake, fill=TRUE)
  
  return(data_final)
  
}
#***********************************************************************************************************************