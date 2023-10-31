
# This function takes a data frame with 'location_id' and aggregates to the spatial level given by the 'level' argument

set_spatial_scale <- function(data, level, hierarchy) {
  
  check_df <- is.data.frame(data) & !(is.data.table(data))
  if (check_df) data <- as.data.table(data)
  if (!('parent_name' %in% colnames(data))) data <- .add_parent_names(d=data, hierarchy = hierarchy)
  
  # Merge in spatial levels from hierarchy
  sel <- which(!(colnames(data) %in% colnames(hierarchy)[-which(colnames(hierarchy) == 'location_id')]))
  data <- merge(data[,..sel], 
                hierarchy[,.(location_id, location_name, parent_id, region_id, region_name, super_region_id, super_region_name)], 
                by='location_id')
  
  if (level == 'global') {
    
    data$location_id <- 1
    data$location_name <- 'Global'
    data$parent_id <- data$parent_name <- data$region_id <- data$region_name <- data$super_region_id <- data$super_region_name <- NA
  
  } else if (level == 'super_region') {
    
    data$location_id <- data$super_region_id
    data$location_name <- data$super_region_name
    data$region_id <- data$region_name <- NA
    data$parent_id <- 1
    data$parent_name <- 'Global'
    
  } else if (level == 'region') {
    
    data$location_id <- data$region_id
    data$location_name <- data$region_name
    data$parent_id <- data$super_region_id
    data$parent_name <- data$super_region_name
    
  } else if (level == 'parent') {
    
    # Keep only locations that comprise a missing national level parent (this will likely be only the US)
    
    national_parents <- hierarchy[most_detailed == 0 & level == 3, location_id]
    
    # National level parents that already have data
    data_parents_present <- data[data$location_id %in% national_parents,]
    
    # National parents that are missing but can be modeled by aggregating children locs
    missing_national_parents <- national_parents[!(national_parents %in% data$location_id)]
    data_parents_missing <- data[data$parent_id %in% missing_national_parents,]
    data_parents_missing$location_id <- data_parents_missing$parent_id
    data_parents_missing$location_name <- data_parents_missing$parent_name
    data_parents_missing$parent_id <- data_parents_missing$region_id
    data_parents_missing$parent_name <- data_parents_missing$region_name
    
    data <- rbind(data_parents_present, data_parents_missing)
    
  } else if (level == 'location') {
    
    data <- .add_parent_names(data, hierarchy)
    
  } else {
    
    stop('level argument not recognized')
  }
  
  if (check_df) data <- as.data.frame(data)
  return(data) 
}