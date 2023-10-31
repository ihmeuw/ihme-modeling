# Miscellaneous utilities for the vaccine pipeline

.get_version_from_path <- function(x) {
  
  x <- unlist(strsplit(x, '/')) 
  x[length(x)]
  
}

# This function grabs the single unique value of a column from a data.table opr data.frame or accepts a vector
# Removes NAs and assumes its a static column (e.g. location_id)
.get_column_val <- function(x) {
  
  if (is.data.table(x)) {
    
    if (ncol(x) > 1) stop('data.table has more than 1 column')
    x <- unique(x[!is.na(x)])
    
  } else if (is.data.frame(x)) {
    
    if (ncol(x) > 1) stop('data.frame has more than 1 column')
    x <- unique(as.vector(x)[!is.na(as.vector(x))])
    
  } else if (is.vector(x)) {
    
    x <- unique(x[!is.na(x)])
    
  }
  
  if (length(x) > 1) stop(paste('Provided object has multiple unique values:', paste(x, collapse=' ')))
  return(x)
  
}

# This function takes a time series vector and fills NAs at the beginning and end of the time series by 
# extending the leading and trailing values out to the end of the provided vector
.extend_end_values <- function(x) {
  
  min_val <- min(x, na.rm=T)
  max_val <- max(x, na.rm=T)
  
  nas_index <- which(is.na(x))
  min_index <- min(which(x == min_val))
  max_index <- max(which(x == max_val))
  
  nas_lead <- nas_index[nas_index < min_index]
  nas_trail <- nas_index[nas_index > max_index]
  
  x[nas_lead] <- min_val
  x[nas_trail] <- max_val
  
  return(x)
}

splice <- function(new, old, locs) {
  
  old$date <- as.Date(old$date)
  
  data <- rbind(
    new[!(location_name %in% locs),],
    old[location_name %in% locs,],
    fill = T
  )
  return(data)
}