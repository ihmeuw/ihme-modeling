#'@param inc Length 1 numeric 
IncPrevFindRoot <- function(inc){
  
  if(length(inc) > 1) stop('IncPrevFindRoot is for single values only.')
  
  
  if(inc > 0.6195216){
    prev <- 0.6162
  } else {
    solution <- uniroot(PrevIncRoot, inc, interval = c(0, 0.6162))
    prev <- solution$root
  }
  
  return(prev)
  
}

#' Function to be solved.
PrevIncRoot <- function(prev, inc){
  prev * 2.616 -
  prev^2 * 3.596 +
  prev^3 * 1.594 -
  inc
}


#'@param inc Numeric vector. 
IncPrevConversion <- function(inc){
  sapply(inc, IncPrevFindRoot)
}
  
#Top function for single values, bottom function just does the top function for a whole vector. 
#This is doing numeric solutions, so will be pretty slow on massive (raster) datasets, but fine for the time series stuff.