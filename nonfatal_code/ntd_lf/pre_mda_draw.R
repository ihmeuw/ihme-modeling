
pre_mda_draw <- function(maximumAge, lengthMDA, minD, maxD) {
  if (maximumAge < maxD) {  
    duration_nonMDA <-  365 / rpois(1000, maximumAge*365)
  } else { 
    duration_nonMDA <- 365 / rpois(1000, 365*((minD+maxD)/2))
  }
  return(duration_nonMDA)
}
