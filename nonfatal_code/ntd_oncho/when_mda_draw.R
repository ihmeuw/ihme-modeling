
when_mda_draw <- function(num_treated, num_needing, max_age, lengthMDA, minD, maxD) {
  num_not_treated <- num_needing - num_treated
  if (num_not_treated < 0) {
    num_not_treated <- 0
  }
  
  rbet_MDA_value <- rbeta(1000, num_treated, num_not_treated)
  rbet_nonMDA_value <- 1 - rbet_MDA_value
  
  if (lengthMDA == 1) {
    duration_MDA <- 365 / (rpois(1000,min(365, max_age*365)))
  } else {
    duration_MDA <- 365 / (rpois(1000, min(182.5, max_age*365)))
  }
  if (max_age < maxD) {    
    duration_nonMDA <-  365 / rpois(1000, max_age*365)
  } else {  
    duration_nonMDA <- 365 / rpois(1000, 365*((minD+maxD)/2))
  }
  
  treated <- rbet_MDA_value*duration_MDA
  non_treated <- rbet_nonMDA_value*duration_nonMDA
  if (num_not_treated == 0) {
    remis <- treated
  } else {
    remis <- treated + non_treated
  }
  return(remis)
}
