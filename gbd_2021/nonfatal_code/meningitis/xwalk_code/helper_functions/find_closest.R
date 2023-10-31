## Purpose - round to nearest arbitrary number from list
## Good for rounding to GBD age groups or years

find_closest <- function(numbers, arbitrary.numbers){
  low<-findInterval(numbers,arbitrary.numbers) # find index of number just below
  high<-low+1 # find the corresponding index just above.
  low.diff<-numbers-arbitrary.numbers[ifelse(low==0,NA,low)]
  high.diff<-arbitrary.numbers[ifelse(high==0,NA,high)]-numbers
  mins<-pmin(low.diff,high.diff,na.rm=T) 
  pick<-ifelse(!is.na(low.diff) & mins==low.diff,low,high)
  
  return(pick)
}
