#######################################################################################
### Date:     24th November 2017
### Purpose:  Converts ORs to RRs using reported OR, proportion of cases, and 
###           proportion exposed to the risk factor. Uses method developed for EpiGear
###           by Barendregt, J, 2010.
#######################################################################################

# or = reported odds ratio
# s = proportion of cases in sample
# p = proportion of sample exposed to risk factor

or_2_rr <- function(or, s, p){
  rr = 1
  test_or = (rr*(1 - (s / (p*rr+1-p)))) / (1 - (rr*s)/(p*rr+1-p))
  if(!is.na(test_or) & !is.na(or)){
    if(test_or < or){
      while(test_or < or){
        rr = rr+0.0001
        test_or = (rr*(1 - (s / (p*rr+1-p)))) / (1 - (rr*s)/(p*rr+1-p))
      }
    }
    if(test_or > or){
      while(test_or > or){
        rr = rr-0.0001
        test_or = (rr*(1 - (s / (p*rr+1-p)))) / (1 - (rr*s)/(p*rr+1-p))
      }
    }
  } else if(is.na(test_or) | is.na(or)){
    rr = as.numeric(NA)
    warning("NA value given. Function requires OR, s, and p to estimate RRs")
  }
  return(rr)
}



