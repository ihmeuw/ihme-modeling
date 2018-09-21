## R functions to calculate population risk attributable fractions of given distributions
## Accepts lower integration bound, upper, rr_scalar (to put into RR unit space), distribution, 
##    temp file name (I/O between STATA and R), and inv_exp as variables

rm(list = ls())
set.seed(12345)
trailargs <- commandArgs(trailingOnly=TRUE)
L <- as.numeric(trailargs[1])
U <- as.numeric(trailargs[2])
rr_scalar <- as.numeric(trailargs[3])
DIST <- (trailargs[4])
F <- (trailargs[5])
inv_exp <- as.numeric(trailargs[6])

## DEFINE FUNCTION ---------------------------------------------------------------------------------

## with RR cap at upper (99th percentile of exposure) or lower if protective (1st percentile of exposure)
calc_paf_cap <- function(lower, upper, mean, sd, rr, tmrel, rr_scalar, dist, inv_exp, cap) {

  if (dist == "normal") {
    bound<-qnorm(.999999,mean,sd)
    if (inv_exp == 0) {
      denom <- integrate(function(x) dnorm(x, mean, sd) * rr^((((x-tmrel + abs(x-tmrel))/2) - ((x-cap)+abs(x-cap))/2)/rr_scalar), lower, bound, stop.on.error=FALSE)$value
      paf <- integrate(function(x) dnorm(x, mean, sd) * (rr^((((x-tmrel + abs(x-tmrel))/2) - ((x-cap)+abs(x-cap))/2)/rr_scalar) - 1)/denom, lower, bound, stop.on.error=FALSE)$value
    } else if (inv_exp == 1) {
      denom <- integrate(function(x) dnorm(x, mean, sd) * rr^((((tmrel-x + abs(tmrel-x))/2) - ((cap-x) + abs(cap-x))/2)/rr_scalar), lower, bound, stop.on.error=FALSE)$value
      paf <- integrate(function(x) dnorm(x, mean, sd) * (rr^((((tmrel-x + abs(tmrel-x))/2) - ((cap-x) + abs(cap-x))/2)/rr_scalar) - 1)/denom, lower, bound, stop.on.error=FALSE)$value
    }
  }
  else if (dist == "lognormal") {
    mu <- log(mean/sqrt(1+(sd^2/(mean^2))))
    sd <- sqrt(log(1+(sd^2/mean^2)))
    mean <- mu
    bound<-qlnorm(.999999,mean,sd)
    if (inv_exp == 0) {
      denom <- integrate(function(x) dlnorm(x, mean, sd) * rr^((((x-tmrel + abs(x-tmrel))/2) - ((x-cap)+abs(x-cap))/2)/rr_scalar), lower, bound, stop.on.error=FALSE)$value
      paf <- integrate(function(x) dlnorm(x, mean, sd) * (rr^((((x-tmrel + abs(x-tmrel))/2) - ((x-cap)+abs(x-cap))/2)/rr_scalar) - 1)/denom, lower, bound, stop.on.error=FALSE)$value
    } else if (inv_exp == 1) {
      denom <- integrate(function(x) dlnorm(x, mean, sd) * rr^((((tmrel-x + abs(tmrel-x))/2) - ((cap-x) + abs(cap-x))/2)/rr_scalar), lower, bound, stop.on.error=FALSE)$value
      paf <- integrate(function(x) dlnorm(x, mean, sd) * (rr^((((tmrel-x + abs(tmrel-x))/2) - ((cap-x) + abs(cap-x))/2)/rr_scalar) - 1)/denom, lower, bound, stop.on.error=FALSE)$value
    }
  }

  return(paf)

}

## CALC PAF ----------------------------------------------------------------------------------------

library(data.table);library(splitstackshape)

# read draws, reshape long by draw
file <- fread(paste0(F,".csv"))
file <- merged.stack(file,var.stubs=c("exp_mean_","exp_sd_","rr_","tmred_mean_"),sep="var.stubs",keep.all=T)
setnames(file,".time_1","draw")

## calc paf
file[,paf_ := calc_paf_cap(lower=L,
                          upper=U,
                          mean=exp_mean_,
                          sd=exp_sd_,
                          rr =rr_,
                          tmrel=tmred_mean_,
                          rr_scalar=rr_scalar,
                          dist =DIST,
                          inv_exp=inv_exp,
                          cap=cap), by = 1:nrow(file)]

# reshape back and save
cols <- setdiff(names(file),c("exp_mean_","exp_sd_","rr_","tmred_mean_","paf_","draw"))
file <- dcast(file,paste(paste(cols, collapse = " + "), "~ draw"),
  value.var=c("exp_mean_","exp_sd_","rr_","tmred_mean_","paf_"),sep="")
write.csv(file, paste0(F,"_OUT.csv"), row.names=T,na="")

## END_OF_R