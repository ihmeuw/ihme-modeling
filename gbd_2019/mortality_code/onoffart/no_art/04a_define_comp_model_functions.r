# NAME
# February 2014
# Define functions for compartmental model for no ART HIV mortality

library(stats)
library(utils)
library(compiler)


####################################################################################################################################
## DEFINE RELATIONSHIP BETWEEN PROGRESSION AND MORTALITY WITH CD4 BIN INDEX
####################################################################################################################################

# Slope of CD4 decline
prog.risk <- function(c, d, i, j, s) {

  r <- c + d*i

  if(i!=1) {
    f <- r/bin.width[i]
  }
  else {
    f <- f.1[j]*s
  }
  p <- 1-exp(-f*dt)
  return(p)
}

# Risk of dying during time interval dt - here changing to be risk of death = 1-exp(-lamda*dt), which is HIV specific
mort.risk <- function(a, b, i) {

  r <- a*exp(b*i)
  m <- 1-exp(-r*dt)
  return(m)
}


####################################################################################################################################
## DEFINE CALCULATE LOSS FUNCTION THAT COMPARES COMPARTMENTAL MODEL OUTPUT TO REGRESSION DRAWS
####################################################################################################################################

calculate.loss <- function(param, draw, age.group, scalar) {

  # Re-define input parameters
  a <- param[1]
  b <- param[2]
  c <- param[3]
  d <- param[4]

  # Set initial category populations
  c1[1] <- s1[age.group]
  c2[1] <- s2[age.group]

  # Set progression risks
  p <- rep(0,6)
  for(i in 1:6) {

    p[i] <- prog.risk(c, d, i, age.group, scalar)
  }

  # Set mortality risks
  m <- rep(0,7)
  for(i in 1:7) {
    m[i] <- mort.risk(a, b, i)
  }

  # Step through the model
  for (t in 2:num.steps) {

    c1[t] <- c1[t-1]                - m[1]*c1[t-1] - p[1]*c1[t-1]
    c2[t] <- c2[t-1] + p[1]*c1[t-1] - m[2]*c2[t-1] - p[2]*c2[t-1]
    c3[t] <- c3[t-1] + p[2]*c2[t-1] - m[3]*c3[t-1] - p[3]*c3[t-1]
    c4[t] <- c4[t-1] + p[3]*c3[t-1] - m[4]*c4[t-1] - p[4]*c4[t-1]
    c5[t] <- c5[t-1] + p[4]*c4[t-1] - m[5]*c5[t-1] - p[5]*c5[t-1]
    c6[t] <- c6[t-1] + p[5]*c5[t-1] - m[6]*c6[t-1] - p[6]*c6[t-1]
    c7[t] <- c7[t-1] + p[6]*c6[t-1] - m[7]*c7[t-1]



    d1[t] <- d1[t-1] +  m[1]*c1[t-1]
    d2[t] <- d2[t-1] +  m[2]*c2[t-1]
    d3[t] <- d3[t-1] +  m[3]*c3[t-1]
    d4[t] <- d4[t-1] +  m[4]*c4[t-1]
    d5[t] <- d5[t-1] +  m[5]*c5[t-1]
    d6[t] <- d6[t-1] +  m[6]*c6[t-1]
    d7[t] <- d7[t-1] +  m[7]*c7[t-1]

  }

  ##################################################################
  ## OUTPUT SURVIVAL CURVE
  ##################################################################
  years <- seq(0,num.steps*dt, by=1)
  tot.alive <- rep(1, length(years))
  tot.dead <- rep(1, length(years))
  prop.surv <- rep(1, length(years))

  for (y in 2:length(years)) {
    ## Total left alive or dead
    z <- (y-1)/dt + 1
    tot.alive[y] <- c1[z]+c2[z]+c3[z]+c4[z]+c5[z]+c6[z]+c7[z]
    tot.dead[y] <-  d1[z]+d2[z]+d3[z]+d4[z]+d5[z]+d6[z]+d7[z]
    prop.surv[y] <- tot.alive[y]/(tot.alive[y]+tot.dead[y])
  }


  # Regression output is every only predicts 12 years from seroconversion, so we'll format the survival curve to match this
  surv.pred <- prop.surv[seq(1,13,1)]


  diff <- surv.pred - draw
  loss <- sum(diff^2)

  return(loss)
}
# This bracket closes the calculate.loss function

####################################################################################################################################
## RUN CALCULATE LOSS FUNCTION BUT HAVE OUTPUT BE SURVIVAL CURVE PREDICTIONS SO THESE CAN BE GRAPHED
####################################################################################################################################
pred.surv <- function(param, draw, age.group, scalar) {

  # Re-define input parameters
  a <- param[1]
  b <- param[2]
  c <- param[3]
  d <- param[4]

  # CD4 bins are vectors whose value at index t takes represents the number of living people with a CD4 count in the bin's range at time t.
  c1 <- rep(0,pred.steps)
  c2 <- rep(0,pred.steps)
  c3 <- rep(0,pred.steps)
  c4 <- rep(0,pred.steps)
  c5 <- rep(0,pred.steps)
  c6 <- rep(0,pred.steps)
  c7 <- rep(0,pred.steps)

  # Death bins
  d1 <- rep(0,pred.steps)
  d2 <- rep(0,pred.steps)
  d3 <- rep(0,pred.steps)
  d4 <- rep(0,pred.steps)
  d5 <- rep(0,pred.steps)
  d6 <- rep(0,pred.steps)
  d7 <- rep(0,pred.steps)

  # Set initial category populations
  c1[1] <- s1[age.group]
  c2[1] <- s2[age.group]

  # Set progression risks
  p <- rep(0,6)
  for(i in 1:6) {

    p[i] <- prog.risk(c, d, i, age.group, scalar)
  }

  # Set mortality risks
  m <- rep(0,7)
  for(i in 1:7) {
    m[i] <- mort.risk(a, b, i)
  }

  # Step through the model
  for (t in 2:pred.steps) {

    c1[t] <- c1[t-1]                - m[1]*c1[t-1] - p[1]*c1[t-1]
    c2[t] <- c2[t-1] + p[1]*c1[t-1] - m[2]*c2[t-1] - p[2]*c2[t-1]
    c3[t] <- c3[t-1] + p[2]*c2[t-1] - m[3]*c3[t-1] - p[3]*c3[t-1]
    c4[t] <- c4[t-1] + p[3]*c3[t-1] - m[4]*c4[t-1] - p[4]*c4[t-1]
    c5[t] <- c5[t-1] + p[4]*c4[t-1] - m[5]*c5[t-1] - p[5]*c5[t-1]
    c6[t] <- c6[t-1] + p[5]*c5[t-1] - m[6]*c6[t-1] - p[6]*c6[t-1]
    c7[t] <- c7[t-1] + p[6]*c6[t-1] - m[7]*c7[t-1]



    d1[t] <- d1[t-1] +  m[1]*c1[t-1]
    d2[t] <- d2[t-1] +  m[2]*c2[t-1]
    d3[t] <- d3[t-1] +  m[3]*c3[t-1]
    d4[t] <- d4[t-1] +  m[4]*c4[t-1]
    d5[t] <- d5[t-1] +  m[5]*c5[t-1]
    d6[t] <- d6[t-1] +  m[6]*c6[t-1]
    d7[t] <- d7[t-1] +  m[7]*c7[t-1]

  }

  ##################################################################
  ## OUTPUT SURVIVAL CURVE
  ##################################################################
  years <- seq(0,pred.steps*dt, by=1)
  tot.alive <- rep(1, length(years))
  tot.dead <- rep(1, length(years))
  prop.surv <- rep(1, length(years))

  for (y in 2:length(years)) {
    ## Total left alive or dead
    z <- (y-1)/dt + 1
    tot.alive[y] <- c1[z]+c2[z]+c3[z]+c4[z]+c5[z]+c6[z]+c7[z]
    tot.dead[y] <-  d1[z]+d2[z]+d3[z]+d4[z]+d5[z]+d6[z]+d7[z]
    prop.surv[y] <- tot.alive[y]/(tot.alive[y]+tot.dead[y])
  }

  # Regression output is every only predicts 12 years from seroconversion, so we'll format the survival curve to match this
  surv.pred <- prop.surv[seq(1,32,1)]

  return(surv.pred)
}
# This bracket closes the calculate.loss function


##################################################################
## COMPILE CALCULATE LOSS FUNCTION AND PREDICT SURVIVAL FUNCTION SO THEY RUN FASTER
##################################################################
cmp.calculate.loss <- cmpfun(calculate.loss)
cmp.pred.surv <- cmpfun(pred.surv)


