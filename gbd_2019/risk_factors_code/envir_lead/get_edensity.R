# Project: RF: Lead Exposure
# Purpose: Build ensemble density provided weights, mean, and sd

## load distribution functions
source("FUNCTION")
## dlist is the universe of distribution families as defined in the above code
## classA is the majority of families, classB is scaled beta, classM is the mirror family of distributions
dlist <- c(classA,classB,classM)

library(dplyr)
library(data.table)
library(compiler)
library(fitdistrplus)


## ensemble density function
# returns a list of x values, fx  - the density, along with XMIN and XMAX
# .min defines user set minimum x value
# .max defines user maximum x value
# scale=TRUE rescales the density so that it integrates to 1 across the defined X vector if the probability is constrained as such
Rcpp::sourceCpp("FILEPATH")

get_edensity <- function(weights,mean,sd,.min,.max,scale=FALSE) {
  W_ <- weights
  M_ <- mean
  S_ <- sd
  
  mu <- log(M_/sqrt(1+(S_^2/(M_^2))))
  sdlog <- sqrt(log(1+(S_^2/M_^2)))
  
  XMIN <- qlnorm(.001,mu,sdlog)
  XMAX <- qlnorm(.999,mu,sdlog)
  
  if(missing(.min) | missing(.max)) {
    xx = seq(XMIN,XMAX,length=1000)
  } else {
    xx = seq(.min,.max,length=1000)
  }
  
  fx = 0*xx
  
  ## remove 0s
  W_ = W_[which(W_>0)]
  
  buildDENlist <- function(jjj) {
    distn = names(W_)[[jjj]]
    EST <- NULL
    LENGTH <- length(formals(unlist(dlist[paste0(distn)][[1]]$mv2par)))
    if (LENGTH==4) {
      EST <- try(unlist(dlist[paste0(distn)][[1]]$mv2par(M_, (S_^2), XMIN=XMIN, XMAX=XMAX)),silent=T)
    } else {
      EST <- try(unlist(dlist[paste0(distn)][[1]]$mv2par(M_, (S_^2))),silent=T)
    }
    d.dist <- NULL
    d.dist <- try(dlist[paste0(distn)][[1]]$dF(xx,EST),silent=T)
    if (class(EST)=="numeric" & class(d.dist)=="numeric") {
      dEST <- EST
      dDEN <- d.dist
      weight <- W_[[jjj]]
    }
    else {
      dEST <- 0
      dDEN <- 0
      weight <- 0
    }
    dDEN[!is.finite(dDEN)] <- 0
    return(list(dDEN=dDEN,weight=weight))
  }
  denOUT <- suppressWarnings(lapply(1:length(W_),buildDENlist))
  
  ## re-scale weights
  TW = unlist(lapply(denOUT, function(x) (x$weight)))
  TW = TW/sum(TW,na.rm = T)
  ## build ensemble density
  fx <- Reduce("+",lapply(1:length(TW),function(jjj) denOUT[[jjj]]$dDEN*TW[jjj]))
  fx[!is.finite(fx)] <- 0
  fx[length(fx)] <- 0
  fx[1] <- 0
  
  if(scale==TRUE) {
    fx <- scale_density_simpson(xx,fx)$s_fx
  }
  
  return(list(fx=fx,x=xx,XMIN=XMIN,XMAX=XMAX))
  
}

get_edensity<-cmpfun(get_edensity)